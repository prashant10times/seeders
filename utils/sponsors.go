package utils

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"seeders/shared"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type SponsorRecord struct {
	CompanyID        *uint32 `ch:"company_id"`
	CompanyUUID      string  `ch:"company_uuid"`
	CompanyIDName    string  `ch:"company_id_name"`
	EditionID        uint32  `ch:"edition_id"`
	EventID          uint32  `ch:"event_id"`
	CompanyWebsite   *string `ch:"company_website"`
	CompanyDomain    *string `ch:"company_domain"`
	CompanyCountry   *string `ch:"company_country"`
	CompanyState     *uint32 `ch:"company_state"`
	CompanyStateName *string `ch:"company_state_name"`
	CompanyCity      *uint32 `ch:"company_city"`
	CompanyCityName  *string `ch:"company_city_name"`
	FacebookID       *string `ch:"facebook_id"`
	LinkedinID       *string `ch:"linkedin_id"`
	TwitterID        *string `ch:"twitter_id"`
	Created          string  `ch:"created"`
	Version          uint32  `ch:"version"`
	LastUpdatedAt    string  `ch:"last_updated_at"`
	Published        int8    `ch:"published"`
	SponsorSourceID  uint32  `ch:"sponsorSourceId"`
}

func ProcessSponsorsOnly(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) {
	log.Println("=== Starting SPONSORS ONLY Processing ===")

	totalRecords, minID, maxID, err := shared.GetTotalRecordsAndIDRange(mysqlDB, "event_sponsors")
	if err != nil {
		log.Fatal("Failed to get total records and ID range from event_sponsors:", err)
	}

	log.Printf("Total sponsors records: %d, Min ID: %d, Max ID: %d", totalRecords, minID, maxID)

	if config.NumChunks <= 0 {
		config.NumChunks = 5
	}

	chunkSize := (maxID - minID + 1) / config.NumChunks
	if chunkSize == 0 {
		chunkSize = 1
	}

	log.Printf("Processing sponsors data in %d chunks with chunk size: %d", config.NumChunks, chunkSize)

	results := make(chan string, config.NumChunks)
	semaphore := make(chan struct{}, config.NumWorkers)

	for i := 0; i < config.NumChunks; i++ {
		startID := minID + (i * chunkSize)
		endID := startID + chunkSize - 1
		if i == config.NumChunks-1 {
			endID = maxID
		}

		if i > 0 {
			delay := 3 * time.Second
			log.Printf("Waiting %v before launching sponsors chunk %d...", delay, i+1)
			time.Sleep(delay)
		}

		semaphore <- struct{}{}
		go func(chunkNum, start, end int) {
			defer func() { <-semaphore }()
			processSponsorsChunk(mysqlDB, clickhouseConn, config, start, end, chunkNum, results)
		}(i+1, startID, endID)
	}

	for i := 0; i < config.NumChunks; i++ {
		result := <-results
		log.Printf("Sponsors Result: %s", result)
	}

	log.Println("Sponsors processing completed!")
}

func processSponsorsChunk(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config, startID, endID int, chunkNum int, results chan<- string) {
	log.Printf("Processing sponsors chunk %d: ID range %d-%d", chunkNum, startID, endID)

	totalRecords := endID - startID + 1
	processed := 0

	offset := 0
	for {
		batchData, err := buildSponsorsMigrationData(mysqlDB, startID, endID, config.BatchSize)
		if err != nil {
			results <- fmt.Sprintf("Sponsors chunk %d batch error: %v", chunkNum, err)
			return
		}

		if len(batchData) == 0 {
			break
		}

		processed += len(batchData)
		progress := float64(processed) / float64(totalRecords) * 100
		log.Printf("Sponsors chunk %d: Retrieved %d records in batch (%.1f%% complete)", chunkNum, len(batchData), progress)

		var sponsorCompanyIDs []int64
		seenCompanyIDs := make(map[int64]bool)
		for _, sponsor := range batchData {
			if companyID, ok := sponsor["company_id"].(int64); ok && companyID > 0 {
				if !seenCompanyIDs[companyID] {
					sponsorCompanyIDs = append(sponsorCompanyIDs, companyID)
					seenCompanyIDs[companyID] = true
				}
			}
		}

		var companyData map[int64]map[string]interface{}
		if len(sponsorCompanyIDs) > 0 {
			log.Printf("Sponsors chunk %d: Fetching company data for %d companies", chunkNum, len(sponsorCompanyIDs))
			startTime := time.Now()
			companyData = fetchSponsorsCompanyData(mysqlDB, sponsorCompanyIDs)
			companyTime := time.Since(startTime)
			log.Printf("Sponsors chunk %d: Retrieved company data for %d companies in %v", chunkNum, len(companyData), companyTime)
		}

		var sponsorCityIDs []int64
		seenCityIDs := make(map[int64]bool)
		for _, company := range companyData {
			if cityID, ok := company["city"].(int64); ok && cityID > 0 {
				if !seenCityIDs[cityID] {
					sponsorCityIDs = append(sponsorCityIDs, cityID)
					seenCityIDs[cityID] = true
				}
			}
		}

		var cityData []map[string]interface{}
		var cityLookup map[int64]map[string]interface{}
		if len(sponsorCityIDs) > 0 {
			log.Printf("Sponsors chunk %d: Fetching city data for %d cities", chunkNum, len(sponsorCityIDs))
			startTime := time.Now()
			cityData = shared.FetchCityDataParallel(mysqlDB, sponsorCityIDs, config.NumWorkers)
			cityTime := time.Since(startTime)
			log.Printf("Sponsors chunk %d: Retrieved city data for %d cities in %v", chunkNum, len(cityData), cityTime)

			cityLookup = make(map[int64]map[string]interface{})
			if len(cityData) > 0 {
				for _, city := range cityData {
					if cityID, ok := city["id"].(int64); ok {
						cityLookup[cityID] = city
					}
				}
			}
		}

		now := time.Now().Format("2006-01-02 15:04:05")
		sponsorRecords := BuildSponsorRecordsFromBatchData(batchData, companyData, cityLookup, now)

		if len(sponsorRecords) > 0 {
			log.Printf("Sponsors chunk %d: Attempting to insert %d records into event_sponsors_ch...", chunkNum, len(sponsorRecords))

			attemptCount := 0
			sponsorInsertErr := shared.RetryWithBackoff(
				func() error {
					if attemptCount > 0 {
						now := time.Now().Format("2006-01-02 15:04:05")
						for i := range sponsorRecords {
							sponsorRecords[i].LastUpdatedAt = now
						}
						log.Printf("Sponsors chunk %d: Updated last_updated_at for retry attempt %d", chunkNum, attemptCount+1)
					}
					attemptCount++
					return insertSponsorsDataIntoClickHouse(clickhouseConn, sponsorRecords, config.ClickHouseWorkers)
				},
				3,
			)

			if sponsorInsertErr != nil {
				log.Printf("Sponsors chunk %d: Insertion failed after retries: %v", chunkNum, sponsorInsertErr)
				results <- fmt.Sprintf("Sponsors chunk %d: Failed to insert %d records", chunkNum, len(sponsorRecords))
				return
			} else {
				log.Printf("Sponsors chunk %d: Successfully inserted %d records into event_sponsors_ch", chunkNum, len(sponsorRecords))
			}
		}

		if len(batchData) > 0 {
			lastID := batchData[len(batchData)-1]["id"]
			if lastID != nil {
				if id, ok := lastID.(int64); ok {
					startID = int(id) + 1
				}
			}
		}

		offset += len(batchData)
		if len(batchData) < config.BatchSize {
			break
		}
	}

	results <- fmt.Sprintf("Sponsors chunk %d: Completed successfully", chunkNum)
}

func BuildSponsorRecordsFromBatchData(
	batchData []map[string]interface{},
	companyData map[int64]map[string]interface{},
	cityLookup map[int64]map[string]interface{},
	now string,
) []SponsorRecord {
	var sponsorRecords []SponsorRecord
	for _, sponsor := range batchData {
		var companyWebsite, companyDomain, facebookID, linkedinID, twitterID, companyCountry, companyCity interface{}
		if companyID, ok := sponsor["company_id"].(int64); ok && companyData != nil {
			if company, exists := companyData[companyID]; exists {
				companyWebsite = company["website"]
				companyCountry = strings.ToUpper(shared.SafeConvertToString(company["country"]))
				companyCity = company["city"]

				if website, ok := companyWebsite.(string); ok && website != "" {
					companyDomain = shared.ExtractDomainFromWebsite(website)
				} else if website, ok := companyWebsite.([]byte); ok && len(website) > 0 {
					websiteStr := string(website)
					companyDomain = shared.ExtractDomainFromWebsite(websiteStr)
				}

				facebookID = company["facebook_id"]
				linkedinID = company["linkedin_id"]
				twitterID = company["twitter_id"]
			}
		}

		var companyCityName *string
		if companyCity != nil {
			if cityID, ok := companyCity.(int64); ok && cityLookup != nil {
				if city, exists := cityLookup[cityID]; exists && city["name"] != nil {
					nameStr := shared.SafeConvertToString(city["name"])
					companyCityName = &nameStr
				}
			}
		}

		var companyState *uint32
		var companyStateName *string
		if companyCity != nil {
			if cityID, ok := companyCity.(int64); ok && cityLookup != nil {
				if city, exists := cityLookup[cityID]; exists {
					if city["state_id"] != nil {
						if stateID, ok := city["state_id"].(int64); ok && stateID > 0 {
							stateIDUint32 := uint32(stateID)
							companyState = &stateIDUint32
						}
					}
					if city["state"] != nil {
						stateStr := shared.SafeConvertToString(city["state"])
						if strings.TrimSpace(stateStr) != "" {
							companyStateName = &stateStr
						}
					}
				}
			}
		}

		companyID := shared.ConvertToUInt32Ptr(sponsor["company_id"])
		editionID := shared.ConvertToUInt32(sponsor["event_edition"])
		eventID := shared.ConvertToUInt32(sponsor["event_id"])
		sponsorSourceID := shared.ConvertToUInt32(sponsor["id"])

		sponsorRecords = append(sponsorRecords, SponsorRecord{
			CompanyID:        companyID,
			CompanyUUID:      shared.GenerateUUIDFromString(fmt.Sprintf("%d-%s", shared.ConvertToUInt32(sponsor["company_id"]), shared.ConvertToString(sponsor["created"]))),
			CompanyIDName:    shared.GetCompanyNameOrDefault(sponsor["name"]),
			EditionID:        editionID,
			EventID:          eventID,
			CompanyWebsite:   shared.ConvertToStringPtr(companyWebsite),
			CompanyDomain:    shared.ConvertToStringPtr(companyDomain),
			CompanyCountry:   shared.ToUpperNullableString(shared.ConvertToStringPtr(companyCountry)),
			CompanyState:     companyState,
			CompanyStateName: companyStateName,
			CompanyCity:      shared.ConvertToUInt32Ptr(companyCity),
			CompanyCityName:  companyCityName,
			FacebookID:       shared.ConvertToStringPtr(facebookID),
			LinkedinID:       shared.ConvertToStringPtr(linkedinID),
			TwitterID:        shared.ConvertToStringPtr(twitterID),
			Created:          shared.SafeConvertToDateTimeString(sponsor["created"]),
			Version:          1,
			LastUpdatedAt:    now,
			Published:        shared.SafeConvertToInt8(sponsor["published"]),
			SponsorSourceID:  sponsorSourceID,
		})
	}
	return sponsorRecords
}

func buildSponsorsChDataForEventIDs(db *sql.DB, eventIDs []int64) ([]map[string]interface{}, error) {
	if len(eventIDs) == 0 {
		return nil, nil
	}
	placeholders := make([]string, len(eventIDs))
	args := make([]interface{}, len(eventIDs))
	for i, id := range eventIDs {
		placeholders[i] = "?"
		args[i] = id
	}
	query := fmt.Sprintf(`
		SELECT id, company_id, name, event_id, event_edition, created, published
		FROM event_sponsors
		WHERE event_id IN (%s) AND published > 0
		ORDER BY event_id, event_edition, id`, strings.Join(placeholders, ","))
	log.Printf("[Query] %s", query)
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var results []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}
		row := make(map[string]interface{})
		for i, col := range columns {
			if values[i] != nil {
				row[col] = values[i]
			} else {
				row[col] = nil
			}
		}
		results = append(results, row)
	}
	return results, rows.Err()
}

func BuildEventSponsorChRecordsForEventIDs(db *sql.DB, eventIDs []int64, config shared.Config) ([]SponsorRecord, error) {
	batchData, err := buildSponsorsChDataForEventIDs(db, eventIDs)
	if err != nil {
		return nil, err
	}
	if len(batchData) == 0 {
		return nil, nil
	}

	var sponsorCompanyIDs []int64
	seenCompanyIDs := make(map[int64]bool)
	for _, sponsor := range batchData {
		if companyID, ok := sponsor["company_id"].(int64); ok && companyID > 0 {
			if !seenCompanyIDs[companyID] {
				sponsorCompanyIDs = append(sponsorCompanyIDs, companyID)
				seenCompanyIDs[companyID] = true
			}
		}
	}
	companyData := fetchSponsorsCompanyData(db, sponsorCompanyIDs)

	var sponsorCityIDs []int64
	seenCityIDs := make(map[int64]bool)
	for _, company := range companyData {
		if cityID, ok := company["city"].(int64); ok && cityID > 0 {
			if !seenCityIDs[cityID] {
				sponsorCityIDs = append(sponsorCityIDs, cityID)
				seenCityIDs[cityID] = true
			}
		}
	}
	var cityLookup map[int64]map[string]interface{}
	if len(sponsorCityIDs) > 0 {
		cityData := shared.FetchCityDataParallel(db, sponsorCityIDs, config.NumWorkers)
		cityLookup = make(map[int64]map[string]interface{})
		for _, city := range cityData {
			if cityID, ok := city["id"].(int64); ok {
				cityLookup[cityID] = city
			}
		}
	}

	now := time.Now().Format("2006-01-02 15:04:05")
	return BuildSponsorRecordsFromBatchData(batchData, companyData, cityLookup, now), nil
}

func buildSponsorsMigrationData(db *sql.DB, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT 
			id, company_id, name, event_id, event_edition, created, published
		FROM event_sponsors 
		WHERE id >= %d AND id <= %d AND published > 0
		ORDER BY id 
		LIMIT %d`, startID, endID, batchSize)

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			if val == nil {
				row[col] = nil
			} else {
				row[col] = val
			}
		}
		results = append(results, row)
	}

	return results, nil
}

func fetchSponsorsCompanyData(db *sql.DB, companyIDs []int64) map[int64]map[string]interface{} {
	if len(companyIDs) == 0 {
		return nil
	}

	batchSize := 1000
	var allCompanyData map[int64]map[string]interface{}

	for i := 0; i < len(companyIDs); i += batchSize {
		end := i + batchSize
		if end > len(companyIDs) {
			end = len(companyIDs)
		}

		batch := companyIDs[i:end]

		placeholders := make([]string, len(batch))
		args := make([]interface{}, len(batch))
		for j, id := range batch {
			placeholders[j] = "?"
			args[j] = id
		}

		query := fmt.Sprintf(`
			SELECT 
				id, website, country, city, facebook_id, linkedin_id, twitter_id
			FROM company 
			WHERE id IN (%s)`, strings.Join(placeholders, ","))

		rows, err := db.Query(query, args...)
		if err != nil {
			log.Printf("Error fetching sponsors company data batch %d-%d: %v", i, end-1, err)
			continue
		}

		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			continue
		}

		for rows.Next() {
			values := make([]interface{}, len(columns))
			valuePtrs := make([]interface{}, len(columns))
			for j := range values {
				valuePtrs[j] = &values[j]
			}

			if err := rows.Scan(valuePtrs...); err != nil {
				continue
			}

			row := make(map[string]interface{})
			for j, col := range columns {
				val := values[j]
				if val == nil {
					row[col] = nil
				} else {
					row[col] = val
				}
			}

			if companyID, ok := row["id"].(int64); ok {
				if allCompanyData == nil {
					allCompanyData = make(map[int64]map[string]interface{})
				}
				allCompanyData[companyID] = row
			}
		}
		rows.Close()
	}

	return allCompanyData
}

func insertSponsorsDataIntoClickHouse(clickhouseConn driver.Conn, sponsorRecords []SponsorRecord, numWorkers int) error {
	return InsertEventSponsorChDataIntoTable(clickhouseConn, sponsorRecords, "event_sponsors_temp", numWorkers)
}

func InsertEventSponsorChDataIntoTable(clickhouseConn driver.Conn, records []SponsorRecord, tableName string, numWorkers int) error {
	if len(records) == 0 {
		return nil
	}
	if numWorkers <= 1 {
		return insertSponsorChBatchIntoTable(clickhouseConn, records, tableName)
	}
	batchSize := (len(records) + numWorkers - 1) / numWorkers
	results := make(chan error, numWorkers)
	semaphore := make(chan struct{}, numWorkers)
	for i := 0; i < numWorkers; i++ {
		start := i * batchSize
		end := start + batchSize
		if end > len(records) {
			end = len(records)
		}
		if start >= len(records) {
			break
		}
		semaphore <- struct{}{}
		go func(start, end int) {
			defer func() { <-semaphore }()
			batch := records[start:end]
			results <- insertSponsorChBatchIntoTable(clickhouseConn, batch, tableName)
		}(start, end)
	}
	for i := 0; i < numWorkers && i*batchSize < len(records); i++ {
		if err := <-results; err != nil {
			return err
		}
	}
	return nil
}

func insertSponsorChBatchIntoTable(clickhouseConn driver.Conn, sponsorRecords []SponsorRecord, tableName string) error {
	if len(sponsorRecords) == 0 {
		return nil
	}
	connectionCheckErr := shared.RetryWithBackoff(
		func() error {
			return shared.CheckClickHouseConnectionAlive(clickhouseConn)
		},
		3,
	)
	if connectionCheckErr != nil {
		return fmt.Errorf("ClickHouse connection is not alive after retries: %w", connectionCheckErr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	query := fmt.Sprintf(`
		INSERT INTO %s (
			company_id, company_uuid, company_id_name, edition_id, event_id, company_website,
			company_domain, company_country, company_state, company_state_name, company_city, company_city_name, facebook_id,
			linkedin_id, twitter_id, created, version, last_updated_at, published, sponsorSourceId
		)`, tableName)
	batch, err := clickhouseConn.PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare batch for %s: %w", tableName, err)
	}
	for _, record := range sponsorRecords {
		if err := batch.Append(
			record.CompanyID,
			record.CompanyUUID,
			record.CompanyIDName,
			record.EditionID,
			record.EventID,
			record.CompanyWebsite,
			record.CompanyDomain,
			record.CompanyCountry,
			record.CompanyState,
			record.CompanyStateName,
			record.CompanyCity,
			record.CompanyCityName,
			record.FacebookID,
			record.LinkedinID,
			record.TwitterID,
			record.Created,
			record.Version,
			record.LastUpdatedAt,
			record.Published,
			record.SponsorSourceID,
		); err != nil {
			return fmt.Errorf("failed to append record to batch: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send ClickHouse batch: %w", err)
	}
	log.Printf("OK: Successfully inserted %d sponsor records into %s", len(sponsorRecords), tableName)
	return nil
}
