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

type ExhibitorRecord struct {
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
}

func ProcessExhibitorOnly(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) {
	log.Println("=== Starting EXHIBITOR ONLY Processing ===")

	totalRecords, minID, maxID, err := shared.GetTotalRecordsAndIDRange(mysqlDB, "event_exhibitor")
	if err != nil {
		log.Fatal("Failed to get total records and ID range from event_exhibitor:", err)
	}

	log.Printf("Total exhibitor records: %d, Min ID: %d, Max ID: %d", totalRecords, minID, maxID)

	if config.NumChunks <= 0 {
		config.NumChunks = 5 // Default to 5 chunks if not specified
	}

	chunkSize := (maxID - minID + 1) / config.NumChunks
	if chunkSize == 0 {
		chunkSize = 1
	}

	log.Printf("Processing exhibitor data in %d chunks with chunk size: %d", config.NumChunks, chunkSize)

	results := make(chan string, config.NumChunks)
	semaphore := make(chan struct{}, config.NumWorkers)

	// Process chunks in parallel
	for i := 0; i < config.NumChunks; i++ {
		startID := minID + (i * chunkSize)
		endID := startID + chunkSize - 1

		// last chunk to include remaining records
		if i == config.NumChunks-1 {
			endID = maxID
		}
		// Add delay between chunk launches to reduce ClickHouse load
		if i > 0 {
			delay := 3 * time.Second
			time.Sleep(delay)
		}

		semaphore <- struct{}{}
		go func(chunkNum, start, end int) {
			defer func() { <-semaphore }()
			processExhibitorChunk(mysqlDB, clickhouseConn, config, start, end, chunkNum, results)
		}(i+1, startID, endID)
	}

	for i := 0; i < config.NumChunks; i++ {
		result := <-results
		log.Printf("Exhibitor Result: %s", result)
	}

	log.Println("Exhibitor processing completed!")
}

func processExhibitorChunk(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config, startID, endID int, chunkNum int, results chan<- string) {
	log.Printf("Processing exhibitor chunk %d: ID range %d-%d", chunkNum, startID, endID)

	totalRecords := endID - startID + 1
	processed := 0

	// Use batching within the chunk
	offset := 0
	for {
		batchData, err := buildExhibitorMigrationData(mysqlDB, startID, endID, config.BatchSize)
		if err != nil {
			results <- fmt.Sprintf("Exhibitor chunk %d batch error: %v", chunkNum, err)
			return
		}

		if len(batchData) == 0 {
			break
		}

		processed += len(batchData)
		progress := float64(processed) / float64(totalRecords) * 100
		log.Printf("Exhibitor chunk %d: Retrieved %d records in batch (%.1f%% complete)", chunkNum, len(batchData), progress)

		eventIDs := extractExhibitorEventIDs(batchData) // Extract event IDs from this batch to fetch social media data
		if len(eventIDs) > 0 {
			log.Printf("Exhibitor chunk %d: Processing %d exhibitor records", chunkNum, len(batchData))

			// Extract company IDs from exhibitor data to fetch social media information
			var exhibitorCompanyIDs []int64
			seenCompanyIDs := make(map[int64]bool)
			for _, exhibitor := range batchData {
				if companyID, ok := exhibitor["company_id"].(int64); ok && companyID > 0 {
					if !seenCompanyIDs[companyID] {
						exhibitorCompanyIDs = append(exhibitorCompanyIDs, companyID)
						seenCompanyIDs[companyID] = true
					}
				}
			}

			// Fetch social media data for exhibitor companies
			var socialData map[int64]map[string]interface{}
			if len(exhibitorCompanyIDs) > 0 {
				log.Printf("Exhibitor chunk %d: Fetching social media data for %d companies", chunkNum, len(exhibitorCompanyIDs))
				startTime := time.Now()
				socialData = fetchExhibitorSocialData(mysqlDB, exhibitorCompanyIDs)
				socialTime := time.Since(startTime)
				log.Printf("Exhibitor chunk %d: Retrieved social media data for %d companies in %v", chunkNum, len(socialData), socialTime)
			}

			// Collect city IDs from exhibitor data
			var exhibitorCityIDs []int64
			seenCityIDs := make(map[int64]bool)
			for _, exhibitor := range batchData {
				if cityID, ok := exhibitor["city"].(int64); ok && cityID > 0 {
					if !seenCityIDs[cityID] {
						exhibitorCityIDs = append(exhibitorCityIDs, cityID)
						seenCityIDs[cityID] = true
					}
				}
			}

			// Fetch city data for exhibitor cities
			var cityData []map[string]interface{}
			var cityLookup map[int64]map[string]interface{}
			if len(exhibitorCityIDs) > 0 {
				log.Printf("Exhibitor chunk %d: Fetching city data for %d cities", chunkNum, len(exhibitorCityIDs))
				startTime := time.Now()
				cityData = shared.FetchCityDataParallel(mysqlDB, exhibitorCityIDs, config.NumWorkers)
				cityTime := time.Since(startTime)
				log.Printf("Exhibitor chunk %d: Retrieved city data for %d cities in %v", chunkNum, len(cityData), cityTime)

				// Create city lookup map
				cityLookup = make(map[int64]map[string]interface{})
				if len(cityData) > 0 {
					for _, city := range cityData {
						if cityID, ok := city["id"].(int64); ok {
							cityLookup[cityID] = city
						}
					}
				}
			}

			var exhibitorRecords []ExhibitorRecord
			now := time.Now().Format("2006-01-02 15:04:05")
			for _, exhibitor := range batchData {
				var companyDomain string
				if website, ok := exhibitor["website"].(string); ok && website != "" {
					companyDomain = shared.ExtractDomainFromWebsite(website)
				} else if website, ok := exhibitor["website"].([]byte); ok && len(website) > 0 {
					websiteStr := string(website)
					companyDomain = shared.ExtractDomainFromWebsite(websiteStr)
				}

				// Get social media data for this company
				var facebookID, linkedinID, twitterID interface{}
				if companyID, ok := exhibitor["company_id"].(int64); ok && socialData != nil {
					if social, exists := socialData[companyID]; exists {
						facebookID = social["facebook_id"]
						linkedinID = social["linkedin_id"]
						twitterID = social["twitter_id"]
					}
				}

				// Get city data for this exhibitor
				var companyCityName *string
				if cityID, ok := exhibitor["city"].(int64); ok && cityLookup != nil {
					if city, exists := cityLookup[cityID]; exists && city["name"] != nil {
						nameStr := shared.SafeConvertToString(city["name"])
						companyCityName = &nameStr
					}
				}

				var companyState *uint32
				var companyStateName *string
				if cityID, ok := exhibitor["city"].(int64); ok && cityLookup != nil {
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

				// Convert data to proper types for protocol
				companyID := shared.ConvertToUInt32Ptr(exhibitor["company_id"])
				editionID := shared.ConvertToUInt32(exhibitor["edition_id"])
				eventID := shared.ConvertToUInt32(exhibitor["event_id"])

				// Create exhibitor record with proper types
				exhibitorRecord := ExhibitorRecord{
					CompanyID:        companyID,
					CompanyUUID:      shared.GenerateCompanyUUID(exhibitor["company_name"], exhibitor["created"]),
					CompanyIDName:    shared.GetCompanyNameOrDefault(exhibitor["company_name"]),
					EditionID:        editionID,
					EventID:          eventID,
					CompanyWebsite:   shared.ConvertToStringPtr(exhibitor["website"]),
					CompanyDomain:    shared.ConvertToStringPtr(companyDomain),
					CompanyCountry:   shared.ToUpperNullableString(shared.ConvertToStringPtr(exhibitor["country"])),
					CompanyState:     companyState,
					CompanyStateName: companyStateName,
					CompanyCity:      shared.ConvertToUInt32Ptr(exhibitor["city"]),
					CompanyCityName:  companyCityName,
					FacebookID:       shared.ConvertToStringPtr(facebookID),
					LinkedinID:       shared.ConvertToStringPtr(linkedinID),
					TwitterID:        shared.ConvertToStringPtr(twitterID),
					Created:          shared.SafeConvertToDateTimeString(exhibitor["created"]),
					Version:          1,
					LastUpdatedAt:    now,
				}

				exhibitorRecords = append(exhibitorRecords, exhibitorRecord)
			}

			// Insert exhibitor data into ClickHouse
			if len(exhibitorRecords) > 0 {
				log.Printf("Exhibitor chunk %d: Attempting to insert %d records into event_exhibitor_ch...", chunkNum, len(exhibitorRecords))
				exhibitorInsertErr := shared.RetryWithBackoff(
					func() error {
						return insertExhibitorDataIntoClickHouse(clickhouseConn, exhibitorRecords, config.ClickHouseWorkers)
					},
					3,
					fmt.Sprintf("exhibitor insertion for chunk %d", chunkNum),
				)

				if exhibitorInsertErr != nil {
					log.Printf("Exhibitor chunk %d: Insertion failed after retries: %v", chunkNum, exhibitorInsertErr)
					results <- fmt.Sprintf("Exhibitor chunk %d: Failed to insert %d records", chunkNum, len(exhibitorRecords))
					return
				} else {
					log.Printf("Exhibitor chunk %d: Successfully inserted %d records into event_exhibitor_ch", chunkNum, len(exhibitorRecords))
				}
			} else {
				log.Printf("Exhibitor chunk %d: No exhibitor records to insert", chunkNum)
			}
		}

		// Get the last ID from this batch for next iteration
		if len(batchData) > 0 {
			lastID := batchData[len(batchData)-1]["id"]
			if lastID != nil {
				// Update startID for next batch within this chunk
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

	results <- fmt.Sprintf("Exhibitor chunk %d: Completed successfully", chunkNum)
}

func buildExhibitorMigrationData(db *sql.DB, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT 
			id, company_id, company_name, event_id, edition_id, country, city, website, created
		FROM event_exhibitor 
		WHERE id >= %d AND id <= %d 
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

func fetchExhibitorSocialData(db *sql.DB, companyIDs []int64) map[int64]map[string]interface{} {
	if len(companyIDs) == 0 {
		return nil
	}

	batchSize := 1000
	var allSocialData map[int64]map[string]interface{}

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
				id, facebook_id, linkedin_id, twitter_id
			FROM company 
			WHERE id IN (%s)`, strings.Join(placeholders, ","))

		rows, err := db.Query(query, args...)
		if err != nil {
			log.Printf("Error fetching exhibitor social data batch %d-%d: %v", i, end-1, err)
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
				if allSocialData == nil {
					allSocialData = make(map[int64]map[string]interface{})
				}
				allSocialData[companyID] = row
			}
		}
		rows.Close()
	}

	return allSocialData
}

func insertExhibitorDataIntoClickHouse(clickhouseConn driver.Conn, exhibitorRecords []ExhibitorRecord, numWorkers int) error {
	if len(exhibitorRecords) == 0 {
		return nil
	}

	if numWorkers <= 1 {
		return insertExhibitorDataSingleWorker(clickhouseConn, exhibitorRecords)
	}

	batchSize := (len(exhibitorRecords) + numWorkers - 1) / numWorkers
	results := make(chan error, numWorkers)
	semaphore := make(chan struct{}, numWorkers)

	for i := 0; i < numWorkers; i++ {
		start := i * batchSize
		end := start + batchSize
		if end > len(exhibitorRecords) {
			end = len(exhibitorRecords)
		}
		if start >= len(exhibitorRecords) {
			break
		}

		semaphore <- struct{}{}
		go func(start, end int) {
			defer func() { <-semaphore }()
			batch := exhibitorRecords[start:end]
			err := insertExhibitorDataSingleWorker(clickhouseConn, batch)
			results <- err
		}(start, end)
	}

	for i := 0; i < numWorkers && i*batchSize < len(exhibitorRecords); i++ {
		if err := <-results; err != nil {
			return err
		}
	}

	return nil
}

func insertExhibitorDataSingleWorker(clickhouseConn driver.Conn, exhibitorRecords []ExhibitorRecord) error {
	if len(exhibitorRecords) == 0 {
		return nil
	}

	log.Printf("Checking ClickHouse connection health before inserting %d event_exhibitor_ch records", len(exhibitorRecords))
	connectionCheckErr := shared.RetryWithBackoff(
		func() error {
			return shared.CheckClickHouseConnectionAlive(clickhouseConn)
		},
		3,
		"ClickHouse connection health check for event_exhibitor_ch",
	)
	if connectionCheckErr != nil {
		return fmt.Errorf("ClickHouse connection is not alive after retries: %w", connectionCheckErr)
	}
	log.Printf("ClickHouse connection is alive, proceeding with event_exhibitor_ch batch insert")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	batch, err := clickhouseConn.PrepareBatch(ctx, `
		INSERT INTO event_exhibitor_temp (
			company_id, company_uuid, company_id_name, edition_id, event_id, company_website,
			company_domain, company_country, company_state, company_state_name, company_city, company_city_name, facebook_id,
			linkedin_id, twitter_id, created, version, last_updated_at
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare ClickHouse batch: %v", err)
	}

	for _, record := range exhibitorRecords {
		err := batch.Append(
			record.CompanyID,        // company_id: Nullable(UInt32)
			record.CompanyUUID,      // company_uuid: UUID
			record.CompanyIDName,    // company_id_name: String NOT NULL
			record.EditionID,        // edition_id: UInt32 NOT NULL
			record.EventID,          // event_id: UInt32 NOT NULL
			record.CompanyWebsite,   // company_website: Nullable(String)
			record.CompanyDomain,    // company_domain: Nullable(String)
			record.CompanyCountry,   // company_country: LowCardinality(FixedString(2))
			record.CompanyState,     // company_state: Nullable(UInt32)
			record.CompanyStateName, // company_state_name: LowCardinality(Nullable(String))
			record.CompanyCity,      // company_city: Nullable(UInt32)
			record.CompanyCityName,  // company_city_name: LowCardinality(Nullable(String))
			record.FacebookID,       // facebook_id: Nullable(String)
			record.LinkedinID,       // linkedin_id: Nullable(String)
			record.TwitterID,        // twitter_id: Nullable(String)
			record.Created,          // created: DateTime
			record.Version,          // version: UInt32 NOT NULL DEFAULT 1
			record.LastUpdatedAt,    // last_updated_at: DateTime
		)
		if err != nil {
			return fmt.Errorf("failed to append record to batch: %v", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send ClickHouse batch: %v", err)
	}

	log.Printf("OK: Successfully inserted %d exhibitor records", len(exhibitorRecords))
	return nil
}

func extractExhibitorEventIDs(exhibitorData []map[string]interface{}) []int64 {
	var eventIDs []int64
	seen := make(map[int64]bool)

	for _, exhibitor := range exhibitorData {
		if eventID, ok := exhibitor["event_id"].(int64); ok && eventID > 0 {
			if !seen[eventID] {
				eventIDs = append(eventIDs, eventID)
				seen[eventID] = true
			}
		}
	}

	return eventIDs
}
