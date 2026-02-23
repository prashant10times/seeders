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

type SpeakerRecord struct {
	UserID           uint32  `ch:"user_id"`
	EventID          uint32  `ch:"event_id"`
	EditionID        uint32  `ch:"edition_id"`
	UserName         string  `ch:"user_name"`
	SourceUserName   *string `ch:"sourceUserName"`
	SpeakerTitle     *string `ch:"speaker_title"`
	SpeakerProfile   *string `ch:"speaker_profile"`
	UserCompanyID    *uint32 `ch:"user_company_id"`
	UserCompany      *string `ch:"user_company"`
	SourceCompanyName *string `ch:"sourceCompanyName"`
	UserDesignation   *string `ch:"user_designation"`
	UserState       *uint32 `ch:"user_state"`
	UserStateName   *string `ch:"user_state_name"`
	UserCity        *uint32 `ch:"user_city"`
	UserCityName    *string `ch:"user_city_name"`
	UserCountry     *string `ch:"user_country"`
	Version         uint32  `ch:"version"`
	LastUpdatedAt   string  `ch:"last_updated_at"`
	Published       int8    `ch:"published"`
	SpeakerSourceID uint32  `ch:"speakerSourceId"`
}

func formatSingleValueWithSpaces(value interface{}) *string {
	s := shared.SafeConvertToString(value)
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return nil
	}
	result := " " + trimmed + " "
	return &result
}


func formatSingleValueWithSpacesOrEmpty(value interface{}) string {
	if p := formatSingleValueWithSpaces(value); p != nil {
		return *p
	}
	return "  "
}

func BuildSpeakerRecordsFromBatchData(
	batchData []map[string]interface{},
	userData map[int64]map[string]interface{},
	companyLookup map[int64]map[string]interface{},
	cityLookup map[int64]map[string]interface{},
	now string,
) []SpeakerRecord {
	var speakerRecords []SpeakerRecord
	for _, speaker := range batchData {
		userCompanyID := speaker["company_id"]
		formattedUserName := formatSingleValueWithSpacesOrEmpty(speaker["speaker_name"])
		speakerNameTrimmed := strings.TrimSpace(shared.SafeConvertToString(speaker["speaker_name"]))

		var sourceUserName *string
		if userID, ok := speaker["user_id"].(int64); ok && userData != nil {
			if user, exists := userData[userID]; exists && user["name"] != nil {
				userNameTrimmed := strings.TrimSpace(shared.SafeConvertToString(user["name"]))
				if userNameTrimmed != "" && !strings.EqualFold(speakerNameTrimmed, userNameTrimmed) {
					sourceUserName = formatSingleValueWithSpaces(user["name"])
				}
			}
		}

		var formattedUserCompany *string
		var companyNameTrimmed string
		if companyID, ok := speaker["company_id"].(int64); ok && companyLookup != nil {
			if company, exists := companyLookup[companyID]; exists && company["name"] != nil {
				companyNameTrimmed = strings.TrimSpace(shared.SafeConvertToString(company["name"]))
				formattedUserCompany = formatSingleValueWithSpaces(company["name"])
			}
		}
		// user_company is String (non-nullable) in CH; use "  " when empty for partition key
		userCompanyVal := "  "
		if formattedUserCompany != nil {
			userCompanyVal = *formattedUserCompany
		}

		var sourceCompanyName *string
		if userID, ok := speaker["user_id"].(int64); ok && userData != nil {
			if user, exists := userData[userID]; exists && user["user_company"] != nil {
				userCompanyTrimmed := strings.TrimSpace(shared.SafeConvertToString(user["user_company"]))
				if userCompanyTrimmed != "" && !strings.EqualFold(companyNameTrimmed, userCompanyTrimmed) {
					sourceCompanyName = formatSingleValueWithSpaces(user["user_company"])
				}
			}
		}

		var userDesignation, userCity, userCountry interface{}
		if userID, ok := speaker["user_id"].(int64); ok && userData != nil {
			if user, exists := userData[userID]; exists {
				userDesignation = user["designation"]
				userCity = user["city"]
				userCountry = strings.ToUpper(shared.SafeConvertToString(user["country"]))
			}
		}

		var userCityName *string
		if userCity != nil {
			if cityID, ok := userCity.(int64); ok && cityLookup != nil {
				if city, exists := cityLookup[cityID]; exists && city["name"] != nil {
					nameStr := shared.SafeConvertToString(city["name"])
					userCityName = &nameStr
				}
			}
		}

		var userStateID *uint32
		var userState *string
		if userCity != nil {
			if cityID, ok := userCity.(int64); ok && cityLookup != nil {
				if city, exists := cityLookup[cityID]; exists {
					if city["state_id"] != nil {
						if stateID, ok := city["state_id"].(int64); ok && stateID > 0 {
							stateIDUint32 := uint32(stateID)
							userStateID = &stateIDUint32
						}
					}
					if city["state"] != nil {
						stateStr := shared.SafeConvertToString(city["state"])
						if strings.TrimSpace(stateStr) != "" {
							userState = &stateStr
						}
					}
				}
			}
		}

		userID := shared.ConvertToUInt32(speaker["user_id"])
		eventID := shared.ConvertToUInt32(speaker["event"])
		editionID := shared.ConvertToUInt32(speaker["edition"])
		speakerSourceID := shared.ConvertToUInt32(speaker["id"])

		var userCompanyIDPtr *uint32
		if userCompanyID != nil {
			if companyID, ok := userCompanyID.(int64); ok && companyID > 0 {
				companyIDUint32 := uint32(companyID)
				userCompanyIDPtr = &companyIDUint32
			}
		}

		speakerRecords = append(speakerRecords, SpeakerRecord{
			UserID:            userID,
			EventID:           eventID,
			EditionID:         editionID,
			UserName:          formattedUserName,
			SourceUserName:    sourceUserName,
			SpeakerTitle:      formatSingleValueWithSpaces(speaker["title"]),
			SpeakerProfile:    formatSingleValueWithSpaces(speaker["speaker_profile"]),
			UserCompanyID:     userCompanyIDPtr,
			UserCompany:       &userCompanyVal,
			SourceCompanyName: sourceCompanyName,
			UserDesignation:   shared.ConvertToStringPtr(userDesignation),
			UserState:         userStateID,
			UserStateName:     userState,
			UserCity:          shared.ConvertToUInt32Ptr(userCity),
			UserCityName:      userCityName,
			UserCountry:       shared.ToUpperNullableString(shared.ConvertToStringPtr(userCountry)),
			Version:           1,
			LastUpdatedAt:     now,
			Published:         shared.SafeConvertToInt8(speaker["published"]),
			SpeakerSourceID:   speakerSourceID,
		})
	}
	return speakerRecords
}

func ProcessSpeakersOnly(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) {
	log.Println("=== Starting SPEAKERS ONLY Processing ===")

	totalRecords, minID, maxID, err := shared.GetTotalRecordsAndIDRange(mysqlDB, "event_speaker")
	if err != nil {
		log.Fatal("Failed to get total records and ID range from event_speaker:", err)
	}

	log.Printf("Total speakers records: %d, Min ID: %d, Max ID: %d", totalRecords, minID, maxID)

	if config.NumChunks <= 0 {
		config.NumChunks = 5
	}

	chunkSize := (maxID - minID + 1) / config.NumChunks
	if chunkSize == 0 {
		chunkSize = 1
	}

	log.Printf("Processing speakers data in %d chunks with chunk size: %d", config.NumChunks, chunkSize)

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
			log.Printf("Waiting %v before launching speakers chunk %d...", delay, i+1)
			time.Sleep(delay)
		}

		semaphore <- struct{}{}
		go func(chunkNum, start, end int) {
			defer func() { <-semaphore }()
			processSpeakersChunk(mysqlDB, clickhouseConn, config, start, end, chunkNum, results)
		}(i+1, startID, endID)
	}

	for i := 0; i < config.NumChunks; i++ {
		result := <-results
		log.Printf("Speakers Result: %s", result)
	}

	log.Println("Speakers processing completed!")
}



func processSpeakersChunk(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config, startID, endID int, chunkNum int, results chan<- string) {
	log.Printf("Processing speakers chunk %d: ID range %d-%d", chunkNum, startID, endID)

	totalRecords := endID - startID + 1
	processed := 0

	offset := 0
	for {
		batchData, err := buildSpeakersMigrationData(mysqlDB, startID, endID, config.BatchSize)
		if err != nil {
			results <- fmt.Sprintf("Speakers chunk %d batch error: %v", chunkNum, err)
			return
		}

		if len(batchData) == 0 {
			break
		}

		processed += len(batchData)
		progress := float64(processed) / float64(totalRecords) * 100
		log.Printf("Speakers chunk %d: Retrieved %d records in batch (%.1f%% complete)", chunkNum, len(batchData), progress)

		var userIDs []int64
		seenUserIDs := make(map[int64]bool)
		for _, speaker := range batchData {
			if userID, ok := speaker["user_id"].(int64); ok && userID > 0 {
				if !seenUserIDs[userID] {
					userIDs = append(userIDs, userID)
					seenUserIDs[userID] = true
				}
			}
		}

		var userData map[int64]map[string]interface{}
		if len(userIDs) > 0 {
			log.Printf("Speakers chunk %d: Fetching user data for %d users", chunkNum, len(userIDs))
			startTime := time.Now()
			userData = fetchSpeakersUserData(mysqlDB, userIDs)
			userTime := time.Since(startTime)
			log.Printf("Speakers chunk %d: Retrieved user data for %d users in %v", chunkNum, len(userData), userTime)
		}

		var speakerCityIDs []int64
		seenCityIDs := make(map[int64]bool)
		for _, user := range userData {
			if cityID, ok := user["city"].(int64); ok && cityID > 0 {
				if !seenCityIDs[cityID] {
					speakerCityIDs = append(speakerCityIDs, cityID)
					seenCityIDs[cityID] = true
				}
			}
		}

		var cityData []map[string]interface{}
		var cityLookup map[int64]map[string]interface{}
		if len(speakerCityIDs) > 0 {
			log.Printf("Speakers chunk %d: Fetching city data for %d cities", chunkNum, len(speakerCityIDs))
			startTime := time.Now()
			cityData = shared.FetchCityDataParallel(mysqlDB, speakerCityIDs, config.NumWorkers)
			cityTime := time.Since(startTime)
			log.Printf("Speakers chunk %d: Retrieved city data for %d cities in %v", chunkNum, len(cityData), cityTime)

			cityLookup = make(map[int64]map[string]interface{})
			if len(cityData) > 0 {
				for _, city := range cityData {
					if cityID, ok := city["id"].(int64); ok {
						cityLookup[cityID] = city
					}
				}
			}
		}

		// Collect company IDs from speaker data for company name lookup
		var speakerCompanyIDs []int64
		seenCompanyIDs := make(map[int64]bool)
		for _, speaker := range batchData {
			if companyID, ok := speaker["company_id"].(int64); ok && companyID > 0 {
				if !seenCompanyIDs[companyID] {
					speakerCompanyIDs = append(speakerCompanyIDs, companyID)
					seenCompanyIDs[companyID] = true
				}
			}
		}

		var companyLookup map[int64]map[string]interface{}
		if len(speakerCompanyIDs) > 0 {
			log.Printf("Speakers chunk %d: Fetching company data for %d companies", chunkNum, len(speakerCompanyIDs))
			startTime := time.Now()
			companyLookup = fetchSpeakersCompanyData(mysqlDB, speakerCompanyIDs)
			companyTime := time.Since(startTime)
			log.Printf("Speakers chunk %d: Retrieved company data for %d companies in %v", chunkNum, len(companyLookup), companyTime)
		}

		now := time.Now().Format("2006-01-02 15:04:05")
		speakerRecords := BuildSpeakerRecordsFromBatchData(batchData, userData, companyLookup, cityLookup, now)

		// Insert speakers data into ClickHouse
		if len(speakerRecords) > 0 {
			log.Printf("Speakers chunk %d: Attempting to insert %d records into event_speaker_ch...", chunkNum, len(speakerRecords))

			attemptCount := 0
			speakerInsertErr := shared.RetryWithBackoff(
				func() error {
					if attemptCount > 0 {
						now := time.Now().Format("2006-01-02 15:04:05")
						for i := range speakerRecords {
							speakerRecords[i].LastUpdatedAt = now
						}
						log.Printf("Speakers chunk %d: Updated last_updated_at for retry attempt %d", chunkNum, attemptCount+1)
					}
					attemptCount++
					return insertSpeakersDataIntoClickHouse(clickhouseConn, speakerRecords, config.ClickHouseWorkers)
				},
				3,
			)

			if speakerInsertErr != nil {
				log.Printf("Speakers chunk %d: Insertion failed after retries: %v", chunkNum, speakerInsertErr)
				results <- fmt.Sprintf("Speakers chunk %d: Failed to insert %d records", chunkNum, len(speakerRecords))
				return
			} else {
				log.Printf("Speakers chunk %d: Successfully inserted %d records into event_speaker_ch", chunkNum, len(speakerRecords))
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

	results <- fmt.Sprintf("Speakers chunk %d: Completed successfully", chunkNum)
}

// buildSpeakersChDataForModifiedRows fetches rows where modified >= yesterday (includes published=0 for soft deletes).
// Used by incremental sync to scope only modified records.
func buildSpeakersChDataForModifiedRows(db *sql.DB) ([]map[string]interface{}, error) {
	query := `
		SELECT id, user_id, event, edition, speaker_name, title, speaker_profile, company_id, published
		FROM event_speaker
		WHERE modified >= CURDATE() - INTERVAL 1 DAY
		ORDER BY event, edition, id`
	log.Printf("[Query] %s", strings.TrimSpace(query))
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

// buildSpeakersChDataForEventIDs fetches event_speaker rows for the given event IDs.
// Used by incremental sync.
func buildSpeakersChDataForEventIDs(db *sql.DB, eventIDs []int64) ([]map[string]interface{}, error) {
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
		SELECT id, user_id, event, edition, speaker_name, title, speaker_profile, company_id, published
		FROM event_speaker
		WHERE event IN (%s) AND published > 0
		ORDER BY event, edition, id`, strings.Join(placeholders, ","))
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

// BuildEventSpeakerChRecordsForEventIDs fetches all speakers for the given event IDs
// and builds SpeakerRecords. Used by incremental sync.
func BuildEventSpeakerChRecordsForEventIDs(db *sql.DB, eventIDs []int64, config shared.Config) ([]SpeakerRecord, error) {
	batchData, err := buildSpeakersChDataForEventIDs(db, eventIDs)
	if err != nil {
		return nil, err
	}
	return BuildEventSpeakerChRecordsFromBatchData(db, batchData, config)
}

// BuildEventSpeakerChRecordsFromBatchData builds SpeakerRecords from raw batch data.
// Used by incremental sync for modified rows only.
func BuildEventSpeakerChRecordsFromBatchData(db *sql.DB, batchData []map[string]interface{}, config shared.Config) ([]SpeakerRecord, error) {
	if len(batchData) == 0 {
		return nil, nil
	}

	var userIDs []int64
	seenUserIDs := make(map[int64]bool)
	for _, speaker := range batchData {
		if userID, ok := speaker["user_id"].(int64); ok && userID > 0 {
			if !seenUserIDs[userID] {
				userIDs = append(userIDs, userID)
				seenUserIDs[userID] = true
			}
		}
	}
	userData := fetchSpeakersUserData(db, userIDs)

	var cityIDs []int64
	seenCityIDs := make(map[int64]bool)
	for _, user := range userData {
		if cityID, ok := user["city"].(int64); ok && cityID > 0 {
			if !seenCityIDs[cityID] {
				cityIDs = append(cityIDs, cityID)
				seenCityIDs[cityID] = true
			}
		}
	}
	var cityLookup map[int64]map[string]interface{}
	if len(cityIDs) > 0 {
		cityData := shared.FetchCityDataParallel(db, cityIDs, config.NumWorkers)
		cityLookup = make(map[int64]map[string]interface{})
		for _, city := range cityData {
			if cityID, ok := city["id"].(int64); ok {
				cityLookup[cityID] = city
			}
		}
	}

	var companyIDs []int64
	seenCompanyIDs := make(map[int64]bool)
	for _, speaker := range batchData {
		if companyID, ok := speaker["company_id"].(int64); ok && companyID > 0 {
			if !seenCompanyIDs[companyID] {
				companyIDs = append(companyIDs, companyID)
				seenCompanyIDs[companyID] = true
			}
		}
	}
	companyLookup := fetchSpeakersCompanyData(db, companyIDs)

	now := time.Now().Format("2006-01-02 15:04:05")
	return BuildSpeakerRecordsFromBatchData(batchData, userData, companyLookup, cityLookup, now), nil
}

func buildSpeakersMigrationData(db *sql.DB, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT 
			id, user_id, event, edition, speaker_name, title, speaker_profile, company_id, published
		FROM event_speaker 
		WHERE id >= %d AND id <= %d AND published > 0
		ORDER BY id 
		LIMIT %d`, startID, endID, batchSize)

	fmt.Printf("Executing query: %s\n", query)

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

func fetchSpeakersUserData(db *sql.DB, userIDs []int64) map[int64]map[string]interface{} {
	if len(userIDs) == 0 {
		return nil
	}

	batchSize := 1000
	var allUserData map[int64]map[string]interface{}

	for i := 0; i < len(userIDs); i += batchSize {
		end := i + batchSize
		if end > len(userIDs) {
			end = len(userIDs)
		}

		batch := userIDs[i:end]

		placeholders := make([]string, len(batch))
		args := make([]interface{}, len(batch))
		for j, id := range batch {
			placeholders[j] = "?"
			args[j] = id
		}

		query := fmt.Sprintf(`
			SELECT 
				id, name, user_company, designation, city, country, company
			FROM user 
			WHERE id IN (%s)`, strings.Join(placeholders, ","))

		rows, err := db.Query(query, args...)
		if err != nil {
			log.Printf("Error fetching speakers user data batch %d-%d: %v", i, end-1, err)
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

			if userID, ok := row["id"].(int64); ok {
				if allUserData == nil {
					allUserData = make(map[int64]map[string]interface{})
				}
				allUserData[userID] = row
			}
		}
		rows.Close()
	}

	return allUserData
}

func fetchSpeakersCompanyData(db *sql.DB, companyIDs []int64) map[int64]map[string]interface{} {
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
			SELECT id, name
			FROM company
			WHERE id IN (%s)`, strings.Join(placeholders, ","))

		rows, err := db.Query(query, args...)
		if err != nil {
			log.Printf("Error fetching speakers company data batch %d-%d: %v", i, end-1, err)
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

func insertSpeakersDataIntoClickHouse(clickhouseConn driver.Conn, speakerRecords []SpeakerRecord, numWorkers int) error {
	if len(speakerRecords) == 0 {
		return nil
	}

	if numWorkers <= 1 {
		return insertSpeakersDataSingleWorker(clickhouseConn, speakerRecords)
	}

	batchSize := (len(speakerRecords) + numWorkers - 1) / numWorkers
	results := make(chan error, numWorkers)
	semaphore := make(chan struct{}, numWorkers)

	for i := 0; i < numWorkers; i++ {
		start := i * batchSize
		end := start + batchSize
		if end > len(speakerRecords) {
			end = len(speakerRecords)
		}
		if start >= len(speakerRecords) {
			break
		}

		semaphore <- struct{}{}
		go func(start, end int) {
			defer func() { <-semaphore }()
			batch := speakerRecords[start:end]
			err := insertSpeakersDataSingleWorker(clickhouseConn, batch)
			results <- err
		}(start, end)
	}

	for i := 0; i < numWorkers && i*batchSize < len(speakerRecords); i++ {
		if err := <-results; err != nil {
			return err
		}
	}

	return nil
}

func insertSpeakersDataSingleWorker(clickhouseConn driver.Conn, speakerRecords []SpeakerRecord) error {
	if len(speakerRecords) == 0 {
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
	log.Printf("ClickHouse connection is alive, proceeding with event_speaker_ch batch insert")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	batch, err := clickhouseConn.PrepareBatch(ctx, `
		INSERT INTO event_speaker_temp (
			user_id, event_id, edition_id, user_name, sourceUserName, speaker_title, speaker_profile, user_company_id, user_company, sourceCompanyName,
			user_designation, user_state, user_state_name, user_city, user_city_name, user_country, version, last_updated_at, published, speakerSourceId
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare ClickHouse batch: %v", err)
	}

	for _, record := range speakerRecords {
		err := batch.Append(
			record.UserID,          // user_id: UInt32 NOT NULL
			record.EventID,         // event_id: UInt32 NOT NULL
			record.EditionID,       // edition_id: UInt32 NOT NULL
			record.UserName,        // user_name: String NOT NULL
			record.SourceUserName,   // sourceUserName: Nullable(String)
			record.SpeakerTitle,    // speaker_title: LowCardinality(Nullable(String))
			record.SpeakerProfile,  // speaker_profile: LowCardinality(Nullable(String))
			record.UserCompanyID,   // user_company_id: Nullable(UInt32)
			record.UserCompany,     // user_company: Nullable(String)
			record.SourceCompanyName, // sourceCompanyName: Nullable(String)
			record.UserDesignation, // user_designation: Nullable(String)
			record.UserState,       // user_state: Nullable(UInt32)
			record.UserStateName,   // user_state_name: LowCardinality(Nullable(String))
			record.UserCity,        // user_city: Nullable(UInt32)
			record.UserCityName,    // user_city_name: LowCardinality(Nullable(String))
			record.UserCountry,     // user_country: LowCardinality(Nullable(FixedString(2)))
			record.Version,         // version: UInt32 NOT NULL DEFAULT 1
			record.LastUpdatedAt,   // last_updated_at: DateTime
			record.Published,       // published: Int8
			record.SpeakerSourceID, // speakerSourceId: UInt32
		)
		if err != nil {
			return fmt.Errorf("failed to append record to batch: %v", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send ClickHouse batch: %v", err)
	}

	log.Printf("OK: Successfully inserted %d speaker records", len(speakerRecords))
	return nil
}

// InsertEventSpeakerChDataIntoTable inserts speaker records directly into the given ClickHouse table.
// Used by incremental sync (inserts into event_speaker_ch, not event_speaker_temp).
func InsertEventSpeakerChDataIntoTable(clickhouseConn driver.Conn, records []SpeakerRecord, tableName string, numWorkers int) error {
	if len(records) == 0 {
		return nil
	}
	if numWorkers <= 1 {
		return insertSpeakerChBatchIntoTable(clickhouseConn, records, tableName)
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
			results <- insertSpeakerChBatchIntoTable(clickhouseConn, batch, tableName)
		}(start, end)
	}
	for i := 0; i < numWorkers && i*batchSize < len(records); i++ {
		if err := <-results; err != nil {
			return err
		}
	}
	return nil
}

func insertSpeakerChBatchIntoTable(clickhouseConn driver.Conn, records []SpeakerRecord, tableName string) error {
	if len(records) == 0 {
		return nil
	}
	connectionCheckErr := shared.RetryWithBackoff(
		func() error { return shared.CheckClickHouseConnectionAlive(clickhouseConn) },
		3,
	)
	if connectionCheckErr != nil {
		return fmt.Errorf("ClickHouse connection is not alive after retries: %w", connectionCheckErr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	query := fmt.Sprintf(`
		INSERT INTO %s (
			user_id, event_id, edition_id, user_name, sourceUserName, speaker_title, speaker_profile,
			user_company_id, user_company, sourceCompanyName, user_designation, user_state, user_state_name,
			user_city, user_city_name, user_country, version, last_updated_at, published, speakerSourceId
		)`, tableName)
	batch, err := clickhouseConn.PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare batch for %s: %w", tableName, err)
	}
	for _, record := range records {
		userCompanyVal := record.UserCompany
		if userCompanyVal == nil {
			empty := "  "
			userCompanyVal = &empty
		}
		if err := batch.Append(
			record.UserID,
			record.EventID,
			record.EditionID,
			record.UserName,
			record.SourceUserName,
			record.SpeakerTitle,
			record.SpeakerProfile,
			record.UserCompanyID,
			userCompanyVal,
			record.SourceCompanyName,
			record.UserDesignation,
			record.UserState,
			record.UserStateName,
			record.UserCity,
			record.UserCityName,
			record.UserCountry,
			record.Version,
			record.LastUpdatedAt,
			record.Published,
			record.SpeakerSourceID,
		); err != nil {
			return fmt.Errorf("failed to append record: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}
	return nil
}