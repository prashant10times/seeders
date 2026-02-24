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

type VisitorRecord struct {
	UserID          uint32  `ch:"user_id"`
	EventID         uint32  `ch:"event_id"`
	EditionID       uint32  `ch:"edition_id"`
	UserName        string  `ch:"user_name"`
	UserCompanyID   *uint32 `ch:"user_company_id"`
	UserCompany     *string `ch:"user_company"`
	UserDesignation *string `ch:"user_designation"`
	UserCity        *uint32 `ch:"user_city"`
	UserCityName    *string `ch:"user_city_name"`
	UserCountry     *string `ch:"user_country"`
	UserStateID     *uint32 `ch:"user_state_id"`
	UserState       *string `ch:"user_state"`
	Version         uint32  `ch:"version"`
	LastUpdatedAt   string  `ch:"last_updated_at"`
	Published       int8    `ch:"published"`
}

func ProcessVisitorsOnly(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) {
	log.Println("=== Starting VISITORS ONLY Processing ===")

	totalRecords, minID, maxID, err := shared.GetTotalRecordsAndIDRange(mysqlDB, "event_visitor")
	if err != nil {
		log.Fatal("Failed to get total records and ID range from event_visitor:", err)
	}

	log.Printf("Total visitors records: %d, Min ID: %d, Max ID: %d", totalRecords, minID, maxID)

	if config.NumChunks <= 0 {
		config.NumChunks = 5 // Default to 5 chunks if not specified
	}

	chunkSize := (maxID - minID + 1) / config.NumChunks
	if chunkSize == 0 {
		chunkSize = 1
	}

	log.Printf("Processing visitors data in %d chunks with chunk size: %d", config.NumChunks, chunkSize)

	results := make(chan string, config.NumChunks)
	semaphore := make(chan struct{}, config.NumWorkers)

	for i := 0; i < config.NumChunks; i++ {
		startID := minID + (i * chunkSize)
		endID := startID + chunkSize - 1

		// Adjust last chunk to include remaining records
		if i == config.NumChunks-1 {
			endID = maxID
		}

		// Add delay between chunk launches to reduce ClickHouse load
		if i > 0 {
			delay := 3 * time.Second
			log.Printf("Waiting %v before launching visitors chunk %d...", delay, i+1)
			time.Sleep(delay)
		}

		semaphore <- struct{}{}
		go func(chunkNum, start, end int) {
			defer func() { <-semaphore }()
			processVisitorsChunk(mysqlDB, clickhouseConn, config, start, end, chunkNum, results)
		}(i+1, startID, endID)
	}

	for i := 0; i < config.NumChunks; i++ {
		result := <-results
		log.Printf("Visitors Result: %s", result)
	}

	log.Println("Visitors processing completed!")
}

// processes a single chunk of visitors data
func processVisitorsChunk(mysqlDB *sql.DB, _ driver.Conn, config shared.Config, startID, endID int, chunkNum int, results chan<- string) {
	log.Printf("Processing visitors chunk %d: ID range %d-%d", chunkNum, startID, endID)

	// Create a dedicated ClickHouse connection for this goroutine
	chConn, err := SetupNativeClickHouseConnection(config)
	if err != nil {
		log.Printf("Visitors chunk %d: Failed to create ClickHouse connection: %v", chunkNum, err)
		results <- fmt.Sprintf("Visitors chunk %d: Failed to create ClickHouse connection: %v", chunkNum, err)
		return
	}
	defer chConn.Close()

	totalRecords := endID - startID + 1
	processed := 0

	// Use batching within the chunk
	offset := 0
	for {
		log.Printf("Visitors chunk %d: Fetching batch starting from ID %d (range %d-%d)", chunkNum, startID, startID, endID)

		batchData, err := buildVisitorsMigrationData(mysqlDB, startID, endID, config.BatchSize)
		if err != nil {
			log.Printf("ERROR: Visitors chunk %d failed to build migration data: %v", chunkNum, err)
			results <- fmt.Sprintf("Visitors chunk %d batch error: %v", chunkNum, err)
			return
		}

		if len(batchData) == 0 {
			log.Printf("Visitors chunk %d: No more data to process, breaking loop", chunkNum)
			break
		}

		log.Printf("Visitors chunk %d: Successfully retrieved %d records from MySQL", chunkNum, len(batchData))

		processed += len(batchData)
		progress := float64(processed) / float64(totalRecords) * 100
		log.Printf("Visitors chunk %d: Retrieved %d records in batch (%.1f%% complete)", chunkNum, len(batchData), progress)

		// Extract user IDs from this batch to fetch user names
		var userIDs []int64
		seenUserIDs := make(map[int64]bool)
		for _, visitor := range batchData {
			if userID, ok := visitor["user"].(int64); ok && userID > 0 {
				if !seenUserIDs[userID] {
					userIDs = append(userIDs, userID)
					seenUserIDs[userID] = true
				}
			}
		}

		// Fetch user data for visitors
		var userData map[int64]map[string]interface{}
		if len(userIDs) > 0 {
			log.Printf("Visitors chunk %d: Fetching user data for %d users", chunkNum, len(userIDs))
			startTime := time.Now()
			userData = fetchVisitorsUserData(mysqlDB, userIDs)
			userTime := time.Since(startTime)
			log.Printf("Visitors chunk %d: Retrieved user data for %d users in %v", chunkNum, len(userData), userTime)

		}

		// Collect city IDs from visitor data
		var visitorCityIDs []int64
		seenCityIDs := make(map[int64]bool)
		for _, visitor := range batchData {
			if cityID, ok := visitor["visitor_city"].(int64); ok && cityID > 0 {
				if !seenCityIDs[cityID] {
					visitorCityIDs = append(visitorCityIDs, cityID)
					seenCityIDs[cityID] = true
				}
			}
		}

		// Fetch city data for visitor cities
		var cityData []map[string]interface{}
		var cityLookup map[int64]map[string]interface{}
		if len(visitorCityIDs) > 0 {
			log.Printf("Visitors chunk %d: Fetching city data for %d cities", chunkNum, len(visitorCityIDs))
			startTime := time.Now()
			cityData = shared.FetchCityDataParallel(mysqlDB, visitorCityIDs, config.NumWorkers)
			cityTime := time.Since(startTime)
			log.Printf("Visitors chunk %d: Retrieved city data for %d cities in %v", chunkNum, len(cityData), cityTime)

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

		now := time.Now().Format("2006-01-02 15:04:05")
		visitorRecords := buildVisitorRecordsFromBatchData(batchData, userData, cityLookup, now)

		if len(visitorRecords) > 0 {
			log.Printf("Visitors chunk %d: Attempting to insert %d records into event_visitor_ch...", chunkNum, len(visitorRecords))

			attemptCount := 0
			visitorInsertErr := shared.RetryWithBackoff(
				func() error {
					if attemptCount > 0 {
						now := time.Now().Format("2006-01-02 15:04:05")
						for i := range visitorRecords {
							visitorRecords[i].LastUpdatedAt = now
						}
						log.Printf("Visitors chunk %d: Updated last_updated_at for retry attempt %d", chunkNum, attemptCount+1)
					}
					attemptCount++
					return insertVisitorsDataIntoClickHouse(chConn, visitorRecords, config.ClickHouseWorkers)
				},
				3,
			)

			if visitorInsertErr != nil {
				log.Printf("Visitors chunk %d: Insertion failed after retries: %v", chunkNum, visitorInsertErr)
				results <- fmt.Sprintf("Visitors chunk %d: Failed to insert %d records", chunkNum, len(visitorRecords))
				return
			} else {
				log.Printf("Visitors chunk %d: Successfully inserted %d records into event_visitor_ch", chunkNum, len(visitorRecords))
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

	results <- fmt.Sprintf("Visitors chunk %d: Completed successfully", chunkNum)
}

func buildVisitorsMigrationData(db *sql.DB, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	if startID < 0 || endID < 0 || startID > endID {
		return nil, fmt.Errorf("invalid ID range: startID=%d, endID=%d", startID, endID)
	}

	if batchSize <= 0 {
		return nil, fmt.Errorf("invalid batch size: %d", batchSize)
	}

	log.Printf("Building visitors migration data: ID range %d-%d, batch size %d", startID, endID, batchSize)

	query := fmt.Sprintf(`
		SELECT 
			id, user, event, edition, visitor_company, visitor_designation, visitor_city, visitor_country, published
		FROM event_visitor 
		WHERE id >= %d AND id <= %d AND published > 0
		ORDER BY id 
		LIMIT %d`, startID, endID, batchSize)

	log.Printf("Executing query: %s", query)

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get column names: %v", err)
	}

	log.Printf("Query returned columns: %v", columns)

	var results []map[string]interface{}
	rowCount := 0

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row %d: %v", rowCount, err)
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

		if row["id"] == nil {
			log.Printf("WARNING: Row %d has null ID", rowCount)
		}
		if row["user"] == nil {
			log.Printf("WARNING: Row %d has null user", rowCount)
		}
		if row["event"] == nil {
			log.Printf("WARNING: Row %d has null event", rowCount)
		}
		if row["edition"] == nil {
			log.Printf("WARNING: Row %d has null edition", rowCount)
		}

		results = append(results, row)
		rowCount++
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during row iteration: %v", err)
	}

	log.Printf("Successfully retrieved %d visitor records from MySQL", len(results))
	return results, nil
}

func fetchVisitorsUserData(db *sql.DB, userIDs []int64) map[int64]map[string]interface{} {
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
				id, name, user_company, company
			FROM user 
			WHERE id IN (%s)`, strings.Join(placeholders, ","))

		rows, err := db.Query(query, args...)
		if err != nil {
			log.Printf("Error fetching visitors user data batch %d-%d: %v", i, end-1, err)
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

func buildVisitorRecordsFromBatchData(batchData []map[string]interface{}, userData map[int64]map[string]interface{}, cityLookup map[int64]map[string]interface{}, now string) []VisitorRecord {
	var visitorRecords []VisitorRecord
	for _, visitor := range batchData {
		var userName, userCompany, userCompanyID interface{}
		if userID, ok := visitor["user"].(int64); ok && userData != nil && userID > 0 {
			if user, exists := userData[userID]; exists {
				userName = user["name"]
				userCompany = user["user_company"]
				userCompanyID = user["company"]
				if userName == nil || shared.ConvertToString(userName) == "" {
					userName = "-----DEFAULT USER NAME-----"
				}
			} else {
				userName = "-----DEFAULT USER NAME-----"
				userCompany = visitor["visitor_company"]
				userCompanyID = nil
			}
		} else {
			userName = "-----DEFAULT USER NAME-----"
			userCompany = visitor["visitor_company"]
			userCompanyID = nil
		}

		var userCityName *string
		if cityID, ok := visitor["visitor_city"].(int64); ok && cityLookup != nil {
			if city, exists := cityLookup[cityID]; exists && city["name"] != nil {
				nameStr := shared.SafeConvertToString(city["name"])
				userCityName = &nameStr
			}
		}

		var userStateID *uint32
		var userState *string
		if cityID, ok := visitor["visitor_city"].(int64); ok && cityLookup != nil {
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

		userID := shared.ConvertToUInt32(visitor["user"])
		eventID := shared.ConvertToUInt32(visitor["event"])
		editionID := shared.ConvertToUInt32(visitor["edition"])
		convertedUserName := shared.ConvertToString(userName)

		var userCompanyIDPtr *uint32
		if userCompanyID != nil {
			if companyID, ok := userCompanyID.(int64); ok && companyID > 0 {
				companyIDUint32 := uint32(companyID)
				userCompanyIDPtr = &companyIDUint32
			}
		}

		visitorRecords = append(visitorRecords, VisitorRecord{
			UserID:          userID,
			EventID:         eventID,
			EditionID:       editionID,
			UserName:        convertedUserName,
			UserCompanyID:   userCompanyIDPtr,
			UserCompany:     shared.ConvertToStringPtr(userCompany),
			UserDesignation: shared.ConvertToStringPtr(visitor["visitor_designation"]),
			UserCity:        shared.ConvertToUInt32Ptr(visitor["visitor_city"]),
			UserCityName:    userCityName,
			UserCountry:     shared.ToUpperNullableString(shared.ConvertToStringPtr(visitor["visitor_country"])),
			UserStateID:     userStateID,
			UserState:       userState,
			Version:         1,
			LastUpdatedAt:   now,
			Published:       shared.SafeConvertToInt8(visitor["published"]),
		})
	}
	return visitorRecords
}

func insertVisitorsDataIntoClickHouse(clickhouseConn driver.Conn, visitorRecords []VisitorRecord, numWorkers int) error {
	if len(visitorRecords) == 0 {
		log.Printf("WARNING: No visitor records provided for insertion")
		return nil
	}

	if numWorkers <= 0 {
		log.Printf("WARNING: Invalid numWorkers (%d), defaulting to 1", numWorkers)
		numWorkers = 1
	}

	if numWorkers > len(visitorRecords) {
		log.Printf("WARNING: numWorkers (%d) exceeds record count (%d), reducing to %d",
			numWorkers, len(visitorRecords), len(visitorRecords))
		numWorkers = len(visitorRecords)
	}

	log.Printf("Inserting %d visitor records using %d workers", len(visitorRecords), numWorkers)

	if numWorkers <= 1 {
		return insertVisitorsDataSingleWorker(clickhouseConn, visitorRecords)
	}

	batchSize := (len(visitorRecords) + numWorkers - 1) / numWorkers
	log.Printf("Batch size per worker: %d records", batchSize)

	if batchSize == 0 {
		return fmt.Errorf("calculated batch size is 0 for %d records with %d workers", len(visitorRecords), numWorkers)
	}

	results := make(chan error, numWorkers)
	semaphore := make(chan struct{}, numWorkers)
	activeWorkers := 0

	for i := 0; i < numWorkers; i++ {
		start := i * batchSize
		end := start + batchSize
		if end > len(visitorRecords) {
			end = len(visitorRecords)
		}
		if start >= len(visitorRecords) {
			break
		}

		batch := visitorRecords[start:end]
		if len(batch) == 0 {
			log.Printf("WARNING: Empty batch for worker %d (start: %d, end: %d)", i, start, end)
			continue
		}

		activeWorkers++
		semaphore <- struct{}{}
		go func(workerID, start, end int) {
			defer func() { <-semaphore }()
			batch := visitorRecords[start:end]
			log.Printf("Worker %d processing batch: %d records (indices %d-%d)", workerID, len(batch), start, end-1)

			err := insertVisitorsDataSingleWorker(clickhouseConn, batch)
			if err != nil {
				log.Printf("Worker %d failed: %v", workerID, err)
			} else {
				log.Printf("Worker %d completed successfully", workerID)
			}
			results <- err
		}(i+1, start, end)
	}

	var lastError error
	for i := 0; i < activeWorkers; i++ {
		if err := <-results; err != nil {
			lastError = err
			log.Printf("Worker %d failed with error: %v", i+1, err)
		}
	}

	if lastError != nil {
		return fmt.Errorf("one or more workers failed during insertion. Last error: %v", lastError)
	}

	log.Printf("All %d workers completed successfully", activeWorkers)
	return nil
}

func insertVisitorsDataSingleWorker(clickhouseConn driver.Conn, visitorRecords []VisitorRecord) error {
	if len(visitorRecords) == 0 {
		log.Printf("WARNING: No visitor records to insert")
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
	log.Printf("ClickHouse connection is alive, proceeding with event_visitors_ch batch insert")

	log.Printf("Starting ClickHouse insertion for %d visitor records", len(visitorRecords))

	for i, record := range visitorRecords {
		if record.EventID == 0 {
			log.Printf("ERROR: Record details - UserID: %d, EventID: %d, EditionID: %d, UserName: '%s'",
				record.UserID, record.EventID, record.EditionID, record.UserName)
			return fmt.Errorf("invalid visitor record at index %d: EventID is 0", i)
		}
		if record.EditionID == 0 {
			log.Printf("ERROR: Record details - UserID: %d, EventID: %d, EditionID: %d, UserName: '%s'",
				record.UserID, record.EventID, record.EditionID, record.UserName)
			return fmt.Errorf("invalid visitor record at index %d: EditionID is 0", i)
		}
		if record.UserName == "" {
			log.Printf("ERROR: Record details - UserID: %d, EventID: %d, EditionID: %d, UserName: '%s'",
				record.UserID, record.EventID, record.EditionID, record.UserName)
			return fmt.Errorf("invalid visitor record at index %d: UserName is empty", i)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	batch, err := clickhouseConn.PrepareBatch(ctx, `
		INSERT INTO event_visitors_temp (
			user_id, event_id, edition_id, user_name, user_company_id, user_company,
			user_designation, user_city, user_city_name, user_country, user_state_id, user_state, version, last_updated_at, published
		)
	`)
	if err != nil {
		log.Printf("ERROR: Failed to prepare ClickHouse batch for visitors table: %v", err)
		return fmt.Errorf("failed to prepare ClickHouse batch for event_visitors_ch_v2: %v", err)
	}

	log.Printf("ClickHouse batch prepared successfully, appending %d records", len(visitorRecords))

	for i, record := range visitorRecords {
		err := batch.Append(
			record.UserID,          // user_id: UInt32 NOT NULL
			record.EventID,         // event_id: UInt32 NOT NULL
			record.EditionID,       // edition_id: UInt32 NOT NULL
			record.UserName,        // user_name: String NOT NULL
			record.UserCompanyID,   // user_company_id: Nullable(UInt32)
			record.UserCompany,     // user_company: Nullable(String)
			record.UserDesignation, // user_designation: Nullable(String)
			record.UserCity,        // user_city: Nullable(UInt32)
			record.UserCityName,    // user_city_name: LowCardinality(Nullable(String))
			record.UserCountry,     // user_country: LowCardinality(Nullable(FixedString(2)))
			record.UserStateID,     // user_state_id: UInt32
			record.UserState,       // user_state: LowCardinality(Nullable(String))
			record.Version,         // version: UInt32 NOT NULL DEFAULT 1
			record.LastUpdatedAt,   // last_updated_at: DateTime
			record.Published,       // published: Int8
		)
		if err != nil {
			return fmt.Errorf("failed to append visitor record %d to batch (UserID: %d, EventID: %d, EditionID: %d): %v",
				i, record.UserID, record.EventID, record.EditionID, err)
		}
	}

	log.Printf("All %d records appended to batch, sending to ClickHouse...", len(visitorRecords))

	if err := batch.Send(); err != nil {
		log.Printf("ERROR: Failed to send ClickHouse batch for visitors")
		log.Printf("ERROR: Table: event_visitors_ch_v2")
		log.Printf("ERROR: Records count: %d", len(visitorRecords))
		log.Printf("ERROR: Send error: %v", err)
		log.Printf("ERROR: Error type: %T", err)
		return fmt.Errorf("failed to send ClickHouse batch to event_visitors_ch_v2: %v", err)
	}

	log.Printf("OK: Successfully inserted %d visitor records", len(visitorRecords))
	return nil
}

// buildVisitorChDataForModifiedRows fetches rows where modified >= yesterday OR created >= yesterday (includes published=0 for soft deletes).
// Used by incremental sync to scope only modified or newly created records.
func buildVisitorChDataForModifiedRows(db *sql.DB) ([]map[string]interface{}, error) {
	query := `
		SELECT id, user, event, edition, visitor_company, visitor_designation, visitor_city, visitor_country, published
		FROM event_visitor
		WHERE modified >= CURDATE() - INTERVAL 1 DAY
		   OR created >= CURDATE() - INTERVAL 1 DAY
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

// buildVisitorChDataForEventIDs fetches event_visitor rows for the given event IDs.
// Used by incremental sync.
func buildVisitorChDataForEventIDs(db *sql.DB, eventIDs []int64) ([]map[string]interface{}, error) {
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
		SELECT id, user, event, edition, visitor_company, visitor_designation, visitor_city, visitor_country, published
		FROM event_visitor
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

// BuildEventVisitorChRecordsForEventIDs fetches all visitors for the given event IDs
// and builds VisitorRecords with user and city data. Used for incremental sync.
func BuildEventVisitorChRecordsForEventIDs(db *sql.DB, eventIDs []int64, config shared.Config) ([]VisitorRecord, error) {
	batchData, err := buildVisitorChDataForEventIDs(db, eventIDs)
	if err != nil {
		return nil, err
	}
	return BuildEventVisitorChRecordsFromBatchData(db, batchData, config)
}

// BuildEventVisitorChRecordsFromBatchData builds VisitorRecords from raw batch data.
// Used by incremental sync for modified rows only.
func BuildEventVisitorChRecordsFromBatchData(db *sql.DB, batchData []map[string]interface{}, config shared.Config) ([]VisitorRecord, error) {
	if len(batchData) == 0 {
		return nil, nil
	}

	var userIDs []int64
	seenUserIDs := make(map[int64]bool)
	for _, visitor := range batchData {
		if userID, ok := visitor["user"].(int64); ok && userID > 0 {
			if !seenUserIDs[userID] {
				userIDs = append(userIDs, userID)
				seenUserIDs[userID] = true
			}
		}
	}
	userData := fetchVisitorsUserData(db, userIDs)

	var visitorCityIDs []int64
	seenCityIDs := make(map[int64]bool)
	for _, visitor := range batchData {
		if cityID, ok := visitor["visitor_city"].(int64); ok && cityID > 0 {
			if !seenCityIDs[cityID] {
				visitorCityIDs = append(visitorCityIDs, cityID)
				seenCityIDs[cityID] = true
			}
		}
	}
	var cityLookup map[int64]map[string]interface{}
	if len(visitorCityIDs) > 0 {
		cityData := shared.FetchCityDataParallel(db, visitorCityIDs, config.NumWorkers)
		cityLookup = make(map[int64]map[string]interface{})
		for _, city := range cityData {
			if cityID, ok := city["id"].(int64); ok {
				cityLookup[cityID] = city
			}
		}
	}

	now := time.Now().Format("2006-01-02 15:04:05")
	return buildVisitorRecordsFromBatchData(batchData, userData, cityLookup, now), nil
}

// InsertEventVisitorChDataIntoTable inserts visitor records directly into the given ClickHouse table.
// Used by incremental sync (inserts into event_visitors_ch, not event_visitors_temp).
func InsertEventVisitorChDataIntoTable(clickhouseConn driver.Conn, records []VisitorRecord, tableName string, numWorkers int) error {
	if len(records) == 0 {
		return nil
	}
	if numWorkers <= 1 {
		return insertVisitorChBatchIntoTable(clickhouseConn, records, tableName)
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
			results <- insertVisitorChBatchIntoTable(clickhouseConn, batch, tableName)
		}(start, end)
	}
	for i := 0; i < numWorkers && i*batchSize < len(records); i++ {
		if err := <-results; err != nil {
			return err
		}
	}
	return nil
}

func insertVisitorChBatchIntoTable(clickhouseConn driver.Conn, records []VisitorRecord, tableName string) error {
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
			user_id, event_id, edition_id, user_name, user_company_id, user_company,
			user_designation, user_city, user_city_name, user_country, user_state_id, user_state,
			version, last_updated_at, published
		)`, tableName)
	batch, err := clickhouseConn.PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare batch for %s: %w", tableName, err)
	}
	for i, record := range records {
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
			record.UserCompanyID,
			userCompanyVal,
			record.UserDesignation,
			record.UserCity,
			record.UserCityName,
			record.UserCountry,
			record.UserStateID,
			record.UserState,
			record.Version,
			record.LastUpdatedAt,
			record.Published,
		); err != nil {
			return fmt.Errorf("failed to append visitor record %d: %w", i, err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}
	return nil
}