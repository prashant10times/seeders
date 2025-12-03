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

// EventRankingChRecord represents the structure for event_ranking_ch table
type EventRankingChRecord struct {
	ID            uint32  `ch:"id"`
	EventID       uint32  `ch:"event_id"`
	Country       string  `ch:"country"` // FixedString(2)
	Category      *uint32 `ch:"category"`
	CategoryName  string  `ch:"category_name"`
	EventRank     uint32  `ch:"event_rank"`
	Created       string  `ch:"created"`
	Version       uint32  `ch:"version"`
	LastUpdatedAt string  `ch:"last_updated_at"`
}

// FetchCategoryNameData fetches category names from category table
func FetchCategoryNameData(db *sql.DB, categoryIDs []uint32) (map[uint32]string, error) {
	if len(categoryIDs) == 0 {
		return make(map[uint32]string), nil
	}

	placeholders := make([]string, len(categoryIDs))
	args := make([]interface{}, len(categoryIDs))
	for i, id := range categoryIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(`
		SELECT id, name
		FROM category
		WHERE id IN (%s)
	`, strings.Join(placeholders, ","))

	rows, err := db.Query(query, args...)
	if err != nil {
		log.Printf("Error fetching category data: %v", err)
		return nil, err
	}
	defer rows.Close()

	categoryMap := make(map[uint32]string)
	for rows.Next() {
		var id uint32
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			log.Printf("Error scanning category row: %v", err)
			continue
		}
		categoryMap[id] = name
	}

	log.Printf("Fetched %d category names", len(categoryMap))
	return categoryMap, nil
}

// ConvertToEventRankingChRecords converts MySQL data to ClickHouse records
func ConvertToEventRankingChRecords(mysqlData []map[string]interface{}, db *sql.DB) []EventRankingChRecord {
	var records []EventRankingChRecord
	now := time.Now().Format("2006-01-02 15:04:05")

	categoryIDSet := make(map[uint32]bool)
	for _, row := range mysqlData {
		if category, ok := row["category"]; ok && category != nil {
			if categoryID, ok := category.(int64); ok {
				categoryIDSet[uint32(categoryID)] = true
			}
		}
	}

	uniqueCategoryIDs := make([]uint32, 0, len(categoryIDSet))
	for id := range categoryIDSet {
		uniqueCategoryIDs = append(uniqueCategoryIDs, id)
	}

	var categoryNames map[uint32]string
	if len(uniqueCategoryIDs) > 0 {
		var err error
		categoryNames, err = FetchCategoryNameData(db, uniqueCategoryIDs)
		if err != nil {
			log.Printf("Warning: Could not fetch category names: %v", err)
			categoryNames = make(map[uint32]string)
		}
	} else {
		categoryNames = make(map[uint32]string)
	}

	for _, row := range mysqlData {
		record := EventRankingChRecord{
			Version:       1,
			LastUpdatedAt: now,
		}

		if id, ok := row["id"]; ok && id != nil {
			if idVal, ok := id.(int64); ok {
				record.ID = uint32(idVal)
			}
		}

		if eventID, ok := row["event_id"]; ok && eventID != nil {
			if eventIDVal, ok := eventID.(int64); ok {
				record.EventID = uint32(eventIDVal)
			}
		}

		// Country - following the same pattern as other tables
		countryStr := strings.ToUpper(shared.SafeConvertToString(row["country"]))
		if len(countryStr) > 2 {
			countryStr = countryStr[:2]
		}
		record.Country = countryStr

		if category, ok := row["category"]; ok && category != nil {
			if categoryVal, ok := category.(int64); ok {
				categoryID := uint32(categoryVal)
				record.Category = &categoryID
				if categoryName, exists := categoryNames[categoryID]; exists {
					record.CategoryName = categoryName
				} else {
					record.CategoryName = ""
				}
			}
		}

		if eventRank, ok := row["event_rank"]; ok && eventRank != nil {
			if eventRankVal, ok := eventRank.(int64); ok {
				record.EventRank = uint32(eventRankVal)
			}
		}

		// Created - following the same pattern as other tables
		record.Created = shared.SafeConvertToDateTimeString(row["created"])

		records = append(records, record)
	}

	return records
}

// InsertEventRankingChDataIntoClickHouse inserts event ranking data into ClickHouse with parallel workers
func InsertEventRankingChDataIntoClickHouse(clickhouseConn driver.Conn, eventRankingRecords []EventRankingChRecord, numWorkers int) error {
	if len(eventRankingRecords) == 0 {
		return nil
	}

	if numWorkers <= 1 {
		return InsertEventRankingChDataSingleWorker(clickhouseConn, eventRankingRecords)
	}

	batchSize := len(eventRankingRecords) / numWorkers
	if batchSize == 0 {
		batchSize = 1
	}

	results := make(chan error, numWorkers)
	semaphore := make(chan struct{}, numWorkers)

	for i := 0; i < numWorkers; i++ {
		start := i * batchSize
		end := start + batchSize
		if i == numWorkers-1 {
			end = len(eventRankingRecords)
		}

		if start >= len(eventRankingRecords) {
			break
		}

		semaphore <- struct{}{}
		go func(start, end int) {
			defer func() { <-semaphore }()
			batch := eventRankingRecords[start:end]
			err := InsertEventRankingChDataSingleWorker(clickhouseConn, batch)
			results <- err
		}(start, end)
	}

	for i := 0; i < numWorkers; i++ {
		if err := <-results; err != nil {
			return err
		}
	}

	return nil
}

// InsertEventRankingChDataSingleWorker inserts event ranking data into ClickHouse
func InsertEventRankingChDataSingleWorker(clickhouseConn driver.Conn, eventRankingRecords []EventRankingChRecord) error {
	if len(eventRankingRecords) == 0 {
		return nil
	}

	log.Printf("Checking ClickHouse connection health before inserting %d event_ranking_ch records", len(eventRankingRecords))
	connectionCheckErr := shared.RetryWithBackoff(
		func() error {
			return shared.CheckClickHouseConnectionAlive(clickhouseConn)
		},
		3,
		"ClickHouse connection health check for event_ranking_ch",
	)
	if connectionCheckErr != nil {
		return fmt.Errorf("ClickHouse connection is not alive after retries: %w", connectionCheckErr)
	}
	log.Printf("ClickHouse connection is alive, proceeding with event_ranking_ch batch insert")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	batch, err := clickhouseConn.PrepareBatch(ctx, `
		INSERT INTO testing_db.event_ranking_temp (
			id, event_id, country, category, category_name, event_rank, created, version, last_updated_at
		)
	`)
	if err != nil {
		log.Printf("ERROR: Failed to prepare ClickHouse batch for event_ranking_ch: %v", err)
		return fmt.Errorf("failed to prepare ClickHouse batch: %v", err)
	}

	for _, record := range eventRankingRecords {
		err := batch.Append(
			record.ID,            // id: UInt32
			record.EventID,       // event_id: UInt32
			record.Country,       // country: LowCardinality(String)
			record.Category,      // category: Nullable(UInt32)
			record.CategoryName,  // category_name: LowCardinality(String)
			record.EventRank,     // event_rank: UInt32
			record.Created,       // created: DateTime
			record.Version,       // version: UInt32 DEFAULT 1
			record.LastUpdatedAt, // last_updated_at: DateTime
		)
		if err != nil {
			log.Printf("ERROR: Failed to append record to batch: %v", err)
			log.Printf("Record data: ID=%d, EventID=%d, Country=%s, Category=%v, CategoryName=%s, EventRank=%d, Created=%v, Version=%d",
				record.ID, record.EventID, record.Country, record.Category, record.CategoryName, record.EventRank, record.Created, record.Version)
			return fmt.Errorf("failed to append record to batch: %v", err)
		}
	}

	if err := batch.Send(); err != nil {
		log.Printf("ERROR: Failed to send ClickHouse batch: %v", err)
		return fmt.Errorf("failed to send ClickHouse batch: %v", err)
	}

	log.Printf("OK: Successfully inserted %d event_ranking_ch records", len(eventRankingRecords))
	return nil
}

// ProcessEventRankingOnly processes only event ranking data
func ProcessEventRankingOnly(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) {
	log.Println("=== Starting event_ranking_ch ONLY Processing ===")

	totalRecords, minID, maxID, err := shared.GetTotalRecordsAndIDRange(mysqlDB, "event_ranking")
	if err != nil {
		log.Fatal("Failed to get total records and ID range from event_ranking:", err)
	}

	log.Printf("Total event_rank records: %d, Min ID: %d, Max ID: %d", totalRecords, minID, maxID)

	if config.NumChunks <= 0 {
		config.NumChunks = 5
	}

	chunkSize := (maxID - minID + 1) / config.NumChunks
	if chunkSize == 0 {
		chunkSize = 1
	}

	log.Printf("Processing event_ranking_ch data in %d chunks with chunk size: %d", config.NumChunks, chunkSize)

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
			log.Printf("Waiting %v before launching event_ranking_ch chunk %d...", delay, i+1)
			time.Sleep(delay)
		}

		semaphore <- struct{}{}
		go func(chunkNum, start, end int) {
			defer func() { <-semaphore }()
			ProcessEventRankingChunk(mysqlDB, clickhouseConn, config, start, end, chunkNum, results)
		}(i+1, startID, endID)
	}

	for i := 0; i < config.NumChunks; i++ {
		result := <-results
		log.Printf("EventRanking Result: %s", result)
	}

	log.Println("=== Event Ranking Processing Complete ===")
}

// ProcessEventRankingChunk processes a chunk of event ranking data
func ProcessEventRankingChunk(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config, startID, endID int, chunkNum int, results chan<- string) {
	log.Printf("Processing event_ranking_ch chunk %d: ID range %d-%d", chunkNum, startID, endID)

	totalRecords := endID - startID + 1
	processed := 0

	// Use batching within the chunk - following the same pattern as other tables
	offset := 0
	for {
		batchData, err := BuildEventRankingMigrationData(mysqlDB, startID, endID, config.BatchSize)
		if err != nil {
			results <- fmt.Sprintf("EventRanking chunk %d batch error: %v", chunkNum, err)
			return
		}

		if len(batchData) == 0 {
			break
		}

		processed += len(batchData)
		progress := float64(processed) / float64(totalRecords) * 100
		log.Printf("EventRanking chunk %d: Retrieved %d records in batch (%.1f%% complete)", chunkNum, len(batchData), progress)

		records := ConvertToEventRankingChRecords(batchData, mysqlDB)

		if len(records) > 0 {
			log.Printf("EventRanking chunk %d: Attempting to insert %d records into event_ranking_ch...", chunkNum, len(records))

			attemptCount := 0
			insertErr := shared.RetryWithBackoff(
				func() error {
					if attemptCount > 0 {
						now := time.Now().Format("2006-01-02 15:04:05")
						for i := range batchData {
							batchData[i]["last_updated_at"] = now
						}
						log.Printf("EventRanking chunk %d: Updated last_updated_at for retry attempt %d", chunkNum, attemptCount+1)
					}
					attemptCount++
					return InsertEventRankingChDataIntoClickHouse(clickhouseConn, records, config.ClickHouseWorkers)
				},
				3,
				fmt.Sprintf("event_ranking_ch insertion for chunk %d", chunkNum),
			)

			if insertErr != nil {
				results <- fmt.Sprintf("EventRanking chunk %d insertion failed: %v", chunkNum, insertErr)
				return
			}

			log.Printf("EventRanking chunk %d: Successfully inserted %d records", chunkNum, len(records))
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

	results <- fmt.Sprintf("EventRanking chunk %d completed successfully", chunkNum)
}

func BuildEventRankingMigrationData(mysqlDB *sql.DB, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	month := time.Now().Month()
	year := time.Now().Year()
	query := `
		SELECT id, event_id, country, category, event_rank, created
		FROM event_ranking
		WHERE id >= ? AND id <= ? AND YEAR(created) = ? AND MONTH(created) = ?
		ORDER BY id
		LIMIT ?
	`

	rows, err := mysqlDB.Query(query, startID, endID, year, month, batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to query event_ranking: %v", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %v", err)
	}

	var results []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			log.Printf("Error scanning event_ranking row: %v", err)
			continue
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			if val != nil {
				row[col] = val
			}
		}

		results = append(results, row)
	}

	return results, nil
}
