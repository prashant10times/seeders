package utils

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"seeders/shared"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type EventCategoryEventChRecord struct {
	Category      uint32 `ch:"category"`        // UInt32
	CategoryUUID  string `ch:"category_uuid"`   // UUID generated
	Event         uint32 `ch:"event"`           // UInt32
	Name          string `ch:"name"`            // LowCardinality(String)
	Slug          string `ch:"slug"`            // String
	Published     int8   `ch:"published"`       // Int8
	ShortName     string `ch:"short_name"`      // String
	IsGroup       uint8  `ch:"is_group"`        // UInt8
	Created       string `ch:"created"`         // DateTime
	Version       uint32 `ch:"version"`         // UInt32
	LastUpdatedAt string `ch:"last_updated_at"` // DateTime
}

func ProcessEventCategoryEventChOnly(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) {
	log.Println("=== Starting event_category_ch ONLY Processing ===")

	totalRecords, minID, maxID, err := shared.GetTotalRecordsAndIDRange(mysqlDB, "event_category")
	if err != nil {
		log.Fatal("Failed to get total records and ID range from event_category:", err)
	}

	log.Printf("Total event_category records: %d, Min ID: %d, Max ID: %d", totalRecords, minID, maxID)

	if config.NumChunks <= 0 {
		config.NumChunks = 5 // Default to 5 chunks if not specified
	}

	chunkSize := (maxID - minID + 1) / config.NumChunks
	if chunkSize == 0 {
		chunkSize = 1
	}

	log.Printf("Processing event_category_ch data in %d chunks with chunk size: %d", config.NumChunks, chunkSize)

	results := make(chan string, config.NumChunks)
	semaphore := make(chan struct{}, config.NumWorkers)

	for i := 0; i < config.NumChunks; i++ {
		startID := minID + (i * chunkSize)
		endID := startID + chunkSize - 1
		if i == config.NumChunks-1 {
			endID = maxID // Last chunk gets remaining records
		}

		// delay between chunk launches to reduce ClickHouse load
		if i > 0 {
			delay := 3 * time.Second
			log.Printf("Waiting %v before launching event_category_ch chunk %d...", delay, i+1)
			time.Sleep(delay)
		}

		semaphore <- struct{}{}
		go func(chunkNum, start, end int) {
			defer func() { <-semaphore }()
			processEventCategoryEventChChunk(mysqlDB, clickhouseConn, config, start, end, chunkNum, results)
		}(i+1, startID, endID)
	}

	for i := 0; i < config.NumChunks; i++ {
		result := <-results
		log.Printf("EventCategoryEventCh Result: %s", result)
	}

	log.Println("EventCategoryEventCh processing completed!")
}

func processEventCategoryEventChChunk(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config, startID, endID int, chunkNum int, results chan<- string) {
	log.Printf("Processing event_category_ch chunk %d: ID range %d-%d", chunkNum, startID, endID)

	totalRecords := endID - startID + 1
	processed := 0

	// Use batching within the chunk
	offset := 0
	for {
		batchData, err := buildEventCategoryEventChMigrationData(mysqlDB, startID, endID, config.BatchSize)
		if err != nil {
			results <- fmt.Sprintf("EventCategoryEventCh chunk %d batch error: %v", chunkNum, err)
			return
		}

		if len(batchData) == 0 {
			break
		}

		processed += len(batchData)
		progress := float64(processed) / float64(totalRecords) * 100
		log.Printf("EventCategoryEventCh chunk %d: Retrieved %d records in batch (%.1f%% complete)", chunkNum, len(batchData), progress)

		var eventCategoryEventChRecords []EventCategoryEventChRecord
		now := time.Now().Format("2006-01-02 15:04:05")
		for _, record := range batchData {
			eventCategoryEventChRecord := EventCategoryEventChRecord{
				Category:      shared.ConvertToUInt32(record["category"]),
				CategoryUUID:  shared.GenerateCategoryUUID(shared.ConvertToUInt32(record["category"]), record["name"], record["created"]),
				Event:         shared.ConvertToUInt32(record["event"]),
				Name:          shared.ConvertToString(record["name"]),
				Slug:          shared.ConvertToString(record["slug"]),
				Published:     shared.ConvertToInt8(record["published"]),
				ShortName:     shared.ConvertToString(record["short_name"]),
				IsGroup:       shared.ConvertToUInt8(record["is_group"]),
				Created:       shared.SafeConvertToDateTimeString(record["created"]),
				Version:       1,
				LastUpdatedAt: now,
			}

			eventCategoryEventChRecords = append(eventCategoryEventChRecords, eventCategoryEventChRecord)
		}

		// Insert event_category_ch data into ClickHouse
		if len(eventCategoryEventChRecords) > 0 {
			log.Printf("EventCategoryEventCh chunk %d: Attempting to insert %d records into event_category_ch...", chunkNum, len(eventCategoryEventChRecords))

			insertErr := shared.RetryWithBackoff(
				func() error {
					return insertEventCategoryEventChDataIntoClickHouse(clickhouseConn, eventCategoryEventChRecords, config.ClickHouseWorkers)
				},
				3,
				fmt.Sprintf("event_category_ch insertion for chunk %d", chunkNum),
			)

			if insertErr != nil {
				log.Printf("EventCategoryEventCh chunk %d: Insertion failed after retries: %v", chunkNum, insertErr)
				results <- fmt.Sprintf("EventCategoryEventCh chunk %d: Failed to insert %d records", chunkNum, len(eventCategoryEventChRecords))
				return
			} else {
				log.Printf("EventCategoryEventCh chunk %d: Successfully inserted %d records into event_category_ch", chunkNum, len(eventCategoryEventChRecords))
			}
		}

		// Update startID for next batch within this chunk (following the same pattern as other working tables)
		if len(batchData) > 0 {
			// Get the last record's ID from the batch and increment it for the next batch
			lastRecord := batchData[len(batchData)-1]
			if lastID, ok := lastRecord["id"].(int64); ok {
				startID = int(lastID) + 1
			} else if lastID, ok := lastRecord["id"].(int32); ok {
				startID = int(lastID) + 1
			} else if lastID, ok := lastRecord["id"].(int); ok {
				startID = lastID + 1
			}
		}

		offset += len(batchData)
		if len(batchData) < config.BatchSize {
			break
		}
	}

	results <- fmt.Sprintf("EventCategoryEventCh chunk %d: Completed successfully", chunkNum)
}

func buildEventCategoryEventChMigrationData(db *sql.DB, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT 
			ec.id,
			ec.category,
			ec.event,
			c.published,
			c.name,
			c.url as slug,
			c.short_name,
			c.is_group,
			c.created
		FROM event_category ec
		INNER JOIN category c ON ec.category = c.id
		WHERE ec.id >= %d AND ec.id <= %d 
		ORDER BY ec.id 
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

func insertEventCategoryEventChDataIntoClickHouse(clickhouseConn driver.Conn, eventCategoryEventChRecords []EventCategoryEventChRecord, numWorkers int) error {
	if len(eventCategoryEventChRecords) == 0 {
		return nil
	}

	if numWorkers <= 1 {
		return insertEventCategoryEventChDataSingleWorker(clickhouseConn, eventCategoryEventChRecords)
	}

	// Split records into batches for parallel processing
	batchSize := len(eventCategoryEventChRecords) / numWorkers
	if batchSize == 0 {
		batchSize = 1
	}

	results := make(chan error, numWorkers)
	semaphore := make(chan struct{}, numWorkers)

	for i := 0; i < numWorkers; i++ {
		start := i * batchSize
		end := start + batchSize
		if i == numWorkers-1 {
			end = len(eventCategoryEventChRecords) // Last worker gets remaining records
		}

		if start >= len(eventCategoryEventChRecords) {
			break
		}

		semaphore <- struct{}{}
		go func(start, end int) {
			defer func() { <-semaphore }()
			batch := eventCategoryEventChRecords[start:end]
			err := insertEventCategoryEventChDataSingleWorker(clickhouseConn, batch)
			results <- err
		}(start, end)
	}

	// Wait for all workers to complete
	for i := 0; i < numWorkers; i++ {
		if err := <-results; err != nil {
			return err
		}
	}

	return nil
}

func insertEventCategoryEventChDataSingleWorker(clickhouseConn driver.Conn, eventCategoryEventChRecords []EventCategoryEventChRecord) error {
	if len(eventCategoryEventChRecords) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	batch, err := clickhouseConn.PrepareBatch(ctx, `
		INSERT INTO event_category_ch (
			category, category_uuid, event, name, slug, published, short_name, is_group, created, version, last_updated_at
		)
	`)
	if err != nil {
		log.Printf("ERROR: Failed to prepare ClickHouse batch for event_category_ch: %v", err)
		return fmt.Errorf("failed to prepare ClickHouse batch: %v", err)
	}

	for _, record := range eventCategoryEventChRecords {
		err := batch.Append(
			record.Category,      // category: UInt32
			record.CategoryUUID,  // category_uuid: UUID
			record.Event,         // event: UInt32
			record.Name,          // name: LowCardinality(String)
			record.Slug,          // slug: String
			record.Published,     // published: Int8
			record.ShortName,     // short_name: String
			record.IsGroup,       // is_group: UInt8
			record.Created,       // created: DateTime
			record.Version,       // version: UInt32 DEFAULT 1
			record.LastUpdatedAt, // last_updated_at: DateTime
		)
		if err != nil {
			log.Printf("ERROR: Failed to append record to batch: %v", err)
			log.Printf("Record data: Category=%d, CategoryUUID=%s, Event=%d, Name=%s, slug=%s, Published=%d, ShortName=%s, IsGroup=%d, Created=%s, Version=%d",
				record.Category, record.CategoryUUID, record.Event, record.Name, record.Slug, record.Published, record.ShortName, record.IsGroup, record.Created, record.Version)
			return fmt.Errorf("failed to append record to batch: %v", err)
		}
	}

	if err := batch.Send(); err != nil {
		log.Printf("ERROR: Failed to send ClickHouse batch: %v", err)
		return fmt.Errorf("failed to send ClickHouse batch: %v", err)
	}

	log.Printf("OK: Successfully inserted %d event_category_ch records", len(eventCategoryEventChRecords))
	return nil
}
