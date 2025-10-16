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

type EventTypeEventChRecord struct {
	EventTypeID   uint32 `ch:"eventtype_id"`
	EventTypeUUID string `ch:"eventtype_uuid"`
	EventID       uint32 `ch:"event_id"`
	Published     int8   `ch:"published"`
	Name          string `ch:"name"`
	URL           string `ch:"url"`
	EventAudience uint16 `ch:"event_audience"`
	Created       string `ch:"created"`
	Version       uint32 `ch:"version"`
}

func ProcessEventTypeEventChOnly(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) {
	log.Println("=== Starting event_type_ch ONLY Processing ===")

	totalRecords, minID, maxID, err := shared.GetTotalRecordsAndIDRange(mysqlDB, "event_type_event")
	if err != nil {
		log.Fatal("Failed to get total records and ID range from event_type_event:", err)
	}

	log.Printf("Total event_type_event records: %d, Min ID: %d, Max ID: %d", totalRecords, minID, maxID)

	if config.NumChunks <= 0 {
		config.NumChunks = 5
	}

	chunkSize := (maxID - minID + 1) / config.NumChunks
	if chunkSize == 0 {
		chunkSize = 1
	}

	log.Printf("Processing event_type_ch data in %d chunks with chunk size: %d", config.NumChunks, chunkSize)

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
			log.Printf("Waiting %v before launching event_type_ch chunk %d...", delay, i+1)
			time.Sleep(delay)
		}

		semaphore <- struct{}{}
		go func(chunkNum, start, end int) {
			defer func() { <-semaphore }()
			processEventTypeEventChChunk(mysqlDB, clickhouseConn, config, start, end, chunkNum, results)
		}(i+1, startID, endID)
	}

	for i := 0; i < config.NumChunks; i++ {
		result := <-results
		log.Printf("EventTypeEventCh Result: %s", result)
	}

	log.Println("EventTypeEventCh processing completed!")
}

// processes a single chunk of event_type_ch data
func processEventTypeEventChChunk(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config, startID, endID int, chunkNum int, results chan<- string) {
	log.Printf("Processing event_type_ch chunk %d: ID range %d-%d", chunkNum, startID, endID)

	totalRecords := endID - startID + 1
	processed := 0

	// Use batching within the chunk
	offset := 0
	for {
		batchData, err := buildEventTypeEventChMigrationData(mysqlDB, startID, endID, config.BatchSize)
		if err != nil {
			results <- fmt.Sprintf("EventTypeEventCh chunk %d batch error: %v", chunkNum, err)
			return
		}

		if len(batchData) == 0 {
			break
		}

		processed += len(batchData)
		progress := float64(processed) / float64(totalRecords) * 100
		log.Printf("EventTypeEventCh chunk %d: Retrieved %d records in batch (%.1f%% complete)", chunkNum, len(batchData), progress)

		var eventTypeEventChRecords []EventTypeEventChRecord
		for _, record := range batchData {
			eventTypeEventChRecord := EventTypeEventChRecord{
				EventTypeID:   shared.ConvertToUInt32(record["eventtype_id"]),
				EventTypeUUID: shared.GenerateEventTypeUUID(shared.ConvertToUInt32(record["eventtype_id"]),shared.ConvertToUInt32(record["event_id"]), shared.ConvertToString(record["name"]), record["created"]),
				EventID:       shared.ConvertToUInt32(record["event_id"]),
				Published:     shared.ConvertToInt8(record["published"]),
				Name:          shared.ConvertToString(record["name"]),
				URL:           shared.ConvertToString(record["url"]),
				EventAudience: shared.SafeConvertToUInt16(record["event_audience"]),
				Created:       shared.SafeConvertToDateTimeString(record["created"]),
				Version:       1,
			}

			eventTypeEventChRecords = append(eventTypeEventChRecords, eventTypeEventChRecord)
		}

		// Insert event_type_ch data into ClickHouse
		if len(eventTypeEventChRecords) > 0 {
			log.Printf("EventTypeEventCh chunk %d: Attempting to insert %d records into event_type_ch...", chunkNum, len(eventTypeEventChRecords))

			insertErr := shared.RetryWithBackoff(
				func() error {
					return insertEventTypeEventChDataIntoClickHouse(clickhouseConn, eventTypeEventChRecords, config.ClickHouseWorkers)
				},
				3,
				fmt.Sprintf("event_type_ch insertion for chunk %d", chunkNum),
			)

			if insertErr != nil {
				log.Printf("EventTypeEventCh chunk %d: Insertion failed after retries: %v", chunkNum, insertErr)
				results <- fmt.Sprintf("EventTypeEventCh chunk %d: Failed to insert %d records", chunkNum, len(eventTypeEventChRecords))
				return
			} else {
				log.Printf("EventTypeEventCh chunk %d: Successfully inserted %d records into event_type_ch", chunkNum, len(eventTypeEventChRecords))
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

	results <- fmt.Sprintf("EventTypeEventCh chunk %d: Completed successfully", chunkNum)
}

func insertEventTypeEventChDataIntoClickHouse(clickhouseConn driver.Conn, eventTypeEventChRecords []EventTypeEventChRecord, numWorkers int) error {
	if len(eventTypeEventChRecords) == 0 {
		return nil
	}

	if numWorkers <= 1 {
		return insertEventTypeEventChDataSingleWorker(clickhouseConn, eventTypeEventChRecords)
	}

	batchSize := (len(eventTypeEventChRecords) + numWorkers - 1) / numWorkers
	results := make(chan error, numWorkers)
	semaphore := make(chan struct{}, numWorkers)

	for i := 0; i < numWorkers; i++ {
		start := i * batchSize
		end := start + batchSize
		if end > len(eventTypeEventChRecords) {
			end = len(eventTypeEventChRecords)
		}
		if start >= len(eventTypeEventChRecords) {
			break
		}

		semaphore <- struct{}{}
		go func(start, end int) {
			defer func() { <-semaphore }()
			batch := eventTypeEventChRecords[start:end]
			err := insertEventTypeEventChDataSingleWorker(clickhouseConn, batch)
			results <- err
		}(start, end)
	}

	for i := 0; i < numWorkers && i*batchSize < len(eventTypeEventChRecords); i++ {
		if err := <-results; err != nil {
			return err
		}
	}

	return nil
}

func insertEventTypeEventChDataSingleWorker(clickhouseConn driver.Conn, eventTypeEventChRecords []EventTypeEventChRecord) error {
	if len(eventTypeEventChRecords) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	batch, err := clickhouseConn.PrepareBatch(ctx, `
		INSERT INTO event_type_ch (
			eventtype_id, eventtype_uuid, event_id, published, name, url, event_audience, created, version
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare ClickHouse batch: %v", err)
	}

	for _, record := range eventTypeEventChRecords {
		err := batch.Append(
			record.EventTypeID,   // eventtype_id: UInt32
			record.EventTypeUUID, // eventtype_uuid: UUID
			record.EventID,       // event_id: UInt32
			record.Published,     // published: Int8
			record.Name,          // name: LowCardinality(String)
			record.URL,           // url: String
			record.EventAudience, // event_audience: UInt16
			record.Created,       // created: DateTime
			record.Version,       // version: UInt32 DEFAULT 1
		)
		if err != nil {
			return fmt.Errorf("failed to append record to batch: %v", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send ClickHouse batch: %v", err)
	}

	log.Printf("OK: Successfully inserted %d event_type_ch records", len(eventTypeEventChRecords))
	return nil
}

func buildEventTypeEventChMigrationData(db *sql.DB, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT 
			ee.id,
			ee.eventtype_id,
			ee.event_id,
			ee.published,
			et.name,
			et.url,
			et.event_audience,
			ee.created
		FROM event_type_event ee
		INNER JOIN event_type et ON ee.eventtype_id = et.id
		WHERE ee.id >= %d AND ee.id <= %d 
		ORDER BY ee.id 
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