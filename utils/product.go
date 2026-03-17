package utils

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"
	"unicode"

	"seeders/shared"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func createSlug(text string) string {
	if text == "" {
		return ""
	}

	text = strings.ToLower(text)

	var builder strings.Builder
	for _, r := range text {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsSpace(r) {
			builder.WriteRune(r)
		}
	}
	text = builder.String()

	spaceRegex := regexp.MustCompile(`\s+`)
	text = spaceRegex.ReplaceAllString(text, "-")

	text = strings.Trim(text, "-")

	return text
}

type EventProductChRecord struct {
	ID                    uint32 `ch:"id"`                      // UInt32
	ProductUUID           string `ch:"product_uuid"`            // UUID generated
	Name                  string `ch:"name"`                    // LowCardinality(String)
	Slug                  string `ch:"slug"`                    // LowCardinality(String)
	Event                 uint32 `ch:"event"`                   // UInt32
	Edition               uint32 `ch:"edition"`                 // UInt32
	ProductPublished      int8   `ch:"product_published"`       // Int8
	EventProductPublished int8   `ch:"event_product_published"` // Int8
	Created               string `ch:"created"`                 // DateTime
	LastUpdatedAt         string `ch:"last_updated_at"`         // DateTime
}

func ProcessEventProductChOnly(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) {
	log.Println("=== Starting event_product_ch ONLY Processing ===")

	totalRecords, minID, maxID, err := shared.GetTotalRecordsAndIDRange(mysqlDB, "event_products")
	if err != nil {
		log.Fatal("Failed to get total records and ID range from event_products:", err)
	}

	log.Printf("Total event_products records: %d, Min ID: %d, Max ID: %d", totalRecords, minID, maxID)

	if config.NumChunks <= 0 {
		config.NumChunks = 5 // Default to 5 chunks if not specified
	}

	chunkSize := (maxID - minID + 1) / config.NumChunks
	if chunkSize == 0 {
		chunkSize = 1
	}

	log.Printf("Processing event_product_ch data in %d chunks with chunk size: %d", config.NumChunks, chunkSize)

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
			log.Printf("Waiting %v before launching event_product_ch chunk %d...", delay, i+1)
			time.Sleep(delay)
		}

		semaphore <- struct{}{}
		go func(chunkNum, start, end int) {
			defer func() { <-semaphore }()
			processEventProductChChunk(mysqlDB, clickhouseConn, config, start, end, chunkNum, results)
		}(i+1, startID, endID)
	}

	for i := 0; i < config.NumChunks; i++ {
		result := <-results
		log.Printf("EventProductCh Result: %s", result)
	}

	log.Println("EventProductCh processing completed!")
}

func processEventProductChChunk(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config, startID, endID int, chunkNum int, results chan<- string) {
	log.Printf("Processing event_product_ch chunk %d: ID range %d-%d", chunkNum, startID, endID)

	totalRecords := endID - startID + 1
	processed := 0

	// Use batching within the chunk
	offset := 0
	for {
		batchData, err := buildEventProductChMigrationData(mysqlDB, startID, endID, config.BatchSize)
		if err != nil {
			results <- fmt.Sprintf("EventProductCh chunk %d batch error: %v", chunkNum, err)
			return
		}

		if len(batchData) == 0 {
			break
		}

		processed += len(batchData)
		progress := float64(processed) / float64(totalRecords) * 100
		log.Printf("EventProductCh chunk %d: Retrieved %d records in batch (%.1f%% complete)", chunkNum, len(batchData), progress)

		var eventProductChRecords []EventProductChRecord
		now := time.Now().Format("2006-01-02 15:04:05")
		for _, record := range batchData {
			productID := shared.ConvertToUInt32(record["product_id"])
			createdStr := shared.SafeConvertToDateTimeString(record["product_created"])
			idInputString := fmt.Sprintf("%d-%s", productID, createdStr)
			productUUID := shared.GenerateUUIDFromString(idInputString)
			productName := shared.ConvertToString(record["product_name"])
			slug := createSlug(productName)

			eventProductChRecord := EventProductChRecord{
				ID:                    productID,
				ProductUUID:           productUUID,
				Name:                  productName,
				Slug:                  slug,
				Event:                 shared.ConvertToUInt32(record["event"]),
				Edition:               shared.ConvertToUInt32(record["edition"]),
				ProductPublished:      shared.ConvertToInt8(record["product_published"]),
				EventProductPublished: shared.ConvertToInt8(record["event_product_published"]),
				Created:               createdStr,
				LastUpdatedAt:         now,
			}

			eventProductChRecords = append(eventProductChRecords, eventProductChRecord)
		}

		// Insert event_product_ch data into ClickHouse
		if len(eventProductChRecords) > 0 {
			log.Printf("EventProductCh chunk %d: Attempting to insert %d records into event_product_ch...", chunkNum, len(eventProductChRecords))

			attemptCount := 0
			insertErr := shared.RetryWithBackoff(
				func() error {
					if attemptCount > 0 {
						now := time.Now().Format("2006-01-02 15:04:05")
						for i := range eventProductChRecords {
							eventProductChRecords[i].LastUpdatedAt = now
						}
						log.Printf("EventProductCh chunk %d: Updated last_updated_at for retry attempt %d", chunkNum, attemptCount+1)
					}
					attemptCount++
					return insertEventProductChDataIntoClickHouse(clickhouseConn, eventProductChRecords, config.ClickHouseWorkers)
				},
				3,
			)

			if insertErr != nil {
				log.Printf("EventProductCh chunk %d: Insertion failed after retries: %v", chunkNum, insertErr)
				results <- fmt.Sprintf("EventProductCh chunk %d: Failed to insert %d records", chunkNum, len(eventProductChRecords))
				return
			} else {
				log.Printf("EventProductCh chunk %d: Successfully inserted %d records into event_product_ch", chunkNum, len(eventProductChRecords))
			}
		}

		if len(batchData) > 0 {
			lastRecord := batchData[len(batchData)-1]
			if lastID, ok := lastRecord["ep_id"].(int64); ok {
				startID = int(lastID) + 1
			} else if lastID, ok := lastRecord["ep_id"].(int32); ok {
				startID = int(lastID) + 1
			} else if lastID, ok := lastRecord["ep_id"].(int); ok {
				startID = lastID + 1
			}
		}

		offset += len(batchData)
		if len(batchData) < config.BatchSize {
			break
		}
	}

	results <- fmt.Sprintf("EventProductCh chunk %d: Completed successfully", chunkNum)
}

func buildEventProductChMigrationData(db *sql.DB, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT 
			ep.id as ep_id,
			p.id as product_id,
			ep.event,
			ep.edition,
			p.name as product_name,
			p.published as product_published,
			ep.published as event_product_published,
			p.created as product_created
		FROM event_products ep
		INNER JOIN product p ON ep.product = p.id
		WHERE ep.published in (0,1)
		and ep.id >= %d AND ep.id <= %d 
		ORDER BY ep.id 
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

// buildEventProductChDataForEventIDs fetches event_products + product data for the given event IDs. Used for incremental sync.
func buildEventProductChDataForEventIDs(db *sql.DB, eventIDs []int64) ([]map[string]interface{}, error) {
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
		SELECT 
			p.id as product_id,
			ep.event,
			ep.edition,
			p.name as product_name,
			p.published as product_published,
			ep.published as event_product_published,
			p.created as product_created
		FROM event_products ep
		INNER JOIN product p ON ep.product = p.id
		WHERE ep.published > 0
		AND ep.event IN (%s)
		ORDER BY ep.event, ep.edition, p.id`, strings.Join(placeholders, ","))
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

// BuildEventProductChRecordsForEventIDs fetches all event_products rows for the given event IDs
// and builds EventProductChRecords. Used for incremental sync.
func BuildEventProductChRecordsForEventIDs(db *sql.DB, eventIDs []int64) ([]EventProductChRecord, error) {
	rawData, err := buildEventProductChDataForEventIDs(db, eventIDs)
	if err != nil {
		return nil, err
	}
	now := time.Now().Format("2006-01-02 15:04:05")
	var records []EventProductChRecord
	for _, rec := range rawData {
		productID := shared.ConvertToUInt32(rec["product_id"])
		createdStr := shared.SafeConvertToDateTimeString(rec["product_created"])
		idInputString := fmt.Sprintf("%d-%s", productID, createdStr)
		productUUID := shared.GenerateUUIDFromString(idInputString)
		productName := shared.ConvertToString(rec["product_name"])
		slug := createSlug(productName)

		records = append(records, EventProductChRecord{
			ID:                    productID,
			ProductUUID:           productUUID,
			Name:                  productName,
			Slug:                  slug,
			Event:                 shared.ConvertToUInt32(rec["event"]),
			Edition:               shared.ConvertToUInt32(rec["edition"]),
			ProductPublished:      shared.ConvertToInt8(rec["product_published"]),
			EventProductPublished: shared.ConvertToInt8(rec["event_product_published"]),
			Created:               createdStr,
			LastUpdatedAt:         now,
		})
	}
	return records, nil
}

// InsertEventProductChDataIntoTable inserts records into the specified table. Used for incremental sync.
func InsertEventProductChDataIntoTable(clickhouseConn driver.Conn, records []EventProductChRecord, tableName string, numWorkers int) error {
	if len(records) == 0 {
		return nil
	}
	if numWorkers <= 1 {
		return insertEventProductChBatchIntoTable(clickhouseConn, records, tableName)
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
			results <- insertEventProductChBatchIntoTable(clickhouseConn, batch, tableName)
		}(start, end)
	}
	for i := 0; i < numWorkers && i*batchSize < len(records); i++ {
		if err := <-results; err != nil {
			return err
		}
	}
	return nil
}

func insertEventProductChBatchIntoTable(clickhouseConn driver.Conn, records []EventProductChRecord, tableName string) error {
	if len(records) == 0 {
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
			id, product_uuid, name, slug, event, edition, product_published, event_product_published, created, last_updated_at
		)`, tableName)

	batch, err := clickhouseConn.PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare batch for %s: %w", tableName, err)
	}
	for _, record := range records {
		if err := batch.Append(
			record.ID,
			record.ProductUUID,
			record.Name,
			record.Slug,
			record.Event,
			record.Edition,
			record.ProductPublished,
			record.EventProductPublished,
			record.Created,
			record.LastUpdatedAt,
		); err != nil {
			batch.Abort()
			return fmt.Errorf("failed to append record: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}
	return nil
}

func insertEventProductChDataIntoClickHouse(clickhouseConn driver.Conn, eventProductChRecords []EventProductChRecord, numWorkers int) error {
	if len(eventProductChRecords) == 0 {
		return nil
	}

	if numWorkers <= 1 {
		return insertEventProductChDataSingleWorker(clickhouseConn, eventProductChRecords)
	}

	// Split records into batches for parallel processing
	batchSize := len(eventProductChRecords) / numWorkers
	if batchSize == 0 {
		batchSize = 1
	}

	results := make(chan error, numWorkers)
	semaphore := make(chan struct{}, numWorkers)

	for i := 0; i < numWorkers; i++ {
		start := i * batchSize
		end := start + batchSize
		if i == numWorkers-1 {
			end = len(eventProductChRecords) // Last worker gets remaining records
		}

		if start >= len(eventProductChRecords) {
			break
		}

		semaphore <- struct{}{}
		go func(start, end int) {
			defer func() { <-semaphore }()
			batch := eventProductChRecords[start:end]
			err := insertEventProductChDataSingleWorker(clickhouseConn, batch)
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

func insertEventProductChDataSingleWorker(clickhouseConn driver.Conn, eventProductChRecords []EventProductChRecord) error {
	if len(eventProductChRecords) == 0 {
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	batch, err := clickhouseConn.PrepareBatch(ctx, `
		INSERT INTO testing_db.event_product_temp (
			id, product_uuid, name, slug, event, edition, product_published, event_product_published, created, last_updated_at
		)
	`)
	if err != nil {
		log.Printf("ERROR: Failed to prepare ClickHouse batch for event_product_ch: %v", err)
		return fmt.Errorf("failed to prepare ClickHouse batch: %v", err)
	}

	for _, record := range eventProductChRecords {
		err := batch.Append(
			record.ID,                    // id: UInt32
			record.ProductUUID,           // product_uuid: UUID
			record.Name,                  // name: LowCardinality(String)
			record.Slug,                  // slug: LowCardinality(String)
			record.Event,                 // event: UInt32
			record.Edition,               // edition: UInt32
			record.ProductPublished,      // product_published: Int8
			record.EventProductPublished, // event_product_published: Int8
			record.Created,               // created: DateTime
			record.LastUpdatedAt,         // last_updated_at: DateTime
		)
		if err != nil {
			log.Printf("ERROR: Failed to append record to batch: %v", err)
			log.Printf("Record data: ID=%d, ProductUUID=%s, Name=%s, Slug=%s, Event=%d, Edition=%d, ProductPublished=%d, EventProductPublished=%d, Created=%s",
				record.ID, record.ProductUUID, record.Name, record.Slug, record.Event, record.Edition, record.ProductPublished, record.EventProductPublished, record.Created)
			return fmt.Errorf("failed to append record to batch: %v", err)
		}
	}

	if err := batch.Send(); err != nil {
		log.Printf("ERROR: Failed to send ClickHouse batch: %v", err)
		return fmt.Errorf("failed to send ClickHouse batch: %v", err)
	}

	log.Printf("OK: Successfully inserted %d event_product_ch records", len(eventProductChRecords))
	return nil
}
