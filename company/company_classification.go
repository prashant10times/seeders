package company

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


var classificationTypeToID = map[string]uint8{
	"entity_type":    1,
	"company_role":   2,
	"specialization": 3,
}

type CompanyClassificationRecord struct {
	SourceID             uint32  `ch:"source_id"`
	CompanyID            uint32  `ch:"company_id"`
	ClassificationTypeID uint8   `ch:"classification_type_id"`
	ClassificationType   string  `ch:"classification_type"`
	ClassificationName   string  `ch:"classification_name"`
	Created              string  `ch:"created"`
	LastUpdatedAt        string  `ch:"last_updated_at"`
}

func ProcessCompanyClassificationOnly(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) {
	log.Println("=== Starting COMPANY CLASSIFICATION Processing ===")

	totalRecords, minID, maxID, err := shared.GetTotalRecordsAndIDRange(mysqlDB, "company_classification_mapping")
	if err != nil {
		log.Fatal("Failed to get total records and ID range from company_classification_mapping:", err)
	}

	log.Printf("Total company_classification_mapping records: %d, Min ID: %d, Max ID: %d", totalRecords, minID, maxID)

	if config.NumChunks <= 0 {
		config.NumChunks = 5
	}

	chunkSize := (maxID - minID + 1) / config.NumChunks
	if chunkSize == 0 {
		chunkSize = 1
	}

	log.Printf("Processing company_classification data in %d chunks with chunk size: %d", config.NumChunks, chunkSize)

	results := make(chan string, config.NumChunks)
	semaphore := make(chan struct{}, config.NumWorkers)

	for i := 0; i < config.NumChunks; i++ {
		startID := minID + (i * chunkSize)
		endID := startID + chunkSize - 1
		if i == config.NumChunks-1 {
			endID = maxID
		}
		if i > 0 {
			time.Sleep(3 * time.Second)
		}

		semaphore <- struct{}{}
		go func(chunkNum, start, end int) {
			defer func() { <-semaphore }()
			processCompanyClassificationChunk(mysqlDB, clickhouseConn, config, start, end, chunkNum, results)
		}(i+1, startID, endID)
	}

	for i := 0; i < config.NumChunks; i++ {
		result := <-results
		log.Printf("Company Classification Result: %s", result)
	}

	log.Println("Company classification processing completed!")
}

func processCompanyClassificationChunk(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config, startID, endID int, chunkNum int, results chan<- string) {
	log.Printf("Processing company_classification chunk %d: ID range %d-%d", chunkNum, startID, endID)

	totalRecords := endID - startID + 1
	processed := 0
	start := startID

	for {
		batchData, err := buildCompanyClassificationMigrationData(mysqlDB, start, endID, config.BatchSize)
		if err != nil {
			results <- fmt.Sprintf("Company classification chunk %d batch error: %v", chunkNum, err)
			return
		}

		if len(batchData) == 0 {
			break
		}

		processed += len(batchData)
		progress := float64(processed) / float64(totalRecords) * 100
		log.Printf("Company classification chunk %d: Retrieved %d records in batch (%.1f%% complete)", chunkNum, len(batchData), progress)

		now := time.Now().Format("2006-01-02 15:04:05")
		var records []CompanyClassificationRecord
		for _, row := range batchData {
			rec := mapRowToCompanyClassificationRecord(row, now)
			records = append(records, rec)
		}

		if len(records) > 0 {
			insertErr := shared.RetryWithBackoff(
				func() error {
					return insertCompanyClassificationDataIntoClickHouse(clickhouseConn, records, config.ClickHouseWorkers)
				},
				3,
			)
			if insertErr != nil {
				log.Printf("Company classification chunk %d: Insert failed after retries: %v", chunkNum, insertErr)
				results <- fmt.Sprintf("Company classification chunk %d: Failed to insert %d records", chunkNum, len(records))
				return
			}
			log.Printf("Company classification chunk %d: Inserted %d records into company_classification_temp", chunkNum, len(records))
		}

		if len(batchData) > 0 {
			if lastID, ok := batchData[len(batchData)-1]["id"].(int64); ok {
				start = int(lastID) + 1
			} else if lastID, ok := batchData[len(batchData)-1]["id"].(int32); ok {
				start = int(lastID) + 1
			} else if lastID, ok := batchData[len(batchData)-1]["id"].(int); ok {
				start = lastID + 1
			}
		}

		if len(batchData) < config.BatchSize {
			break
		}
	}

	results <- fmt.Sprintf("Company classification chunk %d: Completed successfully", chunkNum)
}

func buildCompanyClassificationMigrationData(db *sql.DB, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	// Use COALESCE so created is never null; alias so row["created"] is always set (in case DB column is created_at etc.)
	query := fmt.Sprintf(`
		SELECT id, company_id, classification_type_id, classification_type, name,
			COALESCE(created, modified) AS created,
			modified
		FROM company_classification_mapping
		WHERE id >= %d AND id <= %d
		ORDER BY id
		LIMIT %d`, startID, endID, batchSize)

	log.Printf("[SQL Company Classification] %s", strings.TrimSpace(query))
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
			if val != nil {
				row[col] = val
			}
		}
		results = append(results, row)
	}

	return results, nil
}

func mapRowToCompanyClassificationRecord(row map[string]interface{}, lastUpdatedAt string) CompanyClassificationRecord {
	created := shared.SafeClickHouseDateTimeString(row["created"])
	if mod := row["modified"]; mod != nil {
		s := shared.SafeConvertToString(mod)
		if s != "" && s != "0000-00-00 00:00:00" {
			lastUpdatedAt = shared.SafeClickHouseDateTimeString(mod)
		} else {
			lastUpdatedAt = created
		}
	} else {
		lastUpdatedAt = created
	}
	typeID := shared.ConvertToUInt8(row["classification_type_id"])
	classificationType := shared.SafeConvertToString(row["classification_type"])
	if typeID == 0 && classificationType != "" {
		if id, ok := classificationTypeToID[classificationType]; ok {
			typeID = id
		}
	}
	return CompanyClassificationRecord{
		SourceID:             shared.ConvertToUInt32(row["id"]),
		CompanyID:            shared.ConvertToUInt32(row["company_id"]),
		ClassificationTypeID: typeID,
		ClassificationType:   classificationType,
		ClassificationName:   shared.SafeConvertToString(row["name"]),
		Created:              created,
		LastUpdatedAt:        lastUpdatedAt,
	}
}

func insertCompanyClassificationDataIntoClickHouse(clickhouseConn driver.Conn, records []CompanyClassificationRecord, numWorkers int) error {
	if len(records) == 0 {
		return nil
	}
	if numWorkers <= 1 {
		return insertCompanyClassificationDataSingleWorker(clickhouseConn, records)
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
		go func(s, e int) {
			defer func() { <-semaphore }()
			results <- insertCompanyClassificationDataSingleWorker(clickhouseConn, records[s:e])
		}(start, end)
	}

	for i := 0; i < numWorkers && i*batchSize < len(records); i++ {
		if err := <-results; err != nil {
			return err
		}
	}
	return nil
}

func insertCompanyClassificationDataSingleWorker(clickhouseConn driver.Conn, records []CompanyClassificationRecord) error {
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	batch, err := clickhouseConn.PrepareBatch(ctx, `
		INSERT INTO company_classification_temp (
			source_id, company_id, classification_type_id, classification_type, classification_name, created, last_updated_at
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare ClickHouse batch: %v", err)
	}

	for _, r := range records {
		err := batch.Append(
			r.SourceID,
			r.CompanyID,
			r.ClassificationTypeID,
			r.ClassificationType,
			r.ClassificationName,
			r.Created,
			r.LastUpdatedAt,
		)
		if err != nil {
			return fmt.Errorf("failed to append record to batch: %v", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send ClickHouse batch: %v", err)
	}

	log.Printf("OK: Inserted %d company_classification records into company_classification_temp", len(records))
	return nil
}
