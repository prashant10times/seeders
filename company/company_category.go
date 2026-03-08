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

// CompanyCategoryRecord holds one row for the company_category_ch table (from MySQL company_interests).
type CompanyCategoryRecord struct {
	ID            uint32  `ch:"id"`
	UUID          string  `ch:"uuid"`
	CompanyID     uint32  `ch:"companyId"`
	Interest      string  `ch:"interest"`
	CategoryName  *string `ch:"categoryName"`
	Created       string  `ch:"created"`
	LastUpdatedAt string  `ch:"lastUpdatedAt"`
}

// ProcessCompanyCategoryOnly seeds company_category_ch from MySQL company_interests table (no ES).
func ProcessCompanyCategoryOnly(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) {
	log.Println("=== Starting COMPANY CATEGORY Processing ===")

	totalRecords, minID, maxID, err := shared.GetTotalRecordsAndIDRange(mysqlDB, "company_interests")
	if err != nil {
		log.Fatal("Failed to get total records and ID range from company_interests:", err)
	}

	log.Printf("Total company_interests records: %d, Min ID: %d, Max ID: %d", totalRecords, minID, maxID)

	if config.NumChunks <= 0 {
		config.NumChunks = 5
	}

	chunkSize := (maxID - minID + 1) / config.NumChunks
	if chunkSize == 0 {
		chunkSize = 1
	}

	log.Printf("Processing company_category data in %d chunks with chunk size: %d", config.NumChunks, chunkSize)

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
			processCompanyCategoryChunk(mysqlDB, clickhouseConn, config, start, end, chunkNum, results)
		}(i+1, startID, endID)
	}

	for i := 0; i < config.NumChunks; i++ {
		result := <-results
		log.Printf("Company Category Result: %s", result)
	}

	log.Println("Company category processing completed!")
}

func processCompanyCategoryChunk(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config, startID, endID int, chunkNum int, results chan<- string) {
	log.Printf("Processing company_category chunk %d: ID range %d-%d", chunkNum, startID, endID)

	totalRecords := endID - startID + 1
	processed := 0
	start := startID

	for {
		batchData, err := buildCompanyCategoryMigrationData(mysqlDB, start, endID, config.BatchSize)
		if err != nil {
			results <- fmt.Sprintf("Company category chunk %d batch error: %v", chunkNum, err)
			return
		}

		if len(batchData) == 0 {
			break
		}

		processed += len(batchData)
		progress := float64(processed) / float64(totalRecords) * 100
		log.Printf("Company category chunk %d: Retrieved %d records in batch (%.1f%% complete)", chunkNum, len(batchData), progress)

		now := time.Now().Format("2006-01-02 15:04:05")
		var records []CompanyCategoryRecord
		for _, row := range batchData {
			rec := mapRowToCompanyCategoryRecord(row, now)
			records = append(records, rec)
		}

		if len(records) > 0 {
			insertErr := shared.RetryWithBackoff(
				func() error {
					return insertCompanyCategoryDataIntoClickHouse(clickhouseConn, records, config.ClickHouseWorkers)
				},
				3,
			)
			if insertErr != nil {
				log.Printf("Company category chunk %d: Insert failed after retries: %v", chunkNum, insertErr)
				results <- fmt.Sprintf("Company category chunk %d: Failed to insert %d records", chunkNum, len(records))
				return
			}
			log.Printf("Company category chunk %d: Inserted %d records into company_category_temp", chunkNum, len(records))
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

	results <- fmt.Sprintf("Company category chunk %d: Completed successfully", chunkNum)
}

func buildCompanyCategoryMigrationData(db *sql.DB, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT id, company, interest, value, created
		FROM company_interests
		WHERE interest = 'industry' AND id >= %d AND id <= %d
		ORDER BY id
		LIMIT %d`, startID, endID, batchSize)

	log.Printf("[SQL Company Category] %s", strings.TrimSpace(query))
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

func mapRowToCompanyCategoryRecord(row map[string]interface{}, lastUpdatedAt string) CompanyCategoryRecord {
	id := shared.ConvertToUInt32(row["id"])
	created := shared.SafeClickHouseDateTimeString(row["created"])
	uuid := shared.GenerateUUIDFromString(fmt.Sprintf("%d-%s", id, created))
	return CompanyCategoryRecord{
		ID:            id,
		UUID:          uuid,
		CompanyID:     shared.ConvertToUInt32(row["company"]),
		Interest:      shared.ConvertToString(row["interest"]),
		CategoryName:  shared.ConvertToStringPtr(row["value"]),
		Created:       created,
		LastUpdatedAt: lastUpdatedAt,
	}
}

func insertCompanyCategoryDataIntoClickHouse(clickhouseConn driver.Conn, records []CompanyCategoryRecord, numWorkers int) error {
	if len(records) == 0 {
		return nil
	}
	if numWorkers <= 1 {
		return insertCompanyCategoryDataSingleWorker(clickhouseConn, records)
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
			results <- insertCompanyCategoryDataSingleWorker(clickhouseConn, records[s:e])
		}(start, end)
	}

	for i := 0; i < numWorkers && i*batchSize < len(records); i++ {
		if err := <-results; err != nil {
			return err
		}
	}
	return nil
}

func insertCompanyCategoryDataSingleWorker(clickhouseConn driver.Conn, records []CompanyCategoryRecord) error {
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
		INSERT INTO company_category_temp (
			id, uuid, companyId, interest, categoryName, created, lastUpdatedAt
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare ClickHouse batch: %v", err)
	}

	for _, r := range records {
		err := batch.Append(
			r.ID,
			r.UUID,
			r.CompanyID,
			r.Interest,
			r.CategoryName,
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

	log.Printf("OK: Inserted %d company_category records into company_category_temp", len(records))
	return nil
}
