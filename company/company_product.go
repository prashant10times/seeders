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

// CompanyProductRecord holds one row for the company_product_ch table (from MySQL company_interests + product).
type CompanyProductRecord struct {
	ID             uint32 `ch:"id"`
	UUID           string `ch:"uuid"`
	CompanyID      uint32 `ch:"companyId"`
	Interest       string `ch:"interest"`
	CompanyProduct uint32 `ch:"companyProduct"`
	ProductName    string `ch:"productName"`
	Created        string `ch:"created"`
	LastUpdatedAt  string `ch:"lastUpdatedAt"`
}

// ProcessCompanyProductOnly seeds company_product_ch from MySQL company_interests (interest='product') with product name join.
func ProcessCompanyProductOnly(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) {
	log.Println("=== Starting COMPANY PRODUCT Processing ===")

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

	log.Printf("Processing company_product data in %d chunks with chunk size: %d", config.NumChunks, chunkSize)

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
			processCompanyProductChunk(mysqlDB, clickhouseConn, config, start, end, chunkNum, results)
		}(i+1, startID, endID)
	}

	for i := 0; i < config.NumChunks; i++ {
		result := <-results
		log.Printf("Company Product Result: %s", result)
	}

	log.Println("Company product processing completed!")
}

func processCompanyProductChunk(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config, startID, endID int, chunkNum int, results chan<- string) {
	log.Printf("Processing company_product chunk %d: ID range %d-%d", chunkNum, startID, endID)

	totalRecords := endID - startID + 1
	processed := 0
	start := startID

	for {
		batchData, err := buildCompanyProductMigrationData(mysqlDB, start, endID, config.BatchSize)
		if err != nil {
			results <- fmt.Sprintf("Company product chunk %d batch error: %v", chunkNum, err)
			return
		}

		if len(batchData) == 0 {
			break
		}

		processed += len(batchData)
		progress := float64(processed) / float64(totalRecords) * 100
		log.Printf("Company product chunk %d: Retrieved %d records in batch (%.1f%% complete)", chunkNum, len(batchData), progress)

		now := time.Now().Format("2006-01-02 15:04:05")
		var records []CompanyProductRecord
		for _, row := range batchData {
			rec := mapRowToCompanyProductRecord(row, now)
			records = append(records, rec)
		}

		if len(records) > 0 {
			insertErr := shared.RetryWithBackoff(
				func() error {
					return insertCompanyProductDataIntoClickHouse(clickhouseConn, records, config.ClickHouseWorkers)
				},
				3,
			)
			if insertErr != nil {
				log.Printf("Company product chunk %d: Insert failed after retries: %v", chunkNum, insertErr)
				results <- fmt.Sprintf("Company product chunk %d: Failed to insert %d records", chunkNum, len(records))
				return
			}
			log.Printf("Company product chunk %d: Inserted %d records into company_product_temp", chunkNum, len(records))
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

	results <- fmt.Sprintf("Company product chunk %d: Completed successfully", chunkNum)
}

func buildCompanyProductMigrationData(db *sql.DB, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT ci.id, ci.company, ci.interest, ci.value AS company_product, p.name AS product_name, ci.created, p.id AS product_id, p.created AS product_created
		FROM company_interests ci
		LEFT JOIN product p ON p.id = ci.value
		WHERE ci.interest = 'product' AND ci.id >= %d AND ci.id <= %d
		ORDER BY ci.id
		LIMIT %d`, startID, endID, batchSize)

	log.Printf("[SQL Company Product] %s", strings.TrimSpace(query))
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

func mapRowToCompanyProductRecord(row map[string]interface{}, lastUpdatedAt string) CompanyProductRecord {
	productID := shared.ConvertToUInt32(shared.ConvertToString(row["product_id"]))
	productCreated := shared.SafeClickHouseDateTimeString(row["product_created"])
	uuid := shared.GenerateUUIDFromString(fmt.Sprintf("%d-%s", productID, productCreated))
	// company_product comes from ci.value; driver may return key "company_product" (alias) or "value", and MySQL often returns VARCHAR as []byte
	companyProductVal := row["company_product"]
	if companyProductVal == nil {
		companyProductVal = row["value"]
	}
	companyProduct := shared.ConvertToUInt32(shared.ConvertToString(companyProductVal))
	return CompanyProductRecord{
		ID:             shared.ConvertToUInt32(row["id"]),
		UUID:           uuid,
		CompanyID:      shared.ConvertToUInt32(row["company"]),
		Interest:       shared.ConvertToString(row["interest"]),
		CompanyProduct: companyProduct,
		ProductName:    shared.ConvertToString(row["product_name"]),
		Created:        shared.SafeClickHouseDateTimeString(row["created"]),
		LastUpdatedAt:  lastUpdatedAt,
	}
}

func insertCompanyProductDataIntoClickHouse(clickhouseConn driver.Conn, records []CompanyProductRecord, numWorkers int) error {
	if len(records) == 0 {
		return nil
	}
	if numWorkers <= 1 {
		return insertCompanyProductDataSingleWorker(clickhouseConn, records)
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
			results <- insertCompanyProductDataSingleWorker(clickhouseConn, records[s:e])
		}(start, end)
	}

	for i := 0; i < numWorkers && i*batchSize < len(records); i++ {
		if err := <-results; err != nil {
			return err
		}
	}
	return nil
}

func insertCompanyProductDataSingleWorker(clickhouseConn driver.Conn, records []CompanyProductRecord) error {
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
		INSERT INTO company_product_temp (
			id, uuid, companyId, interest, companyProduct, productName, created, lastUpdatedAt
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
			r.CompanyProduct,
			r.ProductName,
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

	log.Printf("OK: Inserted %d company_product records into company_product_temp", len(records))
	return nil
}
