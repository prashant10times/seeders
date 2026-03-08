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

// CompanyVisitorRecord holds one row for the company_visitor_ch table (from event_visitor + user + designation + city + country).
// visitorNameSearch is MATERIALIZED in ClickHouse so we do not insert it.
type CompanyVisitorRecord struct {
	ID                uint32  `ch:"id"`
	EventID           uint32  `ch:"eventId"`
	EditionID         uint32  `ch:"editionId"`
	CompanyID         *uint32 `ch:"companyId"`
	VisitorName       *string `ch:"visitorName"`
	VisitorCity       *string `ch:"visitorCity"`
	VisitorCountry    *string `ch:"visitorCountry"`
	VisitorLinkedIn   *string `ch:"visitorLinkedIn"`
	VisitorFacebook   *string `ch:"visitorFacebook"`
	VisitorTwitter    *string `ch:"visitorTwitter"`
	DesignationID     *uint32 `ch:"designationId"`
	DesignationName   *string `ch:"designationName"`
	DepartmentName    *string `ch:"departmentName"`
	Created           string  `ch:"created"`
	LastUpdatedAt     string  `ch:"lastUpdatedAt"`
}

// ProcessCompanyVisitorOnly seeds company_visitor_ch from MySQL event_visitor with JOINs to user (name, social URLs),
// designation (display_name), city (name) and country (name via city.country = country.id).
func ProcessCompanyVisitorOnly(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) {
	log.Println("=== Starting COMPANY VISITOR Processing ===")

	totalRecords, minID, maxID, err := shared.GetTotalRecordsAndIDRange(mysqlDB, "event_visitor")
	if err != nil {
		log.Fatal("Failed to get total records and ID range from event_visitor:", err)
	}

	log.Printf("Total event_visitor records: %d, Min ID: %d, Max ID: %d", totalRecords, minID, maxID)

	if config.NumChunks <= 0 {
		config.NumChunks = 5
	}

	chunkSize := (maxID - minID + 1) / config.NumChunks
	if chunkSize == 0 {
		chunkSize = 1
	}

	log.Printf("Processing company_visitor data in %d chunks with chunk size: %d", config.NumChunks, chunkSize)

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
			processCompanyVisitorChunk(mysqlDB, clickhouseConn, config, start, end, chunkNum, results)
		}(i+1, startID, endID)
	}

	for i := 0; i < config.NumChunks; i++ {
		result := <-results
		log.Printf("Company Visitor Result: %s", result)
	}

	log.Println("Company visitor processing completed!")
}

func processCompanyVisitorChunk(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config, startID, endID int, chunkNum int, results chan<- string) {
	log.Printf("Processing company_visitor chunk %d: ID range %d-%d", chunkNum, startID, endID)

	totalRecords := endID - startID + 1
	processed := 0
	start := startID

	for {
		batchData, err := buildCompanyVisitorMigrationData(mysqlDB, start, endID, config.BatchSize)
		if err != nil {
			results <- fmt.Sprintf("Company visitor chunk %d batch error: %v", chunkNum, err)
			return
		}

		if len(batchData) == 0 {
			break
		}

		processed += len(batchData)
		progress := float64(processed) / float64(totalRecords) * 100
		log.Printf("Company visitor chunk %d: Retrieved %d records in batch (%.1f%% complete)", chunkNum, len(batchData), progress)

		now := time.Now().Format("2006-01-02 15:04:05")
		var records []CompanyVisitorRecord
		for _, row := range batchData {
			rec := mapRowToCompanyVisitorRecord(row, now)
			records = append(records, rec)
		}

		if len(records) > 0 {
			insertErr := shared.RetryWithBackoff(
				func() error {
					return insertCompanyVisitorDataIntoClickHouse(clickhouseConn, records, config.ClickHouseWorkers)
				},
				3,
			)
			if insertErr != nil {
				log.Printf("Company visitor chunk %d: Insert failed after retries: %v", chunkNum, insertErr)
				results <- fmt.Sprintf("Company visitor chunk %d: Failed to insert %d records", chunkNum, len(records))
				return
			}
			log.Printf("Company visitor chunk %d: Inserted %d records into company_visitor_temp", chunkNum, len(records))
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

	results <- fmt.Sprintf("Company visitor chunk %d: Completed successfully", chunkNum)
}

// buildCompanyVisitorMigrationData fetches event_visitor rows with JOINs to user (name, linkedin_id, facebook_id, twitter_id),
// designation (display_name), city (name) and country (name via city.country = country.id).
func buildCompanyVisitorMigrationData(db *sql.DB, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT 
			ev.id,
			ev.event AS eventId,
			ev.edition AS editionId,
			ev.visitor_company AS companyId,
			u.name AS visitorName,
			ct.name AS visitorCity,
			ct.country AS visitorCountry,
			u.linkedin_id AS visitorLinkedIn,
			u.facebook_id AS visitorFacebook,
			u.twitter_id AS visitorTwitter,
			ev.designation_id AS designationId,
			d.display_name AS designationName,
			d.department AS departmentName,
			ev.created
		FROM event_visitor ev
		LEFT JOIN user u ON ev.user = u.id
		LEFT JOIN designation d ON ev.designation_id = d.id
		LEFT JOIN city ct ON ev.visitor_city = ct.id
		WHERE ev.id >= %d AND ev.id <= %d AND ev.published > 0
		ORDER BY ev.id
		LIMIT %d`, startID, endID, batchSize)

	log.Printf("[SQL Company Visitor] %s", strings.TrimSpace(query))
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

func mapRowToCompanyVisitorRecord(row map[string]interface{}, lastUpdatedAt string) CompanyVisitorRecord {
	return CompanyVisitorRecord{
		ID:              shared.ConvertToUInt32(row["id"]),
		EventID:         shared.ConvertToUInt32(row["eventId"]),
		EditionID:       shared.ConvertToUInt32(row["editionId"]),
		CompanyID:       shared.ConvertToUInt32Ptr(row["companyId"]),
		VisitorName:     shared.ConvertToStringPtr(row["visitorName"]),
		VisitorCity:     shared.ConvertToStringPtr(row["visitorCity"]),
		VisitorCountry:  shared.ConvertToStringPtr(row["visitorCountry"]),
		VisitorLinkedIn: shared.ConvertToStringPtr(row["visitorLinkedIn"]),
		VisitorFacebook: shared.ConvertToStringPtr(row["visitorFacebook"]),
		VisitorTwitter:  shared.ConvertToStringPtr(row["visitorTwitter"]),
		DesignationID:   shared.ConvertToUInt32Ptr(row["designationId"]),
		DesignationName: shared.ConvertToStringPtr(row["designationName"]),
		DepartmentName:  shared.ConvertToStringPtr(row["departmentName"]),
		Created:         shared.SafeClickHouseDateTimeString(row["created"]),
		LastUpdatedAt:   lastUpdatedAt,
	}
}

func insertCompanyVisitorDataIntoClickHouse(clickhouseConn driver.Conn, records []CompanyVisitorRecord, numWorkers int) error {
	if len(records) == 0 {
		return nil
	}
	if numWorkers <= 1 {
		return insertCompanyVisitorDataSingleWorker(clickhouseConn, records)
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
			results <- insertCompanyVisitorDataSingleWorker(clickhouseConn, records[s:e])
		}(start, end)
	}

	for i := 0; i < numWorkers && i*batchSize < len(records); i++ {
		if err := <-results; err != nil {
			return err
		}
	}
	return nil
}

func insertCompanyVisitorDataSingleWorker(clickhouseConn driver.Conn, records []CompanyVisitorRecord) error {
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

	// visitorNameSearch is MATERIALIZED in ClickHouse, so we do not insert it
	batch, err := clickhouseConn.PrepareBatch(ctx, `
		INSERT INTO company_visitor_temp (
			id, eventId, editionId, companyId, visitorName, visitorCity, visitorCountry,
			visitorLinkedIn, visitorFacebook, visitorTwitter,
			designationId, designationName, departmentName, created, lastUpdatedAt
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare ClickHouse batch: %v", err)
	}

	for _, r := range records {
		err := batch.Append(
			r.ID,
			r.EventID,
			r.EditionID,
			r.CompanyID,
			r.VisitorName,
			r.VisitorCity,
			r.VisitorCountry,
			r.VisitorLinkedIn,
			r.VisitorFacebook,
			r.VisitorTwitter,
			r.DesignationID,
			r.DesignationName,
			r.DepartmentName,
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

	log.Printf("OK: Inserted %d company_visitor records into company_visitor_temp", len(records))
	return nil
}
