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

// CompanySpeakerRecord holds one row for the company_speaker_ch table (from event_speaker + user + designation + city + country).
// Schema: id, eventId, editionId, companyId, speakerName, speakerCity, speakerCountry, speakerLinkedin, speakerFacebook,
// speakerTwitter, speakerProfile, speakerTitle, designationId, designationName, departmentName, created, lastUpdatedAt
type CompanySpeakerRecord struct {
	ID               uint32  `ch:"id"`
	EventID          uint32  `ch:"eventId"`
	EditionID        uint32  `ch:"editionId"`
	CompanyID        *uint32 `ch:"companyId"`
	SpeakerName      *string `ch:"speakerName"`
	SpeakerCity      *string `ch:"speakerCity"`
	SpeakerCountry   *string `ch:"speakerCountry"`
	SpeakerLinkedin  *string `ch:"speakerLinkedin"`
	SpeakerFacebook *string `ch:"speakerFacebook"`
	SpeakerTwitter   *string `ch:"speakerTwitter"`
	SpeakerProfile   *string `ch:"speakerProfile"`
	SpeakerTitle     *string `ch:"speakerTitle"`
	DesignationID    *uint32 `ch:"designationId"`
	DesignationName  *string `ch:"designationName"`
	DepartmentName   *string `ch:"departmentName"`
	Created          string  `ch:"created"`
	LastUpdatedAt    string  `ch:"lastUpdatedAt"`
}

// ProcessCompanySpeakerOnly seeds company_speaker_ch from MySQL event_speaker with JOINs to user (social, designation_id),
// designation (display_name, department), city (name), country (name).
func ProcessCompanySpeakerOnly(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) {
	log.Println("=== Starting COMPANY SPEAKER Processing ===")

	totalRecords, minID, maxID, err := shared.GetTotalRecordsAndIDRange(mysqlDB, "event_speaker")
	if err != nil {
		log.Fatal("Failed to get total records and ID range from event_speaker:", err)
	}

	log.Printf("Total event_speaker records: %d, Min ID: %d, Max ID: %d", totalRecords, minID, maxID)

	if config.NumChunks <= 0 {
		config.NumChunks = 5
	}

	chunkSize := (maxID - minID + 1) / config.NumChunks
	if chunkSize == 0 {
		chunkSize = 1
	}

	log.Printf("Processing company_speaker data in %d chunks with chunk size: %d", config.NumChunks, chunkSize)

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
			processCompanySpeakerChunk(mysqlDB, clickhouseConn, config, start, end, chunkNum, results)
		}(i+1, startID, endID)
	}

	for i := 0; i < config.NumChunks; i++ {
		result := <-results
		log.Printf("Company Speaker Result: %s", result)
	}

	log.Println("Company speaker processing completed!")
}

func processCompanySpeakerChunk(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config, startID, endID int, chunkNum int, results chan<- string) {
	log.Printf("Processing company_speaker chunk %d: ID range %d-%d", chunkNum, startID, endID)

	totalRecords := endID - startID + 1
	processed := 0
	start := startID

	for {
		batchData, err := buildCompanySpeakerMigrationData(mysqlDB, start, endID, config.BatchSize)
		if err != nil {
			results <- fmt.Sprintf("Company speaker chunk %d batch error: %v", chunkNum, err)
			return
		}

		if len(batchData) == 0 {
			break
		}

		processed += len(batchData)
		progress := float64(processed) / float64(totalRecords) * 100
		log.Printf("Company speaker chunk %d: Retrieved %d records in batch (%.1f%% complete)", chunkNum, len(batchData), progress)

		now := time.Now().Format("2006-01-02 15:04:05")
		var records []CompanySpeakerRecord
		for _, row := range batchData {
			rec := mapRowToCompanySpeakerRecord(row, now)
			records = append(records, rec)
		}

		if len(records) > 0 {
			insertErr := shared.RetryWithBackoff(
				func() error {
					return insertCompanySpeakerDataIntoClickHouse(clickhouseConn, records, config.ClickHouseWorkers)
				},
				3,
			)
			if insertErr != nil {
				log.Printf("Company speaker chunk %d: Insert failed after retries: %v", chunkNum, insertErr)
				results <- fmt.Sprintf("Company speaker chunk %d: Failed to insert %d records", chunkNum, len(records))
				return
			}
			log.Printf("Company speaker chunk %d: Inserted %d records into company_speaker_temp", chunkNum, len(records))
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

	results <- fmt.Sprintf("Company speaker chunk %d: Completed successfully", chunkNum)
}

// buildCompanySpeakerMigrationData fetches event_speaker rows with JOINs to user (social URLs, designation_id),
// designation (display_name, department), city (name for speakerCity), country (name for speakerCountry).
func buildCompanySpeakerMigrationData(db *sql.DB, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT 
			es.id,
			es.event AS eventId,
			es.edition AS editionId,
			es.company_id AS companyId,
			COALESCE(NULLIF(TRIM(es.speaker_name), ''), u.name) AS speakerName,
			ct.name AS speakerCity,
			ct.country AS speakerCountry,
			u.linkedin_id AS speakerLinkedin,
			u.facebook_id AS speakerFacebook,
			u.twitter_id AS speakerTwitter,
			es.speaker_profile AS speakerProfile,
			es.title AS speakerTitle,
			u.designation_id AS designationId,
			d.display_name AS designationName,
			d.department AS departmentName,
			es.created
		FROM event_speaker es
		LEFT JOIN user u ON es.user_id = u.id
		LEFT JOIN designation d ON u.designation_id = d.id
		LEFT JOIN city ct ON u.city = ct.id
		WHERE es.id >= %d AND es.id <= %d AND es.published > 0
		ORDER BY es.id
		LIMIT %d`, startID, endID, batchSize)

	log.Printf("[SQL Company Speaker] %s", strings.TrimSpace(query))
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

func mapRowToCompanySpeakerRecord(row map[string]interface{}, lastUpdatedAt string) CompanySpeakerRecord {
	return CompanySpeakerRecord{
		ID:               shared.ConvertToUInt32(row["id"]),
		EventID:          shared.ConvertToUInt32(row["eventId"]),
		EditionID:        shared.ConvertToUInt32(row["editionId"]),
		CompanyID:        shared.ConvertToUInt32Ptr(row["companyId"]),
		SpeakerName:      shared.ConvertToStringPtr(row["speakerName"]),
		SpeakerCity:      shared.ConvertToStringPtr(row["speakerCity"]),
		SpeakerCountry:   shared.ConvertToStringPtr(row["speakerCountry"]),
		SpeakerLinkedin:  shared.ConvertToStringPtr(row["speakerLinkedin"]),
		SpeakerFacebook: shared.ConvertToStringPtr(row["speakerFacebook"]),
		SpeakerTwitter:   shared.ConvertToStringPtr(row["speakerTwitter"]),
		SpeakerProfile:   shared.ConvertToStringPtr(row["speakerProfile"]),
		SpeakerTitle:    shared.ConvertToStringPtr(row["speakerTitle"]),
		DesignationID:   shared.ConvertToUInt32Ptr(row["designationId"]),
		DesignationName: shared.ConvertToStringPtr(row["designationName"]),
		DepartmentName:  shared.ConvertToStringPtr(row["departmentName"]),
		Created:         shared.SafeClickHouseDateTimeString(row["created"]),
		LastUpdatedAt:   lastUpdatedAt,
	}
}

func insertCompanySpeakerDataIntoClickHouse(clickhouseConn driver.Conn, records []CompanySpeakerRecord, numWorkers int) error {
	if len(records) == 0 {
		return nil
	}
	if numWorkers <= 1 {
		return insertCompanySpeakerDataSingleWorker(clickhouseConn, records)
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
			results <- insertCompanySpeakerDataSingleWorker(clickhouseConn, records[s:e])
		}(start, end)
	}

	for i := 0; i < numWorkers && i*batchSize < len(records); i++ {
		if err := <-results; err != nil {
			return err
		}
	}
	return nil
}

func insertCompanySpeakerDataSingleWorker(clickhouseConn driver.Conn, records []CompanySpeakerRecord) error {
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
		INSERT INTO company_speaker_temp (
			id, eventId, editionId, companyId, speakerName, speakerCity, speakerCountry,
			speakerLinkedin, speakerFacebook, speakerTwitter, speakerProfile, speakerTitle,
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
			r.SpeakerName,
			r.SpeakerCity,
			r.SpeakerCountry,
			r.SpeakerLinkedin,
			r.SpeakerFacebook,
			r.SpeakerTwitter,
			r.SpeakerProfile,
			r.SpeakerTitle,
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

	log.Printf("OK: Inserted %d company_speaker records into company_speaker_temp", len(records))
	return nil
}
