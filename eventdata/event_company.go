package eventdata

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

// ParticipationType constants for event_company_ch Enum8
const (
	PartOrganizer  = "organizer"
	PartExhibitor  = "exhibitor"
	PartSponsor    = "sponsor"
	PartSpeaker    = "speaker"
	PartVisitor    = "visitor"
)

// EventCompanyRecord holds one row for the event_company_ch table.
type EventCompanyRecord struct {
	EventID           uint32 `ch:"eventId"`
	EditionID         uint32 `ch:"editionId"`
	CompanyID         uint32 `ch:"companyId"`
	ParticipationType string `ch:"participationType"`
	Created           string `ch:"created"`
	LastUpdatedAt     string `ch:"lastUpdatedAt"`
}

// ProcessEventCompanyOnly seeds event_company_temp from five sources: event_edition (organizer),
// event_exhibitor, event_sponsors, event_speaker, event_visitor. lastUpdatedAt is set at script level.
func ProcessEventCompanyOnly(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) {
	log.Println("=== Starting EVENT COMPANY Processing ===")

	sources := []struct {
		name   string
		run    func(*sql.DB, driver.Conn, shared.Config) error
	}{
		{"organizer", processEventCompanyOrganizer},
		{"exhibitor", processEventCompanyExhibitor},
		{"sponsor", processEventCompanySponsor},
		{"speaker", processEventCompanySpeaker},
		{"visitor", processEventCompanyVisitor},
	}

	for _, src := range sources {
		log.Printf("--- Processing event_company source: %s ---", src.name)
		if err := src.run(mysqlDB, clickhouseConn, config); err != nil {
			log.Printf("ERROR: event_company %s failed: %v", src.name, err)
		}
	}
	log.Println("Event company processing completed!")
}

func processEventCompanyOrganizer(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) error {
	return processEventCompanyByTable(mysqlDB, clickhouseConn, config, "event_edition", PartOrganizer, buildEventCompanyOrganizerData)
}

func processEventCompanyExhibitor(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) error {
	return processEventCompanyByTable(mysqlDB, clickhouseConn, config, "event_exhibitor", PartExhibitor, buildEventCompanyExhibitorData)
}

func processEventCompanySponsor(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) error {
	return processEventCompanyByTable(mysqlDB, clickhouseConn, config, "event_sponsors", PartSponsor, buildEventCompanySponsorData)
}

func processEventCompanySpeaker(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) error {
	return processEventCompanyByTable(mysqlDB, clickhouseConn, config, "event_speaker", PartSpeaker, buildEventCompanySpeakerData)
}

func processEventCompanyVisitor(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) error {
	return processEventCompanyByTable(mysqlDB, clickhouseConn, config, "event_visitor", PartVisitor, buildEventCompanyVisitorData)
}

type buildEventCompanyBatchFunc func(*sql.DB, int, int, int) ([]map[string]interface{}, error)

func processEventCompanyByTable(
	mysqlDB *sql.DB,
	clickhouseConn driver.Conn,
	config shared.Config,
	tableName string,
	participationType string,
	buildBatch buildEventCompanyBatchFunc,
) error {
	totalRecords, minID, maxID, err := shared.GetTotalRecordsAndIDRange(mysqlDB, tableName)
	if err != nil {
		return fmt.Errorf("get range for %s: %w", tableName, err)
	}
	log.Printf("Total %s records: %d, Min ID: %d, Max ID: %d", tableName, totalRecords, minID, maxID)

	if config.NumChunks <= 0 {
		config.NumChunks = 5
	}
	chunkSize := (maxID - minID + 1) / config.NumChunks
	if chunkSize == 0 {
		chunkSize = 1
	}

	startID := minID
	lastUpdatedAt := time.Now().Format("2006-01-02 15:04:05")
	totalInserted := 0

	for {
		batchData, err := buildBatch(mysqlDB, startID, maxID, config.BatchSize)
		if err != nil {
			return err
		}
		if len(batchData) == 0 {
			break
		}

		var records []EventCompanyRecord
		for _, row := range batchData {
			rec := mapRowToEventCompanyRecord(row, participationType, lastUpdatedAt)
			if rec.CompanyID == 0 {
				continue
			}
			records = append(records, rec)
		}

		if len(records) > 0 {
			insertErr := shared.RetryWithBackoff(
				func() error {
					return insertEventCompanyIntoClickHouse(clickhouseConn, records, config.ClickHouseWorkers)
				},
				3,
			)
			if insertErr != nil {
				return fmt.Errorf("insert %s batch: %w", tableName, insertErr)
			}
			totalInserted += len(records)
		}

		if len(batchData) > 0 {
			lastID := rowInt64(batchData[len(batchData)-1], "id")
			if lastID > 0 {
				startID = int(lastID) + 1
			}
		}
		if len(batchData) < config.BatchSize || startID > maxID {
			break
		}
	}

	log.Printf("OK: Inserted %d %s records into event_company_temp", totalInserted, participationType)
	return nil
}

func buildEventCompanyOrganizerData(db *sql.DB, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT ee.id AS id, ee.event AS eventId, ee.id AS editionId, ee.company_id AS companyId, ee.created AS created
		FROM event_edition ee
		WHERE ee.company_id IS NOT NULL AND ee.id >= %d AND ee.id <= %d
		ORDER BY ee.id
		LIMIT %d`, startID, endID, batchSize)
	return runEventCompanyQueryWithID(db, query)
}

func buildEventCompanyExhibitorData(db *sql.DB, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT event_id AS eventId, edition_id AS editionId, company_id AS companyId, created AS created, id
		FROM event_exhibitor
		WHERE company_id IS NOT NULL AND published > 0 AND id >= %d AND id <= %d
		ORDER BY id
		LIMIT %d`, startID, endID, batchSize)
	return runEventCompanyQueryWithID(db, query)
}

func buildEventCompanySponsorData(db *sql.DB, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT event_id AS eventId, event_edition AS editionId, company_id AS companyId, created AS created, id
		FROM event_sponsors
		WHERE company_id IS NOT NULL AND published > 0 AND id >= %d AND id <= %d
		ORDER BY id
		LIMIT %d`, startID, endID, batchSize)
	return runEventCompanyQueryWithID(db, query)
}

func buildEventCompanySpeakerData(db *sql.DB, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT event AS eventId, edition AS editionId, company_id AS companyId, created AS created, id
		FROM event_speaker
		WHERE company_id IS NOT NULL AND published > 0 AND id >= %d AND id <= %d
		ORDER BY id
		LIMIT %d`, startID, endID, batchSize)
	return runEventCompanyQueryWithID(db, query)
}

func buildEventCompanyVisitorData(db *sql.DB, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT ev.event AS eventId, ev.edition AS editionId, u.company AS companyId, ev.created AS created, ev.id
		FROM event_visitor ev
		INNER JOIN user u ON u.id = ev.user
		WHERE u.company IS NOT NULL AND ev.published > 0 AND ev.id >= %d AND ev.id <= %d
		ORDER BY ev.id
		LIMIT %d`, startID, endID, batchSize)
	return runEventCompanyQueryWithID(db, query)
}

func runEventCompanyQueryWithID(db *sql.DB, query string) ([]map[string]interface{}, error) {
	log.Printf("[SQL Event Company] %s", strings.TrimSpace(query))
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRowsToMap(rows)
}

func scanRowsToMap(rows *sql.Rows) ([]map[string]interface{}, error) {
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
			}
		}
		results = append(results, row)
	}
	return results, rows.Err()
}

func mapRowToEventCompanyRecord(row map[string]interface{}, participationType string, lastUpdatedAt string) EventCompanyRecord {
	return EventCompanyRecord{
		EventID:           shared.ConvertToUInt32(row["eventId"]),
		EditionID:         shared.ConvertToUInt32(row["editionId"]),
		CompanyID:         shared.ConvertToUInt32(row["companyId"]),
		ParticipationType:  participationType,
		Created:            shared.SafeClickHouseDateTimeString(row["created"]),
		LastUpdatedAt:      lastUpdatedAt,
	}
}

func insertEventCompanyIntoClickHouse(clickhouseConn driver.Conn, records []EventCompanyRecord, numWorkers int) error {
	if len(records) == 0 {
		return nil
	}
	if numWorkers <= 1 {
		return insertEventCompanySingleWorker(clickhouseConn, records)
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
			results <- insertEventCompanySingleWorker(clickhouseConn, records[s:e])
		}(start, end)
	}
	for i := 0; i < numWorkers && i*batchSize < len(records); i++ {
		if err := <-results; err != nil {
			return err
		}
	}
	return nil
}

func insertEventCompanySingleWorker(clickhouseConn driver.Conn, records []EventCompanyRecord) error {
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
		INSERT INTO event_company_temp (
			eventId, editionId, companyId, participationType, created, lastUpdatedAt
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare ClickHouse batch: %v", err)
	}
	for _, r := range records {
		err := batch.Append(
			r.EventID, r.EditionID, r.CompanyID, r.ParticipationType,
			r.Created, r.LastUpdatedAt,
		)
		if err != nil {
			return fmt.Errorf("failed to append record: %v", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send ClickHouse batch: %v", err)
	}
	return nil
}
