package utils

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"seeders/shared"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func safeConvertToString(value interface{}) string {
	if value == nil {
		return ""
	}
	if str, ok := value.(string); ok {
		return str
	}
	if bytes, ok := value.([]uint8); ok {
		return string(bytes)
	}
	return fmt.Sprintf("%v", value)
}

type EventDesignationChRecord struct {
	EventID         uint32  `ch:"event_id"`
	EditionID       uint32  `ch:"edition_id"`
	SourceVisitorID uint32  `ch:"sourceVisitorId"`
	DesignationID   uint32  `ch:"designation_id"`
	DesignationUUID *string `ch:"designation_uuid"`
	DisplayName     string  `ch:"display_name"`
	Department      string  `ch:"department"`
	Role            string  `ch:"role"`
	TotalVisitors   uint32  `ch:"total_visitors"`
	Version         uint32  `ch:"version"`
	LastUpdatedAt   string  `ch:"last_updated_at"`
}

type DesignationData struct {
	ID          uint32
	DisplayName string
	Department  string
	Role        string
	Created     interface{}
}

type EventEditionPair struct {
	EventID   uint32
	EditionID uint32
}

func FetchDesignationDisplayNameData(db *sql.DB, designationIDs []uint32) (map[uint32]DesignationData, error) {
	if len(designationIDs) == 0 {
		return make(map[uint32]DesignationData), nil
	}

	idStrings := make([]string, len(designationIDs))
	for i, id := range designationIDs {
		idStrings[i] = fmt.Sprintf("%d", id)
	}

	query := fmt.Sprintf(`
		SELECT id, display_name, department, role, created
		FROM designation
		WHERE id IN (%s)
	`, strings.Join(idStrings, ","))

	rows, err := db.Query(query)
	if err != nil {
		log.Printf("Error fetching designation data: %v", err)
		return nil, err
	}
	defer rows.Close()

	designationMap := make(map[uint32]DesignationData)
	for rows.Next() {
		var id uint32
		var displayName, department, role sql.NullString
		var created interface{}
		if err := rows.Scan(&id, &displayName, &department, &role, &created); err != nil {
			log.Printf("Error scanning designation row: %v", err)
			continue
		}
		designationMap[id] = DesignationData{
			ID:          id,
			DisplayName: displayName.String,
			Department:  department.String,
			Role:        role.String,
			Created:     created,
		}
	}

	log.Printf("Fetched %d designation records with display_name, department, and role", len(designationMap))

	scannedIDs := make([]uint32, 0, len(designationMap))
	for id := range designationMap {
		scannedIDs = append(scannedIDs, id)
	}
	missingFromDB := make([]uint32, 0)
	for _, requestedID := range designationIDs {
		if _, found := designationMap[requestedID]; !found {
			missingFromDB = append(missingFromDB, requestedID)
		}
	}
	if len(missingFromDB) > 0 {
		log.Printf("WARNING: %d designation IDs were requested but not found in database: %v", len(missingFromDB), missingFromDB)
	}

	return designationMap, nil
}

func ConvertToEventDesignationChRecords(mysqlData []map[string]interface{}, db *sql.DB) []EventDesignationChRecord {
	aggregatedData := make(map[string]map[string]interface{})

	log.Printf("Processing %d raw visitor records for aggregation", len(mysqlData))

	for _, row := range mysqlData {
		eventID := safeConvertToString(row["event"])
		editionID := safeConvertToString(row["edition"])
		designationID := safeConvertToString(row["designation_id"])

		if eventID == "" || editionID == "" || designationID == "" {
			continue
		}

		key := fmt.Sprintf("%s_%s_%s", eventID, editionID, designationID)

		if _, exists := aggregatedData[key]; !exists {
			aggregatedData[key] = map[string]interface{}{
				"event":            row["event"],
				"edition":          row["edition"],
				"designation_id":   row["designation_id"],
				"created":          row["created"],
				"total_visitors":   0,
				"source_visitor_id": row["id"], // use first visitor id as sourceVisitorId
			}
		}

		if count, ok := aggregatedData[key]["total_visitors"].(int); ok {
			aggregatedData[key]["total_visitors"] = count + 1
		}
	}

	var aggregatedSlice []map[string]interface{}
	for _, data := range aggregatedData {
		aggregatedSlice = append(aggregatedSlice, data)
	}

	log.Printf("Aggregated %d raw records into %d unique event-edition-designation combinations", len(mysqlData), len(aggregatedSlice))

	var records []EventDesignationChRecord
	now := time.Now().Format("2006-01-02 15:04:05")

	designationIDSet := make(map[uint32]bool)
	for _, row := range aggregatedSlice {
		if designationID, ok := row["designation_id"]; ok && designationID != nil {
			if designationIDVal, ok := designationID.(int64); ok {
				designationIDSet[uint32(designationIDVal)] = true
			}
		}
	}

	uniqueDesignationIDs := make([]uint32, 0, len(designationIDSet))
	for id := range designationIDSet {
		uniqueDesignationIDs = append(uniqueDesignationIDs, id)
	}

	var designationData map[uint32]DesignationData
	if len(uniqueDesignationIDs) > 0 {
		var err error
		designationData, err = FetchDesignationDisplayNameData(db, uniqueDesignationIDs)
		if err != nil {
			log.Printf("Warning: Could not fetch designation data: %v", err)
			designationData = make(map[uint32]DesignationData)
		} else {
			missingIDs := make([]uint32, 0)
			for _, requestedID := range uniqueDesignationIDs {
				if _, exists := designationData[requestedID]; !exists {
					missingIDs = append(missingIDs, requestedID)
				}
			}
			if len(missingIDs) > 0 {
				log.Printf("WARNING: %d requested designation IDs were not found in database: %v", len(missingIDs), missingIDs)
			}
		}
	} else {
		designationData = make(map[uint32]DesignationData)
	}

	for _, row := range aggregatedSlice {
		record := EventDesignationChRecord{
			Version:       1,
			LastUpdatedAt: now,
		}

		if eventID, ok := row["event"]; ok && eventID != nil {
			if eventIDVal, ok := eventID.(int64); ok {
				record.EventID = uint32(eventIDVal)
			}
		}

		if editionID, ok := row["edition"]; ok && editionID != nil {
			if editionIDVal, ok := editionID.(int64); ok {
				record.EditionID = uint32(editionIDVal)
			}
		}

		if sourceVisitorID, ok := row["source_visitor_id"]; ok && sourceVisitorID != nil {
			if val, ok := sourceVisitorID.(int64); ok {
				record.SourceVisitorID = uint32(val)
			}
		}

		if designationID, ok := row["designation_id"]; ok && designationID != nil {
			if designationIDVal, ok := designationID.(int64); ok {
				designationIDUint := uint32(designationIDVal)
				record.DesignationID = designationIDUint

				if designationInfo, exists := designationData[designationIDUint]; exists {
					record.DisplayName = designationInfo.DisplayName
					record.Department = designationInfo.Department
					record.Role = designationInfo.Role
					designationIDFromTable := designationInfo.ID
					createdStr := shared.SafeConvertToString(designationInfo.Created)
					idInputString := fmt.Sprintf("%d-%s", designationIDFromTable, createdStr)
					uuid := shared.GenerateUUIDFromString(idInputString)
					record.DesignationUUID = &uuid
				} else {
					record.DesignationUUID = nil
				}
			}
		}

		if totalVisitors, ok := row["total_visitors"]; ok && totalVisitors != nil {
			if totalVisitorsVal, ok := totalVisitors.(int); ok {
				record.TotalVisitors = uint32(totalVisitorsVal)
			}
		}

		records = append(records, record)
	}

	return records
}

// InsertEventDesignationChDataIntoTable inserts records into the specified table. Used for incremental sync.
func InsertEventDesignationChDataIntoTable(clickhouseConn driver.Conn, records []EventDesignationChRecord, tableName string, numWorkers int) error {
	if len(records) == 0 {
		return nil
	}
	if numWorkers <= 1 {
		return insertEventDesignationChBatchIntoTable(clickhouseConn, records, tableName)
	}
	batchSize := len(records) / numWorkers
	if batchSize == 0 {
		batchSize = 1
	}
	var wg sync.WaitGroup
	errorsChan := make(chan error, numWorkers)
	semaphore := make(chan struct{}, numWorkers)
	for i := 0; i < numWorkers; i++ {
		start := i * batchSize
		end := start + batchSize
		if i == numWorkers-1 {
			end = len(records)
		}
		if start >= len(records) {
			break
		}
		wg.Add(1)
		semaphore <- struct{}{}
		go func(start, end int) {
			defer wg.Done()
			defer func() { <-semaphore }()
			batch := records[start:end]
			if err := insertEventDesignationChBatchIntoTable(clickhouseConn, batch, tableName); err != nil {
				errorsChan <- err
			}
		}(start, end)
	}
	wg.Wait()
	close(errorsChan)
	for err := range errorsChan {
		if err != nil {
			return err
		}
	}
	return nil
}

func insertEventDesignationChBatchIntoTable(clickhouseConn driver.Conn, records []EventDesignationChRecord, tableName string) error {
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
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	query := fmt.Sprintf(`
		INSERT INTO %s (event_id, edition_id, sourceVisitorId, designation_id, designation_uuid, display_name, department, role, total_visitors, version, last_updated_at)
	`, tableName)
	batch, err := clickhouseConn.PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare ClickHouse batch for %s: %w", tableName, err)
	}
	for _, record := range records {
		if err := batch.Append(
			record.EventID, record.EditionID, record.SourceVisitorID, record.DesignationID, record.DesignationUUID,
			record.DisplayName, record.Department, record.Role, record.TotalVisitors,
			record.Version, record.LastUpdatedAt,
		); err != nil {
			return fmt.Errorf("failed to append record: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}
	return nil
}

func InsertEventDesignationChDataIntoClickHouse(clickhouseConn driver.Conn, eventDesignationRecords []EventDesignationChRecord, numWorkers int) error {
	if len(eventDesignationRecords) == 0 {
		return nil
	}

	if numWorkers <= 1 {
		return InsertEventDesignationChDataSingleWorker(clickhouseConn, eventDesignationRecords)
	}

	batchSize := len(eventDesignationRecords) / numWorkers
	if batchSize == 0 {
		batchSize = 1
	}

	var wg sync.WaitGroup
	errorsChan := make(chan error, numWorkers)
	semaphore := make(chan struct{}, numWorkers)

	for i := 0; i < numWorkers; i++ {
		start := i * batchSize
		end := start + batchSize
		if i == numWorkers-1 {
			end = len(eventDesignationRecords)
		}

		if start >= len(eventDesignationRecords) {
			break
		}

		wg.Add(1)
		semaphore <- struct{}{}
		go func(start, end int) {
			defer wg.Done()
			defer func() { <-semaphore }()
			batch := eventDesignationRecords[start:end]
			if err := InsertEventDesignationChDataSingleWorker(clickhouseConn, batch); err != nil {
				errorsChan <- err
			}
		}(start, end)
	}

	wg.Wait()
	close(errorsChan)

	for err := range errorsChan {
		if err != nil {
			return err
		}
	}

	return nil
}

func InsertEventDesignationChDataSingleWorker(clickhouseConn driver.Conn, eventDesignationRecords []EventDesignationChRecord) error {
	if len(eventDesignationRecords) == 0 {
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
		INSERT INTO testing_db.event_designation_temp (
			event_id, edition_id, sourceVisitorId, designation_id, designation_uuid, display_name, department, role, total_visitors, version, last_updated_at
		)
	`)
	if err != nil {
		log.Printf("ERROR: Failed to prepare ClickHouse batch for event_designation_ch: %v", err)
		return fmt.Errorf("failed to prepare ClickHouse batch: %v", err)
	}

	for _, record := range eventDesignationRecords {
		uuidStr := "nil"
		if record.DesignationUUID != nil {
			uuidStr = *record.DesignationUUID
		}

		err := batch.Append(
			record.EventID,         // event_id: UInt32
			record.EditionID,       // edition_id: UInt32
			record.SourceVisitorID, // sourceVisitorId: UInt32
			record.DesignationID,   // designation_id: UInt32
			record.DesignationUUID, // designation_uuid: Nullable(UUID)
			record.DisplayName,     // display_name: LowCardinality(String)
			record.Department,      // department: LowCardinality(String)
			record.Role,            // role: LowCardinality(String)
			record.TotalVisitors,   // total_visitors: UInt32
			record.Version,         // version: UInt32 DEFAULT 1
			record.LastUpdatedAt,   // last_updated_at: DateTime
		)
		if err != nil {
			log.Printf("ERROR: Failed to append record to batch: %v", err)
			log.Printf("ERROR Record data: EventID=%d, EditionID=%d, SourceVisitorID=%d, DesignationID=%d, DesignationUUID=%s",
				record.EventID, record.EditionID, record.SourceVisitorID, record.DesignationID, uuidStr)
			return fmt.Errorf("failed to append record to batch: %v", err)
		}
	}

	if err := batch.Send(); err != nil {
		log.Printf("ERROR: Failed to send ClickHouse batch: %v", err)
		return fmt.Errorf("failed to send ClickHouse batch: %v", err)
	}

	log.Printf("OK: Successfully inserted %d event_designation_ch records", len(eventDesignationRecords))
	return nil
}

const defaultDesignationBatchSize = 5000

func ProcessEventDesignationOnly(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) {
	log.Println("=== Starting event_designation_ch ONLY Processing ===")

	batchSize := config.BatchSize
	if batchSize <= 0 {
		batchSize = defaultDesignationBatchSize
		log.Printf("Using default batch size: %d (event_visitor rows per batch)", batchSize)
	} else {
		log.Printf("Using batch size: %d (event_visitor rows per batch, from -batch flag)", batchSize)
	}

	log.Printf("Processing by event_visitor.id (ID-based keyset pagination) - bounded memory")

	batchNum := 0
	lastID := int64(0)

	for {
		batchNum++
		batchData, newLastID, err := fetchEventDesignationChDataByIDBatch(mysqlDB, lastID, batchSize)
		if err != nil {
			log.Fatalf("Failed to fetch event_visitor batch %d: %v", batchNum, err)
		}
		if len(batchData) == 0 {
			log.Printf("No more event_visitor rows, done after %d batches", batchNum-1)
			break
		}

		records := ConvertToEventDesignationChRecords(batchData, mysqlDB)
		if len(records) > 0 {
			log.Printf("Batch %d: %d raw visitors -> %d aggregated records (lastID=%d)", batchNum, len(batchData), len(records), newLastID)

			attemptCount := 0
			insertErr := shared.RetryWithBackoff(
				func() error {
					if attemptCount > 0 {
						now := time.Now().Format("2006-01-02 15:04:05")
						for i := range records {
							records[i].LastUpdatedAt = now
						}
					}
					attemptCount++
					return InsertEventDesignationChDataIntoClickHouse(clickhouseConn, records, config.ClickHouseWorkers)
				},
				3,
			)
			if insertErr != nil {
				log.Fatalf("Batch %d insert failed: %v", batchNum, insertErr)
			}
		}

		lastID = newLastID
		if len(batchData) < batchSize {
			break
		}
	}

	log.Println("=== Event Designation Processing Complete ===")
}

// fetchEventDesignationChDataByIDBatch fetches event_visitor rows using ID-based keyset pagination.
// Returns raw rows and the max ID seen (for next batch). Use lastID=0 for first batch.
func fetchEventDesignationChDataByIDBatch(db *sql.DB, lastID int64, batchSize int) ([]map[string]interface{}, int64, error) {
	query := `
		SELECT
			ev.id,
			ev.event,
			ev.edition,
			ev.designation_id,
			d.created
		FROM event_visitor ev
		JOIN designation d ON ev.designation_id = d.id
		WHERE ev.evisitor = 0
		  AND ev.published = 1
		  AND ev.designation_id IS NOT NULL
		  AND ev.id > ?
		ORDER BY ev.id
		LIMIT ?
	`
	rows, err := db.Query(query, lastID, batchSize)
	if err != nil {
		return nil, lastID, fmt.Errorf("failed to query event_visitor for designation data: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, lastID, fmt.Errorf("failed to get columns: %w", err)
	}

	var results []map[string]interface{}
	var maxID int64 = lastID
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			log.Printf("Error scanning event_visitor row for designation: %v", err)
			continue
		}
		row := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			if val != nil {
				row[col] = val
			} else {
				row[col] = nil
			}
		}
		results = append(results, row)
		if idVal, ok := row["id"]; ok && idVal != nil {
			if idInt := rowIDToInt64(idVal); idInt > maxID {
				maxID = idInt
			}
		}
	}
	return results, maxID, rows.Err()
}

func rowIDToInt64(v interface{}) int64 {
	switch val := v.(type) {
	case int64:
		return val
	case int32:
		return int64(val)
	case []uint8:
		n, _ := strconv.ParseInt(string(val), 10, 64)
		return n
	default:
		return 0
	}
}

// Used for incremental sync.
func BuildEventDesignationChRecordsForEventEditionPairs(db *sql.DB, pairs []EventEditionPair) ([]EventDesignationChRecord, error) {
	rawData, err := buildEventDesignationChDataForEventEditionPairs(db, pairs)
	if err != nil {
		return nil, err
	}
	return ConvertToEventDesignationChRecords(rawData, db), nil
}

func buildEventDesignationChDataForEventEditionPairs(db *sql.DB, pairs []EventEditionPair) ([]map[string]interface{}, error) {
	if len(pairs) == 0 {
		return nil, nil
	}
	var tupleStrs []string
	for _, p := range pairs {
		tupleStrs = append(tupleStrs, fmt.Sprintf("(%d, %d)", p.EventID, p.EditionID))
	}
	query := fmt.Sprintf(`
		SELECT ev.id, ev.event, ev.edition, ev.designation_id, d.created
		FROM event_visitor ev
		JOIN designation d ON ev.designation_id = d.id
		WHERE ev.evisitor = 0
		  AND ev.published = 1
		  AND ev.designation_id IS NOT NULL
		  AND (ev.event, ev.edition) IN (%s)
		ORDER BY ev.event, ev.edition, ev.designation_id, ev.id
	`, strings.Join(tupleStrs, ","))
	log.Printf("[Query] %s", strings.TrimSpace(query))
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
			row[col] = values[i]
		}
		results = append(results, row)
	}
	return results, rows.Err()
}

func BuildEventDesignationMigrationData(mysqlDB *sql.DB, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT
			ev.id,
			ev.event,
			ev.edition,
			ev.designation_id,
			d.created
		FROM event_visitor ev JOIN designation d ON ev.designation_id = d.id
		WHERE
			ev.evisitor = 0
			AND ev.published = 1
			AND ev.designation_id IS NOT NULL
			AND ev.id >= %d AND ev.id <= %d
		ORDER BY ev.id
		LIMIT %d`, startID, endID, batchSize)

	rows, err := mysqlDB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query event_visitor for designation data: %v", err)
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
			log.Printf("Error scanning event_visitor row for designation: %v", err)
			continue
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
