package utils

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"seeders/shared"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// safeConvertToString safely converts a value to string
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

// EventDesignationChRecord represents the structure for event_designation_ch table
type EventDesignationChRecord struct {
	EventID       uint32 `ch:"event_id"`
	EditionID     uint32 `ch:"edition_id"`
	DesignationID uint32 `ch:"designation_id"`
	DisplayName   string `ch:"display_name"`
	Department    string `ch:"department"`
	Role          string `ch:"role"`
	TotalVisitors uint32 `ch:"total_visitors"`
	Version       uint32 `ch:"version"`
}

// DesignationData represents the designation information from MySQL
type DesignationData struct {
	DisplayName string
	Department  string
	Role        string
}

// FetchDesignationDisplayNameData fetches designation display names, department, and role from designation table
func FetchDesignationDisplayNameData(db *sql.DB, designationIDs []uint32) (map[uint32]DesignationData, error) {
	if len(designationIDs) == 0 {
		return make(map[uint32]DesignationData), nil
	}

	// Convert to string slice for query building
	idStrings := make([]string, len(designationIDs))
	for i, id := range designationIDs {
		idStrings[i] = fmt.Sprintf("%d", id)
	}

	// Use direct string formatting like other tables
	query := fmt.Sprintf(`
		SELECT id, display_name, department, role
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
		if err := rows.Scan(&id, &displayName, &department, &role); err != nil {
			log.Printf("Error scanning designation row: %v", err)
			continue
		}
		designationMap[id] = DesignationData{
			DisplayName: displayName.String,
			Department:  department.String,
			Role:        role.String,
		}
	}

	log.Printf("Fetched %d designation records with display_name, department, and role", len(designationMap))
	return designationMap, nil
}

// ConvertToEventDesignationChRecords converts MySQL data to ClickHouse records
func ConvertToEventDesignationChRecords(mysqlData []map[string]interface{}, db *sql.DB) []EventDesignationChRecord {
	// First, aggregate the individual visitor records
	aggregatedData := make(map[string]map[string]interface{})

	log.Printf("Processing %d raw visitor records for aggregation", len(mysqlData))

	for _, row := range mysqlData {
		eventID := safeConvertToString(row["event"])
		editionID := safeConvertToString(row["edition"])
		designationID := safeConvertToString(row["designation_id"])

		// Skip rows with empty or invalid data
		if eventID == "" || editionID == "" || designationID == "" {
			continue
		}

		key := fmt.Sprintf("%s_%s_%s", eventID, editionID, designationID)

		if _, exists := aggregatedData[key]; !exists {
			aggregatedData[key] = map[string]interface{}{
				"event":          row["event"],
				"edition":        row["edition"],
				"designation_id": row["designation_id"],
				"total_visitors": 0,
			}
		}

		// Increment the count
		if count, ok := aggregatedData[key]["total_visitors"].(int); ok {
			aggregatedData[key]["total_visitors"] = count + 1
		}
	}

	// Convert aggregated data to slice
	var aggregatedSlice []map[string]interface{}
	for _, data := range aggregatedData {
		aggregatedSlice = append(aggregatedSlice, data)
	}

	log.Printf("Aggregated %d raw records into %d unique event-edition-designation combinations", len(mysqlData), len(aggregatedSlice))

	// Now process the aggregated data similar to before
	var records []EventDesignationChRecord

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
		}
	} else {
		designationData = make(map[uint32]DesignationData)
	}

	for _, row := range aggregatedSlice {
		record := EventDesignationChRecord{
			Version: 1,
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

		if designationID, ok := row["designation_id"]; ok && designationID != nil {
			if designationIDVal, ok := designationID.(int64); ok {
				designationIDUint := uint32(designationIDVal)
				record.DesignationID = designationIDUint
				if designationInfo, exists := designationData[designationIDUint]; exists {
					record.DisplayName = designationInfo.DisplayName
					record.Department = designationInfo.Department
					record.Role = designationInfo.Role
				} else {
					record.DisplayName = ""
					record.Department = ""
					record.Role = ""
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

// InsertEventDesignationChDataIntoClickHouse inserts event designation data into ClickHouse with parallel workers
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

	// Check if any errors occurred
	for err := range errorsChan {
		if err != nil {
			return err
		}
	}

	return nil
}

// InsertEventDesignationChDataSingleWorker inserts event designation data into ClickHouse
func InsertEventDesignationChDataSingleWorker(clickhouseConn driver.Conn, eventDesignationRecords []EventDesignationChRecord) error {
	if len(eventDesignationRecords) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	batch, err := clickhouseConn.PrepareBatch(ctx, `
		INSERT INTO testing_db.event_designation_ch (
			event_id, edition_id, designation_id, display_name, department, role, total_visitors, version
		)
	`)
	if err != nil {
		log.Printf("ERROR: Failed to prepare ClickHouse batch for event_designation_ch: %v", err)
		return fmt.Errorf("failed to prepare ClickHouse batch: %v", err)
	}

	for _, record := range eventDesignationRecords {
		err := batch.Append(
			record.EventID,       // event_id: UInt32
			record.EditionID,     // edition_id: UInt32
			record.DesignationID, // designation_id: UInt32
			record.DisplayName,   // display_name: LowCardinality(String)
			record.Department,    // department: LowCardinality(String)
			record.Role,          // role: LowCardinality(String)
			record.TotalVisitors, // total_visitors: UInt32
			record.Version,       // version: UInt32 DEFAULT 1
		)
		if err != nil {
			log.Printf("ERROR: Failed to append record to batch: %v", err)
			log.Printf("Record data: EventID=%d, EditionID=%d, DesignationID=%d, DisplayName=%s, Department=%s, Role=%s, TotalVisitors=%d, Version=%d",
				record.EventID, record.EditionID, record.DesignationID, record.DisplayName, record.Department, record.Role, record.TotalVisitors, record.Version)
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

func ProcessEventDesignationOnly(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) {
	log.Println("=== Starting event_designation_ch ONLY Processing ===")

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

	log.Printf("Processing event_designation_ch data in %d chunks with chunk size: %d", config.NumChunks, chunkSize)

	var wg sync.WaitGroup
	semaphore := make(chan struct{}, config.NumWorkers)

	log.Printf("Starting %d chunks with %d workers", config.NumChunks, config.NumWorkers)

	for i := 0; i < config.NumChunks; i++ {
		startID := minID + (i * chunkSize)
		endID := startID + chunkSize - 1
		if i == config.NumChunks-1 {
			endID = maxID
		}

		if i > 0 {
			delay := 3 * time.Second
			log.Printf("Waiting %v before launching event_designation_ch chunk %d...", delay, i+1)
			time.Sleep(delay)
		}

		log.Printf("Launching event_designation_ch chunk %d with ID range %d-%d", i+1, startID, endID)

		wg.Add(1)
		semaphore <- struct{}{}
		go func(chunkNum, start, end int) {
			defer wg.Done()
			defer func() { <-semaphore }()
			ProcessEventDesignationChunk(mysqlDB, clickhouseConn, config, start, end, chunkNum)
		}(i+1, startID, endID)
	}

	wg.Wait()
	log.Println("=== Event Designation Processing Complete ===")
}

func ProcessEventDesignationChunk(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config, startID, endID int, chunkNum int) {
	log.Printf("Processing event_designation_ch chunk %d: ID range %d-%d", chunkNum, startID, endID)

	totalRecords := endID - startID + 1
	processed := 0

	// Use batching within the chunk - following the SAME pattern as other tables
	offset := 0
	for {
		batchData, err := BuildEventDesignationMigrationData(mysqlDB, startID, endID, config.BatchSize)
		if err != nil {
			log.Printf("EventDesignation chunk %d batch error: %v", chunkNum, err)
			return
		}

		if len(batchData) == 0 {
			log.Printf("EventDesignation chunk %d: No more data to process, breaking loop", chunkNum)
			break
		}

		processed += len(batchData)
		progress := float64(processed) / float64(totalRecords) * 100
		log.Printf("EventDesignation chunk %d: Retrieved %d records in batch (%.1f%% complete)", chunkNum, len(batchData), progress)

		records := ConvertToEventDesignationChRecords(batchData, mysqlDB)

		if len(records) > 0 {
			log.Printf("EventDesignation chunk %d: Attempting to insert %d records into event_designation_ch...", chunkNum, len(records))

			insertErr := shared.RetryWithBackoff(
				func() error {
					return InsertEventDesignationChDataIntoClickHouse(clickhouseConn, records, config.ClickHouseWorkers)
				},
				3,
				fmt.Sprintf("event_designation_ch insertion for chunk %d", chunkNum),
			)

			if insertErr != nil {
				log.Printf("EventDesignation chunk %d insertion failed: %v", chunkNum, insertErr)
				return
			}

			log.Printf("EventDesignation chunk %d: Successfully inserted %d records", chunkNum, len(records))
		}

		// Get the last ID from this batch for next iteration - EXACT SAME PATTERN AS OTHER TABLES
		if len(batchData) > 0 {
			lastID := batchData[len(batchData)-1]["id"]
			if lastID != nil {
				// Update startID for next batch within this chunk - DIRECT MODIFICATION LIKE OTHER TABLES
				if id, ok := lastID.(int64); ok {
					startID = int(id) + 1
				}
			}
		}

		offset += len(batchData)
		if len(batchData) < config.BatchSize {
			break
		}
	}

	log.Printf("EventDesignation chunk %d: Completed processing %d records", chunkNum, processed)
}

func BuildEventDesignationMigrationData(mysqlDB *sql.DB, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	// Use the SAME pattern as other tables - direct query with string formatting
	query := fmt.Sprintf(`
		SELECT
			id,
			event,
			edition,
			designation_id
		FROM event_visitor ev
		WHERE
			ev.evisitor = 0
			AND ev.published = 1
			AND ev.designation_id IS NOT NULL
			AND ev.id >= %d AND ev.id <= %d
		ORDER BY id
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
