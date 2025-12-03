// event_type_priority = {
//     1 : {
//         "priority" : 1,
//         "group" : "B2B"
//         },
//     2 : {
//         "priority" : 2,
//         "group" : "B2B"
//         },
//     3 : {
//         "priority" : 3,
//         "group" : "B2B"
//         },
//     5 : {
//         "priority" : 4,
//         "group" : "B2C"
//         },
//     7 : {
//         "priority" : 5,
//         "group" : "B2C"
//         },
//     6 : {
//         "priority" : 6,
//         "group" : "B2C"
//         },
//     12 : {
//         "priority" : 7,
//         "group" : "B2C"
//         },
//     13 : {
//         "priority" : 8,
//         "group" : "B2C"
//         },
// }

// event_type_groups = {
//     1 : ['business','attended'],
//     2 : ['business','attended'],
//     3 : ['business','attended'],
//     5 : ['social','attended'],
//     6 : ['social','attended'],
//     7 : ['social','attended'],
//     12 : ['social','attended'],
//     13 : ['social','attended'],
// }

// holiday_event_types = [
//     {
//         "name" : "Holiday",
//         "slug" : "holiday",
//     },
//     {
//         "name" : "Local Holiday",
//         "slug" : "local-holiday",
//     },
//     {
//         "name" : "National Holiday",
//         "slug" : "national-holiday",
//     },
//     {
//         "name" : "International Holiday",
//         "slug" : "international-holiday",
//     },
//     {
//         "name" : "Observance Holiday",
//         "slug" : "observance-holiday",
//     },
//     {
//         "name" : "Cultural Holiday",
//         "slug" : "cultural-holiday",
//     },
//     {
//         "name" : "Religious Holiday",
//         "slug" : "religious-holiday",
//     }
// ]

// holiday_types_mapping = {
//     "local" : "local-holiday",
//     "national" : "national-holiday",
//     "international" : "international-holiday",
//     "observance" : "observance-holiday",
//     "religious" : "religious-holiday",
//     "cultural" : "cultural-holiday",
// }

package utils

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

type EventTypeEventChRecord struct {
	EventTypeID    uint32   `ch:"eventtype_id"`
	EventTypeUUID  string   `ch:"eventtype_uuid"`
	EventID        uint32   `ch:"event_id"`
	Published      int8     `ch:"published"`
	Name           string   `ch:"name"`
	Slug           string   `ch:"slug"`
	EventAudience  uint16   `ch:"event_audience"`
	EventGroupType string   `ch:"eventGroupType"` // LowCardinality(String) - hardcoded to 'ATTENDED'
	Groups         []string `ch:"groups"`         // Array(String) - empty array when not found
	Priority       *int8    `ch:"priority"`       // Nullable(Int8)
	Created        string   `ch:"created"`
	Version        uint32   `ch:"version"`
	LastUpdatedAt  string   `ch:"last_updated_at"`
}

var eventTypePriority = map[uint32]int8{
	1:  1,
	2:  2,
	3:  3,
	5:  4,
	7:  5,
	6:  6,
	12: 7,
	13: 8,
}

var eventTypeGroups = map[uint32][]string{
	1:  {"business", "attended"},
	2:  {"business", "attended"},
	3:  {"business", "attended"},
	5:  {"social", "attended"},
	6:  {"social", "attended"},
	7:  {"social", "attended"},
	12: {"social", "attended"},
	13: {"social", "attended"},
}

func getPriority(eventTypeID uint32) *int8 {
	if priority, ok := eventTypePriority[eventTypeID]; ok {
		if priority == 0 {
			return nil
		}
		return &priority
	}
	return nil
}

func getGroups(eventTypeID uint32) []string {
	if groups, ok := eventTypeGroups[eventTypeID]; ok {
		return groups
	}
	return []string{}
}

func getEventGroupType(eventTypeID uint32) string {
	if _, ok := eventTypePriority[eventTypeID]; ok {
		return "ATTENDED"
	}
	return "NON_ATTENDED"
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

func processEventTypeEventChChunk(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config, startID, endID int, chunkNum int, results chan<- string) {
	log.Printf("Processing event_type_ch chunk %d: ID range %d-%d", chunkNum, startID, endID)

	totalRecords := endID - startID + 1
	processed := 0

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
		now := time.Now().Format("2006-01-02 15:04:05")
		for _, record := range batchData {
			eventTypeID := shared.ConvertToUInt32(record["eventtype_id"])
			priority := getPriority(eventTypeID)
			groups := getGroups(eventTypeID)
			eventGroupType := getEventGroupType(eventTypeID)

			eventTypeEventChRecord := EventTypeEventChRecord{
				EventTypeID:    eventTypeID,
				EventTypeUUID:  shared.GenerateUUIDFromString(fmt.Sprintf("%d", eventTypeID)),
				EventID:        shared.ConvertToUInt32(record["event_id"]),
				Published:      shared.ConvertToInt8(record["published"]),
				Name:           shared.ConvertToString(record["name"]),
				Slug:           shared.ConvertToString(record["slug"]),
				EventAudience:  shared.SafeConvertToUInt16(record["event_audience"]),
				EventGroupType: eventGroupType,
				Groups:         groups,
				Priority:       priority,
				Created:        shared.SafeConvertToDateTimeString(record["created"]),
				Version:        1,
				LastUpdatedAt:  now,
			}

			eventTypeEventChRecords = append(eventTypeEventChRecords, eventTypeEventChRecord)
		}

		if len(eventTypeEventChRecords) > 0 {
			log.Printf("EventTypeEventCh chunk %d: Attempting to insert %d records into event_type_ch...", chunkNum, len(eventTypeEventChRecords))

			attemptCount := 0
			insertErr := shared.RetryWithBackoff(
				func() error {
					if attemptCount > 0 {
						now := time.Now().Format("2006-01-02 15:04:05")
						for i := range eventTypeEventChRecords {
							eventTypeEventChRecords[i].LastUpdatedAt = now
						}
						log.Printf("EventTypeEventCh chunk %d: Updated last_updated_at for retry attempt %d", chunkNum, attemptCount+1)
					}
					attemptCount++
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

		if len(batchData) > 0 {
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

	log.Printf("Checking ClickHouse connection health before inserting %d event_type_ch records", len(eventTypeEventChRecords))
	connectionCheckErr := shared.RetryWithBackoff(
		func() error {
			return shared.CheckClickHouseConnectionAlive(clickhouseConn)
		},
		3,
		"ClickHouse connection health check for event_type_ch",
	)
	if connectionCheckErr != nil {
		return fmt.Errorf("ClickHouse connection is not alive after retries: %w", connectionCheckErr)
	}
	log.Printf("ClickHouse connection is alive, proceeding with event_type_ch batch insert")

	insertErr := shared.RetryWithBackoff(
		func() error {
			return insertEventTypeEventChBatch(clickhouseConn, eventTypeEventChRecords)
		},
		3,
		"event_type_ch batch insert",
	)

	if insertErr != nil {
		return fmt.Errorf("failed to insert event_type_ch batch after retries: %w", insertErr)
	}

	log.Printf("OK: Successfully inserted %d event_type_ch records", len(eventTypeEventChRecords))
	return nil
}

func insertEventTypeEventChBatch(clickhouseConn driver.Conn, eventTypeEventChRecords []EventTypeEventChRecord) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	log.Printf("Preparing ClickHouse batch for %d event_type_ch records", len(eventTypeEventChRecords))

	batch, err := clickhouseConn.PrepareBatch(ctx, `
		INSERT INTO event_type_temp (
			eventtype_id, eventtype_uuid, event_id, published, name, slug, event_audience, eventGroupType, groups, priority, created, version,
			alert_id, alert_level, alert_type, alert_start_date, alert_end_date, last_updated_at
		)
	`)
	if err != nil {
		log.Printf("ERROR: Failed to prepare ClickHouse batch for event_type_ch: %v", err)
		if strings.Contains(err.Error(), "EOF") {
			return fmt.Errorf("connection error (table may not exist or connection dropped): %v. Please verify table 'event_type_ch' exists with alert fields", err)
		}
		return fmt.Errorf("failed to prepare ClickHouse batch: %v", err)
	}

	for _, record := range eventTypeEventChRecords {
		err := batch.Append(
			record.EventTypeID,    // eventtype_id: UInt32
			record.EventTypeUUID,  // eventtype_uuid: UUID
			record.EventID,        // event_id: UInt32
			record.Published,      // published: Int8
			record.Name,           // name: LowCardinality(String)
			record.Slug,           // slug: String
			record.EventAudience,  // event_audience: UInt16
			record.EventGroupType, // eventGroupType: LowCardinality(String)
			record.Groups,         // groups: Array(String)
			record.Priority,       // priority: Nullable(Int8)
			record.Created,        // created: DateTime
			record.Version,        // version: UInt32 DEFAULT 1
			nil,                   // alert_id: Nullable(UUID)
			nil,                   // alert_level: Nullable(String)
			nil,                   // alert_type: Nullable(String)
			nil,                   // alert_start_date: Nullable(Date)
			nil,                   // alert_end_date: Nullable(Date)
			record.LastUpdatedAt,  // last_updated_at: DateTime
		)
		if err != nil {
			log.Printf("ERROR: Failed to append event_type record to batch: %v", err)
			return fmt.Errorf("failed to append record to batch: %v", err)
		}
	}

	log.Printf("Sending ClickHouse batch with %d event_type_ch records", len(eventTypeEventChRecords))
	if err := batch.Send(); err != nil {
		log.Printf("ERROR: Failed to send ClickHouse batch: %v", err)
		return fmt.Errorf("failed to send ClickHouse batch: %v", err)
	}

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
			et.url as slug,
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
