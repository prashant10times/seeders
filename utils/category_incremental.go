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

const categoryDeleteBatchSize = 500

// categoryEventPair represents (category, event) for diff computation.
type categoryEventPair struct {
	Category uint32
	Event    uint32
}

// ProcessIncrementalEventCategory syncs event_category_ch incrementally using event-level reconciliation.
// Scope: event_ids from event_category where modified >= yesterday.
// For each event in scope: DELETE rows in ClickHouse not in MySQL, INSERT all current MySQL rows.
func ProcessIncrementalEventCategory(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) error {
	startTime := time.Now()
	log.Println("=== Starting Incremental Event Category Sync ===")
	log.Printf("Log file: %s", shared.IncrementalLogFile)

	shared.WriteIncrementalLog("")
	shared.WriteIncrementalLog("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	shared.WriteIncrementalLog(fmt.Sprintf("[%s] INCREMENTAL EVENT CATEGORY SYNC STARTED", startTime.Format("2006-01-02 15:04:05")))
	shared.WriteIncrementalLog("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	scopeEventIDs, err := fetchIncrementalScopeEventCategory(mysqlDB)
	if err != nil {
		return fmt.Errorf("fetch incremental scope: %w", err)
	}
	if len(scopeEventIDs) == 0 {
		log.Println("No events with modified event_category since yesterday, nothing to sync")
		shared.WriteIncrementalLog("SCOPE: No events with modified event_category since yesterday. Nothing to sync.")
		shared.WriteIncrementalLog(fmt.Sprintf("[%s] INCREMENTAL EVENT CATEGORY SYNC COMPLETED (no changes)", time.Now().Format("2006-01-02 15:04:05")))
		return nil
	}

	log.Printf("Incremental scope: %d event IDs to reconcile", len(scopeEventIDs))
	shared.WriteIncrementalLog("")
	shared.WriteIncrementalLog(fmt.Sprintf("1. SCOPE: %d event IDs (event_category modified since yesterday)", len(scopeEventIDs)))

	mysqlRecords, err := BuildEventCategoryEventChRecordsForEventIDs(mysqlDB, scopeEventIDs)
	if err != nil {
		return fmt.Errorf("build MySQL records: %w", err)
	}
	if len(mysqlRecords) == 0 {
		log.Println("No records built from MySQL, nothing to sync")
		shared.WriteIncrementalLog("")
		shared.WriteIncrementalLog("2. MYSQL RECORDS: 0")
		shared.WriteIncrementalLog(fmt.Sprintf("[%s] INCREMENTAL EVENT CATEGORY SYNC COMPLETED (no records)", time.Now().Format("2006-01-02 15:04:05")))
		return nil
	}

	shared.WriteIncrementalLog("")
	shared.WriteIncrementalLog(fmt.Sprintf("2. MYSQL RECORDS: %d (current state for scoped events)", len(mysqlRecords)))

	tableName := shared.GetTableNameWithDB(shared.GetClickHouseTableName("event_category_ch", config), config)

	chRowsByEvent, err := fetchEventCategoryChRowsForEventIDs(clickhouseConn, scopeEventIDs, tableName)
	if err != nil {
		return fmt.Errorf("fetch ClickHouse rows: %w", err)
	}

	toDelete, toInsert := computeEventCategoryDiff(mysqlRecords, chRowsByEvent)

	shared.WriteIncrementalLog("")
	shared.WriteIncrementalLog("3. DIFF:")
	shared.WriteIncrementalLog(fmt.Sprintf("   Rows to DELETE (in CH but not in MySQL): %d", len(toDelete)))
	shared.WriteIncrementalLog(fmt.Sprintf("   Rows to INSERT (current MySQL state): %d", len(toInsert)))

	if len(toDelete) > 0 {
		nativeConn, err := shared.SetupNativeProtocolConnectionForOptimize(config)
		if err != nil {
			return fmt.Errorf("create native connection for DELETE: %w", err)
		}
		defer nativeConn.Close()
		if err := deleteEventCategoryRowsByPrimaryKey(nativeConn, toDelete, tableName); err != nil {
			return fmt.Errorf("delete obsolete rows: %w", err)
		}
		shared.WriteIncrementalLog("")
		shared.WriteIncrementalLog(fmt.Sprintf("4. DELETE: %d rows deleted", len(toDelete)))
	} else {
		shared.WriteIncrementalLog("")
		shared.WriteIncrementalLog("4. DELETE: 0 rows (none required)")
	}

	if len(toInsert) > 0 {
		insertQuery := fmt.Sprintf(`INSERT INTO %s (category, category_uuid, event, name, slug, published, short_name, is_group, created, version, last_updated_at)`, tableName)
		log.Printf("[Query] %s", insertQuery)
		log.Printf("[INSERT] Inserting %d records into %s", len(toInsert), tableName)
		shared.WriteIncrementalLog("")
		shared.WriteIncrementalLog(fmt.Sprintf("5. INSERT: %d records into event_category_ch", len(toInsert)))
		if err := InsertEventCategoryEventChDataIntoTable(clickhouseConn, toInsert, tableName, config.ClickHouseWorkers); err != nil {
			return fmt.Errorf("insert records: %w", err)
		}
		log.Printf("Inserted %d records into event_category_ch", len(toInsert))
	} else {
		shared.WriteIncrementalLog("")
		shared.WriteIncrementalLog("5. INSERT: 0 (no records to insert)")
	}

	log.Printf("Running OPTIMIZE on event_category_ch...")
	optimizeErr := shared.OptimizeSingleTable(clickhouseConn, "event_category_ch", config, "")
	if optimizeErr != nil {
		log.Printf("WARNING: OPTIMIZE failed: %v", optimizeErr)
		shared.WriteIncrementalLog("")
		shared.WriteIncrementalLog(fmt.Sprintf("6. OPTIMIZE: WARNING - Failed: %v", optimizeErr))
	} else {
		log.Println("OPTIMIZE completed successfully")
		shared.WriteIncrementalLog("")
		shared.WriteIncrementalLog("6. OPTIMIZE: Completed successfully")
	}

	endTime := time.Now()
	duration := endTime.Sub(startTime)
	shared.WriteIncrementalLog("")
	shared.WriteIncrementalLog("SUMMARY:")
	shared.WriteIncrementalLog(fmt.Sprintf("   Events reconciled: %d", len(scopeEventIDs)))
	shared.WriteIncrementalLog(fmt.Sprintf("   Rows deleted: %d", len(toDelete)))
	shared.WriteIncrementalLog(fmt.Sprintf("   Rows inserted: %d", len(toInsert)))
	shared.WriteIncrementalLog(fmt.Sprintf("   Duration: %v", duration.Round(time.Millisecond)))
	shared.WriteIncrementalLog(fmt.Sprintf("[%s] INCREMENTAL EVENT CATEGORY SYNC COMPLETED", endTime.Format("2006-01-02 15:04:05")))
	shared.WriteIncrementalLog("")

	log.Println("=== Incremental Event Category Sync Completed ===")
	return nil
}

func fetchIncrementalScopeEventCategory(db *sql.DB) ([]int64, error) {
	query := `
		SELECT DISTINCT event
		FROM event_category
		WHERE modified >= CURDATE() - INTERVAL 1 DAY
		ORDER BY event
	`
	log.Printf("[Query] %s", strings.TrimSpace(query))
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var eventIDs []int64
	for rows.Next() {
		var eventID int64
		if err := rows.Scan(&eventID); err != nil {
			return nil, err
		}
		eventIDs = append(eventIDs, eventID)
	}
	return eventIDs, rows.Err()
}

// fetchEventCategoryChRowsForEventIDs returns map[eventID][]categoryEventPair - all (category, event) in ClickHouse for the given events.
func fetchEventCategoryChRowsForEventIDs(conn driver.Conn, eventIDs []int64, tableName string) (map[int64][]categoryEventPair, error) {
	if len(eventIDs) == 0 {
		return make(map[int64][]categoryEventPair), nil
	}

	placeholders := make([]string, len(eventIDs))
	args := make([]interface{}, len(eventIDs))
	for i, id := range eventIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(
		`SELECT category, event FROM %s FINAL WHERE event IN (%s)`,
		tableName,
		strings.Join(placeholders, ","),
	)
	log.Printf("[Query] %s", query)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[int64][]categoryEventPair)
	for rows.Next() {
		var category, eventID uint32
		if err := rows.Scan(&category, &eventID); err != nil {
			return nil, err
		}
		eid := int64(eventID)
		result[eid] = append(result[eid], categoryEventPair{Category: category, Event: eventID})
	}
	return result, rows.Err()
}

// computeEventCategoryDiff: toDelete = CH_set - MySQL_set per event, toInsert = all MySQL records.
func computeEventCategoryDiff(mysqlRecords []EventCategoryEventChRecord, chRowsByEvent map[int64][]categoryEventPair) (toDelete []categoryEventPair, toInsert []EventCategoryEventChRecord) {
	mysqlSetByEvent := make(map[int64]map[string]bool) // eventID -> set of "category|event"
	for _, rec := range mysqlRecords {
		eid := int64(rec.Event)
		if mysqlSetByEvent[eid] == nil {
			mysqlSetByEvent[eid] = make(map[string]bool)
		}
		key := fmt.Sprintf("%d|%d", rec.Category, rec.Event)
		mysqlSetByEvent[eid][key] = true
	}

	for eid, chPairs := range chRowsByEvent {
		mysqlSet := mysqlSetByEvent[eid]
		for _, p := range chPairs {
			key := fmt.Sprintf("%d|%d", p.Category, p.Event)
			if mysqlSet == nil || !mysqlSet[key] {
				toDelete = append(toDelete, p)
			}
		}
	}

	toInsert = mysqlRecords
	return toDelete, toInsert
}

func deleteEventCategoryRowsByPrimaryKey(conn driver.Conn, pairs []categoryEventPair, tableName string) error {
	if len(pairs) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	for batchStart := 0; batchStart < len(pairs); batchStart += categoryDeleteBatchSize {
		batchEnd := batchStart + categoryDeleteBatchSize
		if batchEnd > len(pairs) {
			batchEnd = len(pairs)
		}
		batch := pairs[batchStart:batchEnd]

		var tuples []string
		for _, p := range batch {
			tuples = append(tuples, fmt.Sprintf("(%d, %d)", p.Category, p.Event))
		}

		deleteQuery := fmt.Sprintf(
			`ALTER TABLE %s DELETE WHERE (category, event) IN (%s) SETTINGS mutations_sync = 1`,
			tableName, strings.Join(tuples, ","),
		)
		log.Printf("[Query] %s", deleteQuery)

		log.Printf("[EventCategory DELETE] Batch %d-%d (%d pairs)", batchStart+1, batchEnd, len(batch))
		shared.WriteIncrementalLog(fmt.Sprintf("   [DELETE] Batch %d-%d: %d pairs", batchStart+1, batchEnd, len(batch)))

		if err := conn.Exec(ctx, deleteQuery); err != nil {
			return fmt.Errorf("ALTER DELETE batch %d-%d: %w", batchStart+1, batchEnd, err)
		}
	}

	return nil
}
