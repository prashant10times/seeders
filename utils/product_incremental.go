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

const eventProductDeleteBatchSize = 500

// eventProductTuple represents (event, edition, id) for diff computation - matches ORDER BY (event, edition, id).
type eventProductTuple struct {
	Event   uint32
	Edition uint32
	ID      uint32
}

// ProcessIncrementalEventProduct syncs event_product_ch incrementally using event-level reconciliation.
// Scope: event_ids from event_products where modified >= yesterday.
// For each event in scope: DELETE rows in ClickHouse not in MySQL, INSERT all current MySQL rows.
func ProcessIncrementalEventProduct(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) error {
	startTime := time.Now()
	log.Println("=== Starting Incremental Event Product Sync ===")
	log.Printf("Log file: %s", shared.IncrementalLogFile)

	shared.WriteIncrementalLog("")
	shared.WriteIncrementalLog("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	shared.WriteIncrementalLog(fmt.Sprintf("[%s] INCREMENTAL EVENT PRODUCT SYNC STARTED", startTime.Format("2006-01-02 15:04:05")))
	shared.WriteIncrementalLog("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	scopeEventIDs, err := fetchIncrementalScopeEventProduct(mysqlDB)
	if err != nil {
		return fmt.Errorf("fetch incremental scope: %w", err)
	}
	if len(scopeEventIDs) == 0 {
		log.Println("No events with modified event_products since yesterday, nothing to sync")
		shared.WriteIncrementalLog("SCOPE: No events with modified event_products since yesterday. Nothing to sync.")
		shared.WriteIncrementalLog(fmt.Sprintf("[%s] INCREMENTAL EVENT PRODUCT SYNC COMPLETED (no changes)", time.Now().Format("2006-01-02 15:04:05")))
		return nil
	}

	log.Printf("Incremental scope: %d event IDs to reconcile", len(scopeEventIDs))
	shared.WriteIncrementalLog("")
	shared.WriteIncrementalLog(fmt.Sprintf("1. SCOPE: %d event IDs (event_products modified since yesterday)", len(scopeEventIDs)))

	mysqlRecords, err := BuildEventProductChRecordsForEventIDs(mysqlDB, scopeEventIDs)
	if err != nil {
		return fmt.Errorf("build MySQL records: %w", err)
	}
	if len(mysqlRecords) == 0 {
		log.Println("No records built from MySQL, nothing to sync")
		shared.WriteIncrementalLog("")
		shared.WriteIncrementalLog("2. MYSQL RECORDS: 0")
		shared.WriteIncrementalLog(fmt.Sprintf("[%s] INCREMENTAL EVENT PRODUCT SYNC COMPLETED (no records)", time.Now().Format("2006-01-02 15:04:05")))
		return nil
	}

	shared.WriteIncrementalLog("")
	shared.WriteIncrementalLog(fmt.Sprintf("2. MYSQL RECORDS: %d (current state for scoped events)", len(mysqlRecords)))

	tableName := shared.GetTableNameWithDB(shared.GetClickHouseTableName("event_product_ch", config), config)

	chRowsByEvent, err := fetchEventProductChRowsForEventIDs(clickhouseConn, scopeEventIDs, tableName)
	if err != nil {
		return fmt.Errorf("fetch ClickHouse rows: %w", err)
	}

	toDelete, toInsert := computeEventProductDiff(mysqlRecords, chRowsByEvent)

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
		if err := deleteEventProductRowsByPrimaryKey(nativeConn, toDelete, tableName); err != nil {
			return fmt.Errorf("delete obsolete rows: %w", err)
		}
		shared.WriteIncrementalLog("")
		shared.WriteIncrementalLog(fmt.Sprintf("4. DELETE: %d rows deleted", len(toDelete)))
	} else {
		shared.WriteIncrementalLog("")
		shared.WriteIncrementalLog("4. DELETE: 0 rows (none required)")
	}

	if len(toInsert) > 0 {
		insertQuery := fmt.Sprintf(`INSERT INTO %s (id, product_uuid, name, slug, event, edition, product_published, event_product_published, created, last_updated_at)`, tableName)
		log.Printf("[Query] %s", insertQuery)
		log.Printf("[INSERT] Inserting %d records into %s", len(toInsert), tableName)
		shared.WriteIncrementalLog("")
		shared.WriteIncrementalLog(fmt.Sprintf("5. INSERT: %d records into event_product_ch", len(toInsert)))
		if err := InsertEventProductChDataIntoTable(clickhouseConn, toInsert, tableName, config.ClickHouseWorkers); err != nil {
			return fmt.Errorf("insert records: %w", err)
		}
		log.Printf("Inserted %d records into event_product_ch", len(toInsert))
	} else {
		shared.WriteIncrementalLog("")
		shared.WriteIncrementalLog("5. INSERT: 0 (no records to insert)")
	}

	log.Printf("Running OPTIMIZE on event_product_ch...")
	optimizeErr := shared.OptimizeSingleTable(clickhouseConn, "event_product_ch", config, "")
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
	shared.WriteIncrementalLog(fmt.Sprintf("[%s] INCREMENTAL EVENT PRODUCT SYNC COMPLETED", endTime.Format("2006-01-02 15:04:05")))
	shared.WriteIncrementalLog("")

	log.Println("=== Incremental Event Product Sync Completed ===")
	return nil
}

func fetchIncrementalScopeEventProduct(db *sql.DB) ([]int64, error) {
	query := `
		SELECT DISTINCT event
		FROM event_products
		WHERE modified >= CURDATE() - INTERVAL 1 DAY
		   OR created >= CURDATE() - INTERVAL 1 DAY
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

// fetchEventProductChRowsForEventIDs returns map[eventID][]eventProductTuple - all (event, edition, id) in ClickHouse for the given events.
func fetchEventProductChRowsForEventIDs(conn driver.Conn, eventIDs []int64, tableName string) (map[int64][]eventProductTuple, error) {
	if len(eventIDs) == 0 {
		return make(map[int64][]eventProductTuple), nil
	}

	placeholders := make([]string, len(eventIDs))
	args := make([]interface{}, len(eventIDs))
	for i, id := range eventIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(
		`SELECT event, edition, id FROM %s FINAL WHERE event IN (%s)`,
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

	result := make(map[int64][]eventProductTuple)
	for rows.Next() {
		var eventID, edition, id uint32
		if err := rows.Scan(&eventID, &edition, &id); err != nil {
			return nil, err
		}
		eid := int64(eventID)
		result[eid] = append(result[eid], eventProductTuple{Event: eventID, Edition: edition, ID: id})
	}
	return result, rows.Err()
}

// computeEventProductDiff: toDelete = CH_set - MySQL_set per event, toInsert = all MySQL records.
func computeEventProductDiff(mysqlRecords []EventProductChRecord, chRowsByEvent map[int64][]eventProductTuple) (toDelete []eventProductTuple, toInsert []EventProductChRecord) {
	mysqlSetByEvent := make(map[int64]map[string]bool) // eventID -> set of "event|edition|id"
	for _, rec := range mysqlRecords {
		eid := int64(rec.Event)
		if mysqlSetByEvent[eid] == nil {
			mysqlSetByEvent[eid] = make(map[string]bool)
		}
		key := fmt.Sprintf("%d|%d|%d", rec.Event, rec.Edition, rec.ID)
		mysqlSetByEvent[eid][key] = true
	}

	for eid, chTuples := range chRowsByEvent {
		mysqlSet := mysqlSetByEvent[eid]
		for _, t := range chTuples {
			key := fmt.Sprintf("%d|%d|%d", t.Event, t.Edition, t.ID)
			if mysqlSet == nil || !mysqlSet[key] {
				toDelete = append(toDelete, t)
			}
		}
	}

	toInsert = mysqlRecords
	return toDelete, toInsert
}

func deleteEventProductRowsByPrimaryKey(conn driver.Conn, tuples []eventProductTuple, tableName string) error {
	if len(tuples) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	for batchStart := 0; batchStart < len(tuples); batchStart += eventProductDeleteBatchSize {
		batchEnd := batchStart + eventProductDeleteBatchSize
		if batchEnd > len(tuples) {
			batchEnd = len(tuples)
		}
		batch := tuples[batchStart:batchEnd]

		var tupleStrs []string
		for _, t := range batch {
			tupleStrs = append(tupleStrs, fmt.Sprintf("(%d, %d, %d)", t.Event, t.Edition, t.ID))
		}

		deleteQuery := fmt.Sprintf(
			`ALTER TABLE %s DELETE WHERE (event, edition, id) IN (%s) SETTINGS mutations_sync = 1`,
			tableName, strings.Join(tupleStrs, ","),
		)
		log.Printf("[Query] %s", deleteQuery)

		log.Printf("[EventProduct DELETE] Batch %d-%d (%d tuples)", batchStart+1, batchEnd, len(batch))
		shared.WriteIncrementalLog(fmt.Sprintf("   [DELETE] Batch %d-%d: %d tuples", batchStart+1, batchEnd, len(batch)))

		if err := conn.Exec(ctx, deleteQuery); err != nil {
			return fmt.Errorf("ALTER DELETE batch %d-%d: %w", batchStart+1, batchEnd, err)
		}
	}

	return nil
}
