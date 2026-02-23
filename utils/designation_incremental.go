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

const designationDeleteBatchSize = 500

type eventEditionDesignationTuple struct {
	EventID       uint32
	EditionID     uint32
	DesignationID uint32
}

func ProcessIncrementalEventDesignation(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) error {
	startTime := time.Now()
	log.Println("=== Starting Incremental Event Designation Sync ===")
	log.Printf("Log file: %s", shared.IncrementalLogFile)

	shared.WriteIncrementalLog("")
	shared.WriteIncrementalLog("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	shared.WriteIncrementalLog(fmt.Sprintf("[%s] INCREMENTAL EVENT DESIGNATION SYNC STARTED", startTime.Format("2006-01-02 15:04:05")))
	shared.WriteIncrementalLog("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	pairs, err := fetchIncrementalScopeEventDesignation(mysqlDB)
	if err != nil {
		return fmt.Errorf("fetch incremental scope: %w", err)
	}
	if len(pairs) == 0 {
		log.Println("No event-edition pairs with modified event_visitor (designation) since yesterday, nothing to sync")
		shared.WriteIncrementalLog("SCOPE: No event-edition pairs with modified event_visitor (designation) since yesterday. Nothing to sync.")
		shared.WriteIncrementalLog(fmt.Sprintf("[%s] INCREMENTAL EVENT DESIGNATION SYNC COMPLETED (no changes)", time.Now().Format("2006-01-02 15:04:05")))
		return nil
	}

	log.Printf("Incremental scope: %d event-edition pairs to reconcile", len(pairs))
	shared.WriteIncrementalLog("")
	shared.WriteIncrementalLog(fmt.Sprintf("1. SCOPE: %d event-edition pairs (event_visitor with designation modified since yesterday)", len(pairs)))

	mysqlRecords, err := BuildEventDesignationChRecordsForEventEditionPairs(mysqlDB, pairs)
	if err != nil {
		return fmt.Errorf("build MySQL records: %w", err)
	}
	if len(mysqlRecords) == 0 {
		log.Println("No records built from MySQL, nothing to sync")
		shared.WriteIncrementalLog("")
		shared.WriteIncrementalLog("2. MYSQL RECORDS: 0")
		shared.WriteIncrementalLog(fmt.Sprintf("[%s] INCREMENTAL EVENT DESIGNATION SYNC COMPLETED (no records)", time.Now().Format("2006-01-02 15:04:05")))
		return nil
	}

	shared.WriteIncrementalLog("")
	shared.WriteIncrementalLog(fmt.Sprintf("2. MYSQL RECORDS: %d (current state for scoped event-edition pairs)", len(mysqlRecords)))

	tableName := shared.GetTableNameWithDB(shared.GetClickHouseTableName("event_designation_ch", config), config)

	chRowsByPair, err := fetchEventDesignationChRowsForEventEditionPairs(clickhouseConn, pairs, tableName)
	if err != nil {
		return fmt.Errorf("fetch ClickHouse rows: %w", err)
	}

	toDelete, toInsert := computeEventDesignationDiff(mysqlRecords, chRowsByPair)

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
		if err := deleteEventDesignationRowsByPrimaryKey(nativeConn, toDelete, tableName); err != nil {
			return fmt.Errorf("delete obsolete rows: %w", err)
		}
		shared.WriteIncrementalLog("")
		shared.WriteIncrementalLog(fmt.Sprintf("4. DELETE: %d rows deleted", len(toDelete)))
	} else {
		shared.WriteIncrementalLog("")
		shared.WriteIncrementalLog("4. DELETE: 0 rows (none required)")
	}

	if len(toInsert) > 0 {
		log.Printf("[INSERT] Inserting %d records into %s", len(toInsert), tableName)
		shared.WriteIncrementalLog("")
		shared.WriteIncrementalLog(fmt.Sprintf("5. INSERT: %d records into event_designation_ch", len(toInsert)))
		if err := InsertEventDesignationChDataIntoTable(clickhouseConn, toInsert, tableName, config.ClickHouseWorkers); err != nil {
			return fmt.Errorf("insert records: %w", err)
		}
		log.Printf("Inserted %d records into event_designation_ch", len(toInsert))
	} else {
		shared.WriteIncrementalLog("")
		shared.WriteIncrementalLog("5. INSERT: 0 (no records to insert)")
	}

	log.Printf("Running OPTIMIZE on event_designation_ch...")
	optimizeErr := shared.OptimizeSingleTable(clickhouseConn, "event_designation_ch", config, "")
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
	shared.WriteIncrementalLog(fmt.Sprintf("   Event-edition pairs reconciled: %d", len(pairs)))
	shared.WriteIncrementalLog(fmt.Sprintf("   Rows deleted: %d", len(toDelete)))
	shared.WriteIncrementalLog(fmt.Sprintf("   Rows inserted: %d", len(toInsert)))
	shared.WriteIncrementalLog(fmt.Sprintf("   Duration: %v", duration.Round(time.Millisecond)))
	shared.WriteIncrementalLog(fmt.Sprintf("[%s] INCREMENTAL EVENT DESIGNATION SYNC COMPLETED", endTime.Format("2006-01-02 15:04:05")))
	shared.WriteIncrementalLog("")

	log.Println("=== Incremental Event Designation Sync Completed ===")
	return nil
}

func fetchIncrementalScopeEventDesignation(db *sql.DB) ([]EventEditionPair, error) {
	query := `
		SELECT DISTINCT ev.event AS event_id, ev.edition AS edition_id
		FROM event_visitor ev
		WHERE ev.modified >= CURDATE() - INTERVAL 1 DAY
		  AND ev.evisitor = 0
		  AND ev.published = 1
		  AND ev.designation_id IS NOT NULL
		ORDER BY ev.event, ev.edition
	`
	log.Printf("[Query] %s", strings.TrimSpace(query))
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pairs []EventEditionPair
	for rows.Next() {
		var p EventEditionPair
		if err := rows.Scan(&p.EventID, &p.EditionID); err != nil {
			return nil, err
		}
		pairs = append(pairs, p)
	}
	return pairs, rows.Err()
}

// fetchEventDesignationChRowsForEventEditionPairs returns map["eventID|editionID"][]eventEditionDesignationTuple.
func fetchEventDesignationChRowsForEventEditionPairs(conn driver.Conn, pairs []EventEditionPair, tableName string) (map[string][]eventEditionDesignationTuple, error) {
	if len(pairs) == 0 {
		return make(map[string][]eventEditionDesignationTuple), nil
	}

	var tupleStrs []string
	for _, p := range pairs {
		tupleStrs = append(tupleStrs, fmt.Sprintf("(%d, %d)", p.EventID, p.EditionID))
	}

	query := fmt.Sprintf(
		`SELECT event_id, edition_id, designation_id FROM %s FINAL WHERE (event_id, edition_id) IN (%s)`,
		tableName,
		strings.Join(tupleStrs, ","),
	)
	log.Printf("[Query] %s", query)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string][]eventEditionDesignationTuple)
	for rows.Next() {
		var eventID, editionID, designationID uint32
		if err := rows.Scan(&eventID, &editionID, &designationID); err != nil {
			return nil, err
		}
		key := fmt.Sprintf("%d|%d", eventID, editionID)
		result[key] = append(result[key], eventEditionDesignationTuple{
			EventID: eventID, EditionID: editionID, DesignationID: designationID,
		})
	}
	return result, rows.Err()
}

func computeEventDesignationDiff(mysqlRecords []EventDesignationChRecord, chRowsByPair map[string][]eventEditionDesignationTuple) (toDelete []eventEditionDesignationTuple, toInsert []EventDesignationChRecord) {
	mysqlSetByPair := make(map[string]map[string]bool) // "eventID|editionID" -> set of "event|edition|designation"
	for _, rec := range mysqlRecords {
		key := fmt.Sprintf("%d|%d", rec.EventID, rec.EditionID)
		if mysqlSetByPair[key] == nil {
			mysqlSetByPair[key] = make(map[string]bool)
		}
		tupleKey := fmt.Sprintf("%d|%d|%d", rec.EventID, rec.EditionID, rec.DesignationID)
		mysqlSetByPair[key][tupleKey] = true
	}

	for pairKey, chTuples := range chRowsByPair {
		mysqlSet := mysqlSetByPair[pairKey]
		for _, t := range chTuples {
			tupleKey := fmt.Sprintf("%d|%d|%d", t.EventID, t.EditionID, t.DesignationID)
			if mysqlSet == nil || !mysqlSet[tupleKey] {
				toDelete = append(toDelete, t)
			}
		}
	}

	toInsert = mysqlRecords
	return toDelete, toInsert
}

func deleteEventDesignationRowsByPrimaryKey(conn driver.Conn, tuples []eventEditionDesignationTuple, tableName string) error {
	if len(tuples) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	for batchStart := 0; batchStart < len(tuples); batchStart += designationDeleteBatchSize {
		batchEnd := batchStart + designationDeleteBatchSize
		if batchEnd > len(tuples) {
			batchEnd = len(tuples)
		}
		batch := tuples[batchStart:batchEnd]

		var tupleStrs []string
		for _, t := range batch {
			tupleStrs = append(tupleStrs, fmt.Sprintf("(%d, %d, %d)", t.EventID, t.EditionID, t.DesignationID))
		}

		deleteQuery := fmt.Sprintf(
			`ALTER TABLE %s DELETE WHERE (event_id, edition_id, designation_id) IN (%s) SETTINGS mutations_sync = 1`,
			tableName, strings.Join(tupleStrs, ","),
		)
		log.Printf("[Query] %s", deleteQuery)

		log.Printf("[EventDesignation DELETE] Batch %d-%d (%d tuples)", batchStart+1, batchEnd, len(batch))
		shared.WriteIncrementalLog(fmt.Sprintf("   [DELETE] Batch %d-%d: %d tuples", batchStart+1, batchEnd, len(batch)))

		if err := conn.Exec(ctx, deleteQuery); err != nil {
			return fmt.Errorf("ALTER DELETE batch %d-%d: %w", batchStart+1, batchEnd, err)
		}
	}

	return nil
}
