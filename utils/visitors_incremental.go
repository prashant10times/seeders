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

const visitorDeleteBatchSize = 500

type visitorTuple struct {
	EventID   uint32
	EditionID uint32
	UserID    uint32
}

func ProcessIncrementalEventVisitor(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) error {
	startTime := time.Now()
	log.Println("=== Starting Incremental Event Visitor Sync ===")
	log.Printf("Log file: %s", shared.IncrementalLogFile)

	shared.WriteIncrementalLog("")
	shared.WriteIncrementalLog("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	shared.WriteIncrementalLog(fmt.Sprintf("[%s] INCREMENTAL EVENT VISITOR SYNC STARTED", startTime.Format("2006-01-02 15:04:05")))
	shared.WriteIncrementalLog("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	modifiedBatchData, err := buildVisitorChDataForModifiedRows(mysqlDB)
	if err != nil {
		return fmt.Errorf("fetch modified visitor rows: %w", err)
	}
	if len(modifiedBatchData) == 0 {
		log.Println("No event_visitor rows modified since yesterday, nothing to sync")
		shared.WriteIncrementalLog("SCOPE: No event_visitor rows modified since yesterday. Nothing to sync.")
		shared.WriteIncrementalLog(fmt.Sprintf("[%s] INCREMENTAL EVENT VISITOR SYNC COMPLETED (no changes)", time.Now().Format("2006-01-02 15:04:05")))
		return nil
	}

	log.Printf("Incremental scope: %d modified visitor rows to reconcile", len(modifiedBatchData))
	shared.WriteIncrementalLog("")
	shared.WriteIncrementalLog(fmt.Sprintf("1. SCOPE: %d modified rows (event_visitor modified since yesterday)", len(modifiedBatchData)))

	toDelete := extractVisitorTuplesFromBatchData(modifiedBatchData)

	var insertBatchData []map[string]interface{}
	for _, row := range modifiedBatchData {
		if shared.SafeConvertToInt8(row["published"]) > 0 {
			insertBatchData = append(insertBatchData, row)
		}
	}

	mysqlRecords, err := BuildEventVisitorChRecordsFromBatchData(mysqlDB, insertBatchData, config)
	if err != nil {
		return fmt.Errorf("build MySQL records: %w", err)
	}

	shared.WriteIncrementalLog("")
	shared.WriteIncrementalLog(fmt.Sprintf("2. MYSQL RECORDS: %d to insert (modified rows with published>0)", len(mysqlRecords)))

	tableName := shared.GetTableNameWithDB(shared.GetClickHouseTableName("event_visitors_ch", config), config)

	toInsert := mysqlRecords

	shared.WriteIncrementalLog("")
	shared.WriteIncrementalLog("3. DIFF:")
	shared.WriteIncrementalLog(fmt.Sprintf("   Rows to DELETE (modified only): %d", len(toDelete)))
	shared.WriteIncrementalLog(fmt.Sprintf("   Rows to INSERT (modified only, published>0): %d", len(toInsert)))

	if len(toDelete) > 0 {
		nativeConn, err := shared.SetupNativeProtocolConnectionForOptimize(config)
		if err != nil {
			return fmt.Errorf("create native connection for DELETE: %w", err)
		}
		defer nativeConn.Close()
		if err := deleteEventVisitorRowsByPrimaryKey(nativeConn, toDelete, tableName); err != nil {
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
		shared.WriteIncrementalLog(fmt.Sprintf("5. INSERT: %d records into event_visitors_ch", len(toInsert)))
		if err := InsertEventVisitorChDataIntoTable(clickhouseConn, toInsert, tableName, config.ClickHouseWorkers); err != nil {
			return fmt.Errorf("insert records: %w", err)
		}
		log.Printf("Inserted %d records into event_visitors_ch", len(toInsert))
	} else {
		shared.WriteIncrementalLog("")
		shared.WriteIncrementalLog("5. INSERT: 0 (no records to insert)")
	}

	log.Printf("Running OPTIMIZE on event_visitors_ch...")
	optimizeErr := shared.OptimizeSingleTable(clickhouseConn, "event_visitors_ch", config, "")
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
	shared.WriteIncrementalLog(fmt.Sprintf("   Modified rows reconciled: %d", len(modifiedBatchData)))
	shared.WriteIncrementalLog(fmt.Sprintf("   Rows deleted: %d", len(toDelete)))
	shared.WriteIncrementalLog(fmt.Sprintf("   Rows inserted: %d", len(toInsert)))
	shared.WriteIncrementalLog(fmt.Sprintf("   Duration: %v", duration.Round(time.Millisecond)))
	shared.WriteIncrementalLog(fmt.Sprintf("[%s] INCREMENTAL EVENT VISITOR SYNC COMPLETED", endTime.Format("2006-01-02 15:04:05")))
	shared.WriteIncrementalLog("")

	log.Println("=== Incremental Event Visitor Sync Completed ===")
	return nil
}

func extractVisitorTuplesFromBatchData(batchData []map[string]interface{}) []visitorTuple {
	var tuples []visitorTuple
	for _, row := range batchData {
		eventID := shared.ConvertToUInt32(row["event"])
		editionID := shared.ConvertToUInt32(row["edition"])
		userID := shared.ConvertToUInt32(row["user"])
		tuples = append(tuples, visitorTuple{
			EventID: eventID, EditionID: editionID, UserID: userID,
		})
	}
	return tuples
}

func deleteEventVisitorRowsByPrimaryKey(conn driver.Conn, tuples []visitorTuple, tableName string) error {
	if len(tuples) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	for batchStart := 0; batchStart < len(tuples); batchStart += visitorDeleteBatchSize {
		batchEnd := batchStart + visitorDeleteBatchSize
		if batchEnd > len(tuples) {
			batchEnd = len(tuples)
		}
		batch := tuples[batchStart:batchEnd]

		var tupleStrs []string
		for _, t := range batch {
			tupleStrs = append(tupleStrs, fmt.Sprintf("(%d, %d, %d)", t.EventID, t.EditionID, t.UserID))
		}

		deleteQuery := fmt.Sprintf(
			`ALTER TABLE %s DELETE WHERE (event_id, edition_id, user_id) IN (%s) SETTINGS mutations_sync = 1`,
			tableName, strings.Join(tupleStrs, ","),
		)
		log.Printf("[Query] %s", deleteQuery)

		log.Printf("[EventVisitor DELETE] Batch %d-%d (%d tuples)", batchStart+1, batchEnd, len(batch))
		shared.WriteIncrementalLog(fmt.Sprintf("   [DELETE] Batch %d-%d: %d tuples", batchStart+1, batchEnd, len(batch)))

		if err := conn.Exec(ctx, deleteQuery); err != nil {
			return fmt.Errorf("ALTER DELETE batch %d-%d: %w", batchStart+1, batchEnd, err)
		}
	}

	return nil
}
