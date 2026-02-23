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

const speakerDeleteBatchSize = 500

type speakerTuple struct {
	EventID         uint32
	EditionID       uint32
	UserID          uint32
	SpeakerSourceID uint32
}

func ProcessIncrementalEventSpeaker(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) error {
	startTime := time.Now()
	log.Println("=== Starting Incremental Event Speaker Sync ===")
	log.Printf("Log file: %s", shared.IncrementalLogFile)

	shared.WriteIncrementalLog("")
	shared.WriteIncrementalLog("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	shared.WriteIncrementalLog(fmt.Sprintf("[%s] INCREMENTAL EVENT SPEAKER SYNC STARTED", startTime.Format("2006-01-02 15:04:05")))
	shared.WriteIncrementalLog("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	scopeEventIDs, err := fetchIncrementalScopeEventSpeaker(mysqlDB)
	if err != nil {
		return fmt.Errorf("fetch incremental scope: %w", err)
	}
	if len(scopeEventIDs) == 0 {
		log.Println("No events with modified event_speaker since yesterday, nothing to sync")
		shared.WriteIncrementalLog("SCOPE: No events with modified event_speaker since yesterday. Nothing to sync.")
		shared.WriteIncrementalLog(fmt.Sprintf("[%s] INCREMENTAL EVENT SPEAKER SYNC COMPLETED (no changes)", time.Now().Format("2006-01-02 15:04:05")))
		return nil
	}

	log.Printf("Incremental scope: %d event IDs to reconcile", len(scopeEventIDs))
	shared.WriteIncrementalLog("")
	shared.WriteIncrementalLog(fmt.Sprintf("1. SCOPE: %d event IDs (event_speaker modified since yesterday)", len(scopeEventIDs)))

	mysqlRecords, err := BuildEventSpeakerChRecordsForEventIDs(mysqlDB, scopeEventIDs, config)
	if err != nil {
		return fmt.Errorf("build MySQL records: %w", err)
	}
	if len(mysqlRecords) == 0 {
		log.Println("No records built from MySQL, nothing to sync")
		shared.WriteIncrementalLog("")
		shared.WriteIncrementalLog("2. MYSQL RECORDS: 0")
		shared.WriteIncrementalLog(fmt.Sprintf("[%s] INCREMENTAL EVENT SPEAKER SYNC COMPLETED (no records)", time.Now().Format("2006-01-02 15:04:05")))
		return nil
	}

	shared.WriteIncrementalLog("")
	shared.WriteIncrementalLog(fmt.Sprintf("2. MYSQL RECORDS: %d (current state for scoped events)", len(mysqlRecords)))

	tableName := shared.GetTableNameWithDB(shared.GetClickHouseTableName("event_speaker_ch", config), config)

	chRowsByEvent, err := fetchEventSpeakerChRowsForEventIDs(clickhouseConn, scopeEventIDs, tableName)
	if err != nil {
		return fmt.Errorf("fetch ClickHouse rows: %w", err)
	}

	toDelete, toInsert := computeEventSpeakerDiff(mysqlRecords, chRowsByEvent)

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
		if err := deleteEventSpeakerRowsByPrimaryKey(nativeConn, toDelete, tableName); err != nil {
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
		shared.WriteIncrementalLog(fmt.Sprintf("5. INSERT: %d records into event_speaker_ch", len(toInsert)))
		if err := InsertEventSpeakerChDataIntoTable(clickhouseConn, toInsert, tableName, config.ClickHouseWorkers); err != nil {
			return fmt.Errorf("insert records: %w", err)
		}
		log.Printf("Inserted %d records into event_speaker_ch", len(toInsert))
	} else {
		shared.WriteIncrementalLog("")
		shared.WriteIncrementalLog("5. INSERT: 0 (no records to insert)")
	}

	log.Printf("Running OPTIMIZE on event_speaker_ch...")
	optimizeErr := shared.OptimizeSingleTable(clickhouseConn, "event_speaker_ch", config, "")
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
	shared.WriteIncrementalLog(fmt.Sprintf("[%s] INCREMENTAL EVENT SPEAKER SYNC COMPLETED", endTime.Format("2006-01-02 15:04:05")))
	shared.WriteIncrementalLog("")

	log.Println("=== Incremental Event Speaker Sync Completed ===")
	return nil
}

func fetchIncrementalScopeEventSpeaker(db *sql.DB) ([]int64, error) {
	query := `
		SELECT DISTINCT event
		FROM event_speaker
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

func fetchEventSpeakerChRowsForEventIDs(conn driver.Conn, eventIDs []int64, tableName string) (map[int64][]speakerTuple, error) {
	if len(eventIDs) == 0 {
		return make(map[int64][]speakerTuple), nil
	}

	placeholders := make([]string, len(eventIDs))
	args := make([]interface{}, len(eventIDs))
	for i, id := range eventIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(
		`SELECT event_id, edition_id, user_id, speakerSourceId FROM %s FINAL WHERE event_id IN (%s)`,
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

	result := make(map[int64][]speakerTuple)
	for rows.Next() {
		var eventID, editionID, userID, speakerSourceID uint32
		if err := rows.Scan(&eventID, &editionID, &userID, &speakerSourceID); err != nil {
			return nil, err
		}
		eid := int64(eventID)
		result[eid] = append(result[eid], speakerTuple{
			EventID: eventID, EditionID: editionID, UserID: userID, SpeakerSourceID: speakerSourceID,
		})
	}
	return result, rows.Err()
}

func computeEventSpeakerDiff(mysqlRecords []SpeakerRecord, chRowsByEvent map[int64][]speakerTuple) (toDelete []speakerTuple, toInsert []SpeakerRecord) {
	for _, chTuples := range chRowsByEvent {
		for _, t := range chTuples {
			toDelete = append(toDelete, t)
		}
	}
	toInsert = mysqlRecords
	return toDelete, toInsert
}

func deleteEventSpeakerRowsByPrimaryKey(conn driver.Conn, tuples []speakerTuple, tableName string) error {
	if len(tuples) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	for batchStart := 0; batchStart < len(tuples); batchStart += speakerDeleteBatchSize {
		batchEnd := batchStart + speakerDeleteBatchSize
		if batchEnd > len(tuples) {
			batchEnd = len(tuples)
		}
		batch := tuples[batchStart:batchEnd]

		var tupleStrs []string
		for _, t := range batch {
			tupleStrs = append(tupleStrs, fmt.Sprintf("(%d, %d, %d, %d)", t.EventID, t.EditionID, t.UserID, t.SpeakerSourceID))
		}

		deleteQuery := fmt.Sprintf(
			`ALTER TABLE %s DELETE WHERE (event_id, edition_id, user_id, speakerSourceId) IN (%s) SETTINGS mutations_sync = 1`,
			tableName, strings.Join(tupleStrs, ","),
		)
		log.Printf("[Query] %s", deleteQuery)

		log.Printf("[EventSpeaker DELETE] Batch %d-%d (%d tuples)", batchStart+1, batchEnd, len(batch))
		shared.WriteIncrementalLog(fmt.Sprintf("   [DELETE] Batch %d-%d: %d tuples", batchStart+1, batchEnd, len(batch)))

		if err := conn.Exec(ctx, deleteQuery); err != nil {
			return fmt.Errorf("ALTER DELETE batch %d-%d: %w", batchStart+1, batchEnd, err)
		}
	}

	return nil
}
