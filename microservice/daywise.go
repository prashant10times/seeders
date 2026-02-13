package microservice

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"seeders/shared"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func fetchEstimateEventIDsBatch(db *sql.DB, lastEventID int64, batchSize int) ([]int64, error) {
	query := `SELECT DISTINCT event_id FROM estimate WHERE event_id > ? ORDER BY event_id LIMIT ?`
	rows, err := db.Query(query, lastEventID, batchSize)
	if err != nil {
		return nil, fmt.Errorf("fetching estimate event IDs: %w", err)
	}
	defer rows.Close()

	var eventIDs []int64
	for rows.Next() {
		var eventID int64
		if err := rows.Scan(&eventID); err != nil {
			continue
		}
		eventIDs = append(eventIDs, eventID)
	}
	return eventIDs, rows.Err()
}

func ProcessDayWiseEconomicImpactOnly(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) {
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	log.Println("=== STANDALONE DAY-WISE ECONOMIC IMPACT SEEDING ===")
	log.Println("Fetching event_ids from estimate table in batches, processing, inserting into event_daywiseEconomicImpact_temp")
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	batchSize := config.BatchSize
	if batchSize <= 0 {
		batchSize = 5000
	}

	// Pre-flight: count total distinct event_ids to process (for verification)
	var totalEstimateCount int64
	if err := mysqlDB.QueryRow(`SELECT COUNT(DISTINCT event_id) FROM estimate WHERE event_id > 0`).Scan(&totalEstimateCount); err != nil {
		log.Printf("WARNING: Could not get estimate count: %v", err)
	} else {
		log.Printf("Total event_ids in estimate table: %d", totalEstimateCount)
	}

	lastEventID := int64(0)
	batchNum := 0
	totalRecordsInserted := 0
	totalEventsProcessed := 0
	lastUpdatedAt := time.Now().Format("2006-01-02 15:04:05")

	for {
		eventIDs, err := fetchEstimateEventIDsBatch(mysqlDB, lastEventID, batchSize)
		if err != nil {
			log.Fatalf("Failed to fetch estimate event IDs: %v", err)
		}
		if len(eventIDs) == 0 {
			break
		}

		lastEventID = eventIDs[len(eventIDs)-1]
		batchNum++

		estimateDataMap := fetchalleventEstimateDataForBatch(mysqlDB, eventIDs)
		if len(estimateDataMap) == 0 {
			log.Printf("Daywise batch %d: No estimate data for %d event IDs", batchNum, len(eventIDs))
			continue
		}

		totalEventsProcessed += len(estimateDataMap)
		processedEconomicData := processalleventEconomicImpactDataParallel(estimateDataMap)
		if len(processedEconomicData) == 0 {
			continue
		}

		dayWiseRecords := aggregateDayWiseRecordsFromEconomicData(processedEconomicData, lastUpdatedAt)
		if len(dayWiseRecords) == 0 {
			log.Printf("Daywise batch %d: No day-wise records to insert for %d events", batchNum, len(processedEconomicData))
			continue
		}

		if err := insertDayWiseEconomicImpactWithRetry(clickhouseConn, dayWiseRecords); err != nil {
			log.Printf("ERROR: Daywise batch %d failed to insert %d records: %v", batchNum, len(dayWiseRecords), err)
			continue
		}

		totalRecordsInserted += len(dayWiseRecords)
		log.Printf("Daywise batch %d: Inserted %d records (events %d-%d)", batchNum, len(dayWiseRecords), eventIDs[0], lastEventID)
	}

	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	log.Printf("=== DAY-WISE ECONOMIC IMPACT SEEDING COMPLETED ===")
	log.Printf("  Events processed: %d | Records inserted: %d | Batches: %d", totalEventsProcessed, totalRecordsInserted, batchNum)
	if totalEstimateCount > 0 && int64(totalEventsProcessed) != totalEstimateCount {
		log.Printf("  ⚠️  WARNING: Processed %d events but estimate table has %d distinct event_ids - possible mismatch", totalEventsProcessed, totalEstimateCount)
	} else if totalEstimateCount > 0 {
		log.Printf("  ✓ All %d event_ids from estimate table were processed", totalEstimateCount)
	}
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
}
