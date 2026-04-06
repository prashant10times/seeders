package microservice

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"seeders/shared"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/elastic/go-elasticsearch/v6"
)

// TableIncrementalConfig holds table-specific config for incremental sync.
type TableIncrementalConfig struct {
	TableName          string
	PrimaryKeyColumns  []string
	OrderByColumns     []string
	ExtractPKTuple     func(record map[string]interface{}) string
	RecordMapKey       func(record map[string]interface{}) string
	ScanOrderByRow     func(rows driver.Rows) (mapKey string, orderByValues map[string]interface{}, err error)
}

// alleventIncrementalConfig is the config for allevent_ch table.
var alleventIncrementalConfig = TableIncrementalConfig{
	TableName:         "allevent_ch",
	PrimaryKeyColumns: []string{"event_id", "edition_id"},
	OrderByColumns:    []string{"published", "status", "edition_type"},
	ExtractPKTuple: func(record map[string]interface{}) string {
		return fmt.Sprintf("(%d, %d)",
			shared.ConvertToUInt32(record["event_id"]),
			shared.ConvertToUInt32(record["edition_id"]))
	},
	RecordMapKey: func(record map[string]interface{}) string {
		return fmt.Sprintf("%d|%d",
			shared.ConvertToUInt32(record["event_id"]),
			shared.ConvertToUInt32(record["edition_id"]))
	},
	ScanOrderByRow: func(rows driver.Rows) (string, map[string]interface{}, error) {
		var eventID, editionID uint32
		var published int8
		var status, editionType string
		if err := rows.Scan(&eventID, &editionID, &published, &status, &editionType); err != nil {
			return "", nil, err
		}
		key := fmt.Sprintf("%d|%d", eventID, editionID)
		orderByValues := map[string]interface{}{
			"event_id":     eventID,
			"edition_id":   editionID,
			"published":    published,
			"status":       strings.TrimSpace(status),
			"edition_type": strings.TrimSpace(editionType),
		}
		return key, orderByValues, nil
	},
}

func formatPairsForLog(pairs []EventEditionPair, maxShow int) []string {
	if len(pairs) == 0 {
		return []string{"     (none)"}
	}
	show := len(pairs)
	truncated := false
	if show > maxShow {
		show = maxShow
		truncated = true
	}
	const perLine = 15
	var lines []string
	var sb strings.Builder
	for i := 0; i < show; i++ {
		if sb.Len() > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("(%d, %d)", pairs[i].EventID, pairs[i].EditionID))
		if (i+1)%perLine == 0 || i == show-1 {
			lines = append(lines, "     "+sb.String())
			sb.Reset()
		}
	}
	if truncated {
		lines = append(lines, fmt.Sprintf("     ... and %d more", len(pairs)-maxShow))
	}
	return lines
}

func writePairsToLog(pairs []EventEditionPair, maxShow int) {
	for _, line := range formatPairsForLog(pairs, maxShow) {
		shared.WriteIncrementalLog(line)
	}
}

func extractPairsFromRecords(records []map[string]interface{}) []EventEditionPair {
	pairs := make([]EventEditionPair, len(records))
	for i, r := range records {
		pairs[i] = EventEditionPair{
			EventID:   uint32(shared.ConvertToUInt32(r["event_id"])),
			EditionID: uint32(shared.ConvertToUInt32(r["edition_id"])),
		}
	}
	return pairs
}

func ProcessIncrementalAllevent(
	mysqlDB *sql.DB,
	clickhouseConn driver.Conn,
	esClient *elasticsearch.Client,
	config shared.Config,
) error {
	startTime := time.Now()
	log.Println("=== Starting Incremental Allevent Sync ===")
	log.Printf("Detailed log file: %s", shared.IncrementalLogFile)

	shared.WriteIncrementalLog("")
	shared.WriteIncrementalLog("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	shared.WriteIncrementalLog(fmt.Sprintf("[%s] INCREMENTAL ALLEVENT SYNC STARTED", startTime.Format("2006-01-02 15:04:05")))
	shared.WriteIncrementalLog("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	if eventTypeIDs == nil {
		ids, err := loadEventTypeIDsFromDB(clickhouseConn, config)
		if err != nil {
			return fmt.Errorf("load event type IDs: %w", err)
		}
		eventTypeIDs = ids
		eventTypeUUIDToID = make(map[string]uint32, len(ids))
		for id, uuid := range ids {
			eventTypeUUIDToID[uuid] = id
		}
	}

	pairs, err := fetchIncrementalScope(mysqlDB)
	if err != nil {
		return fmt.Errorf("fetch incremental scope: %w", err)
	}
	if len(pairs) == 0 {
		log.Println("No changed event-edition pairs since yesterday, nothing to sync")
		shared.WriteIncrementalLog("SCOPE: No changed event-edition pairs since yesterday. Nothing to sync.")
		shared.WriteIncrementalLog(fmt.Sprintf("[%s] INCREMENTAL ALLEVENT SYNC COMPLETED (no changes)", time.Now().Format("2006-01-02 15:04:05")))
		return nil
	}
	log.Printf("Incremental scope: %d (event_id, edition_id) pairs to refresh", len(pairs))
	shared.WriteIncrementalLog("")
	shared.WriteIncrementalLog("1. SCOPE (event-edition pairs modified since yesterday):")
	shared.WriteIncrementalLog(fmt.Sprintf("   Total: %d pairs", len(pairs)))
	shared.WriteIncrementalLog("   Pairs (event_id, edition_id):")
	writePairsToLog(pairs, 500)

	records, err := buildIncrementalRecords(mysqlDB, clickhouseConn, esClient, pairs, config)
	if err != nil {
		return fmt.Errorf("build incremental records: %w", err)
	}
	if len(records) == 0 {
		log.Println("No records built, nothing to sync")
		shared.WriteIncrementalLog("")
		shared.WriteIncrementalLog("2. RECORDS BUILT: 0 (skipped or failed to build)")
		shared.WriteIncrementalLog(fmt.Sprintf("[%s] INCREMENTAL ALLEVENT SYNC COMPLETED (no records)", time.Now().Format("2006-01-02 15:04:05")))
		return nil
	}
	log.Printf("Built %d records for incremental sync", len(records))
	shared.WriteIncrementalLog("")
	shared.WriteIncrementalLog(fmt.Sprintf("2. RECORDS BUILT: %d", len(records)))

	tableName := shared.GetTableNameWithDB(shared.GetClickHouseTableName(alleventIncrementalConfig.TableName, config), config)
	tableCfg := &alleventIncrementalConfig
	situations, err := classifyRecordsByOrderByKey(clickhouseConn, records, tableName, tableCfg)
	if err != nil {
		return fmt.Errorf("classify records: %w", err)
	}
	log.Printf("Key unchanged (insert refresh): %d, Key changed (delete+reinsert): %d, New rows: %d",
		len(situations.KeyUnchangedRecords), len(situations.KeyChangedRecords), len(situations.NewRecords))

	shared.WriteIncrementalLog("")
	shared.WriteIncrementalLog("3. CLASSIFICATION:")
	shared.WriteIncrementalLog(fmt.Sprintf("   Key unchanged (insert refresh - non-key columns changed): %d", len(situations.KeyUnchangedRecords)))
	shared.WriteIncrementalLog(fmt.Sprintf("   Key changed (delete+reinsert - e.g. edition_type current→past): %d", len(situations.KeyChangedRecords)))
	shared.WriteIncrementalLog(fmt.Sprintf("   New rows (not in ClickHouse yet): %d", len(situations.NewRecords)))

	pairsKeyUnchanged := extractPairsFromRecords(situations.KeyUnchangedRecords)
	pairsKeyChanged := extractPairsFromRecords(situations.KeyChangedRecords)
	pairsNew := extractPairsFromRecords(situations.NewRecords)

	if len(pairsKeyUnchanged) > 0 {
		shared.WriteIncrementalLog("   Key unchanged - Event/Edition pairs (insert refresh):")
		writePairsToLog(pairsKeyUnchanged, 500)
	}
	if len(pairsKeyChanged) > 0 {
		shared.WriteIncrementalLog("   Key changed - Event/Edition pairs (delete then re-insert):")
		writePairsToLog(pairsKeyChanged, 500)
	}
	if len(pairsNew) > 0 {
		shared.WriteIncrementalLog("   New rows - Event/Edition pairs (inserted):")
		writePairsToLog(pairsNew, 500)
	}

	// 5. Key changed: DELETE + INSERT (ORDER BY columns cannot be ALTER UPDATE'd).
	if len(situations.KeyChangedRecords) > 0 {
		nativeConn, err := shared.SetupNativeProtocolConnectionForOptimize(config)
		if err != nil {
			return fmt.Errorf("create native connection for DELETE: %w", err)
		}
		defer nativeConn.Close()
		if err := deleteRowsByPrimaryKey(nativeConn, situations.KeyChangedRecords, tableName, tableCfg); err != nil {
			return fmt.Errorf("delete rows for key changed: %w", err)
		}
		shared.WriteIncrementalLog("")
		shared.WriteIncrementalLog(fmt.Sprintf("4. DELETE: %d rows deleted (key changed, will re-insert)", len(situations.KeyChangedRecords)))
	} else {
		shared.WriteIncrementalLog("")
		shared.WriteIncrementalLog("4. DELETE: 0 rows (none required)")
	}

	// 6. INSERT all records: key unchanged + new rows + key changed (re-insert after delete)
	allRecords := append(situations.KeyUnchangedRecords, situations.NewRecords...)
	allRecords = append(allRecords, situations.KeyChangedRecords...)
	insertTableName := shared.GetTableNameWithDB(shared.GetClickHouseTableName(alleventIncrementalConfig.TableName, config), config)
	log.Printf("[INSERT] Inserting %d records into %s (key unchanged: %d, new: %d, key changed re-insert: %d)",
		len(allRecords), insertTableName, len(situations.KeyUnchangedRecords), len(situations.NewRecords), len(situations.KeyChangedRecords))
	shared.WriteIncrementalLog(fmt.Sprintf("   [INSERT] %d records into %s", len(allRecords), insertTableName))
	if err := insertalleventDataIntoClickHouseTable(clickhouseConn, allRecords, config.ClickHouseWorkers, config, insertTableName); err != nil {
		return fmt.Errorf("insert incremental records: %w", err)
	}
	log.Printf("Inserted %d records into ClickHouse", len(allRecords))
	shared.WriteIncrementalLog("")
	shared.WriteIncrementalLog(fmt.Sprintf("5. INSERT: %d records inserted into allevent_ch", len(allRecords)))

	// 7. Run OPTIMIZE
	log.Printf("Running OPTIMIZE on %s...", alleventIncrementalConfig.TableName)
	optimizeErr := shared.OptimizeSingleTable(clickhouseConn, alleventIncrementalConfig.TableName, config, "")
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
	totalModified := len(situations.KeyUnchangedRecords) + len(situations.KeyChangedRecords) + len(situations.NewRecords)

	shared.WriteIncrementalLog("")
	shared.WriteIncrementalLog("SUMMARY:")
	shared.WriteIncrementalLog(fmt.Sprintf("   Total event-edition pairs modified: %d", totalModified))
	shared.WriteIncrementalLog(fmt.Sprintf("   - Key unchanged (insert refresh): %d", len(situations.KeyUnchangedRecords)))
	shared.WriteIncrementalLog(fmt.Sprintf("   - Key changed (DELETE+reinsert): %d", len(situations.KeyChangedRecords)))
	shared.WriteIncrementalLog(fmt.Sprintf("   - New rows inserted: %d", len(situations.NewRecords)))
	shared.WriteIncrementalLog(fmt.Sprintf("   Duration: %v", duration.Round(time.Millisecond)))
	shared.WriteIncrementalLog(fmt.Sprintf("[%s] INCREMENTAL ALLEVENT SYNC COMPLETED", endTime.Format("2006-01-02 15:04:05")))
	shared.WriteIncrementalLog("")

	log.Println("=== Incremental Allevent Sync Completed ===")
	return nil
}

func fetchIncrementalScope(db *sql.DB) ([]EventEditionPair, error) {
	query := `
		SELECT DISTINCT ee.event AS event_id, ee.id AS edition_id
		FROM event_edition ee
		INNER JOIN event e ON ee.event = e.id
		WHERE ee.modified >= CURDATE() - INTERVAL 1 DAY
		   OR ee.created >= CURDATE() - INTERVAL 1 DAY
		   OR e.modified >= CURDATE() - INTERVAL 1 DAY
		   OR e.created >= CURDATE() - INTERVAL 1 DAY
		ORDER BY ee.event, ee.id
	`
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pairs []EventEditionPair
	for rows.Next() {
		var eventID, editionID uint32
		if err := rows.Scan(&eventID, &editionID); err != nil {
			return nil, err
		}
		pairs = append(pairs, EventEditionPair{EventID: eventID, EditionID: editionID})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return pairs, nil
}

func buildIncrementalRecords(
	mysqlDB *sql.DB,
	clickhouseConn driver.Conn,
	esClient *elasticsearch.Client,
	pairs []EventEditionPair,
	config shared.Config,
) ([]map[string]interface{}, error) {
	eventIDSet := make(map[int64]bool)
	for _, p := range pairs {
		eventIDSet[int64(p.EventID)] = true
	}
	eventIDs := make([]int64, 0, len(eventIDSet))
	for id := range eventIDSet {
		eventIDs = append(eventIDs, id)
	}

	editionData := fetchallalleventDataForBatch(mysqlDB, eventIDs)
	if len(editionData) == 0 {
		return nil, nil
	}

	companyIDs := extractalleventCompanyIDs(editionData)
	venueIDs := extractalleventVenueIDs(editionData)
	editionCityIDs := extractalleventCityIDs(editionData)

	companyData := fetchalleventCompanyDataParallel(mysqlDB, companyIDs)
	venueData := fetchalleventVenueDataParallel(mysqlDB, venueIDs)

	companyCityIDs := extractCityIDsFromCompanies(companyData)
	venueCityIDs := extractCityIDsFromVenues(venueData)
	allCityIDs := dedupeIDs(editionCityIDs, companyCityIDs, venueCityIDs)

	cityData := shared.FetchCityDataParallel(mysqlDB, allCityIDs, config.NumWorkers)

	esData := fetchalleventElasticsearchDataForEvents(esClient, config.ElasticsearchIndex, eventIDs)

	locationTableName := shared.GetClickHouseTableName("location_ch", config)
	cityIDLookup, _ := buildCityIDLookupFromId10x(clickhouseConn, locationTableName)
	stateIDLookup, _ := buildStateIDLookupFromId10x(clickhouseConn, locationTableName)
	stateUUIDLookup, _ := buildStateIDLookupFromUUID(clickhouseConn, locationTableName)
	cityStateUUIDLookup, _ := buildCityStateUUIDLookup(clickhouseConn, locationTableName)
	venueIDLookup, _ := buildVenueIDLookupFromId10x(clickhouseConn, locationTableName)

	companyLookup := indexByID10x(companyData)
	venueLookup := indexByID(venueData, "id")
	cityLookup := indexByID(cityData, "id")

	eventDataLookup := make(map[int64]map[string]interface{})
	eventDataForEditions := fetchalleventEventDataForBatch(mysqlDB, eventIDs)
	for _, eventData := range eventDataForEditions {
		if eventID, ok := eventData["id"].(int64); ok {
			eventDataLookup[eventID] = eventData
		}
	}

	estimateDataMap := fetchalleventEstimateDataForBatch(mysqlDB, eventIDs)
	eventTypesMap := fetchalleventEventTypesForBatch(mysqlDB, eventIDs)
	categoryNamesMap := fetchalleventCategoryNamesForEvents(mysqlDB, eventIDs)
	predictedDatesMap := fetchalleventPredictedDatesForBatch(mysqlDB, eventIDs)
	rawTicketData := fetchalleventTicketDataForBatch(mysqlDB, eventIDs)
	ticketDataMap := processalleventTicketData(rawTicketData)
	timingDataMap := fetchalleventTimingDataForBatch(mysqlDB, editionData)
	processedEconomicData := processalleventEconomicImpactDataParallel(estimateDataMap)

	allevents := make(map[int64][]map[string]interface{})
	currentEditionStartDates := make(map[int64]interface{})
	currentEditionIDs := make(map[int64]int64)

	for _, edition := range editionData {
		eventID := int64(shared.ConvertToUInt32(edition["event"]))
		if eventID <= 0 {
			continue
		}
		allevents[eventID] = append(allevents[eventID], edition)
		if curID, ok := edition["current_edition_id"]; ok {
			edID := int64(shared.ConvertToUInt32(edition["edition_id"]))
			curID64 := int64(shared.ConvertToUInt32(curID))
			if curID64 == edID {
				currentEditionStartDates[eventID] = edition["edition_start_date"]
				currentEditionIDs[eventID] = edID
			}
		}
	}

	locationEnrich := buildAlleventLocationEnrichment(
		clickhouseConn,
		locationTableName,
		allevents,
		eventDataLookup,
		companyLookup,
		venueLookup,
		cityLookup,
		cityIDLookup,
		stateIDLookup,
		stateUUIDLookup,
		cityStateUUIDLookup,
		venueIDLookup,
	)

	var records []map[string]interface{}
	for _, pair := range pairs {
		eventID := int64(pair.EventID)
		editionID := int64(pair.EditionID)

		editions := allevents[eventID]
		var targetEdition map[string]interface{}
		for _, ed := range editions {
			if int64(shared.ConvertToUInt32(ed["edition_id"])) == editionID {
				targetEdition = ed
				break
			}
		}
		if targetEdition == nil {
			log.Printf("WARNING: Edition %d for event %d not found in fetched data, skipping", editionID, eventID)
			continue
		}

		eventData := eventDataLookup[eventID]
		if eventData == nil {
			log.Printf("WARNING: Event %d data not found, skipping edition %d", eventID, editionID)
			continue
		}

		company, venue, city, companyCity, venueCity := resolveRelatedDataForEdition(
			targetEdition, companyLookup, venueLookup, cityLookup,
		)

		esInfoMap := esData[eventID]
		if esInfoMap == nil {
			esInfoMap = make(map[string]interface{})
		}

		editionDomain, companyDomain := extractDomainsForEdition(eventData, company)

		editionType := determinealleventType(
			targetEdition["edition_start_date"],
			currentEditionStartDates[eventID],
			editionID,
			currentEditionIDs[eventID],
		)

		editionCountryISO := strings.ToUpper(shared.ConvertToString(eventData["country"]))

		editionCityLocationChID, companyCityLocationChID, venueCityLocationChID,
			editionCityStateLocationChID, venueLocationChID := computeAllLocationIDs(
			city, companyCity, venue, venueCity,
			editionCountryISO,
			cityIDLookup, stateIDLookup, stateUUIDLookup, cityStateUUIDLookup, venueIDLookup,
		)

		var currentEdition map[string]interface{}
		if currentEditionIDs[eventID] > 0 {
			for _, e := range editions {
				if eid, ok := e["edition_id"].(int64); ok && eid == currentEditionIDs[eventID] {
					currentEdition = e
					break
				}
			}
		}

		record := buildAlleventRecord(
			eventData, targetEdition, company, venue, city, companyCity, venueCity,
			esInfoMap, processedEconomicData[eventID], estimateDataMap[eventID],
			eventTypesMap, categoryNamesMap, ticketDataMap, timingDataMap,
			eventID, editionType, currentEditionIDs[eventID], editions,
			currentEdition,
			editionCountryISO, editionDomain, companyDomain,
			editionCityLocationChID, companyCityLocationChID, venueCityLocationChID,
			editionCityStateLocationChID, venueLocationChID,
			predictedDatesMap[eventID],
			locationEnrich,
		)

		record["last_updated_at"] = time.Now().Format("2006-01-02 15:04:05")
		records = append(records, record)
	}

	return records, nil
}

func fetchExistingOrderByKey(
	conn driver.Conn,
	records []map[string]interface{},
	tableName string,
	tableCfg *TableIncrementalConfig,
) (map[string]map[string]interface{}, error) {
	if len(records) == 0 || tableCfg == nil {
		return make(map[string]map[string]interface{}), nil
	}

	seen := make(map[string]bool)
	var tuples []string
	for _, rec := range records {
		tup := tableCfg.ExtractPKTuple(rec)
		if !seen[tup] {
			seen[tup] = true
			tuples = append(tuples, tup)
		}
	}
	if len(tuples) == 0 {
		return make(map[string]map[string]interface{}), nil
	}

	pkCols := strings.Join(tableCfg.PrimaryKeyColumns, ", ")
	orderByCols := strings.Join(tableCfg.OrderByColumns, ", ")
	selectCols := pkCols + ", " + orderByCols

	query := fmt.Sprintf(
		`SELECT %s FROM %s FINAL WHERE (%s) IN (%s)`,
		selectCols, tableName, pkCols, strings.Join(tuples, ","),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]map[string]interface{})
	if tableCfg.ScanOrderByRow == nil {
		return nil, fmt.Errorf("ScanOrderByRow is required: ClickHouse driver does not support scanning into *interface{}")
	}
	for rows.Next() {
		key, orderByValues, err := tableCfg.ScanOrderByRow(rows)
		if err != nil {
			return nil, err
		}
		result[key] = orderByValues
	}
	return result, rows.Err()
}

func orderByValuesEqual(existing map[string]interface{}, record map[string]interface{}, orderByCols []string) bool {
	for _, col := range orderByCols {
		ev := existing[col]
		rv := record[col]
		evStr := strings.TrimSpace(shared.ConvertToString(ev))
		rvStr := strings.TrimSpace(shared.ConvertToString(rv))
		if evStr != rvStr {
			return false
		}
	}
	return true
}

// classifiedRecordsResult holds records grouped by how they must be synced to ClickHouse.
// - KeyUnchangedRecords: ORDER BY key (published, status, edition_type) unchanged; insert-only refresh
// - KeyChangedRecords: ORDER BY key changed (e.g. edition_type current→past); must DELETE then re-insert
// - NewRecords: Row does not exist in ClickHouse yet; insert new row
type classifiedRecordsResult struct {
	KeyUnchangedRecords  []map[string]interface{} // was Situation A
	KeyChangedRecords    []map[string]interface{} // was Situation B Update
	NewRecords           []map[string]interface{} // was Situation B Insert
}

func classifyRecordsByOrderByKey(
	conn driver.Conn,
	records []map[string]interface{},
	tableName string,
	tableCfg *TableIncrementalConfig,
) (classifiedRecordsResult, error) {
	var result classifiedRecordsResult
	if len(records) == 0 || tableCfg == nil {
		return result, nil
	}

	existing, err := fetchExistingOrderByKey(conn, records, tableName, tableCfg)
	if err != nil {
		return result, fmt.Errorf("fetch existing ORDER BY key: %w", err)
	}

	for _, rec := range records {
		key := tableCfg.RecordMapKey(rec)
		existingRow, exists := existing[key]

		if !exists {
			result.NewRecords = append(result.NewRecords, rec)
			continue
		}

		if orderByValuesEqual(existingRow, rec, tableCfg.OrderByColumns) {
			result.KeyUnchangedRecords = append(result.KeyUnchangedRecords, rec)
		} else {
			result.KeyChangedRecords = append(result.KeyChangedRecords, rec)
		}
	}

	return result, nil
}

const deleteBatchSize = 500 // Max rows per ALTER DELETE query to avoid query size limits

func deleteRowsByPrimaryKey(
	conn driver.Conn,
	records []map[string]interface{},
	tableName string,
	tableCfg *TableIncrementalConfig,
) error {
	if len(records) == 0 || tableCfg == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	pkCols := strings.Join(tableCfg.PrimaryKeyColumns, ", ")

	for batchStart := 0; batchStart < len(records); batchStart += deleteBatchSize {
		batchEnd := batchStart + deleteBatchSize
		if batchEnd > len(records) {
			batchEnd = len(records)
		}
		batch := records[batchStart:batchEnd]

		tuples := make([]string, 0, len(batch))
		for _, rec := range batch {
			tuples = append(tuples, tableCfg.ExtractPKTuple(rec))
		}

		deleteQuery := fmt.Sprintf(
			`ALTER TABLE %s DELETE WHERE (%s) IN (%s) SETTINGS mutations_sync = 1`,
			tableName, pkCols, strings.Join(tuples, ","),
		)

		log.Printf("[Query] ALTER DELETE batch %d-%d (%d pairs)", batchStart+1, batchEnd, len(batch))
		tuplesForLog := strings.Join(tuples, ", ")
		const maxLogLen = 2000
		if len(tuplesForLog) > maxLogLen {
			tuplesForLog = tuplesForLog[:maxLogLen] + "... (truncated)"
		}
		shared.WriteIncrementalLog(fmt.Sprintf("   [DELETE] Batch %d-%d: %d pairs: %s", batchStart+1, batchEnd, len(batch), tuplesForLog))

		if err := conn.Exec(ctx, deleteQuery); err != nil {
			return fmt.Errorf("ALTER DELETE batch %d-%d: %w", batchStart+1, batchEnd, err)
		}

		log.Printf("Deleted %d/%d rows (key changed)", batchEnd, len(records))
	}

	log.Printf("Deleted %d rows in ClickHouse (key changed - will re-insert)", len(records))
	return nil
}

func extractCityIDsFromCompanies(companyData []map[string]interface{}) []int64 {
	var ids []int64
	seen := make(map[int64]bool)
	for _, c := range companyData {
		if cityID, ok := c["company_city"].(int64); ok && cityID > 0 && !seen[cityID] {
			seen[cityID] = true
			ids = append(ids, cityID)
		}
	}
	return ids
}

func extractCityIDsFromVenues(venueData []map[string]interface{}) []int64 {
	var ids []int64
	seen := make(map[int64]bool)
	for _, v := range venueData {
		if cityID, ok := v["venue_city"].(int64); ok && cityID > 0 && !seen[cityID] {
			seen[cityID] = true
			ids = append(ids, cityID)
		}
	}
	return ids
}

func dedupeIDs(slices ...[]int64) []int64 {
	seen := make(map[int64]bool)
	var out []int64
	for _, s := range slices {
		for _, id := range s {
			if id > 0 && !seen[id] {
				seen[id] = true
				out = append(out, id)
			}
		}
	}
	return out
}

func indexByID10x(data []map[string]interface{}) map[int64]map[string]interface{} {
	m := make(map[int64]map[string]interface{})
	for _, d := range data {
		if id, ok := d["id_10x"].(int64); ok {
			m[id] = d
		}
	}
	return m
}

func indexByID(data []map[string]interface{}, key string) map[int64]map[string]interface{} {
	m := make(map[int64]map[string]interface{})
	for _, d := range data {
		if id, ok := d[key].(int64); ok {
			m[id] = d
		}
	}
	return m
}

