package eventdata

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"seeders/microservice"
	"seeders/shared"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/elastic/go-elasticsearch/v6"
)

// CompanyEventDataRecord holds one row for the companyEventData_ch table.
type CompanyEventDataRecord struct {
	EventID             uint32  `ch:"eventId"`
	EditionID           uint32  `ch:"editionId"`
	EditionCompanyID    *uint32 `ch:"editionCompanyId"`
	EventName           *string `ch:"eventName"`
	EventDescription    *string `ch:"eventDescription"`
	EditionVenue        *uint32 `ch:"editionVenue"`
	EditionVenueName    *string `ch:"editionVenueName"`
	EditionCity         *uint32 `ch:"editionCity"`
	EditionCityName     *string `ch:"editionCityName"`
	EditionStateName    *string `ch:"editionStateName"`
	EditionCountry      *string `ch:"editionCountry"`
	EditionAudience     *string `ch:"editionAudience"`
	EditionFunctionality string  `ch:"editionFunctionality"`
	EditionType         string  `ch:"editionType"`
	EventStatus         string  `ch:"eventStatus"`
	EventPublished      int8    `ch:"eventPublished"`
	EditionStartDate    string  `ch:"editionStartDate"`
	EditionEndDate      string  `ch:"editionEndDate"`
	EditionWebsite      *string `ch:"editionWebsite"`
	Created             string  `ch:"created"`
	LastUpdatedAt       string  `ch:"lastUpdatedAt"`
}

const editionTypeNA = "NA"

func ProcessCompanyEventDataOnly(mysqlDB *sql.DB, clickhouseConn driver.Conn, esClient *elasticsearch.Client, config shared.Config) {
	log.Println("=== Starting COMPANY EVENT DATA Processing ===")

	totalRecords, minID, maxID, err := shared.GetTotalRecordsAndIDRange(mysqlDB, "event_edition")
	if err != nil {
		log.Fatal("Failed to get total records and ID range from event_edition:", err)
	}
	log.Printf("Total event_edition records: %d, Min ID: %d, Max ID: %d", totalRecords, minID, maxID)

	currentEditionMap, err := fetchCurrentEditionMap(mysqlDB)
	if err != nil {
		log.Fatal("Failed to fetch current edition map from event:", err)
	}
	log.Printf("Loaded current_edition_id for %d events", len(currentEditionMap))

	if config.NumChunks <= 0 {
		config.NumChunks = 5
	}
	chunkSize := (maxID - minID + 1) / config.NumChunks
	if chunkSize == 0 {
		chunkSize = 1
	}
	log.Printf("Processing company event data in %d chunks with chunk size: %d", config.NumChunks, chunkSize)

	results := make(chan string, config.NumChunks)
	semaphore := make(chan struct{}, config.NumWorkers)

	for i := 0; i < config.NumChunks; i++ {
		startID := minID + (i * chunkSize)
		endID := startID + chunkSize - 1
		if i == config.NumChunks-1 {
			endID = maxID
		}
		if i > 0 {
			time.Sleep(3 * time.Second)
		}
		semaphore <- struct{}{}
		go func(chunkNum, start, end int) {
			defer func() { <-semaphore }()
			processCompanyEventDataChunk(mysqlDB, clickhouseConn, esClient, config, start, end, chunkNum, currentEditionMap, results)
		}(i+1, startID, endID)
	}

	for i := 0; i < config.NumChunks; i++ {
		result := <-results
		log.Printf("Company Event Data Result: %s", result)
	}
	log.Println("Company event data processing completed!")
}

// fetchCompanyEventDataElasticsearchData fetches id, name, description from event_v4 index for the given event IDs.
func fetchCompanyEventDataElasticsearchData(esClient *elasticsearch.Client, indexName string, eventIDs []int64) map[int64]map[string]interface{} {
	if len(eventIDs) == 0 {
		return nil
	}
	results := make(map[int64]map[string]interface{})
	eventIDStrings := make([]string, len(eventIDs))
	for i, id := range eventIDs {
		eventIDStrings[i] = strconv.FormatInt(id, 10)
	}

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"terms": map[string]interface{}{
				"id": eventIDStrings,
			},
		},
		"size":    len(eventIDs),
		"_source": []string{"id", "name", "description"},
	}

	queryJSON, _ := json.Marshal(query)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	searchRes, err := esClient.Search(
		esClient.Search.WithContext(ctx),
		esClient.Search.WithIndex(indexName),
		esClient.Search.WithBody(strings.NewReader(string(queryJSON))),
	)
	if err != nil {
		log.Printf("Warning: Failed to search Elasticsearch for company event data: %v", err)
		return results
	}
	defer searchRes.Body.Close()

	if searchRes.IsError() {
		log.Printf("Warning: Elasticsearch search failed: %v", searchRes.Status())
		return results
	}

	var result map[string]interface{}
	if err := json.NewDecoder(searchRes.Body).Decode(&result); err != nil {
		log.Printf("Warning: Failed to decode Elasticsearch response: %v", err)
		return results
	}

	hits, ok := result["hits"].(map[string]interface{})
	if !ok {
		return results
	}
	hitsArray, ok := hits["hits"].([]interface{})
	if !ok || len(hitsArray) == 0 {
		return results
	}

	for _, hit := range hitsArray {
		hitMap, ok := hit.(map[string]interface{})
		if !ok {
			continue
		}
		source, ok := hitMap["_source"].(map[string]interface{})
		if !ok {
			continue
		}

		var eventIDInt int64
		if eventIDStr, ok := source["id"].(string); ok {
			if parsedID, err := strconv.ParseInt(eventIDStr, 10, 64); err == nil {
				eventIDInt = parsedID
			} else {
				continue
			}
		} else if eventIDNum, ok := source["id"].(float64); ok {
			eventIDInt = int64(eventIDNum)
		} else {
			continue
		}
		results[eventIDInt] = source
	}
	log.Printf("Retrieved ES data (name, description) for %d events from %s", len(results), indexName)
	return results
}

func fetchCurrentEditionMap(db *sql.DB) (map[int64]int64, error) {
	query := `
		SELECT id, event_edition
		FROM event
		WHERE event_edition IS NOT NULL`
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	m := make(map[int64]int64)
	for rows.Next() {
		var eventID int64
		var currentEditionID sql.NullInt64
		if err := rows.Scan(&eventID, &currentEditionID); err != nil {
			return nil, err
		}
		if currentEditionID.Valid {
			m[eventID] = currentEditionID.Int64
		}
	}
	return m, rows.Err()
}

func processCompanyEventDataChunk(
	mysqlDB *sql.DB,
	clickhouseConn driver.Conn,
	esClient *elasticsearch.Client,
	config shared.Config,
	startID, endID int,
	chunkNum int,
	currentEditionMap map[int64]int64,
	results chan<- string,
) {
	log.Printf("Processing company event data chunk %d: edition ID range %d-%d", chunkNum, startID, endID)
	totalRecords := endID - startID + 1
	processed := 0
	start := startID
	lastUpdatedAt := time.Now().Format("2006-01-02 15:04:05")

	for {
		batchData, err := buildCompanyEventDataMigrationData(mysqlDB, start, endID, config.BatchSize)
		if err != nil {
			results <- fmt.Sprintf("Company event data chunk %d batch error: %v", chunkNum, err)
			return
		}
		if len(batchData) == 0 {
			break
		}

		processed += len(batchData)
		progress := float64(processed) / float64(totalRecords) * 100
		log.Printf("Company event data chunk %d: Retrieved %d records in batch (%.1f%% complete)", chunkNum, len(batchData), progress)

		eventIDSet := make(map[int64]bool)
		for _, row := range batchData {
			if eventID := rowInt64(row, "eventId"); eventID > 0 {
				eventIDSet[eventID] = true
			}
		}
		eventIDs := make([]int64, 0, len(eventIDSet))
		for id := range eventIDSet {
			eventIDs = append(eventIDs, id)
		}
		var esData map[int64]map[string]interface{}
		if esClient != nil && config.ElasticsearchIndex != "" && len(eventIDs) > 0 {
			esData = fetchCompanyEventDataElasticsearchData(esClient, config.ElasticsearchIndex, eventIDs)
		}

		// Attach current_edition_id to each row and build currentEditionStartDates / currentEditionIDs from this batch
		currentEditionStartDates := make(map[int64]interface{})
		currentEditionIDs := make(map[int64]int64)
		for _, row := range batchData {
			eventID := rowInt64(row, "eventId")
			if eventID == 0 {
				continue
			}
			if curID, ok := currentEditionMap[eventID]; ok {
				row["current_edition_id"] = curID
				editionID := rowInt64(row, "editionId")
				if editionID == curID {
					currentEditionStartDates[eventID] = row["editionStartDate"]
					currentEditionIDs[eventID] = editionID
				}
			}
		}

		var records []CompanyEventDataRecord
		for _, row := range batchData {
			rec := mapRowToCompanyEventDataRecord(row, lastUpdatedAt, currentEditionStartDates, currentEditionIDs, esData)
			records = append(records, rec)
		}

		if len(records) > 0 {
			insertErr := shared.RetryWithBackoff(
				func() error {
					return insertCompanyEventDataIntoClickHouse(clickhouseConn, records, config.ClickHouseWorkers)
				},
				3,
			)
			if insertErr != nil {
				log.Printf("Company event data chunk %d: Insert failed after retries: %v", chunkNum, insertErr)
				results <- fmt.Sprintf("Company event data chunk %d: Failed to insert %d records", chunkNum, len(records))
				return
			}
			log.Printf("Company event data chunk %d: Inserted %d records into companyEventData_temp", chunkNum, len(records))
		}

		if len(batchData) > 0 {
			if lastID := rowInt64(batchData[len(batchData)-1], "editionId"); lastID > 0 {
				start = int(lastID) + 1
			}
		}
		if len(batchData) < config.BatchSize {
			break
		}
	}
	results <- fmt.Sprintf("Company event data chunk %d: Completed successfully", chunkNum)
}

func rowInt64(row map[string]interface{}, key string) int64 {
	v, ok := row[key]
	if !ok || v == nil {
		return 0
	}
	switch x := v.(type) {
	case int64:
		return x
	case int32:
		return int64(x)
	case int:
		return int64(x)
	case float64:
		return int64(x)
	default:
		return int64(shared.ConvertToUInt32(v))
	}
}

func buildCompanyEventDataMigrationData(db *sql.DB, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT
			e.id                  AS eventId,
			ee.id                 AS editionId,
			ee.company_id         AS editionCompanyId,
			ee.city               AS editionCity,
			ct.name               AS editionCityName,
			ct.state              AS editionStateName,
			e.country             AS editionCountry,
			e.event_audience      AS editionAudience,
			e.functionality       AS editionFunctionality,
			e.status              AS eventStatus,
			e.published           AS eventPublished,
			ee.start_date         AS editionStartDate,
			ee.end_date           AS editionEndDate,
			ee.website             AS editionWebsite,
			v.id                  AS editionVenue,
			v.name                AS editionVenueName,
			ee.created            AS created
		FROM event_edition ee
		INNER JOIN event e ON ee.event = e.id
		LEFT JOIN city ct ON ee.city = ct.id
		LEFT JOIN venue v ON ee.venue = v.id
		WHERE ee.id >= %d AND ee.id <= %d
		ORDER BY ee.id
		LIMIT %d`, startID, endID, batchSize)

	log.Printf("[SQL Company Event Data] %s", strings.TrimSpace(query))
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
			if values[i] != nil {
				row[col] = values[i]
			}
		}
		results = append(results, row)
	}
	return results, rows.Err()
}

func mapRowToCompanyEventDataRecord(
	row map[string]interface{},
	lastUpdatedAt string,
	currentEditionStartDates map[int64]interface{},
	currentEditionIDs map[int64]int64,
	esData map[int64]map[string]interface{},
) CompanyEventDataRecord {
	eventID := rowInt64(row, "eventId")
	editionID := rowInt64(row, "editionId")

	editionType := editionTypeNA
	if curID := currentEditionIDs[eventID]; curID != 0 {
		if et := microservice.DetermineEditionType(
			row["editionStartDate"],
			currentEditionStartDates[eventID],
			editionID,
			curID,
		); et != nil {
			editionType = *et
		}
	}

	eventStatus := shared.SafeConvertToString(row["eventStatus"])
	if eventStatus == "" {
		eventStatus = "A"
	}
	editionCountry := toFixedString2(shared.ConvertToStringPtr(row["editionCountry"]))
	editionStartDate := shared.SafeConvertToDateString(row["editionStartDate"])
	editionEndDate := shared.SafeConvertToDateString(row["editionEndDate"])
	if editionStartDate == "" {
		editionStartDate = "1970-01-01"
	}
	if editionEndDate == "" {
		editionEndDate = "1970-01-01"
	}

	editionFunctionality := shared.SafeConvertToString(row["editionFunctionality"])

	// Event name and description from event_v4 ES index
	var eventName, eventDescription *string
	if esData != nil {
		if esRow := esData[eventID]; esRow != nil {
			eventName = shared.ConvertToStringPtr(esRow["name"])
			eventDescription = shared.ConvertToStringPtr(esRow["description"])
		}
	}

	return CompanyEventDataRecord{
		EventID:               shared.ConvertToUInt32(row["eventId"]),
		EditionID:             shared.ConvertToUInt32(row["editionId"]),
		EditionCompanyID:      shared.ConvertToUInt32Ptr(row["editionCompanyId"]),
		EventName:             eventName,
		EventDescription:      eventDescription,
		EditionVenue:          shared.ConvertToUInt32Ptr(row["editionVenue"]),
		EditionVenueName:      shared.ConvertToStringPtr(row["editionVenueName"]),
		EditionCity:           shared.ConvertToUInt32Ptr(row["editionCity"]),
		EditionCityName:       shared.ConvertToStringPtr(row["editionCityName"]),
		EditionStateName:      shared.ConvertToStringPtr(row["editionStateName"]),
		EditionCountry:        editionCountry,
		EditionAudience:       shared.ConvertToStringPtr(row["editionAudience"]),
		EditionFunctionality:  editionFunctionality,
		EditionType:           editionType,
		EventStatus:           eventStatus,
		EventPublished:        shared.SafeConvertToInt8(row["eventPublished"]),
		EditionStartDate:      editionStartDate,
		EditionEndDate:        editionEndDate,
		EditionWebsite:        shared.ConvertToStringPtr(row["editionWebsite"]),
		Created:               shared.SafeClickHouseDateTimeString(row["created"]),
		LastUpdatedAt:         lastUpdatedAt,
	}
}

func toFixedString2(s *string) *string {
	if s == nil || *s == "" {
		return nil
	}
	trimmed := strings.TrimSpace(*s)
	if len(trimmed) >= 2 {
		two := strings.ToUpper(trimmed[:2])
		return &two
	}
	return s
}

func insertCompanyEventDataIntoClickHouse(clickhouseConn driver.Conn, records []CompanyEventDataRecord, numWorkers int) error {
	if len(records) == 0 {
		return nil
	}
	if numWorkers <= 1 {
		return insertCompanyEventDataSingleWorker(clickhouseConn, records)
	}
	batchSize := (len(records) + numWorkers - 1) / numWorkers
	results := make(chan error, numWorkers)
	semaphore := make(chan struct{}, numWorkers)
	for i := 0; i < numWorkers; i++ {
		start := i * batchSize
		end := start + batchSize
		if end > len(records) {
			end = len(records)
		}
		if start >= len(records) {
			break
		}
		semaphore <- struct{}{}
		go func(s, e int) {
			defer func() { <-semaphore }()
			results <- insertCompanyEventDataSingleWorker(clickhouseConn, records[s:e])
		}(start, end)
	}
	for i := 0; i < numWorkers && i*batchSize < len(records); i++ {
		if err := <-results; err != nil {
			return err
		}
	}
	return nil
}

func insertCompanyEventDataSingleWorker(clickhouseConn driver.Conn, records []CompanyEventDataRecord) error {
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	batch, err := clickhouseConn.PrepareBatch(ctx, `
		INSERT INTO companyEventData_temp (
			eventId, editionId, editionCompanyId, eventName, eventDescription, editionVenue, editionVenueName, editionCity, editionCityName, editionStateName, editionCountry, editionAudience, editionFunctionality, editionType,
			eventStatus, eventPublished, editionStartDate, editionEndDate, editionWebsite, created, lastUpdatedAt
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare ClickHouse batch: %v", err)
	}
	for _, r := range records {
		err := batch.Append(
			r.EventID, r.EditionID, r.EditionCompanyID, r.EventName, r.EventDescription, r.EditionVenue, r.EditionVenueName, r.EditionCity, r.EditionCityName, r.EditionStateName, r.EditionCountry, r.EditionAudience, r.EditionFunctionality, r.EditionType,
			r.EventStatus, r.EventPublished, r.EditionStartDate, r.EditionEndDate, r.EditionWebsite, r.Created, r.LastUpdatedAt,
		)
		if err != nil {
			return fmt.Errorf("failed to append record: %v", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send ClickHouse batch: %v", err)
	}
	log.Printf("OK: Inserted %d company event data records into companyEventData_temp", len(records))
	return nil
}
