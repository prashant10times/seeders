package utils

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"seeders/shared"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/elastic/go-elasticsearch/v6"
)

type VisitorSpreadRecord struct {
	EventID       uint32 `ch:"event_id"`
	VisitorSpread string `ch:"visitor_spread"`
	Version       uint32 `ch:"version"`
}

func ProcessVisitorSpreadOnly(mysqlDB *sql.DB, clickhouseConn driver.Conn, esClient *elasticsearch.Client, config shared.Config) {
	log.Println("=== Starting VISITOR SPREAD ONLY Processing ===")

	log.Println("Fetching distinct event IDs from event_edition table...")
	eventIDs, err := getDistinctEventIDs(mysqlDB)
	if err != nil {
		log.Fatal("Failed to get distinct event IDs:", err)
	}

	log.Printf("Found %d distinct event IDs", len(eventIDs))

	if len(eventIDs) == 0 {
		log.Println("No event IDs found, nothing to process")
		return
	}

	if config.NumChunks <= 0 {
		config.NumChunks = 5
	}

	chunkSize := len(eventIDs) / config.NumChunks
	if chunkSize == 0 {
		chunkSize = 1
	}

	log.Printf("Processing visitor spread data in %d chunks with chunk size: %d", config.NumChunks, chunkSize)

	var totalRecordsProcessed int64
	var totalRecordsInserted int64
	var globalCountMutex sync.Mutex

	results := make(chan string, config.NumChunks)
	semaphore := make(chan struct{}, config.NumWorkers)

	for i := 0; i < config.NumChunks; i++ {
		startIdx := i * chunkSize
		endIdx := startIdx + chunkSize

		if i == config.NumChunks-1 {
			endIdx = len(eventIDs)
		}

		chunkEventIDs := eventIDs[startIdx:endIdx]

		if i > 0 {
			delay := 3 * time.Second
			log.Printf("Waiting %v before launching visitor spread chunk %d...", delay, i+1)
			time.Sleep(delay)
		}

		semaphore <- struct{}{}
		go func(chunkNum int, eventIDChunk []int64) {
			defer func() { <-semaphore }()
			processVisitorSpreadChunk(clickhouseConn, esClient, config, eventIDChunk, chunkNum, results, &totalRecordsProcessed, &totalRecordsInserted, &globalCountMutex)
		}(i+1, chunkEventIDs)
	}

	for i := 0; i < config.NumChunks; i++ {
		result := <-results
		log.Printf("Visitor Spread Result: %s", result)
	}

	globalCountMutex.Lock()
	log.Printf("=== FINAL SUMMARY ===")
	log.Printf("Total records processed: %d", totalRecordsProcessed)
	log.Printf("Total records inserted: %d", totalRecordsInserted)
	globalCountMutex.Unlock()

	log.Println("Visitor Spread processing completed!")
}

func getDistinctEventIDs(mysqlDB *sql.DB) ([]int64, error) {
	query := "SELECT DISTINCT event FROM event_edition WHERE event IS NOT NULL ORDER BY event"

	rows, err := mysqlDB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query distinct event IDs: %v", err)
	}
	defer rows.Close()

	var eventIDs []int64
	for rows.Next() {
		var eventID int64
		if err := rows.Scan(&eventID); err != nil {
			return nil, fmt.Errorf("failed to scan event ID: %v", err)
		}
		eventIDs = append(eventIDs, eventID)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over event IDs: %v", err)
	}

	return eventIDs, nil
}

func convertVisitorSpreadDataToRecords(visitorSpreadData map[int64]map[string]interface{}, allEventIDs []int64) []VisitorSpreadRecord {
	records := make([]VisitorSpreadRecord, 0, len(allEventIDs))
	eventsWithoutVisitorData := make([]int64, 0, 10)

	for _, eventID := range allEventIDs {
		var data map[string]interface{}

		if visitorData, exists := visitorSpreadData[eventID]; exists {
			data = visitorData
		} else {
			data = make(map[string]interface{})

			if len(eventsWithoutVisitorData) < 10 {
				eventsWithoutVisitorData = append(eventsWithoutVisitorData, eventID)
			}
		}

		if eventID <= allEventIDs[0]+2 {
			log.Printf("DEBUG: Event %d visitor spread data: %+v", eventID, data)
		}

		var jsonStr string
		if visitorSpreadJSON, exists := data["visitor_spread_json"]; exists {
			if jsonString, ok := visitorSpreadJSON.(string); ok {
				jsonStr = jsonString
			} else {
				log.Printf("Warning: visitor_spread_json is not a string for event %d", eventID)
				jsonStr = "{}"
			}
		} else {
			jsonData, err := json.Marshal(data)
			if err != nil {
				log.Printf("Warning: Failed to marshal visitor spread data for event %d: %v", eventID, err)
				jsonStr = "{}"
			} else {
				jsonStr = string(jsonData)
			}
		}

		if eventID <= allEventIDs[0]+2 {
			log.Printf("DEBUG: Event %d final JSON string: %s", eventID, jsonStr)
		}

		record := VisitorSpreadRecord{
			EventID:       uint32(eventID),
			VisitorSpread: jsonStr,
			Version:       1,
		}
		records = append(records, record)
	}

	if len(eventsWithoutVisitorData) > 0 {
		log.Printf("INFO: First %d event IDs without visitor spread data: %v", len(eventsWithoutVisitorData), eventsWithoutVisitorData)
	}

	log.Printf("INFO: Converted %d total events to records (%d with visitor data, %d without)",
		len(records), len(visitorSpreadData), len(allEventIDs)-len(visitorSpreadData))

	return records
}

func processVisitorSpreadChunk(clickhouseConn driver.Conn, esClient *elasticsearch.Client, config shared.Config, eventIDs []int64, chunkNum int, results chan<- string, totalRecordsProcessed *int64, totalRecordsInserted *int64, globalCountMutex *sync.Mutex) {
	log.Printf("Processing visitor spread chunk %d: %d event IDs", chunkNum, len(eventIDs))

	visitorSpreadData, err := fetchVisitorSpreadDataFromElasticsearch(esClient, eventIDs)
	if err != nil {
		log.Printf("Visitor spread chunk %d: Failed to fetch Elasticsearch data: %v", chunkNum, err)
		results <- fmt.Sprintf("Visitor spread chunk %d: Failed to fetch Elasticsearch data: %v", chunkNum, err)
		return
	}

	log.Printf("Visitor spread chunk %d: Retrieved visitor spread data for %d events", chunkNum, len(visitorSpreadData))

	records := convertVisitorSpreadDataToRecords(visitorSpreadData, eventIDs)
	if len(records) == 0 {
		log.Printf("Visitor spread chunk %d: No records to insert", chunkNum)
		results <- fmt.Sprintf("Visitor spread chunk %d: No records to insert", chunkNum)
		return
	}

	log.Printf("Visitor spread chunk %d: Inserting %d records into ClickHouse with %d workers", chunkNum, len(records), config.ClickHouseWorkers)
	err = insertVisitorSpreadDataIntoClickHouse(clickhouseConn, records, config.ClickHouseWorkers)
	if err != nil {
		log.Printf("ERROR: Visitor spread chunk %d: Failed to insert data into ClickHouse: %v", chunkNum, err)
		results <- fmt.Sprintf("Visitor spread chunk %d: Failed to insert data into ClickHouse: %v", chunkNum, err)
		return
	}

	insertedCount := len(records)

	globalCountMutex.Lock()
	*totalRecordsProcessed += int64(len(visitorSpreadData))
	*totalRecordsInserted += int64(insertedCount)
	globalCountMutex.Unlock()

	log.Printf("Visitor spread chunk %d: Successfully processed %d records, inserted %d records", chunkNum, len(visitorSpreadData), insertedCount)
	results <- fmt.Sprintf("Visitor spread chunk %d: Successfully processed %d records, inserted %d records", chunkNum, len(visitorSpreadData), insertedCount)
}

func fetchVisitorSpreadDataFromElasticsearch(esClient *elasticsearch.Client, eventIDs []int64) (map[int64]map[string]interface{}, error) {
	if len(eventIDs) == 0 {
		return nil, nil
	}

	results := make(map[int64]map[string]interface{})
	batchSize := 200

	expectedBatches := (len(eventIDs) + batchSize - 1) / batchSize
	resultsChan := make(chan map[int64]map[string]interface{}, expectedBatches)
	semaphore := make(chan struct{}, 5)

	var wg sync.WaitGroup

	for i := 0; i < len(eventIDs); i += batchSize {
		endIdx := i + batchSize
		if endIdx > len(eventIDs) {
			endIdx = len(eventIDs)
		}

		eventIDBatch := eventIDs[i:endIdx]
		batchNum := i/batchSize + 1

		wg.Add(1)
		go func(batch []int64, batchNum int) {
			defer func() {
				<-semaphore
				wg.Done()
			}()

			if batchNum > 1 {
				time.Sleep(100 * time.Millisecond)
			}

			var batchResults map[int64]map[string]interface{}
			var lastError error
			maxRetries := 3
			for retry := 0; retry <= maxRetries; retry++ {
				batchResults, lastError = fetchVisitorSpreadBatch(esClient, batch)
				if lastError == nil || retry == maxRetries {
					if retry > 0 && lastError != nil {
						log.Printf("Elasticsearch visitor spread batch %d: Success after %d retries, got %d results (last error: %v)", batchNum, retry, len(batchResults), lastError)
					} else if retry > 0 {
						log.Printf("Elasticsearch visitor spread batch %d: Success after %d retries, got %d results", batchNum, retry, len(batchResults))
					}
					break
				}
				if retry < maxRetries {
					backoffTime := time.Duration(retry+1) * 3 * time.Second
					log.Printf("Elasticsearch visitor spread batch %d: Retry %d/%d after %v backoff (error: %v)", batchNum, retry+1, maxRetries, backoffTime, lastError)
					time.Sleep(backoffTime)
				}
			}
			resultsChan <- batchResults
		}(eventIDBatch, batchNum)

		semaphore <- struct{}{}
	}

	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	for batchResults := range resultsChan {
		for eventID, data := range batchResults {
			results[eventID] = data
		}
	}

	log.Printf("OK: Retrieved visitor spread data for %d events in %d batches", len(results), expectedBatches)
	return results, nil
}

func fetchVisitorSpreadBatch(esClient *elasticsearch.Client, eventIDs []int64) (map[int64]map[string]interface{}, error) {
	if len(eventIDs) == 0 {
		return nil, nil
	}

	eventIDStrings := make([]string, len(eventIDs))
	for i, id := range eventIDs {
		eventIDStrings[i] = fmt.Sprintf("%d", id)
	}

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"terms": map[string]interface{}{
				"id": eventIDStrings,
			},
		},
		"_source": []string{"id", "visitor_spread"},
		"size":    len(eventIDs),
	}

	queryJSON, err := json.Marshal(query)
	if err != nil {
		log.Printf("Warning: Failed to marshal Elasticsearch query: %v", err)
		return nil, fmt.Errorf("failed to marshal Elasticsearch query: %v", err)
	}

	log.Printf("DEBUG: Elasticsearch visitor spread query: %s", string(queryJSON))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	searchRes, err := esClient.Search(
		esClient.Search.WithContext(ctx),
		esClient.Search.WithIndex("event_v4"),
		esClient.Search.WithBody(strings.NewReader(string(queryJSON))),
	)
	if err != nil {
		log.Printf("Warning: Failed to search Elasticsearch for visitor spread batch: %v", err)
		return nil, fmt.Errorf("failed to search Elasticsearch: %v", err)
	}
	defer searchRes.Body.Close()

	if searchRes.IsError() {
		log.Printf("Warning: Elasticsearch visitor spread search failed: %v", searchRes.Status())
		return nil, fmt.Errorf("elasticsearch search failed with status: %v", searchRes.Status())
	}

	var result map[string]interface{}
	if err := json.NewDecoder(searchRes.Body).Decode(&result); err != nil {
		log.Printf("Warning: Failed to decode Elasticsearch visitor spread response: %v", err)
		return nil, fmt.Errorf("failed to decode elasticsearch response: %v", err)
	}

	hits := result["hits"].(map[string]interface{})
	hitsArray := hits["hits"].([]interface{})

	var totalHitsValue interface{}
	if totalHits, ok := hits["total"].(map[string]interface{}); ok {
		totalHitsValue = totalHits["value"]
	} else if totalHits, ok := hits["total"].(float64); ok {
		totalHitsValue = totalHits
	} else {
		totalHitsValue = "unknown"
	}

	log.Printf("DEBUG: Elasticsearch visitor spread response - Total hits: %v, Returned hits: %d", totalHitsValue, len(hitsArray))

	results := make(map[int64]map[string]interface{})
	processedCount := 0
	skippedCount := 0

	for _, hit := range hitsArray {
		hitMap := hit.(map[string]interface{})
		source := hitMap["_source"].(map[string]interface{})

		eventID, ok := source["id"].(string)
		if !ok {
			skippedCount++
			continue
		}

		eventIDInt, err := strconv.ParseInt(eventID, 10, 64)
		if err != nil {
			log.Printf("Warning: Failed to parse event ID '%s' to int64: %v", eventID, err)
			skippedCount++
			continue
		}
		visitorSpreadData := make(map[string]interface{})

		if visitorSpread, exists := source["visitor_spread"]; exists && visitorSpread != nil {
			log.Printf("DEBUG: Event %d raw visitor_spread: %+v", eventIDInt, visitorSpread)

			processedVisitorData := processVisitorSpreadData(eventIDInt, visitorSpread)
			visitorSpreadData = processedVisitorData
		} else {
			log.Printf("DEBUG: Event %d has no visitor_spread data", eventIDInt)
		}

		results[eventIDInt] = visitorSpreadData
		processedCount++
	}

	log.Printf("DEBUG: Processed %d events", processedCount)

	return results, nil
}

func processVisitorSpreadData(eventID int64, visitorSpread interface{}) map[string]interface{} {
	processedData := make(map[string]interface{})
	visitorSpreadJSONBytes, err := json.Marshal(visitorSpread)
	if err != nil {
		log.Printf("Event %d: Failed to marshal visitor spread data: %v", eventID, err)
		return map[string]interface{}{
			"visitor_spread_json": "{}",
		}
	}

	visitorSpreadJSON := string(visitorSpreadJSONBytes)
	log.Printf("DEBUG: Event %d raw visitor spread JSON: %s", eventID, visitorSpreadJSON)

	var visitorSpreadMap map[string]interface{}
	if err := json.Unmarshal([]byte(visitorSpreadJSON), &visitorSpreadMap); err != nil {
		log.Printf("Event %d: Failed to unmarshal visitor spread JSON: %v", eventID, err)
		return map[string]interface{}{
			"visitor_spread_json": "{}",
		}
	}

	if userByCntry, exists := visitorSpreadMap["user_by_cntry"]; exists && userByCntry != nil {
		log.Printf("DEBUG: Event %d user_by_cntry: %+v", eventID, userByCntry)

		if userByCntryJSON, err := json.Marshal(userByCntry); err == nil {
			processedData["user_by_cntry"] = string(userByCntryJSON)
		} else {
			log.Printf("Event %d: Failed to marshal user_by_cntry: %v", eventID, err)
			processedData["user_by_cntry"] = "[]"
		}
	} else {
		processedData["user_by_cntry"] = "[]"
	}

	if userByDesignation, exists := visitorSpreadMap["user_by_designation"]; exists && userByDesignation != nil {
		log.Printf("DEBUG: Event %d user_by_designation: %+v", eventID, userByDesignation)

		if userByDesignationJSON, err := json.Marshal(userByDesignation); err == nil {
			processedData["user_by_designation"] = string(userByDesignationJSON)
		} else {
			log.Printf("Event %d: Failed to marshal user_by_designation: %v", eventID, err)
			processedData["user_by_designation"] = "[]"
		}
	} else {
		processedData["user_by_designation"] = "[]"
	}

	if finalJSON, err := json.Marshal(processedData); err == nil {
		log.Printf("DEBUG: Event %d final processed JSON: %s", eventID, string(finalJSON))
		return map[string]interface{}{
			"visitor_spread_json": string(finalJSON),
		}
	} else {
		log.Printf("Event %d: Failed to marshal final visitor spread data: %v", eventID, err)
		return map[string]interface{}{
			"visitor_spread_json": "{}",
		}
	}
}

func insertVisitorSpreadDataIntoClickHouse(clickhouseConn driver.Conn, records []VisitorSpreadRecord, numWorkers int) error {
	if len(records) == 0 {
		return nil
	}

	if numWorkers <= 1 {
		return insertVisitorSpreadDataSingleWorker(clickhouseConn, records)
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
		go func(start, end int) {
			defer func() { <-semaphore }()
			batch := records[start:end]
			err := insertVisitorSpreadDataSingleWorker(clickhouseConn, batch)
			results <- err
		}(start, end)
	}

	for i := 0; i < numWorkers && i*batchSize < len(records); i++ {
		if err := <-results; err != nil {
			return err
		}
	}

	return nil
}

func insertVisitorSpreadDataSingleWorker(clickhouseConn driver.Conn, records []VisitorSpreadRecord) error {
	if len(records) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	batch, err := clickhouseConn.PrepareBatch(ctx, `
		INSERT INTO event_visitorSpread_ch (
			event_id, visitor_spread, version
		)
	`)
	if err != nil {
		log.Printf("ERROR: Failed to prepare ClickHouse batch for visitor spread: %v", err)
		return fmt.Errorf("failed to prepare ClickHouse batch: %v", err)
	}

	for _, record := range records {
		err := batch.Append(
			record.EventID,
			record.VisitorSpread,
			record.Version,
		)
		if err != nil {
			log.Printf("ERROR: Failed to append visitor spread record to batch: %v", err)
			return fmt.Errorf("failed to append record to batch: %v", err)
		}
	}

	err = batch.Send()
	if err != nil {
		log.Printf("ERROR: Failed to send visitor spread batch to ClickHouse: %v", err)
		return fmt.Errorf("failed to send batch to ClickHouse: %v", err)
	}

	return nil
}
