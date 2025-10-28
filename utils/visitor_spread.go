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
	EventID           uint32        `ch:"event_id"`
	UserByCntry       []interface{} `ch:"user_by_cntry"`
	UserByDesignation []interface{} `ch:"user_by_designation"`
	Version           uint32        `ch:"version"`
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
			processVisitorSpreadChunk(clickhouseConn, esClient, eventIDChunk, chunkNum, results, &totalRecordsProcessed, &totalRecordsInserted, &globalCountMutex)
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

		// Extract user_by_cntry array
		var userByCntry []interface{}
		if cntryData, exists := data["user_by_cntry"]; exists {
			if cntryArray, ok := cntryData.([]interface{}); ok {
				userByCntry = cntryArray
			} else {
				log.Printf("Warning: user_by_cntry is not an array for event %d: %T", eventID, cntryData)
				userByCntry = []interface{}{}
			}
		} else {
			userByCntry = []interface{}{}
		}

		// Extract user_by_designation array
		var userByDesignation []interface{}
		if designationData, exists := data["user_by_designation"]; exists {
			if designationArray, ok := designationData.([]interface{}); ok {
				userByDesignation = designationArray
			} else {
				log.Printf("Warning: user_by_designation is not an array for event %d: %T", eventID, designationData)
				userByDesignation = []interface{}{}
			}
		} else {
			userByDesignation = []interface{}{}
		}

		record := VisitorSpreadRecord{
			EventID:           uint32(eventID),
			UserByCntry:       userByCntry,
			UserByDesignation: userByDesignation,
			Version:           1,
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

func processVisitorSpreadChunk(clickhouseConn driver.Conn, esClient *elasticsearch.Client, eventIDs []int64, chunkNum int, results chan<- string, totalRecordsProcessed *int64, totalRecordsInserted *int64, globalCountMutex *sync.Mutex) {
	log.Printf("Processing visitor spread chunk %d: %d event IDs", chunkNum, len(eventIDs))

	visitorSpreadData, err := fetchVisitorSpreadDataFromElasticsearch(esClient, eventIDs)
	if err != nil {
		log.Printf("Visitor spread chunk %d: Failed to fetch Elasticsearch data: %v", chunkNum, err)
		results <- fmt.Sprintf("Visitor spread chunk %d: Failed to fetch Elasticsearch data: %v", chunkNum, err)
		return
	}

	log.Printf("Visitor spread chunk %d: Retrieved visitor spread data for %d events", chunkNum, len(visitorSpreadData))

	records := convertVisitorSpreadDataToRecords(visitorSpreadData, eventIDs)
	recordCount := len(records)
	visitorSpreadData = nil

	if recordCount == 0 {
		log.Printf("Visitor spread chunk %d: No records to insert", chunkNum)
		results <- fmt.Sprintf("Visitor spread chunk %d: No records to insert", chunkNum)
		return
	}

	log.Printf("Visitor spread chunk %d: Inserting %d records into ClickHouse", chunkNum, recordCount)

	insertErr := shared.RetryWithBackoff(
		func() error {
			return insertVisitorSpreadDataSingleWorker(clickhouseConn, records)
		},
		3,
		fmt.Sprintf("ClickHouse insertion for visitor spread chunk %d", chunkNum),
	)

	if insertErr != nil {
		log.Printf("ERROR: Visitor spread chunk %d: Failed to insert data into ClickHouse after retries: %v", chunkNum, insertErr)
		results <- fmt.Sprintf("Visitor spread chunk %d: Failed to insert data into ClickHouse: %v", chunkNum, insertErr)
		records = nil
		return
	}

	records = nil
	insertedCount := recordCount

	globalCountMutex.Lock()
	*totalRecordsProcessed += int64(recordCount)
	*totalRecordsInserted += int64(insertedCount)
	globalCountMutex.Unlock()

	log.Printf("Visitor spread chunk %d: Successfully processed %d records, inserted %d records", chunkNum, recordCount, insertedCount)
	results <- fmt.Sprintf("Visitor spread chunk %d: Successfully processed %d records, inserted %d records", chunkNum, recordCount, insertedCount)
}

func fetchVisitorSpreadDataFromElasticsearch(esClient *elasticsearch.Client, eventIDs []int64) (map[int64]map[string]interface{}, error) {
	if len(eventIDs) == 0 {
		return nil, nil
	}

	results := make(map[int64]map[string]interface{})
	batchSize := 50

	expectedBatches := (len(eventIDs) + batchSize - 1) / batchSize
	resultsChan := make(chan map[int64]map[string]interface{}, expectedBatches)
	semaphore := make(chan struct{}, 2)

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
				time.Sleep(200 * time.Millisecond)
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
		batchResults = nil
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

	ctx, cancel := context.WithTimeout(context.Background(), 900*time.Second)
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
			processedVisitorData := processVisitorSpreadData(eventIDInt, visitorSpread)
			visitorSpreadData = processedVisitorData
		}

		results[eventIDInt] = visitorSpreadData
		processedCount++
	}

	return results, nil
}

func processVisitorSpreadData(eventID int64, visitorSpread interface{}) map[string]interface{} {
	processedData := make(map[string]interface{})

	visitorSpreadJSONBytes, err := json.Marshal(visitorSpread)
	if err != nil {
		log.Printf("Event %d: Failed to marshal visitor spread data: %v", eventID, err)
		return map[string]interface{}{
			"user_by_cntry":       []interface{}{},
			"user_by_designation": []interface{}{},
		}
	}

	visitorSpreadJSON := string(visitorSpreadJSONBytes)

	var visitorSpreadMap map[string]interface{}
	if err := json.Unmarshal([]byte(visitorSpreadJSON), &visitorSpreadMap); err != nil {
		log.Printf("Event %d: Failed to unmarshal visitor spread JSON: %v", eventID, err)
		return map[string]interface{}{
			"user_by_cntry":       []interface{}{},
			"user_by_designation": []interface{}{},
		}
	}

	// Process user_by_cntry data as array
	if userByCntry, exists := visitorSpreadMap["user_by_cntry"]; exists && userByCntry != nil {
		// Convert to array of interfaces
		if userByCntryArray, ok := userByCntry.([]interface{}); ok {
			processedData["user_by_cntry"] = userByCntryArray
		} else {
			log.Printf("Event %d: user_by_cntry is not an array: %T", eventID, userByCntry)
			processedData["user_by_cntry"] = []interface{}{}
		}
	} else {
		processedData["user_by_cntry"] = []interface{}{}
	}

	// Process user_by_designation data as array
	if userByDesignation, exists := visitorSpreadMap["user_by_designation"]; exists && userByDesignation != nil {

		// Convert to array of interfaces
		if userByDesignationArray, ok := userByDesignation.([]interface{}); ok {
			processedData["user_by_designation"] = userByDesignationArray
		} else {
			log.Printf("Event %d: user_by_designation is not an array: %T", eventID, userByDesignation)
			processedData["user_by_designation"] = []interface{}{}
		}
	} else {
		processedData["user_by_designation"] = []interface{}{}
	}

	return processedData
}

// func insertVisitorSpreadDataIntoClickHouse(clickhouseConn driver.Conn, records []VisitorSpreadRecord, numWorkers int) error {
// 	if len(records) == 0 {
// 		return nil
// 	}

// 	if numWorkers <= 1 {
// 		return insertVisitorSpreadDataSingleWorker(clickhouseConn, records)
// 	}

// 	batchSize := (len(records) + numWorkers - 1) / numWorkers
// 	results := make(chan error, numWorkers)
// 	semaphore := make(chan struct{}, numWorkers)

// 	for i := 0; i < numWorkers; i++ {
// 		start := i * batchSize
// 		end := start + batchSize
// 		if end > len(records) {
// 			end = len(records)
// 		}
// 		if start >= len(records) {
// 			break
// 		}

// 		semaphore <- struct{}{}
// 		go func(start, end int) {
// 			defer func() { <-semaphore }()
// 			batch := records[start:end]
// 			err := insertVisitorSpreadDataSingleWorker(clickhouseConn, batch)
// 			results <- err
// 		}(start, end)
// 	}

// 	for i := 0; i < numWorkers && i*batchSize < len(records); i++ {
// 		if err := <-results; err != nil {
// 			return err
// 		}
// 	}

// 	return nil
// }

func insertVisitorSpreadDataSingleWorker(clickhouseConn driver.Conn, records []VisitorSpreadRecord) error {
	if len(records) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 900*time.Second)
	defer cancel()

	batch, err := clickhouseConn.PrepareBatch(ctx, `
		INSERT INTO event_visitorSpread_ch (
			event_id, user_by_cntry, user_by_designation, version
		)
	`)
	if err != nil {
		log.Printf("ERROR: Failed to prepare ClickHouse batch for visitor spread: %v", err)
		return fmt.Errorf("failed to prepare ClickHouse batch: %v", err)
	}

	for _, record := range records {
		err := batch.Append(
			record.EventID,
			record.UserByCntry,
			record.UserByDesignation,
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
