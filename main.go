package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/elastic/go-elasticsearch/v6"
	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
)

// Config holds all configuration including environment variables and runtime settings
type Config struct {
	// Environment variables (automatically loaded)
	DatabaseHost       string `envconfig:"DB_HOST" required:"true"`
	DatabasePort       int    `envconfig:"DB_PORT" required:"true"`
	DatabaseName       string `envconfig:"DB_NAME" required:"true"`
	DatabaseUser       string `envconfig:"DB_USER" required:"true"`
	DatabasePassword   string `envconfig:"DB_PASSWORD" required:"true"`
	ClickhouseUser     string `envconfig:"CLICKHOUSE_USER" required:"true"`
	ClickhouseHost     string `envconfig:"CLICKHOUSE_HOST" required:"true"`
	ClickhousePort     string `envconfig:"CLICKHOUSE_PORT" required:"true"`
	ClickhouseDB       string `envconfig:"CLICKHOUSE_DB" required:"true"`
	ElasticsearchHost  string `envconfig:"ELASTICSEARCH_HOST" required:"true"`
	ElasticsearchPort  string `envconfig:"ELASTICSEARCH_PORT" required:"true"`
	ElasticsearchIndex string `envconfig:"ELASTICSEARCH_INDEX" required:"true"`

	// Runtime configuration (set after loading env vars)
	MySQLDSN            string
	ClickhouseDSN       string
	BaseTable           string
	BatchSize           int
	ClickHouseBatchSize int
	NumChunks           int
	NumWorkers          int
	ClickHouseWorkers   int // Separate worker count for ClickHouse insertion
	ElasticHost         string
	IndexName           string
}

// Global config variable
var config Config

// loadEnv loads environment variables from .env file and validates them
func loadEnv() error {
	// Load .env file - it must exist
	if err := godotenv.Load(); err != nil {
		return fmt.Errorf("failed to load .env file: %w", err)
	}

	// Automatically load and validate all environment variables
	if err := envconfig.Process("", &config); err != nil {
		return fmt.Errorf("failed to process environment variables: %w", err)
	}

	return nil
}

func setupConnections(config Config) (*sql.DB, *sql.DB, *elasticsearch.Client, error) {
	// Setup MySQL connection
	mysqlDB, err := sql.Open("mysql", config.MySQLDSN)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("MySQL connection failed: %v", err)
	}

	// Setup ClickHouse connection with connection pool settings
	clickhouseDB, err := sql.Open("clickhouse", config.ClickhouseDSN)
	if err != nil {
		mysqlDB.Close()
		return nil, nil, nil, fmt.Errorf("ClickHouse connection failed: %v", err)
	}

	// Configure ClickHouse connection pool
	clickhouseDB.SetMaxOpenConns(10)                  // Limit concurrent connections
	clickhouseDB.SetMaxIdleConns(5)                   // Keep some connections idle
	clickhouseDB.SetConnMaxLifetime(30 * time.Minute) // Connection lifetime
	clickhouseDB.SetConnMaxIdleTime(10 * time.Minute) // Idle connection timeout

	// Test ClickHouse connection
	if err := clickhouseDB.Ping(); err != nil {
		mysqlDB.Close()
		clickhouseDB.Close()
		return nil, nil, nil, fmt.Errorf("ClickHouse ping failed: %v", err)
	}

	// Setup Elasticsearch connection
	esConfig := elasticsearch.Config{
		Addresses: []string{config.ElasticHost},
		// Performance optimizations
		DisableRetry:      true,
		EnableMetrics:     false,
		EnableDebugLogger: false,
		// Optimized timeouts for batch processing
		Transport: &http.Transport{
			ResponseHeaderTimeout: 30 * time.Second, // Increased for larger batches
			DialContext: (&net.Dialer{
				Timeout:   15 * time.Second, // Increased for network latency
				KeepAlive: 60 * time.Second, // Increased keep-alive
			}).DialContext,
			MaxIdleConns:          200,               // Increased connection pool
			MaxIdleConnsPerHost:   50,                // Limit per host
			IdleConnTimeout:       120 * time.Second, // Increased idle timeout
			TLSHandshakeTimeout:   15 * time.Second,  // Increased TLS timeout
			ExpectContinueTimeout: 5 * time.Second,   // Increased expect timeout
			DisableCompression:    false,             // Keep compression for large responses
			DisableKeepAlives:     false,             // Keep keep-alives enabled
		},
	}

	esClient, err := elasticsearch.NewClient(esConfig)
	if err != nil {
		mysqlDB.Close()
		clickhouseDB.Close()
		return nil, nil, nil, fmt.Errorf("elasticsearch connection failed: %v", err)
	}

	return mysqlDB, clickhouseDB, esClient, nil
}

func testElasticsearchConnection(esClient *elasticsearch.Client, indexName string) error {
	// Test basic connection
	res, err := esClient.Info()
	if err != nil {
		return fmt.Errorf("failed to get cluster info: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("elasticsearch info request failed: %v", res.Status())
	}

	fmt.Println("✓ Elasticsearch connection successful")
	fmt.Printf("✓ Cluster info: %s\n", res.Status())

	// Test index existence
	indexRes, err := esClient.Indices.Exists([]string{indexName})
	if err != nil {
		return fmt.Errorf("failed to check index existence: %v", err)
	}
	defer indexRes.Body.Close()

	switch indexRes.StatusCode {
	case 200:
		fmt.Printf("✓ Index '%s' exists\n", indexName)
	case 404:
		fmt.Printf("⚠ Index '%s' does not exist\n", indexName)
	default:
		return fmt.Errorf("unexpected status checking index: %d", indexRes.StatusCode)
	}

	// Test simple query
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
		"size": 1,
	}

	queryJSON, _ := json.Marshal(query)
	searchRes, err := esClient.Search(
		esClient.Search.WithIndex(indexName),
		esClient.Search.WithBody(strings.NewReader(string(queryJSON))),
		esClient.Search.WithSize(1),
	)
	if err != nil {
		return fmt.Errorf("failed to execute search query: %v", err)
	}
	defer searchRes.Body.Close()

	if searchRes.IsError() {
		return fmt.Errorf("search query failed: %v", searchRes.Status())
	}

	var result map[string]interface{}
	if err := json.NewDecoder(searchRes.Body).Decode(&result); err != nil {
		return fmt.Errorf("failed to decode search response: %v", err)
	}

	hits := result["hits"].(map[string]interface{})
	total := hits["total"]

	// Handle different total formats in Elasticsearch 6.x vs 7.x+
	var totalCount interface{}
	switch t := total.(type) {
	case float64:
		// Elasticsearch 6.x format: "total": 1234
		totalCount = t
	case map[string]interface{}:
		// Elasticsearch 7.x+ format: "total": {"value": 1234}
		totalCount = t["value"]
	default:
		totalCount = fmt.Sprintf("unknown format: %T", total)
	}

	fmt.Printf("✓ Search query successful, total documents: %v\n", totalCount)

	return nil
}

func testClickHouseConnection(clickhouseDB *sql.DB) error {
	// Test basic connection
	if err := clickhouseDB.Ping(); err != nil {
		return fmt.Errorf("ClickHouse ping failed: %v", err)
	}

	// Test simple query
	var result int
	query := "SELECT 1"
	if err := clickhouseDB.QueryRow(query).Scan(&result); err != nil {
		return fmt.Errorf("ClickHouse query test failed: %v", err)
	}

	if result != 1 {
		return fmt.Errorf("ClickHouse query returned unexpected result: %d", result)
	}

	// Check table existence
	tableQuery := "SELECT count() FROM event_edition_ch LIMIT 1"
	if _, err := clickhouseDB.Exec(tableQuery); err != nil {
		return fmt.Errorf("ClickHouse table access test failed: %v", err)
	}

	fmt.Println("✓ ClickHouse connection successful")
	fmt.Printf("✓ ClickHouse table 'event_edition_ch' is accessible\n")

	// Display connection stats
	stats := clickhouseDB.Stats()
	fmt.Printf("✓ ClickHouse connection pool: Open=%d, InUse=%d, Idle=%d\n",
		stats.OpenConnections, stats.InUse, stats.Idle)

	return nil
}

func main() {
	// Parse command-line arguments
	var tableName string
	var numChunks int
	var batchSize int
	var numWorkers int
	var clickHouseWorkers int
	var clickHouseBatchSize int
	var showHelp bool
	var exhibitorOnly bool
	var sponsorsOnly bool
	var visitorsOnly bool
	var speakersOnly bool
	var eventEditionOnly bool

	flag.StringVar(&tableName, "table", "event", "Table name to process (default: event)")
	flag.IntVar(&numChunks, "chunks", 5, "Number of chunks to process data in (default: 5)")
	flag.IntVar(&batchSize, "batch", 5000, "MySQL batch size for fetching data (default: 5000)")
	flag.IntVar(&numWorkers, "workers", 5, "Number of parallel workers (default: 5)")
	flag.IntVar(&clickHouseWorkers, "clickhouse-workers", 3, "Number of parallel ClickHouse insertion workers (default: 3)")
	flag.IntVar(&clickHouseBatchSize, "clickhouse-batch", 1000, "ClickHouse batch size for inserting data (default: 1000)")
	flag.BoolVar(&exhibitorOnly, "exhibitor", false, "Process only exhibitor data (default: false)")
	flag.BoolVar(&sponsorsOnly, "sponsors", false, "Process only sponsors data (default: false)")
	flag.BoolVar(&visitorsOnly, "visitors", false, "Process only visitors data (default: false)")
	flag.BoolVar(&speakersOnly, "speakers", false, "Process only speakers data (default: false)")
	flag.BoolVar(&eventEditionOnly, "event-edition", false, "Process only event edition data (default: false)")
	flag.BoolVar(&showHelp, "help", false, "Show help information")
	flag.Parse()

	// Show help if requested
	if showHelp {
		fmt.Println("=== Data Migration Script ===")
		fmt.Println("Usage: go run main.go [options]")
		fmt.Println("\nOptions:")
		fmt.Println("  -table string")
		fmt.Println("        Table name to process (default: event)")
		fmt.Println("  -chunks int")
		fmt.Println("        Number of chunks to process data in (default: 5)")
		fmt.Println("  -batch int")
		fmt.Println("        MySQL batch size for fetching data (default: 5000)")
		fmt.Println("  -workers int")
		fmt.Println("        Number of parallel workers (default: 5)")
		fmt.Println("  -clickhouse-workers int")
		fmt.Println("        Number of parallel ClickHouse insertion workers (default: 3)")
		fmt.Println("  -clickhouse-batch int")
		fmt.Println("        ClickHouse batch size for inserting data (default: 1000)")
		fmt.Println("  -exhibitor")
		fmt.Println("        Process only exhibitor data (default: false)")
		fmt.Println("  -sponsors")
		fmt.Println("        Process only sponsors data (default: false)")
		fmt.Println("  -visitors")
		fmt.Println("        Process only visitors data (default: false)")
		fmt.Println("  -speakers")
		fmt.Println("        Process only speakers data (default: false)")
		fmt.Println("  -event-edition")
		fmt.Println("        Process only event edition data (default: false)")
		fmt.Println("  -help")
		fmt.Println("        Show this help message")
		fmt.Println("\nExamples:")
		fmt.Println("  go run main.go                           # Use defaults (event table, 5 chunks)")
		fmt.Println("  go run main.go -table=venue -chunks=3 -batch=1000 -workers=10 -clickhouse-workers=5 -clickhouse-batch=2000")
		return
	}

	// Validate arguments
	if numChunks <= 0 {
		log.Fatal("Error: chunks must be a positive number")
	}

	if batchSize <= 0 {
		log.Fatal("Error: batch size must be a positive number")
	}

	if numWorkers <= 0 {
		log.Fatal("Error: workers must be a positive number")
	}

	if clickHouseWorkers <= 0 {
		log.Fatal("Error: clickhouse-workers must be a positive number")
	}

	if clickHouseBatchSize <= 0 {
		log.Fatal("Error: clickhouse-batch must be a positive number")
	}

	if tableName == "" {
		log.Fatal("Error: table name cannot be empty")
	}

	// Load environment variables
	if err := loadEnv(); err != nil {
		log.Fatal("Failed to load environment variables:", err)
	}

	// Build connection strings from environment variables
	mysqlDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		config.DatabaseUser,
		config.DatabasePassword,
		config.DatabaseHost,
		config.DatabasePort,
		config.DatabaseName,
	)

	clickhouseDSN := fmt.Sprintf("clickhouse://%s@%s:%s/%s",
		config.ClickhouseUser,
		config.ClickhouseHost,
		config.ClickhousePort,
		config.ClickhouseDB,
	)

	elasticHost := fmt.Sprintf("http://%s:%s",
		config.ElasticsearchHost,
		config.ElasticsearchPort,
	)

	elasticIndex := config.ElasticsearchIndex

	// Set runtime configuration values
	config.MySQLDSN = mysqlDSN
	config.ClickhouseDSN = clickhouseDSN
	config.BaseTable = tableName
	config.BatchSize = batchSize
	config.ClickHouseBatchSize = clickHouseBatchSize
	config.NumChunks = numChunks
	config.NumWorkers = numWorkers
	config.ClickHouseWorkers = clickHouseWorkers
	config.ElasticHost = elasticHost
	config.IndexName = elasticIndex

	// Display configuration
	fmt.Printf("=== Data Migration Configuration ===\n")
	if exhibitorOnly {
		fmt.Printf("Mode: EXHIBITOR ONLY\n")
	} else if sponsorsOnly {
		fmt.Printf("Mode: SPONSORS ONLY\n")
	} else if visitorsOnly {
		fmt.Printf("Mode: VISITORS ONLY\n")
	} else if speakersOnly {
		fmt.Printf("Mode: SPEAKERS ONLY\n")
	} else if eventEditionOnly {
		fmt.Printf("Mode: EVENT EDITION ONLY\n")
	} else {
		fmt.Printf("Table: %s\n", config.BaseTable)
	}
	fmt.Printf("Chunks: %d\n", config.NumChunks)
	fmt.Printf("Batch Size: %d\n", config.BatchSize)
	fmt.Printf("ClickHouse Batch Size: %d\n", config.ClickHouseBatchSize)
	fmt.Printf("Workers: %d\n", config.NumWorkers)
	fmt.Printf("ClickHouse Workers: %d\n", config.ClickHouseWorkers)
	fmt.Printf("==============================\n\n")

	// Setup connections
	mysqlDB, clickhouseDB, esClient, err := setupConnections(config)
	if err != nil {
		log.Fatal(err)
	}
	defer mysqlDB.Close()
	defer clickhouseDB.Close()

	fmt.Println("Connections established successfully!")

	// Test ClickHouse connection health
	if err := testClickHouseConnection(clickhouseDB); err != nil {
		log.Fatalf("ClickHouse connection test failed: %v", err)
	}

	// Test Elasticsearch connection
	if err := testElasticsearchConnection(esClient, config.IndexName); err != nil {
		log.Fatalf("Elasticsearch connection test failed: %v", err)
	}

	if exhibitorOnly {
		// Process exhibitor data only
		processExhibitorOnly(mysqlDB, clickhouseDB, config)
	} else if sponsorsOnly {
		// Process sponsors data only
		processSponsorsOnly(mysqlDB, clickhouseDB, config)
	} else if visitorsOnly {
		// Process visitors data only
		processVisitorsOnly(mysqlDB, clickhouseDB, config)
	} else if speakersOnly {
		// Process speakers data only
		processSpeakersOnly(mysqlDB, clickhouseDB, config)
	} else if eventEditionOnly {
		// Process event edition data only
		processEventEditionOnly(mysqlDB, clickhouseDB, esClient, config)
	} else {
		// Process regular table data
		// Get total records and min/max ID's count from base table
		totalRecords, minID, maxID, err := getTotalRecordsAndIDRange(mysqlDB, config.BaseTable)
		if err != nil {
			log.Fatal("Failed to get total records and ID range:", err)
		}

		fmt.Printf("Total records: %d, Min ID: %d, Max ID: %d\n", totalRecords, minID, maxID)

		// Calculate chunk size based on user input
		if config.NumChunks <= 0 {
			config.NumChunks = 5 // Default to 5 chunks if not specified
		}

		chunkSize := (maxID - minID + 1) / config.NumChunks
		if chunkSize == 0 {
			chunkSize = 1
		}

		fmt.Printf("Processing data in %d chunks with chunk size: %d\n", config.NumChunks, chunkSize)

		// Create channels for results and coordination
		results := make(chan string, config.NumChunks)
		semaphore := make(chan struct{}, config.NumWorkers)

		// Process chunks in parallel
		for i := 0; i < config.NumChunks; i++ {
			startID := minID + (i * chunkSize)
			endID := startID + chunkSize - 1

			// Adjust last chunk to include remaining records
			if i == config.NumChunks-1 {
				endID = maxID
			}

			// Add delay between chunk launches to reduce ClickHouse load
			if i > 0 {
				delay := 3 * time.Second // 3 second delay between chunks
				fmt.Printf("Waiting %v before launching chunk %d...\n", delay, i+1)
				time.Sleep(delay)
			}

			// Use semaphore to limit concurrent workers
			semaphore <- struct{}{}
			go func(chunkNum, start, end int) {
				defer func() { <-semaphore }()
				processChunk(mysqlDB, config, start, end, chunkNum, results)
			}(i+1, startID, endID)
		}

		// Collect results
		for i := 0; i < config.NumChunks; i++ {
			result := <-results
			fmt.Printf("Result: %s\n", result)
		}

		fmt.Println("Migration completed!")
	}
}

func getTotalRecordsAndIDRange(db *sql.DB, table string) (int, int, int, error) {
	query := fmt.Sprintf("SELECT COUNT(*), MIN(id), MAX(id) FROM %s", table)
	var count, minId, maxId int
	err := db.QueryRow(query).Scan(&count, &minId, &maxId)
	return count, minId, maxId, err
}

func buildMigrationData(db *sql.DB, table string, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT id, name as event_name, abbr_name, punchline, start_date, end_date, country, published, status, event_audience, functionality, created FROM %s WHERE id >= %d AND id <= %d ORDER BY id LIMIT %d", table, startID, endID, batchSize)
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
			val := values[i]
			if val == nil {
				row[col] = nil
			} else {
				row[col] = val
			}
		}
		results = append(results, row)
	}

	return results, nil
}

func extractVenueIDs(editionData []map[string]interface{}) []int64 {
	var venueIDs []int64
	seen := make(map[int64]bool)
	nullCount := 0
	zeroCount := 0

	for _, edition := range editionData {
		venueID := edition["venue_id"]
		if venueID == nil {
			nullCount++
			continue
		}

		if id, ok := venueID.(int64); ok && id > 0 {
			if !seen[id] {
				venueIDs = append(venueIDs, id)
				seen[id] = true
			}
		} else {
			zeroCount++
		}
	}

	return venueIDs
}

func fetchVenueDataParallel(db *sql.DB, venueIDs []int64, numWorkers int) []map[string]interface{} {
	if len(venueIDs) == 0 {
		return nil
	}

	// Process venue IDs in batches
	batchSize := 1000 // Process 1000 venue IDs at a time

	// Create channels for coordination
	expectedBatches := (len(venueIDs) + batchSize - 1) / batchSize
	results := make(chan []map[string]interface{}, expectedBatches)
	semaphore := make(chan struct{}, numWorkers)

	var allVenueData []map[string]interface{}

	// Use WaitGroup to track all goroutines
	var wg sync.WaitGroup

	// Launch all workers
	for i := 0; i < len(venueIDs); i += batchSize {
		end := i + batchSize
		if end > len(venueIDs) {
			end = len(venueIDs)
		}

		batch := venueIDs[i:end]

		// Use semaphore to limit concurrent workers
		semaphore <- struct{}{}
		wg.Add(1)

		go func(venueIDBatch []int64, batchNum int) {
			defer func() {
				<-semaphore
				wg.Done()
			}()
			venueData := fetchVenueDataForBatch(db, venueIDBatch)
			results <- venueData
		}(batch, i/batchSize)
	}

	// Wait for all goroutines to complete with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// Collect results with timeout
	completedBatches := 0

collectLoop:
	for completedBatches < expectedBatches {
		select {
		case venueData := <-results:
			allVenueData = append(allVenueData, venueData...)
			completedBatches++
		case <-done:
			// All goroutines completed
			break collectLoop
		case <-time.After(120 * time.Second): // Timeout after 120 seconds
			fmt.Printf("Warning: Timeout waiting for venue data. Completed %d/%d batches\n",
				completedBatches, expectedBatches)
			break collectLoop
		}
	}

	// Find missing venue IDs
	retrievedVenueIDs := make(map[int64]bool)
	for _, venue := range allVenueData {
		if venueID, ok := venue["id"].(int64); ok {
			retrievedVenueIDs[venueID] = true
		}
	}

	var missingVenueIDs []int64
	for _, requestedID := range venueIDs {
		if !retrievedVenueIDs[requestedID] {
			missingVenueIDs = append(missingVenueIDs, requestedID)
		}
	}

	if len(missingVenueIDs) > 0 {
		fmt.Printf("Missing venue IDs (%d): %v\n", len(missingVenueIDs), missingVenueIDs)
	}

	return allVenueData
}

func fetchVenueDataForBatch(db *sql.DB, venueIDs []int64) []map[string]interface{} {
	if len(venueIDs) == 0 {
		return nil
	}

	// Build IN clause for the query
	placeholders := make([]string, len(venueIDs))
	args := make([]interface{}, len(venueIDs))
	for i, id := range venueIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(`
		SELECT 
			id, name as venue_name, country as venue_country, 
			city as venue_city, geo_lat as venue_lat, geo_long as venue_long
		FROM venue 
		WHERE id IN (%s)`, strings.Join(placeholders, ","))

	rows, err := db.Query(query, args...)
	if err != nil {
		fmt.Printf("Error fetching venue data: %v\n", err)
		return nil
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil
	}

	var results []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			continue
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			if val == nil {
				row[col] = nil
			} else {
				// Handle latitude and longitude as numeric values for ClickHouse Float64 compatibility
				if col == "venue_lat" || col == "venue_long" {
					if bytes, ok := val.([]byte); ok {
						// Convert byte array to float64
						if len(bytes) > 0 {
							str := string(bytes)
							if str != "" {
								// Parse string to float64
								if f, err := strconv.ParseFloat(str, 64); err == nil {
									row[col] = f
								} else {
									fmt.Printf("Warning: Could not parse %s value '%s' to float64: %v\n", col, str, err)
									row[col] = nil
								}
							} else {
								row[col] = nil
							}
						} else {
							row[col] = nil
						}
					} else if str, ok := val.(string); ok {
						// Handle string values by parsing to float64
						if str != "" {
							if f, err := strconv.ParseFloat(str, 64); err == nil {
								row[col] = f
							} else {
								fmt.Printf("Warning: Could not parse %s string '%s' to float64: %v\n", col, str, err)
								row[col] = nil
							}
						} else {
							row[col] = nil
						}
					} else {
						// Keep numeric values as-is
						row[col] = val
					}
				} else {
					row[col] = val
				}
			}
		}
		results = append(results, row)
	}

	return results
}

func extractCompanyIDs(editionData []map[string]interface{}) []int64 {
	var companyIDs []int64
	seen := make(map[int64]bool)
	nullCount := 0
	zeroCount := 0

	for _, edition := range editionData {
		companyID := edition["company_id"]
		if companyID == nil {
			nullCount++
			continue
		}

		if id, ok := companyID.(int64); ok && id > 0 {
			if !seen[id] {
				companyIDs = append(companyIDs, id)
				seen[id] = true
			}
		} else {
			zeroCount++
		}
	}

	return companyIDs
}

func fetchCompanyDataParallel(db *sql.DB, companyIDs []int64, numWorkers int) []map[string]interface{} {
	if len(companyIDs) == 0 {
		return nil
	}

	// Process company IDs in batches
	batchSize := 1000 // Process 1000 company IDs at a time

	// Create channels for coordination
	expectedBatches := (len(companyIDs) + batchSize - 1) / batchSize
	results := make(chan []map[string]interface{}, expectedBatches)
	semaphore := make(chan struct{}, numWorkers)

	var allCompanyData []map[string]interface{}

	// Use WaitGroup to track all goroutines
	var wg sync.WaitGroup

	// Launch all workers
	for i := 0; i < len(companyIDs); i += batchSize {
		end := i + batchSize
		if end > len(companyIDs) {
			end = len(companyIDs)
		}

		batch := companyIDs[i:end]

		// Use semaphore to limit concurrent workers
		semaphore <- struct{}{}
		wg.Add(1)

		go func(companyIDBatch []int64, batchNum int) {
			defer func() {
				<-semaphore
				wg.Done()
			}()
			companyData := fetchCompanyDataForBatch(db, companyIDBatch)
			results <- companyData
		}(batch, i/batchSize)
	}

	// Wait for all goroutines to complete with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// Collect results with timeout
	completedBatches := 0

collectLoop:
	for completedBatches < expectedBatches {
		select {
		case companyData := <-results:
			allCompanyData = append(allCompanyData, companyData...)
			completedBatches++
		case <-done:
			// All goroutines completed
			break collectLoop
		case <-time.After(120 * time.Second): // Timeout after 120 seconds
			fmt.Printf("Warning: Timeout waiting for company data. Completed %d/%d batches\n",
				completedBatches, expectedBatches)
			break collectLoop
		}
	}

	// Find missing company IDs
	retrievedCompanyIDs := make(map[int64]bool)
	for _, company := range allCompanyData {
		if companyID, ok := company["id"].(int64); ok {
			retrievedCompanyIDs[companyID] = true
		}
	}

	// var missingCompanyIDs []int64
	// for _, requestedID := range companyIDs {
	// 	if !retrievedCompanyIDs[requestedID] {
	// 		missingCompanyIDs = append(missingCompanyIDs, requestedID)
	// 	}
	// }

	return allCompanyData
}

func fetchCompanyDataForBatch(db *sql.DB, companyIDs []int64) []map[string]interface{} {
	if len(companyIDs) == 0 {
		return nil
	}

	// Build IN clause for the query
	placeholders := make([]string, len(companyIDs))
	args := make([]interface{}, len(companyIDs))
	for i, id := range companyIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(`
		SELECT 
			id, name as company_name, domain as company_domain, 
			website as company_website, country as company_country, 
			city as company_city
		FROM company 
		WHERE id IN (%s)`, strings.Join(placeholders, ","))

	rows, err := db.Query(query, args...)
	if err != nil {
		fmt.Printf("Error fetching company data: %v\n", err)
		return nil
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil
	}

	var results []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			continue
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			if val == nil {
				row[col] = nil
			} else {
				row[col] = val
			}
		}
		results = append(results, row)
	}

	return results
}

func extractEventIDs(batchData []map[string]interface{}) []int64 {
	var eventIDs []int64
	for _, row := range batchData {
		if id, ok := row["id"].(int64); ok {
			eventIDs = append(eventIDs, id)
		}
	}
	return eventIDs
}

func fetchEditionDataParallel(db *sql.DB, eventIDs []int64, numWorkers int) []map[string]interface{} {
	if len(eventIDs) == 0 {
		return nil
	}

	// Process event IDs in batches
	batchSize := 1000 // Process 1000 event IDs at a time

	// Create channels for coordination
	// Buffer the results channel to prevent blocking
	expectedBatches := (len(eventIDs) + batchSize - 1) / batchSize
	results := make(chan []map[string]interface{}, expectedBatches)
	semaphore := make(chan struct{}, numWorkers)

	var allEditionData []map[string]interface{}

	// Use WaitGroup to track all goroutines
	var wg sync.WaitGroup

	// Launch all workers
	for i := 0; i < len(eventIDs); i += batchSize {
		end := i + batchSize
		if end > len(eventIDs) {
			end = len(eventIDs)
		}

		batch := eventIDs[i:end]

		// Use semaphore to limit concurrent workers
		semaphore <- struct{}{}
		wg.Add(1)

		go func(eventIDBatch []int64, batchNum int) {
			defer func() {
				<-semaphore
				wg.Done()
			}()
			editionData := fetchEditionDataForBatch(db, eventIDBatch)
			results <- editionData
		}(batch, i/batchSize)
	}

	// Wait for all goroutines to complete with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// Collect results with timeout
	completedBatches := 0

	for completedBatches < expectedBatches {
		select {
		case editionData := <-results:
			allEditionData = append(allEditionData, editionData...)
			completedBatches++
		case <-done:
			// All goroutines completed
			return allEditionData
		case <-time.After(120 * time.Second): // Timeout after 120 seconds
			fmt.Printf("Warning: Timeout waiting for edition data. Completed %d/%d batches\n",
				completedBatches, expectedBatches)
			return allEditionData
		}
	}

	return allEditionData
}

func fetchEditionDataForBatch(db *sql.DB, eventIDs []int64) []map[string]interface{} {
	if len(eventIDs) == 0 {
		return nil
	}

	// Build IN clause for the query
	placeholders := make([]string, len(eventIDs))
	args := make([]interface{}, len(eventIDs))
	for i, id := range eventIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(`
		SELECT 
			event, id as edition_id, city as edition_city, 
			company_id, venue as venue_id, website as edition_website, 
			created as edition_created
		FROM event_edition 
		WHERE event IN (%s)`, strings.Join(placeholders, ","))

	rows, err := db.Query(query, args...)
	if err != nil {
		fmt.Printf("Error fetching edition data: %v\n", err)
		return nil
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil
	}

	var results []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			continue
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			if val == nil {
				row[col] = nil
			} else {
				row[col] = val
			}
		}
		results = append(results, row)
	}

	return results
}

func extractCityIDs(editionData []map[string]interface{}) []int64 {
	var cityIDs []int64
	seen := make(map[int64]bool)
	nullCount := 0
	zeroCount := 0

	for _, edition := range editionData {
		cityID := edition["edition_city"]
		if cityID == nil {
			nullCount++
			continue
		}

		if id, ok := cityID.(int64); ok && id > 0 {
			if !seen[id] {
				cityIDs = append(cityIDs, id)
				seen[id] = true
			}
		} else {
			zeroCount++
		}
	}

	return cityIDs
}

func fetchCityDataParallel(db *sql.DB, cityIDs []int64, numWorkers int) []map[string]interface{} {
	if len(cityIDs) == 0 {
		return nil
	}

	// Process city IDs in batches
	batchSize := 1000 // Process 1000 city IDs at a time

	// Create channels for coordination
	expectedBatches := (len(cityIDs) + batchSize - 1) / batchSize
	results := make(chan []map[string]interface{}, expectedBatches)
	semaphore := make(chan struct{}, numWorkers)

	var allCityData []map[string]interface{}

	// Use WaitGroup to track all goroutines
	var wg sync.WaitGroup

	// Launch all workers
	for i := 0; i < len(cityIDs); i += batchSize {
		end := i + batchSize
		if end > len(cityIDs) {
			end = len(cityIDs)
		}

		batch := cityIDs[i:end]

		// Use semaphore to limit concurrent workers
		semaphore <- struct{}{}
		wg.Add(1)

		go func(cityIDBatch []int64, batchNum int) {
			defer func() {
				<-semaphore
				wg.Done()
			}()
			cityData := fetchCityDataForBatch(db, cityIDBatch)
			results <- cityData
		}(batch, i/batchSize)
	}

	// Wait for all goroutines to complete with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// Collect results with timeout
	completedBatches := 0

collectLoop:
	for completedBatches < expectedBatches {
		select {
		case cityData := <-results:
			allCityData = append(allCityData, cityData...)
			completedBatches++
		case <-done:
			// All goroutines completed
			break collectLoop
		case <-time.After(120 * time.Second): // Timeout after 120 seconds
			fmt.Printf("Warning: Timeout waiting for city data. Completed %d/%d batches\n",
				completedBatches, expectedBatches)
			break collectLoop
		}
	}

	// Find missing city IDs
	retrievedCityIDs := make(map[int64]bool)
	for _, city := range allCityData {
		if cityID, ok := city["id"].(int64); ok {
			retrievedCityIDs[cityID] = true
		}
	}

	var missingCityIDs []int64
	for _, requestedID := range cityIDs {
		if !retrievedCityIDs[requestedID] {
			missingCityIDs = append(missingCityIDs, requestedID)
		}
	}

	if len(missingCityIDs) > 0 {
		fmt.Printf("Missing city IDs (%d): %v\n", len(missingCityIDs), missingCityIDs)
	}

	return allCityData
}

func fetchCityDataForBatch(db *sql.DB, cityIDs []int64) []map[string]interface{} {
	if len(cityIDs) == 0 {
		return nil
	}

	// Build IN clause for the query
	placeholders := make([]string, len(cityIDs))
	args := make([]interface{}, len(cityIDs))
	for i, id := range cityIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(`
		SELECT 
			id, geo_lat as event_city_lat, geo_long as event_city_long
		FROM city 
		WHERE id IN (%s)`, strings.Join(placeholders, ","))

	rows, err := db.Query(query, args...)
	if err != nil {
		fmt.Printf("Error fetching city data: %v\n", err)
		return nil
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil
	}

	var results []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			continue
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			if val == nil {
				row[col] = nil
			} else {
				// Handle latitude and longitude as numeric values for ClickHouse Float64 compatibility
				if col == "event_city_lat" || col == "event_city_long" {
					if bytes, ok := val.([]byte); ok {
						// Convert byte array to float64
						if len(bytes) > 0 {
							str := string(bytes)
							if str != "" {
								// Parse string to float64
								if f, err := strconv.ParseFloat(str, 64); err == nil {
									row[col] = f
								} else {
									fmt.Printf("Warning: Could not parse %s value '%s' to float64: %v\n", col, str, err)
									row[col] = nil
								}
							} else {
								row[col] = nil
							}
						} else {
							row[col] = nil
						}
					} else if str, ok := val.(string); ok {
						// Handle string values by parsing to float64
						if str != "" {
							if f, err := strconv.ParseFloat(str, 64); err == nil {
								row[col] = f
							} else {
								fmt.Printf("Warning: Could not parse %s string '%s' to float64: %v\n", col, str, err)
								row[col] = nil
							}
						} else {
							row[col] = nil
						}
					} else {
						// Keep numeric values as-is
						row[col] = val
					}
				} else {
					row[col] = val
				}
			}
		}
		results = append(results, row)
	}

	return results
}

func convertToString(value interface{}) string {
	if value == nil {
		return "" // Return empty string instead of "NULL" - ClickHouse will handle as NULL
	}

	switch v := value.(type) {
	case []byte:
		return string(v) // Just return the string, even if empty
	case string:
		return v // Just return the string, even if empty
	case float64:
		return fmt.Sprintf("%.6f", v)
	case float32:
		return fmt.Sprintf("%.6f", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func extractDomainFromWebsite(website interface{}) string {
	if website == nil {
		return ""
	}

	websiteStr := convertToString(website)
	if websiteStr == "" {
		return ""
	}

	websiteStr = strings.TrimSpace(websiteStr)

	if !strings.Contains(websiteStr, "://") {
		websiteStr = "http://" + websiteStr
	}
	// Parse URL
	parsedURL, err := url.Parse(websiteStr)
	if err != nil {
		return ""
	}
	// Extract hostname (domain)
	host := parsedURL.Hostname()
	if host == "" {
		return ""
	}

	host = strings.TrimPrefix(strings.ToLower(host), "www.")

	domainRegex := regexp.MustCompile(`^[a-z0-9][a-z0-9\-]{1,63}(\.[a-z0-9][a-z0-9\-]{1,63})*\.[a-z]{2,}$`)
	if !domainRegex.MatchString(host) {
		return ""
	}

	return host
}

func fetchElasticsearchBatch(esClient *elasticsearch.Client, indexName string, eventIDs []int64) map[int64]map[string]interface{} {
	if len(eventIDs) == 0 {
		return nil
	}

	// Create a map to store results by event ID
	results := make(map[int64]map[string]interface{})

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"terms": map[string]interface{}{
				"id": eventIDs,
			},
		},
		"size":    len(eventIDs),
		"_source": []string{"id", "description", "exhibitors", "speakers", "totalSponsor", "following", "punchline"},
	}

	queryJSON, _ := json.Marshal(query)

	// Execute search with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	searchRes, err := esClient.Search(
		esClient.Search.WithContext(ctx),
		esClient.Search.WithIndex(indexName),
		esClient.Search.WithBody(strings.NewReader(string(queryJSON))),
	)
	if err != nil {
		fmt.Printf("Warning: Failed to search Elasticsearch for batch: %v\n", err)
		return results
	}
	defer searchRes.Body.Close()

	if searchRes.IsError() {
		fmt.Printf("Warning: Elasticsearch search failed: %v\n", searchRes.Status())
		return results
	}

	// Parse response
	var result map[string]interface{}
	if err := json.NewDecoder(searchRes.Body).Decode(&result); err != nil {
		fmt.Printf("Warning: Failed to decode Elasticsearch response: %v\n", err)
		return results
	}

	hits := result["hits"].(map[string]interface{})
	hitsArray := hits["hits"].([]interface{})

	// Process each hit
	if len(hitsArray) == 0 {
		return results
	}

	for _, hit := range hitsArray {
		hitMap := hit.(map[string]interface{})
		source := hitMap["_source"].(map[string]interface{})

		// Handle id as either string or number (changed from event_id to id)
		var eventIDInt int64
		if eventIDStr, ok := source["id"].(string); ok {
			// Convert string to int64
			if parsedID, err := strconv.ParseInt(eventIDStr, 10, 64); err == nil {
				eventIDInt = parsedID
			} else {
				fmt.Printf("Warning: Failed to parse id string '%s': %v\n", eventIDStr, err)
				continue
			}
		} else if eventIDNum, ok := source["id"].(float64); ok {
			// Convert float64 to int64
			eventIDInt = int64(eventIDNum)
		} else {
			fmt.Printf("Warning: Unexpected id type: %T, value: %v\n", source["id"], source["id"])
			continue
		}

		results[eventIDInt] = map[string]interface{}{
			"event_description":  source["description"],
			"event_exhibitors":   source["exhibitors"],
			"event_speakers":     source["speakers"],
			"event_totalSponsor": source["totalSponsor"],
			"event_following":    source["following"],
			"event_punchline":    source["punchline"],
			"edition_exhibitor":  source["exhibitors"],
			"edition_sponsor":    source["totalSponsor"],
			"edition_speaker":    source["speakers"],
			"edition_followers":  source["following"],
		}
	}

	return results
}

func fetchElasticsearchDataForEvents(esClient *elasticsearch.Client, indexName string, eventIDs []int64) map[int64]map[string]interface{} {
	if len(eventIDs) == 0 {
		return nil
	}

	results := make(map[int64]map[string]interface{})
	batchSize := 200 // Process 200 event IDs at a time (larger batches for better performance)

	expectedBatches := (len(eventIDs) + batchSize - 1) / batchSize
	resultsChan := make(chan map[int64]map[string]interface{}, expectedBatches)
	semaphore := make(chan struct{}, 15) // Limit concurrent ES queries to 15

	var allResults []map[int64]map[string]interface{}
	var wg sync.WaitGroup // Use WaitGroup to track all goroutines

	for i := 0; i < len(eventIDs); i += batchSize {
		end := i + batchSize
		if end > len(eventIDs) {
			end = len(eventIDs)
		}
		batch := eventIDs[i:end]
		// Use semaphore to limit concurrent workers
		semaphore <- struct{}{}
		wg.Add(1)

		go func(eventIDBatch []int64, batchNum int) {
			defer func() {
				<-semaphore
				wg.Done()
			}()

			batchResults := fetchElasticsearchBatch(esClient, indexName, eventIDBatch)
			resultsChan <- batchResults
		}(batch, i/batchSize)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	completedBatches := 0

collectLoop:
	for completedBatches < expectedBatches {
		select {
		case batchResults := <-resultsChan:
			allResults = append(allResults, batchResults)
			completedBatches++
		case <-done:
			break collectLoop // All goroutines completed
		case <-time.After(30 * time.Second):
			fmt.Printf("Warning: Timeout waiting for Elasticsearch data. Completed %d/%d batches\n",
				completedBatches, expectedBatches)
			break collectLoop
		}
	}

	// Merge all batch results
	for _, batchResult := range allResults {
		for eventID, data := range batchResult {
			results[eventID] = data
		}
	}

	fmt.Printf("✓ Retrieved Elasticsearch data for %d events in %d batches\n", len(results), len(allResults))
	return results
}

// insertIntoClickHouse inserts a batch of records into ClickHouse
func insertIntoClickHouse(clickhouseDB *sql.DB, records []map[string]interface{}, config Config) error {
	if len(records) == 0 {
		return nil
	}
	fmt.Printf("Starting ClickHouse insertion of %d records in batches of %d...\n", len(records), config.ClickHouseBatchSize)

	// Determine if we should use parallel insertion based on batch count
	batchCount := (len(records) + config.ClickHouseBatchSize - 1) / config.ClickHouseBatchSize
	if batchCount > 1 && config.NumWorkers > 1 {
		return insertIntoClickHouseParallel(clickhouseDB, records, config) // Use parallel insertion for multiple batches
	}
	return insertIntoClickHouseSequential(clickhouseDB, records, config) // Use sequential insertion for single batch or single worker
}

// insertIntoClickHouseSequential inserts records sequentially
func insertIntoClickHouseSequential(clickhouseDB *sql.DB, records []map[string]interface{}, config Config) error {
	// Process records in ClickHouse batch sizes
	for i := 0; i < len(records); i += config.ClickHouseBatchSize {
		end := i + config.ClickHouseBatchSize
		if end > len(records) {
			end = len(records)
		}

		batch := records[i:end]
		fmt.Printf("Processing ClickHouse batch %d-%d (%d records)...\n", i, end-1, len(batch))

		if err := insertClickHouseBatch(clickhouseDB, batch); err != nil {
			return fmt.Errorf("failed to insert ClickHouse batch %d-%d: %v", i, end-1, err)
		}
		fmt.Printf("Successfully inserted ClickHouse batch %d-%d (%d records)\n", i, end-1, len(batch))

		// Add small delay between batches to prevent overwhelming ClickHouse
		if i+config.ClickHouseBatchSize < len(records) {
			time.Sleep(500 * time.Millisecond)
		}
	}
	fmt.Printf("🎉 Completed ClickHouse insertion of all %d records\n", len(records))
	return nil
}

// insertIntoClickHouseParallel inserts records in parallel using multiple workers
func insertIntoClickHouseParallel(clickhouseDB *sql.DB, records []map[string]interface{}, config Config) error {
	// Calculate batch boundaries
	var batches [][]map[string]interface{}
	for i := 0; i < len(records); i += config.ClickHouseBatchSize {
		end := i + config.ClickHouseBatchSize
		if end > len(records) {
			end = len(records)
		}
		batches = append(batches, records[i:end])
	}
	fmt.Printf("🚀 Using parallel insertion with %d workers for %d batches\n", config.NumWorkers, len(batches))

	results := make(chan error, len(batches))
	semaphore := make(chan struct{}, config.ClickHouseWorkers) // Limit concurrent ClickHouse workers

	// Launch workers for each batch
	for i, batch := range batches {
		semaphore <- struct{}{} // Use semaphore to limit concurrent workers

		go func(batchNum int, batchData []map[string]interface{}) {
			defer func() { <-semaphore }()

			fmt.Printf("Worker processing batch %d (%d records)...\n", batchNum+1, len(batchData))

			if err := insertClickHouseBatch(clickhouseDB, batchData); err != nil {
				results <- fmt.Errorf("failed to insert ClickHouse batch %d: %v", batchNum+1, err)
				return
			}

			fmt.Printf("Worker completed batch %d (%d records)\n", batchNum+1, len(batchData))
			results <- nil
		}(i, batch)
	}

	var errors []error
	for i := 0; i < len(batches); i++ {
		if err := <-results; err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("ClickHouse insertion failed with %d errors: %v", len(errors), errors)
	}

	fmt.Printf("🎉 Completed parallel ClickHouse insertion of all %d records in %d batches\n", len(records), len(batches))
	return nil
}

// collectRecordsForClickHouse collects all the combined data into a format ready
func collectRecordsForClickHouse(eventData map[string]interface{}, edition map[string]interface{}, company map[string]interface{}, venue map[string]interface{}, city map[string]interface{}, esInfoMap map[string]interface{}, editionDomain string) map[string]interface{} {
	return map[string]interface{}{
		"event":          eventData,
		"edition":        edition,
		"company":        company,
		"venue":          venue,
		"city":           city,
		"es":             esInfoMap,
		"edition_domain": editionDomain,
	}
}

// insertClickHouseBatch inserts a single batch of records into ClickHouse
func insertClickHouseBatch(clickhouseDB *sql.DB, records []map[string]interface{}) error {
	if len(records) == 0 {
		return nil
	}

	// Build the INSERT statement - matching exact schema order
	query := `
		INSERT INTO event_edition_ch (
			event_id, event_name, event_abbr_name, event_description, event_punchline,
			start_date, end_date, edition_id, edition_country, edition_city,
			edition_city_lat, edition_city_long, company_id, company_name, company_domain,
			company_website, company_country, company_city, venue_id, venue_name,
			venue_country, venue_city, venue_lat, venue_long, published, status,
			editions_audiance_type, edition_functionality, edition_website, edition_domain,
			edition_type, event_followers, edition_followers, event_exhibitor, edition_exhibitor,
			event_sponsor, edition_sponsor, event_speaker, edition_speaker,
			event_created, edition_created, version
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	// Retry logic for connection issues
	maxRetries := 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		fmt.Printf("Attempt %d/%d: Inserting %d records into ClickHouse...\n", attempt, maxRetries, len(records))

		err := executeInsertBatch(clickhouseDB, query, records)
		if err == nil {
			fmt.Printf("✓ Successfully inserted %d records into ClickHouse\n", len(records))
			return nil
		}

		if isConnectionError(err) {
			if attempt < maxRetries {
				fmt.Printf("ClickHouse connection error (attempt %d/%d), retrying in 5 seconds: %v\n",
					attempt, maxRetries, err)
				time.Sleep(5 * time.Second)

				if err := clickhouseDB.Ping(); err != nil {
					fmt.Printf("Database ping failed, attempting to reconnect: %v\n", err)
				}
				continue
			}
		}

		// exhausted retries
		return fmt.Errorf("failed to insert ClickHouse batch after %d attempts: %v", attempt, err)
	}
	return fmt.Errorf("failed to insert ClickHouse batch after %d attempts", maxRetries)
}

func processChunk(mysqlDB *sql.DB, config Config, startID, endID int, chunkNum int, results chan<- string) {
	fmt.Printf("Processing chunk %d: ID range %d-%d\n", chunkNum, startID, endID)
	// Process in batches within the chunk
	offset := 0
	for {
		batchData, err := buildMigrationData(mysqlDB, config.BaseTable, startID, endID, config.BatchSize)
		if err != nil {
			results <- fmt.Sprintf("Chunk %d batch error: %v", chunkNum, err)
			return
		}

		if len(batchData) == 0 {
			break // No more data in this chunk
		}

		// Process batch data
		fmt.Printf("Chunk %d: Retrieved %d records in batch\n", chunkNum, len(batchData))

		// Get the last ID from this batch for next iteration
		if len(batchData) > 0 {
			lastID := batchData[len(batchData)-1]["id"]
			if lastID != nil {
				// Update startID for next batch within this chunk
				if id, ok := lastID.(int64); ok {
					startID = int(id) + 1
				}
			}
		}

		offset += len(batchData)
		if len(batchData) < config.BatchSize {
			break // Last batch in this chunk
		}
	}

	results <- fmt.Sprintf("Chunk %d completed successfully", chunkNum)
}

// parseDate safely parses a date string to ClickHouse Date format
func parseDate(value interface{}) time.Time {
	if value == nil {
		return time.Now()
	}

	dateStr := convertToString(value)
	if dateStr == "" {
		return time.Now()
	}

	// different date formats
	formats := []string{
		"2006-01-02",
		"2006-01-02 15:04:05",
		"02/01/2006",
		"01/02/2006",
		"2006-01-02T15:04:05Z",
	}

	for _, format := range formats {
		if parsed, err := time.Parse(format, dateStr); err == nil {
			return parsed
		}
	}

	// parsing fails, return current date
	fmt.Printf("Warning: Could not parse date '%s', using current date\n", dateStr)
	return time.Now()
}

// parseDateTime parses a datetime to ClickHouse DateTime format
func parseDateTime(value interface{}) time.Time {
	if value == nil {
		return time.Now() // Default to current time if nil
	}

	dateStr := convertToString(value)
	if dateStr == "" {
		return time.Now()
	}

	// Try different datetime formats
	formats := []string{
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05Z",
		"2006-01-02 15:04:05.000",
		"2006-01-02",
		"02/01/2006 15:04:05",
		"01/02/2006 15:04:05",
	}

	for _, format := range formats {
		if parsed, err := time.Parse(format, dateStr); err == nil {
			return parsed
		}
	}

	// parsing fails, return current time
	fmt.Printf("Warning: Could not parse datetime '%s', using current time\n", dateStr)
	return time.Now()
}

// truncateString truncates a string to the specified length
func truncateString(value string, maxLength int) string {
	if len(value) > maxLength {
		return value[:maxLength]
	}
	return value
}

// convertToUInt32 converts a value to uint32
func convertToUInt32(value interface{}) uint32 {
	if value == nil {
		return 0
	}

	switch v := value.(type) {
	case uint32:
		return v
	case uint64:
		if v > 4294967295 {
			return 0
		}
		return uint32(v)
	case int64:
		if v < 0 || v > 4294967295 {
			return 0
		}
		return uint32(v)
	case int32:
		if v < 0 {
			return 0
		}
		return uint32(v)
	case int:
		if v < 0 || v > 4294967295 {
			return 0
		}
		return uint32(v)
	case float64:
		if v < 0 || v > 4294967295 {
			return 0
		}
		return uint32(v)
	case string:
		if i, err := strconv.ParseUint(v, 10, 32); err == nil {
			return uint32(i)
		}
		return 0
	default:
		return 0
	}
}

// safeConvertToUInt32 converts a value to uint32
// Returns nil if the input is nil, allowing ClickHouse to handle as NULL
func safeConvertToUInt32(value interface{}) interface{} {
	if value == nil {
		return nil
	}

	switch v := value.(type) {
	case uint32:
		return v
	case uint64:
		if v > 4294967295 {
			return nil
		}
		return uint32(v)
	case int64:
		if v < 0 || v > 4294967295 {
			return nil
		}
		return uint32(v)
	case int32:
		if v < 0 {
			return nil
		}
		return uint32(v)
	case int:
		if v < 0 || v > 4294967295 {
			return nil
		}
		return uint32(v)
	case float64:
		if v < 0 || v > 4294967295 {
			return nil
		}
		return uint32(v)
	case string:
		if i, err := strconv.ParseUint(v, 10, 32); err == nil {
			return uint32(i)
		}
		return nil
	default:
		return nil
	}
}

// convertToUInt16 converts a value to uint16 for ClickHouse UInt16 fields
func convertToUInt16(value interface{}) uint16 {
	if value == nil {
		return 0
	}

	switch v := value.(type) {
	case uint16:
		return v
	case uint64:
		if v > 65535 {
			return 0
		}
		return uint16(v)
	case uint32:
		if v > 65535 {
			return 0
		}
		return uint16(v)
	case int64:
		if v < 0 || v > 65535 {
			return 0
		}
		return uint16(v)
	case int32:
		if v < 0 || v > 65535 {
			return 0
		}
		return uint16(v)
	case int:
		if v < 0 || v > 65535 {
			return 0
		}
		return uint16(v)
	case float64:
		if v < 0 || v > 65535 {
			return 0
		}
		return uint16(v)
	case string:
		if i, err := strconv.ParseUint(v, 10, 16); err == nil {
			return uint16(i)
		}
		return 0
	case []uint8: // Handle MySQL byte arrays
		if i, err := strconv.ParseUint(string(v), 10, 16); err == nil {
			return uint16(i)
		}
		return 0
	default:
		return 0
	}
}

// isConnectionError checks if the error is related to connection issues
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()

	// common ClickHouse connection error patterns
	connectionErrors := []string{
		"Unexpected packet",
		"connection refused",
		"connection reset",
		"broken pipe",
		"EOF",
		"timeout",
		"network is unreachable",
		"no route to host",
	}

	for _, connErr := range connectionErrors {
		if strings.Contains(strings.ToLower(errStr), strings.ToLower(connErr)) {
			return true
		}
	}

	return false
}

// executeInsertBatch executes the actual insert operation using ClickHouse native types
func executeInsertBatch(clickhouseDB *sql.DB, query string, records []map[string]interface{}) error {
	tx, err := clickhouseDB.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin ClickHouse transaction: %v", err)
	}

	txStmt, err := tx.Prepare(query)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to prepare ClickHouse statement in transaction: %v", err)
	}
	defer txStmt.Close()

	// Insert each record
	for _, record := range records {
		// Extract values using type assertions
		eventData := record["event"].(map[string]interface{})
		edition := record["edition"].(map[string]interface{})
		company := record["company"].(map[string]interface{})
		venue := record["venue"].(map[string]interface{})
		city := record["city"].(map[string]interface{})
		esInfo := record["es"].(map[string]interface{})
		editionDomain := record["edition_domain"].(string)

		_, err := txStmt.Exec(
			eventData["id"],                              // event_id: UInt32 NOT NULL
			convertToString(eventData["event_name"]),     // event_name: String NOT NULL
			convertToString(eventData["abbr_name"]),      // event_abbr_name: Nullable(String)
			convertToString(esInfo["event_description"]), // event_description: Nullable(String)
			convertToString(esInfo["event_punchline"]),   // event_punchline: Nullable(String)

			parseDate(eventData["start_date"]), // start_date: Date NOT NULL
			parseDate(eventData["end_date"]),   // end_date: Date NOT NULL

			edition["edition_id"], // edition_id: UInt32 NOT NULL
			truncateString(convertToString(eventData["country"]), 2), // edition_country: LowCardinality(FixedString(2))
			edition["edition_city"],                                  // edition_city: UInt32 NOT NULL
			city["event_city_lat"],                                   // edition_city_lat: Float64 NOT NULL
			city["event_city_long"],                                  // edition_city_long: Float64 NOT NULL

			company["id"],                                                  // company_id: Nullable(UInt32)
			convertToString(company["company_name"]),                       // company_name: Nullable(String)
			convertToString(company["company_domain"]),                     // company_domain: Nullable(String)
			convertToString(company["company_website"]),                    // company_website: Nullable(String)
			truncateString(convertToString(company["company_country"]), 2), // company_country: LowCardinality(FixedString(2))
			company["company_city"],                                        // company_city: Nullable(UInt32)

			venue["id"],                          // venue_id: Nullable(UInt32)
			convertToString(venue["venue_name"]), // venue_name: Nullable(String)
			truncateString(convertToString(venue["venue_country"]), 2), // venue_country: LowCardinality(FixedString(2))
			venue["venue_city"], // venue_city: Nullable(UInt32)
			venue["venue_lat"],  // venue_lat: Nullable(Float64)
			venue["venue_long"], // venue_long: Nullable(Float64)

			eventData["published"],                                  // published: Int8 NOT NULL
			truncateString(convertToString(eventData["status"]), 1), // status: LowCardinality(FixedString(1))

			convertToUInt16(eventData["event_audience"]), // editions_audiance_type: UInt16 NOT NULL
			convertToString(eventData["functionality"]),  // edition_functionality: LowCardinality(String) NOT NULL
			convertToString(edition["edition_website"]),  // edition_website: Nullable(String)
			editionDomain, // edition_domain: Nullable(String)

			"NA", // edition_type: LowCardinality(Nullable(String)) NOT NULL DEFAULT 'NA'
			safeConvertToUInt32(esInfo["event_following"]),    // event_followers: Nullable(UInt32)
			safeConvertToUInt32(esInfo["event_following"]),    // edition_followers: Nullable(UInt32)
			safeConvertToUInt32(esInfo["event_exhibitors"]),   // event_exhibitor: Nullable(UInt32)
			safeConvertToUInt32(esInfo["event_exhibitors"]),   // edition_exhibitor: Nullable(UInt32)
			safeConvertToUInt32(esInfo["event_totalSponsor"]), // event_sponsor: Nullable(UInt32)
			safeConvertToUInt32(esInfo["event_totalSponsor"]), // edition_sponsor: Nullable(UInt32)
			safeConvertToUInt32(esInfo["event_speakers"]),     // event_speaker: Nullable(UInt32)
			safeConvertToUInt32(esInfo["event_speakers"]),     // edition_speaker: Nullable(UInt32)

			parseDateTime(eventData["created"]),       // event_created: DateTime NOT NULL
			parseDateTime(edition["edition_created"]), // edition_created: DateTime NOT NULL
			uint32(1), // version: UInt32 NOT NULL DEFAULT 1
		)

		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to insert record %v: %v", eventData["id"], err)
		}
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit ClickHouse transaction: %v", err)
	}
	return nil
}

// fetchExhibitorSocialData fetches social media IDs for exhibitor companies
func fetchExhibitorSocialData(db *sql.DB, companyIDs []int64) map[int64]map[string]interface{} {
	if len(companyIDs) == 0 {
		return nil
	}

	batchSize := 1000
	var allSocialData map[int64]map[string]interface{}

	for i := 0; i < len(companyIDs); i += batchSize {
		end := i + batchSize
		if end > len(companyIDs) {
			end = len(companyIDs)
		}

		batch := companyIDs[i:end]
		placeholders := make([]string, len(batch))
		args := make([]interface{}, len(batch))
		for j, id := range batch {
			placeholders[j] = "?"
			args[j] = id
		}

		query := fmt.Sprintf(`
			SELECT 
				id, facebook_id, linkedin_id, twitter_id
			FROM company 
			WHERE id IN (%s)`, strings.Join(placeholders, ","))

		rows, err := db.Query(query, args...)
		if err != nil {
			fmt.Printf("Error fetching exhibitor social data batch %d-%d: %v\n", i, end-1, err)
			continue
		}

		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			continue
		}
		// Process this batch
		for rows.Next() {
			values := make([]interface{}, len(columns))
			valuePtrs := make([]interface{}, len(columns))
			for j := range values {
				valuePtrs[j] = &values[j]
			}

			if err := rows.Scan(valuePtrs...); err != nil {
				continue
			}

			row := make(map[string]interface{})
			for j, col := range columns {
				val := values[j]
				if val == nil {
					row[col] = nil
				} else {
					row[col] = val
				}
			}

			if companyID, ok := row["id"].(int64); ok {
				if allSocialData == nil {
					allSocialData = make(map[int64]map[string]interface{})
				}
				allSocialData[companyID] = row
			}
		}
		rows.Close()
	}

	return allSocialData
}

// insertExhibitorDataIntoClickHouse inserts exhibitor data into event_exhibitor_ch table
func insertExhibitorDataIntoClickHouse(clickhouseDB *sql.DB, exhibitorRecords []map[string]interface{}) error {
	if len(exhibitorRecords) == 0 {
		return nil
	}

	maxRetries := 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if err := executeExhibitorInsertBatchNative(clickhouseDB, exhibitorRecords); err != nil {
			if attempt < maxRetries && isConnectionError(err) {
				fmt.Printf("Exhibitor insertion attempt %d failed due to connection issue, retrying in 10 seconds: %v\n",
					attempt, err)
				time.Sleep(10 * time.Second)
				continue
			}
			return fmt.Errorf("exhibitor insertion failed after %d attempts: %v", attempt, err)
		}
		break
	}

	return nil
}

// executeExhibitorInsertBatchNative executes exhibitor insert
func executeExhibitorInsertBatchNative(clickhouseDB *sql.DB, records []map[string]interface{}) error {
	query := `
		INSERT INTO event_exhibitor_ch (
			company_id, company_id_name, edition_id, event_id, company_website,
			company_domain, company_country, company_city, facebook_id,
			linkedin_id, twitter_id, version
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`
	tx, err := clickhouseDB.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin ClickHouse exhibitor transaction: %v", err)
	}

	txStmt, err := tx.Prepare(query)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to prepare ClickHouse exhibitor statement in transaction: %v", err)
	}
	defer txStmt.Close()

	// Insert each record
	for _, record := range records {
		_, err := txStmt.Exec(
			record["company_id"],      // company_id: Nullable(UInt32)
			record["company_id_name"], // company_id_name: String NOT NULL
			record["edition_id"],      // edition_id: UInt32 NOT NULL
			record["event_id"],        // event_id: UInt32 NOT NULL
			record["company_website"], // company_website: Nullable(String)
			record["company_domain"],  // company_domain: Nullable(String)
			truncateString(convertToString(record["company_country"]), 2), // company_country: LowCardinality(FixedString(2))
			record["company_city"], // company_city: Nullable(UInt32)
			record["facebook_id"],  // facebook_id: Nullable(String)
			record["linkedin_id"],  // linkedin_id: Nullable(String)
			record["twitter_id"],   // twitter_id: Nullable(String)
			uint32(1),              // version: UInt32 NOT NULL DEFAULT 1
		)

		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to insert exhibitor record %v: %v", record["edition_id"], err)
		}
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit ClickHouse exhibitor transaction: %v", err)
	}

	return nil
}

// processExhibitorOnly processes only exhibitor data
func processExhibitorOnly(mysqlDB, clickhouseDB *sql.DB, config Config) {
	fmt.Println("=== Starting EXHIBITOR ONLY Processing ===")

	totalRecords, minID, maxID, err := getTotalRecordsAndIDRange(mysqlDB, "event_exhibitor")
	if err != nil {
		log.Fatal("Failed to get total records and ID range from event_exhibitor:", err)
	}

	fmt.Printf("Total exhibitor records: %d, Min ID: %d, Max ID: %d\n", totalRecords, minID, maxID)

	if config.NumChunks <= 0 {
		config.NumChunks = 5 // Default to 5 chunks if not specified
	}

	chunkSize := (maxID - minID + 1) / config.NumChunks
	if chunkSize == 0 {
		chunkSize = 1
	}

	fmt.Printf("Processing exhibitor data in %d chunks with chunk size: %d\n", config.NumChunks, chunkSize)

	results := make(chan string, config.NumChunks)
	semaphore := make(chan struct{}, config.NumWorkers)

	// Process chunks in parallel
	for i := 0; i < config.NumChunks; i++ {
		startID := minID + (i * chunkSize)
		endID := startID + chunkSize - 1

		// last chunk to include remaining records
		if i == config.NumChunks-1 {
			endID = maxID
		}
		// Add delay between chunk launches to reduce ClickHouse load
		if i > 0 {
			delay := 3 * time.Second
			time.Sleep(delay)
		}

		semaphore <- struct{}{}
		go func(chunkNum, start, end int) {
			defer func() { <-semaphore }()
			processExhibitorChunk(mysqlDB, clickhouseDB, start, end, chunkNum, results)
		}(i+1, startID, endID)
	}

	for i := 0; i < config.NumChunks; i++ {
		result := <-results
		fmt.Printf("Exhibitor Result: %s\n", result)
	}

	fmt.Println("Exhibitor processing completed!")
}

// processExhibitorChunk processes a single chunk of exhibitor data
func processExhibitorChunk(mysqlDB, clickhouseDB *sql.DB, startID, endID int, chunkNum int, results chan<- string) {
	fmt.Printf("Processing exhibitor chunk %d: ID range %d-%d\n", chunkNum, startID, endID)
	// Fetch all data for current chunk
	batchData, err := buildExhibitorMigrationData(mysqlDB, startID, endID, endID-startID+1)
	if err != nil {
		results <- fmt.Sprintf("Exhibitor chunk %d batch error: %v", chunkNum, err)
		return
	}

	if len(batchData) == 0 {
		fmt.Printf("Exhibitor chunk %d: No data found in range %d-%d\n", chunkNum, startID, endID)
		results <- fmt.Sprintf("Exhibitor chunk %d: No data found", chunkNum)
		return
	}
	fmt.Printf("Exhibitor chunk %d: Retrieved %d records in chunk\n", chunkNum, len(batchData))
	eventIDs := extractExhibitorEventIDs(batchData) // Extract event IDs from this batch to fetch social media data
	if len(eventIDs) > 0 {
		fmt.Printf("Exhibitor chunk %d: Processing %d exhibitor records\n", chunkNum, len(batchData))

		// Extract company IDs from exhibitor data to fetch social media information
		var exhibitorCompanyIDs []int64
		seenCompanyIDs := make(map[int64]bool)
		for _, exhibitor := range batchData {
			if companyID, ok := exhibitor["company_id"].(int64); ok && companyID > 0 {
				if !seenCompanyIDs[companyID] {
					exhibitorCompanyIDs = append(exhibitorCompanyIDs, companyID)
					seenCompanyIDs[companyID] = true
				}
			}
		}

		// Fetch social media data for exhibitor companies
		var socialData map[int64]map[string]interface{}
		if len(exhibitorCompanyIDs) > 0 {
			fmt.Printf("Exhibitor chunk %d: Fetching social media data for %d companies\n", chunkNum, len(exhibitorCompanyIDs))
			startTime := time.Now()
			socialData = fetchExhibitorSocialData(mysqlDB, exhibitorCompanyIDs)
			socialTime := time.Since(startTime)
			fmt.Printf("Exhibitor chunk %d: Retrieved social media data for %d companies in %v\n", chunkNum, len(socialData), socialTime)
		}

		// Prepare exhibitor records for ClickHouse insertion
		var exhibitorRecords []map[string]interface{}
		for _, exhibitor := range batchData {
			var companyDomain string
			if website, ok := exhibitor["website"].(string); ok && website != "" {
				companyDomain = extractDomainFromWebsite(website)
			} else if website, ok := exhibitor["website"].([]byte); ok && len(website) > 0 {
				websiteStr := string(website)
				companyDomain = extractDomainFromWebsite(websiteStr)
			}

			// Get social media data for this company
			var facebookID, linkedinID, twitterID interface{}
			if companyID, ok := exhibitor["company_id"].(int64); ok && socialData != nil {
				if social, exists := socialData[companyID]; exists {
					facebookID = social["facebook_id"]
					linkedinID = social["linkedin_id"]
					twitterID = social["twitter_id"]
				}
			}

			// Create exhibitor record
			exhibitorRecord := map[string]interface{}{
				"company_id":      exhibitor["company_id"],
				"company_id_name": getCompanyNameOrDefault(exhibitor["company_name"]),
				"edition_id":      exhibitor["edition_id"],
				"event_id":        exhibitor["event_id"],
				"company_website": exhibitor["website"],
				"company_domain":  companyDomain,
				"company_country": exhibitor["country"],
				"company_city":    exhibitor["city"],
				"facebook_id":     facebookID,
				"linkedin_id":     linkedinID,
				"twitter_id":      twitterID,
			}

			exhibitorRecords = append(exhibitorRecords, exhibitorRecord)
		}

		// Insert exhibitor data into ClickHouse
		if len(exhibitorRecords) > 0 {
			fmt.Printf("Exhibitor chunk %d: Attempting to insert %d records into event_exhibitor_ch...\n", chunkNum, len(exhibitorRecords))
			maxExhibitorRetries := 3
			var exhibitorInsertErr error

			for attempt := 1; attempt <= maxExhibitorRetries; attempt++ {
				if err := insertExhibitorDataIntoClickHouse(clickhouseDB, exhibitorRecords); err != nil {
					exhibitorInsertErr = err
					if attempt < maxExhibitorRetries {
						fmt.Printf("Exhibitor chunk %d: Insertion attempt %d failed, retrying in 10 seconds: %v\n",
							chunkNum, attempt, err)
						time.Sleep(10 * time.Second)
						continue
					}
				} else {
					fmt.Printf("Exhibitor chunk %d: Successfully inserted %d records into event_exhibitor_ch\n", chunkNum, len(exhibitorRecords))
					exhibitorInsertErr = nil
					break
				}
			}

			if exhibitorInsertErr != nil {
				fmt.Printf("Exhibitor chunk %d: Insertion failed after %d attempts: %v\n",
					chunkNum, maxExhibitorRetries, exhibitorInsertErr)
				results <- fmt.Sprintf("Exhibitor chunk %d: Failed to insert %d records", chunkNum, len(exhibitorRecords))
				return
			}
		} else {
			fmt.Printf("Exhibitor chunk %d: No exhibitor records to insert\n", chunkNum)
		}
	}

	results <- fmt.Sprintf("Exhibitor chunk %d: Completed successfully", chunkNum)
}

// buildExhibitorMigrationData builds migration data for exhibitor processing
func buildExhibitorMigrationData(db *sql.DB, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT 
			id, company_id, company_name, event_id, edition_id, country, city, website
		FROM event_exhibitor 
		WHERE id >= %d AND id <= %d 
		ORDER BY id 
		LIMIT %d`, startID, endID, batchSize)

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
			val := values[i]
			if val == nil {
				row[col] = nil
			} else {
				row[col] = val
			}
		}
		results = append(results, row)
	}

	return results, nil
}

// extractExhibitorEventIDs extracts event IDs from exhibitor data
func extractExhibitorEventIDs(exhibitorData []map[string]interface{}) []int64 {
	var eventIDs []int64
	seen := make(map[int64]bool)

	for _, exhibitor := range exhibitorData {
		if eventID, ok := exhibitor["event_id"].(int64); ok && eventID > 0 {
			if !seen[eventID] {
				eventIDs = append(eventIDs, eventID)
				seen[eventID] = true
			}
		}
	}

	return eventIDs
}

// processSponsorsOnly processes only sponsors data
func processSponsorsOnly(mysqlDB, clickhouseDB *sql.DB, config Config) {
	fmt.Println("=== Starting SPONSORS ONLY Processing ===")

	totalRecords, minID, maxID, err := getTotalRecordsAndIDRange(mysqlDB, "event_sponsors")
	if err != nil {
		log.Fatal("Failed to get total records and ID range from event_sponsors:", err)
	}

	fmt.Printf("Total sponsors records: %d, Min ID: %d, Max ID: %d\n", totalRecords, minID, maxID)

	if config.NumChunks <= 0 {
		config.NumChunks = 5 // Default to 5 chunks if not specified
	}

	chunkSize := (maxID - minID + 1) / config.NumChunks
	if chunkSize == 0 {
		chunkSize = 1
	}

	fmt.Printf("Processing sponsors data in %d chunks with chunk size: %d\n", config.NumChunks, chunkSize)

	results := make(chan string, config.NumChunks)
	semaphore := make(chan struct{}, config.NumWorkers)

	for i := 0; i < config.NumChunks; i++ {
		startID := minID + (i * chunkSize)
		endID := startID + chunkSize - 1
		// Adjust last chunk to include remaining records
		if i == config.NumChunks-1 {
			endID = maxID
		}

		// Add delay between chunk launches to reduce ClickHouse load
		if i > 0 {
			delay := 3 * time.Second
			fmt.Printf("Waiting %v before launching sponsors chunk %d...\n", delay, i+1)
			time.Sleep(delay)
		}

		// Use semaphore to limit concurrent workers
		semaphore <- struct{}{}
		go func(chunkNum, start, end int) {
			defer func() { <-semaphore }()
			processSponsorsChunk(mysqlDB, clickhouseDB, start, end, chunkNum, results)
		}(i+1, startID, endID)
	}

	for i := 0; i < config.NumChunks; i++ {
		result := <-results
		fmt.Printf("Sponsors Result: %s\n", result)
	}

	fmt.Println("Sponsors processing completed!")
}

// processSponsorsChunk processes a single chunk of sponsors data
func processSponsorsChunk(mysqlDB, clickhouseDB *sql.DB, startID, endID int, chunkNum int, results chan<- string) {
	fmt.Printf("Processing sponsors chunk %d: ID range %d-%d\n", chunkNum, startID, endID)

	batchData, err := buildSponsorsMigrationData(mysqlDB, startID, endID, endID-startID+1)
	if err != nil {
		results <- fmt.Sprintf("Sponsors chunk %d batch error: %v", chunkNum, err)
		return
	}

	if len(batchData) == 0 {
		fmt.Printf("Sponsors chunk %d: No data found in range %d-%d\n", chunkNum, startID, endID)
		results <- fmt.Sprintf("Sponsors chunk %d: No data found", chunkNum)
		return
	}

	fmt.Printf("Sponsors chunk %d: Retrieved %d records in chunk\n", chunkNum, len(batchData))

	// Extract company IDs from this batch to fetch social media and website information
	var sponsorCompanyIDs []int64
	seenCompanyIDs := make(map[int64]bool)
	for _, sponsor := range batchData {
		if companyID, ok := sponsor["company_id"].(int64); ok && companyID > 0 {
			if !seenCompanyIDs[companyID] {
				sponsorCompanyIDs = append(sponsorCompanyIDs, companyID)
				seenCompanyIDs[companyID] = true
			}
		}
	}

	// Fetch social media and website data for sponsor companies
	var companyData map[int64]map[string]interface{}
	if len(sponsorCompanyIDs) > 0 {
		fmt.Printf("Sponsors chunk %d: Fetching company data for %d companies\n", chunkNum, len(sponsorCompanyIDs))
		startTime := time.Now()
		companyData = fetchSponsorsCompanyData(mysqlDB, sponsorCompanyIDs)
		companyTime := time.Since(startTime)
		fmt.Printf("Sponsors chunk %d: Retrieved company data for %d companies in %v\n", chunkNum, len(companyData), companyTime)
	}

	// Prepare sponsors records for ClickHouse insertion
	var sponsorRecords []map[string]interface{}
	for _, sponsor := range batchData {
		// Get company data for this sponsor
		var companyWebsite, companyDomain, facebookID, linkedinID, twitterID, companyCountry interface{}
		if companyID, ok := sponsor["company_id"].(int64); ok && companyData != nil {
			if company, exists := companyData[companyID]; exists {
				companyWebsite = company["website"]
				companyCountry = company["country"]

				// Extract domain from website
				if website, ok := companyWebsite.(string); ok && website != "" {
					companyDomain = extractDomainFromWebsite(website)
				} else if website, ok := companyWebsite.([]byte); ok && len(website) > 0 {
					websiteStr := string(website)
					companyDomain = extractDomainFromWebsite(websiteStr)
				}

				facebookID = company["facebook_id"]
				linkedinID = company["linkedin_id"]
				twitterID = company["twitter_id"]
			}
		}

		// Create sponsor record
		sponsorRecord := map[string]interface{}{
			"company_id":      sponsor["company_id"],
			"company_id_name": getCompanyNameOrDefault(sponsor["name"]),
			"edition_id":      sponsor["event_edition"],
			"event_id":        sponsor["event_id"],
			"company_website": companyWebsite,
			"company_domain":  companyDomain,
			"company_country": companyCountry,
			"company_city":    nil,
			"facebook_id":     facebookID,
			"linkedin_id":     linkedinID,
			"twitter_id":      twitterID,
		}

		sponsorRecords = append(sponsorRecords, sponsorRecord)
	}

	// Insert sponsors data into ClickHouse
	if len(sponsorRecords) > 0 {
		fmt.Printf("Sponsors chunk %d: Attempting to insert %d records into event_sponsors_ch...\n", chunkNum, len(sponsorRecords))

		maxSponsorRetries := 3
		var sponsorInsertErr error

		for attempt := 1; attempt <= maxSponsorRetries; attempt++ {
			if err := insertSponsorsDataIntoClickHouse(clickhouseDB, sponsorRecords); err != nil {
				sponsorInsertErr = err
				if attempt < maxSponsorRetries {
					fmt.Printf("Sponsors chunk %d: Insertion attempt %d failed, retrying in 10 seconds: %v\n",
						chunkNum, attempt, err)
					time.Sleep(10 * time.Second)
					continue
				}
			} else {
				fmt.Printf("Sponsors chunk %d: Successfully inserted %d records into event_sponsors_ch\n", chunkNum, len(sponsorRecords))
				sponsorInsertErr = nil
				break
			}
		}

		if sponsorInsertErr != nil {
			fmt.Printf("Sponsors chunk %d: Insertion failed after %d attempts: %v\n",
				chunkNum, maxSponsorRetries, sponsorInsertErr)
			results <- fmt.Sprintf("Sponsors chunk %d: Failed to insert %d records", chunkNum, len(sponsorRecords))
			return
		}
	} else {
		fmt.Printf("Sponsors chunk %d: No sponsor records to insert\n", chunkNum)
	}

	results <- fmt.Sprintf("Sponsors chunk %d: Completed successfully", chunkNum)
}

// buildSponsorsMigrationData builds migration data for sponsors processing
func buildSponsorsMigrationData(db *sql.DB, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT 
			id, company_id, name, event_id, event_edition
		FROM event_sponsors 
		WHERE id >= %d AND id <= %d 
		ORDER BY id 
		LIMIT %d`, startID, endID, batchSize)

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
			val := values[i]
			if val == nil {
				row[col] = nil
			} else {
				row[col] = val
			}
		}
		results = append(results, row)
	}

	return results, nil
}

// fetchSponsorsCompanyData fetches company data for sponsors including social media and website
func fetchSponsorsCompanyData(db *sql.DB, companyIDs []int64) map[int64]map[string]interface{} {
	if len(companyIDs) == 0 {
		return nil
	}

	batchSize := 1000
	var allCompanyData map[int64]map[string]interface{}

	for i := 0; i < len(companyIDs); i += batchSize {
		end := i + batchSize
		if end > len(companyIDs) {
			end = len(companyIDs)
		}

		batch := companyIDs[i:end]

		placeholders := make([]string, len(batch))
		args := make([]interface{}, len(batch))
		for j, id := range batch {
			placeholders[j] = "?"
			args[j] = id
		}

		query := fmt.Sprintf(`
			SELECT 
				id, website, country, facebook_id, linkedin_id, twitter_id
			FROM company 
			WHERE id IN (%s)`, strings.Join(placeholders, ","))

		rows, err := db.Query(query, args...)
		if err != nil {
			fmt.Printf("Error fetching sponsors company data batch %d-%d: %v\n", i, end-1, err)
			continue
		}

		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			continue
		}

		for rows.Next() {
			values := make([]interface{}, len(columns))
			valuePtrs := make([]interface{}, len(columns))
			for j := range values {
				valuePtrs[j] = &values[j]
			}

			if err := rows.Scan(valuePtrs...); err != nil {
				continue
			}

			row := make(map[string]interface{})
			for j, col := range columns {
				val := values[j]
				if val == nil {
					row[col] = nil
				} else {
					row[col] = val
				}
			}

			if companyID, ok := row["id"].(int64); ok {
				if allCompanyData == nil {
					allCompanyData = make(map[int64]map[string]interface{})
				}
				allCompanyData[companyID] = row
			}
		}
		rows.Close()
	}

	return allCompanyData
}

// insertSponsorsDataIntoClickHouse inserts sponsors data into event_sponsors_ch table
func insertSponsorsDataIntoClickHouse(clickhouseDB *sql.DB, sponsorRecords []map[string]interface{}) error {
	if len(sponsorRecords) == 0 {
		return nil
	}

	maxRetries := 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if err := executeSponsorsInsertBatchNative(clickhouseDB, sponsorRecords); err != nil {
			if attempt < maxRetries && isConnectionError(err) {
				fmt.Printf("Sponsors insertion attempt %d failed due to connection issue, retrying in 10 seconds: %v\n",
					attempt, err)
				time.Sleep(10 * time.Second)
				continue
			}
			return fmt.Errorf("sponsors insertion failed after %d attempts: %v", attempt, err)
		}
		break
	}

	return nil
}

// executeSponsorsInsertBatchNative executes sponsors insert
func executeSponsorsInsertBatchNative(clickhouseDB *sql.DB, records []map[string]interface{}) error {
	query := `
		INSERT INTO event_sponsors_ch (
			company_id, company_id_name, edition_id, event_id, company_website,
			company_domain, company_country, company_city, facebook_id,
			linkedin_id, twitter_id, version
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	tx, err := clickhouseDB.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin ClickHouse sponsors transaction: %v", err)
	}

	txStmt, err := tx.Prepare(query)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to prepare ClickHouse sponsors statement in transaction: %v", err)
	}
	defer txStmt.Close()

	// Insert each record
	for _, record := range records {
		_, err := txStmt.Exec(
			record["company_id"],      // company_id: Nullable(UInt32)
			record["company_id_name"], // company_id_name: String NOT NULL
			record["edition_id"],      // edition_id: UInt32 NOT NULL
			record["event_id"],        // event_id: UInt32 NOT NULL
			record["company_website"], // company_website: Nullable(String)
			record["company_domain"],  // company_domain: Nullable(String)
			truncateString(convertToString(record["company_country"]), 2), // company_country: LowCardinality(FixedString(2))
			record["company_city"], // company_city: Nullable(UInt32)
			record["facebook_id"],  // facebook_id: Nullable(String)
			record["linkedin_id"],  // linkedin_id: Nullable(String)
			record["twitter_id"],   // twitter_id: Nullable(String)
			uint32(1),              // version: UInt32 NOT NULL DEFAULT 1
		)

		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to insert sponsor record %v: %v", record["edition_id"], err)
		}
	}
	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit ClickHouse sponsors transaction: %v", err)
	}

	return nil
}

// getCompanyNameOrDefault provides a default value for company names if they are NULL
func getCompanyNameOrDefault(companyName interface{}) string {
	if companyName == nil {
		return "N/A" // Default value for NULL company names
	}

	if name, ok := companyName.(string); ok {
		if strings.TrimSpace(name) == "" {
			return "N/A" // Default value for empty strings
		}
		return name
	}

	if name, ok := companyName.([]byte); ok {
		nameStr := string(name)
		if strings.TrimSpace(nameStr) == "" {
			return "N/A" // Default value for empty byte arrays
		}
		return nameStr
	}

	return "N/A" // Default value for unknown types
}

// processVisitorsOnly processes only visitors data without the main event/edition flow
func processVisitorsOnly(mysqlDB, clickhouseDB *sql.DB, config Config) {
	fmt.Println("=== Starting VISITORS ONLY Processing ===")

	totalRecords, minID, maxID, err := getTotalRecordsAndIDRange(mysqlDB, "event_visitor")
	if err != nil {
		log.Fatal("Failed to get total records and ID range from event_visitor:", err)
	}

	fmt.Printf("Total visitors records: %d, Min ID: %d, Max ID: %d\n", totalRecords, minID, maxID)

	if config.NumChunks <= 0 {
		config.NumChunks = 5 // Default to 5 chunks if not specified
	}

	chunkSize := (maxID - minID + 1) / config.NumChunks
	if chunkSize == 0 {
		chunkSize = 1
	}

	fmt.Printf("Processing visitors data in %d chunks with chunk size: %d\n", config.NumChunks, chunkSize)

	results := make(chan string, config.NumChunks)
	semaphore := make(chan struct{}, config.NumWorkers)

	for i := 0; i < config.NumChunks; i++ {
		startID := minID + (i * chunkSize)
		endID := startID + chunkSize - 1

		// Adjust last chunk to include remaining records
		if i == config.NumChunks-1 {
			endID = maxID
		}

		// Add delay between chunk launches to reduce ClickHouse load
		if i > 0 {
			delay := 3 * time.Second
			fmt.Printf("Waiting %v before launching visitors chunk %d...\n", delay, i+1)
			time.Sleep(delay)
		}

		semaphore <- struct{}{}
		go func(chunkNum, start, end int) {
			defer func() { <-semaphore }()
			processVisitorsChunk(mysqlDB, clickhouseDB, start, end, chunkNum, results)
		}(i+1, startID, endID)
	}

	for i := 0; i < config.NumChunks; i++ {
		result := <-results
		fmt.Printf("Visitors Result: %s\n", result)
	}

	fmt.Println("Visitors processing completed!")
}

// processVisitorsChunk processes a single chunk of visitors data
func processVisitorsChunk(mysqlDB, clickhouseDB *sql.DB, startID, endID int, chunkNum int, results chan<- string) {
	fmt.Printf("Processing visitors chunk %d: ID range %d-%d\n", chunkNum, startID, endID)

	batchData, err := buildVisitorsMigrationData(mysqlDB, startID, endID, endID-startID+1)
	if err != nil {
		results <- fmt.Sprintf("Visitors chunk %d batch error: %v", chunkNum, err)
		return
	}

	if len(batchData) == 0 {
		fmt.Printf("Visitors chunk %d: No data found in range %d-%d\n", chunkNum, startID, endID)
		results <- fmt.Sprintf("Visitors chunk %d: No data found", chunkNum)
		return
	}

	fmt.Printf("Visitors chunk %d: Retrieved %d records in chunk\n", chunkNum, len(batchData))

	// Extract user IDs from this batch to fetch user names
	var userIDs []int64
	seenUserIDs := make(map[int64]bool)
	for _, visitor := range batchData {
		if userID, ok := visitor["user"].(int64); ok && userID > 0 {
			if !seenUserIDs[userID] {
				userIDs = append(userIDs, userID)
				seenUserIDs[userID] = true
			}
		}
	}

	// Fetch user data for visitors
	var userData map[int64]map[string]interface{}
	if len(userIDs) > 0 {
		fmt.Printf("Visitors chunk %d: Fetching user data for %d users\n", chunkNum, len(userIDs))
		startTime := time.Now()
		userData = fetchVisitorsUserData(mysqlDB, userIDs)
		userTime := time.Since(startTime)
		fmt.Printf("Visitors chunk %d: Retrieved user data for %d users in %v\n", chunkNum, len(userData), userTime)
	}

	// Prepare visitors records for ClickHouse insertion
	var visitorRecords []map[string]interface{}
	for _, visitor := range batchData {
		// Get user data for this visitor
		var userName, userCompany interface{}
		if userID, ok := visitor["user"].(int64); ok && userData != nil {
			if user, exists := userData[userID]; exists {
				userName = user["name"]
				userCompany = user["user_company"] // Use user_company from user table
			}
		}

		// Create visitor record
		visitorRecord := map[string]interface{}{
			"user_id":          visitor["user"],
			"event_id":         visitor["event"],
			"edition_id":       visitor["edition"],
			"user_name":        convertToString(userName), // No default value, let ClickHouse handle NULL
			"user_company":     userCompany,               // Use user_company from user table
			"user_designation": visitor["visitor_designation"],
			"user_city":        visitor["visitor_city"],
			"user_country":     visitor["visitor_country"],
		}

		visitorRecords = append(visitorRecords, visitorRecord)
	}

	// Insert visitors data into ClickHouse
	if len(visitorRecords) > 0 {
		fmt.Printf("Visitors chunk %d: Attempting to insert %d records into event_visitor_ch...\n", chunkNum, len(visitorRecords))

		maxVisitorRetries := 3
		var visitorInsertErr error

		for attempt := 1; attempt <= maxVisitorRetries; attempt++ {
			if err := insertVisitorsDataIntoClickHouse(clickhouseDB, visitorRecords); err != nil {
				visitorInsertErr = err
				if attempt < maxVisitorRetries {
					fmt.Printf("Visitors chunk %d: Insertion attempt %d failed, retrying in 10 seconds: %v\n",
						chunkNum, attempt, err)
					time.Sleep(10 * time.Second)
					continue
				}
			} else {
				fmt.Printf("Visitors chunk %d: Successfully inserted %d records into event_visitor_ch\n", chunkNum, len(visitorRecords))
				visitorInsertErr = nil
				break
			}
		}

		if visitorInsertErr != nil {
			fmt.Printf("Visitors chunk %d: Insertion failed after %d attempts: %v\n",
				chunkNum, maxVisitorRetries, visitorInsertErr)
			results <- fmt.Sprintf("Visitors chunk %d: Failed to insert %d records", chunkNum, len(visitorRecords))
			return
		}
	} else {
		fmt.Printf("Visitors chunk %d: No visitor records to insert\n", chunkNum)
	}

	results <- fmt.Sprintf("Visitors chunk %d: Completed successfully", chunkNum)
}

// buildVisitorsMigrationData builds migration data for visitors
func buildVisitorsMigrationData(db *sql.DB, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT 
			id, user, event, edition, visitor_company, visitor_designation, visitor_city, visitor_country
		FROM event_visitor 
		WHERE id >= %d AND id <= %d 
		ORDER BY id 
		LIMIT %d`, startID, endID, batchSize)

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
			val := values[i]
			if val == nil {
				row[col] = nil
			} else {
				row[col] = val
			}
		}
		results = append(results, row)
	}

	return results, nil
}

// fetchVisitorsUserData fetches user data for visitors
func fetchVisitorsUserData(db *sql.DB, userIDs []int64) map[int64]map[string]interface{} {
	if len(userIDs) == 0 {
		return nil
	}

	batchSize := 1000
	var allUserData map[int64]map[string]interface{}

	for i := 0; i < len(userIDs); i += batchSize {
		end := i + batchSize
		if end > len(userIDs) {
			end = len(userIDs)
		}

		batch := userIDs[i:end]

		placeholders := make([]string, len(batch))
		args := make([]interface{}, len(batch))
		for j, id := range batch {
			placeholders[j] = "?"
			args[j] = id
		}

		query := fmt.Sprintf(`
			SELECT 
				id, name, user_company
			FROM user 
			WHERE id IN (%s)`, strings.Join(placeholders, ","))

		rows, err := db.Query(query, args...)
		if err != nil {
			fmt.Printf("Error fetching visitors user data batch %d-%d: %v\n", i, end-1, err)
			continue
		}

		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			continue
		}

		for rows.Next() {
			values := make([]interface{}, len(columns))
			valuePtrs := make([]interface{}, len(columns))
			for j := range values {
				valuePtrs[j] = &values[j]
			}

			if err := rows.Scan(valuePtrs...); err != nil {
				continue
			}

			row := make(map[string]interface{})
			for j, col := range columns {
				val := values[j]
				if val == nil {
					row[col] = nil
				} else {
					row[col] = val
				}
			}

			if userID, ok := row["id"].(int64); ok {
				if allUserData == nil {
					allUserData = make(map[int64]map[string]interface{})
				}
				allUserData[userID] = row
			}
		}
		rows.Close()
	}

	return allUserData
}

// insertVisitorsDataIntoClickHouse inserts visitors data into event_visitors_ch table
func insertVisitorsDataIntoClickHouse(clickhouseDB *sql.DB, visitorRecords []map[string]interface{}) error {
	if len(visitorRecords) == 0 {
		return nil
	}

	query := `
		INSERT INTO event_visitors_ch (
			user_id, event_id, edition_id, user_name, user_company,
			user_designation, user_city, user_country, version
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	maxRetries := 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if err := executeVisitorsInsertBatch(clickhouseDB, query, visitorRecords); err != nil {
			if attempt < maxRetries && isConnectionError(err) {
				fmt.Printf("Visitors insertion attempt %d failed due to connection issue, retrying in 10 seconds: %v\n",
					attempt, err)
				time.Sleep(10 * time.Second)
				continue
			}
			return fmt.Errorf("visitors insertion failed after %d attempts: %v", attempt, err)
		}
		break
	}

	return nil
}

// executeVisitorsInsertBatch executes the actual visitors insert operation
func executeVisitorsInsertBatch(clickhouseDB *sql.DB, query string, records []map[string]interface{}) error {
	tx, err := clickhouseDB.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin ClickHouse visitors transaction: %v", err)
	}

	txStmt, err := tx.Prepare(query)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to prepare ClickHouse visitors statement in transaction: %v", err)
	}
	defer txStmt.Close()

	for _, record := range records {
		_, err := txStmt.Exec(
			convertToUInt32(record["user_id"]),                         // user_id: UInt32 NOT NULL
			convertToUInt32(record["event_id"]),                        // event_id: UInt32 NOT NULL
			convertToUInt32(record["edition_id"]),                      // edition_id: UInt32 NOT NULL
			convertToString(record["user_name"]),                       // user_name: String NOT NULL
			convertToString(record["user_company"]),                    // user_company: Nullable(String)
			convertToString(record["user_designation"]),                // user_designation: Nullable(String)
			safeConvertToUInt32(record["user_city"]),                   // user_city: Nullable(UInt32)
			truncateString(convertToString(record["user_country"]), 2), // user_country: LowCardinality(Nullable(FixedString(2)))
			uint32(1), // version: UInt32 NOT NULL DEFAULT 1
		)

		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to insert visitor record %v: %v", record["user_id"], err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit ClickHouse visitors transaction: %v", err)
	}

	return nil
}

// processSpeakersOnly processes only speakers data
func processSpeakersOnly(mysqlDB, clickhouseDB *sql.DB, config Config) {
	fmt.Println("=== Starting SPEAKERS ONLY Processing ===")

	totalRecords, minID, maxID, err := getTotalRecordsAndIDRange(mysqlDB, "event_speaker")
	if err != nil {
		log.Fatal("Failed to get total records and ID range from event_speaker:", err)
	}

	fmt.Printf("Total speakers records: %d, Min ID: %d, Max ID: %d\n", totalRecords, minID, maxID)

	if config.NumChunks <= 0 {
		config.NumChunks = 5 // Default to 5 chunks if not specified
	}

	chunkSize := (maxID - minID + 1) / config.NumChunks
	if chunkSize == 0 {
		chunkSize = 1
	}

	fmt.Printf("Processing speakers data in %d chunks with chunk size: %d\n", config.NumChunks, chunkSize)

	results := make(chan string, config.NumChunks)
	semaphore := make(chan struct{}, config.NumWorkers)

	for i := 0; i < config.NumChunks; i++ {
		startID := minID + (i * chunkSize)
		endID := startID + chunkSize - 1

		// last chunk to include remaining records
		if i == config.NumChunks-1 {
			endID = maxID
		}

		// delay between chunk launches to reduce ClickHouse load
		if i > 0 {
			delay := 3 * time.Second
			fmt.Printf("Waiting %v before launching speakers chunk %d...\n", delay, i+1)
			time.Sleep(delay)
		}

		semaphore <- struct{}{}
		go func(chunkNum, start, end int) {
			defer func() { <-semaphore }()
			processSpeakersChunk(mysqlDB, clickhouseDB, start, end, chunkNum, results)
		}(i+1, startID, endID)
	}

	for i := 0; i < config.NumChunks; i++ {
		result := <-results
		fmt.Printf("Speakers Result: %s\n", result)
	}

	fmt.Println("Speakers processing completed!")
}

// processSpeakersChunk processes a single chunk of speakers data
func processSpeakersChunk(mysqlDB, clickhouseDB *sql.DB, startID, endID int, chunkNum int, results chan<- string) {
	fmt.Printf("Processing speakers chunk %d: ID range %d-%d\n", chunkNum, startID, endID)

	// Fetch all data for current chunk
	batchData, err := buildSpeakersMigrationData(mysqlDB, startID, endID, endID-startID+1)
	if err != nil {
		results <- fmt.Sprintf("Speakers chunk %d batch error: %v", chunkNum, err)
		return
	}

	if len(batchData) == 0 {
		fmt.Printf("Speakers chunk %d: No data found in range %d-%d\n", chunkNum, startID, endID)
		results <- fmt.Sprintf("Speakers chunk %d: No data found", chunkNum)
		return
	}

	fmt.Printf("Speakers chunk %d: Retrieved %d records in chunk\n", chunkNum, len(batchData))

	// Extract user IDs from this batch to fetch user names
	var userIDs []int64
	seenUserIDs := make(map[int64]bool)
	for _, speaker := range batchData {
		if userID, ok := speaker["user_id"].(int64); ok && userID > 0 {
			if !seenUserIDs[userID] {
				userIDs = append(userIDs, userID)
				seenUserIDs[userID] = true
			}
		}
	}

	// Fetch user data for speakers
	var userData map[int64]map[string]interface{}
	if len(userIDs) > 0 {
		fmt.Printf("Speakers chunk %d: Fetching user data for %d users\n", chunkNum, len(userIDs))
		startTime := time.Now()
		userData = fetchSpeakersUserData(mysqlDB, userIDs)
		userTime := time.Since(startTime)
		fmt.Printf("Speakers chunk %d: Retrieved user data for %d users in %v\n", chunkNum, len(userData), userTime)
	}

	// Prepare speakers records for ClickHouse insertion
	var speakerRecords []map[string]interface{}
	for _, speaker := range batchData {
		// Get user data for this speaker
		var userName, userCompany, userDesignation, userCity, userCountry interface{}
		if userID, ok := speaker["user_id"].(int64); ok && userData != nil {
			if user, exists := userData[userID]; exists {
				userName = speaker["speaker_name"] // Use speaker_name from speaker table
				userCompany = user["user_company"] // Use user_company from user table
				userDesignation = user["designation"]
				userCity = user["city"]
				userCountry = user["country"]
			}
		}

		// Create speaker record
		speakerRecord := map[string]interface{}{
			"user_id":          speaker["user_id"],
			"event_id":         speaker["event"],
			"edition_id":       speaker["edition"],
			"user_name":        convertToString(userName), // No default value, let ClickHouse handle NULL
			"user_company":     userCompany,
			"user_designation": userDesignation,
			"user_city":        userCity,
			"user_country":     userCountry,
		}

		speakerRecords = append(speakerRecords, speakerRecord)
	}

	// Insert speakers data into ClickHouse
	if len(speakerRecords) > 0 {
		fmt.Printf("Speakers chunk %d: Attempting to insert %d records into event_speaker_ch...\n", chunkNum, len(speakerRecords))

		maxSpeakerRetries := 3
		var speakerInsertErr error

		for attempt := 1; attempt <= maxSpeakerRetries; attempt++ {
			if err := insertSpeakersDataIntoClickHouse(clickhouseDB, speakerRecords); err != nil {
				speakerInsertErr = err
				if attempt < maxSpeakerRetries {
					fmt.Printf("Speakers chunk %d: Insertion attempt %d failed, retrying in 10 seconds: %v\n",
						chunkNum, attempt, err)
					time.Sleep(10 * time.Second)
					continue
				}
			} else {
				fmt.Printf("Speakers chunk %d: Successfully inserted %d records into event_speaker_ch\n", chunkNum, len(speakerRecords))
				speakerInsertErr = nil
				break
			}
		}

		if speakerInsertErr != nil {
			fmt.Printf("Speakers chunk %d: Insertion failed after %d attempts: %v\n",
				chunkNum, maxSpeakerRetries, speakerInsertErr)
			results <- fmt.Sprintf("Speakers chunk %d: Failed to insert %d records", chunkNum, len(speakerRecords))
			return
		}
	} else {
		fmt.Printf("Speakers chunk %d: No speaker records to insert\n", chunkNum)
	}

	results <- fmt.Sprintf("Speakers chunk %d: Completed successfully", chunkNum)
}

// buildSpeakersMigrationData builds migration data for speakers
func buildSpeakersMigrationData(db *sql.DB, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT 
			user_id, event, edition, speaker_name
		FROM event_speaker 
		WHERE id >= %d AND id <= %d 
		ORDER BY id 
		LIMIT %d`, startID, endID, batchSize)

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
			val := values[i]
			if val == nil {
				row[col] = nil
			} else {
				row[col] = val
			}
		}
		results = append(results, row)
	}

	return results, nil
}

// fetchSpeakersUserData fetches user data for speakers
func fetchSpeakersUserData(db *sql.DB, userIDs []int64) map[int64]map[string]interface{} {
	if len(userIDs) == 0 {
		return nil
	}

	batchSize := 1000
	var allUserData map[int64]map[string]interface{}

	for i := 0; i < len(userIDs); i += batchSize {
		end := i + batchSize
		if end > len(userIDs) {
			end = len(userIDs)
		}

		batch := userIDs[i:end]

		placeholders := make([]string, len(batch))
		args := make([]interface{}, len(batch))
		for j, id := range batch {
			placeholders[j] = "?"
			args[j] = id
		}

		query := fmt.Sprintf(`
			SELECT 
				id, user_company, designation, city, country
			FROM user 
			WHERE id IN (%s)`, strings.Join(placeholders, ","))

		rows, err := db.Query(query, args...)
		if err != nil {
			fmt.Printf("Error fetching speakers user data batch %d-%d: %v\n", i, end-1, err)
			continue
		}

		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			continue
		}

		for rows.Next() {
			values := make([]interface{}, len(columns))
			valuePtrs := make([]interface{}, len(columns))
			for j := range values {
				valuePtrs[j] = &values[j]
			}

			if err := rows.Scan(valuePtrs...); err != nil {
				continue
			}

			row := make(map[string]interface{})
			for j, col := range columns {
				val := values[j]
				if val == nil {
					row[col] = nil
				} else {
					row[col] = val
				}
			}

			if userID, ok := row["id"].(int64); ok {
				if allUserData == nil {
					allUserData = make(map[int64]map[string]interface{})
				}
				allUserData[userID] = row
			}
		}
		rows.Close()
	}

	return allUserData
}

// insertSpeakersDataIntoClickHouse inserts speakers data into event_speaker_ch table
func insertSpeakersDataIntoClickHouse(clickhouseDB *sql.DB, speakerRecords []map[string]interface{}) error {
	if len(speakerRecords) == 0 {
		return nil
	}

	maxRetries := 3
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if err := executeSpeakersInsertBatchNative(clickhouseDB, speakerRecords); err != nil {
			if attempt < maxRetries && isConnectionError(err) {
				fmt.Printf("Speakers insertion attempt %d failed due to connection issue, retrying in 10 seconds: %v\n",
					attempt, err)
				time.Sleep(10 * time.Second)
				continue
			}
			return fmt.Errorf("speakers insertion failed after %d attempts: %v", attempt, err)
		}
		break
	}

	return nil
}

// executeSpeakersInsertBatchNative executes speakers insert
func executeSpeakersInsertBatchNative(clickhouseDB *sql.DB, records []map[string]interface{}) error {
	query := `
		INSERT INTO event_speaker_ch (
			user_id, event_id, edition_id, user_name, user_company,
			user_designation, user_city, user_country, version
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	tx, err := clickhouseDB.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin ClickHouse speakers transaction: %v", err)
	}

	txStmt, err := tx.Prepare(query)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to prepare ClickHouse speakers statement in transaction: %v", err)
	}
	defer txStmt.Close()

	// insert each record
	for _, record := range records {
		_, err := txStmt.Exec(
			record["user_id"],                                          // user_id: UInt32 NOT NULL
			record["event_id"],                                         // event_id: UInt32 NOT NULL
			record["edition_id"],                                       // edition_id: UInt32 NOT NULL
			convertToString(record["user_name"]),                       // user_name: String NOT NULL
			convertToString(record["user_company"]),                    // user_company: Nullable(String)
			convertToString(record["user_designation"]),                // user_designation: Nullable(String)
			record["user_city"],                                        // user_city: Nullable(UInt32)
			truncateString(convertToString(record["user_country"]), 2), // user_country: LowCardinality(FixedString(2))
			uint32(1), // version: UInt32 NOT NULL DEFAULT 1
		)

		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to insert speaker record %v: %v", record["user_id"], err)
		}
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit ClickHouse speakers transaction: %v", err)
	}

	return nil
}

// processEventEditionOnly processes only event_edition data without the main flow
func processEventEditionOnly(mysqlDB, clickhouseDB *sql.DB, esClient *elasticsearch.Client, config Config) {
	fmt.Println("=== Starting EVENT EDITION ONLY Processing ===")

	// Get total records and min/max ID's count from event table
	totalRecords, minID, maxID, err := getTotalRecordsAndIDRange(mysqlDB, "event")
	if err != nil {
		log.Fatal("Failed to get total records and ID range from event:", err)
	}

	fmt.Printf("Total event records: %d, Min ID: %d, Max ID: %d\n", totalRecords, minID, maxID)

	// Calculate chunk size based on user input
	if config.NumChunks <= 0 {
		config.NumChunks = 5 // Default to 5 chunks if not specified
	}

	chunkSize := (maxID - minID + 1) / config.NumChunks
	if chunkSize == 0 {
		chunkSize = 1
	}

	fmt.Printf("Processing event edition data in %d chunks with chunk size: %d\n", config.NumChunks, chunkSize)

	results := make(chan string, config.NumChunks)
	semaphore := make(chan struct{}, config.NumWorkers)

	for i := 0; i < config.NumChunks; i++ {
		startID := minID + (i * chunkSize)
		endID := startID + chunkSize - 1

		// last chunk to include remaining records
		if i == config.NumChunks-1 {
			endID = maxID
		}

		// delay between chunk launches to reduce ClickHouse load
		if i > 0 {
			delay := 3 * time.Second
			fmt.Printf("Waiting %v before launching event edition chunk %d...\n", delay, i+1)
			time.Sleep(delay)
		}

		semaphore <- struct{}{}
		go func(chunkNum, start, end int) {
			defer func() { <-semaphore }()
			processEventEditionChunk(mysqlDB, clickhouseDB, esClient, config, start, end, chunkNum, results)
		}(i+1, startID, endID)
	}

	for i := 0; i < config.NumChunks; i++ {
		result := <-results
		fmt.Printf("Event Edition Result: %s\n", result)
	}

	fmt.Println("Event Edition processing completed!")
}

// processEventEditionChunk processes a single chunk of event edition data
func processEventEditionChunk(mysqlDB, clickhouseDB *sql.DB, esClient *elasticsearch.Client, config Config, startID, endID int, chunkNum int, results chan<- string) {
	fmt.Printf("Processing event edition chunk %d: ID range %d-%d\n", chunkNum, startID, endID)

	offset := 0
	for {
		batchData, err := buildMigrationData(mysqlDB, "event", startID, endID, config.BatchSize)
		if err != nil {
			results <- fmt.Sprintf("Event edition chunk %d batch error: %v", chunkNum, err)
			return
		}

		if len(batchData) == 0 {
			break
		}

		fmt.Printf("Event edition chunk %d: Retrieved %d records in batch\n", chunkNum, len(batchData))

		// Get event IDs from this batch
		eventIDs := extractEventIDs(batchData)
		if len(eventIDs) > 0 {
			fmt.Printf("Event edition chunk %d: Fetching edition data for %d events\n", chunkNum, len(eventIDs))

			// Fetch edition data in parallel
			startTime := time.Now()
			editionData := fetchEditionDataParallel(mysqlDB, eventIDs, config.NumWorkers)
			editionTime := time.Since(startTime)
			fmt.Printf("Event edition chunk %d: Retrieved edition data for %d events in %v\n", chunkNum, len(editionData), editionTime)

			// Fetch company data for all editions
			var companyData []map[string]interface{}
			if len(editionData) > 0 {
				companyIDs := extractCompanyIDs(editionData)
				if len(companyIDs) > 0 {
					fmt.Printf("Event edition chunk %d: Fetching company data for %d companies\n", chunkNum, len(companyIDs))
					startTime = time.Now()
					companyData = fetchCompanyDataParallel(mysqlDB, companyIDs, config.NumWorkers)
					companyTime := time.Since(startTime)
					fmt.Printf("Event edition chunk %d: Retrieved company data for %d companies in %v\n", chunkNum, len(companyData), companyTime)
				}
			}

			// Fetch venue data for all editions
			var venueData []map[string]interface{}
			if len(editionData) > 0 {
				venueIDs := extractVenueIDs(editionData)
				if len(venueIDs) > 0 {
					fmt.Printf("Event edition chunk %d: Fetching venue data for %d venues\n", chunkNum, len(venueIDs))
					startTime = time.Now()
					venueData = fetchVenueDataParallel(mysqlDB, venueIDs, config.NumWorkers)
					venueTime := time.Since(startTime)
					fmt.Printf("Event edition chunk %d: Retrieved venue data for %d venues in %v\n", chunkNum, len(venueData), venueTime)
				}
			}

			// Fetch city data for all editions
			var cityData []map[string]interface{}
			if len(editionData) > 0 {
				cityIDs := extractCityIDs(editionData)
				if len(cityIDs) > 0 {
					fmt.Printf("Event edition chunk %d: Fetching city data for %d cities\n", chunkNum, len(cityIDs))
					startTime = time.Now()
					cityData = fetchCityDataParallel(mysqlDB, cityIDs, config.NumWorkers)
					cityTime := time.Since(startTime)
					fmt.Printf("Event edition chunk %d: Retrieved city data for %d cities in %v\n", chunkNum, len(cityData), cityTime)
				}
			}

			// Fetch Elasticsearch data for all editions
			var esData map[int64]map[string]interface{}
			if len(editionData) > 0 {
				fmt.Printf("Event edition chunk %d: Fetching Elasticsearch data for %d events in batches of 200\n", chunkNum, len(eventIDs))
				startTime = time.Now()
				esData = fetchElasticsearchDataForEvents(esClient, config.IndexName, eventIDs)
				esTime := time.Since(startTime)
				fmt.Printf("Event edition chunk %d: Retrieved Elasticsearch data for %d events in %v\n", chunkNum, len(esData), esTime)
			}

			// Combine and display data from all sources with proper prefixes
			if len(editionData) > 0 {
				// Create lookup maps
				companyLookup := make(map[int64]map[string]interface{})
				if len(companyData) > 0 {
					for _, company := range companyData {
						if companyID, ok := company["id"].(int64); ok {
							companyLookup[companyID] = company
						}
					}
				}

				venueLookup := make(map[int64]map[string]interface{})
				if len(venueData) > 0 {
					for _, venue := range venueData {
						if venueID, ok := venue["id"].(int64); ok {
							venueLookup[venueID] = venue
						}
					}
				}

				cityLookup := make(map[int64]map[string]interface{})
				if len(cityData) > 0 {
					for _, city := range cityData {
						if cityID, ok := city["id"].(int64); ok {
							cityLookup[cityID] = city
						}
					}
				}

				// Group editions by event_id
				eventEditions := make(map[int64][]map[string]interface{})
				for _, edition := range editionData {
					if eventID, ok := edition["event"].(int64); ok {
						eventEditions[eventID] = append(eventEditions[eventID], edition)
					}
				}

				// Collect ALL records for ClickHouse insertion
				var clickHouseRecords []map[string]interface{}
				completeCount := 0
				partialCount := 0
				skippedCount := 0

				for eventID, editions := range eventEditions {
					// Find the event data for this eventID
					var eventData map[string]interface{}
					for _, row := range batchData {
						if id, ok := row["id"].(int64); ok && id == eventID {
							eventData = row
							break
						}
					}

					if eventData != nil {
						for _, edition := range editions {
							// Extract all available data
							companyID := edition["company_id"]
							venueID := edition["venue_id"]
							cityID := edition["edition_city"]
							editionWebsite := edition["edition_website"]

							// Get company data if available
							var company map[string]interface{}
							if companyID != nil {
								if c, exists := companyLookup[companyID.(int64)]; exists {
									company = c // If not found->company remains nil
								}
							}

							// Get venue data if available
							var venue map[string]interface{}
							if venueID != nil {
								if v, exists := venueLookup[venueID.(int64)]; exists {
									venue = v // If not found->venue remains nil
								}

							}

							// Get city data if available
							var city map[string]interface{}
							if cityID != nil {
								if c, exists := cityLookup[cityID.(int64)]; exists {
									city = c // If not found->city remains nil
								}

							}

							// Get Elasticsearch data if available
							esInfoMap := esData[eventID] // If not found, esInfoMap remains nil

							// Extract domain from website if available
							var editionDomain string
							if editionWebsite != nil {
								editionDomain = extractDomainFromWebsite(editionWebsite) // If no website, editionDomain remains empty string
							}

							// Create record for ClickHouse insertion - include ALL data
							record := collectRecordsForClickHouse(eventData, edition, company, venue, city, esInfoMap, editionDomain)
							clickHouseRecords = append(clickHouseRecords, record)

							// Track completeness for reporting (simplified)
							if companyID != nil && venueID != nil && cityID != nil {
								completeCount++
							} else {
								partialCount++
							}
						}
					} else {
						skippedCount++
					}
				}

				fmt.Printf("Event edition chunk %d: Data completeness - Complete: %d, Partial: %d, Skipped: %d\n",
					chunkNum, completeCount, partialCount, skippedCount)

				// Insert collected records into ClickHouse
				if len(clickHouseRecords) > 0 {
					fmt.Printf("Event edition chunk %d: Attempting to insert %d records into ClickHouse...\n", chunkNum, len(clickHouseRecords))

					maxInsertRetries := 3
					var insertErr error

					for attempt := 1; attempt <= maxInsertRetries; attempt++ {
						if err := insertIntoClickHouse(clickhouseDB, clickHouseRecords, config); err != nil {
							insertErr = err
							if attempt < maxInsertRetries {
								fmt.Printf("Event edition chunk %d: ClickHouse insertion attempt %d failed, retrying in 10 seconds: %v\n",
									chunkNum, attempt, err)
								time.Sleep(10 * time.Second)
								continue
							}
						} else {
							fmt.Printf("Event edition chunk %d: Successfully inserted %d records into ClickHouse\n", chunkNum, len(clickHouseRecords))
							insertErr = nil
							break
						}
					}

					if insertErr != nil {
						fmt.Printf("Event edition chunk %d: ClickHouse insertion failed after %d attempts: %v\n",
							chunkNum, maxInsertRetries, insertErr)

						// TODO: implement a failed records queue here
						fmt.Printf("Event edition chunk %d: %d records failed to insert - consider manual retry\n",
							chunkNum, len(clickHouseRecords))
					}
				} else {
					fmt.Printf("Event edition chunk %d: No records to insert into ClickHouse\n", chunkNum)
				}
			}
		}

		// Get the last ID from this batch for next iteration
		if len(batchData) > 0 {
			lastID := batchData[len(batchData)-1]["id"]
			if lastID != nil {
				// Update startID for next batch within this chunk
				if id, ok := lastID.(int64); ok {
					startID = int(id) + 1
				}
			}
		}

		offset += len(batchData)
		if len(batchData) < config.BatchSize {
			break
		}
	}

	fmt.Printf("Event edition chunk %d: Final Summary - Processed %d total records\n", chunkNum, offset)
	results <- fmt.Sprintf("Event edition chunk %d completed successfully", chunkNum)
}
