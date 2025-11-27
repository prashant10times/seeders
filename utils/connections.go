package utils

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"seeders/shared"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/elastic/go-elasticsearch/v6"
	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
)

func LoadEnv() (shared.Config, error) {
	var config shared.Config

	if err := godotenv.Load(); err != nil {
		return config, fmt.Errorf("failed to load .env file: %w", err)
	}

	if err := envconfig.Process("", &config); err != nil {
		return config, fmt.Errorf("failed to process environment variables: %w", err)
	}

	// Debug: Log loaded configuration values
	log.Printf("Loaded configuration:")
	log.Printf("  DB_HOST: %s", config.DatabaseHost)
	log.Printf("  DB_PORT: %d", config.DatabasePort)
	log.Printf("  DB_NAME: %s", config.DatabaseName)
	log.Printf("  DB_USER: %s", config.DatabaseUser)
	log.Printf("  CLICKHOUSE_HOST: %s", config.ClickhouseHost)
	log.Printf("  CLICKHOUSE_PORT: %s", config.ClickhousePort)
	log.Printf("  ELASTICSEARCH_HOST: %s", config.ElasticsearchHost)
	log.Printf("  ELASTICSEARCH_PORT: %s", config.ElasticsearchPort)
	log.Printf("  ELASTICSEARCH_INDEX: %s", config.ElasticsearchIndex)

	// Set default values for configurable settings
	if config.MySQLMaxOpenConns == 0 {
		config.MySQLMaxOpenConns = 25
	}
	if config.MySQLMaxIdleConns == 0 {
		config.MySQLMaxIdleConns = 15
	}
	if config.ClickHouseMaxOpenConns == 0 {
		config.ClickHouseMaxOpenConns = 10
	}
	if config.ClickHouseMaxIdleConns == 0 {
		config.ClickHouseMaxIdleConns = 5
	}
	if config.RetryDelay == 0 {
		config.RetryDelay = 10 * time.Second
	}
	if config.QueryTimeout == 0 {
		config.QueryTimeout = 120 * time.Second
	}

	return config, nil
}

func ValidateConfig(config shared.Config) error {
	if config.BatchSize > config.NumChunks*1000 {
		log.Printf("Warning: Batch size (%d) is large relative to chunk count (%d)", config.BatchSize, config.NumChunks)
	}
	if config.NumWorkers > 50 {
		return fmt.Errorf("too many workers (%d), maximum allowed is 50", config.NumWorkers)
	}
	if config.ClickHouseWorkers > 20 {
		return fmt.Errorf("too many ClickHouse workers (%d), maximum allowed is 20", config.ClickHouseWorkers)
	}
	if config.BatchSize <= 0 {
		return fmt.Errorf("batch size must be positive, got %d", config.BatchSize)
	}
	if config.NumChunks <= 0 {
		return fmt.Errorf("number of chunks must be positive, got %d", config.NumChunks)
	}
	return nil
}

func SetupConnections(config shared.Config) (*sql.DB, driver.Conn, *elasticsearch.Client, error) {
	mysqlDB, err := sql.Open("mysql", config.MySQLDSN)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("MySQL connection failed: %v", err)
	}

	// Set MySQL connection pool settings
	// if config.MySQLMaxOpenConns > 0 {
	// 	mysqlDB.SetMaxOpenConns(config.MySQLMaxOpenConns)
	// } else {
	// 	mysqlDB.SetMaxOpenConns(25)
	// }
	// if config.MySQLMaxIdleConns > 0 {
	// 	mysqlDB.SetMaxIdleConns(config.MySQLMaxIdleConns)
	// } else {
	// 	mysqlDB.SetMaxIdleConns(15)
	// }
	// mysqlDB.SetConnMaxLifetime(30 * time.Minute)
	// mysqlDB.SetConnMaxIdleTime(10 * time.Minute)

	if _, err := mysqlDB.Query("SELECT 1"); err != nil {
		mysqlDB.Close()
		return nil, nil, nil, fmt.Errorf("MySQL connection test failed: %v", err)
	}

	clickhouseDB, err := SetupNativeClickHouseConnection(config)
	if err != nil {
		mysqlDB.Close()
		return nil, nil, nil, fmt.Errorf("ClickHouse connection failed: %v", err)
	}

	esConfig := elasticsearch.Config{
		Addresses:         []string{config.ElasticHost},
		DisableRetry:      true,
		EnableMetrics:     false,
		EnableDebugLogger: false,
		Transport: &http.Transport{
			ResponseHeaderTimeout: 30 * time.Second,
			DialContext: (&net.Dialer{
				Timeout:   15 * time.Second,
				KeepAlive: 60 * time.Second,
			}).DialContext,
			MaxIdleConns:          200,
			MaxIdleConnsPerHost:   50,
			IdleConnTimeout:       120 * time.Second,
			TLSHandshakeTimeout:   15 * time.Second,
			ExpectContinueTimeout: 5 * time.Second,
			DisableCompression:    false,
			DisableKeepAlives:     false,
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

func SetupNativeClickHouseConnection(config shared.Config) (driver.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{config.ClickhouseHost + ":" + config.ClickhousePort},
		Auth: clickhouse.Auth{
			Database: config.ClickhouseDB,
			Username: config.ClickhouseUser,
			Password: config.ClickhousePassword,
		},
		Protocol: clickhouse.HTTP,
		Settings: clickhouse.Settings{
			"max_execution_time": 900, // 15 minutes
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		MaxOpenConns:     50,
		MaxIdleConns:     25,
		DialTimeout:      900 * time.Second, // 15 minutes
		ConnOpenStrategy: clickhouse.ConnOpenInOrder,
		Debug:            false,
	})

	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("ClickHouse connection failed: %v", err)
	}

	return conn, nil
}

func TestElasticsearchConnection(esClient *elasticsearch.Client, indexName string) error {
	res, err := esClient.Info()
	if err != nil {
		return fmt.Errorf("failed to get cluster info: %v", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("elasticsearch info request failed: %v", res.Status())
	}

	log.Println("OK: Elasticsearch connection successful")
	log.Printf("OK: Cluster info: %s", res.Status())

	indexRes, err := esClient.Indices.Exists([]string{indexName})
	if err != nil {
		return fmt.Errorf("failed to check index existence: %v", err)
	}
	defer indexRes.Body.Close()

	switch indexRes.StatusCode {
	case 200:
		log.Printf("OK: Index '%s' exists", indexName)
	case 404:
		log.Printf("WARNING: Index '%s' does not exist", indexName)
	default:
		return fmt.Errorf("unexpected status checking index: %d", indexRes.StatusCode)
	}

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

	var totalCount interface{}
	switch t := total.(type) {
	case float64:
		totalCount = t
	case map[string]interface{}:
		totalCount = t["value"]
	default:
		totalCount = fmt.Sprintf("unknown format: %T", total)
	}

	log.Printf("OK: Search query successful, total documents: %v", totalCount)

	return nil
}

func TestClickHouseConnection(clickhouseConn driver.Conn) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// if err := clickhouseConn.Ping(ctx); err != nil {
	// 	return fmt.Errorf("ClickHouse ping failed: %v", err)
	// }

	var result uint8
	query := "SELECT 1"
	row := clickhouseConn.QueryRow(ctx, query)
	if err := row.Scan(&result); err != nil {
		return fmt.Errorf("ClickHouse query test failed: %v", err)
	}

	if result != 1 {
		return fmt.Errorf("ClickHouse query returned unexpected result: %d", result)
	}

	tableQuery := "SELECT count() FROM allevent_ch LIMIT 1"
	if err := clickhouseConn.Exec(ctx, tableQuery); err != nil {
		return fmt.Errorf("ClickHouse table access test failed: %v", err)
	}

	log.Println("OK: ClickHouse connection successful")
	log.Printf("OK: ClickHouse table 'allevent_ch' is accessible")

	return nil
}
