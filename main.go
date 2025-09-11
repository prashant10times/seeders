package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/elastic/go-elasticsearch/v6"
	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
)

// retryWithBackoff
func retryWithBackoff(operation func() error, maxRetries int, operationName string) error {
	var lastError error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		if err := operation(); err == nil {
			if attempt > 1 {
				log.Printf("Operation %s succeeded on attempt %d", operationName, attempt)
			}
			return nil
		} else {
			lastError = err
			log.Printf("Operation %s failed on attempt %d with error: %v", operationName, attempt, err)
			if strings.Contains(operationName, "insertion") || strings.Contains(operationName, "ClickHouse") {
				log.Printf("Detailed error for %s: %+v", operationName, err)
			}
		}

		if attempt == maxRetries {
			return fmt.Errorf("operation %s failed after %d attempts. Last error: %v", operationName, maxRetries, lastError)
		}

		baseDelay := time.Duration(attempt*attempt) * time.Second
		jitter := time.Duration(rand.Intn(1000)) * time.Millisecond
		delay := baseDelay + jitter

		log.Printf("Operation %s failed on attempt %d, retrying in %v", operationName, attempt, delay)
		time.Sleep(delay)
	}
	return nil
}

// safely converts a value to string for non-nullable fields
func safeConvertToString(value interface{}) string {
	if value == nil {
		return ""
	}
	if str, ok := value.(string); ok {
		return str
	}
	if bytes, ok := value.([]uint8); ok {
		return string(bytes)
	}
	if bytes, ok := value.([]byte); ok {
		return string(bytes)
	}
	return ""
}

// safeConvertToStatusString converts a value to status string with default 'A' for empty values
func safeConvertToStatusString(value interface{}) string {
	status := safeConvertToString(value)
	if status == "" {
		return "A" // Default status value as per ClickHouse schema
	}
	return status
}

// safeConvertToNullableUInt8 converts a value to nullable UInt8, handling both numeric and string types
func safeConvertToNullableUInt8(value interface{}) *uint8 {
	if value == nil {
		return nil
	}

	// Handle direct numeric types first
	if num, ok := value.(uint8); ok {
		return &num
	}
	if num, ok := value.(uint32); ok {
		if num <= 255 {
			result := uint8(num)
			return &result
		}
		return nil
	}
	if num, ok := value.(int64); ok {
		if num >= 0 && num <= 255 {
			result := uint8(num)
			return &result
		}
		return nil
	}
	if num, ok := value.(int); ok {
		if num >= 0 && num <= 255 {
			result := uint8(num)
			return &result
		}
		return nil
	}

	// Handle string conversion
	str := safeConvertToString(value)
	if str == "" {
		return nil
	}
	if num, err := strconv.ParseUint(str, 10, 8); err == nil {
		result := uint8(num)
		return &result
	}
	return nil
}

// safely converts a value to nullable string for nullable fields
func safeConvertToNullableString(value interface{}) *string {
	if value == nil {
		return nil
	}
	if str, ok := value.(string); ok {
		return &str
	}
	if strPtr, ok := value.(*string); ok {
		return strPtr
	}
	if bytes, ok := value.([]uint8); ok {
		str := string(bytes)
		return &str
	}
	if bytes, ok := value.([]byte); ok {
		str := string(bytes)
		return &str
	}
	return nil
}

// converts a nullable string to uppercase
func toUpperNullableString(s *string) *string {
	if s == nil {
		return nil
	}
	upper := strings.ToUpper(*s)
	return &upper
}

func safeConvertToDateString(value interface{}) string {
	str := safeConvertToString(value)
	if str == "" {
		return "1970-01-01"
	}
	return str
}

func safeConvertToDateTimeString(value interface{}) string {
	str := safeConvertToString(value)
	if str == "" {
		return "1970-01-01 00:00:00"
	}
	return str
}

func safeConvertToUInt32(value interface{}) uint32 {
	if value == nil {
		return 0
	}
	if num, ok := value.(uint32); ok {
		return num
	}
	if num, ok := value.(int64); ok {
		return uint32(num)
	}
	if num, ok := value.(int); ok {
		return uint32(num)
	}
	if num, ok := value.(uint64); ok {
		return uint32(num)
	}
	return 0
}

func safeConvertToUInt16(value interface{}) uint16 {
	if value == nil {
		return 0
	}
	if num, ok := value.(uint16); ok {
		return num
	}
	if num, ok := value.(int64); ok {
		return uint16(num)
	}
	if num, ok := value.(int); ok {
		return uint16(num)
	}
	if num, ok := value.(uint32); ok {
		return uint16(num)
	}
	if num, ok := value.(uint64); ok {
		return uint16(num)
	}

	if str, ok := value.(string); ok {
		if i, err := strconv.ParseUint(str, 10, 16); err == nil {
			return uint16(i)
		}
		return 0
	}
	if bytes, ok := value.([]byte); ok {
		str := string(bytes)
		if i, err := strconv.ParseUint(str, 10, 16); err == nil {
			return uint16(i)
		}
		return 0
	}
	return 0
}

func safeConvertToInt8(value interface{}) int8 {
	if value == nil {
		return 0
	}
	if num, ok := value.(int8); ok {
		return num
	}
	if num, ok := value.(int64); ok {
		return int8(num)
	}
	if num, ok := value.(int); ok {
		return int8(num)
	}
	if num, ok := value.(uint32); ok {
		return int8(num)
	}
	if num, ok := value.(uint64); ok {
		return int8(num)
	}
	return 0
}

func safeConvertToNullableUInt32(value interface{}) *uint32 {
	if value == nil {
		return nil
	}
	if num, ok := value.(uint32); ok {
		return &num
	}
	if num, ok := value.(int64); ok {
		u32 := uint32(num)
		return &u32
	}
	if num, ok := value.(int); ok {
		u32 := uint32(num)
		return &u32
	}
	if num, ok := value.(uint64); ok {
		u32 := uint32(num)
		return &u32
	}
	return nil
}

func safeConvertToFloat64(value interface{}) float64 {
	if value == nil {
		return 0.0
	}
	if num, ok := value.(float64); ok {
		return num
	}
	if num, ok := value.(float32); ok {
		return float64(num)
	}
	if num, ok := value.(int64); ok {
		return float64(num)
	}
	if num, ok := value.(int); ok {
		return float64(num)
	}
	if num, ok := value.(uint32); ok {
		return float64(num)
	}
	if num, ok := value.(uint64); ok {
		return float64(num)
	}
	return 0.0
}

func safeConvertToNullableFloat64(value interface{}) *float64 {
	if value == nil {
		return nil
	}
	if num, ok := value.(float64); ok {
		return &num
	}
	if num, ok := value.(float32); ok {
		f64 := float64(num)
		return &f64
	}
	if num, ok := value.(int64); ok {
		f64 := float64(num)
		return &f64
	}
	if num, ok := value.(int); ok {
		f64 := float64(num)
		return &f64
	}
	if num, ok := value.(uint32); ok {
		f64 := float64(num)
		return &f64
	}
	if num, ok := value.(uint64); ok {
		f64 := float64(num)
		return &f64
	}
	return nil
}

// converts a float64 value to a decimal string for ClickHouse Decimal(3,2) field
func safeConvertFloat64ToDecimalString(value interface{}) *string {
	if value == nil {
		return nil
	}

	var floatVal float64
	switch v := value.(type) {
	case float64:
		floatVal = v
	case float32:
		floatVal = float64(v)
	case int64:
		floatVal = float64(v)
	case int:
		floatVal = float64(v)
	case uint32:
		floatVal = float64(v)
	case uint64:
		floatVal = float64(v)
	default:
		return nil
	}

	// Format to 2 decimal places for Decimal(3,2)
	result := fmt.Sprintf("%.2f", floatVal)
	return &result
}

// converts a value to uint32
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

// converts a value to uint8
func convertToUInt8(value interface{}) uint8 {
	if value == nil {
		return 0
	}

	switch v := value.(type) {
	case uint8:
		return v
	case uint16:
		if v > 255 {
			return 0
		}
		return uint8(v)
	case uint32:
		if v > 255 {
			return 0
		}
		return uint8(v)
	case uint64:
		if v > 255 {
			return 0
		}
		return uint8(v)
	case int8:
		if v < 0 {
			return 0
		}
		return uint8(v)
	case int16:
		if v < 0 || v > 255 {
			return 0
		}
		return uint8(v)
	case int32:
		if v < 0 || v > 255 {
			return 0
		}
		return uint8(v)
	case int64:
		if v < 0 || v > 255 {
			return 0
		}
		return uint8(v)
	case int:
		if v < 0 || v > 255 {
			return 0
		}
		return uint8(v)
	case float64:
		if v < 0 || v > 255 {
			return 0
		}
		return uint8(v)
	case string:
		if i, err := strconv.ParseUint(v, 10, 8); err == nil {
			return uint8(i)
		}
		return 0
	default:
		return 0
	}
}

func extractDomainFromWebsite(website interface{}) string {
	if website == nil {
		return ""
	}

	websiteStrPtr := convertToStringPtr(website)
	if websiteStrPtr == nil {
		return ""
	}
	websiteStr := *websiteStrPtr
	if websiteStr == "" {
		return ""
	}

	websiteStr = strings.TrimSpace(websiteStr)

	if !strings.Contains(websiteStr, "://") {
		websiteStr = "https://" + websiteStr
	}
	parsedURL, err := url.Parse(websiteStr)
	if err != nil {
		return ""
	}
	host := parsedURL.Hostname()
	if host == "" {
		return ""
	}

	host = strings.TrimPrefix(strings.ToLower(host), "www.")

	if strings.Contains(host, "@") {
		parts := strings.Split(host, "@")
		if len(parts) > 1 {
			host = parts[len(parts)-1]
		}
	}

	domainRegex := regexp.MustCompile(`(?P<domain>[a-z0-9][a-z0-9\-_]*(\.[a-z0-9][a-z0-9\-_]*)*\.[a-z]{2,})$`)
	matches := domainRegex.FindStringSubmatch(host)

	if len(matches) > 1 {
		return matches[1]
	}

	return host
}

// getCompanyNameOrDefault
func getCompanyNameOrDefault(companyName interface{}) string {
	if companyName == nil {
		return "N/A"
	}

	if name, ok := companyName.(string); ok {
		if strings.TrimSpace(name) == "" {
			return "N/A"
		}
		return name
	}

	if name, ok := companyName.([]byte); ok {
		nameStr := string(name)
		if strings.TrimSpace(nameStr) == "" {
			return "N/A"
		}
		return nameStr
	}

	return "N/A" // default
}

// converts a value to *uint32 for nullable fields
func convertToUInt32Ptr(value interface{}) *uint32 {
	if value == nil {
		return nil
	}

	u32 := convertToUInt32(value)
	return &u32
}

// converts a value to *string for nullable fields
func convertToStringPtr(value interface{}) *string {
	if value == nil {
		return nil
	}

	switch v := value.(type) {
	case []byte:
		str := string(v)
		return &str
	case string:
		return &v
	case float64:
		str := fmt.Sprintf("%.6f", v)
		return &str
	case float32:
		str := fmt.Sprintf("%.6f", v)
		return &str
	default:
		str := fmt.Sprintf("%v", v)
		return &str
	}
}

func convertToString(value interface{}) string {
	ptr := convertToStringPtr(value)
	if ptr == nil {
		return ""
	}
	return *ptr
}

func convertToInt8(value interface{}) int8 {
	if value == nil {
		return 0
	}

	switch v := value.(type) {
	case int8:
		return v
	case int16:
		if v < -128 || v > 127 {
			return 0
		}
		return int8(v)
	case int32:
		if v < -128 || v > 127 {
			return 0
		}
		return int8(v)
	case int64:
		if v < -128 || v > 127 {
			return 0
		}
		return int8(v)
	case int:
		if v < -128 || v > 127 {
			return 0
		}
		return int8(v)
	case uint8:
		if v > 127 {
			return 0
		}
		return int8(v)
	case uint16:
		if v > 127 {
			return 0
		}
		return int8(v)
	case uint32:
		if v > 127 {
			return 0
		}
		return int8(v)
	case uint64:
		if v > 127 {
			return 0
		}
		return int8(v)
	case float64:
		if v < -128 || v > 127 {
			return 0
		}
		return int8(v)
	case string:
		if i, err := strconv.ParseInt(v, 10, 8); err == nil {
			return int8(i)
		}
		return 0
	default:
		return 0
	}
}

// converts a map to EventEditionRecord struct
func convertToEventEditionRecord(record map[string]interface{}) EventEditionRecord {
	return EventEditionRecord{
		EventID:          safeConvertToUInt32(record["event_id"]),
		EventName:        safeConvertToString(record["event_name"]),
		EventAbbrName:    safeConvertToNullableString(record["event_abbr_name"]),
		EventDescription: safeConvertToNullableString(record["event_description"]),
		EventPunchline:   safeConvertToNullableString(record["event_punchline"]),
		StartDate:        safeConvertToDateString(record["start_date"]),
		EndDate:          safeConvertToDateString(record["end_date"]),
		EditionID:        safeConvertToUInt32(record["edition_id"]),
		EditionCountry:   strings.ToUpper(safeConvertToString(record["edition_country"])),
		EditionCity:      safeConvertToUInt32(record["edition_city"]),
		EditionCityName:  safeConvertToString(record["edition_city_name"]),
		EditionCityLat:   safeConvertToFloat64(record["edition_city_lat"]),
		EditionCityLong:  safeConvertToFloat64(record["edition_city_long"]),
		CompanyID:        safeConvertToNullableUInt32(record["company_id"]),
		CompanyName:      safeConvertToNullableString(record["company_name"]),
		CompanyDomain:    safeConvertToNullableString(record["company_domain"]),
		CompanyWebsite:   safeConvertToNullableString(record["company_website"]),
		CompanyCountry:   toUpperNullableString(safeConvertToNullableString(record["company_country"])),
		CompanyCity:      safeConvertToNullableUInt32(record["company_city"]),
		CompanyCityName: func() *string {
			if val, ok := record["company_city_name"].(*string); ok {
				return val
			}
			return nil
		}(),
		VenueID:      safeConvertToNullableUInt32(record["venue_id"]),
		VenueName:    safeConvertToNullableString(record["venue_name"]),
		VenueCountry: toUpperNullableString(safeConvertToNullableString(record["venue_country"])),
		VenueCity:    safeConvertToNullableUInt32(record["venue_city"]),
		VenueCityName: func() *string {
			if val, ok := record["venue_city_name"].(*string); ok {
				return val
			}
			return nil
		}(),
		VenueLat:             safeConvertToNullableFloat64(record["venue_lat"]),
		VenueLong:            safeConvertToNullableFloat64(record["venue_long"]),
		Published:            safeConvertToInt8(record["published"]),
		Status:               safeConvertToStatusString(record["status"]),
		EditionsAudianceType: safeConvertToUInt16(record["editions_audiance_type"]),
		EditionFunctionality: safeConvertToString(record["edition_functionality"]),
		EditionWebsite:       safeConvertToNullableString(record["edition_website"]),
		EditionDomain:        safeConvertToNullableString(record["edition_domain"]),
		EditionType:          safeConvertEditionType(record["edition_type"]),
		EventFollowers:       safeConvertToNullableUInt32(record["event_followers"]),
		EditionFollowers:     safeConvertToNullableUInt32(record["edition_followers"]),
		EventExhibitor:       safeConvertToNullableUInt32(record["event_exhibitor"]),
		EditionExhibitor:     safeConvertToNullableUInt32(record["edition_exhibitor"]),
		EventSponsor:         safeConvertToNullableUInt32(record["event_sponsor"]),
		EditionSponsor:       safeConvertToNullableUInt32(record["edition_sponsor"]),
		EventSpeaker:         safeConvertToNullableUInt32(record["event_speaker"]),
		EditionSpeaker:       safeConvertToNullableUInt32(record["edition_speaker"]),
		EventCreated:         safeConvertToDateTimeString(record["event_created"]),
		EditionCreated:       safeConvertToDateTimeString(record["edition_created"]),
		EventHybrid:          safeConvertToNullableUInt8(record["event_hybrid"]),
		IsBranded: func() *uint32 {
			if val, ok := record["isBranded"].(*uint32); ok {
				return val
			}
			return nil
		}(),
		Maturity:               safeConvertToNullableString(record["maturity"]),
		EventPricing:           safeConvertToNullableString(record["event_pricing"]),
		EventLogo:              safeConvertToNullableString(record["event_logo"]),
		EventEstimatedVisitors: safeConvertToNullableString(record["event_estimatedVisitors"]),
		EventFrequency:         safeConvertToNullableString(record["event_frequency"]),
		EventAvgRating:         safeConvertFloat64ToDecimalString(record["event_avgRating"]),
		Version:                safeConvertToUInt32(record["version"]),
	}
}

type Config struct {
	DatabaseHost       string `envconfig:"DB_HOST" required:"true"`
	DatabasePort       int    `envconfig:"DB_PORT" required:"true"`
	DatabaseName       string `envconfig:"DB_NAME" required:"true"`
	DatabaseUser       string `envconfig:"DB_USER" required:"true"`
	DatabasePassword   string `envconfig:"DB_PASSWORD" required:"true"`
	ClickhouseUser     string `envconfig:"CLICKHOUSE_USER" required:"true"`
	ClickhousePassword string `envconfig:"CLICKHOUSE_PASSWORD" required:"true"`
	ClickhouseHost     string `envconfig:"CLICKHOUSE_HOST" required:"true"`
	ClickhousePort     string `envconfig:"CLICKHOUSE_PORT" required:"true"`
	ClickhouseDB       string `envconfig:"CLICKHOUSE_DB" required:"true"`
	ElasticsearchHost  string `envconfig:"ELASTICSEARCH_HOST" required:"true"`
	ElasticsearchPort  string `envconfig:"ELASTICSEARCH_PORT" required:"true"`
	ElasticsearchIndex string `envconfig:"ELASTICSEARCH_INDEX" required:"true"`

	MySQLDSN               string
	ClickhouseDSN          string
	BatchSize              int
	NumChunks              int
	NumWorkers             int
	ClickHouseWorkers      int
	ElasticHost            string
	IndexName              string
	MySQLMaxOpenConns      int
	MySQLMaxIdleConns      int
	ClickHouseMaxOpenConns int
	ClickHouseMaxIdleConns int
	RetryDelay             time.Duration
	QueryTimeout           time.Duration
}

var config Config

func loadEnv() error {

	if err := godotenv.Load(); err != nil {
		return fmt.Errorf("failed to load .env file: %w", err)
	}

	if err := envconfig.Process("", &config); err != nil {
		return fmt.Errorf("failed to process environment variables: %w", err)
	}

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

	return nil
}

func validateConfig(config Config) error {
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

func setupConnections(config Config) (*sql.DB, driver.Conn, *elasticsearch.Client, error) {
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

	clickhouseDB, err := setupNativeClickHouseConnection(config)
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

func setupNativeClickHouseConnection(config Config) (driver.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{config.ClickhouseHost + ":" + config.ClickhousePort},
		Auth: clickhouse.Auth{
			Database: config.ClickhouseDB,
			Username: config.ClickhouseUser,
			Password: config.ClickhousePassword,
		},
		Protocol: clickhouse.HTTP,
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Debug: false,
	})

	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("ClickHouse connection failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("ClickHouse ping failed: %v", err)
	}

	return conn, nil
}

func testElasticsearchConnection(esClient *elasticsearch.Client, indexName string) error {
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

func testClickHouseConnection(clickhouseConn driver.Conn) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := clickhouseConn.Ping(ctx); err != nil {
		return fmt.Errorf("ClickHouse ping failed: %v", err)
	}

	var result uint8
	query := "SELECT 1"
	row := clickhouseConn.QueryRow(ctx, query)
	if err := row.Scan(&result); err != nil {
		return fmt.Errorf("ClickHouse query test failed: %v", err)
	}

	if result != 1 {
		return fmt.Errorf("ClickHouse query returned unexpected result: %d", result)
	}

	tableQuery := "SELECT count() FROM event_edition_ch LIMIT 1"
	if err := clickhouseConn.Exec(ctx, tableQuery); err != nil {
		return fmt.Errorf("ClickHouse table access test failed: %v", err)
	}

	log.Println("OK: ClickHouse connection successful")
	log.Printf("OK: ClickHouse table 'event_edition_ch' is accessible")

	return nil
}

func getTotalRecordsAndIDRange(db *sql.DB, table string) (int, int, int, error) {
	query := fmt.Sprintf("SELECT COUNT(*), MIN(id), MAX(id) FROM %s", table)
	fmt.Printf("Executing query: %s\n", query)

	start := time.Now()
	var count, minId, maxId int
	err := db.QueryRow(query).Scan(&count, &minId, &maxId)
	duration := time.Since(start)

	if err != nil {
		fmt.Printf("SQL error in getTotalRecordsAndIDRange for table %s: %v\n", table, err)
		fmt.Printf("Query execution time: %v\n", duration)
		return 0, 0, 0, fmt.Errorf("failed to get records for table %s: %v", table, err)
	}

	fmt.Printf("Query completed successfully in %v\n", duration)
	return count, minId, maxId, nil
}

func buildMigrationData(db *sql.DB, table string, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT id, name as event_name, abbr_name, punchline, start_date, end_date, country, published, status, event_audience, functionality, brand_id, created FROM %s WHERE id >= %d AND id <= %d ORDER BY id LIMIT %d", table, startID, endID, batchSize)
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

	batchSize := 1000

	expectedBatches := (len(venueIDs) + batchSize - 1) / batchSize
	results := make(chan []map[string]interface{}, expectedBatches)
	semaphore := make(chan struct{}, numWorkers)

	var allVenueData []map[string]interface{}

	var wg sync.WaitGroup

	for i := 0; i < len(venueIDs); i += batchSize {
		end := i + batchSize
		if end > len(venueIDs) {
			end = len(venueIDs)
		}

		batch := venueIDs[i:end]

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

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	completedBatches := 0

collectLoop:
	for completedBatches < expectedBatches {
		select {
		case venueData := <-results:
			allVenueData = append(allVenueData, venueData...)
			completedBatches++
		case <-done:

			break collectLoop
		case <-time.After(120 * time.Second):
			log.Printf("Warning: Timeout waiting for venue data. Completed %d/%d batches",
				completedBatches, expectedBatches)
			break collectLoop
		}
	}

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
		log.Printf("Missing venue IDs (%d): %v", len(missingVenueIDs), missingVenueIDs)
	}

	return allVenueData
}

func fetchVenueDataForBatch(db *sql.DB, venueIDs []int64) []map[string]interface{} {
	if len(venueIDs) == 0 {
		return nil
	}

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
		log.Printf("Error fetching venue data: %v", err)
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
									log.Printf("Warning: Could not parse %s value '%s' to float64: %v", col, str, err)
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
								log.Printf("Warning: Could not parse %s string '%s' to float64: %v", col, str, err)
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

	batchSize := 1000

	expectedBatches := (len(companyIDs) + batchSize - 1) / batchSize
	results := make(chan []map[string]interface{}, expectedBatches)
	semaphore := make(chan struct{}, numWorkers)

	var allCompanyData []map[string]interface{}

	var wg sync.WaitGroup

	for i := 0; i < len(companyIDs); i += batchSize {
		end := i + batchSize
		if end > len(companyIDs) {
			end = len(companyIDs)
		}

		batch := companyIDs[i:end]

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

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	completedBatches := 0

collectLoop:
	for completedBatches < expectedBatches {
		select {
		case companyData := <-results:
			allCompanyData = append(allCompanyData, companyData...)
			completedBatches++
		case <-done:

			break collectLoop
		case <-time.After(120 * time.Second):
			log.Printf("Warning: Timeout waiting for company data. Completed %d/%d batches",
				completedBatches, expectedBatches)
			break collectLoop
		}
	}

	retrievedCompanyIDs := make(map[int64]bool)
	for _, company := range allCompanyData {
		if companyID, ok := company["id"].(int64); ok {
			retrievedCompanyIDs[companyID] = true
		}
	}

	return allCompanyData
}

func fetchCompanyDataForBatch(db *sql.DB, companyIDs []int64) []map[string]interface{} {
	if len(companyIDs) == 0 {
		return nil
	}

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
		log.Printf("Error fetching company data: %v", err)
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

	batchSize := 1000

	expectedBatches := (len(eventIDs) + batchSize - 1) / batchSize
	results := make(chan []map[string]interface{}, expectedBatches)
	semaphore := make(chan struct{}, numWorkers)

	var allEditionData []map[string]interface{}

	var wg sync.WaitGroup

	for i := 0; i < len(eventIDs); i += batchSize {
		end := i + batchSize
		if end > len(eventIDs) {
			end = len(eventIDs)
		}

		batch := eventIDs[i:end]

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

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	completedBatches := 0

	for completedBatches < expectedBatches {
		select {
		case editionData := <-results:
			allEditionData = append(allEditionData, editionData...)
			completedBatches++
		case <-done:

			return allEditionData
		case <-time.After(120 * time.Second):
			log.Printf("Warning: Timeout waiting for edition data. Completed %d/%d batches",
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

	placeholders := make([]string, len(eventIDs))
	args := make([]interface{}, len(eventIDs))
	for i, id := range eventIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	// First, fetch the current edition ID for each event from the event table
	currentEditionQuery := fmt.Sprintf(`
		SELECT 
			id as event_id, 
			event_edition as current_edition_id
		FROM event 
		WHERE id IN (%s)`, strings.Join(placeholders, ","))

	currentEditionRows, err := db.Query(currentEditionQuery, args...)
	if err != nil {
		log.Printf("Error fetching current edition data: %v", err)
		return nil
	}
	defer currentEditionRows.Close()

	// Create a map of event_id to current_edition_id
	currentEditionMap := make(map[int64]int64)
	for currentEditionRows.Next() {
		var eventID int64
		var currentEditionID sql.NullInt64
		if err := currentEditionRows.Scan(&eventID, &currentEditionID); err != nil {
			continue
		}
		if currentEditionID.Valid {
			currentEditionMap[eventID] = currentEditionID.Int64
		}
	}

	// Now fetch all edition data with start_date
	editionQuery := fmt.Sprintf(`
		SELECT 
			event, id as edition_id, city as edition_city, 
			company_id, venue as venue_id, website as edition_website, 
			created as edition_created, start_date as edition_start_date
		FROM event_edition 
		WHERE event IN (%s)`, strings.Join(placeholders, ","))

	editionRows, err := db.Query(editionQuery, args...)
	if err != nil {
		log.Printf("Error fetching edition data: %v", err)
		return nil
	}
	defer editionRows.Close()

	columns, err := editionRows.Columns()
	if err != nil {
		return nil
	}

	var results []map[string]interface{}
	for editionRows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := editionRows.Scan(valuePtrs...); err != nil {
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

		// Add current_edition_id to the row
		if eventID, ok := row["event"].(int64); ok {
			if currentEditionID, exists := currentEditionMap[eventID]; exists {
				row["current_edition_id"] = currentEditionID
			}
		}

		results = append(results, row)
	}

	return results
}

func processEventEditionOnly(mysqlDB *sql.DB, clickhouseConn driver.Conn, esClient *elasticsearch.Client, config Config) {
	log.Println("=== Starting EVENT EDITION ONLY Processing ===")

	// Get total records and min/max ID's count from event table
	totalRecords, minID, maxID, err := getTotalRecordsAndIDRange(mysqlDB, "event")
	if err != nil {
		log.Fatal("Failed to get total records and ID range from event:", err)
	}

	log.Printf("Total event records: %d, Min ID: %d, Max ID: %d", totalRecords, minID, maxID)

	// Calculate chunk size based on user input
	if config.NumChunks <= 0 {
		config.NumChunks = 5 // Default to 5 chunks if not specified
	}

	chunkSize := (maxID - minID + 1) / config.NumChunks
	if chunkSize == 0 {
		chunkSize = 1
	}

	log.Printf("Processing event edition data in %d chunks with chunk size: %d", config.NumChunks, chunkSize)

	results := make(chan string, config.NumChunks)
	semaphore := make(chan struct{}, config.NumWorkers)

	for i := 0; i < config.NumChunks; i++ {
		startID := minID + (i * chunkSize)
		endID := startID + chunkSize - 1

		// last chunk to include remaining records
		if i == config.NumChunks-1 {
			endID = maxID
		}

		// delay between chunk launches
		if i > 0 {
			delay := 3 * time.Second
			log.Printf("Waiting %v before launching event edition chunk %d...", delay, i+1)
			time.Sleep(delay)
		}

		semaphore <- struct{}{}
		go func(chunkNum, start, end int) {
			defer func() { <-semaphore }()
			processEventEditionChunk(mysqlDB, clickhouseConn, esClient, config, start, end, chunkNum, results)
		}(i+1, startID, endID)
	}

	for i := 0; i < config.NumChunks; i++ {
		result := <-results
		log.Printf("Event Edition Result: %s", result)
	}

	log.Println("Event Edition processing completed!")
}

// processes a single chunk of event edition data
func processEventEditionChunk(mysqlDB *sql.DB, clickhouseConn driver.Conn, esClient *elasticsearch.Client, config Config, startID, endID int, chunkNum int, results chan<- string) {
	log.Printf("Processing event edition chunk %d: ID range %d-%d", chunkNum, startID, endID)

	totalRecords := endID - startID + 1
	processed := 0

	offset := 0
	for {
		batchData, err := buildMigrationData(mysqlDB, "event", startID, endID, config.BatchSize)
		if err != nil {
			log.Printf("Event edition chunk %d batch error: %v", chunkNum, err)
			results <- fmt.Sprintf("Event edition chunk %d batch error: %v", chunkNum, err)
			return
		}

		if len(batchData) == 0 {
			break
		}

		processed += len(batchData)
		progress := float64(processed) / float64(totalRecords) * 100
		log.Printf("Event edition chunk %d: Retrieved %d records in batch (%.1f%% complete)", chunkNum, len(batchData), progress)

		// Get event IDs from this batch
		eventIDs := extractEventIDs(batchData)
		if len(eventIDs) > 0 {
			log.Printf("Event edition chunk %d: Fetching edition data for %d events", chunkNum, len(eventIDs))

			// Fetch edition data in parallel
			startTime := time.Now()
			editionData := fetchEditionDataParallel(mysqlDB, eventIDs, config.NumWorkers)
			editionTime := time.Since(startTime)
			log.Printf("Event edition chunk %d: Retrieved edition data for %d events in %v", chunkNum, len(editionData), editionTime)

			// Fetch company data for all editions
			var companyData []map[string]interface{}
			if len(editionData) > 0 {
				companyIDs := extractCompanyIDs(editionData)
				if len(companyIDs) > 0 {
					log.Printf("Event edition chunk %d: Fetching company data for %d companies", chunkNum, len(companyIDs))
					startTime = time.Now()
					companyData = fetchCompanyDataParallel(mysqlDB, companyIDs, config.NumWorkers)
					companyTime := time.Since(startTime)
					log.Printf("Event edition chunk %d: Retrieved company data for %d companies in %v", chunkNum, len(companyData), companyTime)
				}
			}

			// Fetch venue data for all editions
			var venueData []map[string]interface{}
			if len(editionData) > 0 {
				venueIDs := extractVenueIDs(editionData)
				if len(venueIDs) > 0 {
					log.Printf("Event edition chunk %d: Fetching venue data for %d venues", chunkNum, len(venueIDs))
					startTime = time.Now()
					venueData = fetchVenueDataParallel(mysqlDB, venueIDs, config.NumWorkers)
					venueTime := time.Since(startTime)
					log.Printf("Event edition chunk %d: Retrieved venue data for %d venues in %v", chunkNum, len(venueData), venueTime)
				}
			}

			// Fetch city data for all editions, companies, and venues
			var cityData []map[string]interface{}
			if len(editionData) > 0 {
				// Collect city IDs from edition data
				editionCityIDs := extractCityIDs(editionData)

				// Collect city IDs from company data
				var companyCityIDs []int64
				seenCompanyCityIDs := make(map[int64]bool)
				for _, company := range companyData {
					if cityID, ok := company["company_city"].(int64); ok && cityID > 0 {
						if !seenCompanyCityIDs[cityID] {
							companyCityIDs = append(companyCityIDs, cityID)
							seenCompanyCityIDs[cityID] = true
						}
					}
				}

				// Collect city IDs from venue data
				var venueCityIDs []int64
				seenVenueCityIDs := make(map[int64]bool)
				for _, venue := range venueData {
					if cityID, ok := venue["venue_city"].(int64); ok && cityID > 0 {
						if !seenVenueCityIDs[cityID] {
							venueCityIDs = append(venueCityIDs, cityID)
							seenVenueCityIDs[cityID] = true
						}
					}
				}

				// Combine all city IDs
				allCityIDs := make([]int64, 0, len(editionCityIDs)+len(companyCityIDs)+len(venueCityIDs))
				seenAllCityIDs := make(map[int64]bool)

				// Add edition city IDs
				for _, cityID := range editionCityIDs {
					if !seenAllCityIDs[cityID] {
						allCityIDs = append(allCityIDs, cityID)
						seenAllCityIDs[cityID] = true
					}
				}

				// Add company city IDs
				for _, cityID := range companyCityIDs {
					if !seenAllCityIDs[cityID] {
						allCityIDs = append(allCityIDs, cityID)
						seenAllCityIDs[cityID] = true
					}
				}

				// Add venue city IDs
				for _, cityID := range venueCityIDs {
					if !seenAllCityIDs[cityID] {
						allCityIDs = append(allCityIDs, cityID)
						seenAllCityIDs[cityID] = true
					}
				}

				if len(allCityIDs) > 0 {
					log.Printf("Event edition chunk %d: Fetching city data for %d cities (edition: %d, company: %d, venue: %d)",
						chunkNum, len(allCityIDs), len(editionCityIDs), len(companyCityIDs), len(venueCityIDs))
					startTime = time.Now()
					cityData = fetchCityDataParallel(mysqlDB, allCityIDs, config.NumWorkers)
					cityTime := time.Since(startTime)
					log.Printf("Event edition chunk %d: Retrieved city data for %d cities in %v", chunkNum, len(cityData), cityTime)
				}
			}

			// Fetch Elasticsearch data for all editions
			var esData map[int64]map[string]interface{}
			if len(editionData) > 0 {
				log.Printf("Event edition chunk %d: Fetching Elasticsearch data for %d events in batches of 200", chunkNum, len(eventIDs))
				startTime = time.Now()
				esData = fetchElasticsearchDataForEvents(esClient, config.IndexName, eventIDs)
				esTime := time.Since(startTime)
				log.Printf("Event edition chunk %d: Retrieved Elasticsearch data for %d events in %v", chunkNum, len(esData), esTime)
			}

			// Denormalize data, create lookup maps
			if len(editionData) > 0 {
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

				// Group editions by event_id and create current edition lookup
				eventEditions := make(map[int64][]map[string]interface{})
				currentEditionStartDates := make(map[int64]interface{})
				currentEditionIDs := make(map[int64]int64)

				for _, edition := range editionData {
					if eventID, ok := edition["event"].(int64); ok {
						eventEditions[eventID] = append(eventEditions[eventID], edition)
						// Storing current edition ID and start date for each event
						if currentEditionID, exists := edition["current_edition_id"]; exists {
							if editionID, ok := edition["edition_id"].(int64); ok {
								if currentEditionID.(int64) == editionID {
									currentEditionStartDates[eventID] = edition["edition_start_date"]
									currentEditionIDs[eventID] = editionID
								}
							}
						}
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
							companyID := edition["company_id"]
							venueID := edition["venue_id"]
							cityID := edition["edition_city"]
							editionWebsite := edition["edition_website"]

							// Get company data
							var company map[string]interface{}
							if companyID != nil {
								if c, exists := companyLookup[companyID.(int64)]; exists {
									company = c // If not found->company remains nil
								}
							}

							// Get venue data
							var venue map[string]interface{}
							if venueID != nil {
								if v, exists := venueLookup[venueID.(int64)]; exists {
									venue = v // If not found->venue remains nil
								}

							}

							// Get city data
							var city map[string]interface{}
							if cityID != nil {
								if c, exists := cityLookup[cityID.(int64)]; exists {
									city = c // If not found->city remains nil
								}

							}

							// Get company city data
							var companyCity map[string]interface{}
							if company != nil && company["company_city"] != nil {
								if companyCityID, ok := company["company_city"].(int64); ok {
									if c, exists := cityLookup[companyCityID]; exists {
										companyCity = c
									}
								}
							}

							// Get venue city data
							var venueCity map[string]interface{}
							if venue != nil && venue["venue_city"] != nil {
								if venueCityID, ok := venue["venue_city"].(int64); ok {
									if c, exists := cityLookup[venueCityID]; exists {
										venueCity = c
									}
								}
							}

							// Get Elasticsearch data
							esInfoMap := esData[eventID] // If not found, esInfoMap remains nil

							// Extract domain from edition website
							var editionDomain string
							if editionWebsite != nil {
								editionDomain = extractDomainFromWebsite(editionWebsite) // If no website, editionDomain remains empty string
							}

							// Extract domain from company website
							var companyDomain string
							if company != nil && company["company_website"] != nil {
								companyDomain = extractDomainFromWebsite(company["company_website"])
							}

							// Determine edition type using simplified logic
							editionType := determineEditionType(
								edition["edition_start_date"],
								currentEditionStartDates[eventID],
								edition["edition_id"].(int64),
								currentEditionIDs[eventID],
							)

							// Create record for ClickHouse insertion - include ALL data
							record := map[string]interface{}{
								"event_id":          eventData["id"],
								"event_name":        eventData["event_name"],
								"event_abbr_name":   eventData["abbr_name"],
								"event_description": esInfoMap["event_description"],
								"event_punchline":   esInfoMap["event_punchline"],
								"start_date":        eventData["start_date"],
								"end_date":          eventData["end_date"],
								"edition_id":        edition["edition_id"],
								"edition_country":   strings.ToUpper(safeConvertToString(eventData["country"])),
								"edition_city":      edition["edition_city"],
								"edition_city_name": safeConvertToString(city["name"]),
								"edition_city_lat":  city["event_city_lat"],
								"edition_city_long": city["event_city_long"],
								"company_id":        company["id"],
								"company_name":      company["company_name"],
								"company_domain":    companyDomain,
								"company_website":   company["company_website"],
								"company_country":   strings.ToUpper(safeConvertToString(company["company_country"])),
								"company_city":      company["company_city"],
								"company_city_name": func() *string {
									if companyCity != nil && companyCity["name"] != nil {
										nameStr := safeConvertToString(companyCity["name"])
										return &nameStr
									}
									return nil
								}(),
								"venue_id":      venue["id"],
								"venue_name":    venue["venue_name"],
								"venue_country": strings.ToUpper(safeConvertToString(venue["venue_country"])),
								"venue_city":    venue["venue_city"],
								"venue_city_name": func() *string {
									if venueCity != nil && venueCity["name"] != nil {
										nameStr := safeConvertToString(venueCity["name"])
										return &nameStr
									}
									return nil
								}(),
								"venue_lat":              venue["venue_lat"],
								"venue_long":             venue["venue_long"],
								"published":              eventData["published"],
								"status":                 eventData["status"],
								"editions_audiance_type": eventData["event_audience"],
								"edition_functionality":  eventData["functionality"],
								"edition_website":        edition["edition_website"],
								"edition_domain":         editionDomain,
								"event_followers":        esInfoMap["event_following"],
								"edition_followers":      esInfoMap["event_following"],
								"event_exhibitor":        esInfoMap["event_exhibitors"],
								"edition_exhibitor":      esInfoMap["edition_exhibitor"],
								"event_sponsor":          esInfoMap["event_totalSponsor"],
								"edition_sponsor":        esInfoMap["edition_sponsor"],
								"event_speaker":          esInfoMap["event_speakers"],
								"edition_speaker":        esInfoMap["edition_speaker"],
								"event_created":          eventData["created"],
								"edition_created":        edition["edition_created"],
								"event_hybrid":           esInfoMap["event_hybrid"],
								"isBranded": func() *uint32 {
									if eventData["brand_id"] != nil {
										// If brand_id exists, set isBranded to 1 (true)
										val := uint32(1)
										return &val
									}
									// If brand_id is null, set isBranded to 0 (false)
									val := uint32(0)
									return &val
								}(),
								"maturity":                determineMaturity(esInfoMap["total_edition"]),
								"event_pricing":           esInfoMap["event_pricing"],
								"event_logo":              esInfoMap["event_logo"],
								"event_estimatedVisitors": esInfoMap["eventEstimatedTag"],
								"event_frequency":         esInfoMap["event_frequency"],
								"event_avgRating":         esInfoMap["avg_rating"],
								"version":                 1,
							}

							// Set edition_type with default value if nil
							if editionType != nil {
								record["edition_type"] = *editionType
							} else {
								record["edition_type"] = "NA"
							}

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

				// Count events with missing current editions
				eventsWithMissingCurrentEdition := 0
				for eventID := range eventEditions {
					if currentEditionIDs[eventID] == 0 {
						eventsWithMissingCurrentEdition++
					}
				}

				log.Printf("Event edition chunk %d: Data completeness - Complete: %d, Partial: %d, Skipped: %d",
					chunkNum, completeCount, partialCount, skippedCount)
				if eventsWithMissingCurrentEdition > 0 {
					log.Printf("Event edition chunk %d: Warning - %d events have no current edition (event_edition is NULL)",
						chunkNum, eventsWithMissingCurrentEdition)
				}

				// Insert collected records into ClickHouse
				if len(clickHouseRecords) > 0 {
					log.Printf("Event edition chunk %d: Attempting to insert %d records into ClickHouse...", chunkNum, len(clickHouseRecords))

					insertErr := retryWithBackoff(
						func() error {
							return insertEventEditionDataIntoClickHouse(clickhouseConn, clickHouseRecords, config.ClickHouseWorkers)
						},
						3,
						fmt.Sprintf("ClickHouse insertion for chunk %d", chunkNum),
					)

					if insertErr != nil {
						log.Printf("Event edition chunk %d: ClickHouse insertion failed after retries: %v", chunkNum, insertErr)
						log.Printf("Event edition chunk %d: %d records failed to insert - consider manual retry", chunkNum, len(clickHouseRecords))
					} else {
						log.Printf("Event edition chunk %d: Successfully inserted %d records into ClickHouse", chunkNum, len(clickHouseRecords))
					}
				} else {
					log.Printf("Event edition chunk %d: No records to insert into ClickHouse", chunkNum)
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

	results <- fmt.Sprintf("Event edition chunk %d completed successfully", chunkNum)
}

// 1. current_edition: The edition_id that matches event.event_edition (only one per event)
// 2. future_edition: All editions with start_date > current_edition start_date
// 3. past_edition: All editions with start_date < current_edition start_date
func determineEditionType(editionStartDate, currentEditionStartDate interface{}, editionID, currentEditionID int64) *string {
	// edition is the current edition, return "current_edition"
	if editionID == currentEditionID {
		editionType := "current_edition"
		return &editionType
	}

	var editionDateStr, currentDateStr string

	switch v := editionStartDate.(type) {
	case string:
		editionDateStr = v
	case []uint8:
		editionDateStr = string(v)
	default:
		return nil
	}

	switch v := currentEditionStartDate.(type) {
	case string:
		currentDateStr = v
	case []uint8:
		currentDateStr = string(v)
	default:
		return nil
	}

	editionDate, err := time.Parse("2006-01-02", editionDateStr)
	if err != nil {
		return nil
	}

	currentDate, err := time.Parse("2006-01-02", currentDateStr)
	if err != nil {
		return nil
	}

	if editionDate.After(currentDate) {
		editionType := "future_edition"
		return &editionType
	} else if editionDate.Before(currentDate) {
		editionType := "past_edition"
		return &editionType
	} else {
		editionType := "past_edition"
		return &editionType
	}
}

// determineMaturity determines the maturity level based on total_edition count
// new: 1 edition, growing: 2-3 editions, established: 4-8 editions, flagship: 9+ editions
func determineMaturity(totalEdition interface{}) *string {
	if totalEdition == nil {
		return nil
	}

	var editionCount uint32
	switch v := totalEdition.(type) {
	case uint32:
		editionCount = v
	case int:
		if v >= 0 {
			editionCount = uint32(v)
		} else {
			return nil
		}
	case int64:
		if v >= 0 {
			editionCount = uint32(v)
		} else {
			return nil
		}
	case float64:
		if v >= 0 && v == float64(uint32(v)) {
			editionCount = uint32(v)
		} else {
			return nil
		}
	default:
		return nil
	}

	var maturity string
	switch {
	case editionCount == 1:
		maturity = "new"
	case editionCount >= 2 && editionCount <= 3:
		maturity = "growing"
	case editionCount >= 4 && editionCount <= 8:
		maturity = "established"
	case editionCount >= 9:
		maturity = "flagship"
	default:
		return nil
	}

	return &maturity
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

	batchSize := 1000

	expectedBatches := (len(cityIDs) + batchSize - 1) / batchSize
	results := make(chan []map[string]interface{}, expectedBatches)
	semaphore := make(chan struct{}, numWorkers)

	var allCityData []map[string]interface{}

	var wg sync.WaitGroup

	for i := 0; i < len(cityIDs); i += batchSize {
		end := i + batchSize
		if end > len(cityIDs) {
			end = len(cityIDs)
		}

		batch := cityIDs[i:end]

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

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	completedBatches := 0

collectLoop:
	for completedBatches < expectedBatches {
		select {
		case cityData := <-results:
			allCityData = append(allCityData, cityData...)
			completedBatches++
		case <-done:

			break collectLoop
		case <-time.After(120 * time.Second):
			log.Printf("Warning: Timeout waiting for city data. Completed %d/%d batches",
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
		log.Printf("Missing city IDs (%d): %v", len(missingCityIDs), missingCityIDs)
	}

	return allCityData
}

func fetchCityDataForBatch(db *sql.DB, cityIDs []int64) []map[string]interface{} {
	if len(cityIDs) == 0 {
		return nil
	}

	placeholders := make([]string, len(cityIDs))
	args := make([]interface{}, len(cityIDs))
	for i, id := range cityIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(`
		SELECT 
			id, name, geo_lat as event_city_lat, geo_long as event_city_long
		FROM city 
		WHERE id IN (%s)`, strings.Join(placeholders, ","))

	rows, err := db.Query(query, args...)
	if err != nil {
		log.Printf("Error fetching city data: %v", err)
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
									log.Printf("Warning: Could not parse %s value '%s' to float64: %v", col, str, err)
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
								log.Printf("Warning: Could not parse %s string '%s' to float64: %v", col, str, err)
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
		"_source": []string{"id", "description", "exhibitors", "speakers", "totalSponsor", "following", "punchline", "frequency", "city", "hybrid", "logo", "pricing", "total_edition", "avg_rating", "eventEstimatedTag"},
	}

	queryJSON, _ := json.Marshal(query)

	// Execute search with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	searchRes, err := esClient.Search(
		esClient.Search.WithContext(ctx),
		esClient.Search.WithIndex(indexName),
		esClient.Search.WithBody(strings.NewReader(string(queryJSON))),
	)
	if err != nil {
		log.Printf("Warning: Failed to search Elasticsearch for batch: %v", err)
		return results
	}
	defer searchRes.Body.Close()

	if searchRes.IsError() {
		log.Printf("Warning: Elasticsearch search failed: %v", searchRes.Status())
		return results
	}

	// Parse response
	var result map[string]interface{}
	if err := json.NewDecoder(searchRes.Body).Decode(&result); err != nil {
		log.Printf("Warning: Failed to decode Elasticsearch response: %v", err)
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
				log.Printf("Warning: Failed to parse id string '%s': %v", eventIDStr, err)
				continue
			}
		} else if eventIDNum, ok := source["id"].(float64); ok {
			// Convert float64 to int64
			eventIDInt = int64(eventIDNum)
		} else {
			log.Printf("Warning: Unexpected id type: %T, value: %v", source["id"], source["id"])
			continue
		}

		// Helper function to convert string to uint32 with fallback
		convertStringToUInt32 := func(key string) interface{} {
			if val, exists := source[key]; exists && val != nil {
				strVal := convertToString(val)
				if strVal != "" {
					if num, err := strconv.ParseUint(strVal, 10, 32); err == nil {
						return uint32(num)
					}
				}
			}
			return nil // Return nil instead of 0 for failed conversions
		}

		// Helper function to convert string to uint8 with fallback
		convertStringToUInt8 := func(key string) interface{} {
			if val, exists := source[key]; exists && val != nil {
				// Handle float64 values directly (common from Elasticsearch)
				if floatVal, ok := val.(float64); ok {
					if floatVal >= 0 && floatVal <= 255 && floatVal == float64(uint8(floatVal)) {
						return uint8(floatVal)
					}
					return nil
				}
				// Handle other numeric types
				if intVal, ok := val.(int); ok {
					if intVal >= 0 && intVal <= 255 {
						return uint8(intVal)
					}
					return nil
				}
				// Handle string conversion as fallback
				strVal := convertToString(val)
				if strVal != "" {
					if num, err := strconv.ParseUint(strVal, 10, 8); err == nil {
						return uint8(num)
					}
				}
			}
			return nil // Return nil instead of 0 for failed conversions
		}

		// Convert total_edition properly - it comes as float64 from ES, not string
		var convertedTotalEdition interface{}
		if rawTotalEdition := source["total_edition"]; rawTotalEdition != nil {
			if floatVal, ok := rawTotalEdition.(float64); ok {
				if floatVal >= 0 && floatVal == float64(uint32(floatVal)) {
					convertedTotalEdition = uint32(floatVal)
				} else {
					convertedTotalEdition = nil
				}
			} else {
				convertedTotalEdition = nil
			}
		} else {
			convertedTotalEdition = nil
		}

		results[eventIDInt] = map[string]interface{}{
			"event_description":  convertToString(source["description"]),
			"event_exhibitors":   convertStringToUInt32("exhibitors"),
			"event_speakers":     convertStringToUInt32("speakers"),
			"event_totalSponsor": convertStringToUInt32("totalSponsor"),
			"event_following":    convertStringToUInt32("following"),
			"event_punchline":    convertToString(source["punchline"]),
			"edition_exhibitor":  convertStringToUInt32("exhibitors"),
			"edition_sponsor":    convertStringToUInt32("totalSponsor"),
			"edition_speaker":    convertStringToUInt32("speakers"),
			"edition_followers":  convertStringToUInt32("following"),
			"event_frequency":    convertToString(source["frequency"]),
			"event_hybrid":       convertStringToUInt8("hybrid"),
			"event_logo":         convertToString(source["logo"]),
			"event_pricing":      convertToString(source["pricing"]),
			"total_edition":      convertedTotalEdition,
			"avg_rating":         source["avg_rating"],
			"eventEstimatedTag":  convertToString(source["eventEstimatedTag"]),
		}
	}

	return results
}

func fetchElasticsearchDataForEvents(esClient *elasticsearch.Client, indexName string, eventIDs []int64) map[int64]map[string]interface{} {
	if len(eventIDs) == 0 {
		return nil
	}

	results := make(map[int64]map[string]interface{})
	batchSize := 200 //reduced batch size to prevent Elasticsearch timeouts

	expectedBatches := (len(eventIDs) + batchSize - 1) / batchSize
	resultsChan := make(chan map[int64]map[string]interface{}, expectedBatches)
	semaphore := make(chan struct{}, 5) //reduced concurrency to prevent Elasticsearch overload

	var allResults []map[int64]map[string]interface{}
	var wg sync.WaitGroup

	for i := 0; i < len(eventIDs); i += batchSize {
		end := i + batchSize
		if end > len(eventIDs) {
			end = len(eventIDs)
		}
		batch := eventIDs[i:end]

		semaphore <- struct{}{}
		wg.Add(1)

		go func(eventIDBatch []int64, batchNum int) {
			defer func() {
				<-semaphore
				wg.Done()
			}()

			// Add small delay between batches to reduce Elasticsearch pressure
			if batchNum > 0 {
				time.Sleep(100 * time.Millisecond)
			}

			// Add retry logic for failed batches
			var batchResults map[int64]map[string]interface{}
			maxRetries := 3 //increased retries
			for retry := 0; retry <= maxRetries; retry++ {
				batchResults = fetchElasticsearchBatch(esClient, indexName, eventIDBatch)
				if len(batchResults) > 0 || retry == maxRetries {
					if retry > 0 {
						log.Printf("Elasticsearch batch %d: Success after %d retries, got %d results", batchNum, retry, len(batchResults))
					}
					break
				}
				if retry < maxRetries {
					backoffTime := time.Duration(retry+1) * 3 * time.Second // Increased backoff time
					log.Printf("Elasticsearch batch %d: Retry %d/%d after %v backoff", batchNum, retry+1, maxRetries, backoffTime)
					time.Sleep(backoffTime)
				}
			}
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
			break collectLoop
		case <-time.After(120 * time.Second):
			log.Printf("Warning: Timeout waiting for Elasticsearch data. Completed %d/%d batches",
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

	log.Printf("OK: Retrieved Elasticsearch data for %d events in %d batches", len(results), len(allResults))
	return results
}

// inserts event edition data into event_edition_ch
func insertEventEditionDataIntoClickHouse(clickhouseConn driver.Conn, records []map[string]interface{}, numWorkers int) error {
	if len(records) == 0 {
		return nil
	}

	if numWorkers <= 1 {
		return insertEventEditionDataSingleWorker(clickhouseConn, records)
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
			err := insertEventEditionDataSingleWorker(clickhouseConn, batch)
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

func insertEventEditionDataSingleWorker(clickhouseConn driver.Conn, records []map[string]interface{}) error {
	if len(records) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	batch, err := clickhouseConn.PrepareBatch(ctx, `
		INSERT INTO event_edition_ch (
			event_id, event_name, event_abbr_name, event_description, event_punchline, event_avgRating,
			start_date, end_date,
			edition_id, edition_country, edition_city, edition_city_name, edition_city_lat, edition_city_long,
			company_id, company_name, company_domain, company_website, company_country, company_city, company_city_name,
			venue_id, venue_name, venue_country, venue_city, venue_city_name, venue_lat, venue_long,
			published, status, editions_audiance_type, edition_functionality, edition_website, edition_domain,
			edition_type, event_followers, edition_followers, event_exhibitor, edition_exhibitor,
			event_sponsor, edition_sponsor, event_speaker, edition_speaker,
			event_created, edition_created, event_hybrid, isBranded, maturity,
			event_pricing, event_logo, event_estimatedVisitors, event_frequency, version
		)
	`)
	if err != nil {
		log.Printf("ERROR: Failed to prepare ClickHouse batch for event_edition_ch: %v", err)
		return fmt.Errorf("failed to prepare ClickHouse batch: %v", err)
	}

	for _, record := range records {
		eventEditionRecord := convertToEventEditionRecord(record)

		err := batch.Append(
			eventEditionRecord.EventID,                // event_id: UInt32 NOT NULL
			eventEditionRecord.EventName,              // event_name: String NOT NULL
			eventEditionRecord.EventAbbrName,          // event_abbr_name: Nullable(String)
			eventEditionRecord.EventDescription,       // event_description: Nullable(String)
			eventEditionRecord.EventPunchline,         // event_punchline: Nullable(String)
			eventEditionRecord.EventAvgRating,         // event_avgRating: Nullable(Decimal(3,2))
			eventEditionRecord.StartDate,              // start_date: Date NOT NULL
			eventEditionRecord.EndDate,                // end_date: Date NOT NULL
			eventEditionRecord.EditionID,              // edition_id: UInt32 NOT NULL
			eventEditionRecord.EditionCountry,         // edition_country: LowCardinality(FixedString(2)) NOT NULL
			eventEditionRecord.EditionCity,            // edition_city: UInt32 NOT NULL
			eventEditionRecord.EditionCityName,        // edition_city_name: String NOT NULL
			eventEditionRecord.EditionCityLat,         // edition_city_lat: Float64 NOT NULL
			eventEditionRecord.EditionCityLong,        // edition_city_long: Float64 NOT NULL
			eventEditionRecord.CompanyID,              // company_id: Nullable(UInt32)
			eventEditionRecord.CompanyName,            // company_name: Nullable(String)
			eventEditionRecord.CompanyDomain,          // company_domain: Nullable(String)
			eventEditionRecord.CompanyWebsite,         // company_website: Nullable(String)
			eventEditionRecord.CompanyCountry,         // company_country: LowCardinality(Nullable(FixedString(2)))
			eventEditionRecord.CompanyCity,            // company_city: Nullable(UInt32)
			eventEditionRecord.CompanyCityName,        // company_city_name: Nullable(String)
			eventEditionRecord.VenueID,                // venue_id: Nullable(UInt32)
			eventEditionRecord.VenueName,              // venue_name: Nullable(String)
			eventEditionRecord.VenueCountry,           // venue_country: LowCardinality(Nullable(FixedString(2)))
			eventEditionRecord.VenueCity,              // venue_city: Nullable(UInt32)
			eventEditionRecord.VenueCityName,          // venue_city_name: Nullable(String)
			eventEditionRecord.VenueLat,               // venue_lat: Nullable(Float64)
			eventEditionRecord.VenueLong,              // venue_long: Nullable(Float64)
			eventEditionRecord.Published,              // published: Int8 NOT NULL
			eventEditionRecord.Status,                 // status: LowCardinality(FixedString(1)) NOT NULL DEFAULT 'A'
			eventEditionRecord.EditionsAudianceType,   // editions_audiance_type: UInt16 NOT NULL
			eventEditionRecord.EditionFunctionality,   // edition_functionality: LowCardinality(String) NOT NULL
			eventEditionRecord.EditionWebsite,         // edition_website: Nullable(String)
			eventEditionRecord.EditionDomain,          // edition_domain: Nullable(String)
			eventEditionRecord.EditionType,            // edition_type: LowCardinality(Nullable(String)) DEFAULT 'NA'
			eventEditionRecord.EventFollowers,         // event_followers: Nullable(UInt32)
			eventEditionRecord.EditionFollowers,       // edition_followers: Nullable(UInt32)
			eventEditionRecord.EventExhibitor,         // event_exhibitor: Nullable(UInt32)
			eventEditionRecord.EditionExhibitor,       // edition_exhibitor: Nullable(UInt32)
			eventEditionRecord.EventSponsor,           // event_sponsor: Nullable(UInt32)
			eventEditionRecord.EditionSponsor,         // edition_sponsor: Nullable(UInt32)
			eventEditionRecord.EventSpeaker,           // event_speaker: Nullable(UInt32)
			eventEditionRecord.EditionSpeaker,         // edition_speaker: Nullable(UInt32)
			eventEditionRecord.EventCreated,           // event_created: DateTime NOT NULL
			eventEditionRecord.EditionCreated,         // edition_created: DateTime NOT NULL
			eventEditionRecord.EventHybrid,            // event_hybrid: Nullable(UInt32)
			eventEditionRecord.IsBranded,              // isBranded: Nullable(UInt32)
			eventEditionRecord.Maturity,               // maturity: LowCardinality(Nullable(String))
			eventEditionRecord.EventPricing,           // event_pricing: LowCardinality(Nullable(String))
			eventEditionRecord.EventLogo,              // event_logo: Nullable(String)
			eventEditionRecord.EventEstimatedVisitors, // event_estimatedVisitors: LowCardinality(Nullable(String))
			eventEditionRecord.EventFrequency,         // event_frequency: LowCardinality(Nullable(String))
			eventEditionRecord.Version,                // version: UInt32 NOT NULL DEFAULT 1
		)
		if err != nil {
			log.Printf("ERROR: Failed to append record to batch: %v", err)
			log.Printf("Record data: EventID=%d, EventName=%s, EventAvgRating=%v",
				eventEditionRecord.EventID, eventEditionRecord.EventName, eventEditionRecord.EventAvgRating)
			return fmt.Errorf("failed to append record to batch: %v", err)
		}
	}

	if err := batch.Send(); err != nil {
		log.Printf("ERROR: Failed to send ClickHouse batch: %v", err)
		return fmt.Errorf("failed to send ClickHouse batch: %v", err)
	}

	log.Printf("OK: Successfully inserted %d event edition records", len(records))
	return nil
}

func processExhibitorOnly(mysqlDB *sql.DB, clickhouseConn driver.Conn, config Config) {
	log.Println("=== Starting EXHIBITOR ONLY Processing ===")

	totalRecords, minID, maxID, err := getTotalRecordsAndIDRange(mysqlDB, "event_exhibitor")
	if err != nil {
		log.Fatal("Failed to get total records and ID range from event_exhibitor:", err)
	}

	log.Printf("Total exhibitor records: %d, Min ID: %d, Max ID: %d", totalRecords, minID, maxID)

	if config.NumChunks <= 0 {
		config.NumChunks = 5 // Default to 5 chunks if not specified
	}

	chunkSize := (maxID - minID + 1) / config.NumChunks
	if chunkSize == 0 {
		chunkSize = 1
	}

	log.Printf("Processing exhibitor data in %d chunks with chunk size: %d", config.NumChunks, chunkSize)

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
			processExhibitorChunk(mysqlDB, clickhouseConn, config, start, end, chunkNum, results)
		}(i+1, startID, endID)
	}

	for i := 0; i < config.NumChunks; i++ {
		result := <-results
		log.Printf("Exhibitor Result: %s", result)
	}

	log.Println("Exhibitor processing completed!")
}

func processExhibitorChunk(mysqlDB *sql.DB, clickhouseConn driver.Conn, config Config, startID, endID int, chunkNum int, results chan<- string) {
	log.Printf("Processing exhibitor chunk %d: ID range %d-%d", chunkNum, startID, endID)

	totalRecords := endID - startID + 1
	processed := 0

	// Use batching within the chunk
	offset := 0
	for {
		batchData, err := buildExhibitorMigrationData(mysqlDB, startID, endID, config.BatchSize)
		if err != nil {
			results <- fmt.Sprintf("Exhibitor chunk %d batch error: %v", chunkNum, err)
			return
		}

		if len(batchData) == 0 {
			break
		}

		processed += len(batchData)
		progress := float64(processed) / float64(totalRecords) * 100
		log.Printf("Exhibitor chunk %d: Retrieved %d records in batch (%.1f%% complete)", chunkNum, len(batchData), progress)

		eventIDs := extractExhibitorEventIDs(batchData) // Extract event IDs from this batch to fetch social media data
		if len(eventIDs) > 0 {
			log.Printf("Exhibitor chunk %d: Processing %d exhibitor records", chunkNum, len(batchData))

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
				log.Printf("Exhibitor chunk %d: Fetching social media data for %d companies", chunkNum, len(exhibitorCompanyIDs))
				startTime := time.Now()
				socialData = fetchExhibitorSocialData(mysqlDB, exhibitorCompanyIDs)
				socialTime := time.Since(startTime)
				log.Printf("Exhibitor chunk %d: Retrieved social media data for %d companies in %v", chunkNum, len(socialData), socialTime)
			}

			// Collect city IDs from exhibitor data
			var exhibitorCityIDs []int64
			seenCityIDs := make(map[int64]bool)
			for _, exhibitor := range batchData {
				if cityID, ok := exhibitor["city"].(int64); ok && cityID > 0 {
					if !seenCityIDs[cityID] {
						exhibitorCityIDs = append(exhibitorCityIDs, cityID)
						seenCityIDs[cityID] = true
					}
				}
			}

			// Fetch city data for exhibitor cities
			var cityData []map[string]interface{}
			var cityLookup map[int64]map[string]interface{}
			if len(exhibitorCityIDs) > 0 {
				log.Printf("Exhibitor chunk %d: Fetching city data for %d cities", chunkNum, len(exhibitorCityIDs))
				startTime := time.Now()
				cityData = fetchCityDataParallel(mysqlDB, exhibitorCityIDs, config.NumWorkers)
				cityTime := time.Since(startTime)
				log.Printf("Exhibitor chunk %d: Retrieved city data for %d cities in %v", chunkNum, len(cityData), cityTime)

				// Create city lookup map
				cityLookup = make(map[int64]map[string]interface{})
				if len(cityData) > 0 {
					for _, city := range cityData {
						if cityID, ok := city["id"].(int64); ok {
							cityLookup[cityID] = city
						}
					}
				}
			}

			var exhibitorRecords []ExhibitorRecord
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

				// Get city data for this exhibitor
				var companyCityName *string
				if cityID, ok := exhibitor["city"].(int64); ok && cityLookup != nil {
					if city, exists := cityLookup[cityID]; exists && city["name"] != nil {
						nameStr := safeConvertToString(city["name"])
						companyCityName = &nameStr
					}
				}

				// Convert data to proper types for protocol
				companyID := convertToUInt32Ptr(exhibitor["company_id"])
				editionID := convertToUInt32(exhibitor["edition_id"])
				eventID := convertToUInt32(exhibitor["event_id"])

				// Create exhibitor record with proper types
				exhibitorRecord := ExhibitorRecord{
					CompanyID:       companyID,
					CompanyIDName:   getCompanyNameOrDefault(exhibitor["company_name"]),
					EditionID:       editionID,
					EventID:         eventID,
					CompanyWebsite:  convertToStringPtr(exhibitor["website"]),
					CompanyDomain:   convertToStringPtr(companyDomain),
					CompanyCountry:  toUpperNullableString(convertToStringPtr(exhibitor["country"])),
					CompanyCity:     convertToUInt32Ptr(exhibitor["city"]),
					CompanyCityName: companyCityName,
					FacebookID:      convertToStringPtr(facebookID),
					LinkedinID:      convertToStringPtr(linkedinID),
					TwitterID:       convertToStringPtr(twitterID),
					Version:         1,
				}

				exhibitorRecords = append(exhibitorRecords, exhibitorRecord)
			}

			// Insert exhibitor data into ClickHouse
			if len(exhibitorRecords) > 0 {
				log.Printf("Exhibitor chunk %d: Attempting to insert %d records into event_exhibitor_ch...", chunkNum, len(exhibitorRecords))
				exhibitorInsertErr := retryWithBackoff(
					func() error {
						return insertExhibitorDataIntoClickHouse(clickhouseConn, exhibitorRecords, config.ClickHouseWorkers)
					},
					3,
					fmt.Sprintf("exhibitor insertion for chunk %d", chunkNum),
				)

				if exhibitorInsertErr != nil {
					log.Printf("Exhibitor chunk %d: Insertion failed after retries: %v", chunkNum, exhibitorInsertErr)
					results <- fmt.Sprintf("Exhibitor chunk %d: Failed to insert %d records", chunkNum, len(exhibitorRecords))
					return
				} else {
					log.Printf("Exhibitor chunk %d: Successfully inserted %d records into event_exhibitor_ch", chunkNum, len(exhibitorRecords))
				}
			} else {
				log.Printf("Exhibitor chunk %d: No exhibitor records to insert", chunkNum)
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

	results <- fmt.Sprintf("Exhibitor chunk %d: Completed successfully", chunkNum)
}

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
			log.Printf("Error fetching exhibitor social data batch %d-%d: %v", i, end-1, err)
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

func insertExhibitorDataIntoClickHouse(clickhouseConn driver.Conn, exhibitorRecords []ExhibitorRecord, numWorkers int) error {
	if len(exhibitorRecords) == 0 {
		return nil
	}

	if numWorkers <= 1 {
		return insertExhibitorDataSingleWorker(clickhouseConn, exhibitorRecords)
	}

	batchSize := (len(exhibitorRecords) + numWorkers - 1) / numWorkers
	results := make(chan error, numWorkers)
	semaphore := make(chan struct{}, numWorkers)

	for i := 0; i < numWorkers; i++ {
		start := i * batchSize
		end := start + batchSize
		if end > len(exhibitorRecords) {
			end = len(exhibitorRecords)
		}
		if start >= len(exhibitorRecords) {
			break
		}

		semaphore <- struct{}{}
		go func(start, end int) {
			defer func() { <-semaphore }()
			batch := exhibitorRecords[start:end]
			err := insertExhibitorDataSingleWorker(clickhouseConn, batch)
			results <- err
		}(start, end)
	}

	for i := 0; i < numWorkers && i*batchSize < len(exhibitorRecords); i++ {
		if err := <-results; err != nil {
			return err
		}
	}

	return nil
}

func insertExhibitorDataSingleWorker(clickhouseConn driver.Conn, exhibitorRecords []ExhibitorRecord) error {
	if len(exhibitorRecords) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	batch, err := clickhouseConn.PrepareBatch(ctx, `
		INSERT INTO event_exhibitor_ch (
			company_id, company_id_name, edition_id, event_id, company_website,
			company_domain, company_country, company_city, company_city_name, facebook_id,
			linkedin_id, twitter_id, version
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare ClickHouse batch: %v", err)
	}

	for _, record := range exhibitorRecords {
		err := batch.Append(
			record.CompanyID,       // company_id: Nullable(UInt32)
			record.CompanyIDName,   // company_id_name: String NOT NULL
			record.EditionID,       // edition_id: UInt32 NOT NULL
			record.EventID,         // event_id: UInt32 NOT NULL
			record.CompanyWebsite,  // company_website: Nullable(String)
			record.CompanyDomain,   // company_domain: Nullable(String)
			record.CompanyCountry,  // company_country: LowCardinality(FixedString(2))
			record.CompanyCity,     // company_city: Nullable(UInt32)
			record.CompanyCityName, // company_city_name: LowCardinality(Nullable(String))
			record.FacebookID,      // facebook_id: Nullable(String)
			record.LinkedinID,      // linkedin_id: Nullable(String)
			record.TwitterID,       // twitter_id: Nullable(String)
			record.Version,         // version: UInt32 NOT NULL DEFAULT 1
		)
		if err != nil {
			return fmt.Errorf("failed to append record to batch: %v", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send ClickHouse batch: %v", err)
	}

	log.Printf("OK: Successfully inserted %d exhibitor records", len(exhibitorRecords))
	return nil
}

// extracts event IDs from exhibitor data
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

func processSponsorsOnly(mysqlDB *sql.DB, clickhouseConn driver.Conn, config Config) {
	log.Println("=== Starting SPONSORS ONLY Processing ===")

	totalRecords, minID, maxID, err := getTotalRecordsAndIDRange(mysqlDB, "event_sponsors")
	if err != nil {
		log.Fatal("Failed to get total records and ID range from event_sponsors:", err)
	}

	log.Printf("Total sponsors records: %d, Min ID: %d, Max ID: %d", totalRecords, minID, maxID)

	if config.NumChunks <= 0 {
		config.NumChunks = 5 // Default to 5 chunks if not specified
	}

	chunkSize := (maxID - minID + 1) / config.NumChunks
	if chunkSize == 0 {
		chunkSize = 1
	}

	log.Printf("Processing sponsors data in %d chunks with chunk size: %d", config.NumChunks, chunkSize)

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
			log.Printf("Waiting %v before launching sponsors chunk %d...", delay, i+1)
			time.Sleep(delay)
		}

		semaphore <- struct{}{}
		go func(chunkNum, start, end int) {
			defer func() { <-semaphore }()
			processSponsorsChunk(mysqlDB, clickhouseConn, config, start, end, chunkNum, results)
		}(i+1, startID, endID)
	}

	for i := 0; i < config.NumChunks; i++ {
		result := <-results
		log.Printf("Sponsors Result: %s", result)
	}

	log.Println("Sponsors processing completed!")
}

func processSponsorsChunk(mysqlDB *sql.DB, clickhouseConn driver.Conn, config Config, startID, endID int, chunkNum int, results chan<- string) {
	log.Printf("Processing sponsors chunk %d: ID range %d-%d", chunkNum, startID, endID)

	totalRecords := endID - startID + 1
	processed := 0

	// Use batching within the chunk
	offset := 0
	for {
		batchData, err := buildSponsorsMigrationData(mysqlDB, startID, endID, config.BatchSize)
		if err != nil {
			results <- fmt.Sprintf("Sponsors chunk %d batch error: %v", chunkNum, err)
			return
		}

		if len(batchData) == 0 {
			break
		}

		processed += len(batchData)
		progress := float64(processed) / float64(totalRecords) * 100
		log.Printf("Sponsors chunk %d: Retrieved %d records in batch (%.1f%% complete)", chunkNum, len(batchData), progress)

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
			log.Printf("Sponsors chunk %d: Fetching company data for %d companies", chunkNum, len(sponsorCompanyIDs))
			startTime := time.Now()
			companyData = fetchSponsorsCompanyData(mysqlDB, sponsorCompanyIDs)
			companyTime := time.Since(startTime)
			log.Printf("Sponsors chunk %d: Retrieved company data for %d companies in %v", chunkNum, len(companyData), companyTime)
		}

		// Collect city IDs from sponsor company data
		var sponsorCityIDs []int64
		seenCityIDs := make(map[int64]bool)
		for _, company := range companyData {
			if cityID, ok := company["city"].(int64); ok && cityID > 0 {
				if !seenCityIDs[cityID] {
					sponsorCityIDs = append(sponsorCityIDs, cityID)
					seenCityIDs[cityID] = true
				}
			}
		}

		// Fetch city data for sponsor cities
		var cityData []map[string]interface{}
		var cityLookup map[int64]map[string]interface{}
		if len(sponsorCityIDs) > 0 {
			log.Printf("Sponsors chunk %d: Fetching city data for %d cities", chunkNum, len(sponsorCityIDs))
			startTime := time.Now()
			cityData = fetchCityDataParallel(mysqlDB, sponsorCityIDs, config.NumWorkers)
			cityTime := time.Since(startTime)
			log.Printf("Sponsors chunk %d: Retrieved city data for %d cities in %v", chunkNum, len(cityData), cityTime)

			// Create city lookup map
			cityLookup = make(map[int64]map[string]interface{})
			if len(cityData) > 0 {
				for _, city := range cityData {
					if cityID, ok := city["id"].(int64); ok {
						cityLookup[cityID] = city
					}
				}
			}
		}

		var sponsorRecords []SponsorRecord
		for _, sponsor := range batchData {
			// Get company data for this sponsor
			var companyWebsite, companyDomain, facebookID, linkedinID, twitterID, companyCountry, companyCity interface{}
			if companyID, ok := sponsor["company_id"].(int64); ok && companyData != nil {
				if company, exists := companyData[companyID]; exists {
					companyWebsite = company["website"]
					companyCountry = strings.ToUpper(safeConvertToString(company["country"]))
					companyCity = company["city"]

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

			// Get city data for this sponsor
			var companyCityName *string
			if companyCity != nil {
				if cityID, ok := companyCity.(int64); ok && cityLookup != nil {
					if city, exists := cityLookup[cityID]; exists && city["name"] != nil {
						nameStr := safeConvertToString(city["name"])
						companyCityName = &nameStr
					}
				}
			}

			// Convert data to proper types
			companyID := convertToUInt32Ptr(sponsor["company_id"])
			editionID := convertToUInt32(sponsor["event_edition"])
			eventID := convertToUInt32(sponsor["event_id"])

			// Create sponsor record with proper types
			sponsorRecord := SponsorRecord{
				CompanyID:       companyID,
				CompanyIDName:   getCompanyNameOrDefault(sponsor["name"]),
				EditionID:       editionID,
				EventID:         eventID,
				CompanyWebsite:  convertToStringPtr(companyWebsite),
				CompanyDomain:   convertToStringPtr(companyDomain),
				CompanyCountry:  toUpperNullableString(convertToStringPtr(companyCountry)),
				CompanyCity:     convertToUInt32Ptr(companyCity),
				CompanyCityName: companyCityName,
				FacebookID:      convertToStringPtr(facebookID),
				LinkedinID:      convertToStringPtr(linkedinID),
				TwitterID:       convertToStringPtr(twitterID),
				Version:         1,
			}

			sponsorRecords = append(sponsorRecords, sponsorRecord)
		}

		// Insert sponsors data into ClickHouse
		if len(sponsorRecords) > 0 {
			log.Printf("Sponsors chunk %d: Attempting to insert %d records into event_sponsors_ch...", chunkNum, len(sponsorRecords))

			sponsorInsertErr := retryWithBackoff(
				func() error {
					return insertSponsorsDataIntoClickHouse(clickhouseConn, sponsorRecords, config.ClickHouseWorkers)
				},
				3,
				fmt.Sprintf("sponsors insertion for chunk %d", chunkNum),
			)

			if sponsorInsertErr != nil {
				log.Printf("Sponsors chunk %d: Insertion failed after retries: %v", chunkNum, sponsorInsertErr)
				results <- fmt.Sprintf("Sponsors chunk %d: Failed to insert %d records", chunkNum, len(sponsorRecords))
				return
			} else {
				log.Printf("Sponsors chunk %d: Successfully inserted %d records into event_sponsors_ch", chunkNum, len(sponsorRecords))
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

	results <- fmt.Sprintf("Sponsors chunk %d: Completed successfully", chunkNum)
}

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
				id, website, country, city, facebook_id, linkedin_id, twitter_id
			FROM company 
			WHERE id IN (%s)`, strings.Join(placeholders, ","))

		rows, err := db.Query(query, args...)
		if err != nil {
			log.Printf("Error fetching sponsors company data batch %d-%d: %v", i, end-1, err)
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

func insertSponsorsDataIntoClickHouse(clickhouseConn driver.Conn, sponsorRecords []SponsorRecord, numWorkers int) error {
	if len(sponsorRecords) == 0 {
		return nil
	}

	if numWorkers <= 1 {
		return insertSponsorsDataSingleWorker(clickhouseConn, sponsorRecords)
	}

	batchSize := (len(sponsorRecords) + numWorkers - 1) / numWorkers
	results := make(chan error, numWorkers)
	semaphore := make(chan struct{}, numWorkers)

	for i := 0; i < numWorkers; i++ {
		start := i * batchSize
		end := start + batchSize
		if end > len(sponsorRecords) {
			end = len(sponsorRecords)
		}
		if start >= len(sponsorRecords) {
			break
		}

		semaphore <- struct{}{}
		go func(start, end int) {
			defer func() { <-semaphore }()
			batch := sponsorRecords[start:end]
			err := insertSponsorsDataSingleWorker(clickhouseConn, batch)
			results <- err
		}(start, end)
	}

	for i := 0; i < numWorkers && i*batchSize < len(sponsorRecords); i++ {
		if err := <-results; err != nil {
			return err
		}
	}

	return nil
}

func insertSponsorsDataSingleWorker(clickhouseConn driver.Conn, sponsorRecords []SponsorRecord) error {
	if len(sponsorRecords) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	batch, err := clickhouseConn.PrepareBatch(ctx, `
		INSERT INTO event_sponsors_ch (
			company_id, company_id_name, edition_id, event_id, company_website,
			company_domain, company_country, company_city, company_city_name, facebook_id,
			linkedin_id, twitter_id, version
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare ClickHouse batch: %v", err)
	}

	for _, record := range sponsorRecords {
		err := batch.Append(
			record.CompanyID,       // company_id: Nullable(UInt32)
			record.CompanyIDName,   // company_id_name: String NOT NULL
			record.EditionID,       // edition_id: UInt32 NOT NULL
			record.EventID,         // event_id: UInt32 NOT NULL
			record.CompanyWebsite,  // company_website: Nullable(String)
			record.CompanyDomain,   // company_domain: Nullable(String)
			record.CompanyCountry,  // company_country: LowCardinality(FixedString(2))
			record.CompanyCity,     // company_city: Nullable(UInt32)
			record.CompanyCityName, // company_city_name: LowCardinality(Nullable(String))
			record.FacebookID,      // facebook_id: Nullable(String)
			record.LinkedinID,      // linkedin_id: Nullable(String)
			record.TwitterID,       // twitter_id: Nullable(String)
			record.Version,         // version: UInt32 NOT NULL DEFAULT 1
		)
		if err != nil {
			return fmt.Errorf("failed to append record to batch: %v", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send ClickHouse batch: %v", err)
	}

	log.Printf("OK: Successfully inserted %d sponsor records", len(sponsorRecords))
	return nil
}

func processVisitorsOnly(mysqlDB *sql.DB, clickhouseConn driver.Conn, config Config) {
	log.Println("=== Starting VISITORS ONLY Processing ===")

	totalRecords, minID, maxID, err := getTotalRecordsAndIDRange(mysqlDB, "event_visitor")
	if err != nil {
		log.Fatal("Failed to get total records and ID range from event_visitor:", err)
	}

	log.Printf("Total visitors records: %d, Min ID: %d, Max ID: %d", totalRecords, minID, maxID)

	if config.NumChunks <= 0 {
		config.NumChunks = 5 // Default to 5 chunks if not specified
	}

	chunkSize := (maxID - minID + 1) / config.NumChunks
	if chunkSize == 0 {
		chunkSize = 1
	}

	log.Printf("Processing visitors data in %d chunks with chunk size: %d", config.NumChunks, chunkSize)

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
			log.Printf("Waiting %v before launching visitors chunk %d...", delay, i+1)
			time.Sleep(delay)
		}

		semaphore <- struct{}{}
		go func(chunkNum, start, end int) {
			defer func() { <-semaphore }()
			processVisitorsChunk(mysqlDB, clickhouseConn, config, start, end, chunkNum, results)
		}(i+1, startID, endID)
	}

	for i := 0; i < config.NumChunks; i++ {
		result := <-results
		log.Printf("Visitors Result: %s", result)
	}

	log.Println("Visitors processing completed!")
}

// processes a single chunk of visitors data
func processVisitorsChunk(mysqlDB *sql.DB, _ driver.Conn, config Config, startID, endID int, chunkNum int, results chan<- string) {
	log.Printf("Processing visitors chunk %d: ID range %d-%d", chunkNum, startID, endID)

	// Create a dedicated ClickHouse connection for this goroutine
	chConn, err := setupNativeClickHouseConnection(config)
	if err != nil {
		log.Printf("Visitors chunk %d: Failed to create ClickHouse connection: %v", chunkNum, err)
		results <- fmt.Sprintf("Visitors chunk %d: Failed to create ClickHouse connection: %v", chunkNum, err)
		return
	}
	defer chConn.Close()

	totalRecords := endID - startID + 1
	processed := 0

	// Use batching within the chunk
	offset := 0
	for {
		log.Printf("Visitors chunk %d: Fetching batch starting from ID %d (range %d-%d)", chunkNum, startID, startID, endID)

		batchData, err := buildVisitorsMigrationData(mysqlDB, startID, endID, config.BatchSize)
		if err != nil {
			log.Printf("ERROR: Visitors chunk %d failed to build migration data: %v", chunkNum, err)
			results <- fmt.Sprintf("Visitors chunk %d batch error: %v", chunkNum, err)
			return
		}

		if len(batchData) == 0 {
			log.Printf("Visitors chunk %d: No more data to process, breaking loop", chunkNum)
			break
		}

		log.Printf("Visitors chunk %d: Successfully retrieved %d records from MySQL", chunkNum, len(batchData))

		processed += len(batchData)
		progress := float64(processed) / float64(totalRecords) * 100
		log.Printf("Visitors chunk %d: Retrieved %d records in batch (%.1f%% complete)", chunkNum, len(batchData), progress)

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
			log.Printf("Visitors chunk %d: Fetching user data for %d users", chunkNum, len(userIDs))
			startTime := time.Now()
			userData = fetchVisitorsUserData(mysqlDB, userIDs)
			userTime := time.Since(startTime)
			log.Printf("Visitors chunk %d: Retrieved user data for %d users in %v", chunkNum, len(userData), userTime)

		}

		// Collect city IDs from visitor data
		var visitorCityIDs []int64
		seenCityIDs := make(map[int64]bool)
		for _, visitor := range batchData {
			if cityID, ok := visitor["visitor_city"].(int64); ok && cityID > 0 {
				if !seenCityIDs[cityID] {
					visitorCityIDs = append(visitorCityIDs, cityID)
					seenCityIDs[cityID] = true
				}
			}
		}

		// Fetch city data for visitor cities
		var cityData []map[string]interface{}
		var cityLookup map[int64]map[string]interface{}
		if len(visitorCityIDs) > 0 {
			log.Printf("Visitors chunk %d: Fetching city data for %d cities", chunkNum, len(visitorCityIDs))
			startTime := time.Now()
			cityData = fetchCityDataParallel(mysqlDB, visitorCityIDs, config.NumWorkers)
			cityTime := time.Since(startTime)
			log.Printf("Visitors chunk %d: Retrieved city data for %d cities in %v", chunkNum, len(cityData), cityTime)

			// Create city lookup map
			cityLookup = make(map[int64]map[string]interface{})
			if len(cityData) > 0 {
				for _, city := range cityData {
					if cityID, ok := city["id"].(int64); ok {
						cityLookup[cityID] = city
					}
				}
			}
		}

		var visitorRecords []VisitorRecord
		for _, visitor := range batchData {
			var userName, userCompany interface{}
			if userID, ok := visitor["user"].(int64); ok && userData != nil && userID > 0 {
				if user, exists := userData[userID]; exists {
					userName = user["name"]
					userCompany = user["user_company"]
					if userName == nil || convertToString(userName) == "" {
						userName = "-----DEFAULT USER NAME-----"
					}
				} else {
					userName = "-----DEFAULT USER NAME-----"
					userCompany = visitor["visitor_company"]
				}
			} else {
				userName = "-----DEFAULT USER NAME-----"
				userCompany = visitor["visitor_company"]
			}

			var userCityName *string
			if cityID, ok := visitor["visitor_city"].(int64); ok && cityLookup != nil {
				if city, exists := cityLookup[cityID]; exists && city["name"] != nil {
					nameStr := safeConvertToString(city["name"])
					userCityName = &nameStr
				}
			}

			userID := convertToUInt32(visitor["user"])
			eventID := convertToUInt32(visitor["event"])
			editionID := convertToUInt32(visitor["edition"])

			convertedUserName := convertToString(userName)

			visitorRecord := VisitorRecord{
				UserID:          userID,
				EventID:         eventID,
				EditionID:       editionID,
				UserName:        convertedUserName,
				UserCompany:     convertToStringPtr(userCompany),
				UserDesignation: convertToStringPtr(visitor["visitor_designation"]),
				UserCity:        convertToUInt32Ptr(visitor["visitor_city"]),
				UserCityName:    userCityName,
				UserCountry:     toUpperNullableString(convertToStringPtr(visitor["visitor_country"])),
				Version:         1,
			}

			visitorRecords = append(visitorRecords, visitorRecord)
		}

		if len(visitorRecords) > 0 {
			log.Printf("Visitors chunk %d: Attempting to insert %d records into event_visitor_ch...", chunkNum, len(visitorRecords))

			visitorInsertErr := retryWithBackoff(
				func() error {
					return insertVisitorsDataIntoClickHouse(chConn, visitorRecords, config.ClickHouseWorkers)
				},
				3,
				fmt.Sprintf("visitors insertion for chunk %d", chunkNum),
			)

			if visitorInsertErr != nil {
				log.Printf("Visitors chunk %d: Insertion failed after retries: %v", chunkNum, visitorInsertErr)
				results <- fmt.Sprintf("Visitors chunk %d: Failed to insert %d records", chunkNum, len(visitorRecords))
				return
			} else {
				log.Printf("Visitors chunk %d: Successfully inserted %d records into event_visitor_ch", chunkNum, len(visitorRecords))
			}
		}

		if len(batchData) > 0 {
			lastID := batchData[len(batchData)-1]["id"]
			if lastID != nil {
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

	results <- fmt.Sprintf("Visitors chunk %d: Completed successfully", chunkNum)
}

func buildVisitorsMigrationData(db *sql.DB, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	if startID < 0 || endID < 0 || startID > endID {
		return nil, fmt.Errorf("invalid ID range: startID=%d, endID=%d", startID, endID)
	}

	if batchSize <= 0 {
		return nil, fmt.Errorf("invalid batch size: %d", batchSize)
	}

	log.Printf("Building visitors migration data: ID range %d-%d, batch size %d", startID, endID, batchSize)

	query := fmt.Sprintf(`
		SELECT 
			id, user, event, edition, visitor_company, visitor_designation, visitor_city, visitor_country
		FROM event_visitor 
		WHERE id >= %d AND id <= %d 
		ORDER BY id 
		LIMIT %d`, startID, endID, batchSize)

	log.Printf("Executing query: %s", query)

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get column names: %v", err)
	}

	log.Printf("Query returned columns: %v", columns)

	var results []map[string]interface{}
	rowCount := 0

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row %d: %v", rowCount, err)
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

		if row["id"] == nil {
			log.Printf("WARNING: Row %d has null ID", rowCount)
		}
		if row["user"] == nil {
			log.Printf("WARNING: Row %d has null user", rowCount)
		}
		if row["event"] == nil {
			log.Printf("WARNING: Row %d has null event", rowCount)
		}
		if row["edition"] == nil {
			log.Printf("WARNING: Row %d has null edition", rowCount)
		}

		results = append(results, row)
		rowCount++
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during row iteration: %v", err)
	}

	log.Printf("Successfully retrieved %d visitor records from MySQL", len(results))
	return results, nil
}

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
			log.Printf("Error fetching visitors user data batch %d-%d: %v", i, end-1, err)
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

func insertVisitorsDataIntoClickHouse(clickhouseConn driver.Conn, visitorRecords []VisitorRecord, numWorkers int) error {
	if len(visitorRecords) == 0 {
		log.Printf("WARNING: No visitor records provided for insertion")
		return nil
	}

	if numWorkers <= 0 {
		log.Printf("WARNING: Invalid numWorkers (%d), defaulting to 1", numWorkers)
		numWorkers = 1
	}

	if numWorkers > len(visitorRecords) {
		log.Printf("WARNING: numWorkers (%d) exceeds record count (%d), reducing to %d",
			numWorkers, len(visitorRecords), len(visitorRecords))
		numWorkers = len(visitorRecords)
	}

	log.Printf("Inserting %d visitor records using %d workers", len(visitorRecords), numWorkers)

	if numWorkers <= 1 {
		return insertVisitorsDataSingleWorker(clickhouseConn, visitorRecords)
	}

	batchSize := (len(visitorRecords) + numWorkers - 1) / numWorkers
	log.Printf("Batch size per worker: %d records", batchSize)

	if batchSize == 0 {
		return fmt.Errorf("calculated batch size is 0 for %d records with %d workers", len(visitorRecords), numWorkers)
	}

	results := make(chan error, numWorkers)
	semaphore := make(chan struct{}, numWorkers)
	activeWorkers := 0

	for i := 0; i < numWorkers; i++ {
		start := i * batchSize
		end := start + batchSize
		if end > len(visitorRecords) {
			end = len(visitorRecords)
		}
		if start >= len(visitorRecords) {
			break
		}

		batch := visitorRecords[start:end]
		if len(batch) == 0 {
			log.Printf("WARNING: Empty batch for worker %d (start: %d, end: %d)", i, start, end)
			continue
		}

		activeWorkers++
		semaphore <- struct{}{}
		go func(workerID, start, end int) {
			defer func() { <-semaphore }()
			batch := visitorRecords[start:end]
			log.Printf("Worker %d processing batch: %d records (indices %d-%d)", workerID, len(batch), start, end-1)

			err := insertVisitorsDataSingleWorker(clickhouseConn, batch)
			if err != nil {
				log.Printf("Worker %d failed: %v", workerID, err)
			} else {
				log.Printf("Worker %d completed successfully", workerID)
			}
			results <- err
		}(i+1, start, end)
	}

	var lastError error
	for i := 0; i < activeWorkers; i++ {
		if err := <-results; err != nil {
			lastError = err
			log.Printf("Worker %d failed with error: %v", i+1, err)
		}
	}

	if lastError != nil {
		return fmt.Errorf("one or more workers failed during insertion. Last error: %v", lastError)
	}

	log.Printf("All %d workers completed successfully", activeWorkers)
	return nil
}

func insertVisitorsDataSingleWorker(clickhouseConn driver.Conn, visitorRecords []VisitorRecord) error {
	if len(visitorRecords) == 0 {
		log.Printf("WARNING: No visitor records to insert")
		return nil
	}

	log.Printf("Starting ClickHouse insertion for %d visitor records", len(visitorRecords))

	for i, record := range visitorRecords {
		if record.EventID == 0 {
			log.Printf("ERROR: Record details - UserID: %d, EventID: %d, EditionID: %d, UserName: '%s'",
				record.UserID, record.EventID, record.EditionID, record.UserName)
			return fmt.Errorf("invalid visitor record at index %d: EventID is 0", i)
		}
		if record.EditionID == 0 {
			log.Printf("ERROR: Record details - UserID: %d, EventID: %d, EditionID: %d, UserName: '%s'",
				record.UserID, record.EventID, record.EditionID, record.UserName)
			return fmt.Errorf("invalid visitor record at index %d: EditionID is 0", i)
		}
		if record.UserName == "" {
			log.Printf("ERROR: Record details - UserID: %d, EventID: %d, EditionID: %d, UserName: '%s'",
				record.UserID, record.EventID, record.EditionID, record.UserName)
			return fmt.Errorf("invalid visitor record at index %d: UserName is empty", i)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := clickhouseConn.Ping(ctx); err != nil {
		log.Printf("ERROR: ClickHouse connection ping failed for visitors insertion")
		return fmt.Errorf("ClickHouse connection ping failed for event_visitors_ch_v2: %v", err)
	}

	batch, err := clickhouseConn.PrepareBatch(ctx, `
		INSERT INTO event_visitors_ch (
			user_id, event_id, edition_id, user_name, user_company,
			user_designation, user_city, user_city_name, user_country, version
		)
	`)
	if err != nil {
		log.Printf("ERROR: Failed to prepare ClickHouse batch for visitors table: %v", err)
		return fmt.Errorf("failed to prepare ClickHouse batch for event_visitors_ch_v2: %v", err)
	}

	log.Printf("ClickHouse batch prepared successfully, appending %d records", len(visitorRecords))

	for i, record := range visitorRecords {
		err := batch.Append(
			record.UserID,          // user_id: UInt32 NOT NULL
			record.EventID,         // event_id: UInt32 NOT NULL
			record.EditionID,       // edition_id: UInt32 NOT NULL
			record.UserName,        // user_name: String NOT NULL
			record.UserCompany,     // user_company: Nullable(String)
			record.UserDesignation, // user_designation: Nullable(String)
			record.UserCity,        // user_city: Nullable(UInt32)
			record.UserCityName,    // user_city_name: LowCardinality(Nullable(String))
			record.UserCountry,     // user_country: LowCardinality(Nullable(FixedString(2)))
			record.Version,         // version: UInt32 NOT NULL DEFAULT 1
		)
		if err != nil {
			return fmt.Errorf("failed to append visitor record %d to batch (UserID: %d, EventID: %d, EditionID: %d): %v",
				i, record.UserID, record.EventID, record.EditionID, err)
		}
	}

	log.Printf("All %d records appended to batch, sending to ClickHouse...", len(visitorRecords))

	if err := batch.Send(); err != nil {
		log.Printf("ERROR: Failed to send ClickHouse batch for visitors")
		log.Printf("ERROR: Table: event_visitors_ch_v2")
		log.Printf("ERROR: Records count: %d", len(visitorRecords))
		log.Printf("ERROR: Send error: %v", err)
		log.Printf("ERROR: Error type: %T", err)
		return fmt.Errorf("failed to send ClickHouse batch to event_visitors_ch_v2: %v", err)
	}

	log.Printf("OK: Successfully inserted %d visitor records", len(visitorRecords))
	return nil
}

func processSpeakersOnly(mysqlDB *sql.DB, clickhouseConn driver.Conn, config Config) {
	log.Println("=== Starting SPEAKERS ONLY Processing ===")

	totalRecords, minID, maxID, err := getTotalRecordsAndIDRange(mysqlDB, "event_speaker")
	if err != nil {
		log.Fatal("Failed to get total records and ID range from event_speaker:", err)
	}

	log.Printf("Total speakers records: %d, Min ID: %d, Max ID: %d", totalRecords, minID, maxID)

	if config.NumChunks <= 0 {
		config.NumChunks = 5
	}

	chunkSize := (maxID - minID + 1) / config.NumChunks
	if chunkSize == 0 {
		chunkSize = 1
	}

	log.Printf("Processing speakers data in %d chunks with chunk size: %d", config.NumChunks, chunkSize)

	results := make(chan string, config.NumChunks)
	semaphore := make(chan struct{}, config.NumWorkers)

	for i := 0; i < config.NumChunks; i++ {
		startID := minID + (i * chunkSize)
		endID := startID + chunkSize - 1

		if i == config.NumChunks-1 {
			endID = maxID
		}

		if i > 0 {
			delay := 3 * time.Second
			log.Printf("Waiting %v before launching speakers chunk %d...", delay, i+1)
			time.Sleep(delay)
		}

		semaphore <- struct{}{}
		go func(chunkNum, start, end int) {
			defer func() { <-semaphore }()
			processSpeakersChunk(mysqlDB, clickhouseConn, config, start, end, chunkNum, results)
		}(i+1, startID, endID)
	}

	for i := 0; i < config.NumChunks; i++ {
		result := <-results
		log.Printf("Speakers Result: %s", result)
	}

	log.Println("Speakers processing completed!")
}

func processEventTypeEventChOnly(mysqlDB *sql.DB, clickhouseConn driver.Conn, config Config) {
	log.Println("=== Starting event_type_ch ONLY Processing ===")

	totalRecords, minID, maxID, err := getTotalRecordsAndIDRange(mysqlDB, "event_type_event")
	if err != nil {
		log.Fatal("Failed to get total records and ID range from event_type_event:", err)
	}

	log.Printf("Total event_type_event records: %d, Min ID: %d, Max ID: %d", totalRecords, minID, maxID)

	if config.NumChunks <= 0 {
		config.NumChunks = 5
	}

	chunkSize := (maxID - minID + 1) / config.NumChunks
	if chunkSize == 0 {
		chunkSize = 1
	}

	log.Printf("Processing event_type_ch data in %d chunks with chunk size: %d", config.NumChunks, chunkSize)

	results := make(chan string, config.NumChunks)
	semaphore := make(chan struct{}, config.NumWorkers)

	for i := 0; i < config.NumChunks; i++ {
		startID := minID + (i * chunkSize)
		endID := startID + chunkSize - 1

		if i == config.NumChunks-1 {
			endID = maxID
		}

		if i > 0 {
			delay := 3 * time.Second
			log.Printf("Waiting %v before launching event_type_ch chunk %d...", delay, i+1)
			time.Sleep(delay)
		}

		semaphore <- struct{}{}
		go func(chunkNum, start, end int) {
			defer func() { <-semaphore }()
			processEventTypeEventChChunk(mysqlDB, clickhouseConn, config, start, end, chunkNum, results)
		}(i+1, startID, endID)
	}

	for i := 0; i < config.NumChunks; i++ {
		result := <-results
		log.Printf("EventTypeEventCh Result: %s", result)
	}

	log.Println("EventTypeEventCh processing completed!")
}

func processSpeakersChunk(mysqlDB *sql.DB, clickhouseConn driver.Conn, config Config, startID, endID int, chunkNum int, results chan<- string) {
	log.Printf("Processing speakers chunk %d: ID range %d-%d", chunkNum, startID, endID)

	totalRecords := endID - startID + 1
	processed := 0

	offset := 0
	for {
		batchData, err := buildSpeakersMigrationData(mysqlDB, startID, endID, config.BatchSize)
		if err != nil {
			results <- fmt.Sprintf("Speakers chunk %d batch error: %v", chunkNum, err)
			return
		}

		if len(batchData) == 0 {
			break
		}

		processed += len(batchData)
		progress := float64(processed) / float64(totalRecords) * 100
		log.Printf("Speakers chunk %d: Retrieved %d records in batch (%.1f%% complete)", chunkNum, len(batchData), progress)

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

		var userData map[int64]map[string]interface{}
		if len(userIDs) > 0 {
			log.Printf("Speakers chunk %d: Fetching user data for %d users", chunkNum, len(userIDs))
			startTime := time.Now()
			userData = fetchSpeakersUserData(mysqlDB, userIDs)
			userTime := time.Since(startTime)
			log.Printf("Speakers chunk %d: Retrieved user data for %d users in %v", chunkNum, len(userData), userTime)
		}

		var speakerCityIDs []int64
		seenCityIDs := make(map[int64]bool)
		for _, user := range userData {
			if cityID, ok := user["city"].(int64); ok && cityID > 0 {
				if !seenCityIDs[cityID] {
					speakerCityIDs = append(speakerCityIDs, cityID)
					seenCityIDs[cityID] = true
				}
			}
		}

		var cityData []map[string]interface{}
		var cityLookup map[int64]map[string]interface{}
		if len(speakerCityIDs) > 0 {
			log.Printf("Speakers chunk %d: Fetching city data for %d cities", chunkNum, len(speakerCityIDs))
			startTime := time.Now()
			cityData = fetchCityDataParallel(mysqlDB, speakerCityIDs, config.NumWorkers)
			cityTime := time.Since(startTime)
			log.Printf("Speakers chunk %d: Retrieved city data for %d cities in %v", chunkNum, len(cityData), cityTime)

			cityLookup = make(map[int64]map[string]interface{})
			if len(cityData) > 0 {
				for _, city := range cityData {
					if cityID, ok := city["id"].(int64); ok {
						cityLookup[cityID] = city
					}
				}
			}
		}

		var speakerRecords []SpeakerRecord
		for _, speaker := range batchData {
			// Get user data for this speaker
			var userName, userCompany, userDesignation, userCity, userCountry interface{}
			if userID, ok := speaker["user_id"].(int64); ok && userData != nil {
				if user, exists := userData[userID]; exists {
					userName = speaker["speaker_name"] // Use speaker_name from speaker table
					userCompany = user["user_company"] // Use user_company from user table
					userDesignation = user["designation"]
					userCity = user["city"]
					userCountry = strings.ToUpper(safeConvertToString(user["country"]))
				}
			}

			// Get city data for this speaker
			var userCityName *string
			if userCity != nil {
				if cityID, ok := userCity.(int64); ok && cityLookup != nil {
					if city, exists := cityLookup[cityID]; exists && city["name"] != nil {
						nameStr := safeConvertToString(city["name"])
						userCityName = &nameStr
					}
				}
			}

			userID := convertToUInt32(speaker["user_id"])
			eventID := convertToUInt32(speaker["event"])
			editionID := convertToUInt32(speaker["edition"])

			speakerRecord := SpeakerRecord{
				UserID:          userID,
				EventID:         eventID,
				EditionID:       editionID,
				UserName:        convertToString(userName),
				UserCompany:     convertToStringPtr(userCompany),
				UserDesignation: convertToStringPtr(userDesignation),
				UserCity:        convertToUInt32Ptr(userCity),
				UserCityName:    userCityName,
				UserCountry:     toUpperNullableString(convertToStringPtr(userCountry)),
				Version:         1,
			}

			speakerRecords = append(speakerRecords, speakerRecord)
		}

		// Insert speakers data into ClickHouse
		if len(speakerRecords) > 0 {
			log.Printf("Speakers chunk %d: Attempting to insert %d records into event_speaker_ch...", chunkNum, len(speakerRecords))

			speakerInsertErr := retryWithBackoff(
				func() error {
					return insertSpeakersDataIntoClickHouse(clickhouseConn, speakerRecords, config.ClickHouseWorkers)
				},
				3,
				fmt.Sprintf("speakers insertion for chunk %d", chunkNum),
			)

			if speakerInsertErr != nil {
				log.Printf("Speakers chunk %d: Insertion failed after retries: %v", chunkNum, speakerInsertErr)
				results <- fmt.Sprintf("Speakers chunk %d: Failed to insert %d records", chunkNum, len(speakerRecords))
				return
			} else {
				log.Printf("Speakers chunk %d: Successfully inserted %d records into event_speaker_ch", chunkNum, len(speakerRecords))
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

	results <- fmt.Sprintf("Speakers chunk %d: Completed successfully", chunkNum)
}

// processes a single chunk of event_type_ch data
func processEventTypeEventChChunk(mysqlDB *sql.DB, clickhouseConn driver.Conn, config Config, startID, endID int, chunkNum int, results chan<- string) {
	log.Printf("Processing event_type_ch chunk %d: ID range %d-%d", chunkNum, startID, endID)

	totalRecords := endID - startID + 1
	processed := 0

	// Use batching within the chunk
	offset := 0
	for {
		batchData, err := buildEventTypeEventChMigrationData(mysqlDB, startID, endID, config.BatchSize)
		if err != nil {
			results <- fmt.Sprintf("EventTypeEventCh chunk %d batch error: %v", chunkNum, err)
			return
		}

		if len(batchData) == 0 {
			break
		}

		processed += len(batchData)
		progress := float64(processed) / float64(totalRecords) * 100
		log.Printf("EventTypeEventCh chunk %d: Retrieved %d records in batch (%.1f%% complete)", chunkNum, len(batchData), progress)

		var eventTypeEventChRecords []EventTypeEventChRecord
		for _, record := range batchData {
			eventTypeEventChRecord := EventTypeEventChRecord{
				EventTypeID:   convertToUInt32(record["eventtype_id"]),
				EventID:       convertToUInt32(record["event_id"]),
				Published:     convertToInt8(record["published"]),
				Name:          convertToString(record["name"]),
				URL:           convertToString(record["url"]),
				EventAudience: safeConvertToUInt16(record["event_audience"]),
				Version:       1,
			}

			eventTypeEventChRecords = append(eventTypeEventChRecords, eventTypeEventChRecord)
		}

		// Insert event_type_ch data into ClickHouse
		if len(eventTypeEventChRecords) > 0 {
			log.Printf("EventTypeEventCh chunk %d: Attempting to insert %d records into event_type_ch...", chunkNum, len(eventTypeEventChRecords))

			insertErr := retryWithBackoff(
				func() error {
					return insertEventTypeEventChDataIntoClickHouse(clickhouseConn, eventTypeEventChRecords, config.ClickHouseWorkers)
				},
				3,
				fmt.Sprintf("event_type_ch insertion for chunk %d", chunkNum),
			)

			if insertErr != nil {
				log.Printf("EventTypeEventCh chunk %d: Insertion failed after retries: %v", chunkNum, insertErr)
				results <- fmt.Sprintf("EventTypeEventCh chunk %d: Failed to insert %d records", chunkNum, len(eventTypeEventChRecords))
				return
			} else {
				log.Printf("EventTypeEventCh chunk %d: Successfully inserted %d records into event_type_ch", chunkNum, len(eventTypeEventChRecords))
			}
		}

		// Update startID for next batch within this chunk (following the same pattern as other working tables)
		if len(batchData) > 0 {
			// Get the last record's ID from the batch and increment it for the next batch
			lastRecord := batchData[len(batchData)-1]
			if lastID, ok := lastRecord["id"].(int64); ok {
				startID = int(lastID) + 1
			} else if lastID, ok := lastRecord["id"].(int32); ok {
				startID = int(lastID) + 1
			} else if lastID, ok := lastRecord["id"].(int); ok {
				startID = lastID + 1
			}
		}

		offset += len(batchData)
		if len(batchData) < config.BatchSize {
			break
		}
	}

	results <- fmt.Sprintf("EventTypeEventCh chunk %d: Completed successfully", chunkNum)
}

func buildSpeakersMigrationData(db *sql.DB, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT 
			id, user_id, event, edition, speaker_name
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

func buildEventTypeEventChMigrationData(db *sql.DB, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT 
			ee.id,
			ee.eventtype_id,
			ee.event_id,
			ee.published,
			et.name,
			et.url,
			et.event_audience
		FROM event_type_event ee
		INNER JOIN event_type et ON ee.eventtype_id = et.id
		WHERE ee.id >= %d AND ee.id <= %d 
		ORDER BY ee.id 
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

// Event Category Event Ch Processing Functions

func processEventCategoryEventChOnly(mysqlDB *sql.DB, clickhouseConn driver.Conn, config Config) {
	log.Println("=== Starting event_category_ch ONLY Processing ===")

	totalRecords, minID, maxID, err := getTotalRecordsAndIDRange(mysqlDB, "event_category")
	if err != nil {
		log.Fatal("Failed to get total records and ID range from event_category:", err)
	}

	log.Printf("Total event_category records: %d, Min ID: %d, Max ID: %d", totalRecords, minID, maxID)

	if config.NumChunks <= 0 {
		config.NumChunks = 5 // Default to 5 chunks if not specified
	}

	chunkSize := (maxID - minID + 1) / config.NumChunks
	if chunkSize == 0 {
		chunkSize = 1
	}

	log.Printf("Processing event_category_ch data in %d chunks with chunk size: %d", config.NumChunks, chunkSize)

	results := make(chan string, config.NumChunks)
	semaphore := make(chan struct{}, config.NumWorkers)

	for i := 0; i < config.NumChunks; i++ {
		startID := minID + (i * chunkSize)
		endID := startID + chunkSize - 1
		if i == config.NumChunks-1 {
			endID = maxID // Last chunk gets remaining records
		}

		// delay between chunk launches to reduce ClickHouse load
		if i > 0 {
			delay := 3 * time.Second
			log.Printf("Waiting %v before launching event_category_ch chunk %d...", delay, i+1)
			time.Sleep(delay)
		}

		semaphore <- struct{}{}
		go func(chunkNum, start, end int) {
			defer func() { <-semaphore }()
			processEventCategoryEventChChunk(mysqlDB, clickhouseConn, config, start, end, chunkNum, results)
		}(i+1, startID, endID)
	}

	for i := 0; i < config.NumChunks; i++ {
		result := <-results
		log.Printf("EventCategoryEventCh Result: %s", result)
	}

	log.Println("EventCategoryEventCh processing completed!")
}

// processes a single chunk of event_category_ch data
func processEventCategoryEventChChunk(mysqlDB *sql.DB, clickhouseConn driver.Conn, config Config, startID, endID int, chunkNum int, results chan<- string) {
	log.Printf("Processing event_category_ch chunk %d: ID range %d-%d", chunkNum, startID, endID)

	totalRecords := endID - startID + 1
	processed := 0

	// Use batching within the chunk
	offset := 0
	for {
		batchData, err := buildEventCategoryEventChMigrationData(mysqlDB, startID, endID, config.BatchSize)
		if err != nil {
			results <- fmt.Sprintf("EventCategoryEventCh chunk %d batch error: %v", chunkNum, err)
			return
		}

		if len(batchData) == 0 {
			break
		}

		processed += len(batchData)
		progress := float64(processed) / float64(totalRecords) * 100
		log.Printf("EventCategoryEventCh chunk %d: Retrieved %d records in batch (%.1f%% complete)", chunkNum, len(batchData), progress)

		var eventCategoryEventChRecords []EventCategoryEventChRecord
		for _, record := range batchData {
			eventCategoryEventChRecord := EventCategoryEventChRecord{
				Category:  convertToUInt32(record["category"]),
				Event:     convertToUInt32(record["event"]),
				Name:      convertToString(record["name"]),
				URL:       convertToString(record["url"]),
				Published: convertToInt8(record["published"]),
				ShortName: convertToString(record["short_name"]),
				IsGroup:   convertToUInt8(record["is_group"]),
				Version:   1,
			}

			eventCategoryEventChRecords = append(eventCategoryEventChRecords, eventCategoryEventChRecord)
		}

		// Insert event_category_ch data into ClickHouse
		if len(eventCategoryEventChRecords) > 0 {
			log.Printf("EventCategoryEventCh chunk %d: Attempting to insert %d records into event_category_ch...", chunkNum, len(eventCategoryEventChRecords))

			insertErr := retryWithBackoff(
				func() error {
					return insertEventCategoryEventChDataIntoClickHouse(clickhouseConn, eventCategoryEventChRecords, config.ClickHouseWorkers)
				},
				3,
				fmt.Sprintf("event_category_ch insertion for chunk %d", chunkNum),
			)

			if insertErr != nil {
				log.Printf("EventCategoryEventCh chunk %d: Insertion failed after retries: %v", chunkNum, insertErr)
				results <- fmt.Sprintf("EventCategoryEventCh chunk %d: Failed to insert %d records", chunkNum, len(eventCategoryEventChRecords))
				return
			} else {
				log.Printf("EventCategoryEventCh chunk %d: Successfully inserted %d records into event_category_ch", chunkNum, len(eventCategoryEventChRecords))
			}
		}

		// Update startID for next batch within this chunk (following the same pattern as other working tables)
		if len(batchData) > 0 {
			// Get the last record's ID from the batch and increment it for the next batch
			lastRecord := batchData[len(batchData)-1]
			if lastID, ok := lastRecord["id"].(int64); ok {
				startID = int(lastID) + 1
			} else if lastID, ok := lastRecord["id"].(int32); ok {
				startID = int(lastID) + 1
			} else if lastID, ok := lastRecord["id"].(int); ok {
				startID = lastID + 1
			}
		}

		offset += len(batchData)
		if len(batchData) < config.BatchSize {
			break
		}
	}

	results <- fmt.Sprintf("EventCategoryEventCh chunk %d: Completed successfully", chunkNum)
}

func buildEventCategoryEventChMigrationData(db *sql.DB, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT 
			ec.id,
			ec.category,
			ec.event,
			c.published,
			c.name,
			c.url,
			c.short_name,
			c.is_group
		FROM event_category ec
		INNER JOIN category c ON ec.category = c.id
		WHERE ec.id >= %d AND ec.id <= %d 
		ORDER BY ec.id 
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

func insertEventCategoryEventChDataIntoClickHouse(clickhouseConn driver.Conn, eventCategoryEventChRecords []EventCategoryEventChRecord, numWorkers int) error {
	if len(eventCategoryEventChRecords) == 0 {
		return nil
	}

	if numWorkers <= 1 {
		return insertEventCategoryEventChDataSingleWorker(clickhouseConn, eventCategoryEventChRecords)
	}

	// Split records into batches for parallel processing
	batchSize := len(eventCategoryEventChRecords) / numWorkers
	if batchSize == 0 {
		batchSize = 1
	}

	results := make(chan error, numWorkers)
	semaphore := make(chan struct{}, numWorkers)

	for i := 0; i < numWorkers; i++ {
		start := i * batchSize
		end := start + batchSize
		if i == numWorkers-1 {
			end = len(eventCategoryEventChRecords) // Last worker gets remaining records
		}

		if start >= len(eventCategoryEventChRecords) {
			break
		}

		semaphore <- struct{}{}
		go func(start, end int) {
			defer func() { <-semaphore }()
			batch := eventCategoryEventChRecords[start:end]
			err := insertEventCategoryEventChDataSingleWorker(clickhouseConn, batch)
			results <- err
		}(start, end)
	}

	// Wait for all workers to complete
	for i := 0; i < numWorkers; i++ {
		if err := <-results; err != nil {
			return err
		}
	}

	return nil
}

func insertEventCategoryEventChDataSingleWorker(clickhouseConn driver.Conn, eventCategoryEventChRecords []EventCategoryEventChRecord) error {
	if len(eventCategoryEventChRecords) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	batch, err := clickhouseConn.PrepareBatch(ctx, `
		INSERT INTO event_category_ch (
			category, event, name, url, published, short_name, is_group, version
		)
	`)
	if err != nil {
		log.Printf("ERROR: Failed to prepare ClickHouse batch for event_category_ch: %v", err)
		return fmt.Errorf("failed to prepare ClickHouse batch: %v", err)
	}

	for _, record := range eventCategoryEventChRecords {
		err := batch.Append(
			record.Category,  // category: UInt32
			record.Event,     // event: UInt32
			record.Name,      // name: LowCardinality(String)
			record.URL,       // url: String
			record.Published, // published: Int8
			record.ShortName, // short_name: String
			record.IsGroup,   // is_group: UInt8
			record.Version,   // version: UInt32 DEFAULT 1
		)
		if err != nil {
			log.Printf("ERROR: Failed to append record to batch: %v", err)
			log.Printf("Record data: Category=%d, Event=%d, Name=%s, URL=%s, Published=%d, ShortName=%s, IsGroup=%d, Version=%d",
				record.Category, record.Event, record.Name, record.URL, record.Published, record.ShortName, record.IsGroup, record.Version)
			return fmt.Errorf("failed to append record to batch: %v", err)
		}
	}

	if err := batch.Send(); err != nil {
		log.Printf("ERROR: Failed to send ClickHouse batch: %v", err)
		return fmt.Errorf("failed to send ClickHouse batch: %v", err)
	}

	log.Printf("OK: Successfully inserted %d event_category_ch records", len(eventCategoryEventChRecords))
	return nil
}

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
			log.Printf("Error fetching speakers user data batch %d-%d: %v", i, end-1, err)
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

func insertSpeakersDataIntoClickHouse(clickhouseConn driver.Conn, speakerRecords []SpeakerRecord, numWorkers int) error {
	if len(speakerRecords) == 0 {
		return nil
	}

	if numWorkers <= 1 {
		return insertSpeakersDataSingleWorker(clickhouseConn, speakerRecords)
	}

	batchSize := (len(speakerRecords) + numWorkers - 1) / numWorkers
	results := make(chan error, numWorkers)
	semaphore := make(chan struct{}, numWorkers)

	for i := 0; i < numWorkers; i++ {
		start := i * batchSize
		end := start + batchSize
		if end > len(speakerRecords) {
			end = len(speakerRecords)
		}
		if start >= len(speakerRecords) {
			break
		}

		semaphore <- struct{}{}
		go func(start, end int) {
			defer func() { <-semaphore }()
			batch := speakerRecords[start:end]
			err := insertSpeakersDataSingleWorker(clickhouseConn, batch)
			results <- err
		}(start, end)
	}

	for i := 0; i < numWorkers && i*batchSize < len(speakerRecords); i++ {
		if err := <-results; err != nil {
			return err
		}
	}

	return nil
}

func insertSpeakersDataSingleWorker(clickhouseConn driver.Conn, speakerRecords []SpeakerRecord) error {
	if len(speakerRecords) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	batch, err := clickhouseConn.PrepareBatch(ctx, `
		INSERT INTO event_speaker_ch (
			user_id, event_id, edition_id, user_name, user_company,
			user_designation, user_city, user_city_name, user_country, version
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare ClickHouse batch: %v", err)
	}

	for _, record := range speakerRecords {
		err := batch.Append(
			record.UserID,          // user_id: UInt32 NOT NULL
			record.EventID,         // event_id: UInt32 NOT NULL
			record.EditionID,       // edition_id: UInt32 NOT NULL
			record.UserName,        // user_name: String NOT NULL
			record.UserCompany,     // user_company: Nullable(String)
			record.UserDesignation, // user_designation: Nullable(String)
			record.UserCity,        // user_city: Nullable(UInt32)
			record.UserCityName,    // user_city_name: LowCardinality(Nullable(String))
			record.UserCountry,     // user_country: LowCardinality(Nullable(FixedString(2)))
			record.Version,         // version: UInt32 NOT NULL DEFAULT 1
		)
		if err != nil {
			return fmt.Errorf("failed to append record to batch: %v", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send ClickHouse batch: %v", err)
	}

	log.Printf("OK: Successfully inserted %d speaker records", len(speakerRecords))
	return nil
}

func insertEventTypeEventChDataIntoClickHouse(clickhouseConn driver.Conn, eventTypeEventChRecords []EventTypeEventChRecord, numWorkers int) error {
	if len(eventTypeEventChRecords) == 0 {
		return nil
	}

	if numWorkers <= 1 {
		return insertEventTypeEventChDataSingleWorker(clickhouseConn, eventTypeEventChRecords)
	}

	batchSize := (len(eventTypeEventChRecords) + numWorkers - 1) / numWorkers
	results := make(chan error, numWorkers)
	semaphore := make(chan struct{}, numWorkers)

	for i := 0; i < numWorkers; i++ {
		start := i * batchSize
		end := start + batchSize
		if end > len(eventTypeEventChRecords) {
			end = len(eventTypeEventChRecords)
		}
		if start >= len(eventTypeEventChRecords) {
			break
		}

		semaphore <- struct{}{}
		go func(start, end int) {
			defer func() { <-semaphore }()
			batch := eventTypeEventChRecords[start:end]
			err := insertEventTypeEventChDataSingleWorker(clickhouseConn, batch)
			results <- err
		}(start, end)
	}

	for i := 0; i < numWorkers && i*batchSize < len(eventTypeEventChRecords); i++ {
		if err := <-results; err != nil {
			return err
		}
	}

	return nil
}

func insertEventTypeEventChDataSingleWorker(clickhouseConn driver.Conn, eventTypeEventChRecords []EventTypeEventChRecord) error {
	if len(eventTypeEventChRecords) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	batch, err := clickhouseConn.PrepareBatch(ctx, `
		INSERT INTO event_type_ch (
			eventtype_id, event_id, published, name, url, event_audience, version
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare ClickHouse batch: %v", err)
	}

	for _, record := range eventTypeEventChRecords {
		err := batch.Append(
			record.EventTypeID,   // eventtype_id: UInt32
			record.EventID,       // event_id: UInt32
			record.Published,     // published: Int8
			record.Name,          // name: LowCardinality(String)
			record.URL,           // url: String
			record.EventAudience, // event_audience: UInt16
			record.Version,       // version: UInt32 DEFAULT 1
		)
		if err != nil {
			return fmt.Errorf("failed to append record to batch: %v", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send ClickHouse batch: %v", err)
	}

	log.Printf("OK: Successfully inserted %d event_type_ch records", len(eventTypeEventChRecords))
	return nil
}

// represents a sponsor record for ClickHouse insertion
type SponsorRecord struct {
	CompanyID       *uint32 `ch:"company_id"`
	CompanyIDName   string  `ch:"company_id_name"`
	EditionID       uint32  `ch:"edition_id"`
	EventID         uint32  `ch:"event_id"`
	CompanyWebsite  *string `ch:"company_website"`
	CompanyDomain   *string `ch:"company_domain"`
	CompanyCountry  *string `ch:"company_country"`
	CompanyCity     *uint32 `ch:"company_city"`
	CompanyCityName *string `ch:"company_city_name"`
	FacebookID      *string `ch:"facebook_id"`
	LinkedinID      *string `ch:"linkedin_id"`
	TwitterID       *string `ch:"twitter_id"`
	Version         uint32  `ch:"version"`
}

// SpeakerRecord represents a speaker record for ClickHouse insertion
type SpeakerRecord struct {
	UserID          uint32  `ch:"user_id"`
	EventID         uint32  `ch:"event_id"`
	EditionID       uint32  `ch:"edition_id"`
	UserName        string  `ch:"user_name"`
	UserCompany     *string `ch:"user_company"`
	UserDesignation *string `ch:"user_designation"`
	UserCity        *uint32 `ch:"user_city"`
	UserCityName    *string `ch:"user_city_name"`
	UserCountry     *string `ch:"user_country"`
	Version         uint32  `ch:"version"`
}

// ExhibitorRecord represents an exhibitor record for ClickHouse insertion
type ExhibitorRecord struct {
	CompanyID       *uint32 `ch:"company_id"`
	CompanyIDName   string  `ch:"company_id_name"`
	EditionID       uint32  `ch:"edition_id"`
	EventID         uint32  `ch:"event_id"`
	CompanyWebsite  *string `ch:"company_website"`
	CompanyDomain   *string `ch:"company_domain"`
	CompanyCountry  *string `ch:"company_country"`
	CompanyCity     *uint32 `ch:"company_city"`
	CompanyCityName *string `ch:"company_city_name"`
	FacebookID      *string `ch:"facebook_id"`
	LinkedinID      *string `ch:"linkedin_id"`
	TwitterID       *string `ch:"twitter_id"`
	Version         uint32  `ch:"version"`
}

// VisitorRecord represents a visitor record for ClickHouse insertion
type VisitorRecord struct {
	UserID          uint32  `ch:"user_id"`
	EventID         uint32  `ch:"event_id"`
	EditionID       uint32  `ch:"edition_id"`
	UserName        string  `ch:"user_name"`
	UserCompany     *string `ch:"user_company"`
	UserDesignation *string `ch:"user_designation"`
	UserCity        *uint32 `ch:"user_city"`
	UserCityName    *string `ch:"user_city_name"`
	UserCountry     *string `ch:"user_country"`
	Version         uint32  `ch:"version"`
}

// EventEditionRecord represents an event edition record for ClickHouse insertion
type EventEditionRecord struct {
	EventID                uint32   `ch:"event_id"`
	EventName              string   `ch:"event_name"`
	EventAbbrName          *string  `ch:"event_abbr_name"`
	EventDescription       *string  `ch:"event_description"`
	EventPunchline         *string  `ch:"event_punchline"`
	EventAvgRating         *string  `ch:"event_avgRating"` // Nullable(Decimal(3,2))
	StartDate              string   `ch:"start_date"`      // Date NOT NULL
	EndDate                string   `ch:"end_date"`        // Date NOT NULL
	EditionID              uint32   `ch:"edition_id"`
	EditionCountry         string   `ch:"edition_country"`   // LowCardinality(FixedString(2)) NOT NULL
	EditionCity            uint32   `ch:"edition_city"`      // UInt32 NOT NULL
	EditionCityName        string   `ch:"edition_city_name"` // String NOT NULL
	EditionCityLat         float64  `ch:"edition_city_lat"`  // Float64 NOT NULL
	EditionCityLong        float64  `ch:"edition_city_long"` // Float64 NOT NULL
	CompanyID              *uint32  `ch:"company_id"`
	CompanyName            *string  `ch:"company_name"`
	CompanyDomain          *string  `ch:"company_domain"`
	CompanyWebsite         *string  `ch:"company_website"`
	CompanyCountry         *string  `ch:"company_country"`
	CompanyCity            *uint32  `ch:"company_city"`
	CompanyCityName        *string  `ch:"company_city_name"`
	VenueID                *uint32  `ch:"venue_id"`
	VenueName              *string  `ch:"venue_name"`
	VenueCountry           *string  `ch:"venue_country"`
	VenueCity              *uint32  `ch:"venue_city"`
	VenueCityName          *string  `ch:"venue_city_name"`
	VenueLat               *float64 `ch:"venue_lat"`
	VenueLong              *float64 `ch:"venue_long"`
	Published              int8     `ch:"published"`              // Int8 NOT NULL
	Status                 string   `ch:"status"`                 // LowCardinality(FixedString(1)) NOT NULL DEFAULT 'A'
	EditionsAudianceType   uint16   `ch:"editions_audiance_type"` // UInt16 NOT NULL
	EditionFunctionality   string   `ch:"edition_functionality"`  // LowCardinality(String) NOT NULL
	EditionWebsite         *string  `ch:"edition_website"`
	EditionDomain          *string  `ch:"edition_domain"`
	EditionType            string   `ch:"edition_type"` // LowCardinality(Nullable(String)) DEFAULT 'NA'
	EventFollowers         *uint32  `ch:"event_followers"`
	EditionFollowers       *uint32  `ch:"edition_followers"`
	EventExhibitor         *uint32  `ch:"event_exhibitor"`
	EditionExhibitor       *uint32  `ch:"edition_exhibitor"`
	EventSponsor           *uint32  `ch:"event_sponsor"`
	EditionSponsor         *uint32  `ch:"edition_sponsor"`
	EventSpeaker           *uint32  `ch:"event_speaker"`
	EditionSpeaker         *uint32  `ch:"edition_speaker"`
	EventCreated           string   `ch:"event_created"`           // DateTime NOT NULL
	EditionCreated         string   `ch:"edition_created"`         // DateTime NOT NULL
	EventHybrid            *uint8   `ch:"event_hybrid"`            // Nullable(UInt8)
	IsBranded              *uint32  `ch:"isBranded"`               // Nullable(UInt32)
	Maturity               *string  `ch:"maturity"`                // LowCardinality(Nullable(String))
	EventPricing           *string  `ch:"event_pricing"`           // LowCardinality(Nullable(String))
	EventLogo              *string  `ch:"event_logo"`              // Nullable(String)
	EventEstimatedVisitors *string  `ch:"event_estimatedVisitors"` // LowCardinality(Nullable(String))
	EventFrequency         *string  `ch:"event_frequency"`         // LowCardinality(Nullable(String))
	Version                uint32   `ch:"version"`
}

type EventTypeEventChRecord struct {
	EventTypeID   uint32 `ch:"eventtype_id"`
	EventID       uint32 `ch:"event_id"`
	Published     int8   `ch:"published"`
	Name          string `ch:"name"`           // LowCardinality(String)
	URL           string `ch:"url"`            // String
	EventAudience uint16 `ch:"event_audience"` // UInt16
	Version       uint32 `ch:"version"`
}

type EventCategoryEventChRecord struct {
	Category  uint32 `ch:"category"`   // UInt32
	Event     uint32 `ch:"event"`      // UInt32
	Name      string `ch:"name"`       // LowCardinality(String)
	URL       string `ch:"url"`        // String
	Published int8   `ch:"published"`  // Int8
	ShortName string `ch:"short_name"` // String
	IsGroup   uint8  `ch:"is_group"`   // UInt8
	Version   uint32 `ch:"version"`    // UInt32
}

func main() {
	var numChunks int
	var batchSize int
	var numWorkers int
	var clickHouseWorkers int

	var showHelp bool
	var exhibitorOnly bool
	var sponsorsOnly bool
	var visitorsOnly bool
	var speakersOnly bool
	var eventEditionOnly bool
	var eventTypeEventChOnly bool
	var eventCategoryEventChOnly bool

	flag.IntVar(&numChunks, "chunks", 5, "Number of chunks to process data in (default: 5)")
	flag.IntVar(&batchSize, "batch", 5000, "MySQL batch size for fetching data (default: 5000)")
	flag.IntVar(&numWorkers, "workers", 5, "Number of parallel workers (default: 5)")
	flag.IntVar(&clickHouseWorkers, "clickhouse-workers", 3, "Number of parallel ClickHouse insertion workers (default: 3)")

	flag.BoolVar(&exhibitorOnly, "exhibitor", false, "Process only exhibitor data (default: false)")
	flag.BoolVar(&sponsorsOnly, "sponsors", false, "Process only sponsors data (default: false)")
	flag.BoolVar(&visitorsOnly, "visitors", false, "Process only visitors data (default: false)")
	flag.BoolVar(&speakersOnly, "speakers", false, "Process only speakers data (default: false)")
	flag.BoolVar(&eventEditionOnly, "event-edition", false, "Process only event edition data (default: false)")
	flag.BoolVar(&eventTypeEventChOnly, "eventtype", false, "Process only eventtype data (default: false)")
	flag.BoolVar(&eventCategoryEventChOnly, "eventcategory", false, "Process only eventcategory data (default: false)")

	flag.BoolVar(&showHelp, "help", false, "Show help information")
	flag.Parse()

	if showHelp {
		log.Println("=== Data Migration Script ===")
		log.Println("Usage: go run main.go [table-mode] [options]")
		log.Println("\nRequired Table Mode (choose one):")
		log.Println("  -event-edition    # Process event edition data")
		log.Println("  -sponsors         # Process sponsors data")
		log.Println("  -exhibitors       # Process exhibitors data")
		log.Println("  -visitors         # Process visitors data")
		log.Println("  -speakers         # Process speakers data")
		log.Println("  -eventtype        # Process eventtype data")
		log.Println("  -eventcategory    # Process eventcategory data")
		log.Println("\nOptions:")
		log.Println("  -chunks int")
		log.Println("        Number of chunks to process data in (default: 5)")
		log.Println("  -batch int")
		log.Println("        MySQL batch size for fetching data (default: 5000)")
		log.Println("  -workers int")
		log.Println("        Number of parallel workers (default: 5)")
		log.Println("  -clickhouse-workers int")
		log.Println("        Number of parallel ClickHouse insertion workers (default: 3)")
		log.Println("  -exhibitor")
		log.Println("        Process only exhibitor data (default: false)")
		log.Println("  -sponsors")
		log.Println("        Process only sponsors data (default: false)")
		log.Println("  -visitors")
		log.Println("        Process only visitors data (default: false)")
		log.Println("  -speakers")
		log.Println("        Process only speakers data (default: false)")
		log.Println("  -event-edition")
		log.Println("        Process only event edition data (default: false)")

		log.Println("  -help")
		log.Println("        Show this help message")
		log.Println("\nExamples:")
		log.Println("  go run main.go -event-edition -chunks=10 -workers=20 -batch=50000")
		log.Println("  go run main.go -sponsors -chunks=5 -workers=10 -batch=10000")
		log.Println("  go run main.go -exhibitors -chunks=8 -workers=15 -batch=20000")
		log.Println("  go run main.go -visitors -chunks=3 -workers=8 -batch=5000")
		log.Println("  go run main.go -speakers -chunks=6 -workers=12 -batch=15000")
		log.Println("  go run main.go -eventtype -chunks=5 -workers=10 -batch=10000")
		return
	}

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

	if err := loadEnv(); err != nil {
		log.Fatal("Failed to load environment variables:", err)
	}

	mysqlDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		config.DatabaseUser,
		config.DatabasePassword,
		config.DatabaseHost,
		config.DatabasePort,
		config.DatabaseName,
	)

	clickhouseDSN := fmt.Sprintf("clickhouse://%s:%s@%s:%s/%s",
		config.ClickhouseUser,
		config.ClickhousePassword,
		config.ClickhouseHost,
		config.ClickhousePort,
		config.ClickhouseDB,
	)

	elasticHost := fmt.Sprintf("http://%s:%s",
		config.ElasticsearchHost,
		config.ElasticsearchPort,
	)

	elasticIndex := config.ElasticsearchIndex

	config.MySQLDSN = mysqlDSN
	config.ClickhouseDSN = clickhouseDSN

	config.BatchSize = batchSize
	config.NumChunks = numChunks
	config.NumWorkers = numWorkers
	config.ClickHouseWorkers = clickHouseWorkers
	config.ElasticHost = elasticHost
	config.IndexName = elasticIndex

	// Validate configuration
	if err := validateConfig(config); err != nil {
		log.Fatal("Configuration validation failed:", err)
	}

	log.Printf("=== Data Migration Configuration ===")
	if exhibitorOnly {
		log.Printf("Mode: EXHIBITOR ONLY")
	} else if sponsorsOnly {
		log.Printf("Mode: SPONSORS ONLY")
	} else if visitorsOnly {
		log.Printf("Mode: VISITORS ONLY")
	} else if speakersOnly {
		log.Printf("Mode: SPEAKERS ONLY")
	} else if eventEditionOnly {
		log.Printf("Mode: EVENT EDITION ONLY")
	} else if eventTypeEventChOnly {
		log.Printf("Mode: EVENT TYPE ONLY")
	} else if eventCategoryEventChOnly {
		log.Printf("Mode: EVENT CATEGORY ONLY")
	}

	if sponsorsOnly {
		log.Printf("Elasticsearch: Skipped (not needed for sponsors)")
	} else if speakersOnly {
		log.Printf("Elasticsearch: Skipped (not needed for speakers)")
	} else if visitorsOnly {
		log.Printf("Elasticsearch: Skipped (not needed for visitors)")
	} else if exhibitorOnly {
		log.Printf("Elasticsearch: Skipped (not needed for exhibitors)")
	} else if eventTypeEventChOnly {
		log.Printf("Elasticsearch: Skipped (not needed for event Type)")
	} else if eventCategoryEventChOnly {
		log.Printf("Elasticsearch: Skipped (not needed for event Category)")
	}
	log.Printf("==============================\n")

	mysqlDB, clickhouseDB, esClient, err := setupConnections(config)
	if err != nil {
		log.Fatal(err)
	}
	defer mysqlDB.Close()
	defer clickhouseDB.Close()

	log.Println("Connections established successfully!")

	if err := testClickHouseConnection(clickhouseDB); err != nil {
		log.Fatalf("ClickHouse connection test failed: %v", err)
	}

	if !sponsorsOnly && !speakersOnly && !visitorsOnly && !exhibitorOnly && !eventTypeEventChOnly {
		if err := testElasticsearchConnection(esClient, config.IndexName); err != nil {
			log.Fatalf("Elasticsearch connection test failed: %v", err)
		}
	} else {
		if sponsorsOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for sponsors processing)")
		} else if speakersOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for speakers processing)")
		} else if visitorsOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for visitors processing)")
		} else if exhibitorOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for exhibitors processing)")
		} else if eventTypeEventChOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for event_type_ch processing)")
		} else if eventCategoryEventChOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for event_category_ch processing)")
		}
	}

	if exhibitorOnly {
		processExhibitorOnly(mysqlDB, clickhouseDB, config)
	} else if sponsorsOnly {
		processSponsorsOnly(mysqlDB, clickhouseDB, config)
	} else if visitorsOnly {
		processVisitorsOnly(mysqlDB, clickhouseDB, config)
	} else if speakersOnly {
		processSpeakersOnly(mysqlDB, clickhouseDB, config)
	} else if eventEditionOnly {
		processEventEditionOnly(mysqlDB, clickhouseDB, esClient, config)
	} else if eventTypeEventChOnly {
		processEventTypeEventChOnly(mysqlDB, clickhouseDB, config)
	} else if eventCategoryEventChOnly {
		processEventCategoryEventChOnly(mysqlDB, clickhouseDB, config)
	} else {
		log.Println("Error: No specific table mode selected!")
		log.Println("Please specify one of the following modes:")
		log.Println("  -event-edition    # Process event edition data")
		log.Println("  -sponsors         # Process sponsors data")
		log.Println("  -exhibitors       # Process exhibitors data")
		log.Println("  -visitors         # Process visitors data")
		log.Println("  -speakers         # Process speakers data")
		log.Println("  -eventtype        # Process eventtype data")
		log.Println("  -eventcategory    # Process eventcategory data")
		log.Println("")
		log.Println("Example: go run main.go -event-edition -chunks=10 -workers=20")
		os.Exit(1)
	}
}

// Special function for edition_type that preserves empty strings as "NA"
func safeConvertEditionType(value interface{}) string {
	if value == nil {
		return "NA"
	}
	if str, ok := value.(string); ok {
		if str == "" {
			return "NA"
		}
		return str
	}
	if bytes, ok := value.([]uint8); ok {
		str := string(bytes)
		if str == "" {
			return "NA"
		}
		return str
	}
	if bytes, ok := value.([]byte); ok {
		str := string(bytes)
		if str == "" {
			return "NA"
		}
		return str
	}
	return "NA"
}
