package shared

import (
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"fmt"
	"log"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Config represents the application configuration
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

	MySQLDSN          string
	ClickhouseDSN     string
	BatchSize         int
	NumChunks         int
	NumWorkers        int
	ClickHouseWorkers int
}

// RetryWithBackoff retries an operation with exponential backoff
func RetryWithBackoff(operation func() error, maxRetries int, operationName string) error {
	var lastError error
	for i := 0; i < maxRetries; i++ {
		if err := operation(); err != nil {
			lastError = err
			if i < maxRetries-1 {
				backoffDuration := time.Duration(i+1) * time.Second
				log.Printf("Retrying %s in %v (attempt %d/%d): %v", operationName, backoffDuration, i+1, maxRetries, err)
				time.Sleep(backoffDuration)
			}
		} else {
			return nil
		}
	}
	return fmt.Errorf("operation %s failed after %d retries: %v", operationName, maxRetries, lastError)
}

func GetTotalRecordsAndIDRange(db *sql.DB, table string) (int, int, int, error) {
	query := fmt.Sprintf("SELECT COUNT(*), MIN(id), MAX(id) FROM %s", table)
	fmt.Printf("Executing query: %s\n", query)

	start := time.Now()
	var count, minId, maxId int
	err := db.QueryRow(query).Scan(&count, &minId, &maxId)
	duration := time.Since(start)

	if err != nil {
		fmt.Printf("SQL error in GetTotalRecordsAndIDRange for table %s: %v\n", table, err)
		fmt.Printf("Query execution time: %v\n", duration)
		return 0, 0, 0, fmt.Errorf("failed to get records for table %s: %v", table, err)
	}

	fmt.Printf("Query completed successfully in %v\n", duration)
	return count, minId, maxId, nil
}

func SafeConvertToDateTimeString(value interface{}) string {
	str := SafeConvertToString(value)
	if str == "" {
		return "1970-01-01 00:00:00"
	}
	return str
}

// safely converts a value to string for non-nullable fields
func SafeConvertToString(value interface{}) string {
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
	// Handle numeric types
	if intVal, ok := value.(int64); ok {
		return strconv.FormatInt(intVal, 10)
	}
	if intVal, ok := value.(int32); ok {
		return strconv.FormatInt(int64(intVal), 10)
	}
	if intVal, ok := value.(int); ok {
		return strconv.Itoa(intVal)
	}
	if uintVal, ok := value.(uint64); ok {
		return strconv.FormatUint(uintVal, 10)
	}
	if uintVal, ok := value.(uint32); ok {
		return strconv.FormatUint(uint64(uintVal), 10)
	}
	if uintVal, ok := value.(uint); ok {
		return strconv.FormatUint(uint64(uintVal), 10)
	}
	return ""
}

// safeConvertToStatusString converts a value to status string with default 'A' for empty values
func SafeConvertToStatusString(value interface{}) string {
	status := SafeConvertToString(value)
	if status == "" {
		return "A" // Default status value as per ClickHouse schema
	}
	return status
}

// safeConvertToNullableUInt8 converts a value to nullable UInt8, handling both numeric and string types
func SafeConvertToNullableUInt8(value interface{}) *uint8 {
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
	str := SafeConvertToString(value)
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
func SafeConvertToNullableString(value interface{}) *string {
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

// generateEventUUID creates a deterministic UUID from event_id and event_created
func GenerateEventUUID(eventID uint32, eventCreated interface{}) string {
	// Convert event_created to string for consistent input
	var createdStr string
	if eventCreated != nil {
		createdStr = SafeConvertToString(eventCreated)
	}

	// Create input string combining event_id and event_created
	input := fmt.Sprintf("%d_%s", eventID, createdStr)

	// Generate SHA256 hash of the input
	hash := sha256.Sum256([]byte(input))

	// Convert first 16 bytes to UUID format (version 5 style)
	uuid := make([]byte, 16)
	copy(uuid, hash[:16])

	// Set version (4 bits) and variant (2 bits) for UUID v4 compatibility
	uuid[6] = (uuid[6] & 0x0f) | 0x40 // Version 4
	uuid[8] = (uuid[8] & 0x3f) | 0x80 // Variant bits

	// Format as UUID string
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		binary.BigEndian.Uint32(uuid[0:4]),
		binary.BigEndian.Uint16(uuid[4:6]),
		binary.BigEndian.Uint16(uuid[6:8]),
		binary.BigEndian.Uint16(uuid[8:10]),
		binary.BigEndian.Uint64(append([]byte{0, 0}, uuid[10:16]...))&0xffffffffffff)
}

func GenerateCategoryUUID(categoryID uint32, categoryName interface{}, categoryCreated interface{}) string {
	var nameStr string
	if categoryName != nil {
		nameStr = SafeConvertToString(categoryName)
	}

	var createdStr string
	if categoryCreated != nil {
		createdStr = SafeConvertToString(categoryCreated)
	}

	input := fmt.Sprintf("%d_%s_%s", categoryID, nameStr, createdStr)
	hash := sha256.Sum256([]byte(input))
	uuid := make([]byte, 16)
	copy(uuid, hash[:16])
	uuid[6] = (uuid[6] & 0x0f) | 0x40
	uuid[8] = (uuid[8] & 0x3f) | 0x80

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		binary.BigEndian.Uint32(uuid[0:4]),
		binary.BigEndian.Uint16(uuid[4:6]),
		binary.BigEndian.Uint16(uuid[6:8]),
		binary.BigEndian.Uint16(uuid[8:10]),
		binary.BigEndian.Uint64(append([]byte{0, 0}, uuid[10:16]...))&0xffffffffffff)
}

// converts a nullable string to uppercase
func ToUpperNullableString(s *string) *string {
	if s == nil {
		return nil
	}
	upper := strings.ToUpper(*s)
	return &upper
}

func SafeConvertToDateString(value interface{}) string {
	str := SafeConvertToString(value)
	if str == "" {
		return "1970-01-01"
	}
	return str
}

func SafeConvertToUInt32(value interface{}) uint32 {
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

func SafeConvertToUInt16(value interface{}) uint16 {
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

func SafeConvertToInt8(value interface{}) int8 {
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

func SafeConvertToNullableUInt32(value interface{}) *uint32 {
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

func SafeConvertToFloat64(value interface{}) float64 {
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

func SafeConvertToNullableFloat64(value interface{}) *float64 {
	if value == nil {
		return nil
	}
	if ptr, ok := value.(*float64); ok {
		return ptr
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
func SafeConvertFloat64ToDecimalString(value interface{}) *string {
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
func ConvertToUInt32(value interface{}) uint32 {
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
func ConvertToUInt8(value interface{}) uint8 {
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

func ExtractDomainFromWebsite(website interface{}) string {
	if website == nil {
		return ""
	}

	websiteStrPtr := ConvertToStringPtr(website)
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
func GetCompanyNameOrDefault(companyName interface{}) string {
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
func ConvertToUInt32Ptr(value interface{}) *uint32 {
	if value == nil {
		return nil
	}

	u32 := ConvertToUInt32(value)
	return &u32
}

// converts a value to *string for nullable fields
func ConvertToStringPtr(value interface{}) *string {
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

func ConvertToString(value interface{}) string {
	ptr := ConvertToStringPtr(value)
	if ptr == nil {
		return ""
	}
	return *ptr
}

func ConvertToInt8(value interface{}) int8 {
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
