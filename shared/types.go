package shared

import (
	"database/sql"
	"fmt"
	"log"
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

// GetTotalRecordsAndIDRange gets total records and ID range for a table
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

// SafeConvertToString safely converts a value to string
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
	return fmt.Sprintf("%v", value)
}
