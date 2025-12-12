package shared

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type TableMapping struct {
	Original string
	Temp     string
	Backup   string
}

func EscapeTableName(tableName string) string {
	escaped := strings.ReplaceAll(tableName, "`", "``")
	return fmt.Sprintf("`%s`", escaped)
}

func GetTableNameWithDB(tableName string, config Config) string {
	return fmt.Sprintf("%s.%s", EscapeTableName(config.ClickhouseDB), EscapeTableName(tableName))
}

func GetClickHouseTableName(baseTableName string, config Config) string {
	if config.UseTempTables {
		if strings.HasSuffix(baseTableName, "_ch") {
			return strings.Replace(baseTableName, "_ch", "_temp", 1)
		}
		return baseTableName + "_temp"
	}
	if !strings.HasSuffix(baseTableName, "_ch") {
		return baseTableName + "_ch"
	}
	return baseTableName
}

func GetHumanReadableTimestamp() string {
	return time.Now().Format("2006-01-02_15-04-05")
}

func GetAllTableMappings(config Config) []TableMapping {
	timestamp := GetHumanReadableTimestamp()

	return []TableMapping{
		{"alerts_ch", "alerts_temp", fmt.Sprintf("alerts_ch_backup_%s", timestamp)},
		{"allevent_ch", "allevent_temp", fmt.Sprintf("allevent_ch_backup_%s", timestamp)},
		{"event_category_ch", "event_category_temp", fmt.Sprintf("event_category_ch_backup_%s", timestamp)},
		{"event_designation_ch", "event_designation_temp", fmt.Sprintf("event_designation_ch_backup_%s", timestamp)},
		{"event_exhibitor_ch", "event_exhibitor_temp", fmt.Sprintf("event_exhibitor_ch_backup_%s", timestamp)},
		{"event_ranking_ch", "event_ranking_temp", fmt.Sprintf("event_ranking_ch_backup_%s", timestamp)},
		{"event_speaker_ch", "event_speaker_temp", fmt.Sprintf("event_speaker_ch_backup_%s", timestamp)},
		{"event_sponsors_ch", "event_sponsors_temp", fmt.Sprintf("event_sponsors_ch_backup_%s", timestamp)},
		{"event_type_ch", "event_type_temp", fmt.Sprintf("event_type_ch_backup_%s", timestamp)},
		{"event_visitorSpread_ch", "event_visitorSpread_temp", fmt.Sprintf("event_visitorSpread_ch_backup_%s", timestamp)},
		{"event_visitors_ch", "event_visitors_temp", fmt.Sprintf("event_visitors_ch_backup_%s", timestamp)},
		{"location_polygons_ch", "location_polygons_temp", fmt.Sprintf("location_polygons_ch_backup_%s", timestamp)},
		{"location_ch", "location_temp", fmt.Sprintf("location_ch_backup_%s", timestamp)},
	}
}

func GetMainTableNames() []string {
	return []string{
		"alerts_ch",
		"allevent_ch",
		"event_category_ch",
		"event_designation_ch",
		"event_exhibitor_ch",
		"event_ranking_ch",
		"event_speaker_ch",
		"event_sponsors_ch",
		"event_type_ch",
		"event_visitorSpread_ch",
		"event_visitors_ch",
		"location_polygons_ch",
		"location_ch",
	}
}

func GetTableMapping(originalTable string, config Config) TableMapping {
	timestamp := GetHumanReadableTimestamp()
	tempTable := strings.Replace(originalTable, "_ch", "_temp", 1)
	backupTable := fmt.Sprintf("%s_backup_%s", originalTable, timestamp)
	return TableMapping{
		Original: originalTable,
		Temp:     tempTable,
		Backup:   backupTable,
	}
}

func SwapSingleTable(clickhouseConn driver.Conn, tableName string, config Config, errorLogFile string) error {
	log.Printf("Swapping table: %s", tableName)

	mapping := GetTableMapping(tableName, config)

	exists, err := TableExists(clickhouseConn, mapping.Temp, config)
	if err != nil {
		err = fmt.Errorf("failed to check existence of temp table %s: %w", mapping.Temp, err)
		logErrorToFile("Table Swap", err, errorLogFile)
		return err
	}
	if !exists {
		err := fmt.Errorf("temp table %s does not exist", mapping.Temp)
		logErrorToFile("Table Swap", err, errorLogFile)
		return err
	}

	originalExists, err := TableExists(clickhouseConn, mapping.Original, config)
	if err == nil && originalExists {
		originalSchema, err := GetTableSchema(clickhouseConn, mapping.Original, config)
		if err != nil {
			err = fmt.Errorf("failed to get schema for original table %s: %w", mapping.Original, err)
			logErrorToFile("Table Swap", err, errorLogFile)
			return err
		}

		tempSchema, err := GetTableSchema(clickhouseConn, mapping.Temp, config)
		if err != nil {
			err = fmt.Errorf("failed to get schema for temp table %s: %w", mapping.Temp, err)
			logErrorToFile("Table Swap", err, errorLogFile)
			return err
		}

		if err := CompareTableSchemas(originalSchema, tempSchema, mapping.Original, mapping.Temp); err != nil {
			err = fmt.Errorf("schema mismatch for %s: %w", mapping.Original, err)
			logErrorToFile("Table Swap", err, errorLogFile)
			return err
		}
		log.Printf("✓ Schema validation passed for %s", mapping.Original)
	}

	tableMappings := []TableMapping{mapping}
	if err := SwapTables(clickhouseConn, tableMappings, config, errorLogFile); err != nil {
		return fmt.Errorf("failed to swap table %s: %w", tableName, err)
	}

	log.Printf("✓ Successfully swapped table: %s", tableName)
	return nil
}

type TableSchema struct {
	Name        string
	Type        string
	DefaultType string
	DefaultExpr string
	Comment     string
	CodecExpr   string
	TTLExpr     string
}

func GetTableSchema(clickhouseConn driver.Conn, tableName string, config Config) ([]TableSchema, error) {
	fullTableName := GetTableNameWithDB(tableName, config)

	log.Printf("Checking ClickHouse connection health before getting schema for %s", fullTableName)
	connectionCheckErr := RetryWithBackoff(
		func() error {
			return CheckClickHouseConnectionAlive(clickhouseConn)
		},
		3,
		fmt.Sprintf("ClickHouse connection health check for schema query %s", fullTableName),
	)
	if connectionCheckErr != nil {
		return nil, fmt.Errorf("ClickHouse connection is not alive after retries: %w", connectionCheckErr)
	}

	query := fmt.Sprintf("DESCRIBE TABLE %s", fullTableName)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var rows driver.Rows
	var err error
	queryErr := RetryWithBackoff(
		func() error {
			rows, err = clickhouseConn.Query(ctx, query)
			if err != nil {
				return fmt.Errorf("failed to query schema for %s: %w", fullTableName, err)
			}
			return nil
		},
		3,
		fmt.Sprintf("query schema for %s", fullTableName),
	)
	if queryErr != nil {
		return nil, queryErr
	}
	defer rows.Close()

	var schemas []TableSchema
	for rows.Next() {
		var schema TableSchema
		if err := rows.Scan(
			&schema.Name,
			&schema.Type,
			&schema.DefaultType,
			&schema.DefaultExpr,
			&schema.Comment,
			&schema.CodecExpr,
			&schema.TTLExpr,
		); err != nil {
			return nil, fmt.Errorf("failed to scan schema row for %s: %w", fullTableName, err)
		}
		schemas = append(schemas, schema)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating schema rows for %s: %w", fullTableName, err)
	}

	return schemas, nil
}

func CompareTableSchemas(originalSchema, tempSchema []TableSchema, originalTable, tempTable string) error {
	if len(originalSchema) != len(tempSchema) {
		return fmt.Errorf("schema column count mismatch: %s has %d columns, %s has %d columns",
			originalTable, len(originalSchema), tempTable, len(tempSchema))
	}

	for i := range originalSchema {
		orig := originalSchema[i]
		temp := tempSchema[i]

		if !strings.EqualFold(orig.Name, temp.Name) {
			return fmt.Errorf("column name mismatch at position %d: %s has '%s', %s has '%s'",
				i, originalTable, orig.Name, tempTable, temp.Name)
		}

		origType := normalizeType(orig.Type)
		tempType := normalizeType(temp.Type)
		if origType != tempType {
			return fmt.Errorf("column type mismatch for '%s': %s has '%s', %s has '%s'",
				orig.Name, originalTable, orig.Type, tempTable, temp.Type)
		}
	}

	return nil
}

func normalizeType(typeStr string) string {
	return strings.ToLower(strings.TrimSpace(typeStr))
}

func TableExists(clickhouseConn driver.Conn, tableName string, config Config) (bool, error) {
	fullTableName := GetTableNameWithDB(tableName, config)

	log.Printf("Checking ClickHouse connection health before checking table existence for %s", fullTableName)
	connectionCheckErr := RetryWithBackoff(
		func() error {
			return CheckClickHouseConnectionAlive(clickhouseConn)
		},
		3,
		fmt.Sprintf("ClickHouse connection health check for table existence %s", fullTableName),
	)
	if connectionCheckErr != nil {
		return false, fmt.Errorf("ClickHouse connection is not alive after retries: %w", connectionCheckErr)
	}

	query := fmt.Sprintf("EXISTS TABLE %s", fullTableName)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var exists uint8
	var err error
	queryErr := RetryWithBackoff(
		func() error {
			row := clickhouseConn.QueryRow(ctx, query)
			err = row.Scan(&exists)
			if err != nil {
				return fmt.Errorf("failed to check table existence for %s: %w", fullTableName, err)
			}
			return nil
		},
		3,
		fmt.Sprintf("check table existence for %s", fullTableName),
	)
	if queryErr != nil {
		return false, queryErr
	}

	return exists == 1, nil
}

func GetTableRowCount(clickhouseConn driver.Conn, tableName string, config Config) (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fullTableName := GetTableNameWithDB(tableName, config)
	query := fmt.Sprintf("SELECT count() FROM %s", fullTableName)

	var count uint64
	row := clickhouseConn.QueryRow(ctx, query)
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to get row count for %s: %w", fullTableName, err)
	}

	return count, nil
}

func ValidateTempTables(clickhouseConn driver.Conn, tableMappings []TableMapping, config Config, errorLogFile string) error {
	log.Println("Validating temp tables...")

	for _, mapping := range tableMappings {
		exists, err := TableExists(clickhouseConn, mapping.Temp, config)
		if err != nil {
			err = fmt.Errorf("failed to check existence of temp table %s: %w", mapping.Temp, err)
			logErrorToFile("Table Validation", err, errorLogFile)
			return err
		}
		if !exists {
			err := fmt.Errorf("temp table %s does not exist", mapping.Temp)
			logErrorToFile("Table Validation", err, errorLogFile)
			return err
		}

		originalExists, err := TableExists(clickhouseConn, mapping.Original, config)
		if err != nil {
			log.Printf("WARNING: Failed to check existence of original table %s: %v", mapping.Original, err)
		}

		if originalExists {
			originalSchema, err := GetTableSchema(clickhouseConn, mapping.Original, config)
			if err != nil {
				err = fmt.Errorf("failed to get schema for original table %s: %w", mapping.Original, err)
				logErrorToFile("Table Validation", err, errorLogFile)
				return err
			}

			tempSchema, err := GetTableSchema(clickhouseConn, mapping.Temp, config)
			if err != nil {
				err = fmt.Errorf("failed to get schema for temp table %s: %w", mapping.Temp, err)
				logErrorToFile("Table Validation", err, errorLogFile)
				return err
			}

			if err := CompareTableSchemas(originalSchema, tempSchema, mapping.Original, mapping.Temp); err != nil {
				err = fmt.Errorf("schema mismatch for %s: %w", mapping.Original, err)
				logErrorToFile("Table Validation", err, errorLogFile)
				return err
			}

			log.Printf("✓ Schema validation passed for %s", mapping.Original)
		} else {
			log.Printf("⚠ Original table %s does not exist (first run?), skipping schema validation", mapping.Original)
		}

		rowCount, err := GetTableRowCount(clickhouseConn, mapping.Temp, config)
		if err != nil {
			err = fmt.Errorf("failed to get row count for temp table %s: %w", mapping.Temp, err)
			logErrorToFile("Table Validation", err, errorLogFile)
			return err
		}

		if rowCount == 0 {
			log.Printf("WARNING: Temp table %s has 0 rows", mapping.Temp)
		} else {
			log.Printf("✓ Temp table %s has %d rows", mapping.Temp, rowCount)
		}
	}

	log.Println("✓ All temp tables validated successfully")
	return nil
}

func RenameTable(clickhouseConn driver.Conn, oldName, newName string, config Config, errorLogFile string) error {
	fullOldName := GetTableNameWithDB(oldName, config)
	fullNewName := GetTableNameWithDB(newName, config)

	log.Printf("Checking ClickHouse connection health before renaming %s to %s", oldName, newName)
	connectionCheckErr := RetryWithBackoff(
		func() error {
			return CheckClickHouseConnectionAlive(clickhouseConn)
		},
		3,
		fmt.Sprintf("ClickHouse connection health check for rename %s to %s", oldName, newName),
	)
	if connectionCheckErr != nil {
		err := fmt.Errorf("ClickHouse connection is not alive after retries: %w", connectionCheckErr)
		logErrorToFile("Table Rename", err, errorLogFile)
		return err
	}
	log.Printf("ClickHouse connection is alive, proceeding with table rename")

	renameErr := RetryWithBackoff(
		func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			query := fmt.Sprintf("RENAME TABLE %s TO %s", fullOldName, fullNewName)
			log.Printf("Executing: %s", query)

			if err := clickhouseConn.Exec(ctx, query); err != nil {
				return fmt.Errorf("failed to rename table: %w", err)
			}

			log.Printf("✓ Successfully renamed %s to %s", fullOldName, fullNewName)
			return nil
		},
		3,
		fmt.Sprintf("rename table %s to %s", oldName, newName),
	)

	if renameErr != nil {
		err := fmt.Errorf("failed to rename table %s to %s after retries: %w", oldName, newName, renameErr)
		logErrorToFile("Table Rename", err, errorLogFile)
		return err
	}

	return nil
}

func SwapTables(clickhouseConn driver.Conn, tableMappings []TableMapping, config Config, errorLogFile string) error {
	log.Println("Starting table swap process...")

	var successfulSwaps []TableMapping
	var swapErrors []error

	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	log.Println("STAGE 1: Renaming original tables to backup...")
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	for _, mapping := range tableMappings {
		exists, err := TableExists(clickhouseConn, mapping.Original, config)
		if err != nil {
			log.Printf("WARNING: Failed to check existence of %s: %v", mapping.Original, err)
			continue
		}

		if !exists {
			log.Printf("⚠ Original table %s does not exist (first run?), skipping backup rename", mapping.Original)
			continue
		}

		backupExists, err := TableExists(clickhouseConn, mapping.Backup, config)
		if err != nil {
			log.Printf("WARNING: Failed to check existence of backup table %s: %v", mapping.Backup, err)
		}

		if backupExists {
			log.Printf("⚠ Backup table %s already exists, will be overwritten", mapping.Backup)
		}

		if err := RenameTable(clickhouseConn, mapping.Original, mapping.Backup, config, errorLogFile); err != nil {
			err = fmt.Errorf("failed to rename %s to %s: %w", mapping.Original, mapping.Backup, err)
			swapErrors = append(swapErrors, err)
			logErrorToFile("Table Swap Stage 1", err, errorLogFile)
			continue
		}

		successfulSwaps = append(successfulSwaps, mapping)
		log.Printf("✓ Renamed %s → %s", mapping.Original, mapping.Backup)
	}

	if len(swapErrors) > 0 {
		log.Printf("⚠ %d errors occurred during Stage 1, but continuing with Stage 2", len(swapErrors))
	}

	log.Println("")
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	log.Println("STAGE 2: Renaming temp tables to original...")
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	stage2Errors := []error{}
	for _, mapping := range tableMappings {
		exists, err := TableExists(clickhouseConn, mapping.Temp, config)
		if err != nil {
			err = fmt.Errorf("failed to check existence of temp table %s: %w", mapping.Temp, err)
			stage2Errors = append(stage2Errors, err)
			logErrorToFile("Table Swap Stage 2", err, errorLogFile)
			continue
		}

		if !exists {
			err := fmt.Errorf("temp table %s does not exist", mapping.Temp)
			stage2Errors = append(stage2Errors, err)
			logErrorToFile("Table Swap Stage 2", err, errorLogFile)
			continue
		}

		if err := RenameTable(clickhouseConn, mapping.Temp, mapping.Original, config, errorLogFile); err != nil {
			err = fmt.Errorf("failed to rename %s to %s: %w", mapping.Temp, mapping.Original, err)
			stage2Errors = append(stage2Errors, err)
			logErrorToFile("Table Swap Stage 2", err, errorLogFile)
			continue
		}

		log.Printf("✓ Renamed %s → %s", mapping.Temp, mapping.Original)
	}

	log.Println("")
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	if len(stage2Errors) > 0 {
		log.Printf("⚠ Table swap completed with %d errors in Stage 2", len(stage2Errors))
		for _, err := range stage2Errors {
			log.Printf("  - %v", err)
		}
		return fmt.Errorf("table swap completed with %d errors: %v", len(stage2Errors), stage2Errors[0])
	}

	log.Println("✓ All tables swapped successfully!")
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	return nil
}

func logErrorToFile(operationName string, err error, errorLogFile string) {
	file, fileErr := os.OpenFile(errorLogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if fileErr != nil {
		log.Printf("WARNING: Failed to open error log file: %v", fileErr)
		return
	}
	defer file.Close()

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	errorMsg := fmt.Sprintf("[%s] ERROR in %s: %v\n", timestamp, operationName, err)
	if _, writeErr := file.WriteString(errorMsg); writeErr != nil {
		log.Printf("WARNING: Failed to write to error log file: %v", writeErr)
	}
	log.Printf("ERROR logged to %s: %s - %v", errorLogFile, operationName, err)
}

func GetTableCreateStatement(clickhouseConn driver.Conn, tableName string, config Config) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fullTableName := GetTableNameWithDB(tableName, config)
	query := fmt.Sprintf("SHOW CREATE TABLE %s", fullTableName)

	var createStatement string
	row := clickhouseConn.QueryRow(ctx, query)
	if err := row.Scan(&createStatement); err != nil {
		return "", fmt.Errorf("failed to get CREATE TABLE statement for %s: %w", fullTableName, err)
	}

	return createStatement, nil
}

func GetAllBackupTables(clickhouseConn driver.Conn, config Config) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dbName := EscapeTableName(config.ClickhouseDB)
	query := fmt.Sprintf("SHOW TABLES FROM %s LIKE '%%_backup_%%'", dbName)

	rows, err := clickhouseConn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query backup tables: %w", err)
	}
	defer rows.Close()

	var backupTables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan backup table name: %w", err)
		}
		backupTables = append(backupTables, tableName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating backup tables: %w", err)
	}

	return backupTables, nil
}

func DropTable(clickhouseConn driver.Conn, tableName string, config Config, errorLogFile string) error {
	fullTableName := GetTableNameWithDB(tableName, config)

	log.Printf("Checking ClickHouse connection health before dropping table %s", tableName)
	connectionCheckErr := RetryWithBackoff(
		func() error {
			return CheckClickHouseConnectionAlive(clickhouseConn)
		},
		3,
		fmt.Sprintf("ClickHouse connection health check for drop table %s", tableName),
	)
	if connectionCheckErr != nil {
		err := fmt.Errorf("ClickHouse connection is not alive after retries: %w", connectionCheckErr)
		logErrorToFile("Drop Table", err, errorLogFile)
		return err
	}
	log.Printf("ClickHouse connection is alive, proceeding with table drop")

	dropErr := RetryWithBackoff(
		func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			query := fmt.Sprintf("DROP TABLE IF EXISTS %s", fullTableName)

			if err := clickhouseConn.Exec(ctx, query); err != nil {
				return fmt.Errorf("failed to drop table: %w", err)
			}

			log.Printf("✓ Successfully dropped table %s", fullTableName)
			return nil
		},
		3,
		fmt.Sprintf("drop table %s", tableName),
	)

	if dropErr != nil {
		err := fmt.Errorf("failed to drop table %s after retries: %w", tableName, dropErr)
		logErrorToFile("Drop Table", err, errorLogFile)
		return err
	}

	return nil
}

func CreateTempTableFromOriginal(clickhouseConn driver.Conn, originalTable, tempTable string, config Config, errorLogFile string) error {
	createStatement, err := GetTableCreateStatement(clickhouseConn, originalTable, config)
	log.Printf("CREATE TABLE statement: %s", createStatement)
	if err != nil {
		err = fmt.Errorf("failed to get CREATE TABLE statement for %s: %w", originalTable, err)
		logErrorToFile("Create Temp Table", err, errorLogFile)
		return err
	}

	fullOriginalNameQuoted := GetTableNameWithDB(originalTable, config)
	fullTempNameQuoted := GetTableNameWithDB(tempTable, config)

	fullOriginalNameUnquoted := fmt.Sprintf("%s.%s", config.ClickhouseDB, originalTable)
	fullTempNameUnquoted := fmt.Sprintf("%s.%s", config.ClickhouseDB, tempTable)

	log.Printf("FULL ORIGINAL NAME (quoted): %s", fullOriginalNameQuoted)
	log.Printf("FULL TEMP NAME (quoted): %s", fullTempNameQuoted)
	log.Printf("FULL ORIGINAL NAME (unquoted): %s", fullOriginalNameUnquoted)
	log.Printf("FULL TEMP NAME (unquoted): %s", fullTempNameUnquoted)

	if strings.Contains(createStatement, fullOriginalNameQuoted) {
		createStatement = strings.Replace(createStatement, fullOriginalNameQuoted, fullTempNameQuoted, 1)
		log.Printf("Replaced quoted table name")
	} else if strings.Contains(createStatement, fullOriginalNameUnquoted) {
		createStatement = strings.Replace(createStatement, fullOriginalNameUnquoted, fullTempNameUnquoted, 1)
		log.Printf("Replaced unquoted table name")
	} else {
		createStatement = strings.Replace(createStatement, originalTable, tempTable, -1)
		log.Printf("Replaced table name directly (fallback)")
	}

	log.Printf("TEMP CREATE TABLE statement: %s", createStatement)

	log.Printf("Checking ClickHouse connection health before creating temp table %s", tempTable)
	connectionCheckErr := RetryWithBackoff(
		func() error {
			return CheckClickHouseConnectionAlive(clickhouseConn)
		},
		3,
		fmt.Sprintf("ClickHouse connection health check for create temp table %s", tempTable),
	)
	if connectionCheckErr != nil {
		err := fmt.Errorf("ClickHouse connection is not alive after retries: %w", connectionCheckErr)
		logErrorToFile("Create Temp Table", err, errorLogFile)
		return err
	}
	log.Printf("ClickHouse connection is alive, proceeding with temp table creation")

	createErr := RetryWithBackoff(
		func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			log.Printf("Executing: %s", createStatement)

			if err := clickhouseConn.Exec(ctx, createStatement); err != nil {
				return fmt.Errorf("failed to create temp table: %w", err)
			}

			log.Printf("✓ Successfully created temp table %s", fullTempNameQuoted)
			return nil
		},
		3,
		fmt.Sprintf("create temp table %s", tempTable),
	)

	if createErr != nil {
		err := fmt.Errorf("failed to create temp table %s after retries: %w", tempTable, createErr)
		logErrorToFile("Create Temp Table", err, errorLogFile)
		return err
	}

	return nil
}

func EnsureTempTablesExist(clickhouseConn driver.Conn, config Config, errorLogFile string) error {
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	log.Println("ENSURING TEMP TABLES EXIST AND CLEANING UP BACKUP TABLES")
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	mainTables := GetMainTableNames()

	log.Println("Checking existing temp tables...")
	for _, mainTable := range mainTables {
		tempTable := strings.Replace(mainTable, "_ch", "_temp", 1)
		exists, err := TableExists(clickhouseConn, tempTable, config)
		if err != nil {
			log.Printf("WARNING: Failed to check existence of temp table %s: %v", tempTable, err)
			continue
		}

		if exists {
			log.Printf("⚠ Temp table %s already exists. Dropping and recreating from original...", tempTable)
			if err := DropTable(clickhouseConn, tempTable, config, errorLogFile); err != nil {
				err = fmt.Errorf("failed to drop existing temp table %s: %w", tempTable, err)
				logErrorToFile("Ensure Temp Tables", err, errorLogFile)
				return err
			}
			log.Printf("✓ Dropped existing temp table %s", tempTable)
		}

		originalTable := mainTable
		originalExists, err := TableExists(clickhouseConn, originalTable, config)
		if err != nil {
			err = fmt.Errorf("failed to check existence of original table %s: %w", originalTable, err)
			logErrorToFile("Ensure Temp Tables", err, errorLogFile)
			return err
		}

		if !originalExists {
			log.Printf("⚠ Original table %s does not exist, skipping temp table creation", originalTable)
			continue
		}

		log.Printf("Creating temp table %s from original table %s...", tempTable, originalTable)
		if err := CreateTempTableFromOriginal(clickhouseConn, originalTable, tempTable, config, errorLogFile); err != nil {
			err = fmt.Errorf("failed to create temp table %s: %w", tempTable, err)
			logErrorToFile("Ensure Temp Tables", err, errorLogFile)
			return err
		}
		log.Printf("✓ Successfully created temp table %s", tempTable)
	}

	log.Println("✓ All temp tables verified and recreated successfully")

	log.Println("Cleaning up backup tables...")
	backupTables, err := GetAllBackupTables(clickhouseConn, config)
	if err != nil {
		log.Printf("WARNING: Failed to get backup tables: %v", err)
	} else {
		if len(backupTables) > 0 {
			log.Printf("Found %d backup tables to delete", len(backupTables))
			for _, backupTable := range backupTables {
				log.Printf("Dropping backup table %s...", backupTable)
				if err := DropTable(clickhouseConn, backupTable, config, errorLogFile); err != nil {
					log.Printf("WARNING: Failed to drop backup table %s: %v", backupTable, err)
					continue
				}
				log.Printf("✓ Dropped backup table %s", backupTable)
			}
			log.Printf("✓ Cleaned up %d backup tables", len(backupTables))
		} else {
			log.Println("✓ No backup tables found")
		}
	}

	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	log.Println("TEMP TABLES VERIFICATION AND CLEANUP COMPLETED")
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	log.Println("")

	return nil
}

func EnsureSingleTempTableExists(clickhouseConn driver.Conn, mainTableName string, config Config, errorLogFile string) error {
	tempTableName := strings.Replace(mainTableName, "_ch", "_temp", 1)

	log.Printf("Checking if temp table %s exists for %s...", tempTableName, mainTableName)
	exists, err := TableExists(clickhouseConn, tempTableName, config)
	if err != nil {
		err = fmt.Errorf("failed to check existence of temp table %s: %w", tempTableName, err)
		logErrorToFile("Ensure Single Temp Table", err, errorLogFile)
		return err
	}

	if exists {
		log.Printf("⚠ Temp table %s already exists. Dropping and recreating from original...", tempTableName)

		if err := DropTable(clickhouseConn, tempTableName, config, errorLogFile); err != nil {
			err = fmt.Errorf("failed to drop existing temp table %s: %w", tempTableName, err)
			logErrorToFile("Ensure Single Temp Table", err, errorLogFile)
			return err
		}

		log.Printf("✓ Dropped existing temp table %s", tempTableName)
	}

	originalExists, err := TableExists(clickhouseConn, mainTableName, config)
	if err != nil {
		err = fmt.Errorf("failed to check existence of original table %s: %w", mainTableName, err)
		logErrorToFile("Ensure Single Temp Table", err, errorLogFile)
		return err
	}

	if !originalExists {
		return fmt.Errorf("original table %s does not exist, cannot create temp table", mainTableName)
	}

	log.Printf("Creating temp table %s from original table %s...", tempTableName, mainTableName)
	if err := CreateTempTableFromOriginal(clickhouseConn, mainTableName, tempTableName, config, errorLogFile); err != nil {
		err = fmt.Errorf("failed to create temp table %s: %w", tempTableName, err)
		logErrorToFile("Ensure Single Temp Table", err, errorLogFile)
		return err
	}

	log.Printf("✓ Successfully created temp table %s", tempTableName)

	log.Printf("Cleaning up backup tables for %s...", mainTableName)
	allBackupTables, err := GetAllBackupTables(clickhouseConn, config)
	if err != nil {
		log.Printf("WARNING: No backup tables found for %s", mainTableName)
	} else {
		backupPrefix := fmt.Sprintf("%s_backup_", mainTableName)
		deletedCount := 0
		for _, backupTable := range allBackupTables {
			if strings.HasPrefix(backupTable, backupPrefix) {
				log.Printf("Dropping backup table %s...", backupTable)
				if err := DropTable(clickhouseConn, backupTable, config, errorLogFile); err != nil {
					log.Printf("WARNING: Failed to drop backup table %s: %v", backupTable, err)
					continue
				}
				log.Printf("✓ Dropped backup table %s", backupTable)
				deletedCount++
			}
		}
		if deletedCount > 0 {
			log.Printf("✓ Cleaned up %d backup table(s) for %s", deletedCount, mainTableName)
		} else {
			log.Printf("✓ No backup tables found for %s", mainTableName)
		}
	}

	return nil
}
