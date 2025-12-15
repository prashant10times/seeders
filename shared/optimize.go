package shared

import (
	"context"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type TableOptimizeConfig struct {
	TableName             string
	TempTableName         string
	PartitionExpression   string
	DuplicateCheckColumns []string
}

func GetTableOptimizeConfigs() map[string]TableOptimizeConfig {
	return map[string]TableOptimizeConfig{
		"allevent_ch": {
			TableName:             "allevent_ch",
			TempTableName:         "allevent_temp",
			PartitionExpression:   "",
			DuplicateCheckColumns: []string{"published", "status", "edition_type", "event_id", "edition_id"},
		},
		"location_ch": {
			TableName:             "location_ch",
			TempTableName:         "location_temp",
			PartitionExpression:   "",
			DuplicateCheckColumns: []string{"id_uuid"},
		},
		"event_type_ch": {
			TableName:             "event_type_ch",
			TempTableName:         "event_type_temp",
			PartitionExpression:   "",
			DuplicateCheckColumns: []string{"eventtype_uuid", "event_id"},
		},
		"event_category_ch": {
			TableName:             "event_category_ch",
			TempTableName:         "event_category_temp",
			PartitionExpression:   "",
			DuplicateCheckColumns: []string{"is_group", "published", "category", "event"},
		},
		"event_ranking_ch": {
			TableName:             "event_ranking_ch",
			TempTableName:         "event_ranking_temp",
			PartitionExpression:   "",
			DuplicateCheckColumns: []string{"country", "category_name", "event_rank"},
		},
		"event_designation_ch": {
			TableName:             "event_designation_ch",
			TempTableName:         "event_designation_temp",
			PartitionExpression:   "",
			DuplicateCheckColumns: []string{"event_id", "edition_id", "designation_id"},
		},
		"event_exhibitor_ch": {
			TableName:             "event_exhibitor_ch",
			TempTableName:         "event_exhibitor_temp",
			PartitionExpression:   "",
			DuplicateCheckColumns: []string{"event_id", "edition_id", "user_id"},
		},
		"event_speaker_ch": {
			TableName:             "event_speaker_ch",
			TempTableName:         "event_speaker_temp",
			PartitionExpression:   "",
			DuplicateCheckColumns: []string{"event_id", "edition_id", "user_id"},
		},
		"event_sponsors_ch": {
			TableName:             "event_sponsors_ch",
			TempTableName:         "event_sponsors_temp",
			PartitionExpression:   "",
			DuplicateCheckColumns: []string{"event_id", "edition_id", "company_uuid"},
		},
		"event_visitors_ch": {
			TableName:             "event_visitors_ch",
			TempTableName:         "event_visitors_temp",
			PartitionExpression:   "",
			DuplicateCheckColumns: []string{"event_id", "edition_id", "user_id"},
		},
		"event_visitorSpread_ch": {
			TableName:             "event_visitorSpread_ch",
			TempTableName:         "event_visitorSpread_temp",
			PartitionExpression:   "",
			DuplicateCheckColumns: []string{"event_id"},
		},
	}
}

func ExtractPartitionExpression(createStatement string) (string, error) {
	createStatement = strings.ReplaceAll(createStatement, "\n", " ")
	createStatement = strings.ReplaceAll(createStatement, "\t", " ")
	createStatement = regexp.MustCompile(`\s+`).ReplaceAllString(createStatement, " ")

	re := regexp.MustCompile(`(?i)PARTITION\s+BY\s+`)
	match := re.FindStringIndex(createStatement)
	if match == nil {
		return "", fmt.Errorf("PARTITION BY clause not found in CREATE TABLE statement")
	}

	start := match[1]

	pos := start
	parenDepth := 0
	inString := false
	stringChar := byte(0)

	for pos < len(createStatement) {
		char := createStatement[pos]

		if !inString && (char == '\'' || char == '"') {
			inString = true
			stringChar = char
		} else if inString && char == stringChar {
			if pos+1 < len(createStatement) && createStatement[pos+1] == stringChar {
				pos++
			} else {
				inString = false
			}
		} else if !inString {
			if char == '(' {
				parenDepth++
			} else if char == ')' {
				parenDepth--
				if parenDepth == 0 {
					remaining := strings.TrimSpace(createStatement[pos+1:])
					remainingUpper := strings.ToUpper(remaining)
					if strings.HasPrefix(remainingUpper, "ORDER BY") || strings.HasPrefix(remainingUpper, "SETTINGS") {
						pos++
						break
					}
				}
			} else if parenDepth == 0 {
				remaining := strings.ToUpper(strings.TrimSpace(createStatement[pos:]))
				if strings.HasPrefix(remaining, "ORDER BY") || strings.HasPrefix(remaining, "SETTINGS") {
					break
				}
			}
		}
		pos++
	}

	partitionExpr := strings.TrimSpace(createStatement[start:pos])
	return partitionExpr, nil
}

func logOptimizeToFile(level, operationName, message string) {
	const optimizeLogFile = "optimize_logs.log"
	file, fileErr := os.OpenFile(optimizeLogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if fileErr != nil {
		log.Printf("WARNING: Failed to open optimization log file: %v", fileErr)
		return
	}
	defer file.Close()

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	logMsg := fmt.Sprintf("[%s] [%s] %s: %s\n", timestamp, level, operationName, message)
	if _, writeErr := file.WriteString(logMsg); writeErr != nil {
		log.Printf("WARNING: Failed to write to optimization log file: %v", writeErr)
	}
}

type PartitionInfo struct {
	Partition        string
	PartName         string
	Rows             uint64
	BytesOnDisk      uint64
	Size             string
	MinDate          string
	MaxDate          string
	ModificationTime string
	Level            uint32
}

func GetAllPartitionsFromSystemParts(clickhouseConn driver.Conn, tableName string, dbConfig Config) ([]PartitionInfo, error) {
	dbName := dbConfig.ClickhouseDB
	escapedTableName := strings.ReplaceAll(tableName, "'", "''")
	escapedDbName := strings.ReplaceAll(dbName, "'", "''")

	query := fmt.Sprintf(`
		SELECT
			partition,
			name AS part_name,
			rows,
			bytes_on_disk,
			formatReadableSize(bytes_on_disk) AS size,
			toString(min_date) AS min_date,
			toString(max_date) AS max_date,
			toString(modification_time) AS modification_time,
			level
		FROM system.parts
		WHERE active
			AND database = '%s'
			AND table = '%s'
		ORDER BY bytes_on_disk DESC
	`, escapedDbName, escapedTableName)

	log.Printf("Getting all partitions from system.parts for %s...", tableName)
	log.Printf("Query: %s", query)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	connectionCheckErr := RetryWithBackoff(
		func() error {
			return CheckClickHouseConnectionAlive(clickhouseConn)
		},
		3,
		fmt.Sprintf("ClickHouse connection health check for system.parts query %s", tableName),
	)
	if connectionCheckErr != nil {
		return nil, fmt.Errorf("ClickHouse connection is not alive after retries: %w", connectionCheckErr)
	}

	var partitions []PartitionInfo
	queryErr := RetryWithBackoff(
		func() error {
			rows, err := clickhouseConn.Query(ctx, query)
			if err != nil {
				return fmt.Errorf("failed to query system.parts: %w", err)
			}
			defer rows.Close()

			partitions = []PartitionInfo{}
			seenPartitions := make(map[string]bool)

			for rows.Next() {
				var part PartitionInfo
				if err := rows.Scan(
					&part.Partition,
					&part.PartName,
					&part.Rows,
					&part.BytesOnDisk,
					&part.Size,
					&part.MinDate,
					&part.MaxDate,
					&part.ModificationTime,
					&part.Level,
				); err != nil {
					return fmt.Errorf("failed to scan partition row: %w", err)
				}

				if !seenPartitions[part.Partition] {
					partitions = append(partitions, part)
					seenPartitions[part.Partition] = true
				}
			}

			if err := rows.Err(); err != nil {
				return fmt.Errorf("error iterating partition rows: %w", err)
			}

			return nil
		},
		3,
		fmt.Sprintf("get all partitions from system.parts for %s", tableName),
	)

	if queryErr != nil {
		return nil, queryErr
	}

	log.Printf("Found %d unique partitions in %s", len(partitions), tableName)
	return partitions, nil
}

func GetPartitionsWithDuplicates(clickhouseConn driver.Conn, config TableOptimizeConfig, dbConfig Config) ([]string, error) {
	fullTempTableName := GetTableNameWithDB(config.TempTableName, dbConfig)

	if config.PartitionExpression == "" {
		return nil, fmt.Errorf("partition expression not set for table %s", config.TableName)
	}

	if len(config.DuplicateCheckColumns) == 0 {
		return nil, fmt.Errorf("duplicate check columns not configured for table %s", config.TableName)
	}

	groupByColumns := []string{config.PartitionExpression}
	groupByColumns = append(groupByColumns, config.DuplicateCheckColumns...)
	groupByClause := strings.Join(groupByColumns, ", ")

	selectClause := fmt.Sprintf("(%s) AS partition", config.PartitionExpression)
	for _, col := range config.DuplicateCheckColumns {
		selectClause += fmt.Sprintf(", %s", col)
	}
	selectClause += ", count(*) AS cnt"

	query := fmt.Sprintf(`
		SELECT
			%s
		FROM %s
		GROUP BY
			%s
		HAVING cnt > 1
		ORDER BY partition, cnt DESC
	`, selectClause, fullTempTableName, groupByClause)

	log.Printf("Checking for duplicate partitions in %s...", config.TempTableName)
	log.Printf("Query: %s", query)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	connectionCheckErr := RetryWithBackoff(
		func() error {
			return CheckClickHouseConnectionAlive(clickhouseConn)
		},
		3,
		fmt.Sprintf("ClickHouse connection health check for duplicate detection %s", config.TempTableName),
	)
	if connectionCheckErr != nil {
		return nil, fmt.Errorf("ClickHouse connection is not alive after retries: %w", connectionCheckErr)
	}

	var partitions []string
	queryErr := RetryWithBackoff(
		func() error {
			rows, err := clickhouseConn.Query(ctx, query)
			if err != nil {
				return fmt.Errorf("failed to query duplicates: %w", err)
			}
			defer rows.Close()

			partitions = []string{}
			seenPartitions := make(map[string]bool)

			for rows.Next() {
				var partitionValue interface{}
				scanArgs := []interface{}{&partitionValue}

				for i := 0; i < len(config.DuplicateCheckColumns); i++ {
					var dummy interface{}
					scanArgs = append(scanArgs, &dummy)
				}

				var count interface{}
				scanArgs = append(scanArgs, &count)

				if err := rows.Scan(scanArgs...); err != nil {
					return fmt.Errorf("failed to scan duplicate row: %w", err)
				}

				partitionStr := fmt.Sprintf("%v", partitionValue)
				if !seenPartitions[partitionStr] {
					partitions = append(partitions, partitionStr)
					seenPartitions[partitionStr] = true
				}
			}

			if err := rows.Err(); err != nil {
				return fmt.Errorf("error iterating duplicate rows: %w", err)
			}

			return nil
		},
		3,
		fmt.Sprintf("get duplicate partitions for %s", config.TempTableName),
	)

	if queryErr != nil {
		return nil, queryErr
	}

	log.Printf("Found %d partitions with duplicates in %s", len(partitions), config.TempTableName)
	return partitions, nil
}

func formatPartitionValue(partitionValue string) string {
	if _, err := strconv.ParseFloat(partitionValue, 64); err == nil {
		return partitionValue
	}
	return fmt.Sprintf("'%s'", strings.ReplaceAll(partitionValue, "'", "''"))
}

func OptimizeTablePartition(clickhouseConn driver.Conn, tableName, partitionValue string, dbConfig Config, errorLogFile string) error {
	fullTableName := GetTableNameWithDB(tableName, dbConfig)
	formattedPartition := formatPartitionValue(partitionValue)
	query := fmt.Sprintf("OPTIMIZE TABLE %s PARTITION %s FINAL", fullTableName, formattedPartition)

	log.Printf("Starting OPTIMIZE for %s PARTITION %s", tableName, partitionValue)
	log.Printf("Query: %s", query)
	logOptimizeToFile("INFO", "Optimize Partition", fmt.Sprintf("Starting OPTIMIZE for %s PARTITION %s - Query: %s", tableName, partitionValue, query))

	connectionCheckErr := RetryWithBackoff(
		func() error {
			return CheckClickHouseConnectionAlive(clickhouseConn)
		},
		3,
		fmt.Sprintf("ClickHouse connection health check before OPTIMIZE %s PARTITION %s", tableName, partitionValue),
	)
	if connectionCheckErr != nil {
		err := fmt.Errorf("ClickHouse connection is not alive before OPTIMIZE: %w", connectionCheckErr)
		logErrorToFile("Optimize Partition", err, errorLogFile)
		logOptimizeToFile("ERROR", "Optimize Partition", fmt.Sprintf("ClickHouse connection is not alive before OPTIMIZE for %s PARTITION %s: %v", tableName, partitionValue, connectionCheckErr))
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		err := clickhouseConn.Exec(ctx, query)
		done <- err
	}()

	keepAliveTicker := time.NewTicker(30 * time.Second)
	defer keepAliveTicker.Stop()

	startTime := time.Now()
	for {
		select {
		case err := <-done:
			duration := time.Since(startTime)
			if err != nil {
				errMsg := fmt.Errorf("OPTIMIZE failed for %s PARTITION %s after %v: %w", tableName, partitionValue, duration, err)
				logErrorToFile("Optimize Partition", errMsg, errorLogFile)
				logOptimizeToFile("ERROR", "Optimize Partition", fmt.Sprintf("OPTIMIZE failed for %s PARTITION %s after %v: %v", tableName, partitionValue, duration, err))
				return errMsg
			}
			successMsg := fmt.Sprintf("OPTIMIZE completed for %s PARTITION %s in %v", tableName, partitionValue, duration)
			log.Printf("✓ %s", successMsg)
			logOptimizeToFile("SUCCESS", "Optimize Partition", successMsg)
			return nil

		case <-keepAliveTicker.C:
			keepAliveCtx, keepAliveCancel := context.WithTimeout(context.Background(), 10*time.Second)
			_ = clickhouseConn.QueryRow(keepAliveCtx, "SELECT 1")
			keepAliveCancel()
			elapsed := time.Since(startTime)
			log.Printf("OPTIMIZE in progress for %s PARTITION %s (elapsed: %v)...", tableName, partitionValue, elapsed)

		case <-ctx.Done():
			duration := time.Since(startTime)
			err := fmt.Errorf("OPTIMIZE timeout for %s PARTITION %s after %v: %w", tableName, partitionValue, duration, ctx.Err())
			logErrorToFile("Optimize Partition", err, errorLogFile)
			logOptimizeToFile("ERROR", "Optimize Partition", fmt.Sprintf("OPTIMIZE timeout for %s PARTITION %s after %v: %v", tableName, partitionValue, duration, ctx.Err()))
			return err
		}
	}
}

func OptimizeTablePartitions(clickhouseConn driver.Conn, optimizeConfig TableOptimizeConfig, dbConfig Config, errorLogFile string) error {
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	log.Printf("OPTIMIZING TABLE: %s", optimizeConfig.TableName)
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	logOptimizeToFile("INFO", "Optimize Table", fmt.Sprintf("Starting optimization for table: %s (temp table: %s)", optimizeConfig.TableName, optimizeConfig.TempTableName))
	partitionInfos, err := GetAllPartitionsFromSystemParts(clickhouseConn, optimizeConfig.TempTableName, dbConfig)
	if err != nil {
		errMsg := fmt.Errorf("failed to get partitions from system.parts for %s: %w", optimizeConfig.TableName, err)
		logErrorToFile("Optimize Table", errMsg, errorLogFile)
		logOptimizeToFile("ERROR", "Optimize Table", fmt.Sprintf("Failed to get partitions from system.parts for %s: %v", optimizeConfig.TableName, err))
		return errMsg
	}

	if len(partitionInfos) == 0 {
		noPartitionsMsg := fmt.Sprintf("No active partitions found in %s, skipping optimization", optimizeConfig.TableName)
		log.Printf("⚠️  %s", noPartitionsMsg)
		logOptimizeToFile("INFO", "Optimize Table", noPartitionsMsg)
		return nil
	}

	log.Printf("Found %d partitions in %s:", len(partitionInfos), optimizeConfig.TableName)
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	log.Printf("%-15s %-20s %-12s %-15s %-12s %-12s %-10s", "PARTITION", "PART_NAME", "ROWS", "SIZE", "MIN_DATE", "MAX_DATE", "LEVEL")
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	for _, part := range partitionInfos {
		log.Printf("%-15s %-20s %-12d %-15s %-12s %-12s %-10d",
			part.Partition, part.PartName, part.Rows, part.Size, part.MinDate, part.MaxDate, part.Level)
	}
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	log.Println("")

	uniquePartitions := make(map[string]bool)
	var allPartitions []string
	for _, part := range partitionInfos {
		if !uniquePartitions[part.Partition] {
			allPartitions = append(allPartitions, part.Partition)
			uniquePartitions[part.Partition] = true
		}
	}

	var duplicatePartitions []string
	var partitionsWithDuplicatesSet map[string]bool
	createStatement, err := GetTableCreateStatement(clickhouseConn, optimizeConfig.TempTableName, dbConfig)
	if err == nil {
		partitionExpr, err := ExtractPartitionExpression(createStatement)
		if err == nil {
			optimizeConfig.PartitionExpression = partitionExpr
			duplicatePartitions, err = GetPartitionsWithDuplicates(clickhouseConn, optimizeConfig, dbConfig)
			if err == nil {
				partitionsWithDuplicatesSet = make(map[string]bool)
				for _, dupPart := range duplicatePartitions {
					partitionsWithDuplicatesSet[dupPart] = true
				}
				if len(duplicatePartitions) > 0 {
					log.Printf("⚠️  Found %d partitions with duplicates (PRIORITY - will optimize first): %v", len(duplicatePartitions), duplicatePartitions)
				} else {
					log.Printf("✓ No partitions with duplicates found")
				}
			}
		}
	}

	var priorityPartitions []string
	var normalPartitions []string

	for _, partition := range allPartitions {
		if partitionsWithDuplicatesSet != nil && partitionsWithDuplicatesSet[partition] {
			priorityPartitions = append(priorityPartitions, partition)
		} else {
			normalPartitions = append(normalPartitions, partition)
		}
	}

	totalPartitions := len(priorityPartitions) + len(normalPartitions)
	priorityMsg := fmt.Sprintf("Optimization priority: %d partitions with duplicates (first), then %d normal partitions. Total: %d partitions to optimize in %s", len(priorityPartitions), len(normalPartitions), totalPartitions, optimizeConfig.TableName)
	log.Printf("Optimization priority: %d partitions with duplicates (first), then %d normal partitions", len(priorityPartitions), len(normalPartitions))
	log.Printf("Total: %d partitions to optimize in %s", totalPartitions, optimizeConfig.TableName)
	logOptimizeToFile("INFO", "Optimize Table", priorityMsg)
	log.Println("")

	partitionsToOptimize := append(priorityPartitions, normalPartitions...)

	for i, partition := range partitionsToOptimize {
		var partInfo *PartitionInfo
		for _, part := range partitionInfos {
			if part.Partition == partition {
				partInfo = &part
				break
			}
		}
		
		isPriority := partitionsWithDuplicatesSet != nil && partitionsWithDuplicatesSet[partition]
		priorityLabel := ""
		if isPriority {
			priorityLabel = " [PRIORITY - HAS DUPLICATES]"
		}

		log.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		if partInfo != nil {
			log.Printf("Optimizing partition %d/%d: %s (partition: %s, rows: %d, size: %s, level: %d)%s",
				i+1, len(partitionsToOptimize), optimizeConfig.TableName, partition, partInfo.Rows, partInfo.Size, partInfo.Level, priorityLabel)
		} else {
			log.Printf("Optimizing partition %d/%d: %s (partition value: %s)%s", i+1, len(partitionsToOptimize), optimizeConfig.TableName, partition, priorityLabel)
		}
		log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

		optimizeErr := RetryWithBackoff(
			func() error {
				return OptimizeTablePartition(clickhouseConn, optimizeConfig.TempTableName, partition, dbConfig, errorLogFile)
			},
			3,
			fmt.Sprintf("optimize partition %s for %s", partition, optimizeConfig.TableName),
		)

		if optimizeErr != nil {
			errMsg := fmt.Errorf("failed to optimize partition %s for %s after retries: %w", partition, optimizeConfig.TableName, optimizeErr)
			logErrorToFile("Optimize Table", errMsg, errorLogFile)
			logOptimizeToFile("ERROR", "Optimize Table", fmt.Sprintf("Failed to optimize partition %s for %s after retries: %v", partition, optimizeConfig.TableName, optimizeErr))
			log.Printf("⚠️  Error optimizing partition %s for %s: %v", partition, optimizeConfig.TableName, optimizeErr)
			log.Printf("⚠️  Continuing with next partition...")
		} else {
			successMsg := fmt.Sprintf("Successfully optimized partition %s for %s", partition, optimizeConfig.TableName)
			log.Printf("✓ %s", successMsg)
			logOptimizeToFile("SUCCESS", "Optimize Table", successMsg)
		}
		log.Println("")
	}

	completionMsg := fmt.Sprintf("Completed optimization for %s (%d partitions processed)", optimizeConfig.TableName, len(partitionsToOptimize))
	log.Printf("✓ %s", completionMsg)
	logOptimizeToFile("SUCCESS", "Optimize Table", completionMsg)
	return nil
}

func OptimizeAllPhase1Tables(clickhouseConn driver.Conn, config Config, errorLogFile string) error {
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	log.Println("OPTIMIZING ALL PHASE 1 TABLES - REMOVING DUPLICATES")
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	log.Println("")
	logOptimizeToFile("INFO", "Optimize All Phase 1 Tables", "Starting optimization for all Phase 1 tables")

	optimizeConfigs := GetTableOptimizeConfigs()
	phase1Tables := []string{
		"location_ch",
		"event_type_ch",
		"allevent_ch",
		"event_category_ch",
		"event_ranking_ch",
		"event_designation_ch",
		"event_exhibitor_ch",
		"event_speaker_ch",
		"event_sponsors_ch",
		"event_visitors_ch",
		"event_visitorSpread_ch",
	}

	for i, tableName := range phase1Tables {
		optimizeConfig, exists := optimizeConfigs[tableName]
		if !exists {
			log.Printf("⚠️  No optimization config found for %s, skipping", tableName)
			continue
		}

		log.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		log.Printf("Processing table %d/%d: %s", i+1, len(phase1Tables), tableName)
		log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

		if err := OptimizeTablePartitions(clickhouseConn, optimizeConfig, config, errorLogFile); err != nil {
			logOptimizeToFile("ERROR", "Optimize All Phase 1 Tables", fmt.Sprintf("Error optimizing %s: %v", tableName, err))
			log.Printf("⚠️  Error optimizing %s: %v", tableName, err)
			log.Printf("⚠️  Continuing with next table...")
		} else {
			logOptimizeToFile("SUCCESS", "Optimize All Phase 1 Tables", fmt.Sprintf("Successfully completed optimization for %s", tableName))
		}

		log.Println("")
	}

	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	log.Println("✓ COMPLETED OPTIMIZATION FOR ALL PHASE 1 TABLES")
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	log.Println("")
	logOptimizeToFile("SUCCESS", "Optimize All Phase 1 Tables", "Completed optimization for all Phase 1 tables")

	return nil
}
