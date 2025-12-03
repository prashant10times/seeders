package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"seeders/microservice"
	"seeders/shared"
	"seeders/utils"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/elastic/go-elasticsearch/v6"
	_ "github.com/go-sql-driver/mysql"
)

const errorLogFile = "seeding_errors.log"

func logErrorToFile(scriptName string, err error) {
	file, fileErr := os.OpenFile(errorLogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if fileErr != nil {
		log.Printf("WARNING: Failed to open error log file: %v", fileErr)
		return
	}
	defer file.Close()

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	errorMsg := fmt.Sprintf("[%s] ERROR in %s: %v\n", timestamp, scriptName, err)
	if _, writeErr := file.WriteString(errorMsg); writeErr != nil {
		log.Printf("WARNING: Failed to write to error log file: %v", writeErr)
	}
	log.Printf("ERROR logged to %s: %s - %v", errorLogFile, scriptName, err)
}

func runAllScripts(mysqlDB *sql.DB, clickhouseDB driver.Conn, esClient *elasticsearch.Client, config shared.Config) {
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	log.Println("=== STARTING ALL SEEDING SCRIPTS IN ORDER ===")
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	log.Println("")

	if err := os.Remove(errorLogFile); err != nil && !os.IsNotExist(err) {
		log.Printf("WARNING: Failed to remove existing error log file: %v", err)
	}

	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	log.Println("STEP 0: PREPARING DATABASE - ENSURING TEMP TABLES EXIST")
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	if err := shared.EnsureTempTablesExist(clickhouseDB, config, errorLogFile); err != nil {
		logErrorToFile("Ensure Temp Tables", err)
		log.Fatalf("Failed to ensure temp tables exist: %v", err)
	}
	log.Println("✓ STEP 0 (PREPARE DATABASE) COMPLETED SUCCESSFULLY")
	log.Println("")

	scripts := []struct {
		name     string
		critical bool
		run      func() error
	}{
		{
			name:     "Location",
			critical: true,
			run: func() error {
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				log.Println("STEP 1/12: PROCESSING LOCATION")
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				locConfig := shared.Config{
					BatchSize:         config.BatchSize,
					NumChunks:         config.NumChunks,
					NumWorkers:        config.NumWorkers,
					ClickHouseWorkers: config.ClickHouseWorkers,
				}
				nextID := microservice.ProcessLocationCountriesCh(mysqlDB, clickhouseDB, locConfig, 1)
				nextID = microservice.ProcessLocationStatesCh(mysqlDB, clickhouseDB, locConfig, nextID)
				nextID = microservice.ProcessLocationCitiesCh(mysqlDB, clickhouseDB, locConfig, nextID)
				nextID = microservice.ProcessLocationVenuesCh(mysqlDB, clickhouseDB, locConfig, nextID)
				microservice.ProcessLocationSubVenuesCh(mysqlDB, clickhouseDB, locConfig, nextID)
				log.Println("✓ STEP 1/12 (LOCATION) COMPLETED SUCCESSFULLY")
				log.Println("")
				return nil
			},
		},
		// 2. Event Type
		{
			name:     "Event Type",
			critical: false,
			run: func() error {
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				log.Println("STEP 2/12: PROCESSING EVENT TYPE")
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				utilsConfig := shared.Config{
					BatchSize:         config.BatchSize,
					NumChunks:         config.NumChunks,
					NumWorkers:        config.NumWorkers,
					ClickHouseWorkers: config.ClickHouseWorkers,
				}
				utils.ProcessEventTypeEventChOnly(mysqlDB, clickhouseDB, utilsConfig)
				log.Println("✓ STEP 2/12 (EVENT TYPE) COMPLETED SUCCESSFULLY")
				log.Println("")
				return nil
			},
		},
		// 3. All Event (CRITICAL - break on error)
		{
			name:     "All Event",
			critical: true,
			run: func() error {
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				log.Println("STEP 3/12: PROCESSING ALL EVENT")
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				utilsConfig := shared.Config{
					BatchSize:          config.BatchSize,
					NumChunks:          config.NumChunks,
					NumWorkers:         config.NumWorkers,
					ClickHouseWorkers:  config.ClickHouseWorkers,
					ElasticsearchIndex: config.ElasticsearchIndex,
				}
				microservice.ProcessAllEventOnly(mysqlDB, clickhouseDB, esClient, utilsConfig)
				log.Println("✓ STEP 3/12 (ALL EVENT) COMPLETED SUCCESSFULLY")
				log.Println("")
				return nil
			},
		},
		// 4. Event Category
		{
			name:     "Event Category",
			critical: false,
			run: func() error {
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				log.Println("STEP 4/12: PROCESSING EVENT CATEGORY")
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				utilsConfig := shared.Config{
					BatchSize:         config.BatchSize,
					NumChunks:         config.NumChunks,
					NumWorkers:        config.NumWorkers,
					ClickHouseWorkers: config.ClickHouseWorkers,
				}
				utils.ProcessEventCategoryEventChOnly(mysqlDB, clickhouseDB, utilsConfig)
				log.Println("✓ STEP 4/12 (EVENT CATEGORY) COMPLETED SUCCESSFULLY")
				log.Println("")
				return nil
			},
		},
		// 5. Event Ranking
		{
			name:     "Event Ranking",
			critical: false,
			run: func() error {
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				log.Println("STEP 5/12: PROCESSING EVENT RANKING")
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				utilsConfig := shared.Config{
					BatchSize:         config.BatchSize,
					NumChunks:         config.NumChunks,
					NumWorkers:        config.NumWorkers,
					ClickHouseWorkers: config.ClickHouseWorkers,
				}
				utils.ProcessEventRankingOnly(mysqlDB, clickhouseDB, utilsConfig)
				log.Println("✓ STEP 5/12 (EVENT RANKING) COMPLETED SUCCESSFULLY")
				log.Println("")
				return nil
			},
		},
		// 6. Event Designation
		{
			name:     "Event Designation",
			critical: false,
			run: func() error {
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				log.Println("STEP 6/12: PROCESSING EVENT DESIGNATION")
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				utilsConfig := shared.Config{
					BatchSize:         config.BatchSize,
					NumChunks:         config.NumChunks,
					NumWorkers:        config.NumWorkers,
					ClickHouseWorkers: config.ClickHouseWorkers,
				}
				utils.ProcessEventDesignationOnly(mysqlDB, clickhouseDB, utilsConfig)
				log.Println("✓ STEP 6/12 (EVENT DESIGNATION) COMPLETED SUCCESSFULLY")
				log.Println("")
				return nil
			},
		},
		// 7. Exhibitor
		{
			name:     "Exhibitor",
			critical: false,
			run: func() error {
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				log.Println("STEP 7/12: PROCESSING EXHIBITOR")
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				utilsConfig := shared.Config{
					BatchSize:         config.BatchSize,
					NumChunks:         config.NumChunks,
					NumWorkers:        config.NumWorkers,
					ClickHouseWorkers: config.ClickHouseWorkers,
				}
				utils.ProcessExhibitorOnly(mysqlDB, clickhouseDB, utilsConfig)
				log.Println("✓ STEP 7/12 (EXHIBITOR) COMPLETED SUCCESSFULLY")
				log.Println("")
				return nil
			},
		},
		// 8. Speaker
		{
			name:     "Speaker",
			critical: false,
			run: func() error {
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				log.Println("STEP 8/12: PROCESSING SPEAKER")
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				processSpeakersOnly(mysqlDB, clickhouseDB, config)
				log.Println("✓ STEP 8/12 (SPEAKER) COMPLETED SUCCESSFULLY")
				log.Println("")
				return nil
			},
		},
		// 9. Sponsor
		{
			name:     "Sponsor",
			critical: false,
			run: func() error {
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				log.Println("STEP 9/12: PROCESSING SPONSOR")
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				utilsConfig := shared.Config{
					BatchSize:         config.BatchSize,
					NumChunks:         config.NumChunks,
					NumWorkers:        config.NumWorkers,
					ClickHouseWorkers: config.ClickHouseWorkers,
				}
				utils.ProcessSponsorsOnly(mysqlDB, clickhouseDB, utilsConfig)
				log.Println("✓ STEP 9/12 (SPONSOR) COMPLETED SUCCESSFULLY")
				log.Println("")
				return nil
			},
		},
		// 10. Visitors
		{
			name:     "Visitors",
			critical: false,
			run: func() error {
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				log.Println("STEP 10/12: PROCESSING VISITORS")
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				processVisitorsOnly(mysqlDB, clickhouseDB, config)
				log.Println("✓ STEP 10/12 (VISITORS) COMPLETED SUCCESSFULLY")
				log.Println("")
				return nil
			},
		},
		// 11. Visitor Spread
		{
			name:     "Visitor Spread",
			critical: false,
			run: func() error {
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				log.Println("STEP 11/12: PROCESSING VISITOR SPREAD")
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				utilsConfig := shared.Config{
					BatchSize:          config.BatchSize,
					NumChunks:          config.NumChunks,
					NumWorkers:         config.NumWorkers,
					ClickHouseWorkers:  config.ClickHouseWorkers,
					ElasticsearchIndex: config.ElasticsearchIndex,
				}
				utils.ProcessVisitorSpreadOnly(mysqlDB, clickhouseDB, esClient, utilsConfig)
				log.Println("✓ STEP 11/12 (VISITOR SPREAD) COMPLETED SUCCESSFULLY")
				log.Println("")
				return nil
			},
		},
		// 12. Holidays
		{
			name:     "Holidays",
			critical: false,
			run: func() error {
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				log.Println("STEP 12/13: PROCESSING HOLIDAYS")
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				holidayConfig := shared.Config{
					BatchSize:         config.BatchSize,
					NumChunks:         config.NumChunks,
					NumWorkers:        config.NumWorkers,
					ClickHouseWorkers: config.ClickHouseWorkers,
				}
				microservice.ProcessHolidays(mysqlDB, clickhouseDB, holidayConfig)
				log.Println("✓ STEP 12/13 (HOLIDAYS) COMPLETED SUCCESSFULLY")
				log.Println("")
				return nil
			},
		},
		// 13. Alerts
		{
			name:     "Alerts",
			critical: false,
			run: func() error {
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				log.Println("STEP 13/13: PROCESSING ALERTS")
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				gdacBaseURL := os.Getenv("gdac_base_url")
				gdacEndpoint := os.Getenv("gdac_event_search_endpoint")

				if gdacBaseURL == "" {
					return fmt.Errorf("GDAC_BASE_URL environment variable is not set")
				}
				if gdacEndpoint == "" {
					return fmt.Errorf("GDAC_EVENT_SEARCH_ENDPOINT environment variable is not set")
				}

				validCountries, err := microservice.GetValidCountries()
				if err != nil {
					return fmt.Errorf("failed to get valid countries: %v", err)
				}

				if len(validCountries) == 0 {
					return fmt.Errorf("no valid countries found")
				}

				if err := microservice.ProcessAlertsFromAPI(clickhouseDB, gdacBaseURL, gdacEndpoint, validCountries); err != nil {
					return err
				}
				log.Println("✓ STEP 13/13 (ALERTS) COMPLETED SUCCESSFULLY")
				log.Println("")
				return nil
			},
		},
	}

	for i, script := range scripts {
		log.Printf("Starting script %d/%d: %s", i+1, len(scripts), script.name)

		err := func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic occurred: %v", r)
				}
			}()
			return script.run()
		}()

		if err != nil {
			if script.critical {
				log.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				log.Printf("FATAL ERROR in CRITICAL script: %s", script.name)
				log.Printf("Error: %v", err)
				log.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				log.Fatalf("Stopping execution due to critical error in %s: %v", script.name, err)
			} else {
				logErrorToFile(script.name, err)
				log.Printf("⚠️  Non-critical error in %s (logged to %s), continuing...", script.name, errorLogFile)
			}
		}

		if i == 10 {
			log.Println("")
			log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
			log.Println("VALIDATION: Validating all first batch temp tables before swap")
			log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

			firstBatchTables := []string{
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

			var firstBatchMappings []shared.TableMapping
			for _, tableName := range firstBatchTables {
				firstBatchMappings = append(firstBatchMappings, shared.GetTableMapping(tableName, config))
			}

			if err := shared.ValidateTempTables(clickhouseDB, firstBatchMappings, config, errorLogFile); err != nil {
				logErrorToFile("Pre-Swap Validation", err)
				log.Fatalf("Validation failed before batch swap: %v", err)
			}

			log.Println("✓ All temp tables validated successfully, proceeding with batch swap")
			log.Println("")

			log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
			log.Println("BATCH SWAP: Swapping all first batch tables in one go")
			log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

			if err := shared.SwapTables(clickhouseDB, firstBatchMappings, config, errorLogFile); err != nil {
				logErrorToFile("First Batch Table Swap", err)
				log.Fatalf("Failed to swap first batch tables: %v", err)
			}

			log.Println("✓ All first batch tables swapped successfully")
			log.Println("")
		}
	}

	// After all scripts complete, swap second batch (alerts and location_polygons)
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	log.Println("VALIDATION: Validating second batch temp tables before swap")
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	secondBatchTables := []string{
		"alerts_ch",
		"location_polygons_ch",
	}

	var secondBatchMappings []shared.TableMapping
	for _, tableName := range secondBatchTables {
		secondBatchMappings = append(secondBatchMappings, shared.GetTableMapping(tableName, config))
	}

	if err := shared.ValidateTempTables(clickhouseDB, secondBatchMappings, config, errorLogFile); err != nil {
		logErrorToFile("Pre-Swap Validation (Second Batch)", err)
		log.Printf("⚠️  Validation warning for second batch (may not exist yet): %v", err)
	} else {
		log.Println("✓ All second batch temp tables validated successfully, proceeding with batch swap")
		log.Println("")

		log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		log.Println("BATCH SWAP: Swapping second batch tables (alerts, location_polygons)")
		log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

		if err := shared.SwapTables(clickhouseDB, secondBatchMappings, config, errorLogFile); err != nil {
			logErrorToFile("Second Batch Table Swap", err)
			log.Printf("⚠️  Failed to swap second batch tables: %v", err)
		} else {
			log.Println("✓ All second batch tables swapped successfully")
		}
		log.Println("")
	}

	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	log.Println("=== ALL SEEDING SCRIPTS COMPLETED ===")
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	if _, err := os.Stat(errorLogFile); err == nil {
		log.Printf("Note: Check %s for any errors that occurred during non-critical scripts", errorLogFile)
	}
}

func processVisitorsOnly(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) {
	log.Println("=== Starting VISITORS ONLY Processing ===")

	totalRecords, minID, maxID, err := shared.GetTotalRecordsAndIDRange(mysqlDB, "event_visitor")
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
func processVisitorsChunk(mysqlDB *sql.DB, _ driver.Conn, config shared.Config, startID, endID int, chunkNum int, results chan<- string) {
	log.Printf("Processing visitors chunk %d: ID range %d-%d", chunkNum, startID, endID)

	// Create a dedicated ClickHouse connection for this goroutine
	chConn, err := utils.SetupNativeClickHouseConnection(config)
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
			cityData = shared.FetchCityDataParallel(mysqlDB, visitorCityIDs, config.NumWorkers)
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
		now := time.Now().Format("2006-01-02 15:04:05")
		for _, visitor := range batchData {
			var userName, userCompany interface{}
			if userID, ok := visitor["user"].(int64); ok && userData != nil && userID > 0 {
				if user, exists := userData[userID]; exists {
					userName = user["name"]
					userCompany = user["user_company"]
					if userName == nil || shared.ConvertToString(userName) == "" {
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
					nameStr := shared.SafeConvertToString(city["name"])
					userCityName = &nameStr
				}
			}

			var userStateID *uint32
			var userState *string
			if cityID, ok := visitor["visitor_city"].(int64); ok && cityLookup != nil {
				if city, exists := cityLookup[cityID]; exists {
					if city["state_id"] != nil {
						if stateID, ok := city["state_id"].(int64); ok && stateID > 0 {
							stateIDUint32 := uint32(stateID)
							userStateID = &stateIDUint32
						}
					}
					if city["state"] != nil {
						stateStr := shared.SafeConvertToString(city["state"])
						if strings.TrimSpace(stateStr) != "" {
							userState = &stateStr
						}
					}
				}
			}

			userID := shared.ConvertToUInt32(visitor["user"])
			eventID := shared.ConvertToUInt32(visitor["event"])
			editionID := shared.ConvertToUInt32(visitor["edition"])

			convertedUserName := shared.ConvertToString(userName)

			visitorRecord := VisitorRecord{
				UserID:          userID,
				EventID:         eventID,
				EditionID:       editionID,
				UserName:        convertedUserName,
				UserCompany:     shared.ConvertToStringPtr(userCompany),
				UserDesignation: shared.ConvertToStringPtr(visitor["visitor_designation"]),
				UserCity:        shared.ConvertToUInt32Ptr(visitor["visitor_city"]),
				UserCityName:    userCityName,
				UserCountry:     shared.ToUpperNullableString(shared.ConvertToStringPtr(visitor["visitor_country"])),
				UserStateID:     userStateID,
				UserState:       userState,
				Version:         1,
				LastUpdatedAt:   now,
			}

			visitorRecords = append(visitorRecords, visitorRecord)
		}

		if len(visitorRecords) > 0 {
			log.Printf("Visitors chunk %d: Attempting to insert %d records into event_visitor_ch...", chunkNum, len(visitorRecords))

			attemptCount := 0
			visitorInsertErr := shared.RetryWithBackoff(
				func() error {
					if attemptCount > 0 {
						now := time.Now().Format("2006-01-02 15:04:05")
						for i := range visitorRecords {
							visitorRecords[i].LastUpdatedAt = now
						}
						log.Printf("Visitors chunk %d: Updated last_updated_at for retry attempt %d", chunkNum, attemptCount+1)
					}
					attemptCount++
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

	log.Printf("Checking ClickHouse connection health before inserting %d event_visitors_ch records", len(visitorRecords))
	connectionCheckErr := shared.RetryWithBackoff(
		func() error {
			return shared.CheckClickHouseConnectionAlive(clickhouseConn)
		},
		3,
		"ClickHouse connection health check for event_visitors_ch",
	)
	if connectionCheckErr != nil {
		return fmt.Errorf("ClickHouse connection is not alive after retries: %w", connectionCheckErr)
	}
	log.Printf("ClickHouse connection is alive, proceeding with event_visitors_ch batch insert")

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

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	batch, err := clickhouseConn.PrepareBatch(ctx, `
		INSERT INTO event_visitors_temp (
			user_id, event_id, edition_id, user_name, user_company,
			user_designation, user_city, user_city_name, user_country, user_state_id, user_state, version, last_updated_at
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
			record.UserStateID,     // user_state_id: UInt32
			record.UserState,       // user_state: LowCardinality(Nullable(String))
			record.Version,         // version: UInt32 NOT NULL DEFAULT 1
			record.LastUpdatedAt,   // last_updated_at: DateTime
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

func processSpeakersOnly(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) {
	log.Println("=== Starting SPEAKERS ONLY Processing ===")

	totalRecords, minID, maxID, err := shared.GetTotalRecordsAndIDRange(mysqlDB, "event_speaker")
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

func processSpeakersChunk(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config, startID, endID int, chunkNum int, results chan<- string) {
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
			cityData = shared.FetchCityDataParallel(mysqlDB, speakerCityIDs, config.NumWorkers)
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
		now := time.Now().Format("2006-01-02 15:04:05")
		for _, speaker := range batchData {
			// Get user data for this speaker
			var userName, userCompany, userDesignation, userCity, userCountry interface{}
			if userID, ok := speaker["user_id"].(int64); ok && userData != nil {
				if user, exists := userData[userID]; exists {
					userName = speaker["speaker_name"] // Use speaker_name from speaker table
					userCompany = user["user_company"] // Use user_company from user table
					userDesignation = user["designation"]
					userCity = user["city"]
					userCountry = strings.ToUpper(shared.SafeConvertToString(user["country"]))
				}
			}

			// Get city data for this speaker
			var userCityName *string
			if userCity != nil {
				if cityID, ok := userCity.(int64); ok && cityLookup != nil {
					if city, exists := cityLookup[cityID]; exists && city["name"] != nil {
						nameStr := shared.SafeConvertToString(city["name"])
						userCityName = &nameStr
					}
				}
			}

			var userStateID *uint32
			var userState *string
			if userCity != nil {
				if cityID, ok := userCity.(int64); ok && cityLookup != nil {
					if city, exists := cityLookup[cityID]; exists {
						if city["state_id"] != nil {
							if stateID, ok := city["state_id"].(int64); ok && stateID > 0 {
								stateIDUint32 := uint32(stateID)
								userStateID = &stateIDUint32
							}
						}
						if city["state"] != nil {
							stateStr := shared.SafeConvertToString(city["state"])
							if strings.TrimSpace(stateStr) != "" {
								userState = &stateStr
							}
						}
					}
				}
			}

			userID := shared.ConvertToUInt32(speaker["user_id"])
			eventID := shared.ConvertToUInt32(speaker["event"])
			editionID := shared.ConvertToUInt32(speaker["edition"])

			speakerRecord := SpeakerRecord{
				UserID:          userID,
				EventID:         eventID,
				EditionID:       editionID,
				UserName:        shared.ConvertToString(userName),
				UserCompany:     shared.ConvertToStringPtr(userCompany),
				UserDesignation: shared.ConvertToStringPtr(userDesignation),
				UserState:       userStateID,
				UserStateName:   userState,
				UserCity:        shared.ConvertToUInt32Ptr(userCity),
				UserCityName:    userCityName,
				UserCountry:     shared.ToUpperNullableString(shared.ConvertToStringPtr(userCountry)),
				Version:         1,
				LastUpdatedAt:   now,
			}

			speakerRecords = append(speakerRecords, speakerRecord)
		}

		// Insert speakers data into ClickHouse
		if len(speakerRecords) > 0 {
			log.Printf("Speakers chunk %d: Attempting to insert %d records into event_speaker_ch...", chunkNum, len(speakerRecords))

			attemptCount := 0
			speakerInsertErr := shared.RetryWithBackoff(
				func() error {
					if attemptCount > 0 {
						now := time.Now().Format("2006-01-02 15:04:05")
						for i := range batchData {
							batchData[i]["last_updated_at"] = now
						}
						log.Printf("Speakers chunk %d: Updated last_updated_at for retry attempt %d", chunkNum, attemptCount+1)
					}
					attemptCount++
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

	log.Printf("Checking ClickHouse connection health before inserting %d event_speaker_ch records", len(speakerRecords))
	connectionCheckErr := shared.RetryWithBackoff(
		func() error {
			return shared.CheckClickHouseConnectionAlive(clickhouseConn)
		},
		3,
		"ClickHouse connection health check for event_speaker_ch",
	)
	if connectionCheckErr != nil {
		return fmt.Errorf("ClickHouse connection is not alive after retries: %w", connectionCheckErr)
	}
	log.Printf("ClickHouse connection is alive, proceeding with event_speaker_ch batch insert")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	batch, err := clickhouseConn.PrepareBatch(ctx, `
		INSERT INTO event_speaker_temp (
			user_id, event_id, edition_id, user_name, user_company,
			user_designation, user_state, user_state_name, user_city, user_city_name, user_country, version, last_updated_at
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
			record.UserState,       // user_state: Nullable(UInt32)
			record.UserStateName,   // user_state_name: LowCardinality(Nullable(String))
			record.UserCity,        // user_city: Nullable(UInt32)
			record.UserCityName,    // user_city_name: LowCardinality(Nullable(String))
			record.UserCountry,     // user_country: LowCardinality(Nullable(FixedString(2)))
			record.Version,         // version: UInt32 NOT NULL DEFAULT 1
			record.LastUpdatedAt,   // last_updated_at: DateTime
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

// SpeakerRecord represents a speaker record for ClickHouse insertion
type SpeakerRecord struct {
	UserID          uint32  `ch:"user_id"`
	EventID         uint32  `ch:"event_id"`
	EditionID       uint32  `ch:"edition_id"`
	UserName        string  `ch:"user_name"`
	UserCompany     *string `ch:"user_company"`
	UserDesignation *string `ch:"user_designation"`
	UserState       *uint32 `ch:"user_state"`
	UserStateName   *string `ch:"user_state_name"`
	UserCity        *uint32 `ch:"user_city"`
	UserCityName    *string `ch:"user_city_name"`
	UserCountry     *string `ch:"user_country"`
	Version         uint32  `ch:"version"`
	LastUpdatedAt   string  `ch:"last_updated_at"`
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
	UserStateID     *uint32 `ch:"user_state_id"`
	UserState       *string `ch:"user_state"`
	Version         uint32  `ch:"version"`
	LastUpdatedAt   string  `ch:"last_updated_at"`
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
	var locationCountriesOnly bool
	var locationStatesOnly bool
	var locationCitiesOnly bool
	var locationVenuesOnly bool
	var locationSubVenuesOnly bool
	var locationAll bool
	var eventRankingOnly bool
	var eventDesignationOnly bool
	var visitorSpreadOnly bool
	var allEventOnly bool
	var holidaysOnly bool
	var alertsOnly bool
	var allScripts bool

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
	flag.BoolVar(&locationCountriesOnly, "location-countries", false, "Process only location countries into location_ch (default: false)")
	flag.BoolVar(&locationStatesOnly, "location-states", false, "Process only location states into location_ch (default: false)")
	flag.BoolVar(&locationCitiesOnly, "location-cities", false, "Process only location cities into location_ch (default: false)")
	flag.BoolVar(&locationVenuesOnly, "location-venues", false, "Process only location venues into location_ch (default: false)")
	flag.BoolVar(&locationSubVenuesOnly, "location-sub-venues", false, "Process only location sub-venues into location_ch (default: false)")
	flag.BoolVar(&locationAll, "location", false, "Process all location types (countries, states, cities, venues, sub-venues) into location_ch (default: false)")
	flag.BoolVar(&eventRankingOnly, "eventranking", false, "Process only event ranking data (default: false)")
	flag.BoolVar(&eventDesignationOnly, "eventdesignation", false, "Process only event designation data (default: false)")
	flag.BoolVar(&visitorSpreadOnly, "visitorspread", false, "Process only visitor spread data (default: false)")
	flag.BoolVar(&allEventOnly, "allevent", false, "Process only all event data (default: false)")
	flag.BoolVar(&holidaysOnly, "holidays", false, "Process holidays into allevent_ch (automatically handles event types) (default: false)")
	flag.BoolVar(&alertsOnly, "alerts", false, "Process alerts from GDAC API into alerts_ch (default: false)")
	flag.BoolVar(&allScripts, "all", false, "Run all seeding scripts in order: location, eventtype, allevent, category, ranking, designation, holidays, alerts, exhibitor, speaker, sponsor, visitors, visitorspread (default: false)")
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
		log.Println("  -eventtype")
		log.Println("        Process only eventtype data (default: false)")
		log.Println("  -eventcategory")
		log.Println("        Process only eventcategory data (default: false)")
		log.Println("  -eventranking")
		log.Println("        Process only eventranking data (default: false)")
		log.Println("  -visitorspread")
		log.Println("        Process only visitor spread data (default: false)")
		log.Println("  -allevent")
		log.Println("        Process only all event data (default: false)")
		log.Println("  -holiday-eventtypes")
		log.Println("        Process only holiday event types into event_type_ch (default: false)")
		log.Println("  -holidays")
		log.Println("        Process only holidays into allevent_ch (default: false)")
		log.Println("  -help")
		log.Println("        Show this help message")
		log.Println("\nExamples:")
		log.Println("  go run main.go -event-edition -chunks=10 -workers=20 -batch=50000")
		log.Println("  go run main.go -sponsors -chunks=5 -workers=10 -batch=10000")
		log.Println("  go run main.go -exhibitors -chunks=8 -workers=15 -batch=20000")
		log.Println("  go run main.go -visitors -chunks=3 -workers=8 -batch=5000")
		log.Println("  go run main.go -speakers -chunks=6 -workers=12 -batch=15000")
		log.Println("  go run main.go -eventtype -chunks=5 -workers=10 -batch=10000")
		log.Println("  go run main.go -holiday-eventtypes -clickhouse-workers=3")
		log.Println("  go run main.go -holidays -batch=1000 -clickhouse-workers=5")
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

	config, err := utils.LoadEnv()
	if err != nil {
		log.Fatal("Failed to load environment variables:", err)
	}

	mysqlDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=latin1",
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

	log.Printf("Constructed Elasticsearch URL: %s", elasticHost)

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
	if err := utils.ValidateConfig(config); err != nil {
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
	} else if eventRankingOnly {
		log.Printf("Mode: EVENT RANKING ONLY")
	} else if eventDesignationOnly {
		log.Printf("Mode: EVENT DESIGNATION ONLY")
	} else if visitorSpreadOnly {
		log.Printf("Mode: VISITOR SPREAD ONLY")
	} else if allEventOnly {
		log.Printf("Mode: ALL EVENT ONLY")
	} else if holidaysOnly {
		log.Printf("Mode: HOLIDAYS ONLY")
	} else if locationAll {
		log.Printf("Mode: LOCATION ALL (countries, states, cities, venues, sub-venues)")
	} else if locationCountriesOnly {
		log.Printf("Mode: LOCATION COUNTRIES ONLY")
	} else if locationStatesOnly {
		log.Printf("Mode: LOCATION STATES ONLY")
	} else if locationCitiesOnly {
		log.Printf("Mode: LOCATION CITIES ONLY")
	} else if locationVenuesOnly {
		log.Printf("Mode: LOCATION VENUES ONLY")
	} else if locationSubVenuesOnly {
		log.Printf("Mode: LOCATION SUB-VENUES ONLY")
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
	} else if eventRankingOnly {
		log.Printf("Elasticsearch: Skipped (not needed for event ranking)")
	} else if eventDesignationOnly {
		log.Printf("Elasticsearch: Skipped (not needed for event designation)")
	} else if visitorSpreadOnly {
		log.Printf("Elasticsearch: Required (needed for visitor spread data)")
	} else if allEventOnly {
		log.Printf("Elasticsearch: Required (needed for all event data)")
	}
	log.Printf("==============================\n")

	mysqlDB, clickhouseDB, esClient, err := utils.SetupConnections(config)
	if err != nil {
		log.Fatal(err)
	}
	defer mysqlDB.Close()
	defer clickhouseDB.Close()

	log.Println("Connections established successfully!")

	if err := utils.TestClickHouseConnection(clickhouseDB); err != nil {
		log.Fatalf("ClickHouse connection test failed: %v", err)
	}

	if allScripts {
		runAllScripts(mysqlDB, clickhouseDB, esClient, config)
		return
	}

	if !sponsorsOnly && !speakersOnly && !visitorsOnly && !exhibitorOnly && !eventTypeEventChOnly && !eventCategoryEventChOnly && !eventRankingOnly && !eventDesignationOnly && !locationCountriesOnly && !locationStatesOnly && !locationCitiesOnly && !locationVenuesOnly && !locationSubVenuesOnly && !locationAll && !holidaysOnly {
		if err := utils.TestElasticsearchConnection(esClient, config.ElasticsearchIndex); err != nil {
			log.Fatalf("Elasticsearch connection test failed: %v", err)
		}
	} else if visitorSpreadOnly {
		if err := utils.TestElasticsearchConnection(esClient, config.ElasticsearchIndex); err != nil {
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
		} else if eventRankingOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for event_ranking_ch processing)")
		} else if eventDesignationOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for event_designation_ch processing)")
		} else if locationAll || locationCountriesOnly || locationStatesOnly || locationCitiesOnly || locationVenuesOnly || locationSubVenuesOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for location_ch processing)")
		} else if holidaysOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for holidays processing)")
		}
	}

	if exhibitorOnly {
		// Ensure temp table exists
		if err := shared.EnsureSingleTempTableExists(clickhouseDB, "event_exhibitor_ch", config, errorLogFile); err != nil {
			logErrorToFile("Ensure Temp Table (Exhibitor)", err)
			log.Fatalf("Failed to ensure temp table exists: %v", err)
		}

		utilsConfig := shared.Config{
			BatchSize:         config.BatchSize,
			NumChunks:         config.NumChunks,
			NumWorkers:        config.NumWorkers,
			ClickHouseWorkers: config.ClickHouseWorkers,
		}
		utils.ProcessExhibitorOnly(mysqlDB, clickhouseDB, utilsConfig)

		// Swap table after processing
		log.Println("Swapping event_exhibitor_ch table...")
		if err := shared.SwapSingleTable(clickhouseDB, "event_exhibitor_ch", config, errorLogFile); err != nil {
			logErrorToFile("Exhibitor Table Swap", err)
			log.Fatalf("Failed to swap event_exhibitor_ch: %v", err)
		}
		log.Println("✓ event_exhibitor_ch swapped successfully")
	} else if sponsorsOnly {
		// Ensure temp table exists
		if err := shared.EnsureSingleTempTableExists(clickhouseDB, "event_sponsors_ch", config, errorLogFile); err != nil {
			logErrorToFile("Ensure Temp Table (Sponsors)", err)
			log.Fatalf("Failed to ensure temp table exists: %v", err)
		}

		utilsConfig := shared.Config{
			BatchSize:         config.BatchSize,
			NumChunks:         config.NumChunks,
			NumWorkers:        config.NumWorkers,
			ClickHouseWorkers: config.ClickHouseWorkers,
		}
		utils.ProcessSponsorsOnly(mysqlDB, clickhouseDB, utilsConfig)

		// Swap table after processing
		log.Println("Swapping event_sponsors_ch table...")
		if err := shared.SwapSingleTable(clickhouseDB, "event_sponsors_ch", config, errorLogFile); err != nil {
			logErrorToFile("Sponsors Table Swap", err)
			log.Fatalf("Failed to swap event_sponsors_ch: %v", err)
		}
		log.Println("✓ event_sponsors_ch swapped successfully")
	} else if visitorsOnly {
		// Ensure temp table exists
		if err := shared.EnsureSingleTempTableExists(clickhouseDB, "event_visitors_ch", config, errorLogFile); err != nil {
			logErrorToFile("Ensure Temp Table (Visitors)", err)
			log.Fatalf("Failed to ensure temp table exists: %v", err)
		}

		processVisitorsOnly(mysqlDB, clickhouseDB, config)

		// Swap table after processing
		log.Println("Swapping event_visitors_ch table...")
		if err := shared.SwapSingleTable(clickhouseDB, "event_visitors_ch", config, errorLogFile); err != nil {
			logErrorToFile("Visitors Table Swap", err)
			log.Fatalf("Failed to swap event_visitors_ch: %v", err)
		}
		log.Println("✓ event_visitors_ch swapped successfully")
	} else if speakersOnly {
		// Ensure temp table exists
		if err := shared.EnsureSingleTempTableExists(clickhouseDB, "event_speaker_ch", config, errorLogFile); err != nil {
			logErrorToFile("Ensure Temp Table (Speakers)", err)
			log.Fatalf("Failed to ensure temp table exists: %v", err)
		}

		processSpeakersOnly(mysqlDB, clickhouseDB, config)

		// Swap table after processing
		log.Println("Swapping event_speaker_ch table...")
		if err := shared.SwapSingleTable(clickhouseDB, "event_speaker_ch", config, errorLogFile); err != nil {
			logErrorToFile("Speakers Table Swap", err)
			log.Fatalf("Failed to swap event_speaker_ch: %v", err)
		}
		log.Println("✓ event_speaker_ch swapped successfully")
	} else if eventTypeEventChOnly {
		// Ensure temp table exists
		if err := shared.EnsureSingleTempTableExists(clickhouseDB, "event_type_ch", config, errorLogFile); err != nil {
			logErrorToFile("Ensure Temp Table (Event Type)", err)
			log.Fatalf("Failed to ensure temp table exists: %v", err)
		}

		utilsConfig := shared.Config{
			BatchSize:         config.BatchSize,
			NumChunks:         config.NumChunks,
			NumWorkers:        config.NumWorkers,
			ClickHouseWorkers: config.ClickHouseWorkers,
		}
		utils.ProcessEventTypeEventChOnly(mysqlDB, clickhouseDB, utilsConfig)

		// Swap table after processing
		log.Println("Swapping event_type_ch table...")
		if err := shared.SwapSingleTable(clickhouseDB, "event_type_ch", config, errorLogFile); err != nil {
			logErrorToFile("Event Type Table Swap", err)
			log.Fatalf("Failed to swap event_type_ch: %v", err)
		}
		log.Println("✓ event_type_ch swapped successfully")
	} else if eventCategoryEventChOnly {
		// Ensure temp table exists
		if err := shared.EnsureSingleTempTableExists(clickhouseDB, "event_category_ch", config, errorLogFile); err != nil {
			logErrorToFile("Ensure Temp Table (Event Category)", err)
			log.Fatalf("Failed to ensure temp table exists: %v", err)
		}

		utilsConfig := shared.Config{
			BatchSize:         config.BatchSize,
			NumChunks:         config.NumChunks,
			NumWorkers:        config.NumWorkers,
			ClickHouseWorkers: config.ClickHouseWorkers,
		}
		utils.ProcessEventCategoryEventChOnly(mysqlDB, clickhouseDB, utilsConfig)

		// Swap table after processing
		log.Println("Swapping event_category_ch table...")
		if err := shared.SwapSingleTable(clickhouseDB, "event_category_ch", config, errorLogFile); err != nil {
			logErrorToFile("Event Category Table Swap", err)
			log.Fatalf("Failed to swap event_category_ch: %v", err)
		}
		log.Println("✓ event_category_ch swapped successfully")
	} else if eventRankingOnly {
		// Ensure temp table exists
		if err := shared.EnsureSingleTempTableExists(clickhouseDB, "event_ranking_ch", config, errorLogFile); err != nil {
			logErrorToFile("Ensure Temp Table (Event Ranking)", err)
			log.Fatalf("Failed to ensure temp table exists: %v", err)
		}

		utilsConfig := shared.Config{
			BatchSize:         config.BatchSize,
			NumChunks:         config.NumChunks,
			NumWorkers:        config.NumWorkers,
			ClickHouseWorkers: config.ClickHouseWorkers,
		}
		utils.ProcessEventRankingOnly(mysqlDB, clickhouseDB, utilsConfig)

		// Swap table after processing
		log.Println("Swapping event_ranking_ch table...")
		if err := shared.SwapSingleTable(clickhouseDB, "event_ranking_ch", config, errorLogFile); err != nil {
			logErrorToFile("Event Ranking Table Swap", err)
			log.Fatalf("Failed to swap event_ranking_ch: %v", err)
		}
		log.Println("✓ event_ranking_ch swapped successfully")
	} else if eventDesignationOnly {
		// Ensure temp table exists
		if err := shared.EnsureSingleTempTableExists(clickhouseDB, "event_designation_ch", config, errorLogFile); err != nil {
			logErrorToFile("Ensure Temp Table (Event Designation)", err)
			log.Fatalf("Failed to ensure temp table exists: %v", err)
		}

		utilsConfig := shared.Config{
			BatchSize:         config.BatchSize,
			NumChunks:         config.NumChunks,
			NumWorkers:        config.NumWorkers,
			ClickHouseWorkers: config.ClickHouseWorkers,
		}
		utils.ProcessEventDesignationOnly(mysqlDB, clickhouseDB, utilsConfig)

		// Swap table after processing
		log.Println("Swapping event_designation_ch table...")
		if err := shared.SwapSingleTable(clickhouseDB, "event_designation_ch", config, errorLogFile); err != nil {
			logErrorToFile("Event Designation Table Swap", err)
			log.Fatalf("Failed to swap event_designation_ch: %v", err)
		}
		log.Println("✓ event_designation_ch swapped successfully")
	} else if visitorSpreadOnly {
		// Ensure temp table exists
		if err := shared.EnsureSingleTempTableExists(clickhouseDB, "event_visitorSpread_ch", config, errorLogFile); err != nil {
			logErrorToFile("Ensure Temp Table (Visitor Spread)", err)
			log.Fatalf("Failed to ensure temp table exists: %v", err)
		}

		utilsConfig := shared.Config{
			BatchSize:          config.BatchSize,
			NumChunks:          config.NumChunks,
			NumWorkers:         config.NumWorkers,
			ClickHouseWorkers:  config.ClickHouseWorkers,
			ElasticsearchIndex: config.ElasticsearchIndex,
		}
		utils.ProcessVisitorSpreadOnly(mysqlDB, clickhouseDB, esClient, utilsConfig)

		// Swap table after processing
		log.Println("Swapping event_visitorSpread_ch table...")
		if err := shared.SwapSingleTable(clickhouseDB, "event_visitorSpread_ch", config, errorLogFile); err != nil {
			logErrorToFile("Visitor Spread Table Swap", err)
			log.Fatalf("Failed to swap event_visitorSpread_ch: %v", err)
		}
		log.Println("✓ event_visitorSpread_ch swapped successfully")
	} else if allEventOnly {
		// Ensure temp table exists
		if err := shared.EnsureSingleTempTableExists(clickhouseDB, "allevent_ch", config, errorLogFile); err != nil {
			logErrorToFile("Ensure Temp Table (All Event)", err)
			log.Fatalf("Failed to ensure temp table exists: %v", err)
		}

		utilsConfig := shared.Config{
			BatchSize:          config.BatchSize,
			NumChunks:          config.NumChunks,
			NumWorkers:         config.NumWorkers,
			ClickHouseWorkers:  config.ClickHouseWorkers,
			ElasticsearchIndex: config.ElasticsearchIndex,
		}
		microservice.ProcessAllEventOnly(mysqlDB, clickhouseDB, esClient, utilsConfig)

		// Swap table after processing
		log.Println("Swapping allevent_ch table...")
		if err := shared.SwapSingleTable(clickhouseDB, "allevent_ch", config, errorLogFile); err != nil {
			logErrorToFile("All Event Table Swap", err)
			log.Fatalf("Failed to swap allevent_ch: %v", err)
		}
		log.Println("✓ allevent_ch swapped successfully")
	} else if locationCountriesOnly || locationStatesOnly || locationCitiesOnly || locationVenuesOnly || locationSubVenuesOnly {
		// Ensure temp table exists (all location types use location_ch)
		if err := shared.EnsureSingleTempTableExists(clickhouseDB, "location_ch", config, errorLogFile); err != nil {
			logErrorToFile("Ensure Temp Table (Location)", err)
			log.Fatalf("Failed to ensure temp table exists: %v", err)
		}

		locConfig := shared.Config{
			BatchSize:         config.BatchSize,
			NumChunks:         config.NumChunks,
			NumWorkers:        config.NumWorkers,
			ClickHouseWorkers: config.ClickHouseWorkers,
		}

		if locationCountriesOnly {
			microservice.ProcessLocationCountriesCh(mysqlDB, clickhouseDB, locConfig, 1)
		} else if locationStatesOnly {
			microservice.ProcessLocationStatesCh(mysqlDB, clickhouseDB, locConfig, 1)
		} else if locationCitiesOnly {
			microservice.ProcessLocationCitiesCh(mysqlDB, clickhouseDB, locConfig, 1)
		} else if locationVenuesOnly {
			microservice.ProcessLocationVenuesCh(mysqlDB, clickhouseDB, locConfig, 1)
		} else if locationSubVenuesOnly {
			microservice.ProcessLocationSubVenuesCh(mysqlDB, clickhouseDB, locConfig, 1)
		}

		// Swap location_ch after processing (all location types use the same table)
		log.Println("Swapping location_ch table...")
		if err := shared.SwapSingleTable(clickhouseDB, "location_ch", config, errorLogFile); err != nil {
			logErrorToFile("Location Table Swap", err)
			log.Fatalf("Failed to swap location_ch: %v", err)
		}
		log.Println("✓ location_ch swapped successfully")
	} else if locationAll {
		// Ensure temp table exists
		if err := shared.EnsureSingleTempTableExists(clickhouseDB, "location_ch", config, errorLogFile); err != nil {
			logErrorToFile("Ensure Temp Table (Location All)", err)
			log.Fatalf("Failed to ensure temp table exists: %v", err)
		}
		// Process all location types in sequence
		locConfig := shared.Config{
			BatchSize:         config.BatchSize,
			NumChunks:         config.NumChunks,
			NumWorkers:        config.NumWorkers,
			ClickHouseWorkers: config.ClickHouseWorkers,
		}

		log.Println("=== Processing all location types in sequence ===")
		log.Println("")

		// 1. Countries (needed for states)
		log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		log.Println("STEP 1/5: PROCESSING COUNTRIES")
		log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		nextID := microservice.ProcessLocationCountriesCh(mysqlDB, clickhouseDB, locConfig, 1)
		log.Println("✓ STEP 1/5 (COUNTRIES) COMPLETED SUCCESSFULLY")
		log.Println("")

		// 2. States (needed for cities)
		log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		log.Println("STEP 2/5: PROCESSING STATES")
		log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		nextID = microservice.ProcessLocationStatesCh(mysqlDB, clickhouseDB, locConfig, nextID)
		log.Println("✓ STEP 2/5 (STATES) COMPLETED SUCCESSFULLY")
		log.Println("")

		// 3. Cities (needed for venues)
		log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		log.Println("STEP 3/5: PROCESSING CITIES")
		log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		nextID = microservice.ProcessLocationCitiesCh(mysqlDB, clickhouseDB, locConfig, nextID)
		log.Println("✓ STEP 3/5 (CITIES) COMPLETED SUCCESSFULLY")
		log.Println("")

		// 4. Venues (needed for sub-venues)
		log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		log.Println("STEP 4/5: PROCESSING VENUES")
		log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		nextID = microservice.ProcessLocationVenuesCh(mysqlDB, clickhouseDB, locConfig, nextID)
		log.Println("✓ STEP 4/5 (VENUES) COMPLETED SUCCESSFULLY")
		log.Println("")

		// 5. Sub-venues
		log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		log.Println("STEP 5/5: PROCESSING SUB-VENUES")
		log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		microservice.ProcessLocationSubVenuesCh(mysqlDB, clickhouseDB, locConfig, nextID)
		log.Println("✓ STEP 5/5 (SUB-VENUES) COMPLETED SUCCESSFULLY")
		log.Println("")

		log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		log.Println("=== ALL LOCATION TYPES PROCESSING COMPLETED SUCCESSFULLY! ===")
		log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

		// Swap location_ch after processing
		log.Println("Swapping location_ch table...")
		if err := shared.SwapSingleTable(clickhouseDB, "location_ch", config, errorLogFile); err != nil {
			logErrorToFile("Location Table Swap", err)
			log.Fatalf("Failed to swap location_ch: %v", err)
		}
		log.Println("✓ location_ch swapped successfully")
	} else if holidaysOnly {
		holidayConfig := shared.Config{
			BatchSize:         config.BatchSize,
			NumChunks:         config.NumChunks,
			NumWorkers:        config.NumWorkers,
			ClickHouseWorkers: config.ClickHouseWorkers,
		}
		microservice.ProcessHolidays(mysqlDB, clickhouseDB, holidayConfig)
	} else if alertsOnly {
		if err := shared.EnsureSingleTempTableExists(clickhouseDB, "alerts_ch", config, errorLogFile); err != nil {
			logErrorToFile("Ensure Temp Table (Alerts)", err)
			log.Fatalf("Failed to ensure temp table exists: %v", err)
		}
		if err := shared.EnsureSingleTempTableExists(clickhouseDB, "location_polygons_ch", config, errorLogFile); err != nil {
			logErrorToFile("Ensure Temp Table (Location Polygons)", err)
			log.Fatalf("Failed to ensure temp table exists: %v", err)
		}

		log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		log.Println("=== PROCESSING ALERTS FROM GDAC API ===")
		log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

		gdacBaseURL := os.Getenv("gdac_base_url")
		gdacEndpoint := os.Getenv("gdac_event_search_endpoint")

		if gdacBaseURL == "" {
			log.Fatal("ERROR: GDAC_BASE_URL environment variable is not set")
		}
		if gdacEndpoint == "" {
			log.Fatal("ERROR: GDAC_EVENT_SEARCH_ENDPOINT environment variable is not set")
		}

		log.Printf("GDAC Base URL: %s", gdacBaseURL)
		log.Printf("GDAC Endpoint: %s", gdacEndpoint)

		validCountries, err := microservice.GetValidCountries()
		if err != nil {
			log.Fatalf("ERROR: Failed to get valid countries: %v", err)
		}

		if len(validCountries) == 0 {
			log.Fatal("ERROR: No valid countries found")
		}

		log.Printf("Processing alerts for %d valid countries", len(validCountries))

		if err := microservice.ProcessAlertsFromAPI(clickhouseDB, gdacBaseURL, gdacEndpoint, validCountries); err != nil {
			log.Fatalf("ERROR: Failed to process alerts: %v", err)
		}

		log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		log.Println("=== ALERTS PROCESSING COMPLETED SUCCESSFULLY! ===")

		log.Println("Swapping alerts_ch table...")
		if err := shared.SwapSingleTable(clickhouseDB, "alerts_ch", config, errorLogFile); err != nil {
			logErrorToFile("Alerts Table Swap", err)
			log.Fatalf("Failed to swap alerts_ch: %v", err)
		}
		log.Println("✓ alerts_ch swapped successfully")

		log.Println("Swapping location_polygons_ch table...")
		if err := shared.SwapSingleTable(clickhouseDB, "location_polygons_ch", config, errorLogFile); err != nil {
			logErrorToFile("Location Polygons Table Swap", err)
			log.Fatalf("Failed to swap location_polygons_ch: %v", err)
		}
		log.Println("✓ location_polygons_ch swapped successfully")
		log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
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
		log.Println("  -eventranking     # Process event ranking data")
		log.Println("  -eventdesignation # Process event designation data")
		log.Println("  -visitorspread    # Process visitor spread data")
		log.Println("  -allevent         # Process all event data")
		log.Println("  -holidays         # Process holidays into allevent_ch (automatically handles event types)")
		log.Println("  -alerts           # Process alerts from GDAC API into alerts_ch")
		log.Println("  -location         # Process all location types (countries, states, cities, venues, sub-venues)")
		log.Println("  -location-countries   # Process only location countries")
		log.Println("  -location-states      # Process only location states")
		log.Println("  -location-cities      # Process only location cities")
		log.Println("  -location-venues      # Process only location venues")
		log.Println("  -location-sub-venues  # Process only location sub-venues")
		log.Println("  -all                 # Run all seeding scripts in order: location, eventtype, allevent, category, ranking, designation, holidays, alerts, exhibitor, speaker, sponsor, visitors, visitorspread")
		log.Println("")
		log.Println("Example: go run main.go -event-edition -chunks=10 -workers=20")
		log.Println("Example: go run main.go -location -batch=1000 -clickhouse-workers=5")
		log.Println("Example: go run main.go -all -chunks=5 -workers=10 -batch=5000")
		os.Exit(1)
	}
}
