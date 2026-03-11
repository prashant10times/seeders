package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"seeders/company"
	"seeders/eventdata"
	"seeders/microservice"
	"seeders/shared"
	"seeders/utils"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/elastic/go-elasticsearch/v6"
	_ "github.com/go-sql-driver/mysql"
)

const errorLogFile = "seeding_errors.log"
const seederLogFile = "seeder.log"

func initSeederLogFile() error {
	f, err := os.OpenFile(seederLogFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	log.SetOutput(io.MultiWriter(os.Stdout, f))
	log.SetFlags(log.LstdFlags)
	return nil
}

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

	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	log.Println("STEP 0.5: STOPPING CLICKHOUSE MERGES FOR PHASE 1 (OPTIMAL INSERT PERFORMANCE)")
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	if err := shared.StopClickHouseMerges(clickhouseDB); err != nil {
		logErrorToFile("Stop ClickHouse Merges (Phase 1)", err)
		log.Printf("⚠️  Warning: Failed to stop ClickHouse merges before Phase 1: %v", err)
		log.Printf("⚠️  Continuing with Phase 1 processing...")
	} else {
		log.Println("✓ ClickHouse merges stopped successfully before Phase 1")
	}
	log.Println("✓ STEP 0.5 (STOP MERGES FOR PHASE 1) COMPLETED")
	log.Println("")

	tableNameMap := map[int]string{
		0:  "location_ch",
		1:  "event_type_ch",
		2:  "allevent_ch",
		3:  "event_category_ch",
		4:  "event_product_ch",
		5:  "event_ranking_ch",
		6:  "event_designation_ch",
		7:  "event_exhibitor_ch",
		8:  "event_speaker_ch",
		9:  "event_sponsors_ch",
		10: "event_visitors_ch",
		11: "event_visitorSpread_ch",
	}

	mergesStarted := false

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
				log.Println("STEP 1/15: PROCESSING LOCATION")
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
				log.Println("✓ STEP 1/15 (LOCATION) COMPLETED SUCCESSFULLY")
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
				log.Println("STEP 2/15: PROCESSING EVENT TYPE")
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				utilsConfig := shared.Config{
					BatchSize:         config.BatchSize,
					NumChunks:         config.NumChunks,
					NumWorkers:        config.NumWorkers,
					ClickHouseWorkers: config.ClickHouseWorkers,
				}
				utils.ProcessEventTypeEventChOnly(mysqlDB, clickhouseDB, utilsConfig)
				log.Println("✓ STEP 2/15 (EVENT TYPE) COMPLETED SUCCESSFULLY")
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
				log.Println("STEP 3/15: PROCESSING ALL EVENT")
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				utilsConfig := config
				utilsConfig.UseTempTables = true // When running -all, read from temp tables
				microservice.ProcessAllEventOnly(mysqlDB, clickhouseDB, esClient, utilsConfig)

				if microservice.HasRemainingFailedBatches() {
					log.Println("")
					log.Println("⚠️  WARNING: Failed batches still remain after retry")
					log.Println("⚠️  Please rerun the script to retry failed batches")
					log.Println("⚠️  Optimization will be skipped until all batches succeed")
					log.Println("")
					return fmt.Errorf("failed batches still remain - rerun script to retry")
				}

				log.Println("✓ STEP 3/15 (ALL EVENT) COMPLETED SUCCESSFULLY")
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
				log.Println("STEP 4/15: PROCESSING EVENT CATEGORY")
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				utilsConfig := shared.Config{
					BatchSize:         config.BatchSize,
					NumChunks:         config.NumChunks,
					NumWorkers:        config.NumWorkers,
					ClickHouseWorkers: config.ClickHouseWorkers,
				}
				utils.ProcessEventCategoryEventChOnly(mysqlDB, clickhouseDB, utilsConfig)
				log.Println("✓ STEP 4/15 (EVENT CATEGORY) COMPLETED SUCCESSFULLY")
				log.Println("")
				return nil
			},
		},
		// 5. Event Product
		{
			name:     "Event Product",
			critical: false,
			run: func() error {
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				log.Println("STEP 5/15: PROCESSING EVENT PRODUCT")
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				utilsConfig := shared.Config{
					BatchSize:         config.BatchSize,
					NumChunks:         config.NumChunks,
					NumWorkers:        config.NumWorkers,
					ClickHouseWorkers: config.ClickHouseWorkers,
				}
				utils.ProcessEventProductChOnly(mysqlDB, clickhouseDB, utilsConfig)
				log.Println("✓ STEP 5/15 (EVENT PRODUCT) COMPLETED SUCCESSFULLY")
				log.Println("")
				return nil
			},
		},
		// 6. Event Ranking
		{
			name:     "Event Ranking",
			critical: false,
			run: func() error {
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				log.Println("STEP 6/15: PROCESSING EVENT RANKING")
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				utilsConfig := shared.Config{
					BatchSize:         config.BatchSize,
					NumChunks:         config.NumChunks,
					NumWorkers:        config.NumWorkers,
					ClickHouseWorkers: config.ClickHouseWorkers,
				}
				utils.ProcessEventRankingOnly(mysqlDB, clickhouseDB, utilsConfig)
				log.Println("✓ STEP 6/15 (EVENT RANKING) COMPLETED SUCCESSFULLY")
				log.Println("")
				return nil
			},
		},
		// 7. Event Designation
		{
			name:     "Event Designation",
			critical: false,
			run: func() error {
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				log.Println("STEP 7/15: PROCESSING EVENT DESIGNATION")
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				utilsConfig := shared.Config{
					BatchSize:         config.BatchSize,
					NumChunks:         config.NumChunks,
					NumWorkers:        config.NumWorkers,
					ClickHouseWorkers: config.ClickHouseWorkers,
				}
				utils.ProcessEventDesignationOnly(mysqlDB, clickhouseDB, utilsConfig)
				log.Println("✓ STEP 7/15 (EVENT DESIGNATION) COMPLETED SUCCESSFULLY")
				log.Println("")
				return nil
			},
		},
		// 8. Exhibitor
		{
			name:     "Exhibitor",
			critical: false,
			run: func() error {
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				log.Println("STEP 8/15: PROCESSING EXHIBITOR")
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				utilsConfig := shared.Config{
					BatchSize:         config.BatchSize,
					NumChunks:         config.NumChunks,
					NumWorkers:        config.NumWorkers,
					ClickHouseWorkers: config.ClickHouseWorkers,
				}
				utils.ProcessExhibitorOnly(mysqlDB, clickhouseDB, utilsConfig)
				log.Println("✓ STEP 8/15 (EXHIBITOR) COMPLETED SUCCESSFULLY")
				log.Println("")
				return nil
			},
		},
		// 9. Speaker
		{
			name:     "Speaker",
			critical: false,
			run: func() error {
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				log.Println("STEP 9/15: PROCESSING SPEAKER")
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				utils.ProcessSpeakersOnly(mysqlDB, clickhouseDB, config)
				log.Println("✓ STEP 9/15 (SPEAKER) COMPLETED SUCCESSFULLY")
				log.Println("")
				return nil
			},
		},
		// 10. Sponsor
		{
			name:     "Sponsor",
			critical: false,
			run: func() error {
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				log.Println("STEP 10/15: PROCESSING SPONSOR")
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				utilsConfig := shared.Config{
					BatchSize:         config.BatchSize,
					NumChunks:         config.NumChunks,
					NumWorkers:        config.NumWorkers,
					ClickHouseWorkers: config.ClickHouseWorkers,
				}
				utils.ProcessSponsorsOnly(mysqlDB, clickhouseDB, utilsConfig)
				log.Println("✓ STEP 10/15 (SPONSOR) COMPLETED SUCCESSFULLY")
				log.Println("")
				return nil
			},
		},
		// 11. Visitors
		{
			name:     "Visitors",
			critical: false,
			run: func() error {
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				log.Println("STEP 11/15: PROCESSING VISITORS")
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				utils.ProcessVisitorsOnly(mysqlDB, clickhouseDB, config)
				log.Println("✓ STEP 11/15 (VISITORS) COMPLETED SUCCESSFULLY")
				log.Println("")
				return nil
			},
		},
		// 12. Visitor Spread
		{
			name:     "Visitor Spread",
			critical: false,
			run: func() error {
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				log.Println("STEP 12/15: PROCESSING VISITOR SPREAD")
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				utilsConfig := shared.Config{
					BatchSize:          config.BatchSize,
					NumChunks:          config.NumChunks,
					NumWorkers:         config.NumWorkers,
					ClickHouseWorkers:  config.ClickHouseWorkers,
					ElasticsearchIndex: config.ElasticsearchIndex,
				}
				utils.ProcessVisitorSpreadOnly(mysqlDB, clickhouseDB, esClient, utilsConfig)
				log.Println("✓ STEP 12/15 (VISITOR SPREAD) COMPLETED SUCCESSFULLY")
				log.Println("")
				return nil
			},
		},
		// 13. Holidays
		{
			name:     "Holidays",
			critical: false,
			run: func() error {
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				log.Println("STEP 13/15: PROCESSING HOLIDAYS")
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				holidayConfig := shared.Config{
					BatchSize:         config.BatchSize,
					NumChunks:         config.NumChunks,
					NumWorkers:        config.NumWorkers,
					ClickHouseWorkers: config.ClickHouseWorkers,
					UseTempTables:     true, // Swap-at-end: holidays read/write temp tables
					ClickhouseDB:      config.ClickhouseDB,
				}
				microservice.ProcessHolidays(mysqlDB, clickhouseDB, holidayConfig)
				log.Println("✓ STEP 13/15 (HOLIDAYS) COMPLETED SUCCESSFULLY")
				log.Println("")
				return nil
			},
		},
		// 14. Company Classification
		{
			name:     "Company Classification",
			critical: false,
			run: func() error {
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				log.Println("STEP 14/15: PROCESSING COMPANY CLASSIFICATION")
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				utilsConfig := shared.Config{
					BatchSize:         config.BatchSize,
					NumChunks:         config.NumChunks,
					NumWorkers:        config.NumWorkers,
					ClickHouseWorkers: config.ClickHouseWorkers,
				}
				company.ProcessCompanyClassificationOnly(mysqlDB, clickhouseDB, utilsConfig)
				log.Println("Optimizing company_classification_ch table...")
				optimizeConfig := config
				optimizeConfig.UseTempTables = true
				if err := shared.OptimizeSingleTable(clickhouseDB, "company_classification_ch", optimizeConfig, errorLogFile); err != nil {
					logErrorToFile("Company Classification Optimization", err)
					log.Printf("⚠️  Error optimizing company_classification_ch table: %v", err)
					log.Printf("⚠️  Continuing (swap will happen in batch at end)...")
				} else {
					log.Println("✓ company_classification_ch optimized successfully")
				}
				log.Println("✓ STEP 14/15 (COMPANY CLASSIFICATION) COMPLETED SUCCESSFULLY (swap deferred to batch)")
				log.Println("")
				return nil
			},
		},
		// 15. Alerts
		{
			name:     "Alerts",
			critical: false,
			run: func() error {
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				log.Println("STEP 15/15: PROCESSING ALERTS")
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

				alertConfig := shared.Config{
					BatchSize:         config.BatchSize,
					NumChunks:         config.NumChunks,
					NumWorkers:        config.NumWorkers,
					ClickHouseWorkers: config.ClickHouseWorkers,
					UseTempTables:     true, // -all: write to event_type_temp so alert data survives batch swap
					ClickhouseDB:      config.ClickhouseDB,
				}
				if err := microservice.ProcessAlertsFromAPI(clickhouseDB, gdacBaseURL, gdacEndpoint, validCountries, alertConfig); err != nil {
					return err
				}
				log.Println("✓ STEP 15/15 (ALERTS) COMPLETED SUCCESSFULLY")
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

		if tableName, isPhase1Table := tableNameMap[i]; isPhase1Table {
			if !mergesStarted {
				log.Println("")
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				log.Println("STARTING CLICKHOUSE MERGES FOR OPTIMIZATION (PHASE 1)")
				log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
				if err := shared.StartClickHouseMerges(clickhouseDB); err != nil {
					logErrorToFile("Start ClickHouse Merges (Before Optimization)", err)
					log.Printf("⚠️  Warning: Failed to start ClickHouse merges before optimization: %v", err)
					log.Printf("⚠️  You may need to manually run 'SYSTEM START MERGES' in ClickHouse")
					log.Printf("⚠️  Continuing with optimization anyway...")
				} else {
					log.Println("✓ ClickHouse merges started successfully for optimization")
					mergesStarted = true
				}
				log.Println("")
			}

			log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
			log.Printf("OPTIMIZING TABLE IMMEDIATELY AFTER INSERTION: %s", tableName)
			log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

			if optimizeErr := shared.OptimizeSingleTable(clickhouseDB, tableName, config, errorLogFile); optimizeErr != nil {
				logErrorToFile("Optimize Single Table", optimizeErr)
				log.Printf("⚠️  Warning: Error optimizing %s: %v", tableName, optimizeErr)
				log.Printf("⚠️  Continuing with next table...")
			}
			log.Println("")
		}

	}

	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	log.Println("PHASE 2 PREPARATION: STOPPING CLICKHOUSE MERGES FOR OPTIMAL INSERT PERFORMANCE")
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	if err := shared.StopClickHouseMerges(clickhouseDB); err != nil {
		logErrorToFile("Stop ClickHouse Merges (Phase 2)", err)
		log.Printf("⚠️  Warning: Failed to stop ClickHouse merges before Phase 2: %v", err)
		log.Printf("⚠️  Continuing with Phase 2 processing...")
	} else {
		log.Println("✓ ClickHouse merges stopped successfully before Phase 2")
	}
	log.Println("")

	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	log.Println("VALIDATION: Validating all temp tables before swap")
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	allTablesToSwap := []string{
		"location_ch",
		"event_type_ch",
		"allevent_ch",
		"event_daywiseEconomicImpact_ch",
		"event_category_ch",
		"event_product_ch",
		"event_ranking_ch",
		"event_designation_ch",
		"event_exhibitor_ch",
		"event_speaker_ch",
		"event_sponsors_ch",
		"event_visitors_ch",
		"event_visitorSpread_ch",
		"company_classification_ch",
		"alerts_ch",
		"location_polygons_ch",
	}

	var allTableMappings []shared.TableMapping
	for _, tableName := range allTablesToSwap {
		allTableMappings = append(allTableMappings, shared.GetTableMapping(tableName, config))
	}

	if err := shared.ValidateTempTables(clickhouseDB, allTableMappings, config, errorLogFile); err != nil {
		logErrorToFile("Pre-Swap Validation", err)
		log.Fatalf("Validation failed before swap: %v", err)
	}

	log.Println("✓ All temp tables validated successfully, proceeding with batch swap")
	log.Println("")

	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	log.Println("BATCH SWAP: Swapping all tables (data sync complete)")
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	if err := shared.SwapTables(clickhouseDB, allTableMappings, config, errorLogFile); err != nil {
		logErrorToFile("Table Swap", err)
		log.Fatalf("Failed to swap tables: %v", err)
	}

	log.Println("✓ All tables swapped successfully")
	log.Println("")

	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	log.Println("PHASE 2 COMPLETE: STARTING CLICKHOUSE MERGES")
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	if err := shared.StartClickHouseMerges(clickhouseDB); err != nil {
		logErrorToFile("Start ClickHouse Merges (Phase 2)", err)
		log.Printf("⚠️  Warning: Failed to start ClickHouse merges after Phase 2: %v", err)
		log.Printf("⚠️  You may need to manually run 'SYSTEM START MERGES' in ClickHouse")
	} else {
		log.Println("✓ ClickHouse merges started successfully after Phase 2")
	}
	log.Println("")

	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	log.Println("=== ALL SEEDING SCRIPTS COMPLETED ===")
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	log.Println("=== ALL SEEDING SCRIPTS COMPLETED ===")
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	if _, err := os.Stat(errorLogFile); err == nil {
		log.Printf("Note: Check %s for any errors that occurred during non-critical scripts", errorLogFile)
	}
}

func runAllIncrementalSequential(mysqlDB *sql.DB, clickhouseDB driver.Conn, esClient *elasticsearch.Client, config shared.Config) {
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	log.Println("=== STARTING ALL INCREMENTAL SYNC (sequential) ===")
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	log.Println("")

	utilsConfig := config
	utilsConfig.UseTempTables = false

	tasks := []struct {
		name string
		run  func() error
	}{
		{"allevent-incremental", func() error {
			return microservice.ProcessIncrementalAllevent(mysqlDB, clickhouseDB, esClient, utilsConfig)
		}},
		{"eventtype-incremental", func() error { return utils.ProcessIncrementalEventType(mysqlDB, clickhouseDB, utilsConfig) }},
		{"eventcategory-incremental", func() error { return utils.ProcessIncrementalEventCategory(mysqlDB, clickhouseDB, utilsConfig) }},
		{"eventproduct-incremental", func() error { return utils.ProcessIncrementalEventProduct(mysqlDB, clickhouseDB, utilsConfig) }},
		{"eventdesignation-incremental", func() error { return utils.ProcessIncrementalEventDesignation(mysqlDB, clickhouseDB, utilsConfig) }},
		{"eventexhibitor-incremental", func() error { return utils.ProcessIncrementalEventExhibitor(mysqlDB, clickhouseDB, utilsConfig) }},
		{"eventspeaker-incremental", func() error { return utils.ProcessIncrementalEventSpeaker(mysqlDB, clickhouseDB, utilsConfig) }},
		{"eventsponsor-incremental", func() error { return utils.ProcessIncrementalEventSponsor(mysqlDB, clickhouseDB, utilsConfig) }},
		{"eventvisitor-incremental", func() error { return utils.ProcessIncrementalEventVisitor(mysqlDB, clickhouseDB, utilsConfig) }},
		{"alerts-incremental", func() error {
			gdacBaseURL := os.Getenv("gdac_base_url")
			gdacEndpoint := os.Getenv("gdac_event_search_endpoint")
			if gdacBaseURL == "" {
				log.Fatal("ERROR: GDAC_BASE_URL environment variable is not set")
			}
			if gdacEndpoint == "" {
				log.Fatal("ERROR: GDAC_EVENT_SEARCH_ENDPOINT environment variable is not set")
			}
			validCountries, err := microservice.GetValidCountries()
			if err != nil {
				return fmt.Errorf("get valid countries: %w", err)
			}
			if len(validCountries) == 0 {
				log.Fatal("ERROR: No valid countries found")
			}
			alertConfig := shared.Config{
				BatchSize:         config.BatchSize,
				NumChunks:         config.NumChunks,
				NumWorkers:        config.NumWorkers,
				ClickHouseWorkers: config.ClickHouseWorkers,
				ClickhouseDB:      config.ClickhouseDB,
			}
			return microservice.ProcessIncrementalAlerts(clickhouseDB, gdacBaseURL, gdacEndpoint, validCountries, alertConfig)
		}},
	}

	for i, t := range tasks {
		log.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		log.Printf("Step %d/%d: %s", i+1, len(tasks), t.name)
		log.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		if err := t.run(); err != nil {
			logErrorToFile(t.name, err)
			log.Fatalf("Incremental sync failed at %s: %v", t.name, err)
		}
		log.Printf("✓ %s completed", t.name)
		log.Println("")
	}

	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	log.Println("=== ALL INCREMENTAL SYNC COMPLETED SUCCESSFULLY ===")
	log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
}

func main() {
	var numChunks int
	var batchSize int
	var numWorkers int
	var clickHouseWorkers int

	var showHelp bool
	var companyOnly bool
	var companyCategoryOnly bool
	var companyClassificationOnly bool
	var companyProductOnly bool
	var companySpeakerOnly bool
	var companyVisitorOnly bool
	var companyEventDataOnly bool
	var eventCompanyOnly bool
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
	var eventProductChOnly bool
	var visitorSpreadOnly bool
	var allEventOnly bool
	var incrementalAlleventOnly bool
	var incrementalEventTypeOnly bool
	var incrementalCategoryOnly bool
	var incrementalProductOnly bool
	var incrementalDesignationOnly bool
	var incrementalExhibitorOnly bool
	var incrementalSpeakerOnly bool
	var incrementalSponsorOnly bool
	var incrementalVisitorOnly bool
	var holidaysOnly bool
	var alertsOnly bool
	var incrementalAlertsOnly bool
	var allIncrementalOnly bool
	var allScripts bool
	var daywiseOnly bool

	flag.IntVar(&numChunks, "chunks", 5, "Number of chunks to process data in (default: 5)")
	flag.IntVar(&batchSize, "batch", 5000, "MySQL batch size for fetching data (default: 5000)")
	flag.IntVar(&numWorkers, "workers", 5, "Number of parallel workers (default: 5)")
	flag.IntVar(&clickHouseWorkers, "clickhouse-workers", 3, "Number of parallel ClickHouse insertion workers (default: 3)")

	flag.BoolVar(&companyOnly, "company", false, "Process only company data into allCompany_ch (default: false)")
	flag.BoolVar(&companyCategoryOnly, "company-category", false, "Process only company_category data from company_interests into company_category_ch (default: false)")
	flag.BoolVar(&companyClassificationOnly, "company-classification", false, "Process only company_classification data from company_classification_mapping into company_classification_ch (default: false)")
	flag.BoolVar(&companyProductOnly, "company-product", false, "Process only company_product data from company_interests (interest=product) into company_product_ch (default: false)")
	flag.BoolVar(&companySpeakerOnly, "company-speaker", false, "Process only company_speaker data from event_speaker into company_speaker_ch (default: false)")
	flag.BoolVar(&companyVisitorOnly, "company-visitor", false, "Process only company_visitor data from event_visitor into company_visitor_ch (default: false)")
	flag.BoolVar(&companyEventDataOnly, "company-event-data", false, "Process only company event data (event+edition dimension) into companyEventData_ch (default: false)")
	flag.BoolVar(&eventCompanyOnly, "event-company", false, "Process only event–company participation mapping into event_company_ch (default: false)")
	flag.BoolVar(&exhibitorOnly, "exhibitor", false, "Process only exhibitor data (default: false)")
	flag.BoolVar(&sponsorsOnly, "sponsors", false, "Process only sponsors data (default: false)")
	flag.BoolVar(&visitorsOnly, "visitors", false, "Process only visitors data (default: false)")
	flag.BoolVar(&speakersOnly, "speakers", false, "Process only speakers data (default: false)")
	flag.BoolVar(&eventEditionOnly, "event-edition", false, "Process only event edition data (default: false)")
	flag.BoolVar(&eventTypeEventChOnly, "eventtype", false, "Process only eventtype data (default: false)")
	flag.BoolVar(&eventCategoryEventChOnly, "eventcategory", false, "Process only eventcategory data (default: false)")
	flag.BoolVar(&eventProductChOnly, "eventproduct", false, "Process only event_product data (default: false)")
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
	flag.BoolVar(&incrementalAlleventOnly, "allevent-incremental", false, "Incremental sync for allevent (only changed since yesterday) (default: false)")
	flag.BoolVar(&incrementalEventTypeOnly, "eventtype-incremental", false, "Incremental sync for event_type_ch (only changed since yesterday) (default: false)")
	flag.BoolVar(&incrementalCategoryOnly, "eventcategory-incremental", false, "Incremental sync for event_category_ch (only changed since yesterday) (default: false)")
	flag.BoolVar(&incrementalProductOnly, "eventproduct-incremental", false, "Incremental sync for event_product_ch (only changed since yesterday) (default: false)")
	flag.BoolVar(&incrementalDesignationOnly, "eventdesignation-incremental", false, "Incremental sync for event_designation_ch (only changed since yesterday) (default: false)")
	flag.BoolVar(&incrementalExhibitorOnly, "eventexhibitor-incremental", false, "Incremental sync for event_exhibitor_ch (only changed since yesterday) (default: false)")
	flag.BoolVar(&incrementalSpeakerOnly, "eventspeaker-incremental", false, "Incremental sync for event_speaker_ch (only changed since yesterday) (default: false)")
	flag.BoolVar(&incrementalSponsorOnly, "eventsponsor-incremental", false, "Incremental sync for event_sponsors_ch (only changed since yesterday) (default: false)")
	flag.BoolVar(&incrementalVisitorOnly, "eventvisitor-incremental", false, "Incremental sync for event_visitors_ch (only changed since yesterday) (default: false)")
	flag.BoolVar(&holidaysOnly, "holidays", false, "Process holidays into allevent_ch (automatically handles event types) (default: false)")
	flag.BoolVar(&alertsOnly, "alerts", false, "Process alerts from GDAC API into alerts_ch (default: false)")
	flag.BoolVar(&incrementalAlertsOnly, "alerts-incremental", false, "Incremental alert sync: fetch yesterday only and upsert into alerts_ch, location_polygons_ch, event_type_ch (default: false)")
	flag.BoolVar(&allScripts, "all", false, "Run all seeding scripts in order: location, eventtype, allevent, category, product, ranking, designation, holidays, alerts, exhibitor, speaker, sponsor, visitors, visitorspread (default: false)")
	flag.BoolVar(&daywiseOnly, "daywise", false, "Process only day-wise economic impact data from estimate table into event_daywiseEconomicImpact_ch (standalone, never runs with -all)")
	flag.BoolVar(&showHelp, "help", false, "Show help information")
	task := flag.String("task", "", "Incremental task only (cron-friendly). E.g. -task=allevent-incremental")
	flag.Parse()

	// Map -task to incremental boolean flags only (for cron scheduling of incremental syncs)
	if *task != "" {
		switch *task {
		case "allevent-incremental":
			incrementalAlleventOnly = true
		case "eventtype-incremental":
			incrementalEventTypeOnly = true
		case "eventcategory-incremental":
			incrementalCategoryOnly = true
		case "eventproduct-incremental":
			incrementalProductOnly = true
		case "eventdesignation-incremental":
			incrementalDesignationOnly = true
		case "eventexhibitor-incremental":
			incrementalExhibitorOnly = true
		case "eventspeaker-incremental":
			incrementalSpeakerOnly = true
		case "eventsponsor-incremental":
			incrementalSponsorOnly = true
		case "eventvisitor-incremental":
			incrementalVisitorOnly = true
		case "alerts-incremental":
			incrementalAlertsOnly = true
		case "all-incremental":
			allIncrementalOnly = true
		default:
			log.Fatalf("Unknown incremental task: %q. Valid: allevent-incremental, eventtype-incremental, eventcategory-incremental, eventproduct-incremental, eventdesignation-incremental, eventexhibitor-incremental, eventspeaker-incremental, eventsponsor-incremental, eventvisitor-incremental, alerts-incremental, all-incremental", *task)
		}
	}

	if showHelp {
		log.Println("=== Data Migration Script ===")
		log.Println("Usage: go run main.go [table-mode] [options]")
		log.Println("\nRequired Table Mode (choose one):")
		log.Println("  -task=NAME        # Incremental sync only (cron-friendly). E.g. -task=allevent-incremental, -task=eventvisitor-incremental")
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
		log.Println("  -allevent-incremental")
		log.Println("        Incremental sync for allevent (only changed since yesterday) (default: false)")
		log.Println("  -eventtype-incremental")
		log.Println("        Incremental sync for event_type_ch (only changed since yesterday) (default: false)")
		log.Println("  -holiday-eventtypes")
		log.Println("        Process only holiday event types into event_type_ch (default: false)")
		log.Println("  -holidays")
		log.Println("        Process only holidays into allevent_ch (default: false)")
		log.Println("  -help")
		log.Println("        Show this help message")
		log.Println("\nExamples:")
		log.Println("  go run main.go -event-edition -chunks=10 -workers=20 -batch=50000")
		log.Println("  go run main.go -sponsors -chunks=5 -workers=10 -batch=10000")
		log.Println("  go run main.go -exhibitor -chunks=8 -workers=15 -batch=20000")
		log.Println("\nIncremental sync (cron-friendly, -task only):")
		log.Println("  go run main.go -task=allevent-incremental")
		log.Println("  go run main.go -task=all-incremental    # Run all incremental syncs sequentially")
		log.Println("\nCron (run all incremental syncs daily at 2 AM):")
		log.Println("  0 2 * * * /opt/seeding/seeder -task=all-incremental >> /opt/seeding/incremental_cron.log 2>&1")
		log.Println("  go run main.go -visitors -chunks=3 -workers=8 -batch=5000")
		log.Println("  go run main.go -speakers -chunks=6 -workers=12 -batch=15000")
		log.Println("  go run main.go -eventtype -chunks=5 -workers=10 -batch=10000")
		log.Println("  go run main.go -holiday-eventtypes -clickhouse-workers=3")
		log.Println("  go run main.go -holidays -batch=1000 -clickhouse-workers=5")
		return
	}

	if err := initSeederLogFile(); err != nil {
		log.Printf("WARNING: Could not open seeder log file %s: %v (logging to stdout only)", seederLogFile, err)
	} else {
		log.Printf("Logging to stdout and %s", seederLogFile)
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

	mysqlDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4",
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
	if companyOnly {
		log.Printf("Mode: COMPANY ONLY")
	} else if companyCategoryOnly {
		log.Printf("Mode: COMPANY CATEGORY ONLY")
	} else if companyClassificationOnly {
		log.Printf("Mode: COMPANY CLASSIFICATION ONLY")
	} else if companyProductOnly {
		log.Printf("Mode: COMPANY PRODUCT ONLY")
	} else if companySpeakerOnly {
		log.Printf("Mode: COMPANY SPEAKER ONLY")
	} else if companyVisitorOnly {
		log.Printf("Mode: COMPANY VISITOR ONLY")
	} else if exhibitorOnly {
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
	} else if eventProductChOnly {
		log.Printf("Mode: EVENT PRODUCT ONLY")
	} else if eventRankingOnly {
		log.Printf("Mode: EVENT RANKING ONLY")
	} else if eventDesignationOnly {
		log.Printf("Mode: EVENT DESIGNATION ONLY")
	} else if visitorSpreadOnly {
		log.Printf("Mode: VISITOR SPREAD ONLY")
	} else if allEventOnly {
		log.Printf("Mode: ALL EVENT ONLY")
	} else if incrementalAlleventOnly {
		log.Printf("Mode: INCREMENTAL ALLEVENT (changed since yesterday)")
	} else if incrementalEventTypeOnly {
		log.Printf("Mode: INCREMENTAL EVENT TYPE (changed since yesterday)")
	} else if incrementalCategoryOnly {
		log.Printf("Mode: INCREMENTAL EVENT CATEGORY (changed since yesterday)")
	} else if incrementalProductOnly {
		log.Printf("Mode: INCREMENTAL EVENT PRODUCT (changed since yesterday)")
	} else if incrementalDesignationOnly {
		log.Printf("Mode: INCREMENTAL EVENT DESIGNATION (changed since yesterday)")
	} else if incrementalExhibitorOnly {
		log.Printf("Mode: INCREMENTAL EVENT EXHIBITOR (changed since yesterday)")
	} else if incrementalSpeakerOnly {
		log.Printf("Mode: INCREMENTAL EVENT SPEAKER (changed since yesterday)")
	} else if incrementalSponsorOnly {
		log.Printf("Mode: INCREMENTAL EVENT SPONSOR (changed since yesterday)")
	} else if incrementalVisitorOnly {
		log.Printf("Mode: INCREMENTAL EVENT VISITOR (changed since yesterday)")
	} else if incrementalAlertsOnly {
		log.Printf("Mode: INCREMENTAL ALERTS (yesterday only, upsert)")
	} else if allIncrementalOnly {
		log.Printf("Mode: ALL INCREMENTAL (run all incremental syncs sequentially)")
	} else if holidaysOnly {
		log.Printf("Mode: HOLIDAYS ONLY")
	} else if daywiseOnly {
		log.Printf("Mode: DAY-WISE ECONOMIC IMPACT ONLY (standalone)")
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

	if companyOnly {
		log.Printf("Elasticsearch: Required for company (COMPANY_INDEX from .env)")
	} else if companyCategoryOnly {
		log.Printf("Elasticsearch: Skipped (not needed for company category)")
	} else if companyClassificationOnly {
		log.Printf("Elasticsearch: Skipped (not needed for company classification)")
	} else if companyProductOnly {
		log.Printf("Elasticsearch: Skipped (not needed for company product)")
	} else if companySpeakerOnly {
		log.Printf("Elasticsearch: Skipped (not needed for company speaker)")
	} else if companyVisitorOnly {
		log.Printf("Elasticsearch: Skipped (not needed for company visitor)")
	} else if sponsorsOnly {
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
	} else if incrementalAlleventOnly {
		log.Printf("Elasticsearch: Required (needed for incremental allevent data)")
	} else if incrementalEventTypeOnly {
		log.Printf("Elasticsearch: Skipped (not needed for event type incremental)")
	} else if incrementalCategoryOnly {
		log.Printf("Elasticsearch: Skipped (not needed for event category incremental)")
	} else if incrementalProductOnly {
		log.Printf("Elasticsearch: Skipped (not needed for event product incremental)")
	} else if incrementalDesignationOnly {
		log.Printf("Elasticsearch: Skipped (not needed for event designation incremental)")
	} else if daywiseOnly {
		log.Printf("Elasticsearch: Skipped (not needed for day-wise economic impact)")
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

	if allIncrementalOnly {
		runAllIncrementalSequential(mysqlDB, clickhouseDB, esClient, config)
		return
	}

	if !companyOnly && !companyCategoryOnly && !companyClassificationOnly && !companyProductOnly && !companySpeakerOnly && !companyVisitorOnly && !companyEventDataOnly && !eventCompanyOnly && !sponsorsOnly && !speakersOnly && !visitorsOnly && !exhibitorOnly && !eventTypeEventChOnly && !eventCategoryEventChOnly && !eventProductChOnly && !eventRankingOnly && !eventDesignationOnly && !locationCountriesOnly && !locationStatesOnly && !locationCitiesOnly && !locationVenuesOnly && !locationSubVenuesOnly && !locationAll && !holidaysOnly && !daywiseOnly && !incrementalEventTypeOnly && !incrementalCategoryOnly && !incrementalProductOnly && !incrementalDesignationOnly && !incrementalExhibitorOnly && !incrementalSpeakerOnly && !incrementalSponsorOnly && !incrementalVisitorOnly && !incrementalAlertsOnly && !alertsOnly {
		if err := utils.TestElasticsearchConnection(esClient, config.ElasticsearchIndex); err != nil {
			log.Fatalf("Elasticsearch connection test failed: %v", err)
		}
	} else if visitorSpreadOnly {
		if err := utils.TestElasticsearchConnection(esClient, config.ElasticsearchIndex); err != nil {
			log.Fatalf("Elasticsearch connection test failed: %v", err)
		}
	} else if allIncrementalOnly {
		if err := utils.TestElasticsearchConnection(esClient, config.ElasticsearchIndex); err != nil {
			log.Fatalf("Elasticsearch connection test failed: %v", err)
		}
	} else {
		if companyOnly {
			if config.CompanyIndex == "" {
				log.Fatal("COMPANY_INDEX is required for company seeding (set in .env)")
			}
			if err := utils.TestElasticsearchConnection(esClient, config.CompanyIndex); err != nil {
				log.Fatalf("Elasticsearch connection test failed for %s: %v", config.CompanyIndex, err)
			}
		} else if companyCategoryOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for company category processing)")
		} else if companyClassificationOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for company classification processing)")
		} else if companyProductOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for company product processing)")
		} else if companySpeakerOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for company speaker processing)")
		} else if companyVisitorOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for company visitor processing)")
		} else if companyEventDataOnly {
			if err := utils.TestElasticsearchConnection(esClient, config.ElasticsearchIndex); err != nil {
				log.Fatalf("Elasticsearch connection test failed (required for company event data): %v", err)
			}
		} else if eventCompanyOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for event-company processing)")
		} else if sponsorsOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for sponsors processing)")
		} else if speakersOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for speakers processing)")
		} else if visitorsOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for visitors processing)")
		} else if exhibitorOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for exhibitors processing)")
		} else if eventTypeEventChOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for event_type_ch processing)")
		} else if incrementalEventTypeOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for event type incremental)")
		} else if incrementalCategoryOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for event category incremental)")
		} else if incrementalProductOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for event product incremental)")
		} else if incrementalDesignationOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for event designation incremental)")
		} else if incrementalExhibitorOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for event exhibitor incremental)")
		} else if incrementalSpeakerOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for event speaker incremental)")
		} else if incrementalSponsorOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for event sponsor incremental)")
		} else if incrementalVisitorOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for event visitor incremental)")
		} else if incrementalAlertsOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for alerts incremental)")
		} else if alertsOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for alerts processing)")
		} else if eventCategoryEventChOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for event_category_ch processing)")
		} else if eventProductChOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for event_product_ch processing)")
		} else if eventRankingOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for event_ranking_ch processing)")
		} else if eventDesignationOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for event_designation_ch processing)")
		} else if locationAll || locationCountriesOnly || locationStatesOnly || locationCitiesOnly || locationVenuesOnly || locationSubVenuesOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for location_ch processing)")
		} else if holidaysOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for holidays processing)")
		} else if daywiseOnly {
			log.Println("WARNING: Skipping Elasticsearch connection test (not needed for day-wise economic impact processing)")
		}
	}

	if companyOnly {
		if err := shared.EnsureSingleTempTableExists(clickhouseDB, "allCompany_ch", config, errorLogFile); err != nil {
			logErrorToFile("Ensure Temp Table (Company)", err)
			log.Fatalf("Failed to ensure temp table exists: %v", err)
		}

		utilsConfig := shared.Config{
			BatchSize:         config.BatchSize,
			NumChunks:         config.NumChunks,
			NumWorkers:        config.NumWorkers,
			ClickHouseWorkers: config.ClickHouseWorkers,
			CompanyIndex:      config.CompanyIndex,
			CompanyV1Index:    config.CompanyV1Index,
		}
		company.ProcessCompanyOnly(mysqlDB, clickhouseDB, esClient, utilsConfig)

		log.Println("Swapping allCompany_ch table...")
		if err := shared.SwapSingleTable(clickhouseDB, "allCompany_ch", config, errorLogFile); err != nil {
			logErrorToFile("Company Table Swap", err)
			log.Fatalf("Failed to swap allCompany_ch: %v", err)
		}
		log.Println("✓ allCompany_ch swapped successfully")
	} else if companyCategoryOnly {
		if err := shared.EnsureSingleTempTableExists(clickhouseDB, "company_category_ch", config, errorLogFile); err != nil {
			logErrorToFile("Ensure Temp Table (Company Category)", err)
			log.Fatalf("Failed to ensure temp table exists: %v", err)
		}

		utilsConfig := shared.Config{
			BatchSize:         config.BatchSize,
			NumChunks:         config.NumChunks,
			NumWorkers:        config.NumWorkers,
			ClickHouseWorkers: config.ClickHouseWorkers,
		}
		company.ProcessCompanyCategoryOnly(mysqlDB, clickhouseDB, utilsConfig)

		log.Println("Optimizing company_category_ch table...")
		optimizeConfig := config
		optimizeConfig.UseTempTables = true // Optimize temp table where data was just inserted
		if err := shared.OptimizeSingleTable(clickhouseDB, "company_category_ch", optimizeConfig, errorLogFile); err != nil {
			logErrorToFile("Company Category Optimization", err)
			log.Printf("⚠️  Error optimizing company_category_ch table: %v", err)
			log.Printf("⚠️  Continuing with table swap...")
		} else {
			log.Println("✓ company_category_ch optimized successfully")
		}

		log.Println("Swapping company_category_ch table...")
		if err := shared.SwapSingleTable(clickhouseDB, "company_category_ch", config, errorLogFile); err != nil {
			logErrorToFile("Company Category Table Swap", err)
			log.Fatalf("Failed to swap company_category_ch: %v", err)
		}
		log.Println("✓ company_category_ch swapped successfully")
	} else if companyClassificationOnly {
		if err := shared.EnsureSingleTempTableExists(clickhouseDB, "company_classification_ch", config, errorLogFile); err != nil {
			logErrorToFile("Ensure Temp Table (Company Classification)", err)
			log.Fatalf("Failed to ensure temp table exists: %v", err)
		}

		utilsConfig := shared.Config{
			BatchSize:         config.BatchSize,
			NumChunks:         config.NumChunks,
			NumWorkers:        config.NumWorkers,
			ClickHouseWorkers: config.ClickHouseWorkers,
		}
		company.ProcessCompanyClassificationOnly(mysqlDB, clickhouseDB, utilsConfig)

		log.Println("Optimizing company_classification_ch table...")
		optimizeConfig := config
		optimizeConfig.UseTempTables = true
		if err := shared.OptimizeSingleTable(clickhouseDB, "company_classification_ch", optimizeConfig, errorLogFile); err != nil {
			logErrorToFile("Company Classification Optimization", err)
			log.Printf("⚠️  Error optimizing company_classification_ch table: %v", err)
			log.Printf("⚠️  Continuing with table swap...")
		} else {
			log.Println("✓ company_classification_ch optimized successfully")
		}

		log.Println("Swapping company_classification_ch table...")
		if err := shared.SwapSingleTable(clickhouseDB, "company_classification_ch", config, errorLogFile); err != nil {
			logErrorToFile("Company Classification Table Swap", err)
			log.Fatalf("Failed to swap company_classification_ch: %v", err)
		}
		log.Println("✓ company_classification_ch swapped successfully")
	} else if companyProductOnly {
		if err := shared.EnsureSingleTempTableExists(clickhouseDB, "company_product_ch", config, errorLogFile); err != nil {
			logErrorToFile("Ensure Temp Table (Company Product)", err)
			log.Fatalf("Failed to ensure temp table exists: %v", err)
		}

		utilsConfig := shared.Config{
			BatchSize:         config.BatchSize,
			NumChunks:         config.NumChunks,
			NumWorkers:        config.NumWorkers,
			ClickHouseWorkers: config.ClickHouseWorkers,
		}
		company.ProcessCompanyProductOnly(mysqlDB, clickhouseDB, utilsConfig)

		log.Println("Optimizing company_product_ch table...")
		optimizeConfig := config
		optimizeConfig.UseTempTables = true // Optimize temp table where data was just inserted
		if err := shared.OptimizeSingleTable(clickhouseDB, "company_product_ch", optimizeConfig, errorLogFile); err != nil {
			logErrorToFile("Company Product Optimization", err)
			log.Printf("⚠️  Error optimizing company_product_ch table: %v", err)
			log.Printf("⚠️  Continuing with table swap...")
		} else {
			log.Println("✓ company_product_ch optimized successfully")
		}

		log.Println("Swapping company_product_ch table...")
		if err := shared.SwapSingleTable(clickhouseDB, "company_product_ch", config, errorLogFile); err != nil {
			logErrorToFile("Company Product Table Swap", err)
			log.Fatalf("Failed to swap company_product_ch: %v", err)
		}
		log.Println("✓ company_product_ch swapped successfully")
	} else if companySpeakerOnly {
		if err := shared.EnsureSingleTempTableExists(clickhouseDB, "company_speaker_ch", config, errorLogFile); err != nil {
			logErrorToFile("Ensure Temp Table (Company Speaker)", err)
			log.Fatalf("Failed to ensure temp table exists: %v", err)
		}

		utilsConfig := shared.Config{
			BatchSize:         config.BatchSize,
			NumChunks:         config.NumChunks,
			NumWorkers:        config.NumWorkers,
			ClickHouseWorkers: config.ClickHouseWorkers,
		}
		company.ProcessCompanySpeakerOnly(mysqlDB, clickhouseDB, utilsConfig)

		log.Println("Optimizing company_speaker_ch table...")
		optimizeConfig := config
		optimizeConfig.UseTempTables = true // Optimize temp table where data was just inserted
		if err := shared.OptimizeSingleTable(clickhouseDB, "company_speaker_ch", optimizeConfig, errorLogFile); err != nil {
			logErrorToFile("Company Speaker Optimization", err)
			log.Printf("⚠️  Error optimizing company_speaker_ch table: %v", err)
			log.Printf("⚠️  Continuing with table swap...")
		} else {
			log.Println("✓ company_speaker_ch optimized successfully")
		}

		log.Println("Swapping company_speaker_ch table...")
		if err := shared.SwapSingleTable(clickhouseDB, "company_speaker_ch", config, errorLogFile); err != nil {
			logErrorToFile("Company Speaker Table Swap", err)
			log.Fatalf("Failed to swap company_speaker_ch: %v", err)
		}
		log.Println("✓ company_speaker_ch swapped successfully")
	} else if companyVisitorOnly {
		if err := shared.EnsureSingleTempTableExists(clickhouseDB, "company_visitor_ch", config, errorLogFile); err != nil {
			logErrorToFile("Ensure Temp Table (Company Visitor)", err)
			log.Fatalf("Failed to ensure temp table exists: %v", err)
		}

		utilsConfig := shared.Config{
			BatchSize:         config.BatchSize,
			NumChunks:         config.NumChunks,
			NumWorkers:        config.NumWorkers,
			ClickHouseWorkers: config.ClickHouseWorkers,
		}
		company.ProcessCompanyVisitorOnly(mysqlDB, clickhouseDB, utilsConfig)

		log.Println("Optimizing company_visitor_ch table...")
		optimizeConfig := config
		optimizeConfig.UseTempTables = true // Optimize temp table where data was just inserted
		if err := shared.OptimizeSingleTable(clickhouseDB, "company_visitor_ch", optimizeConfig, errorLogFile); err != nil {
			logErrorToFile("Company Visitor Optimization", err)
			log.Printf("⚠️  Error optimizing company_visitor_ch table: %v", err)
			log.Printf("⚠️  Continuing with table swap...")
		} else {
			log.Println("✓ company_visitor_ch optimized successfully")
		}

		log.Println("Swapping company_visitor_ch table...")
		if err := shared.SwapSingleTable(clickhouseDB, "company_visitor_ch", config, errorLogFile); err != nil {
			logErrorToFile("Company Visitor Table Swap", err)
			log.Fatalf("Failed to swap company_visitor_ch: %v", err)
		}
		log.Println("✓ company_visitor_ch swapped successfully")
	} else if companyEventDataOnly {
		if err := shared.EnsureSingleTempTableExists(clickhouseDB, "companyEventData_ch", config, errorLogFile); err != nil {
			logErrorToFile("Ensure Temp Table (Company Event Data)", err)
			log.Fatalf("Failed to ensure temp table exists: %v", err)
		}
		utilsConfig := shared.Config{
			BatchSize:          config.BatchSize,
			NumChunks:          config.NumChunks,
			NumWorkers:         config.NumWorkers,
			ClickHouseWorkers:  config.ClickHouseWorkers,
			ElasticsearchIndex: config.ElasticsearchIndex,
		}
		eventdata.ProcessCompanyEventDataOnly(mysqlDB, clickhouseDB, esClient, utilsConfig)
		log.Println("Swapping companyEventData_ch table...")
		if err := shared.SwapSingleTable(clickhouseDB, "companyEventData_ch", config, errorLogFile); err != nil {
			logErrorToFile("Company Event Data Table Swap", err)
			log.Fatalf("Failed to swap companyEventData_ch: %v", err)
		}
		log.Println("✓ companyEventData_ch swapped successfully")
	} else if eventCompanyOnly {
		if err := shared.EnsureSingleTempTableExists(clickhouseDB, "event_company_ch", config, errorLogFile); err != nil {
			logErrorToFile("Ensure Temp Table (Event Company)", err)
			log.Fatalf("Failed to ensure temp table exists: %v", err)
		}
		utilsConfig := shared.Config{
			BatchSize:         config.BatchSize,
			NumChunks:         config.NumChunks,
			NumWorkers:        config.NumWorkers,
			ClickHouseWorkers: config.ClickHouseWorkers,
		}
		eventdata.ProcessEventCompanyOnly(mysqlDB, clickhouseDB, utilsConfig)
		log.Println("Swapping event_company_ch table...")
		if err := shared.SwapSingleTable(clickhouseDB, "event_company_ch", config, errorLogFile); err != nil {
			logErrorToFile("Event Company Table Swap", err)
			log.Fatalf("Failed to swap event_company_ch: %v", err)
		}
		log.Println("✓ event_company_ch swapped successfully")
	} else if exhibitorOnly {
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

		utils.ProcessVisitorsOnly(mysqlDB, clickhouseDB, config)

		log.Println("Optimizing event_visitors_ch table...")
		if err := shared.OptimizeSingleTable(clickhouseDB, "event_visitors_ch", config, errorLogFile); err != nil {
			logErrorToFile("Visitors Optimization", err)
			log.Printf("⚠️  Error optimizing event_visitors_ch table: %v", err)
			log.Printf("⚠️  Continuing with table swap...")
		} else {
			log.Println("✓ event_visitors_ch optimized successfully")
		}

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

		utils.ProcessSpeakersOnly(mysqlDB, clickhouseDB, config)

		log.Println("Optimizing event_speaker_ch table...")
		if err := shared.OptimizeSingleTable(clickhouseDB, "event_speaker_ch", config, errorLogFile); err != nil {
			logErrorToFile("Speakers Optimization", err)
			log.Printf("⚠️  Error optimizing event_speaker_ch table: %v", err)
			log.Printf("⚠️  Continuing with table swap...")
		} else {
			log.Println("✓ event_speaker_ch optimized successfully")
		}

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

		log.Println("Optimizing event_type_ch table...")
		if err := shared.OptimizeSingleTable(clickhouseDB, "event_type_ch", config, errorLogFile); err != nil {
			logErrorToFile("Event Type Optimization", err)
			log.Printf("⚠️  Error optimizing event_type_ch table: %v", err)
			log.Printf("⚠️  Continuing with table swap...")
		} else {
			log.Println("✓ event_type_ch optimized successfully")
		}

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
	} else if eventProductChOnly {
		if err := shared.EnsureSingleTempTableExists(clickhouseDB, "event_product_ch", config, errorLogFile); err != nil {
			logErrorToFile("Ensure Temp Table (Event Product)", err)
			log.Fatalf("Failed to ensure temp table exists: %v", err)
		}

		utilsConfig := shared.Config{
			BatchSize:         config.BatchSize,
			NumChunks:         config.NumChunks,
			NumWorkers:        config.NumWorkers,
			ClickHouseWorkers: config.ClickHouseWorkers,
		}
		utils.ProcessEventProductChOnly(mysqlDB, clickhouseDB, utilsConfig)

		log.Println("Optimizing event_product_ch table...")
		if err := shared.OptimizeSingleTable(clickhouseDB, "event_product_ch", config, errorLogFile); err != nil {
			logErrorToFile("Event Product Optimization", err)
			log.Printf("⚠️  Error optimizing event_product_ch table: %v", err)
			log.Printf("⚠️  Continuing with table swap...")
		} else {
			log.Println("✓ event_product_ch optimized successfully")
		}

		// Swap table after processing
		log.Println("Swapping event_product_ch table...")
		if err := shared.SwapSingleTable(clickhouseDB, "event_product_ch", config, errorLogFile); err != nil {
			logErrorToFile("Event Product Table Swap", err)
			log.Fatalf("Failed to swap event_product_ch: %v", err)
		}
		log.Println("✓ event_product_ch swapped successfully")
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

		log.Println("Optimizing event_designation_ch table...")
		optimizeConfig := config
		optimizeConfig.UseTempTables = true // Optimize temp table where data was just inserted
		if err := shared.OptimizeSingleTable(clickhouseDB, "event_designation_ch", optimizeConfig, errorLogFile); err != nil {
			logErrorToFile("Event Designation Optimization", err)
			log.Printf("⚠️  Error optimizing event_designation_ch table: %v", err)
			log.Printf("⚠️  Continuing with table swap...")
		} else {
			log.Println("✓ event_designation_ch optimized successfully")
		}

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
	} else if incrementalAlleventOnly {
		utilsConfig := config
		utilsConfig.UseTempTables = false
		if err := microservice.ProcessIncrementalAllevent(mysqlDB, clickhouseDB, esClient, utilsConfig); err != nil {
			logErrorToFile("Incremental Allevent", err)
			log.Fatalf("Incremental allevent sync failed: %v", err)
		}
		log.Println("✓ Incremental allevent sync completed successfully")
	} else if incrementalEventTypeOnly {
		utilsConfig := config
		if err := utils.ProcessIncrementalEventType(mysqlDB, clickhouseDB, utilsConfig); err != nil {
			logErrorToFile("Incremental Event Type", err)
			log.Fatalf("Incremental event type sync failed: %v", err)
		}
		log.Println("✓ Incremental event type sync completed successfully")
	} else if incrementalCategoryOnly {
		utilsConfig := config
		if err := utils.ProcessIncrementalEventCategory(mysqlDB, clickhouseDB, utilsConfig); err != nil {
			logErrorToFile("Incremental Event Category", err)
			log.Fatalf("Incremental event category sync failed: %v", err)
		}
		log.Println("✓ Incremental event category sync completed successfully")
	} else if incrementalProductOnly {
		utilsConfig := config
		if err := utils.ProcessIncrementalEventProduct(mysqlDB, clickhouseDB, utilsConfig); err != nil {
			logErrorToFile("Incremental Event Product", err)
			log.Fatalf("Incremental event product sync failed: %v", err)
		}
		log.Println("✓ Incremental event product sync completed successfully")
	} else if incrementalDesignationOnly {
		utilsConfig := config
		if err := utils.ProcessIncrementalEventDesignation(mysqlDB, clickhouseDB, utilsConfig); err != nil {
			logErrorToFile("Incremental Event Designation", err)
			log.Fatalf("Incremental event designation sync failed: %v", err)
		}
		log.Println("✓ Incremental event designation sync completed successfully")
	} else if incrementalExhibitorOnly {
		utilsConfig := config
		if err := utils.ProcessIncrementalEventExhibitor(mysqlDB, clickhouseDB, utilsConfig); err != nil {
			logErrorToFile("Incremental Event Exhibitor", err)
			log.Fatalf("Incremental event exhibitor sync failed: %v", err)
		}
		log.Println("✓ Incremental event exhibitor sync completed successfully")
	} else if incrementalSpeakerOnly {
		utilsConfig := config
		if err := utils.ProcessIncrementalEventSpeaker(mysqlDB, clickhouseDB, utilsConfig); err != nil {
			logErrorToFile("Incremental Event Speaker", err)
			log.Fatalf("Incremental event speaker sync failed: %v", err)
		}
		log.Println("✓ Incremental event speaker sync completed successfully")
	} else if incrementalSponsorOnly {
		utilsConfig := config
		if err := utils.ProcessIncrementalEventSponsor(mysqlDB, clickhouseDB, utilsConfig); err != nil {
			logErrorToFile("Incremental Event Sponsor", err)
			log.Fatalf("Incremental event sponsor sync failed: %v", err)
		}
		log.Println("✓ Incremental event sponsor sync completed successfully")
	} else if incrementalVisitorOnly {
		utilsConfig := config
		if err := utils.ProcessIncrementalEventVisitor(mysqlDB, clickhouseDB, utilsConfig); err != nil {
			logErrorToFile("Incremental Event Visitor", err)
			log.Fatalf("Incremental event visitor sync failed: %v", err)
		}
		log.Println("✓ Incremental event visitor sync completed successfully")
	} else if incrementalAlertsOnly {
		gdacBaseURL := os.Getenv("gdac_base_url")
		gdacEndpoint := os.Getenv("gdac_event_search_endpoint")
		if gdacBaseURL == "" {
			log.Fatal("ERROR: GDAC_BASE_URL environment variable is not set")
		}
		if gdacEndpoint == "" {
			log.Fatal("ERROR: GDAC_EVENT_SEARCH_ENDPOINT environment variable is not set")
		}
		validCountries, err := microservice.GetValidCountries()
		if err != nil {
			log.Fatalf("ERROR: Failed to get valid countries: %v", err)
		}
		if len(validCountries) == 0 {
			log.Fatal("ERROR: No valid countries found")
		}
		alertConfig := shared.Config{
			BatchSize:         config.BatchSize,
			NumChunks:         config.NumChunks,
			NumWorkers:        config.NumWorkers,
			ClickHouseWorkers: config.ClickHouseWorkers,
			ClickhouseDB:      config.ClickhouseDB,
		}
		if err := microservice.ProcessIncrementalAlerts(clickhouseDB, gdacBaseURL, gdacEndpoint, validCountries, alertConfig); err != nil {
			logErrorToFile("Incremental Alerts", err)
			log.Fatalf("Incremental alert sync failed: %v", err)
		}
		log.Println("✓ Incremental alert sync completed successfully")
	} else if allEventOnly {
		if err := shared.EnsureSingleTempTableExists(clickhouseDB, "allevent_ch", config, errorLogFile); err != nil {
			logErrorToFile("Ensure Temp Table (All Event)", err)
			log.Fatalf("Failed to ensure allevent temp table exists: %v", err)
		}

		if err := shared.EnsureSingleTempTableExists(clickhouseDB, "event_daywiseEconomicImpact_ch", config, errorLogFile); err != nil {
			logErrorToFile("Ensure Temp Table (Day-Wise Economic Impact)", err)
			log.Fatalf("Failed to ensure day-wise economic impact temp table exists: %v", err)
		}

		utilsConfig := config
		utilsConfig.UseTempTables = false // When running individually, read from production _ch tables
		microservice.ProcessAllEventOnly(mysqlDB, clickhouseDB, esClient, utilsConfig)

		// Check if there are remaining failed batches before optimization
		if microservice.HasRemainingFailedBatches() {
			log.Println("")
			log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
			log.Println("⚠️  WARNING: Failed batches still remain - skipping optimization")
			log.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
			log.Println("Some batches failed to insert due to memory errors.")
			log.Println("Please rerun the script to retry failed batches.")
			log.Println("Optimization and table swap will be performed after all batches succeed.")
			log.Println("")
			return // Exit early, don't proceed with optimization
		}

		log.Println("Optimizing allevent_ch table...")
		if err := shared.OptimizeSingleTable(clickhouseDB, "allevent_ch", config, errorLogFile); err != nil {
			logErrorToFile("All Event Optimization", err)
			log.Printf("⚠️  Error optimizing allevent_ch table: %v", err)
			log.Printf("⚠️  Continuing with table swap...")
		} else {
			log.Println("✓ allevent_ch optimized successfully")
		}

		log.Println("Optimizing event_daywiseEconomicImpact_ch table...")
		if err := shared.OptimizeSingleTable(clickhouseDB, "event_daywiseEconomicImpact_ch", config, errorLogFile); err != nil {
			logErrorToFile("Day-Wise Economic Impact Optimization", err)
			log.Printf("⚠️  Error optimizing event_daywiseEconomicImpact_ch table: %v", err)
			log.Printf("⚠️  Continuing with table swap...")
		} else {
			log.Println("✓ event_daywiseEconomicImpact_ch optimized successfully")
		}

		log.Println("Swapping allevent_ch table...")
		if err := shared.SwapSingleTable(clickhouseDB, "allevent_ch", config, errorLogFile); err != nil {
			logErrorToFile("All Event Table Swap", err)
			log.Fatalf("Failed to swap allevent_ch: %v", err)
		}
		log.Println("✓ allevent_ch swapped successfully")

		log.Println("Swapping event_daywiseEconomicImpact_ch table...")
		if err := shared.SwapSingleTable(clickhouseDB, "event_daywiseEconomicImpact_ch", config, errorLogFile); err != nil {
			logErrorToFile("Day-Wise Economic Impact Table Swap", err)
			log.Fatalf("Failed to swap event_daywiseEconomicImpact_ch: %v", err)
		}
		log.Println("✓ event_daywiseEconomicImpact_ch swapped successfully")
	} else if locationCountriesOnly || locationStatesOnly || locationCitiesOnly || locationVenuesOnly || locationSubVenuesOnly {
		// Individual location types - no temp table creation/dropping, just process directly
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

		alertConfig := shared.Config{
			BatchSize:         config.BatchSize,
			NumChunks:         config.NumChunks,
			NumWorkers:        config.NumWorkers,
			ClickHouseWorkers: config.ClickHouseWorkers,
			UseTempTables:     false, // -alerts only: write to event_type_ch (production)
			ClickhouseDB:      config.ClickhouseDB,
		}
		if err := microservice.ProcessAlertsFromAPI(clickhouseDB, gdacBaseURL, gdacEndpoint, validCountries, alertConfig); err != nil {
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
	} else if daywiseOnly {
		// Standalone day-wise flow: fetch event_ids from estimate in batches, process, insert. Never runs with -all.
		if err := shared.EnsureSingleTempTableExists(clickhouseDB, "event_daywiseEconomicImpact_ch", config, errorLogFile); err != nil {
			logErrorToFile("Ensure Temp Table (Day-Wise Economic Impact)", err)
			log.Fatalf("Failed to ensure day-wise economic impact temp table exists: %v", err)
		}

		utilsConfig := shared.Config{
			BatchSize:         config.BatchSize,
			NumChunks:         config.NumChunks,
			NumWorkers:        config.NumWorkers,
			ClickHouseWorkers: config.ClickHouseWorkers,
		}
		microservice.ProcessDayWiseEconomicImpactOnly(mysqlDB, clickhouseDB, utilsConfig)

		log.Println("Optimizing event_daywiseEconomicImpact_ch table...")
		if err := shared.OptimizeSingleTable(clickhouseDB, "event_daywiseEconomicImpact_ch", config, errorLogFile); err != nil {
			logErrorToFile("Day-Wise Economic Impact Optimization", err)
			log.Printf("⚠️  Error optimizing event_daywiseEconomicImpact_ch table: %v", err)
			log.Printf("⚠️  Continuing with table swap...")
		} else {
			log.Println("✓ event_daywiseEconomicImpact_ch optimized successfully")
		}

		log.Println("Swapping event_daywiseEconomicImpact_ch table...")
		if err := shared.SwapSingleTable(clickhouseDB, "event_daywiseEconomicImpact_ch", config, errorLogFile); err != nil {
			logErrorToFile("Day-Wise Economic Impact Table Swap", err)
			log.Fatalf("Failed to swap event_daywiseEconomicImpact_ch: %v", err)
		}
		log.Println("✓ event_daywiseEconomicImpact_ch swapped successfully")
	} else {
		log.Println("Error: No specific table mode selected!")
		log.Println("Please specify one of the individual flags (e.g. -visitors, -allevent-incremental)")
		log.Println("For incremental syncs via cron, use -task=<name> (e.g. -task=allevent-incremental)")
		log.Println("")
		log.Println("Example: go run main.go -task=visitors -chunks=10 -workers=30")
		log.Println("Example: go run main.go -task=allevent-incremental")
		os.Exit(1)
	}
}
