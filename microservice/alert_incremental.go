package microservice

import (
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"seeders/shared"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ProcessIncrementalAlerts fetches alerts from GDAC API for yesterday only and upserts into
// alerts_ch, location_polygons_ch, and event_type_ch. No deletion of old data.
func ProcessIncrementalAlerts(clickhouseConn driver.Conn, gdacBaseURL, gdacEndpoint string, validCountries []string, config shared.Config) error {
	startTime := time.Now()
	log.Println("=== Starting Incremental Alert Sync (yesterday only, upsert) ===")
	log.Printf("Log file: %s", shared.IncrementalLogFile)

	shared.WriteIncrementalLog("")
	shared.WriteIncrementalLog("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	shared.WriteIncrementalLog(fmt.Sprintf("[%s] INCREMENTAL ALERT SYNC STARTED", startTime.Format("2006-01-02 15:04:05")))
	shared.WriteIncrementalLog("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	yesterday := time.Now().AddDate(0, 0, -1)
	yesterdayStr := yesterday.Format("2006-01-02")

	shared.WriteIncrementalLog("")
	shared.WriteIncrementalLog(fmt.Sprintf("1. SCOPE: Fetching alerts for yesterday (%s) only", yesterdayStr))

	// Production tables (upsert directly, no temp/swap)
	alertsTableName := shared.GetTableNameWithDB(shared.GetClickHouseTableName("alerts_ch", shared.Config{UseTempTables: false, ClickhouseDB: config.ClickhouseDB}), config)
	locationPolygonsTableName := shared.GetTableNameWithDB(shared.GetClickHouseTableName("location_polygons_ch", shared.Config{UseTempTables: false, ClickhouseDB: config.ClickhouseDB}), config)
	eventTypeTableName := shared.GetTableNameWithDB(shared.GetClickHouseTableName("event_type_ch", shared.Config{UseTempTables: false, ClickhouseDB: config.ClickhouseDB}), config)

	// Initialize loggers
	if err := InitPolygonErrorLogger(); err != nil {
		log.Printf("WARNING: Failed to initialize polygon error logger: %v", err)
	}
	defer func() {
		if err := FinalizePolygonErrorLogger(); err != nil {
			log.Printf("WARNING: Failed to finalize polygon error logger: %v", err)
		}
	}()

	if err := InitAlertCountryLogger(); err != nil {
		log.Printf("WARNING: Failed to initialize alert country logger: %v", err)
	}
	defer func() {
		if err := FinalizeAlertCountryLogger(); err != nil {
			log.Printf("WARNING: Failed to finalize alert country logger: %v", err)
		}
	}()

	httpClient := &http.Client{
		Timeout: 30 * time.Second,
	}

	var allAlerts []AlertChRecord
	allMetadata := make(map[string]*AlertMetadata)
	var mu sync.Mutex

	semaphore := make(chan struct{}, 5)
	var wg sync.WaitGroup

	for _, country := range validCountries {
		wg.Add(1)
		semaphore <- struct{}{}

		go func(countryName string) {
			defer func() {
				<-semaphore
				wg.Done()
			}()

			countryStart := time.Now()
			log.Printf("Fetching alerts for %s (yesterday: %s)", countryName, yesterdayStr)

			alertLevel := "Green;Orange;Red"
			payload := FetchAlertRequest{
				Country:  &countryName,
				Level:    &alertLevel,
				FromDate: &yesterdayStr,
				ToDate:   &yesterdayStr,
			}

			initialGDACSearchURL, _, err := fetchAlerts(gdacBaseURL, gdacEndpoint, payload)
			if err != nil {
				log.Printf("ERROR: Failed to build GDAC search URL for %s: %v", countryName, err)
				LogAlertCountryFetchError(countryName, err, time.Since(countryStart))
				return
			}

			features, err := getAlerts(httpClient, gdacBaseURL, gdacEndpoint, []FetchAlertRequest{payload}, 1)
			if err != nil {
				log.Printf("ERROR: Failed to fetch alerts for %s: %v", countryName, err)
				LogAlertCountryFetchError(countryName, err, time.Since(countryStart))
				return
			}

			log.Printf("Fetched %d alerts for %s", len(features), countryName)

			now := time.Now()
			var countryAlerts []AlertChRecord
			countryMetadata := make(map[string]*AlertMetadata)
			skippedDroughts := 0
			var parseErrors []string

			for _, feature := range features {
				alert, metadata, err := parseAlertFeature(feature, now, countryName)
				if err != nil {
					parseErrors = append(parseErrors, err.Error())
					continue
				}
				if alert != nil && metadata != nil {
					metadata.GDACSearchURL = initialGDACSearchURL
					countryAlerts = append(countryAlerts, *alert)
					if metadata.GeometryLink != "" {
						countryMetadata[metadata.AlertID] = metadata
					}
				} else {
					skippedDroughts++
				}
			}

			mu.Lock()
			allAlerts = append(allAlerts, countryAlerts...)
			for alertID, meta := range countryMetadata {
				allMetadata[alertID] = meta
			}
			mu.Unlock()

			duration := time.Since(countryStart)
			log.Printf("Processed %d alerts for %s in %v", len(countryAlerts), countryName, duration)
			LogAlertCountryResult(countryName, len(features), len(countryAlerts), skippedDroughts, parseErrors, duration, true)
		}(country)
	}

	wg.Wait()

	shared.WriteIncrementalLog("")
	shared.WriteIncrementalLog(fmt.Sprintf("2. FETCH: %d alerts from GDAC for yesterday (%s)", len(allAlerts), yesterdayStr))

	if len(allAlerts) == 0 {
		log.Println("No alerts fetched for yesterday, nothing to sync")
		shared.WriteIncrementalLog("")
		shared.WriteIncrementalLog(fmt.Sprintf("[%s] INCREMENTAL ALERT SYNC COMPLETED (no alerts)", time.Now().Format("2006-01-02 15:04:05")))
		return nil
	}

	// Upsert alerts_ch
	batchSize := 1000
	for i := 0; i < len(allAlerts); i += batchSize {
		end := i + batchSize
		if end > len(allAlerts) {
			end = len(allAlerts)
		}
		batch := allAlerts[i:end]
		if err := InsertAlertsChDataIntoTable(clickhouseConn, batch, alertsTableName); err != nil {
			shared.WriteIncrementalLog(fmt.Sprintf("3. INSERT alerts_ch: FAILED - %v", err))
			return fmt.Errorf("failed to insert alert batch: %w", err)
		}
	}

	shared.WriteIncrementalLog("")
	shared.WriteIncrementalLog(fmt.Sprintf("3. INSERT alerts_ch: %d records upserted", len(allAlerts)))

	// Upsert location_polygons_ch and event_type_ch
	if len(allMetadata) > 0 {
		log.Printf("Processing polygons for %d alerts with geometry links", len(allMetadata))
		if err := processPolygons(clickhouseConn, httpClient, allMetadata, semaphore, eventTypeTableName, locationPolygonsTableName); err != nil {
			shared.WriteIncrementalLog("")
			shared.WriteIncrementalLog(fmt.Sprintf("4. POLYGONS: FAILED - %v", err))
			return fmt.Errorf("failed to process polygons: %w", err)
		}
		shared.WriteIncrementalLog("")
		shared.WriteIncrementalLog(fmt.Sprintf("4. POLYGONS: %d alerts with geometry processed", len(allMetadata)))
	} else {
		shared.WriteIncrementalLog("")
		shared.WriteIncrementalLog("4. POLYGONS: 0 (no alerts with geometry links)")
	}

	// Optional OPTIMIZE
	log.Printf("Running OPTIMIZE on alerts_ch...")
	if optimizeErr := shared.OptimizeSingleTable(clickhouseConn, "alerts_ch", config, ""); optimizeErr != nil {
		log.Printf("WARNING: OPTIMIZE on alerts_ch failed: %v", optimizeErr)
	}
	log.Printf("Running OPTIMIZE on location_polygons_ch...")
	if optimizeErr := shared.OptimizeSingleTable(clickhouseConn, "location_polygons_ch", config, ""); optimizeErr != nil {
		log.Printf("WARNING: OPTIMIZE on location_polygons_ch failed: %v", optimizeErr)
	}

	endTime := time.Now()
	duration := endTime.Sub(startTime)
	shared.WriteIncrementalLog("")
	shared.WriteIncrementalLog("SUMMARY:")
	shared.WriteIncrementalLog(fmt.Sprintf("   Date: %s (yesterday)", yesterdayStr))
	shared.WriteIncrementalLog(fmt.Sprintf("   Alerts upserted: %d", len(allAlerts)))
	shared.WriteIncrementalLog(fmt.Sprintf("   Alerts with polygons: %d", len(allMetadata)))
	shared.WriteIncrementalLog(fmt.Sprintf("   Duration: %v", duration.Round(time.Millisecond)))
	shared.WriteIncrementalLog(fmt.Sprintf("[%s] INCREMENTAL ALERT SYNC COMPLETED", endTime.Format("2006-01-02 15:04:05")))
	shared.WriteIncrementalLog("")

	log.Println("=== Incremental Alert Sync Completed ===")
	return nil
}
