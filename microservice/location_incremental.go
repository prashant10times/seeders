package microservice

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"seeders/shared"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const locationIncrementalIDBuffer uint32 = 100000

func ProcessIncrementalLocation(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) error {
	startTime := time.Now()
	log.Println("=== Starting Incremental Location Sync ===")
	log.Printf("Log file: %s", shared.IncrementalLogFile)

	shared.WriteIncrementalLog("")
	shared.WriteIncrementalLog("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	shared.WriteIncrementalLog(fmt.Sprintf("[%s] INCREMENTAL LOCATION SYNC STARTED", startTime.Format("2006-01-02 15:04:05")))
	shared.WriteIncrementalLog("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	shared.WriteIncrementalLog("Scope rule: created >= CURDATE() - INTERVAL 1 DAY")
	shared.WriteIncrementalLog("Insert rule: add new rows only (skip if id_10x already exists)")

	tableName := shared.GetTableNameWithDB(shared.GetClickHouseTableName("location_ch", config), config)
	startID, err := getIncrementalLocationStartID(clickhouseConn, tableName, locationIncrementalIDBuffer)
	if err != nil {
		return fmt.Errorf("get start location id: %w", err)
	}
	log.Printf("Incremental location start id: %d (buffer: %d)", startID, locationIncrementalIDBuffer)
	shared.WriteIncrementalLog(fmt.Sprintf("Starting ID: %d (MAX(id) + %d + 1)", startID, locationIncrementalIDBuffer))

	countryCoordsLookup, err := buildCountryCoordinatesLookup()
	if err != nil {
		return fmt.Errorf("build country coordinates lookup: %w", err)
	}

	currentID := startID
	inserted := map[string]int{
		"COUNTRY":   0,
		"STATE":     0,
		"CITY":      0,
		"VENUE":     0,
		"SUB_VENUE": 0,
	}

	countryInserted, nextID, err := processIncrementalCountries(mysqlDB, clickhouseConn, tableName, config, currentID, countryCoordsLookup)
	if err != nil {
		return err
	}
	inserted["COUNTRY"] = countryInserted
	currentID = nextID

	stateInserted, nextID, err := processIncrementalStates(mysqlDB, clickhouseConn, tableName, config, currentID)
	if err != nil {
		return err
	}
	inserted["STATE"] = stateInserted
	currentID = nextID

	cityInserted, nextID, err := processIncrementalCities(mysqlDB, clickhouseConn, tableName, config, currentID)
	if err != nil {
		return err
	}
	inserted["CITY"] = cityInserted
	currentID = nextID

	venueInserted, nextID, err := processIncrementalVenues(mysqlDB, clickhouseConn, tableName, config, currentID)
	if err != nil {
		return err
	}
	inserted["VENUE"] = venueInserted
	currentID = nextID

	subVenueInserted, nextID, err := processIncrementalSubVenues(mysqlDB, clickhouseConn, tableName, config, currentID)
	if err != nil {
		return err
	}
	inserted["SUB_VENUE"] = subVenueInserted
	currentID = nextID

	totalInserted := inserted["COUNTRY"] + inserted["STATE"] + inserted["CITY"] + inserted["VENUE"] + inserted["SUB_VENUE"]

	log.Printf("Incremental location inserted rows: countries=%d, states=%d, cities=%d, venues=%d, sub_venues=%d, total=%d",
		inserted["COUNTRY"], inserted["STATE"], inserted["CITY"], inserted["VENUE"], inserted["SUB_VENUE"], totalInserted)

	shared.WriteIncrementalLog("")
	shared.WriteIncrementalLog("SUMMARY:")
	shared.WriteIncrementalLog(fmt.Sprintf("   Countries inserted: %d", inserted["COUNTRY"]))
	shared.WriteIncrementalLog(fmt.Sprintf("   States inserted: %d", inserted["STATE"]))
	shared.WriteIncrementalLog(fmt.Sprintf("   Cities inserted: %d", inserted["CITY"]))
	shared.WriteIncrementalLog(fmt.Sprintf("   Venues inserted: %d", inserted["VENUE"]))
	shared.WriteIncrementalLog(fmt.Sprintf("   Sub-venues inserted: %d", inserted["SUB_VENUE"]))
	shared.WriteIncrementalLog(fmt.Sprintf("   Total inserted: %d", totalInserted))
	shared.WriteIncrementalLog(fmt.Sprintf("[%s] INCREMENTAL LOCATION SYNC COMPLETED", time.Now().Format("2006-01-02 15:04:05")))

	if optimizeErr := shared.OptimizeSingleTable(clickhouseConn, "location_ch", config, ""); optimizeErr != nil {
		log.Printf("WARNING: OPTIMIZE location_ch failed: %v", optimizeErr)
		shared.WriteIncrementalLog(fmt.Sprintf("OPTIMIZE: WARNING - Failed: %v", optimizeErr))
	} else {
		log.Println("OPTIMIZE location_ch completed successfully")
		shared.WriteIncrementalLog("OPTIMIZE: Completed successfully")
	}

	_ = currentID
	return nil
}

func getIncrementalLocationStartID(clickhouseConn driver.Conn, tableName string, buffer uint32) (uint32, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	query := fmt.Sprintf("SELECT ifNull(max(id), 0) FROM %s", tableName)
	log.Println("Location Incremental CH Query (max id):", query)
	row := clickhouseConn.QueryRow(ctx, query)

	var maxID uint32
	if err := row.Scan(&maxID); err != nil {
		return 0, fmt.Errorf("scan max(id): %w", err)
	}
	return maxID + buffer + 1, nil
}

func processIncrementalCountries(mysqlDB *sql.DB, clickhouseConn driver.Conn, tableName string, config shared.Config, startID uint32, countryCoordsLookup map[string]CountryCoordinatesMap) (int, uint32, error) {
	offset := 0
	currentID := startID
	inserted := 0
	batchSize := config.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	for {
		batchData, err := fetchCountryIncrementalBatch(mysqlDB, offset, batchSize)
		if err != nil {
			return inserted, currentID, fmt.Errorf("fetch country incremental batch: %w", err)
		}
		if len(batchData) == 0 {
			break
		}

		now := time.Now()
		records := make([]LocationChRecord, 0, len(batchData))
		candidates := make([]string, 0, len(batchData))

		for _, row := range batchData {
			rec, ok := buildCountryLocationRecord(row, currentID, now, countryCoordsLookup)
			if !ok {
				continue
			}
			records = append(records, rec)
			candidates = append(candidates, rec.ID10x)
			currentID++
		}

		newRecords, err := filterNewLocationRecords(clickhouseConn, tableName, "COUNTRY", records, candidates)
		if err != nil {
			return inserted, currentID, fmt.Errorf("filter new country records: %w", err)
		}
		if len(newRecords) > 0 {
			if err := insertLocationCountriesIntoTable(clickhouseConn, tableName, newRecords); err != nil {
				return inserted, currentID, err
			}
			inserted += len(newRecords)
		}

		offset += len(batchData)
		if len(batchData) < batchSize {
			break
		}
	}

	return inserted, currentID, nil
}

func processIncrementalStates(mysqlDB *sql.DB, clickhouseConn driver.Conn, tableName string, config shared.Config, startID uint32) (int, uint32, error) {
	offset := 0
	currentID := startID
	inserted := 0
	seenStates := make(map[string]bool)

	batchSize := config.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	countryUUIDLookup, countryNameLookup, err := buildCountryLookupFromTable(clickhouseConn, tableName)
	if err != nil {
		return inserted, currentID, fmt.Errorf("build country lookup for states: %w", err)
	}

	for {
		batchData, err := fetchStateIncrementalBatch(mysqlDB, offset, batchSize)
		if err != nil {
			return inserted, currentID, fmt.Errorf("fetch state incremental batch: %w", err)
		}
		if len(batchData) == 0 {
			break
		}

		now := time.Now()
		records := make([]LocationChRecord, 0, len(batchData))
		candidates := make([]string, 0, len(batchData))

		for _, row := range batchData {
			rec, ok := buildStateLocationRecord(row, currentID, now, countryUUIDLookup, countryNameLookup, seenStates)
			if !ok {
				continue
			}
			records = append(records, rec)
			candidates = append(candidates, rec.ID10x)
			currentID++
		}

		newRecords, err := filterNewLocationRecords(clickhouseConn, tableName, "STATE", records, candidates)
		if err != nil {
			return inserted, currentID, fmt.Errorf("filter new state records: %w", err)
		}
		if len(newRecords) > 0 {
			if err := insertLocationStatesIntoTable(clickhouseConn, tableName, newRecords); err != nil {
				return inserted, currentID, err
			}
			inserted += len(newRecords)
		}

		offset += len(batchData)
		if len(batchData) < batchSize {
			break
		}
	}

	return inserted, currentID, nil
}

func processIncrementalCities(mysqlDB *sql.DB, clickhouseConn driver.Conn, tableName string, config shared.Config, startID uint32) (int, uint32, error) {
	lastID := 0
	currentID := startID
	inserted := 0

	batchSize := config.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	countryUUIDLookup, countryNameLookup, err := buildCountryLookupFromTable(clickhouseConn, tableName)
	if err != nil {
		return inserted, currentID, fmt.Errorf("build country lookup for cities: %w", err)
	}

	for {
		batchData, err := fetchCityIncrementalBatch(mysqlDB, lastID, batchSize)
		if err != nil {
			return inserted, currentID, fmt.Errorf("fetch city incremental batch: %w", err)
		}
		if len(batchData) == 0 {
			break
		}

		now := time.Now()
		records := make([]LocationChRecord, 0, len(batchData))
		candidates := make([]string, 0, len(batchData))

		for _, row := range batchData {
			rec, ok := buildCityLocationRecord(row, currentID, now, countryUUIDLookup, countryNameLookup)
			if !ok {
				continue
			}
			records = append(records, rec)
			candidates = append(candidates, rec.ID10x)
			currentID++
		}

		newRecords, err := filterNewLocationRecords(clickhouseConn, tableName, "CITY", records, candidates)
		if err != nil {
			return inserted, currentID, fmt.Errorf("filter new city records: %w", err)
		}
		if len(newRecords) > 0 {
			if err := insertLocationCitiesIntoTable(clickhouseConn, tableName, newRecords); err != nil {
				return inserted, currentID, err
			}
			inserted += len(newRecords)
		}

		lastRowID := shared.SafeConvertToString(batchData[len(batchData)-1]["id_10x"])
		parsedID, parseErr := strconv.Atoi(lastRowID)
		if parseErr != nil {
			return inserted, currentID, fmt.Errorf("parse city id_10x %q: %w", lastRowID, parseErr)
		}
		lastID = parsedID

		if len(batchData) < batchSize {
			break
		}
	}

	return inserted, currentID, nil
}

func processIncrementalVenues(mysqlDB *sql.DB, clickhouseConn driver.Conn, tableName string, config shared.Config, startID uint32) (int, uint32, error) {
	lastID := 0
	currentID := startID
	inserted := 0

	batchSize := config.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	countryUUIDLookup, countryNameLookup, err := buildCountryLookupFromTable(clickhouseConn, tableName)
	if err != nil {
		return inserted, currentID, fmt.Errorf("build country lookup for venues: %w", err)
	}
	cityUUIDLookup, err := buildCityLookupFromTable(clickhouseConn, tableName)
	if err != nil {
		return inserted, currentID, fmt.Errorf("build city lookup for venues: %w", err)
	}
	stateUUIDLookup, err := buildStateLookupFromTable(clickhouseConn, tableName)
	if err != nil {
		return inserted, currentID, fmt.Errorf("build state lookup for venues: %w", err)
	}

	for {
		batchData, err := fetchVenueIncrementalBatch(mysqlDB, lastID, batchSize)
		if err != nil {
			return inserted, currentID, fmt.Errorf("fetch venue incremental batch: %w", err)
		}
		if len(batchData) == 0 {
			break
		}

		now := time.Now()
		records := make([]LocationChRecord, 0, len(batchData))
		candidates := make([]string, 0, len(batchData))

		for _, row := range batchData {
			rec, ok := buildVenueLocationRecord(row, currentID, now, countryUUIDLookup, countryNameLookup, cityUUIDLookup, stateUUIDLookup)
			if !ok {
				continue
			}
			records = append(records, rec)
			candidates = append(candidates, rec.ID10x)
			currentID++
		}

		newRecords, err := filterNewLocationRecords(clickhouseConn, tableName, "VENUE", records, candidates)
		if err != nil {
			return inserted, currentID, fmt.Errorf("filter new venue records: %w", err)
		}
		if len(newRecords) > 0 {
			if err := insertLocationVenuesIntoTable(clickhouseConn, tableName, newRecords); err != nil {
				return inserted, currentID, err
			}
			inserted += len(newRecords)
		}

		lastRowID := shared.SafeConvertToString(batchData[len(batchData)-1]["id_10x"])
		parsedID, parseErr := strconv.Atoi(lastRowID)
		if parseErr != nil {
			return inserted, currentID, fmt.Errorf("parse venue id_10x %q: %w", lastRowID, parseErr)
		}
		lastID = parsedID

		if len(batchData) < batchSize {
			break
		}
	}

	return inserted, currentID, nil
}

func processIncrementalSubVenues(mysqlDB *sql.DB, clickhouseConn driver.Conn, tableName string, config shared.Config, startID uint32) (int, uint32, error) {
	lastID := 0
	currentID := startID
	inserted := 0

	batchSize := config.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	venueUUIDLookup, err := buildVenueLookupFromTable(clickhouseConn, tableName)
	if err != nil {
		return inserted, currentID, fmt.Errorf("build venue lookup for sub-venues: %w", err)
	}

	for {
		batchData, err := fetchSubVenueIncrementalBatch(mysqlDB, lastID, batchSize)
		if err != nil {
			return inserted, currentID, fmt.Errorf("fetch sub-venue incremental batch: %w", err)
		}
		if len(batchData) == 0 {
			break
		}

		now := time.Now()
		records := make([]LocationChRecord, 0, len(batchData))
		candidates := make([]string, 0, len(batchData))

		for _, row := range batchData {
			rec, ok := buildSubVenueLocationRecord(row, currentID, now, venueUUIDLookup)
			if !ok {
				continue
			}
			records = append(records, rec)
			candidates = append(candidates, rec.ID10x)
			currentID++
		}

		newRecords, err := filterNewLocationRecords(clickhouseConn, tableName, "SUB_VENUE", records, candidates)
		if err != nil {
			return inserted, currentID, fmt.Errorf("filter new sub-venue records: %w", err)
		}
		if len(newRecords) > 0 {
			if err := insertLocationSubVenuesIntoTable(clickhouseConn, tableName, newRecords); err != nil {
				return inserted, currentID, err
			}
			inserted += len(newRecords)
		}

		lastRowID := shared.SafeConvertToString(batchData[len(batchData)-1]["id_10x"])
		parsedID, parseErr := strconv.Atoi(lastRowID)
		if parseErr != nil {
			return inserted, currentID, fmt.Errorf("parse sub-venue id_10x %q: %w", lastRowID, parseErr)
		}
		lastID = parsedID

		if len(batchData) < batchSize {
			break
		}
	}

	return inserted, currentID, nil
}

func fetchCountryIncrementalBatch(db *sql.DB, offset, limit int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT
			c.id as id_10x,
			c.name,
			c.shortname as alias,
			c.phonecode,
			c.currency,
			c.continent,
			c.zone as regions,
			c.created,
			c.published,
			c.url as slug
		FROM country c
		WHERE c.created >= CURDATE() - INTERVAL 1 DAY
		ORDER BY c.id
		LIMIT %d OFFSET %d`, limit, offset)
	return executeGenericFetchQuery(db, query)
}

func fetchStateIncrementalBatch(db *sql.DB, offset, limit int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT
			area_values.id,
			area_values.name as id_10x,
			area_values.url as slug,
			area_values.name as name,
			area_values.abbr_name as alias,
			area_values.country as countryId,
			area_values.geometry,
			area_values.published
		FROM area_values
		WHERE type_value != 'subregion'
		  AND area_values.created >= CURDATE() - INTERVAL 1 DAY
		GROUP BY area_values.name, area_values.country
		ORDER BY area_values.id
		LIMIT %d OFFSET %d`, limit, offset)
	return executeGenericFetchQuery(db, query)
}

func fetchCityIncrementalBatch(db *sql.DB, lastID, limit int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT
			city.id as id_10x,
			city.name,
			states.name as state_name,
			states.id as state_id,
			city.country as countryId,
			country.created as country_created,
			city.geo_lat as latitude,
			city.geo_long as longitude,
			city.utc_offset,
			city.timezone,
			city.created,
			city.published,
			city.alias,
			city.url as slug
		FROM city
		LEFT JOIN (
			SELECT id, name, country
			FROM area_values
			GROUP BY name, country
		) as states ON city.state = states.name AND city.country = states.country
		LEFT JOIN country ON city.country = country.id
		WHERE city.id > %d
		  AND city.created >= CURDATE() - INTERVAL 1 DAY
		ORDER BY city.id
		LIMIT %d`, lastID, limit)
	return executeGenericFetchQuery(db, query)
}

func fetchVenueIncrementalBatch(db *sql.DB, lastID, limit int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT
			v.id as id_10x,
			v.name,
			v.address,
			c.id as cityId,
			c.name as cityName,
			c.geo_lat as cityLat,
			c.geo_long as cityLong,
			v.country as countryId,
			v.geo_lat as latitude,
			v.geo_long as longitude,
			v.website,
			v.postal_code as postalcode,
			v.created,
			v.published,
			s.name as stateName,
			s.id as state_id,
			v.url as slug
		FROM venue v
		LEFT JOIN city c ON v.city = c.id
		LEFT JOIN area_values s ON c.state_id = s.id
		WHERE v.id > %d
		  AND v.created >= CURDATE() - INTERVAL 1 DAY
		ORDER BY v.id
		LIMIT %d`, lastID, limit)
	return executeGenericFetchQuery(db, query)
}

func fetchSubVenueIncrementalBatch(db *sql.DB, lastID, limit int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT
			id as id_10x,
			venue as venueId,
			name,
			area,
			type as venueCategoryId,
			created,
			published
		FROM sub_venue
		WHERE id > %d
		  AND created >= CURDATE() - INTERVAL 1 DAY
		  AND published = 1
		ORDER BY id
		LIMIT %d`, lastID, limit)
	return executeGenericFetchQuery(db, query)
}

func executeGenericFetchQuery(db *sql.DB, query string) ([]map[string]interface{}, error) {
	log.Println("Location Incremental MySQL Fetch Query:", query)
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
			row[col] = values[i]
		}
		results = append(results, row)
	}
	return results, nil
}

func filterNewLocationRecords(clickhouseConn driver.Conn, tableName, locationType string, records []LocationChRecord, id10xCandidates []string) ([]LocationChRecord, error) {
	if len(records) == 0 || len(id10xCandidates) == 0 {
		return []LocationChRecord{}, nil
	}

	existing, err := fetchExistingID10x(clickhouseConn, tableName, locationType, id10xCandidates)
	if err != nil {
		return nil, err
	}

	newRecords := make([]LocationChRecord, 0, len(records))
	for _, rec := range records {
		if _, found := existing[rec.ID10x]; found {
			continue
		}
		newRecords = append(newRecords, rec)
	}
	return newRecords, nil
}

func fetchExistingID10x(clickhouseConn driver.Conn, tableName, locationType string, id10xValues []string) (map[string]struct{}, error) {
	result := make(map[string]struct{})
	if len(id10xValues) == 0 {
		return result, nil
	}

	quoted := make([]string, 0, len(id10xValues))
	for _, val := range id10xValues {
		escaped := strings.ReplaceAll(val, "'", "''")
		quoted = append(quoted, fmt.Sprintf("'%s'", escaped))
	}

	query := fmt.Sprintf(
		"SELECT id_10x FROM %s WHERE location_type = '%s' AND id_10x IN (%s)",
		tableName, locationType, strings.Join(quoted, ","),
	)
	log.Printf("Location Incremental CH Query (existing %s keys): %s", locationType, query)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	rows, err := clickhouseConn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var id10x string
		if err := rows.Scan(&id10x); err != nil {
			return nil, err
		}
		result[id10x] = struct{}{}
	}
	return result, rows.Err()
}

func buildCountryLookupFromTable(clickhouseConn driver.Conn, tableName string) (map[string]string, map[string]string, error) {
	isoToUUID := make(map[string]string)
	isoToName := make(map[string]string)

	ctx := context.Background()
	query := fmt.Sprintf("SELECT id_uuid, id_10x, name FROM %s WHERE location_type = 'COUNTRY'", tableName)
	log.Println("Location Incremental CH Query (country lookup):", query)
	rows, err := clickhouseConn.Query(ctx, query)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var idUUID, id10x string
		var name *string
		if err := rows.Scan(&idUUID, &id10x, &name); err != nil {
			return nil, nil, err
		}
		if strings.HasPrefix(id10x, "country-") {
			isoUpper := strings.ToUpper(strings.TrimPrefix(id10x, "country-"))
			isoToUUID[isoUpper] = idUUID
			if name != nil {
				isoToName[isoUpper] = *name
			}
		}
	}
	return isoToUUID, isoToName, rows.Err()
}

func buildCityLookupFromTable(clickhouseConn driver.Conn, tableName string) (map[string]string, error) {
	cityToUUID := make(map[string]string)
	ctx := context.Background()
	query := fmt.Sprintf("SELECT id_uuid, id_10x FROM %s WHERE location_type = 'CITY'", tableName)
	log.Println("Location Incremental CH Query (city lookup):", query)
	rows, err := clickhouseConn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var idUUID, id10x string
		if err := rows.Scan(&idUUID, &id10x); err != nil {
			return nil, err
		}
		if strings.HasPrefix(id10x, "city-") {
			cityToUUID[strings.TrimPrefix(id10x, "city-")] = idUUID
		}
	}
	return cityToUUID, rows.Err()
}

func buildStateLookupFromTable(clickhouseConn driver.Conn, tableName string) (map[string]string, error) {
	stateToUUID := make(map[string]string)
	ctx := context.Background()
	query := fmt.Sprintf("SELECT id_uuid, id_10x FROM %s WHERE location_type = 'STATE'", tableName)
	log.Println("Location Incremental CH Query (state lookup):", query)
	rows, err := clickhouseConn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var idUUID, id10x string
		if err := rows.Scan(&idUUID, &id10x); err != nil {
			return nil, err
		}
		if strings.HasPrefix(id10x, "state-") {
			stateToUUID[strings.TrimPrefix(id10x, "state-")] = idUUID
		}
	}
	return stateToUUID, rows.Err()
}

func buildVenueLookupFromTable(clickhouseConn driver.Conn, tableName string) (map[string]string, error) {
	venueToUUID := make(map[string]string)
	ctx := context.Background()
	query := fmt.Sprintf("SELECT id_uuid, id_10x FROM %s WHERE location_type = 'VENUE'", tableName)
	log.Println("Location Incremental CH Query (venue lookup):", query)
	rows, err := clickhouseConn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var idUUID, id10x string
		if err := rows.Scan(&idUUID, &id10x); err != nil {
			return nil, err
		}
		if strings.HasPrefix(id10x, "venue-") {
			venueToUUID[strings.TrimPrefix(id10x, "venue-")] = idUUID
		}
	}
	return venueToUUID, rows.Err()
}

func insertLocationCountriesIntoTable(clickhouseConn driver.Conn, tableName string, records []LocationChRecord) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	insertQuery := fmt.Sprintf(`
		INSERT INTO %s (
			id_uuid, id, id_10x, location_type, name, alias, slug, phonecode, currency, continent,
			regions, latitude, longitude, published, iso, last_updated_at, version
		)`, tableName)
	log.Printf("Location Incremental CH Insert Query (COUNTRY): %s", insertQuery)
	log.Printf("Location Incremental CH Insert Rows (COUNTRY): %d", len(records))
	batch, err := clickhouseConn.PrepareBatch(ctx, insertQuery)
	if err != nil {
		return err
	}
	for _, r := range records {
		if err := batch.Append(r.IDUUID, r.ID, r.ID10x, r.LocationType, r.Name, r.Alias, r.Slug, r.Phonecode, r.Currency, r.Continent, r.Regions, r.Latitude, r.Longitude, r.Published, r.ISO, r.LastUpdatedAt, r.Version); err != nil {
			return err
		}
	}
	return batch.Send()
}

func insertLocationStatesIntoTable(clickhouseConn driver.Conn, tableName string, records []LocationChRecord) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	insertQuery := fmt.Sprintf(`
		INSERT INTO %s (
			id_uuid, id, id_10x, location_type, name, alias, slug, geometry,
			latitude, longitude, published, iso, country_uuid, country_name,
			last_updated_at, version
		)`, tableName)
	log.Printf("Location Incremental CH Insert Query (STATE): %s", insertQuery)
	log.Printf("Location Incremental CH Insert Rows (STATE): %d", len(records))
	batch, err := clickhouseConn.PrepareBatch(ctx, insertQuery)
	if err != nil {
		return err
	}
	for _, r := range records {
		if err := batch.Append(r.IDUUID, r.ID, r.ID10x, r.LocationType, r.Name, r.Alias, r.Slug, r.Geometry, r.Latitude, r.Longitude, r.Published, r.ISO, r.CountryUUID, r.CountryName, r.LastUpdatedAt, r.Version); err != nil {
			return err
		}
	}
	return batch.Send()
}

func insertLocationCitiesIntoTable(clickhouseConn driver.Conn, tableName string, records []LocationChRecord) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	insertQuery := fmt.Sprintf(`
		INSERT INTO %s (
			id_uuid, id, id_10x, location_type, name, alias, slug, latitude, longitude,
			utc_offset, timezone, published, iso, state_uuid, state_name,
			country_uuid, country_name, last_updated_at, version
		)`, tableName)
	log.Printf("Location Incremental CH Insert Query (CITY): %s", insertQuery)
	log.Printf("Location Incremental CH Insert Rows (CITY): %d", len(records))
	batch, err := clickhouseConn.PrepareBatch(ctx, insertQuery)
	if err != nil {
		return err
	}
	for _, r := range records {
		if err := batch.Append(r.IDUUID, r.ID, r.ID10x, r.LocationType, r.Name, r.Alias, r.Slug, r.Latitude, r.Longitude, r.UTCOffset, r.Timezone, r.Published, r.ISO, r.StateUUID, r.StateName, r.CountryUUID, r.CountryName, r.LastUpdatedAt, r.Version); err != nil {
			return err
		}
	}
	return batch.Send()
}

func insertLocationVenuesIntoTable(clickhouseConn driver.Conn, tableName string, records []LocationChRecord) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	insertQuery := fmt.Sprintf(`
		INSERT INTO %s (
			id_uuid, id, id_10x, location_type, name, address, slug, latitude, longitude,
			website, postalcode, published, iso, state_uuid, state_name,
			country_uuid, country_name, city_id, city_uuid, city_name, city_latitude,
			city_longitude, last_updated_at, version
		)`, tableName)
	log.Printf("Location Incremental CH Insert Query (VENUE): %s", insertQuery)
	log.Printf("Location Incremental CH Insert Rows (VENUE): %d", len(records))
	batch, err := clickhouseConn.PrepareBatch(ctx, insertQuery)
	if err != nil {
		return err
	}
	for _, r := range records {
		if err := batch.Append(r.IDUUID, r.ID, r.ID10x, r.LocationType, r.Name, r.Address, r.Slug, r.Latitude, r.Longitude, r.Website, r.Postalcode, r.Published, r.ISO, r.StateUUID, r.StateName, r.CountryUUID, r.CountryName, r.CityID, r.CityUUID, r.CityName, r.CityLatitude, r.CityLongitude, r.LastUpdatedAt, r.Version); err != nil {
			return err
		}
	}
	return batch.Send()
}

func insertLocationSubVenuesIntoTable(clickhouseConn driver.Conn, tableName string, records []LocationChRecord) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	insertQuery := fmt.Sprintf(`
		INSERT INTO %s (
			id_uuid, id, id_10x, location_type, name, area, published,
			venue_uuid, last_updated_at, version
		)`, tableName)
	log.Printf("Location Incremental CH Insert Query (SUB_VENUE): %s", insertQuery)
	log.Printf("Location Incremental CH Insert Rows (SUB_VENUE): %d", len(records))
	batch, err := clickhouseConn.PrepareBatch(ctx, insertQuery)
	if err != nil {
		return err
	}
	for _, r := range records {
		if err := batch.Append(r.IDUUID, r.ID, r.ID10x, r.LocationType, r.Name, r.Area, r.Published, r.VenueUUID, r.LastUpdatedAt, r.Version); err != nil {
			return err
		}
	}
	return batch.Send()
}
