// CREATE TABLE testing_db.location_ch
// (
//     `id_uuid` UUID, --uuid for incremental ID
//     `id_10x` Nullable(String),
//     `location_id` UInt32,

//     `name` Nullable(String),
//     `alias` Nullable(String),
//     `location_type` LowCardinality(String),  -- COUNTRY, STATE, CITY, VENUE, SUB_VENUE
//     `area` Nullable(Float64),
//     `address` Nullable(String),
//     `postalcode` Nullable(String),
//     `website` Nullable(String),

//     `latitude` Nullable(Float64),
//     `longitude` Nullable(Float64),
//     `coordinate` Nullable(String),
//     `h3_index` Nullable(UInt64) MATERIALIZED if((latitude IS NULL) OR (longitude IS NULL), NULL, geoToH3(latitude, longitude, 9)),

//     `venue_id` Nullable(UInt32),
//     `venue_uuid` Nullable(UUID),
//     `city_id` Nullable(UInt32),
//     `city_uuid` Nullable(UUID),
//     `state_id` Nullable(UInt32),
//     `state_uuid` Nullable(UUID),
//     `country_id` Nullable(UInt32),
//     `country_uuid` Nullable(UUID),

//     `venue_name` Nullable(String),
//     `city_name` Nullable(String),
//     `city_latitude` Nullable(Float64),
//     `city_longitude` Nullable(Float64),
//     `state_name` Nullable(String),
//     `country_name` Nullable(String),
//     `iso` LowCardinality(Nullable(FixedString(2))),

//     `utc_offset` Nullable(String),
//     `timezone` LowCardinality(Nullable(String)),
//     `phonecode` Nullable(String),
//     `currency` LowCardinality(Nullable(String)),
//     `continent` LowCardinality(Nullable(String)),
//     `regions` Array(String),
//     `geometry` Nullable(String),
//     `published` Int8 DEFAULT 0,
//     `created_at` DateTime DEFAULT now(),
//     `updated_at` DateTime DEFAULT now(),
//     `last_updated_at` DateTime DEFAULT now(),
//     `version` UInt32 DEFAULT 1
// )
// ENGINE = MergeTree()
// PARITION BY toYear(created_at)
// ORDER BY (location_type, location_id)
// SETTINGS index_granularity = 8192

package microservice

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"seeders/shared"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"golang.org/x/text/unicode/norm"
)

type LocationChRecord struct {
	IDUUID        string    `ch:"id_uuid"`
	ID10x         string    `ch:"id_10x"`
	LocationType  string    `ch:"location_type"`
	Name          *string   `ch:"name"`
	Alias         *string   `ch:"alias"`
	Phonecode     *string   `ch:"phonecode"`
	Currency      *string   `ch:"currency"`
	Continent     *string   `ch:"continent"`
	Regions       []string  `ch:"regions"`
	Latitude      *float64  `ch:"latitude"`
	Longitude     *float64  `ch:"longitude"`
	Published     int8      `ch:"published"`
	ISO           *string   `ch:"iso"`
	Geometry      *string   `ch:"geometry"`
	CountryUUID   *string   `ch:"country_uuid"`
	CountryName   *string   `ch:"country_name"`
	StateUUID     *string   `ch:"state_uuid"`
	StateName     *string   `ch:"state_name"`
	UTCOffset     *string   `ch:"utc_offset"`
	Timezone      *string   `ch:"timezone"`
	Address       *string   `ch:"address"`
	Postalcode    *string   `ch:"postalcode"`
	Website       *string   `ch:"website"`
	CityUUID      *string   `ch:"city_uuid"`
	CityName      *string   `ch:"city_name"`
	CityLatitude  *float64  `ch:"city_latitude"`
	CityLongitude *float64  `ch:"city_longitude"`
	Area          *float64  `ch:"area"`
	VenueUUID     *string   `ch:"venue_uuid"`
	LastUpdatedAt time.Time `ch:"last_updated_at"`
	Version       uint32    `ch:"version"`
}

// ProcessLocationCountriesCh ports the country logic from Python to ClickHouse
func ProcessLocationCountriesCh(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) {
	log.Println("=== Starting location_ch (countries) Processing ===")

	offset := 0
	batchSize := config.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	for {
		batchData, err := fetchCountryBatch(mysqlDB, offset, batchSize)
		if err != nil {
			log.Fatalf("Failed fetching country batch: %v", err)
		}

		if len(batchData) == 0 {
			break
		}

		records := make([]LocationChRecord, 0, len(batchData))
		now := time.Now()

		for _, row := range batchData {
			iso := shared.SafeConvertToString(row["id"]) // MySQL country.id is ISO
			created := shared.SafeConvertToString(row["created"])

			if iso == "" || created == "" {
				continue
			}

			// Normalize the raw ISO (matching Python: utils.convert_to_utf8)
			normalizedISO := normalizeNFC(strings.TrimSpace(iso))
			isoUpper := strings.ToUpper(normalizedISO)

			// Generate UUID from normalized raw ISO (UPPERCASE) and created (matching Python: "{id_10x.upper()}-{created}")
			// Python uses the raw normalized ISO, NOT the prefixed version
			idInputString := fmt.Sprintf("%s-%s", isoUpper, created)
			idUUID := shared.GenerateUUIDFromString(idInputString)

			// Build id_10x with prefix for insert: 'country-{ISO}'
			id10x := fmt.Sprintf("country-%s", isoUpper)

			name := shared.SafeConvertToNullableString(row["name"])   // normalized/escaped not strictly required for CH
			alias := shared.SafeConvertToNullableString(row["alias"]) // shortname in MySQL
			phonecode := shared.SafeConvertToNullableString(row["phonecode"])
			currency := shared.SafeConvertToNullableString(row["currency"])
			continent := shared.SafeConvertToNullableString(row["continent"])
			published := shared.SafeConvertToInt8(row["published"])

			// regions: keep behavior close to Python (stores array with raw regions value)
			var regions []string
			if r := shared.SafeConvertToString(row["regions"]); r != "" {
				// heuristic: if comma-separated, split; else single element
				if strings.Contains(r, ",") {
					parts := strings.Split(r, ",")
					for i := range parts {
						parts[i] = strings.TrimSpace(parts[i])
					}
					regions = parts
				} else {
					regions = []string{r}
				}
			}

			// latitude/longitude enrichment skipped (no country.json here)
			var latPtr *float64
			var lonPtr *float64

			// iso column stores 2-letter code
			isoPtr := (*string)(nil)
			if isoUpper != "" {
				isoPtr = &isoUpper
			}

			rec := LocationChRecord{
				IDUUID:        idUUID,
				ID10x:         id10x,
				LocationType:  "COUNTRY",
				Name:          name,
				Alias:         alias,
				Phonecode:     phonecode,
				Currency:      currency,
				Continent:     continent,
				Regions:       regions,
				Latitude:      latPtr,
				Longitude:     lonPtr,
				Published:     published,
				ISO:           isoPtr,
				LastUpdatedAt: now,
				Version:       1,
			}

			records = append(records, rec)
		}

		if len(records) > 0 {
			if err := insertLocationCountriesCh(clickhouseConn, records, config.ClickHouseWorkers); err != nil {
				log.Fatalf("Failed inserting countries into location_ch: %v", err)
			}
			log.Printf("Inserted %d country records into location_ch (offset=%d)", len(records), offset)
		}

		offset += len(batchData)
		if len(batchData) < batchSize {
			break
		}
	}

	log.Println("Location countries processing completed!")
}

func fetchCountryBatch(db *sql.DB, offset, limit int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
        SELECT 
            c.id,                    -- ISO code
            c.name,                  -- name
            c.shortname as alias,    -- alias
            c.phonecode,
            c.currency,
            c.continent,
            c.zone as regions,
            c.created,
            c.published
        FROM country c
        ORDER BY c.id
        LIMIT %d OFFSET %d`, limit, offset)

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

func insertLocationCountriesCh(clickhouseConn driver.Conn, records []LocationChRecord, numWorkers int) error {
	if len(records) == 0 {
		return nil
	}

	if numWorkers <= 1 {
		return insertLocationCountriesChSingleWorker(clickhouseConn, records)
	}

	batchSize := (len(records) + numWorkers - 1) / numWorkers
	results := make(chan error, numWorkers)
	semaphore := make(chan struct{}, numWorkers)
	active := 0

	for i := 0; i < numWorkers; i++ {
		start := i * batchSize
		end := start + batchSize
		if end > len(records) {
			end = len(records)
		}
		if start >= len(records) {
			break
		}
		part := records[start:end]
		if len(part) == 0 {
			continue
		}
		active++
		semaphore <- struct{}{}
		go func(batch []LocationChRecord) {
			defer func() { <-semaphore }()
			results <- insertLocationCountriesChSingleWorker(clickhouseConn, batch)
		}(part)
	}

	var lastErr error
	for i := 0; i < active; i++ {
		if err := <-results; err != nil {
			lastErr = err
		}
	}
	if lastErr != nil {
		return lastErr
	}
	return nil
}

func insertLocationCountriesChSingleWorker(clickhouseConn driver.Conn, records []LocationChRecord) error {
	if len(records) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	batch, err := clickhouseConn.PrepareBatch(ctx, `
        INSERT INTO location_ch (
            id_uuid, id_10x, location_type, name, alias, phonecode, currency, continent,
            regions, latitude, longitude, published, iso, last_updated_at, version
        )
    `)
	if err != nil {
		return fmt.Errorf("failed to prepare ClickHouse batch for location_ch (countries): %v", err)
	}

	for _, r := range records {
		if err := batch.Append(
			r.IDUUID,
			r.ID10x,
			r.LocationType,
			r.Name,
			r.Alias,
			r.Phonecode,
			r.Currency,
			r.Continent,
			r.Regions,
			r.Latitude,
			r.Longitude,
			r.Published,
			r.ISO,
			r.LastUpdatedAt,
			r.Version,
		); err != nil {
			return fmt.Errorf("failed to append country record: %v", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send ClickHouse batch for location_ch (countries): %v", err)
	}
	return nil
}

func removeSpecialCharacters(s string) string {
	var result strings.Builder
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsNumber(r) || unicode.IsSpace(r) || r == '-' || r == '_' {
			result.WriteRune(r)
		}
	}
	return result.String()
}

func normalizeNFC(s string) string {
	return norm.NFC.String(strings.TrimSpace(s))
}

func parseGeometryLatLng(geometryStr string) (*float64, *float64) {
	if geometryStr == "" {
		return nil, nil
	}
	var geoMap map[string]interface{}
	if err := json.Unmarshal([]byte(geometryStr), &geoMap); err != nil {
		return nil, nil
	}
	location, ok := geoMap["location"].(map[string]interface{})
	if !ok {
		return nil, nil
	}
	var lat, lng *float64
	if latVal, ok := location["lat"].(float64); ok {
		lat = &latVal
	}
	if lngVal, ok := location["lng"].(float64); ok {
		lng = &lngVal
	}
	return lat, lng
}

func buildCountryLookup(mysqlDB *sql.DB) (map[string]string, map[string]string, error) {
	isoToUUID := make(map[string]string)
	isoToName := make(map[string]string)

	query := `SELECT id, name FROM country ORDER BY id`
	rows, err := mysqlDB.Query(query)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var iso, name string
		if err := rows.Scan(&iso, &name); err != nil {
			continue
		}
		isoUpper := strings.ToUpper(strings.TrimSpace(iso))
		id10x := fmt.Sprintf("country-%s", isoUpper)
		uuid := shared.GenerateUUIDFromString(id10x)
		isoToUUID[isoUpper] = uuid
		isoToName[isoUpper] = name
	}

	return isoToUUID, isoToName, nil
}

// ProcessLocationStatesCh ports the state logic from Python to ClickHouse
func ProcessLocationStatesCh(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) {
	log.Println("=== Starting location_ch (states) Processing ===")

	countryUUIDLookup, countryNameLookup, err := buildCountryLookup(mysqlDB)
	if err != nil {
		log.Fatalf("Failed to build country lookup: %v", err)
	}

	offset := 0
	batchSize := config.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	seenStates := make(map[string]bool)

	for {
		batchData, err := fetchStateBatch(mysqlDB, offset, batchSize)
		if err != nil {
			log.Fatalf("Failed fetching state batch: %v", err)
		}

		if len(batchData) == 0 {
			break
		}

		records := make([]LocationChRecord, 0)
		now := time.Now()

		for _, row := range batchData {
			stateName := shared.SafeConvertToString(row["id_10x"]) // MySQL area_values.name
			countryISO := shared.SafeConvertToString(row["countryId"])

			if stateName == "" || countryISO == "" {
				continue
			}

			countryISOUpper := strings.ToUpper(strings.TrimSpace(countryISO))
			stateNameClean := removeSpecialCharacters(strings.TrimSpace(stateName))

			id10x := fmt.Sprintf("state-%s-%s", stateNameClean, countryISOUpper)

			if seenStates[id10x] {
				continue
			}
			seenStates[id10x] = true

			idInputString := normalizeNFC(fmt.Sprintf("%s-%s", stateNameClean, countryISOUpper))
			idUUID := shared.GenerateUUIDFromString(idInputString)

			name := shared.SafeConvertToNullableString(row["name"])
			alias := shared.SafeConvertToNullableString(row["alias"])
			geometry := shared.SafeConvertToNullableString(row["geometry"])
			published := shared.SafeConvertToInt8(row["published"])

			var latPtr, lonPtr *float64
			if geometry != nil && *geometry != "" {
				latPtr, lonPtr = parseGeometryLatLng(*geometry)
			}

			countryUUID := (*string)(nil)
			countryName := (*string)(nil)
			if uuid, ok := countryUUIDLookup[countryISOUpper]; ok {
				countryUUID = &uuid
			}
			if name, ok := countryNameLookup[countryISOUpper]; ok {
				caser := cases.Title(language.English)
				nameTitle := caser.String(strings.ToLower(name))
				countryName = &nameTitle
			}

			var isoPtr *string
			if countryISOUpper != "NAN" {
				isoPtr = &countryISOUpper
			}

			rec := LocationChRecord{
				IDUUID:        idUUID,
				ID10x:         id10x,
				LocationType:  "STATE",
				Name:          name,
				Alias:         alias,
				Geometry:      geometry,
				Latitude:      latPtr,
				Longitude:     lonPtr,
				Published:     published,
				ISO:           isoPtr,
				CountryUUID:   countryUUID,
				CountryName:   countryName,
				LastUpdatedAt: now,
				Version:       1,
			}

			records = append(records, rec)
		}

		if len(records) > 0 {
			if err := insertLocationStatesCh(clickhouseConn, records, config.ClickHouseWorkers); err != nil {
				log.Fatalf("Failed inserting states into location_ch: %v", err)
			}
			log.Printf("Inserted %d state records into location_ch (offset=%d)", len(records), offset)
		}

		if len(batchData) > 0 {
			if lastID, ok := batchData[len(batchData)-1]["id"].(int64); ok {
				offset = int(lastID)
			} else if lastID, ok := batchData[len(batchData)-1]["id"].(int); ok {
				offset = lastID
			} else {
				offset += len(batchData)
			}
		} else {
			offset += batchSize
		}

		if len(batchData) < batchSize {
			break
		}
	}

	log.Println("Location states processing completed!")
}

func fetchStateBatch(db *sql.DB, offset, limit int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
        SELECT 
            area_values.id,
            area_values.name as id_10x,
            area_values.name as name,
            area_values.abbr_name as alias,
            area_values.country as countryId,
            area_values.geometry,
            area_values.published
        FROM area_values 
        WHERE type_value != 'subregion' AND area_values.id > %d
        GROUP BY area_values.name, area_values.country
        ORDER BY area_values.id
        LIMIT %d`, offset, limit)

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

func insertLocationStatesCh(clickhouseConn driver.Conn, records []LocationChRecord, numWorkers int) error {
	if len(records) == 0 {
		return nil
	}

	if numWorkers <= 1 {
		return insertLocationStatesChSingleWorker(clickhouseConn, records)
	}

	batchSize := (len(records) + numWorkers - 1) / numWorkers
	results := make(chan error, numWorkers)
	semaphore := make(chan struct{}, numWorkers)
	active := 0

	for i := 0; i < numWorkers; i++ {
		start := i * batchSize
		end := start + batchSize
		if end > len(records) {
			end = len(records)
		}
		if start >= len(records) {
			break
		}
		part := records[start:end]
		if len(part) == 0 {
			continue
		}
		active++
		semaphore <- struct{}{}
		go func(batch []LocationChRecord) {
			defer func() { <-semaphore }()
			results <- insertLocationStatesChSingleWorker(clickhouseConn, batch)
		}(part)
	}

	var lastErr error
	for i := 0; i < active; i++ {
		if err := <-results; err != nil {
			lastErr = err
		}
	}
	if lastErr != nil {
		return lastErr
	}
	return nil
}

func insertLocationStatesChSingleWorker(clickhouseConn driver.Conn, records []LocationChRecord) error {
	if len(records) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	batch, err := clickhouseConn.PrepareBatch(ctx, `
        INSERT INTO location_ch (
            id_uuid, id_10x, location_type, name, alias, geometry,
            latitude, longitude, published, iso, country_uuid, country_name,
            last_updated_at, version
        )
    `)
	if err != nil {
		return fmt.Errorf("failed to prepare ClickHouse batch for location_ch (states): %v", err)
	}

	for _, r := range records {
		if err := batch.Append(
			r.IDUUID,
			r.ID10x,
			r.LocationType,
			r.Name,
			r.Alias,
			r.Geometry,
			r.Latitude,
			r.Longitude,
			r.Published,
			r.ISO,
			r.CountryUUID,
			r.CountryName,
			r.LastUpdatedAt,
			r.Version,
		); err != nil {
			return fmt.Errorf("failed to append state record: %v", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send ClickHouse batch for location_ch (states): %v", err)
	}
	return nil
}

// Build state lookup map: stateName+countryISO -> UUID
func buildStateLookup(mysqlDB *sql.DB) (map[string]string, error) {
	stateToUUID := make(map[string]string)

	query := `
		SELECT 
			av.name as state_name,
			av.country as country_iso
		FROM area_values av
		WHERE av.type_value != 'subregion'
		GROUP BY av.name, av.country
	`
	rows, err := mysqlDB.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var stateName, countryISO string
		if err := rows.Scan(&stateName, &countryISO); err != nil {
			continue
		}
		countryISOUpper := strings.ToUpper(strings.TrimSpace(countryISO))
		stateNameClean := removeSpecialCharacters(strings.TrimSpace(stateName))
		key := fmt.Sprintf("%s-%s", stateNameClean, countryISOUpper)
		idInputString := normalizeNFC(fmt.Sprintf("%s-%s", stateNameClean, countryISOUpper))
		uuid := shared.GenerateUUIDFromString(idInputString)
		stateToUUID[key] = uuid
	}

	return stateToUUID, nil
}

// ProcessLocationCitiesCh ports the city logic from Python to ClickHouse
func ProcessLocationCitiesCh(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) {
	log.Println("=== Starting location_ch (cities) Processing ===")

	// Build lookups
	countryUUIDLookup, countryNameLookup, err := buildCountryLookup(mysqlDB)
	if err != nil {
		log.Fatalf("Failed to build country lookup: %v", err)
	}

	stateUUIDLookup, err := buildStateLookup(mysqlDB)
	if err != nil {
		log.Fatalf("Failed to build state lookup: %v", err)
	}

	offset := 0
	batchSize := config.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	for {
		batchData, err := fetchCityBatch(mysqlDB, offset, batchSize)
		if err != nil {
			log.Fatalf("Failed fetching city batch: %v", err)
		}

		if len(batchData) == 0 {
			break
		}

		records := make([]LocationChRecord, 0)
		now := time.Now()

		for _, row := range batchData {
			cityID := shared.SafeConvertToString(row["id_10x"])
			created := shared.SafeConvertToString(row["created"])

			if cityID == "" || created == "" {
				continue
			}

			// Normalize the raw ID (matching Python: utils.convert_to_utf8)
			normalizedID := normalizeNFC(cityID)

			// Generate UUID from normalized raw ID and created date (matching Python: "{id_10x}-{created}")
			// Python uses the raw normalized ID, NOT the prefixed version
			idInputString := fmt.Sprintf("%s-%s", normalizedID, created)
			idUUID := shared.GenerateUUIDFromString(idInputString)

			// Build id_10x with prefix for insert: 'city-{id}'
			id10x := fmt.Sprintf("city-%s", normalizedID)

			name := shared.SafeConvertToNullableString(row["name"])
			alias := shared.SafeConvertToNullableString(row["alias"])
			stateName := shared.SafeConvertToNullableString(row["state_name"])
			countrySourceID := shared.SafeConvertToString(row["countryId"])
			latitude := shared.SafeConvertToNullableFloat64(row["latitude"])
			longitude := shared.SafeConvertToNullableFloat64(row["longitude"])
			utcOffset := shared.SafeConvertToNullableString(row["utc_offset"])
			timezone := shared.SafeConvertToNullableString(row["timezone"])
			published := shared.SafeConvertToInt8(row["published"])

			// Generate state UUID if state_name and country are available
			var stateUUID *string
			if stateName != nil && *stateName != "" && countrySourceID != "" {
				countryISOUpper := strings.ToUpper(strings.TrimSpace(countrySourceID))
				stateNameClean := removeSpecialCharacters(strings.TrimSpace(*stateName))
				key := fmt.Sprintf("%s-%s", stateNameClean, countryISOUpper)
				if uuid, ok := stateUUIDLookup[key]; ok {
					stateUUID = &uuid
				}
			}

			// Get country UUID from lookup (matching country table UUID generation)
			var countryUUID *string
			if countrySourceID != "" {
				countryISOUpper := strings.ToUpper(strings.TrimSpace(countrySourceID))
				if uuid, ok := countryUUIDLookup[countryISOUpper]; ok {
					countryUUID = &uuid
				}
			}

			// Get country name
			var countryName *string
			if countrySourceID != "" {
				countryISOUpper := strings.ToUpper(strings.TrimSpace(countrySourceID))
				if name, ok := countryNameLookup[countryISOUpper]; ok {
					caser := cases.Title(language.English)
					nameTitle := caser.String(strings.ToLower(name))
					countryName = &nameTitle
				}
			}

			// ISO code with NAN check
			var isoPtr *string
			if countrySourceID != "" {
				countryISOUpper := strings.ToUpper(strings.TrimSpace(countrySourceID))
				if countryISOUpper != "NAN" {
					isoPtr = &countryISOUpper
				}
			}

			rec := LocationChRecord{
				IDUUID:        idUUID,
				ID10x:         id10x,
				LocationType:  "CITY",
				Name:          name,
				Alias:         alias,
				Latitude:      latitude,
				Longitude:     longitude,
				UTCOffset:     utcOffset,
				Timezone:      timezone,
				Published:     published,
				ISO:           isoPtr,
				StateUUID:     stateUUID,
				StateName:     stateName,
				CountryUUID:   countryUUID,
				CountryName:   countryName,
				LastUpdatedAt: now,
				Version:       1,
			}

			records = append(records, rec)
		}

		if len(records) > 0 {
			if err := insertLocationCitiesCh(clickhouseConn, records, config.ClickHouseWorkers); err != nil {
				log.Fatalf("Failed inserting cities into location_ch: %v", err)
			}
			log.Printf("Inserted %d city records into location_ch (offset=%d)", len(records), offset)
		}

		// Update offset for next iteration
		if len(batchData) > 0 {
			if lastID, ok := batchData[len(batchData)-1]["id_10x"].(int64); ok {
				offset = int(lastID)
			} else if lastID, ok := batchData[len(batchData)-1]["id_10x"].(int); ok {
				offset = lastID
			} else if lastID, ok := batchData[len(batchData)-1]["id_10x"].(string); ok {
				if id, err := strconv.Atoi(lastID); err == nil {
					offset = id
				} else {
					offset += len(batchData)
				}
			} else {
				offset += len(batchData)
			}
		} else {
			offset += batchSize
		}

		if len(batchData) < batchSize {
			break
		}
	}

	log.Println("Location cities processing completed!")
}

func fetchCityBatch(db *sql.DB, offset, limit int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
        SELECT 
            city.id as id_10x,
            city.name,
            states.name as state_name,
            city.country as countryId,
            country.created as country_created,
            city.geo_lat as latitude,
            city.geo_long as longitude,
            city.utc_offset,
            city.timezone,
            city.created,
            city.published,
            city.alias
        FROM city
        LEFT JOIN (
            SELECT name, country 
            FROM area_values 
            GROUP BY name, country
        ) as states ON city.state = states.name AND city.country = states.country
        LEFT JOIN country ON city.country = country.id
        WHERE city.id > %d
        ORDER BY city.id
        LIMIT %d`, offset, limit)

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

func insertLocationCitiesCh(clickhouseConn driver.Conn, records []LocationChRecord, numWorkers int) error {
	if len(records) == 0 {
		return nil
	}

	if numWorkers <= 1 {
		return insertLocationCitiesChSingleWorker(clickhouseConn, records)
	}

	batchSize := (len(records) + numWorkers - 1) / numWorkers
	results := make(chan error, numWorkers)
	semaphore := make(chan struct{}, numWorkers)
	active := 0

	for i := 0; i < numWorkers; i++ {
		start := i * batchSize
		end := start + batchSize
		if end > len(records) {
			end = len(records)
		}
		if start >= len(records) {
			break
		}
		part := records[start:end]
		if len(part) == 0 {
			continue
		}
		active++
		semaphore <- struct{}{}
		go func(batch []LocationChRecord) {
			defer func() { <-semaphore }()
			results <- insertLocationCitiesChSingleWorker(clickhouseConn, batch)
		}(part)
	}

	var lastErr error
	for i := 0; i < active; i++ {
		if err := <-results; err != nil {
			lastErr = err
		}
	}
	if lastErr != nil {
		return lastErr
	}
	return nil
}

func insertLocationCitiesChSingleWorker(clickhouseConn driver.Conn, records []LocationChRecord) error {
	if len(records) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	batch, err := clickhouseConn.PrepareBatch(ctx, `
        INSERT INTO location_ch (
            id_uuid, id_10x, location_type, name, alias, latitude, longitude,
            utc_offset, timezone, published, iso, state_uuid, state_name,
            country_uuid, country_name, last_updated_at, version
        )
    `)
	if err != nil {
		return fmt.Errorf("failed to prepare ClickHouse batch for location_ch (cities): %v", err)
	}

	for _, r := range records {
		if err := batch.Append(
			r.IDUUID,
			r.ID10x,
			r.LocationType,
			r.Name,
			r.Alias,
			r.Latitude,
			r.Longitude,
			r.UTCOffset,
			r.Timezone,
			r.Published,
			r.ISO,
			r.StateUUID,
			r.StateName,
			r.CountryUUID,
			r.CountryName,
			r.LastUpdatedAt,
			r.Version,
		); err != nil {
			return fmt.Errorf("failed to append city record: %v", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send ClickHouse batch for location_ch (cities): %v", err)
	}
	return nil
}

// Build city lookup map: cityID -> UUID
func buildCityLookup(mysqlDB *sql.DB) (map[string]string, error) {
	cityToUUID := make(map[string]string)

	query := `SELECT id, created FROM city ORDER BY id`
	rows, err := mysqlDB.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var cityID string
		var created string
		if err := rows.Scan(&cityID, &created); err != nil {
			continue
		}
		// Normalize the raw ID (matching Python: utils.convert_to_utf8)
		normalizedID := normalizeNFC(cityID)
		// Generate UUID from normalized raw ID and created (matching Python: "{id_10x}-{created}")
		idInputString := fmt.Sprintf("%s-%s", normalizedID, created)
		uuid := shared.GenerateUUIDFromString(idInputString)
		cityToUUID[cityID] = uuid
	}

	return cityToUUID, nil
}

// Build venue lookup map: venueID -> UUID
func buildVenueLookup(mysqlDB *sql.DB) (map[string]string, error) {
	venueToUUID := make(map[string]string)

	query := `SELECT id, created FROM venue ORDER BY id`
	rows, err := mysqlDB.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var venueID string
		var created string
		if err := rows.Scan(&venueID, &created); err != nil {
			continue
		}
		// Normalize the raw ID (matching Python: utils.convert_to_utf8)
		normalizedID := normalizeNFC(venueID)
		// Generate UUID from normalized raw ID and created (matching Python: "{id_10x}-{created}")
		idInputString := fmt.Sprintf("%s-%s", normalizedID, created)
		uuid := shared.GenerateUUIDFromString(idInputString)
		venueToUUID[venueID] = uuid
	}

	return venueToUUID, nil
}

// ProcessLocationVenuesCh ports the venue logic from Python to ClickHouse
func ProcessLocationVenuesCh(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) {
	log.Println("=== Starting location_ch (venues) Processing ===")

	// Build lookups
	countryUUIDLookup, countryNameLookup, err := buildCountryLookup(mysqlDB)
	if err != nil {
		log.Fatalf("Failed to build country lookup: %v", err)
	}

	stateUUIDLookup, err := buildStateLookup(mysqlDB)
	if err != nil {
		log.Fatalf("Failed to build state lookup: %v", err)
	}

	cityUUIDLookup, err := buildCityLookup(mysqlDB)
	if err != nil {
		log.Fatalf("Failed to build city lookup: %v", err)
	}

	offset := 0
	batchSize := config.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	for {
		batchData, err := fetchVenueBatch(mysqlDB, offset, batchSize)
		if err != nil {
			log.Fatalf("Failed fetching venue batch: %v", err)
		}

		if len(batchData) == 0 {
			break
		}

		records := make([]LocationChRecord, 0)
		now := time.Now()

		for _, row := range batchData {
			venueID := shared.SafeConvertToString(row["id_10x"])
			created := shared.SafeConvertToString(row["created"])

			if venueID == "" || created == "" {
				continue
			}

			// Normalize the raw ID (matching Python: utils.convert_to_utf8)
			normalizedID := normalizeNFC(venueID)

			// Generate UUID from normalized raw ID and created date (matching Python: "{id_10x}-{created}")
			// Python uses the raw normalized ID, NOT the prefixed version
			idInputString := fmt.Sprintf("%s-%s", normalizedID, created)
			idUUID := shared.GenerateUUIDFromString(idInputString)

			// Build id_10x with prefix for insert: 'venue-{id}'
			id10x := fmt.Sprintf("venue-%s", normalizedID)

			name := shared.SafeConvertToNullableString(row["name"])
			address := shared.SafeConvertToNullableString(row["address"])
			website := shared.SafeConvertToNullableString(row["website"])
			postalcode := shared.SafeConvertToNullableString(row["postalcode"])
			latitude := shared.SafeConvertToNullableFloat64(row["latitude"])
			longitude := shared.SafeConvertToNullableFloat64(row["longitude"])
			cityName := shared.SafeConvertToNullableString(row["cityName"])
			stateName := shared.SafeConvertToNullableString(row["stateName"])
			cityIDStr := shared.SafeConvertToString(row["cityId"])
			countrySourceID := shared.SafeConvertToString(row["countryId"])
			cityLat := shared.SafeConvertToNullableFloat64(row["cityLat"])
			cityLong := shared.SafeConvertToNullableFloat64(row["cityLong"])
			published := shared.SafeConvertToInt8(row["published"])

			// Clean address (replace newlines with spaces like Python)
			if address != nil && *address != "" {
				cleanAddr := strings.ReplaceAll(*address, "\n", " ")
				address = &cleanAddr
			}

			// Get city UUID from lookup
			var cityUUID *string
			if cityIDStr != "" {
				if uuid, ok := cityUUIDLookup[cityIDStr]; ok {
					cityUUID = &uuid
				}
			}

			// Generate state UUID if state_name and country are available
			var stateUUID *string
			if stateName != nil && *stateName != "" && countrySourceID != "" {
				countryISOUpper := strings.ToUpper(strings.TrimSpace(countrySourceID))
				stateNameClean := removeSpecialCharacters(strings.TrimSpace(*stateName))
				key := fmt.Sprintf("%s-%s", stateNameClean, countryISOUpper)
				if uuid, ok := stateUUIDLookup[key]; ok {
					stateUUID = &uuid
				}
			}

			// Get country UUID and name
			var countryUUID *string
			var countryName *string
			if countrySourceID != "" {
				countryISOUpper := strings.ToUpper(strings.TrimSpace(countrySourceID))
				if uuid, ok := countryUUIDLookup[countryISOUpper]; ok {
					countryUUID = &uuid
				}
				if name, ok := countryNameLookup[countryISOUpper]; ok {
					caser := cases.Title(language.English)
					nameTitle := caser.String(strings.ToLower(name))
					countryName = &nameTitle
				}
			}

			// ISO code with NAN check
			var isoPtr *string
			if countrySourceID != "" {
				countryISOUpper := strings.ToUpper(strings.TrimSpace(countrySourceID))
				if countryISOUpper != "NAN" {
					isoPtr = &countryISOUpper
				}
			}

			rec := LocationChRecord{
				IDUUID:        idUUID,
				ID10x:         id10x,
				LocationType:  "VENUE",
				Name:          name,
				Address:       address,
				Latitude:      latitude,
				Longitude:     longitude,
				Website:       website,
				Postalcode:    postalcode,
				Published:     published,
				ISO:           isoPtr,
				StateUUID:     stateUUID,
				StateName:     stateName,
				CountryUUID:   countryUUID,
				CountryName:   countryName,
				CityUUID:      cityUUID,
				CityName:      cityName,
				CityLatitude:  cityLat,
				CityLongitude: cityLong,
				LastUpdatedAt: now,
				Version:       1,
			}

			records = append(records, rec)
		}

		if len(records) > 0 {
			if err := insertLocationVenuesCh(clickhouseConn, records, config.ClickHouseWorkers); err != nil {
				log.Fatalf("Failed inserting venues into location_ch: %v", err)
			}
			log.Printf("Inserted %d venue records into location_ch (offset=%d)", len(records), offset)
		}

		// Update offset for next iteration
		if len(batchData) > 0 {
			if lastID, ok := batchData[len(batchData)-1]["id_10x"].(int64); ok {
				offset = int(lastID)
			} else if lastID, ok := batchData[len(batchData)-1]["id_10x"].(int); ok {
				offset = lastID
			} else if lastID, ok := batchData[len(batchData)-1]["id_10x"].(string); ok {
				if id, err := strconv.Atoi(lastID); err == nil {
					offset = id
				} else {
					offset += len(batchData)
				}
			} else {
				offset += len(batchData)
			}
		} else {
			offset += batchSize
		}

		if len(batchData) < batchSize {
			break
		}
	}

	log.Println("Location venues processing completed!")
}

func fetchVenueBatch(db *sql.DB, offset, limit int) ([]map[string]interface{}, error) {
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
            s.name as stateName
        FROM venue v
        LEFT JOIN city c ON v.city = c.id
        LEFT JOIN area_values s ON c.state_id = s.id
        WHERE v.id > %d
        ORDER BY v.id
        LIMIT %d`, offset, limit)

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

func insertLocationVenuesCh(clickhouseConn driver.Conn, records []LocationChRecord, numWorkers int) error {
	if len(records) == 0 {
		return nil
	}

	if numWorkers <= 1 {
		return insertLocationVenuesChSingleWorker(clickhouseConn, records)
	}

	batchSize := (len(records) + numWorkers - 1) / numWorkers
	results := make(chan error, numWorkers)
	semaphore := make(chan struct{}, numWorkers)
	active := 0

	for i := 0; i < numWorkers; i++ {
		start := i * batchSize
		end := start + batchSize
		if end > len(records) {
			end = len(records)
		}
		if start >= len(records) {
			break
		}
		part := records[start:end]
		if len(part) == 0 {
			continue
		}
		active++
		semaphore <- struct{}{}
		go func(batch []LocationChRecord) {
			defer func() { <-semaphore }()
			results <- insertLocationVenuesChSingleWorker(clickhouseConn, batch)
		}(part)
	}

	var lastErr error
	for i := 0; i < active; i++ {
		if err := <-results; err != nil {
			lastErr = err
		}
	}
	if lastErr != nil {
		return lastErr
	}
	return nil
}

func insertLocationVenuesChSingleWorker(clickhouseConn driver.Conn, records []LocationChRecord) error {
	if len(records) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	batch, err := clickhouseConn.PrepareBatch(ctx, `
        INSERT INTO location_ch (
            id_uuid, id_10x, location_type, name, address, latitude, longitude,
            website, postalcode, published, iso, state_uuid, state_name,
            country_uuid, country_name, city_uuid, city_name, city_latitude,
            city_longitude, last_updated_at, version
        )
    `)
	if err != nil {
		return fmt.Errorf("failed to prepare ClickHouse batch for location_ch (venues): %v", err)
	}

	for _, r := range records {
		if err := batch.Append(
			r.IDUUID,
			r.ID10x,
			r.LocationType,
			r.Name,
			r.Address,
			r.Latitude,
			r.Longitude,
			r.Website,
			r.Postalcode,
			r.Published,
			r.ISO,
			r.StateUUID,
			r.StateName,
			r.CountryUUID,
			r.CountryName,
			r.CityUUID,
			r.CityName,
			r.CityLatitude,
			r.CityLongitude,
			r.LastUpdatedAt,
			r.Version,
		); err != nil {
			return fmt.Errorf("failed to append venue record: %v", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send ClickHouse batch for location_ch (venues): %v", err)
	}
	return nil
}

// ProcessLocationSubVenuesCh ports the sub-venue logic from Python to ClickHouse
func ProcessLocationSubVenuesCh(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) {
	log.Println("=== Starting location_ch (sub-venues) Processing ===")

	// Build venue lookup
	venueUUIDLookup, err := buildVenueLookup(mysqlDB)
	if err != nil {
		log.Fatalf("Failed to build venue lookup: %v", err)
	}

	offset := 0
	batchSize := config.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	for {
		batchData, err := fetchSubVenueBatch(mysqlDB, offset, batchSize)
		if err != nil {
			log.Fatalf("Failed fetching sub-venue batch: %v", err)
		}

		if len(batchData) == 0 {
			break
		}

		records := make([]LocationChRecord, 0)
		now := time.Now()

		for _, row := range batchData {
			subVenueID := shared.SafeConvertToString(row["id_10x"])
			created := shared.SafeConvertToString(row["created"])

			if subVenueID == "" || created == "" {
				continue
			}

			// Normalize the raw ID (matching Python: utils.convert_to_utf8)
			normalizedID := normalizeNFC(subVenueID)

			// Generate UUID from normalized raw ID and created date (matching Python: "{id_10x}-{created}")
			// Python uses the raw normalized ID, NOT the prefixed version
			idInputString := fmt.Sprintf("%s-%s", normalizedID, created)
			idUUID := shared.GenerateUUIDFromString(idInputString)

			// Build id_10x with prefix for insert: 'sub_venue-{id}'
			id10x := fmt.Sprintf("sub_venue-%s", normalizedID)

			name := shared.SafeConvertToNullableString(row["name"])
			area := shared.SafeConvertToNullableFloat64(row["area"])
			published := shared.SafeConvertToInt8(row["published"])
			venueIDStr := shared.SafeConvertToString(row["venueId"])

			// Clean name (remove backslashes and quotes like Python)
			// Python: name = utils.convert_to_utf8(data.get('name',None),f'sub_venue-{id_10x}-name').replace("\\","").replace("'","''")
			if name != nil {
				cleanedName := normalizeNFC(*name)
				cleanedName = strings.ReplaceAll(cleanedName, "\\", "")
				cleanedName = strings.ReplaceAll(cleanedName, "'", "''")
				name = &cleanedName
			}

			// Lookup venue UUID
			var venueUUID *string
			if venueIDStr != "" {
				if uuid, found := venueUUIDLookup[venueIDStr]; found {
					venueUUID = &uuid
				}
			}

			record := LocationChRecord{
				IDUUID:        idUUID,
				ID10x:         id10x,
				LocationType:  "SUB_VENUE",
				Name:          name,
				Area:          area,
				Published:     published,
				VenueUUID:     venueUUID,
				LastUpdatedAt: now,
				Version:       1,
			}

			records = append(records, record)
		}

		if len(records) > 0 {
			if err := insertLocationSubVenuesCh(clickhouseConn, records, config.ClickHouseWorkers); err != nil {
				log.Fatalf("Failed inserting sub-venues into location_ch: %v", err)
			}
			log.Printf("Inserted %d sub-venue records into location_ch (offset=%d)", len(records), offset)
		}

		// Update offset for next iteration
		if len(batchData) > 0 {
			if lastID, ok := batchData[len(batchData)-1]["id_10x"].(int64); ok {
				offset = int(lastID)
			} else if lastID, ok := batchData[len(batchData)-1]["id_10x"].(int); ok {
				offset = lastID
			} else if lastID, ok := batchData[len(batchData)-1]["id_10x"].(string); ok {
				if id, err := strconv.Atoi(lastID); err == nil {
					offset = id
				} else {
					offset += len(batchData)
				}
			} else {
				offset += len(batchData)
			}
		} else {
			offset += batchSize
		}

		if len(batchData) < batchSize {
			break
		}
	}

	log.Println("Location sub-venues processing completed!")
}

func fetchSubVenueBatch(db *sql.DB, offset, limit int) ([]map[string]interface{}, error) {
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
        WHERE id > %d AND published = 1
        ORDER BY id
        LIMIT %d`, offset, limit)

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
			continue
		}

		rowMap := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			if val != nil {
				rowMap[col] = val
			}
		}
		results = append(results, rowMap)
	}

	return results, nil
}

func insertLocationSubVenuesCh(clickhouseConn driver.Conn, records []LocationChRecord, numWorkers int) error {
	if len(records) == 0 {
		return nil
	}

	type workItem struct {
		batch []LocationChRecord
		index int
	}

	workChan := make(chan workItem, numWorkers)
	errChan := make(chan error, numWorkers)
	var wg sync.WaitGroup

	// Split records into batches for workers
	batchSize := len(records) / numWorkers
	if batchSize == 0 {
		batchSize = 1
	}

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range workChan {
				if err := insertLocationSubVenuesChSingleWorker(clickhouseConn, item.batch); err != nil {
					errChan <- fmt.Errorf("worker %d failed: %v", item.index, err)
					return
				}
			}
		}()
	}

	// Send work items
	go func() {
		defer close(workChan)
		for i := 0; i < len(records); i += batchSize {
			end := i + batchSize
			if end > len(records) {
				end = len(records)
			}
			workChan <- workItem{
				batch: records[i:end],
				index: i / batchSize,
			}
		}
	}()

	// Wait for completion
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// Collect errors
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		return fmt.Errorf("encountered %d errors during sub-venue insertion: %v", len(errors), errors[0])
	}

	return nil
}

func insertLocationSubVenuesChSingleWorker(clickhouseConn driver.Conn, records []LocationChRecord) error {
	if len(records) == 0 {
		return nil
	}

	ctx := context.Background()
	batch, err := clickhouseConn.PrepareBatch(ctx, `
		INSERT INTO location_ch (
			id_uuid, id_10x, location_type, name, area, published,
			venue_uuid, last_updated_at, version
		) VALUES`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %v", err)
	}

	for _, r := range records {
		if err := batch.Append(
			r.IDUUID,
			r.ID10x,
			r.LocationType,
			r.Name,
			r.Area,
			r.Published,
			r.VenueUUID,
			r.LastUpdatedAt,
			r.Version,
		); err != nil {
			return fmt.Errorf("failed to append sub-venue record: %v", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send ClickHouse batch for location_ch (sub-venues): %v", err)
	}
	return nil
}
