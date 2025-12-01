// holiday_event_types = [
//     {
//         "uuid" : "5b37e581-53f7-5dcf-8177-c6a43774b168",
//         "name" : "Holiday",
//         "slug" : "holiday",
//     },
//     {
//         "uuid" : "5108a4a9-cc29-5d8d-b6b0-fdbcadd3411f",
//         "name" : "Local Holiday",
//         "slug" : "local-holiday",
//     },
//     {
//         "uuid" : "1c90796d-409d-5f43-a816-8bf1e988ffdd",
//         "name" : "National Holiday",
//         "slug" : "national-holiday",
//     },
//     {
//         "uuid" : "209aaa1d-8dfa-5ec7-a0a3-bd112947de26",
//         "name" : "International Holiday",
//         "slug" : "international-holiday",
//     },
//     {
//         "uuid" : "3da888aa-b349-5880-9c5b-3ffa6d253a3f",
//         "name" : "Observance Holiday",
//         "slug" : "observance-holiday",
//     },
//     {
//         "uuid" : "17ab8ffe-d17d-5bd5-a645-08cc16b61c77",
//         "name" : "Cultural Holiday",
//         "slug" : "cultural-holiday",
//     },
//     {
//         "uuid" : "7d05fc2c-2f59-579c-80f2-c92979472eda",
//         "name" : "Religious Holiday",
//         "slug" : "religious-holiday",
//     }
// ]

package microservice

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"seeders/shared"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type HolidayEventTypeRecord struct {
	EventTypeID    uint32   `ch:"eventtype_id"`
	EventTypeUUID  string   `ch:"eventtype_uuid"`
	EventID        uint32   `ch:"event_id"`
	Published      int8     `ch:"published"`
	Name           string   `ch:"name"`
	Slug           string   `ch:"slug"`
	EventAudience  *uint16  `ch:"event_audience"` // Nullable
	EventGroupType string   `ch:"eventGroupType"`
	Groups         []string `ch:"groups"`
	Priority       *int8    `ch:"priority"` // Nullable
	Created        string   `ch:"created"`
	Version        uint32   `ch:"version"`
	LastUpdatedAt  string   `ch:"last_updated_at"`
}

type HolidayEventType struct {
	UUID string
	Name string
	Slug string
}

var holidayEventTypes = []HolidayEventType{
	{"5b37e581-53f7-5dcf-8177-c6a43774b168", "Holiday", "holiday"},
	{"5108a4a9-cc29-5d8d-b6b0-fdbcadd3411f", "Local Holiday", "local-holiday"},
	{"1c90796d-409d-5f43-a816-8bf1e988ffdd", "National Holiday", "national-holiday"},
	{"209aaa1d-8dfa-5ec7-a0a3-bd112947de26", "International Holiday", "international-holiday"},
	{"3da888aa-b349-5880-9c5b-3ffa6d253a3f", "Observance Holiday", "observance-holiday"},
	{"17ab8ffe-d17d-5bd5-a645-08cc16b61c77", "Cultural Holiday", "cultural-holiday"},
	{"7d05fc2c-2f59-579c-80f2-c92979472eda", "Religious Holiday", "religious-holiday"},
}

func getMaxEventTypeID(clickhouseConn driver.Conn) (uint32, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	query := "SELECT MAX(eventtype_id) FROM event_type_ch"
	row := clickhouseConn.QueryRow(ctx, query)

	var maxID uint32
	err := row.Scan(&maxID)
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			log.Println("No existing records in event_type_ch, starting from 0")
			return 0, nil
		}
		return 0, fmt.Errorf("failed to get max eventtype_id: %v", err)
	}

	log.Printf("Max eventtype_id from event_type_ch: %d", maxID)
	return maxID, nil
}

func ProcessHolidayEventTypes(clickhouseConn driver.Conn, config shared.Config) (map[string]uint32, error) {
	log.Println("=== Starting Holiday Event Types Processing ===")

	maxID, err := getMaxEventTypeID(clickhouseConn)
	if err != nil {
		return nil, fmt.Errorf("failed to get max eventtype_id: %v", err)
	}

	startID := maxID + 10 + 1
	log.Printf("Starting eventtype_id generation from: %d (max: %d + buffer: 10)", startID, maxID)

	uuidToEventTypeID := make(map[string]uint32)

	for i, holidayType := range holidayEventTypes {
		eventTypeID := startID + uint32(i)

		uuidToEventTypeID[holidayType.UUID] = eventTypeID
		log.Printf("Mapped event type: eventtype_id=%d, name=%s, slug=%s, uuid=%s",
			eventTypeID, holidayType.Name, holidayType.Slug, holidayType.UUID)
	}

	log.Printf("Built lookup map: %d holiday event types (UUID -> eventtype_id)", len(uuidToEventTypeID))
	log.Println("Note: No seed records inserted. Actual mappings will be created during holiday processing.")
	log.Println("=== Holiday Event Types Processing Completed Successfully ===")
	return uuidToEventTypeID, nil
}

type HolidayCacheEntry struct {
	ClusterName string
	StartDate   string
	EndDate     string
	Types       []string
	Subtypes    []string
	EventID     uint32
	EventUUID   string
}

func processSynonyms(synonyms string) []string {
	if synonyms == "" {
		return []string{}
	}

	parts := strings.Split(synonyms, ",")
	seen := make(map[string]bool)
	result := []string{}

	for _, part := range parts {
		cleaned := strings.TrimSpace(part)
		cleaned = removeSpecialCharacters(cleaned)
		cleaned = strings.ToLower(cleaned)

		if cleaned != "" && !seen[cleaned] {
			seen[cleaned] = true
			result = append(result, cleaned)
		}
	}

	return result
}

func getMaxEventID(clickhouseConn driver.Conn) (uint32, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	query := "SELECT MAX(event_id) FROM allevent_ch"
	row := clickhouseConn.QueryRow(ctx, query)

	var maxID uint32
	err := row.Scan(&maxID)
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			log.Println("No existing records in allevent_ch, starting from 0")
			return 0, nil
		}
		return 0, fmt.Errorf("failed to get max event_id: %v", err)
	}

	log.Printf("Max event_id from allevent_ch: %d", maxID)
	return maxID, nil
}

func fetchHolidays(mysqlDB *sql.DB, limit, offset int, startDate string) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT 
			cluster_name, start_date, end_date,
			GROUP_CONCAT(DISTINCT synonyms SEPARATOR ',') as synonyms,
			GROUP_CONCAT(DISTINCT type_llm SEPARATOR ',') as types,
			GROUP_CONCAT(DISTINCT sub_type_llm SEPARATOR ',') as subtypes
		FROM holiday 
		WHERE 
			cluster_name IS NOT NULL 
			AND start_date >= '%s'
		GROUP BY cluster_name, start_date, end_date 
		ORDER BY cluster_name 
		LIMIT %d OFFSET %d
	`, startDate, limit, offset)

	rows, err := mysqlDB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute holiday query: %v", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get column names: %v", err)
	}

	var results []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
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

func fetchHolidayLocationsBatch(mysqlDB *sql.DB, holidays []map[string]interface{}, numWorkers int) (map[string][]map[string]interface{}, map[string]bool, error) {
	if len(holidays) == 0 {
		return make(map[string][]map[string]interface{}), make(map[string]bool), nil
	}

	allHolidayLocations := make(map[string][]map[string]interface{})
	allLocationSourceIDs := make(map[string]bool)
	var mu sync.Mutex
	var wg sync.WaitGroup

	if numWorkers <= 0 {
		numWorkers = 5
	}
	if numWorkers > len(holidays) {
		numWorkers = len(holidays)
	}

	semaphore := make(chan struct{}, numWorkers)
	errors := make(chan error, len(holidays))

	for _, holiday := range holidays {
		clusterName := shared.SafeConvertToString(holiday["cluster_name"])
		startDateStr := shared.SafeConvertToString(holiday["start_date"])
		endDateStr := shared.SafeConvertToString(holiday["end_date"])

		wg.Add(1)
		semaphore <- struct{}{}

		go func(clusterName, startDateStr, endDateStr string) {
			defer func() {
				<-semaphore
				wg.Done()
			}()

			holidayLocations, err := fetchHolidayLocations(mysqlDB, clusterName, startDateStr, endDateStr)
			if err != nil {
				log.Printf("Warning: Failed to fetch holiday locations for %s (%s-%s): %v", clusterName, startDateStr, endDateStr, err)
				errors <- err
				return
			}

			holidayKey := fmt.Sprintf("%s-%s-%s", clusterName, startDateStr, endDateStr)

			mu.Lock()
			allHolidayLocations[holidayKey] = holidayLocations

			distinctIDs := getDistinctLocationSourceIDs(holidayLocations)
			for _, id := range distinctIDs {
				allLocationSourceIDs[id] = true
			}
			mu.Unlock()
		}(clusterName, startDateStr, endDateStr)
	}

	wg.Wait()
	close(errors)

	for range errors {
	}

	return allHolidayLocations, allLocationSourceIDs, nil
}

type HolidayLocationInfo struct {
	CityID     uint32
	CityName   string
	StateID    *uint32
	StateName  string
	CountryISO string
}

func fetchHolidayLocations(mysqlDB *sql.DB, clusterName, startDate, endDate string) ([]map[string]interface{}, error) {
	query := `
		SELECT hs.id, hs.entity_id, hs.entity_type
		FROM holiday h 
		LEFT JOIN holiday_score hs ON h.id = hs.holiday_id 
		WHERE h.cluster_name = ?
		AND h.start_date = ?
		AND h.end_date = ?
		AND hs.id IS NOT NULL
		ORDER BY hs.id
	`
	rows, err := mysqlDB.Query(query, clusterName, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("failed to execute holiday_score query: %v", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get column names: %v", err)
	}

	var results []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
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

func getDistinctLocationSourceIDs(holidayLocations []map[string]interface{}) []string {
	locationSourceIDs := make(map[string]bool)

	for _, holidayLocation := range holidayLocations {
		entityID := shared.SafeConvertToString(holidayLocation["entity_id"])
		entityType := shared.SafeConvertToString(holidayLocation["entity_type"])

		if entityID != "" && entityType != "" {
			sourceID := fmt.Sprintf("%s-%s", entityType, entityID)
			locationSourceIDs[sourceID] = true
		}
	}

	result := make([]string, 0, len(locationSourceIDs))
	for sourceID := range locationSourceIDs {
		result = append(result, sourceID)
	}

	return result
}

func fetchLocationsFromClickHouse(clickhouseConn driver.Conn, id10xValues []string) (map[string]map[string]interface{}, error) {
	if len(id10xValues) == 0 {
		return make(map[string]map[string]interface{}), nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	quotedValues := make([]string, len(id10xValues))
	for i, val := range id10xValues {
		quotedValues[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(val, "'", "''"))
	}

	query := fmt.Sprintf(`
		SELECT id, name, iso, id_10x
		FROM location_ch
		WHERE id_10x IN (%s)
	`, strings.Join(quotedValues, ","))

	rows, err := clickhouseConn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query location_ch: %v", err)
	}
	defer rows.Close()

	locationMap := make(map[string]map[string]interface{})
	foundIDs := make(map[string]bool)

	for rows.Next() {
		var id uint32
		var name *string
		var iso *string
		var id10x string

		if err := rows.Scan(&id, &name, &iso, &id10x); err != nil {
			log.Printf("Warning: Failed to scan location row: %v", err)
			continue
		}

		locationMap[id10x] = map[string]interface{}{
			"id":   id,
			"name": name,
			"iso":  iso,
		}
		foundIDs[id10x] = true
	}

	missingLocations := make([]string, 0)
	for _, id10x := range id10xValues {
		if !foundIDs[id10x] {
			missingLocations = append(missingLocations, id10x)
		}
	}

	if len(missingLocations) > 0 {
		log.Printf("Warning: %d location(s) not found in location_ch: %v", len(missingLocations), missingLocations)
	}

	return locationMap, nil
}

func mapHolidayLocations(holidayLocations []map[string]interface{}, locationMap map[string]map[string]interface{}) HolidayLocationInfo {
	locationInfo := HolidayLocationInfo{
		CityID:     0,
		CityName:   "",
		StateID:    nil,
		StateName:  "",
		CountryISO: "",
	}

	cityFound := false
	stateFound := false
	countryFound := false

	for _, holidayLocation := range holidayLocations {
		entityID := shared.SafeConvertToString(holidayLocation["entity_id"])
		entityType := shared.SafeConvertToString(holidayLocation["entity_type"])

		if entityID == "" || entityType == "" {
			continue
		}

		sourceID := fmt.Sprintf("%s-%s", entityType, entityID)
		locationData, exists := locationMap[sourceID]
		if !exists {
			continue
		}

		locationID, ok := locationData["id"].(uint32)
		if !ok {
			continue
		}

		locationName, _ := locationData["name"].(*string)
		locationISO, _ := locationData["iso"].(*string)

		switch strings.ToLower(entityType) {
		case "city":
			if !cityFound {
				locationInfo.CityID = locationID
				if locationName != nil {
					locationInfo.CityName = *locationName
				}
				cityFound = true
			}
		case "state":
			if !stateFound {
				stateID := locationID
				locationInfo.StateID = &stateID
				if locationName != nil {
					locationInfo.StateName = *locationName
				}
				stateFound = true
			}
		case "country":
			if !countryFound {
				if locationISO != nil {
					locationInfo.CountryISO = *locationISO
				}
				countryFound = true
			}
		}
	}

	return locationInfo
}

func insertHolidaysIntoAllevent(clickhouseConn driver.Conn, records []alleventRecord, numWorkers int) error {
	if len(records) == 0 {
		return nil
	}

	if numWorkers <= 1 {
		return insertHolidaysIntoAlleventSingleWorker(clickhouseConn, records)
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
			err := insertHolidaysIntoAlleventSingleWorker(clickhouseConn, batch)
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

func insertHolidaysIntoAlleventSingleWorker(clickhouseConn driver.Conn, records []alleventRecord) error {
	if len(records) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 900*time.Second)
	defer cancel()

	insertSQL := `
		INSERT INTO allevent_ch (
			event_id, event_uuid, event_name, event_abbr_name, event_description, event_punchline, event_avgRating,
			start_date, end_date,
			edition_id, edition_country, edition_city, edition_city_name, edition_city_state_id, edition_city_state, edition_city_lat, edition_city_long,
			company_id, company_name, company_domain, company_website, company_country, company_state, company_city, company_city_name,
			venue_id, venue_name, venue_country, venue_city, venue_city_name, venue_lat, venue_long,
			published, status, editions_audiance_type, edition_functionality, edition_website, edition_domain,
			edition_type, event_followers, edition_followers, event_exhibitor, edition_exhibitor,
			exhibitors_upper_bound, exhibitors_lower_bound, exhibitors_mean,
			event_sponsor, edition_sponsor, event_speaker, edition_speaker,
			event_created, edition_created, event_hybrid, isBranded, maturity,
			event_pricing, tickets, event_logo, event_estimatedVisitors, event_frequency, inboundScore, internationalScore, repeatSentimentChangePercentage, audienceZone,
			event_economic_FoodAndBevarage, event_economic_Transportation, event_economic_Accomodation, event_economic_Utilities, event_economic_flights, event_economic_value,
			event_economic_dayWiseEconomicImpact, event_economic_breakdown, event_economic_impact, keywords, PrimaryEventType, version
		)
	`

	batch, err := clickhouseConn.PrepareBatch(ctx, insertSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare ClickHouse batch: %v", err)
	}

	for _, record := range records {
		err := batch.Append(
			record.EventID,                         // event_id: UInt32
			record.EventUUID,                       // event_uuid: UUID
			record.EventName,                       // event_name: String
			record.EventAbbrName,                   // event_abbr_name: Nullable(String)
			record.EventDescription,                // event_description: Nullable(String)
			record.EventPunchline,                  // event_punchline: Nullable(String)
			record.EventAvgRating,                  // event_avgRating: Nullable(Decimal(3,2))
			record.StartDate,                       // start_date: Date
			record.EndDate,                         // end_date: Date
			record.EditionID,                       // edition_id: UInt32
			record.EditionCountry,                  // edition_country: FixedString(2)
			record.EditionCity,                     // edition_city: UInt32
			record.EditionCityName,                 // edition_city_name: String
			record.EditionCityStateID,              // edition_city_state_id: Nullable(UInt32)
			record.EditionCityState,                // edition_city_state: String
			record.EditionCityLat,                  // edition_city_lat: Float64
			record.EditionCityLong,                 // edition_city_long: Float64
			record.CompanyID,                       // company_id: Nullable(UInt32)
			record.CompanyName,                     // company_name: Nullable(String)
			record.CompanyDomain,                   // company_domain: Nullable(String)
			record.CompanyWebsite,                  // company_website: Nullable(String)
			record.CompanyCountry,                  // company_country: Nullable(FixedString(2))
			record.CompanyState,                    // company_state: Nullable(String)
			record.CompanyCity,                     // company_city: Nullable(UInt32)
			record.CompanyCityName,                 // company_city_name: Nullable(String)
			record.VenueID,                         // venue_id: Nullable(UInt32)
			record.VenueName,                       // venue_name: Nullable(String)
			record.VenueCountry,                    // venue_country: Nullable(FixedString(2))
			record.VenueCity,                       // venue_city: Nullable(UInt32)
			record.VenueCityName,                   // venue_city_name: Nullable(String)
			record.VenueLat,                        // venue_lat: Nullable(Float64)
			record.VenueLong,                       // venue_long: Nullable(Float64)
			record.Published,                       // published: Int8
			record.Status,                          // status: FixedString(1)
			record.EditionsAudianceType,            // editions_audiance_type: UInt16
			record.EditionFunctionality,            // edition_functionality: String
			record.EditionWebsite,                  // edition_website: Nullable(String)
			record.EditionDomain,                   // edition_domain: Nullable(String)
			record.EditionType,                     // edition_type: String
			record.EventFollowers,                  // event_followers: Nullable(UInt32)
			record.EditionFollowers,                // edition_followers: Nullable(UInt32)
			record.EventExhibitor,                  // event_exhibitor: Nullable(UInt32)
			record.EditionExhibitor,                // edition_exhibitor: Nullable(UInt32)
			record.ExhibitorsUpperBound,            // exhibitors_upper_bound: Nullable(UInt32)
			record.ExhibitorsLowerBound,            // exhibitors_lower_bound: Nullable(UInt32)
			record.ExhibitorsMean,                  // exhibitors_mean: Nullable(UInt32)
			record.EventSponsor,                    // event_sponsor: Nullable(UInt32)
			record.EditionSponsor,                  // edition_sponsor: Nullable(UInt32)
			record.EventSpeaker,                    // event_speaker: Nullable(UInt32)
			record.EditionSpeaker,                  // edition_speaker: Nullable(UInt32)
			record.EventCreated,                    // event_created: DateTime
			record.EditionCreated,                  // edition_created: DateTime
			record.EventHybrid,                     // event_hybrid: Nullable(UInt8)
			record.IsBranded,                       // isBranded: Nullable(UInt32)
			record.Maturity,                        // maturity: Nullable(String)
			record.EventPricing,                    // event_pricing: Nullable(String)
			record.Tickets,                         // tickets: Array(String)
			record.EventLogo,                       // event_logo: Nullable(String)
			record.EventEstimatedVisitors,          // event_estimatedVisitors: Nullable(String)
			record.EventFrequency,                  // event_frequency: Nullable(String)
			record.InboundScore,                    // inboundScore: Nullable(UInt32)
			record.InternationalScore,              // internationalScore: Nullable(UInt32)
			record.RepeatSentimentChangePercentage, // repeatSentimentChangePercentage: Nullable(Float64)
			record.AudienceZone,                    // audienceZone: Nullable(String)
			record.EventEconomicFoodAndBevarage,    // event_economic_FoodAndBevarage: Nullable(Float64)
			record.EventEconomicTransportation,     // event_economic_Transportation: Nullable(Float64)
			record.EventEconomicAccomodation,       // event_economic_Accomodation: Nullable(Float64)
			record.EventEconomicUtilities,          // event_economic_Utilities: Nullable(Float64)
			record.EventEconomicFlights,            // event_economic_flights: Nullable(Float64)
			record.EventEconomicValue,              // event_economic_value: Nullable(Float64)
			record.EventEconomicDayWiseImpact,      // event_economic_dayWiseEconomicImpact: JSON
			record.EventEconomicBreakdown,          // event_economic_breakdown: JSON
			record.EventEconomicImpact,             // event_economic_impact: JSON
			record.Keywords,                        // keywords: Array(String)
			record.PrimaryEventType,                // PrimaryEventType: Nullable(UUID)
			record.Version,                         // version: UInt32
		)
		if err != nil {
			return fmt.Errorf("failed to append record to batch: %v", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send ClickHouse batch: %v", err)
	}

	log.Printf("OK: Successfully inserted %d holiday records into allevent_ch", len(records))
	return nil
}

func checkHolidayEventTypesExist(clickhouseConn driver.Conn) (bool, error) {
	baseHolidayUUID := "5b37e581-53f7-5dcf-8177-c6a43774b168"
	_, err := getEventTypeIDByUUID(clickhouseConn, baseHolidayUUID)
	if err != nil {
		return false, nil
	}
	return true, nil
}

func ProcessHolidays(mysqlDB *sql.DB, clickhouseConn driver.Conn, config shared.Config) error {
	log.Println("=== Starting Holiday Processing ===")

	var eventTypeLookup map[string]uint32
	eventTypesExist, err := checkHolidayEventTypesExist(clickhouseConn)
	if err != nil {
		return fmt.Errorf("failed to check holiday event types: %v", err)
	}

	if !eventTypesExist {
		log.Println("Holiday event types not found in event_type_ch. Inserting them first...")
		eventTypeLookup, err = ProcessHolidayEventTypes(clickhouseConn, config)
		if err != nil {
			return fmt.Errorf("failed to insert holiday event types: %v", err)
		}
		log.Println("Holiday event types inserted successfully. Continuing with holiday processing...")
	} else {
		log.Println("Holiday event types already exist in event_type_ch. Building lookup map from database...")
		eventTypeLookup, err = buildEventTypeUUIDLookup(clickhouseConn)
		if err != nil {
			return fmt.Errorf("failed to build event type lookup: %v", err)
		}
	}

	maxEventID, err := getMaxEventID(clickhouseConn)
	if err != nil {
		return fmt.Errorf("failed to get max event_id: %v", err)
	}

	startEventID := maxEventID + 10 + 1
	log.Printf("Starting event_id generation from: %d (max: %d + buffer: 10)", startEventID, maxEventID)

	today := time.Now()
	startDate := today.AddDate(0, 0, -366).Format("2006-01-02")
	log.Printf("Fetching holidays with start_date >= %s", startDate)

	currentTime := time.Now()
	createdStr := currentTime.Format("2006-01-02 15:04:05")

	offset := 0
	fetchLimit := config.BatchSize
	if fetchLimit <= 0 {
		fetchLimit = 5000
	}

	insertLimit := config.BatchSize
	if insertLimit <= 0 {
		insertLimit = 1000
	}

	currentEventID := startEventID
	totalHolidaysProcessed := 0

	for {
		holidays, err := fetchHolidays(mysqlDB, fetchLimit, offset, startDate)
		if err != nil {
			return fmt.Errorf("failed to fetch holidays: %v", err)
		}

		if len(holidays) == 0 {
			break
		}

		log.Printf("Fetched %d holidays from MySQL (offset: %d)", len(holidays), offset)

		for chunk := 0; chunk < len(holidays); chunk += insertLimit {
			chunkEnd := chunk + insertLimit
			if chunkEnd > len(holidays) {
				chunkEnd = len(holidays)
			}

			chunkedHolidays := holidays[chunk:chunkEnd]
			log.Printf("Processing chunk: %d holidays (indices %d-%d)", len(chunkedHolidays), chunk, chunkEnd-1)

			allHolidayLocations, allLocationSourceIDs, err := fetchHolidayLocationsBatch(mysqlDB, chunkedHolidays, config.NumWorkers)
			if err != nil {
				return fmt.Errorf("failed to fetch holiday locations: %v", err)
			}

			locationSourceIDsList := make([]string, 0, len(allLocationSourceIDs))
			for id := range allLocationSourceIDs {
				locationSourceIDsList = append(locationSourceIDsList, id)
			}

			locationMap := make(map[string]map[string]interface{})
			if len(locationSourceIDsList) > 0 {
				log.Printf("Fetching %d distinct locations from location_ch...", len(locationSourceIDsList))
				locationMap, err = fetchLocationsFromClickHouse(clickhouseConn, locationSourceIDsList)
				if err != nil {
					log.Printf("Warning: Failed to fetch locations from ClickHouse: %v", err)
				} else {
					log.Printf("Fetched %d locations from location_ch", len(locationMap))
				}
			}

			chunkCache := make(map[string]HolidayCacheEntry)
			var alleventRecords []alleventRecord

			for _, holiday := range chunkedHolidays {
				clusterName := shared.SafeConvertToString(holiday["cluster_name"])
				startDateStr := shared.SafeConvertToString(holiday["start_date"])
				endDateStr := shared.SafeConvertToString(holiday["end_date"])
				synonyms := shared.SafeConvertToString(holiday["synonyms"])
				typesStr := shared.SafeConvertToString(holiday["types"])
				subtypesStr := shared.SafeConvertToString(holiday["subtypes"])

				clusterNameClean := removeSpecialCharacters(clusterName)

				idInput := fmt.Sprintf("%s-%s-%s", clusterNameClean, startDateStr, endDateStr)
				eventUUID := shared.GenerateUUIDFromString(idInput)

				keywords := processSynonyms(synonyms)

				var types []string
				if typesStr != "" {
					parts := strings.Split(typesStr, ",")
					for _, part := range parts {
						cleaned := strings.TrimSpace(part)
						if cleaned != "" {
							types = append(types, cleaned)
						}
					}
				}

				var subtypes []string
				if subtypesStr != "" {
					parts := strings.Split(subtypesStr, ",")
					for _, part := range parts {
						cleaned := strings.TrimSpace(part)
						if cleaned != "" {
							subtypes = append(subtypes, cleaned)
						}
					}
				}

				holidayKey := fmt.Sprintf("%s-%s-%s", clusterName, startDateStr, endDateStr)
				holidayLocations := allHolidayLocations[holidayKey]
				locationInfo := mapHolidayLocations(holidayLocations, locationMap)

				chunkCache[eventUUID] = HolidayCacheEntry{
					ClusterName: clusterNameClean,
					StartDate:   startDateStr,
					EndDate:     endDateStr,
					Types:       types,
					Subtypes:    subtypes,
					EventID:     currentEventID,
					EventUUID:   eventUUID,
				}

				primaryEventTypeUUID := "5b37e581-53f7-5dcf-8177-c6a43774b168"
				record := alleventRecord{
					EventID:                         currentEventID,
					EventUUID:                       eventUUID,
					EventName:                       clusterNameClean,
					EventAbbrName:                   nil,
					EventDescription:                nil,
					EventPunchline:                  nil,
					EventAvgRating:                  nil,
					StartDate:                       startDateStr,
					EndDate:                         endDateStr,
					EditionID:                       0,
					EditionCountry:                  locationInfo.CountryISO,
					EditionCity:                     locationInfo.CityID,
					EditionCityName:                 locationInfo.CityName,
					EditionCityStateID:              locationInfo.StateID,
					EditionCityState:                locationInfo.StateName,
					EditionCityLat:                  0.0,
					EditionCityLong:                 0.0,
					CompanyID:                       nil,
					CompanyName:                     nil,
					CompanyDomain:                   nil,
					CompanyWebsite:                  nil,
					CompanyCountry:                  nil,
					CompanyState:                    nil,
					CompanyCity:                     nil,
					CompanyCityName:                 nil,
					VenueID:                         nil,
					VenueName:                       nil,
					VenueCountry:                    nil,
					VenueCity:                       nil,
					VenueCityName:                   nil,
					VenueLat:                        nil,
					VenueLong:                       nil,
					Published:                       4,
					Status:                          "A",
					EditionsAudianceType:            0,
					EditionFunctionality:            "",
					EditionWebsite:                  nil,
					EditionDomain:                   nil,
					EditionType:                     "current_edition",
					EventFollowers:                  nil,
					EditionFollowers:                nil,
					EventExhibitor:                  nil,
					EditionExhibitor:                nil,
					ExhibitorsUpperBound:            nil,
					ExhibitorsLowerBound:            nil,
					ExhibitorsMean:                  nil,
					EventSponsor:                    nil,
					EditionSponsor:                  nil,
					EventSpeaker:                    nil,
					EditionSpeaker:                  nil,
					EventCreated:                    createdStr,
					EditionCreated:                  createdStr,
					EventHybrid:                     nil,
					IsBranded:                       nil,
					Maturity:                        nil,
					EventPricing:                    nil,
					Tickets:                         []string{},
					EventLogo:                       nil,
					EventEstimatedVisitors:          nil,
					EventFrequency:                  nil,
					InboundScore:                    nil,
					InternationalScore:              nil,
					RepeatSentimentChangePercentage: nil,
					AudienceZone:                    nil,
					EventEconomicFoodAndBevarage:    nil,
					EventEconomicTransportation:     nil,
					EventEconomicAccomodation:       nil,
					EventEconomicUtilities:          nil,
					EventEconomicFlights:            nil,
					EventEconomicValue:              nil,
					EventEconomicDayWiseImpact:      "{}",
					EventEconomicBreakdown:          "{}",
					EventEconomicImpact:             "{}",
					Keywords:                        keywords,
					PrimaryEventType:                &primaryEventTypeUUID,
					Version:                         1,
				}

				alleventRecords = append(alleventRecords, record)
				currentEventID++
			}

			if len(alleventRecords) > 0 {
				log.Printf("Inserting %d holiday records into allevent_ch...", len(alleventRecords))
				insertErr := shared.RetryWithBackoff(
					func() error {
						return insertHolidaysIntoAllevent(clickhouseConn, alleventRecords, config.ClickHouseWorkers)
					},
					3,
					"holiday insertion",
				)

				if insertErr != nil {
					return fmt.Errorf("failed to insert holidays after retries: %v", insertErr)
				}

				if len(chunkCache) > 0 {
					log.Printf("Processing event type mappings for chunk (%d holidays)...", len(chunkCache))
					mappingErr := ProcessHolidayEventTypeMappings(clickhouseConn, chunkCache, eventTypeLookup, config)
					if mappingErr != nil {
						return fmt.Errorf("failed to process holiday event type mappings for chunk: %v", mappingErr)
					}
					totalHolidaysProcessed += len(chunkCache)
					chunkCache = nil
				}
			}
		}

		offset += len(holidays)
		if len(holidays) < fetchLimit {
			break
		}
	}

	log.Printf("Total holidays processed: %d", totalHolidaysProcessed)
	log.Println("=== Holiday Processing Completed Successfully ===")

	return nil
}

var holidayTypesMapping = map[string]string{
	"local":         "local-holiday",
	"national":      "national-holiday",
	"international": "international-holiday",
	"observance":    "observance-holiday",
	"religious":     "religious-holiday",
	"cultural":      "cultural-holiday",
}

func getEventTypeIDByUUID(clickhouseConn driver.Conn, eventTypeUUID string) (uint32, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	query := fmt.Sprintf("SELECT eventtype_id FROM event_type_ch WHERE eventtype_uuid = '%s' LIMIT 1", eventTypeUUID)
	row := clickhouseConn.QueryRow(ctx, query)

	var eventTypeID uint32
	err := row.Scan(&eventTypeID)
	if err != nil {
		return 0, fmt.Errorf("failed to get eventtype_id for UUID %s: %v", eventTypeUUID, err)
	}

	return eventTypeID, nil
}

func buildEventTypeUUIDLookup(clickhouseConn driver.Conn) (map[string]uint32, error) {
	lookup := make(map[string]uint32)

	for _, holidayType := range holidayEventTypes {
		eventTypeID, err := getEventTypeIDByUUID(clickhouseConn, holidayType.UUID)
		if err != nil {
			log.Printf("Warning: Could not find eventtype_id for UUID %s (%s): %v", holidayType.UUID, holidayType.Name, err)
			continue
		}
		lookup[holidayType.UUID] = eventTypeID
		log.Printf("Mapped event type: %s (UUID: %s) -> eventtype_id: %d", holidayType.Name, holidayType.UUID, eventTypeID)
	}

	return lookup, nil
}

func insertHolidayEventTypeMappings(clickhouseConn driver.Conn, records []HolidayEventTypeRecord, numWorkers int) error {
	if len(records) == 0 {
		return nil
	}

	if numWorkers <= 1 {
		return insertHolidayEventTypeMappingsSingleWorker(clickhouseConn, records)
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
			err := insertHolidayEventTypeMappingsSingleWorker(clickhouseConn, batch)
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

func insertHolidayEventTypeMappingsSingleWorker(clickhouseConn driver.Conn, records []HolidayEventTypeRecord) error {
	if len(records) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	batch, err := clickhouseConn.PrepareBatch(ctx, `
		INSERT INTO event_type_ch (
			eventtype_id, eventtype_uuid, event_id, published, name, slug, event_audience, eventGroupType, groups, priority, created, version, last_updated_at
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare ClickHouse batch: %v", err)
	}

	for _, record := range records {
		err := batch.Append(
			record.EventTypeID,    // eventtype_id: UInt32
			record.EventTypeUUID,  // eventtype_uuid: UUID
			record.EventID,        // event_id: UInt32 (holiday event ID)
			record.Published,      // published: Int8
			record.Name,           // name: LowCardinality(String)
			record.Slug,           // slug: String
			record.EventAudience,  // event_audience: Nullable(UInt16)
			record.EventGroupType, // eventGroupType: LowCardinality(String)
			record.Groups,         // groups: Array(String)
			record.Priority,       // priority: Nullable(Int8)
			record.Created,        // created: DateTime
			record.Version,        // version: UInt32 DEFAULT 1
			record.LastUpdatedAt,  // last_updated_at: DateTime
		)
		if err != nil {
			return fmt.Errorf("failed to append record to batch: %v", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send ClickHouse batch: %v", err)
	}

	log.Printf("OK: Successfully inserted %d holiday event type mapping records", len(records))
	return nil
}

func ProcessHolidayEventTypeMappings(clickhouseConn driver.Conn, holidayCache map[string]HolidayCacheEntry, eventTypeLookup map[string]uint32, config shared.Config) error {
	log.Println("=== Starting Holiday Event Type Mapping Processing ===")

	if len(holidayCache) == 0 {
		log.Println("No holidays in cache to map event types")
		return nil
	}

	if len(eventTypeLookup) == 0 {
		return fmt.Errorf("event type lookup map is empty")
	}

	var baseHolidayUUID string
	var baseHolidayEventTypeID uint32
	for _, ht := range holidayEventTypes {
		if ht.Slug == "holiday" {
			baseHolidayUUID = ht.UUID
			var exists bool
			baseHolidayEventTypeID, exists = eventTypeLookup[baseHolidayUUID]
			if !exists {
				return fmt.Errorf("base holiday event type not found in lookup map (UUID: %s)", baseHolidayUUID)
			}
			break
		}
	}
	if baseHolidayEventTypeID == 0 {
		return fmt.Errorf("base holiday event type not found")
	}

	log.Printf("Base holiday event type: UUID=%s, eventtype_id=%d", baseHolidayUUID, baseHolidayEventTypeID)

	currentTime := time.Now()
	createdStr := currentTime.Format("2006-01-02 15:04:05")
	lastUpdatedAt := currentTime.Format("2006-01-02 15:04:05")

	var mappingRecords []HolidayEventTypeRecord
	mappingCount := 0

	for _, holidayEntry := range holidayCache {
		baseRecord := HolidayEventTypeRecord{
			EventTypeID:    baseHolidayEventTypeID,
			EventTypeUUID:  baseHolidayUUID,
			EventID:        holidayEntry.EventID,
			Published:      4,
			Name:           "Holiday",
			Slug:           "holiday",
			EventAudience:  nil,
			EventGroupType: "NON_ATTENDED",
			Groups:         []string{"unattended"},
			Priority:       nil,
			Created:        createdStr,
			Version:        1,
			LastUpdatedAt:  lastUpdatedAt,
		}
		mappingRecords = append(mappingRecords, baseRecord)
		mappingCount++

		for _, typeName := range holidayEntry.Types {
			typeNameLower := strings.ToLower(strings.TrimSpace(typeName))
			if mappedSlug, ok := holidayTypesMapping[typeNameLower]; ok {
				var foundType *HolidayEventType
				for i := range holidayEventTypes {
					if holidayEventTypes[i].Slug == mappedSlug {
						foundType = &holidayEventTypes[i]
						break
					}
				}

				if foundType == nil {
					log.Printf("Warning: Could not find event type for mapped slug: %s (from type: %s)", mappedSlug, typeName)
					continue
				}

				eventTypeID, exists := eventTypeLookup[foundType.UUID]
				if !exists {
					log.Printf("Warning: Could not find eventtype_id for mapped type %s (slug: %s, UUID: %s) in lookup map", typeName, mappedSlug, foundType.UUID)
					continue
				}

				mappingRecord := HolidayEventTypeRecord{
					EventTypeID:    eventTypeID,
					EventTypeUUID:  foundType.UUID,
					EventID:        holidayEntry.EventID,
					Published:      4,
					Name:           foundType.Name,
					Slug:           foundType.Slug,
					EventAudience:  nil,
					EventGroupType: "NON_ATTENDED",
					Groups:         []string{"unattended"},
					Priority:       nil,
					Created:        createdStr,
					Version:        1,
					LastUpdatedAt:  lastUpdatedAt,
				}
				mappingRecords = append(mappingRecords, mappingRecord)
				mappingCount++
			}
		}

		for _, subtypeName := range holidayEntry.Subtypes {
			subtypeNameLower := strings.ToLower(strings.TrimSpace(subtypeName))
			if mappedSlug, ok := holidayTypesMapping[subtypeNameLower]; ok {
				var foundType *HolidayEventType
				for i := range holidayEventTypes {
					if holidayEventTypes[i].Slug == mappedSlug {
						foundType = &holidayEventTypes[i]
						break
					}
				}

				if foundType == nil {
					log.Printf("Warning: Could not find event type for mapped slug: %s (from subtype: %s)", mappedSlug, subtypeName)
					continue
				}

				eventTypeID, exists := eventTypeLookup[foundType.UUID]
				if !exists {
					log.Printf("Warning: Could not find eventtype_id for mapped subtype %s (slug: %s, UUID: %s) in lookup map", subtypeName, mappedSlug, foundType.UUID)
					continue
				}

				mappingRecord := HolidayEventTypeRecord{
					EventTypeID:    eventTypeID,
					EventTypeUUID:  foundType.UUID,
					EventID:        holidayEntry.EventID,
					Published:      4,
					Name:           foundType.Name,
					Slug:           foundType.Slug,
					EventAudience:  nil,
					EventGroupType: "NON_ATTENDED",
					Groups:         []string{"unattended"},
					Priority:       nil,
					Created:        createdStr,
					Version:        1,
					LastUpdatedAt:  lastUpdatedAt,
				}
				mappingRecords = append(mappingRecords, mappingRecord)
				mappingCount++
			}
		}
	}

	if len(mappingRecords) == 0 {
		log.Println("No event type mappings to insert")
		return nil
	}

	log.Printf("Prepared %d event type mapping records for %d holidays", mappingCount, len(holidayCache))

	batchSize := config.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	for i := 0; i < len(mappingRecords); i += batchSize {
		end := i + batchSize
		if end > len(mappingRecords) {
			end = len(mappingRecords)
		}

		batch := mappingRecords[i:end]
		log.Printf("Inserting batch of %d event type mappings (indices %d-%d)...", len(batch), i, end-1)

		insertErr := shared.RetryWithBackoff(
			func() error {
				return insertHolidayEventTypeMappings(clickhouseConn, batch, config.ClickHouseWorkers)
			},
			3,
			"holiday event type mapping insertion",
		)

		if insertErr != nil {
			return fmt.Errorf("failed to insert holiday event type mappings after retries: %v", insertErr)
		}
	}

	log.Printf("Successfully inserted %d event type mapping records", len(mappingRecords))
	log.Println("=== Holiday Event Type Mapping Processing Completed Successfully ===")
	return nil
}
