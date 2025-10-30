package microservice

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"seeders/shared"
	"strconv"
	"strings"
	"sync"
	"time"

	"seeders/utils"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/elastic/go-elasticsearch/v6"
)

// converts a map to allEventRecord struct
func convertToallEventRecord(record map[string]interface{}) allEventRecord {
	return allEventRecord{
		EventID:            shared.SafeConvertToUInt32(record["event_id"]),
		EventUUID:          shared.SafeConvertToString(record["event_uuid"]),
		EventName:          shared.SafeConvertToString(record["event_name"]),
		EventAbbrName:      shared.SafeConvertToNullableString(record["event_abbr_name"]),
		EventDescription:   shared.SafeConvertToNullableString(record["event_description"]),
		EventPunchline:     shared.SafeConvertToNullableString(record["event_punchline"]),
		StartDate:          shared.SafeConvertToDateString(record["start_date"]),
		EndDate:            shared.SafeConvertToDateString(record["end_date"]),
		EditionID:          shared.SafeConvertToUInt32(record["edition_id"]),
		EditionCountry:     strings.ToUpper(shared.SafeConvertToString(record["edition_country"])),
		EditionCity:        shared.SafeConvertToUInt32(record["edition_city"]),
		EditionCityName:    shared.SafeConvertToString(record["edition_city_name"]),
		EditionCityStateID: shared.SafeConvertToNullableUInt32(record["edition_city_state_id"]),
		EditionCityState:   shared.SafeConvertToString(record["edition_city_state"]),
		EditionCityLat:     shared.SafeConvertToFloat64(record["edition_city_lat"]),
		EditionCityLong:    shared.SafeConvertToFloat64(record["edition_city_long"]),
		CompanyID:          shared.SafeConvertToNullableUInt32(record["company_id"]),
		CompanyName:        shared.SafeConvertToNullableString(record["company_name"]),
		CompanyDomain:      shared.SafeConvertToNullableString(record["company_domain"]),
		CompanyWebsite:     shared.SafeConvertToNullableString(record["company_website"]),
		CompanyCountry:     shared.ToUpperNullableString(shared.SafeConvertToNullableString(record["company_country"])),
		CompanyState:       shared.SafeConvertToNullableString(record["company_state"]),
		CompanyCity:        shared.SafeConvertToNullableUInt32(record["company_city"]),
		CompanyCityName: func() *string {
			if val, ok := record["company_city_name"].(*string); ok {
				return val
			}
			return nil
		}(),
		VenueID:      shared.SafeConvertToNullableUInt32(record["venue_id"]),
		VenueName:    shared.SafeConvertToNullableString(record["venue_name"]),
		VenueCountry: shared.ToUpperNullableString(shared.SafeConvertToNullableString(record["venue_country"])),
		VenueCity:    shared.SafeConvertToNullableUInt32(record["venue_city"]),
		VenueCityName: func() *string {
			if val, ok := record["venue_city_name"].(*string); ok {
				return val
			}
			return nil
		}(),
		VenueLat:             shared.SafeConvertToNullableFloat64(record["venue_lat"]),
		VenueLong:            shared.SafeConvertToNullableFloat64(record["venue_long"]),
		Published:            shared.SafeConvertToInt8(record["published"]),
		Status:               shared.SafeConvertToStatusString(record["status"]),
		EditionsAudianceType: shared.SafeConvertToUInt16(record["editions_audiance_type"]),
		EditionFunctionality: shared.SafeConvertToString(record["edition_functionality"]),
		EditionWebsite:       shared.SafeConvertToNullableString(record["edition_website"]),
		EditionDomain:        shared.SafeConvertToNullableString(record["edition_domain"]),
		EditionType:          *shared.SafeConvertToNullableString(record["edition_type"]),
		EventFollowers:       shared.SafeConvertToNullableUInt32(record["event_followers"]),
		EditionFollowers:     shared.SafeConvertToNullableUInt32(record["edition_followers"]),
		EventExhibitor:       shared.SafeConvertToNullableUInt32(record["event_exhibitor"]),
		EditionExhibitor:     shared.SafeConvertToNullableUInt32(record["edition_exhibitor"]),
		ExhibitorsUpperBound: shared.SafeConvertToNullableUInt32(record["exhibitors_upper_bound"]),
		ExhibitorsLowerBound: shared.SafeConvertToNullableUInt32(record["exhibitors_lower_bound"]),
		ExhibitorsMean:       shared.SafeConvertToNullableUInt32(record["exhibitors_mean"]),
		EventSponsor:         shared.SafeConvertToNullableUInt32(record["event_sponsor"]),
		EditionSponsor:       shared.SafeConvertToNullableUInt32(record["edition_sponsor"]),
		EventSpeaker:         shared.SafeConvertToNullableUInt32(record["event_speaker"]),
		EditionSpeaker:       shared.SafeConvertToNullableUInt32(record["edition_speaker"]),
		EventCreated:         shared.SafeConvertToDateTimeString(record["event_created"]),
		EditionCreated:       shared.SafeConvertToDateTimeString(record["edition_created"]),
		EventHybrid:          shared.SafeConvertToNullableUInt8(record["event_hybrid"]),
		IsBranded: func() *uint32 {
			if val, ok := record["isBranded"].(*uint32); ok {
				return val
			}
			return nil
		}(),
		Maturity:                        shared.SafeConvertToNullableString(record["maturity"]),
		EventPricing:                    shared.SafeConvertToNullableString(record["event_pricing"]),
		EventLogo:                       shared.SafeConvertToNullableString(record["event_logo"]),
		EventEstimatedVisitors:          shared.SafeConvertToNullableString(record["event_estimatedVisitors"]),
		EventFrequency:                  shared.SafeConvertToNullableString(record["event_frequency"]),
		InboundScore:                    shared.SafeConvertToNullableUInt32(record["inboundScore"]),
		InternationalScore:              shared.SafeConvertToNullableUInt32(record["internationalScore"]),
		RepeatSentimentChangePercentage: shared.SafeConvertToNullableFloat64(record["repeatSentimentChangePercentage"]),
		AudienceZone:                    shared.SafeConvertToNullableString(record["audienceZone"]),
		EventEconomicFoodAndBevarage:    shared.SafeConvertToNullableFloat64(record["event_economic_FoodAndBevarage"]),
		EventEconomicTransportation:     shared.SafeConvertToNullableFloat64(record["event_economic_Transportation"]),
		EventEconomicAccomodation:       shared.SafeConvertToNullableFloat64(record["event_economic_Accomodation"]),
		EventEconomicUtilities:          shared.SafeConvertToNullableFloat64(record["event_economic_Utilities"]),
		EventEconomicFlights:            shared.SafeConvertToNullableFloat64(record["event_economic_flights"]),
		EventEconomicValue:              shared.SafeConvertToNullableFloat64(record["event_economic_value"]),
		EventEconomicDayWiseImpact:      shared.SafeConvertToString(record["event_economic_dayWiseEconomicImpact"]),
		EventEconomicBreakdown:          shared.SafeConvertToString(record["event_economic_breakdown"]),
		EventEconomicImpact:             shared.SafeConvertToString(record["event_economic_impact"]),
		EventAvgRating:                  shared.SafeConvertFloat64ToDecimalString(record["event_avgRating"]),
		Version:                         shared.SafeConvertToUInt32(record["version"]),
	}
}

type allEventRecord struct {
	EventID                         uint32   `ch:"event_id"`
	EventUUID                       string   `ch:"event_uuid"` // UUID generated from event_id + event_created
	EventName                       string   `ch:"event_name"`
	EventAbbrName                   *string  `ch:"event_abbr_name"`
	EventDescription                *string  `ch:"event_description"`
	EventPunchline                  *string  `ch:"event_punchline"`
	EventAvgRating                  *string  `ch:"event_avgRating"` // Nullable(Decimal(3,2))
	StartDate                       string   `ch:"start_date"`      // Date NOT NULL
	EndDate                         string   `ch:"end_date"`        // Date NOT NULL
	EditionID                       uint32   `ch:"edition_id"`
	EditionCountry                  string   `ch:"edition_country"`       // LowCardinality(FixedString(2)) NOT NULL
	EditionCity                     uint32   `ch:"edition_city"`          // UInt32 NOT NULL
	EditionCityName                 string   `ch:"edition_city_name"`     // String NOT NULL
	EditionCityStateID              *uint32  `ch:"edition_city_state_id"` // Nullable(UInt32)
	EditionCityState                string   `ch:"edition_city_state"`    // LowCardinality(String) NOT NULL
	EditionCityLat                  float64  `ch:"edition_city_lat"`      // Float64 NOT NULL
	EditionCityLong                 float64  `ch:"edition_city_long"`     // Float64 NOT NULL
	CompanyID                       *uint32  `ch:"company_id"`
	CompanyName                     *string  `ch:"company_name"`
	CompanyDomain                   *string  `ch:"company_domain"`
	CompanyWebsite                  *string  `ch:"company_website"`
	CompanyCountry                  *string  `ch:"company_country"`
	CompanyState                    *string  `ch:"company_state"` // LowCardinality(Nullable(String))
	CompanyCity                     *uint32  `ch:"company_city"`
	CompanyCityName                 *string  `ch:"company_city_name"`
	VenueID                         *uint32  `ch:"venue_id"`
	VenueName                       *string  `ch:"venue_name"`
	VenueCountry                    *string  `ch:"venue_country"`
	VenueCity                       *uint32  `ch:"venue_city"`
	VenueCityName                   *string  `ch:"venue_city_name"`
	VenueLat                        *float64 `ch:"venue_lat"`
	VenueLong                       *float64 `ch:"venue_long"`
	Published                       int8     `ch:"published"`              // Int8 NOT NULL
	Status                          string   `ch:"status"`                 // LowCardinality(FixedString(1)) NOT NULL DEFAULT 'A'
	EditionsAudianceType            uint16   `ch:"editions_audiance_type"` // UInt16 NOT NULL
	EditionFunctionality            string   `ch:"edition_functionality"`  // LowCardinality(String) NOT NULL
	EditionWebsite                  *string  `ch:"edition_website"`
	EditionDomain                   *string  `ch:"edition_domain"`
	EditionType                     string   `ch:"edition_type"` // LowCardinality(Nullable(String)) DEFAULT 'NA'
	EventFollowers                  *uint32  `ch:"event_followers"`
	EditionFollowers                *uint32  `ch:"edition_followers"`
	EventExhibitor                  *uint32  `ch:"event_exhibitor"`
	EditionExhibitor                *uint32  `ch:"edition_exhibitor"`
	ExhibitorsUpperBound            *uint32  `ch:"exhibitors_upper_bound"`
	ExhibitorsLowerBound            *uint32  `ch:"exhibitors_lower_bound"`
	ExhibitorsMean                  *uint32  `ch:"exhibitors_mean"`
	EventSponsor                    *uint32  `ch:"event_sponsor"`
	EditionSponsor                  *uint32  `ch:"edition_sponsor"`
	EventSpeaker                    *uint32  `ch:"event_speaker"`
	EditionSpeaker                  *uint32  `ch:"edition_speaker"`
	EventCreated                    string   `ch:"event_created"`                        // DateTime NOT NULL
	EditionCreated                  string   `ch:"edition_created"`                      // DateTime NOT NULL
	EventHybrid                     *uint8   `ch:"event_hybrid"`                         // Nullable(UInt8)
	IsBranded                       *uint32  `ch:"isBranded"`                            // Nullable(UInt32)
	Maturity                        *string  `ch:"maturity"`                             // LowCardinality(Nullable(String))
	EventPricing                    *string  `ch:"event_pricing"`                        // LowCardinality(Nullable(String))
	EventLogo                       *string  `ch:"event_logo"`                           // Nullable(String)
	EventEstimatedVisitors          *string  `ch:"event_estimatedVisitors"`              // LowCardinality(Nullable(String))
	EventFrequency                  *string  `ch:"event_frequency"`                      // LowCardinality(Nullable(String))
	InboundScore                    *uint32  `ch:"inboundScore"`                         // Nullable(UInt32)
	InternationalScore              *uint32  `ch:"internationalScore"`                   // Nullable(UInt32)
	RepeatSentimentChangePercentage *float64 `ch:"repeatSentimentChangePercentage"`      // Nullable(Float64)
	AudienceZone                    *string  `ch:"audienceZone"`                         // LowCardinality(Nullable(String))
	EventEconomicFoodAndBevarage    *float64 `ch:"event_economic_FoodAndBevarage"`       // Nullable(Float64)
	EventEconomicTransportation     *float64 `ch:"event_economic_Transportation"`        // Nullable(Float64)
	EventEconomicAccomodation       *float64 `ch:"event_economic_Accomodation"`          // Nullable(Float64)
	EventEconomicUtilities          *float64 `ch:"event_economic_Utilities"`             // Nullable(Float64)
	EventEconomicFlights            *float64 `ch:"event_economic_flights"`               // Nullable(Float64)
	EventEconomicValue              *float64 `ch:"event_economic_value"`                 // Nullable(Float64)
	EventEconomicDayWiseImpact      string   `ch:"event_economic_dayWiseEconomicImpact"` // JSON
	EventEconomicBreakdown          string   `ch:"event_economic_breakdown"`             // JSON
	EventEconomicImpact             string   `ch:"event_economic_impact"`                // JSON
	Version                         uint32   `ch:"version"`
}

func buildAllEventMigrationData(db *sql.DB, table string, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT id, name as event_name, abbr_name, punchline, start_date, end_date, country, published, status, event_audience, functionality, brand_id, created FROM %s WHERE id >= %d AND id <= %d ORDER BY id LIMIT %d", table, startID, endID, batchSize)
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

func extractAllEventVenueIDs(editionData []map[string]interface{}) []int64 {
	var venueIDs []int64
	seen := make(map[int64]bool)
	nullCount := 0
	zeroCount := 0

	for _, edition := range editionData {
		venueID := edition["venue_id"]
		if venueID == nil {
			nullCount++
			continue
		}

		if id, ok := venueID.(int64); ok && id > 0 {
			if !seen[id] {
				venueIDs = append(venueIDs, id)
				seen[id] = true
			}
		} else {
			zeroCount++
		}
	}

	return venueIDs
}

func fetchAllEventVenueDataParallel(db *sql.DB, venueIDs []int64) []map[string]interface{} {
	if len(venueIDs) == 0 {
		return nil
	}

	batchSize := 1000
	var allVenueData []map[string]interface{}

	// Process batches sequentially (no nested workers)
	for i := 0; i < len(venueIDs); i += batchSize {
		end := i + batchSize
		if end > len(venueIDs) {
			end = len(venueIDs)
		}

		batch := venueIDs[i:end]
		venueData := fetchAllEventVenueDataForBatch(db, batch)
		allVenueData = append(allVenueData, venueData...)
	}

	retrievedVenueIDs := make(map[int64]bool)
	for _, venue := range allVenueData {
		if venueID, ok := venue["id"].(int64); ok {
			retrievedVenueIDs[venueID] = true
		}
	}

	var missingVenueIDs []int64
	for _, requestedID := range venueIDs {
		if !retrievedVenueIDs[requestedID] {
			missingVenueIDs = append(missingVenueIDs, requestedID)
		}
	}

	if len(missingVenueIDs) > 0 {
		log.Printf("Missing venue IDs (%d): %v", len(missingVenueIDs), missingVenueIDs)
	}

	return allVenueData
}

func fetchAllEventVenueDataForBatch(db *sql.DB, venueIDs []int64) []map[string]interface{} {
	if len(venueIDs) == 0 {
		return nil
	}

	placeholders := make([]string, len(venueIDs))
	args := make([]interface{}, len(venueIDs))
	for i, id := range venueIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(`
		SELECT 
			id, name as venue_name, country as venue_country, 
			city as venue_city, geo_lat as venue_lat, geo_long as venue_long
		FROM venue 
		WHERE id IN (%s)`, strings.Join(placeholders, ","))

	rows, err := db.Query(query, args...)
	if err != nil {
		log.Printf("Error fetching venue data: %v", err)
		return nil
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil
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

		row := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			if val == nil {
				row[col] = nil
			} else {
				// Handle latitude and longitude as numeric values for ClickHouse Float64 compatibility
				if col == "venue_lat" || col == "venue_long" {
					if bytes, ok := val.([]byte); ok {
						// Convert byte array to float64
						if len(bytes) > 0 {
							str := string(bytes)
							if str != "" {
								// Parse string to float64
								if f, err := strconv.ParseFloat(str, 64); err == nil {
									row[col] = f
								} else {
									log.Printf("Warning: Could not parse %s value '%s' to float64: %v", col, str, err)
									row[col] = nil
								}
							} else {
								row[col] = nil
							}
						} else {
							row[col] = nil
						}
					} else if str, ok := val.(string); ok {
						// Handle string values by parsing to float64
						if str != "" {
							if f, err := strconv.ParseFloat(str, 64); err == nil {
								row[col] = f
							} else {
								log.Printf("Warning: Could not parse %s string '%s' to float64: %v", col, str, err)
								row[col] = nil
							}
						} else {
							row[col] = nil
						}
					} else {
						// Keep numeric values as-is
						row[col] = val
					}
				} else {
					row[col] = val
				}
			}
		}
		results = append(results, row)
	}

	return results
}

func extractAllEventCompanyIDs(editionData []map[string]interface{}) []int64 {
	var companyIDs []int64
	seen := make(map[int64]bool)
	nullCount := 0
	zeroCount := 0

	for _, edition := range editionData {
		companyID := edition["company_id"]
		if companyID == nil {
			nullCount++
			continue
		}

		if id, ok := companyID.(int64); ok && id > 0 {
			if !seen[id] {
				companyIDs = append(companyIDs, id)
				seen[id] = true
			}
		} else {
			zeroCount++
		}
	}

	return companyIDs
}

func fetchAllEventCompanyDataParallel(db *sql.DB, companyIDs []int64) []map[string]interface{} {
	if len(companyIDs) == 0 {
		return nil
	}

	batchSize := 1000
	var allCompanyData []map[string]interface{}

	// Process batches sequentially (no nested workers)
	for i := 0; i < len(companyIDs); i += batchSize {
		end := i + batchSize
		if end > len(companyIDs) {
			end = len(companyIDs)
		}

		batch := companyIDs[i:end]
		companyData := fetchAllEventCompanyDataForBatch(db, batch)
		allCompanyData = append(allCompanyData, companyData...)
	}

	retrievedCompanyIDs := make(map[int64]bool)
	for _, company := range allCompanyData {
		if companyID, ok := company["id"].(int64); ok {
			retrievedCompanyIDs[companyID] = true
		}
	}

	return allCompanyData
}

func fetchAllEventCompanyDataForBatch(db *sql.DB, companyIDs []int64) []map[string]interface{} {
	if len(companyIDs) == 0 {
		return nil
	}

	placeholders := make([]string, len(companyIDs))
	args := make([]interface{}, len(companyIDs))
	for i, id := range companyIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(`
		SELECT 
			id, name as company_name, domain as company_domain, 
			website as company_website, country as company_country, 
			city as company_city
		FROM company 
		WHERE id IN (%s)`, strings.Join(placeholders, ","))

	rows, err := db.Query(query, args...)
	if err != nil {
		log.Printf("Error fetching company data: %v", err)
		return nil
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil
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

	return results
}

func extractAllEventIDs(batchData []map[string]interface{}) []int64 {
	var eventIDs []int64
	for _, row := range batchData {
		if id, ok := row["id"].(int64); ok {
			eventIDs = append(eventIDs, id)
		}
	}
	return eventIDs
}

func fetchAllEventEditionDataParallel(db *sql.DB, eventIDs []int64) []map[string]interface{} {
	if len(eventIDs) == 0 {
		return nil
	}

	batchSize := 1000
	var allEditionData []map[string]interface{}

	// Process batches sequentially (no nested workers)
	for i := 0; i < len(eventIDs); i += batchSize {
		end := i + batchSize
		if end > len(eventIDs) {
			end = len(eventIDs)
		}

		batch := eventIDs[i:end]
		editionData := fetchAllEventEditionDataForBatch(db, batch)
		allEditionData = append(allEditionData, editionData...)
	}

	return allEditionData
}

func fetchAllEventEditionDataForBatch(db *sql.DB, eventIDs []int64) []map[string]interface{} {
	if len(eventIDs) == 0 {
		return nil
	}

	placeholders := make([]string, len(eventIDs))
	args := make([]interface{}, len(eventIDs))
	for i, id := range eventIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	// First, fetch the current edition ID for each event from the event table
	currentEditionQuery := fmt.Sprintf(`
		SELECT 
			id as event_id, 
			event_edition as current_edition_id
		FROM event 
		WHERE id IN (%s)`, strings.Join(placeholders, ","))

	currentEditionRows, err := db.Query(currentEditionQuery, args...)
	if err != nil {
		log.Printf("Error fetching current edition data: %v", err)
		return nil
	}
	defer currentEditionRows.Close()

	// Create a map of event_id to current_edition_id
	currentEditionMap := make(map[int64]int64)
	for currentEditionRows.Next() {
		var eventID int64
		var currentEditionID sql.NullInt64
		if err := currentEditionRows.Scan(&eventID, &currentEditionID); err != nil {
			continue
		}
		if currentEditionID.Valid {
			currentEditionMap[eventID] = currentEditionID.Int64
		}
	}

	editionQuery := fmt.Sprintf(`
		SELECT 
			event, id as edition_id, city as edition_city, 
			company_id, venue as venue_id, website as edition_website, 
			created as edition_created, start_date as edition_start_date,
			exhibitors_total
		FROM event_edition 
		WHERE event IN (%s)`, strings.Join(placeholders, ","))

	// log.Printf("Fetching editions for %d events: %v", len(eventIDs), eventIDs)

	editionRows, err := db.Query(editionQuery, args...)
	if err != nil {
		log.Printf("Error fetching edition data: %v", err)
		return nil
	}
	defer editionRows.Close()

	columns, err := editionRows.Columns()
	if err != nil {
		return nil
	}

	var results []map[string]interface{}
	for editionRows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := editionRows.Scan(valuePtrs...); err != nil {
			continue
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

		// Add current_edition_id and exhibitors_total for current editions only
		if eventID, ok := row["event"].(int64); ok {
			if currentEditionID, exists := currentEditionMap[eventID]; exists {
				row["current_edition_id"] = currentEditionID

				// Only add exhibitors_total if this is the current edition
				if editionID, ok := row["edition_id"].(int64); ok && editionID == currentEditionID {
					row["current_edition_exhibitors_total"] = row["exhibitors_total"]
				}
			}
		}

		results = append(results, row)
	}

	// log.Printf("Fetched %d editions for %d events", len(results), len(eventIDs))
	return results
}

func fetchAllEventDataForBatch(db *sql.DB, eventIDs []int64) []map[string]interface{} {
	if len(eventIDs) == 0 {
		return nil
	}

	placeholders := make([]string, len(eventIDs))
	args := make([]interface{}, len(eventIDs))
	for i, id := range eventIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(`
		SELECT id, name as event_name, abbr_name, punchline, start_date, end_date, 
		       country, published, status, event_audience, functionality, brand_id, created, event_type 
		FROM event 
		WHERE id IN (%s)`, strings.Join(placeholders, ","))

	rows, err := db.Query(query, args...)
	if err != nil {
		log.Printf("Error fetching event data: %v", err)
		return nil
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil
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

	return results
}

func fetchAllEventEstimateDataForBatch(db *sql.DB, eventIDs []int64) map[int64]string {
	if len(eventIDs) == 0 {
		return nil
	}

	placeholders := make([]string, len(eventIDs))
	args := make([]interface{}, len(eventIDs))
	for i, id := range eventIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(`SELECT event_id, economic_impact FROM estimate WHERE event_id IN (%s)`, strings.Join(placeholders, ","))

	rows, err := db.Query(query, args...)
	if err != nil {
		log.Printf("Error fetching batch estimate data: %v", err)
		return nil
	}
	defer rows.Close()

	result := make(map[int64]string)
	for rows.Next() {
		var eventID int64
		var economicImpact sql.NullString
		if err := rows.Scan(&eventID, &economicImpact); err != nil {
			continue
		}
		if economicImpact.Valid {
			result[eventID] = economicImpact.String
		}
	}

	return result
}

func processAllEventEconomicImpactDataParallel(estimateDataMap map[int64]string) map[int64]map[string]interface{} {
	if len(estimateDataMap) == 0 {
		return nil
	}

	finalResult := make(map[int64]map[string]interface{})
	for eventID, economicImpact := range estimateDataMap {
		result := processAllEventSingleEconomicImpact(eventID, economicImpact)
		for id, data := range result {
			finalResult[id] = data
		}
	}

	return finalResult
}

func processAllEventSingleEconomicImpact(eventID int64, economicImpact string) map[int64]map[string]interface{} {
	result := make(map[int64]map[string]interface{})

	processedData := make(map[string]interface{})
	if json.Valid([]byte(economicImpact)) {
		processedData["rawJSON"] = economicImpact
	} else {
		processedData["rawJSON"] = "{}"
	}

	var economicImpactJSON map[string]interface{}
	if err := json.Unmarshal([]byte(economicImpact), &economicImpactJSON); err != nil {
		result[eventID] = processedData
		return result
	}

	// Skip processing if error field exists, but still save raw data
	if errorField, exists := economicImpactJSON["error"]; exists && errorField != nil {
		result[eventID] = processedData
		return result
	}

	// Try to extract data, but continue even if extraction fails
	total, totalBreakdown, dayWiseFormatted := formatAllEventEconomicImpact(eventID, economicImpact)

	if totalVal, ok := total.(float64); ok {
		processedData["total"] = totalVal
	}

	if breakdownMap, ok := totalBreakdown.(map[string]float64); ok {
		if flights, exists := breakdownMap["Flights"]; exists {
			processedData["flights"] = flights
		}
		if foodBeverages, exists := breakdownMap["Food & Beverages"]; exists {
			processedData["foodBeverages"] = foodBeverages
		}
		if transportation, exists := breakdownMap["Transportation"]; exists {
			processedData["transportation"] = transportation
		}
		if utilities, exists := breakdownMap["Utilities"]; exists {
			processedData["utilities"] = utilities
		}
		if accommodation, exists := breakdownMap["Accommodation"]; exists {
			processedData["accommodation"] = accommodation
		}
	}

	if totalBreakdownJSON, err := json.Marshal(totalBreakdown); err == nil {
		processedData["breakdownJSON"] = string(totalBreakdownJSON)
	} else {
		processedData["breakdownJSON"] = "{}"
	}

	if dayWiseJSON, err := json.Marshal(dayWiseFormatted); err == nil {
		processedData["dayWiseJSON"] = string(dayWiseJSON)
	} else {
		processedData["dayWiseJSON"] = "{}"
	}

	result[eventID] = processedData
	return result
}

func formatAllEventEconomicImpact(_ int64, economicImpact string) (interface{}, interface{}, interface{}) {
	var economicImpactJSON map[string]interface{}
	if err := json.Unmarshal([]byte(economicImpact), &economicImpactJSON); err != nil {
		return 0.0, make(map[string]float64), make(map[string]map[string]interface{})
	}

	if errorField, exists := economicImpactJSON["error"]; exists && errorField != nil {
		return 0.0, make(map[string]float64), make(map[string]map[string]interface{})
	}

	overallEstimate, ok := economicImpactJSON["AllDayWiseTotal"].(map[string]interface{})
	if !ok {
		return 0.0, make(map[string]float64), make(map[string]map[string]interface{})
	}

	dayTotal, ok := overallEstimate["day total"].(map[string]interface{})
	if !ok {
		return 0.0, make(map[string]float64), make(map[string]map[string]interface{})
	}

	var total float64
	totalBreakdown := make(map[string]float64)

	for key, value := range dayTotal {
		if val, ok := value.(float64); ok {
			roundedVal := math.Round(val*100) / 100
			if strings.ToLower(key) == "cost" {
				total = roundedVal
				continue
			}
			totalBreakdown[key] = roundedVal
		}
	}

	dayWise, ok := economicImpactJSON["dayWise"].(map[string]interface{})
	if !ok {
		return total, totalBreakdown, make(map[string]map[string]interface{})
	}

	dayWiseFormatted := make(map[string]map[string]interface{})
	for date, dayData := range dayWise {
		dayDataMap, ok := dayData.(map[string]interface{})
		if !ok {
			continue
		}

		dayTotalData, ok := dayDataMap["day total"].(map[string]interface{})
		if !ok {
			continue
		}

		var dayTotal float64
		dayTotalBreakdown := make(map[string]float64)

		for key, value := range dayTotalData {
			if val, ok := value.(float64); ok {
				roundedVal := math.Round(val*100) / 100 // Round to 2 decimal places
				if strings.ToLower(key) == "cost" {
					dayTotal = roundedVal
					continue
				}
				dayTotalBreakdown[key] = roundedVal
			}
		}

		dateParts := strings.Split(date, "/")
		if len(dateParts) == 3 {
			formattedDate := "20" + dateParts[0] + "-" + dateParts[1] + "-" + dateParts[2]
			dayWiseFormatted[formattedDate] = map[string]interface{}{
				"total":     dayTotal,
				"breakdown": dayTotalBreakdown,
			}
		}
	}

	return total, totalBreakdown, dayWiseFormatted
}

func ProcessAllEventOnly(mysqlDB *sql.DB, clickhouseConn driver.Conn, esClient *elasticsearch.Client, config shared.Config) {
	log.Println("=== Starting all event ONLY Processing ===")

	// Get total records and min/max ID's count from event table
	totalRecords, minID, maxID, err := shared.GetTotalRecordsAndIDRange(mysqlDB, "event")
	if err != nil {
		log.Fatal("Failed to get total records and ID range from event:", err)
	}

	log.Printf("Total event records: %d, Min ID: %d, Max ID: %d", totalRecords, minID, maxID)

	// Calculate chunk size based on user input
	if config.NumChunks <= 0 {
		config.NumChunks = 5 // Default to 5 chunks if not specified
	}

	chunkSize := (maxID - minID + 1) / config.NumChunks
	if chunkSize == 0 {
		chunkSize = 1
	}

	log.Printf("Processing all event data in %d chunks with chunk size: %d", config.NumChunks, chunkSize)

	globalUniqueRecords := make(map[string]bool)
	var globalMutex sync.RWMutex

	var totalRecordsProcessed int64
	var totalRecordsSkipped int64
	var totalRecordsInserted int64
	var globalCountMutex sync.Mutex

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
			log.Printf("Waiting %v before launching all event chunk %d...", delay, i+1)
			time.Sleep(delay)
		}

		semaphore <- struct{}{}
		go func(chunkNum, start, end int) {
			defer func() { <-semaphore }()
			processallEventChunk(mysqlDB, clickhouseConn, esClient, config, start, end, chunkNum, results, globalUniqueRecords, &globalMutex, &totalRecordsProcessed, &totalRecordsSkipped, &totalRecordsInserted, &globalCountMutex)
		}(i+1, startID, endID)
	}

	for i := 0; i < config.NumChunks; i++ {
		result := <-results
		log.Printf("all event Result: %s", result)
	}

	// Print final summary
	globalCountMutex.Lock()
	log.Printf("=== FINAL SUMMARY ===")
	log.Printf("Total records processed: %d", totalRecordsProcessed)
	log.Printf("Total records skipped (duplicates): %d", totalRecordsSkipped)
	log.Printf("Total records inserted: %d", totalRecordsInserted)

	// Check for missing records in source data
	var nullEventCount int
	var invalidEventCount int
	var totalEditionsInSource int
	var validEventCount int

	// Get total editions in source
	err = mysqlDB.QueryRow("SELECT COUNT(*) FROM event_edition").Scan(&totalEditionsInSource)
	if err != nil {
		log.Printf("Error getting total editions count: %v", err)
	} else {
		log.Printf("Total editions in source (event_edition table): %d", totalEditionsInSource)
	}

	// Check for editions with NULL event values
	err = mysqlDB.QueryRow("SELECT COUNT(*) FROM event_edition WHERE event IS NULL").Scan(&nullEventCount)
	if err != nil {
		log.Printf("Error checking NULL events: %v", err)
	} else {
		log.Printf("Editions with NULL event values: %d", nullEventCount)
	}

	// Check for editions with invalid event IDs (not in event table)
	err = mysqlDB.QueryRow(`
		SELECT COUNT(*) 
		FROM event_edition ee 
		LEFT JOIN event e ON ee.event = e.id 
		WHERE ee.event IS NOT NULL AND e.id IS NULL
	`).Scan(&invalidEventCount)
	if err != nil {
		log.Printf("Error checking invalid events: %v", err)
	} else {
		log.Printf("Editions with invalid event IDs: %d", invalidEventCount)
	}

	// Check for editions that should be processed (valid event IDs)
	err = mysqlDB.QueryRow(`
		SELECT COUNT(*) 
		FROM event_edition ee 
		INNER JOIN event e ON ee.event = e.id
	`).Scan(&validEventCount)
	if err != nil {
		log.Printf("Error checking valid events: %v", err)
	} else {
		log.Printf("Editions with valid event IDs: %d", validEventCount)
	}

	log.Printf("Total missing editions (NULL + invalid): %d", nullEventCount+invalidEventCount)
	log.Printf("Verification: NULL(%d) + Invalid(%d) + Valid(%d) = Total(%d)",
		nullEventCount, invalidEventCount, validEventCount, totalEditionsInSource)
	globalCountMutex.Unlock()

	log.Println("all event processing completed!")
}

// processes a single chunk of all event data
func processallEventChunk(mysqlDB *sql.DB, clickhouseConn driver.Conn, esClient *elasticsearch.Client, config shared.Config, startID, endID int, chunkNum int, results chan<- string, globalUniqueRecords map[string]bool, globalMutex *sync.RWMutex, totalRecordsProcessed *int64, totalRecordsSkipped *int64, totalRecordsInserted *int64, globalCountMutex *sync.Mutex) {
	log.Printf("Processing all event chunk %d: ID range %d-%d", chunkNum, startID, endID)

	totalRecords := endID - startID + 1
	processed := 0

	offset := 0
	for {
		batchData, err := buildAllEventMigrationData(mysqlDB, "event", startID, endID, config.BatchSize)
		if err != nil {
			log.Printf("all event chunk %d batch error: %v", chunkNum, err)
			results <- fmt.Sprintf("all event chunk %d batch error: %v", chunkNum, err)
			return
		}

		if len(batchData) == 0 {
			break
		}

		processed += len(batchData)
		progress := float64(processed) / float64(totalRecords) * 100
		log.Printf("all event chunk %d: Retrieved %d records in batch (%.1f%% complete)", chunkNum, len(batchData), progress)

		// Get event IDs from this batch
		eventIDs := extractAllEventIDs(batchData)
		if len(eventIDs) > 0 {
			log.Printf("all event chunk %d: Fetching edition data for %d events", chunkNum, len(eventIDs))

			// Fetch edition data in parallel
			startTime := time.Now()
			editionData := fetchAllEventEditionDataParallel(mysqlDB, eventIDs)
			editionTime := time.Since(startTime)
			log.Printf("all event chunk %d: Retrieved edition data for %d events in %v", chunkNum, len(editionData), editionTime)

			var companyData []map[string]interface{}
			if len(editionData) > 0 {
				companyIDs := extractAllEventCompanyIDs(editionData)
				if len(companyIDs) > 0 {
					log.Printf("all event chunk %d: Fetching company data for %d companies", chunkNum, len(companyIDs))
					startTime = time.Now()
					companyData = fetchAllEventCompanyDataParallel(mysqlDB, companyIDs)
					companyTime := time.Since(startTime)
					log.Printf("all event chunk %d: Retrieved company data for %d companies in %v", chunkNum, len(companyData), companyTime)
				}
			}

			// Fetch venue data for all editions
			var venueData []map[string]interface{}
			if len(editionData) > 0 {
				venueIDs := extractAllEventVenueIDs(editionData)
				if len(venueIDs) > 0 {
					log.Printf("all event chunk %d: Fetching venue data for %d venues", chunkNum, len(venueIDs))
					startTime = time.Now()
					venueData = fetchAllEventVenueDataParallel(mysqlDB, venueIDs)
					venueTime := time.Since(startTime)
					log.Printf("all event chunk %d: Retrieved venue data for %d venues in %v", chunkNum, len(venueData), venueTime)
				}
			}

			var cityData []map[string]interface{}
			if len(editionData) > 0 {
				editionCityIDs := extractAllEventCityIDs(editionData)

				var companyCityIDs []int64
				seenCompanyCityIDs := make(map[int64]bool)
				for _, company := range companyData {
					if cityID, ok := company["company_city"].(int64); ok && cityID > 0 {
						if !seenCompanyCityIDs[cityID] {
							companyCityIDs = append(companyCityIDs, cityID)
							seenCompanyCityIDs[cityID] = true
						}
					}
				}

				var venueCityIDs []int64
				seenVenueCityIDs := make(map[int64]bool)
				for _, venue := range venueData {
					if cityID, ok := venue["venue_city"].(int64); ok && cityID > 0 {
						if !seenVenueCityIDs[cityID] {
							venueCityIDs = append(venueCityIDs, cityID)
							seenVenueCityIDs[cityID] = true
						}
					}
				}

				allCityIDs := make([]int64, 0, len(editionCityIDs)+len(companyCityIDs)+len(venueCityIDs))
				seenAllCityIDs := make(map[int64]bool)

				for _, cityID := range editionCityIDs {
					if !seenAllCityIDs[cityID] {
						allCityIDs = append(allCityIDs, cityID)
						seenAllCityIDs[cityID] = true
					}
				}

				for _, cityID := range companyCityIDs {
					if !seenAllCityIDs[cityID] {
						allCityIDs = append(allCityIDs, cityID)
						seenAllCityIDs[cityID] = true
					}
				}

				for _, cityID := range venueCityIDs {
					if !seenAllCityIDs[cityID] {
						allCityIDs = append(allCityIDs, cityID)
						seenAllCityIDs[cityID] = true
					}
				}

				if len(allCityIDs) > 0 {
					log.Printf("all event chunk %d: Fetching city data for %d cities (edition: %d, company: %d, venue: %d)",
						chunkNum, len(allCityIDs), len(editionCityIDs), len(companyCityIDs), len(venueCityIDs))
					startTime = time.Now()
					cityData = shared.FetchCityDataParallel(mysqlDB, allCityIDs, config.NumWorkers)
					cityTime := time.Since(startTime)
					log.Printf("all event chunk %d: Retrieved city data for %d cities in %v", chunkNum, len(cityData), cityTime)
				}
			}

			var esData map[int64]map[string]interface{}
			if len(editionData) > 0 {
				log.Printf("all event chunk %d: Fetching Elasticsearch data for %d events in batches of 200", chunkNum, len(eventIDs))
				startTime = time.Now()
				esData = fetchAllEventElasticsearchDataForEvents(esClient, config.ElasticsearchIndex, eventIDs)
				esTime := time.Since(startTime)
				log.Printf("all event chunk %d: Retrieved Elasticsearch data for %d events in %v", chunkNum, len(esData), esTime)
			}

			if len(editionData) > 0 {
				companyLookup := make(map[int64]map[string]interface{})
				if len(companyData) > 0 {
					for _, company := range companyData {
						if companyID, ok := company["id"].(int64); ok {
							companyLookup[companyID] = company
						}
					}
				}

				venueLookup := make(map[int64]map[string]interface{})
				if len(venueData) > 0 {
					for _, venue := range venueData {
						if venueID, ok := venue["id"].(int64); ok {
							venueLookup[venueID] = venue
						}
					}
				}

				cityLookup := make(map[int64]map[string]interface{})
				if len(cityData) > 0 {
					for _, city := range cityData {
						if cityID, ok := city["id"].(int64); ok {
							cityLookup[cityID] = city
						}
					}
				}

				allEvents := make(map[int64][]map[string]interface{})
				currentEditionStartDates := make(map[int64]interface{})
				currentEditionIDs := make(map[int64]int64)

				editionsProcessed := 0
				editionsSkipped := 0
				for _, edition := range editionData {
					if eventID, ok := edition["event"].(int64); ok {
						allEvents[eventID] = append(allEvents[eventID], edition)
						editionsProcessed++
						if currentEditionID, exists := edition["current_edition_id"]; exists {
							if editionID, ok := edition["edition_id"].(int64); ok {
								if currentEditionID.(int64) == editionID {
									currentEditionStartDates[eventID] = edition["edition_start_date"]
									currentEditionIDs[eventID] = editionID
								}
							}
						}
					} else {
						editionsSkipped++
						log.Printf("all event chunk %d: Skipping edition - invalid event ID: %v", chunkNum, edition["event"])
					}
				}
				log.Printf("all event chunk %d: Editions processed: %d, skipped: %d", chunkNum, editionsProcessed, editionsSkipped)

				var clickHouseRecords []map[string]interface{}
				completeCount := 0
				partialCount := 0
				skippedCount := 0

				eventIDsForEditions := make([]int64, 0, len(allEvents))
				for eventID := range allEvents {
					eventIDsForEditions = append(eventIDsForEditions, eventID)
				}

				eventDataLookup := make(map[int64]map[string]interface{})
				if len(eventIDsForEditions) > 0 {
					log.Printf("all event chunk %d: Fetching event data for %d events with editions", chunkNum, len(eventIDsForEditions))
					eventDataForEditions := fetchAllEventDataForBatch(mysqlDB, eventIDsForEditions)
					for _, eventData := range eventDataForEditions {
						if eventID, ok := eventData["id"].(int64); ok {
							eventDataLookup[eventID] = eventData
						}
					}
				}

				estimateDataMap := make(map[int64]string)
				if len(eventIDsForEditions) > 0 {
					estimateDataMap = fetchAllEventEstimateDataForBatch(mysqlDB, eventIDsForEditions)
				}

				processedEconomicData := make(map[int64]map[string]interface{})
				if len(estimateDataMap) > 0 {
					processedEconomicData = processAllEventEconomicImpactDataParallel(estimateDataMap)
				}

				for eventID, editions := range allEvents {
					eventData := eventDataLookup[eventID]

					economicData := processedEconomicData[eventID]

					if eventData != nil {
						for _, edition := range editions {
							companyID := edition["company_id"]
							venueID := edition["venue_id"]
							cityID := edition["edition_city"]
							editionWebsite := edition["edition_website"]

							var company map[string]interface{}
							if companyID != nil {
								if c, exists := companyLookup[companyID.(int64)]; exists {
									company = c
								}
							}

							var venue map[string]interface{}
							if venueID != nil {
								if v, exists := venueLookup[venueID.(int64)]; exists {
									venue = v
								}

							}

							var city map[string]interface{}
							if cityID != nil {
								if c, exists := cityLookup[cityID.(int64)]; exists {
									city = c // If not found->city remains nil
								}
							}

							// Get company city data
							var companyCity map[string]interface{}
							if company != nil && company["company_city"] != nil {
								if companyCityID, ok := company["company_city"].(int64); ok {
									if c, exists := cityLookup[companyCityID]; exists {
										companyCity = c
									}
								}
							}

							// Get venue city data
							var venueCity map[string]interface{}
							if venue != nil && venue["venue_city"] != nil {
								if venueCityID, ok := venue["venue_city"].(int64); ok {
									if c, exists := cityLookup[venueCityID]; exists {
										venueCity = c
									}
								}
							}

							esInfoMap := esData[eventID]

							// Extract domain from edition website
							var editionDomain string
							if editionWebsite != nil {
								editionDomain = shared.ExtractDomainFromWebsite(editionWebsite) // If no website, editionDomain remains empty string
							}

							// Extract domain from company website
							var companyDomain string
							if company != nil && company["company_website"] != nil {
								companyDomain = shared.ExtractDomainFromWebsite(company["company_website"])
							}

							// Determine edition type using simplified logic
							editionType := determineAllEventEditionType(
								edition["edition_start_date"],
								currentEditionStartDates[eventID],
								edition["edition_id"].(int64),
								currentEditionIDs[eventID],
							)

							// Create unique key for deduplication (event_id + edition_id)
							eventIDStr := shared.ConvertToString(eventData["id"])
							editionIDStr := shared.ConvertToString(edition["edition_id"])
							uniqueKey := eventIDStr + "_" + editionIDStr

							globalMutex.RLock()
							exists := globalUniqueRecords[uniqueKey]
							globalMutex.RUnlock()

							if exists {
								globalCountMutex.Lock()
								*totalRecordsSkipped++
								globalCountMutex.Unlock()
								continue
							}

							globalMutex.Lock()
							globalUniqueRecords[uniqueKey] = true
							globalMutex.Unlock()

							record := map[string]interface{}{
								"event_id":          eventData["id"],
								"event_uuid":        shared.GenerateEventUUID(shared.ConvertToUInt32(eventData["id"]), eventData["created"]),
								"event_name":        eventData["event_name"],
								"event_abbr_name":   eventData["abbr_name"],
								"event_description": esInfoMap["event_description"],
								"event_punchline":   esInfoMap["event_punchline"],
								"start_date":        eventData["start_date"],
								"end_date":          eventData["end_date"],
								"edition_id":        edition["edition_id"],
								"edition_country":   strings.ToUpper(shared.ConvertToString(eventData["country"])),
								"edition_city":      edition["edition_city"],
								"edition_city_name": shared.ConvertToString(city["name"]),
								"edition_city_state": func() string {
									if city != nil && city["state"] != nil {
										stateStr := shared.ConvertToString(city["state"])
										if strings.TrimSpace(stateStr) == "" {
											return "any"
										}
										return stateStr
									}
									return "any"
								}(),
								"edition_city_state_id": func() interface{} {
									if city != nil && city["state_id"] != nil {
										if stateID, ok := city["state_id"].(int64); ok && stateID > 0 {
											stateIDUint32 := uint32(stateID)
											return stateIDUint32
										}
									}
									return nil
								}(),
								"edition_city_lat":  city["event_city_lat"],
								"edition_city_long": city["event_city_long"],
								"company_id":        company["id"],
								"company_name":      company["company_name"],
								"company_domain":    companyDomain,
								"company_website":   company["company_website"],
								"company_country":   strings.ToUpper(shared.ConvertToString(company["company_country"])),
								"company_state": func() *string {
									if companyCity != nil && companyCity["state"] != nil {
										stateStr := shared.ConvertToString(companyCity["state"])
										if strings.TrimSpace(stateStr) == "" {
											return nil
										}
										return &stateStr
									}
									return nil
								}(),
								"company_city": company["company_city"],
								"company_city_name": func() *string {
									if companyCity != nil && companyCity["name"] != nil {
										nameStr := shared.ConvertToString(companyCity["name"])
										return &nameStr
									}
									return nil
								}(),
								"venue_id":      venue["id"],
								"venue_name":    venue["venue_name"],
								"venue_country": strings.ToUpper(shared.ConvertToString(venue["venue_country"])),
								"venue_city":    venue["venue_city"],
								"venue_city_name": func() *string {
									if venueCity != nil && venueCity["name"] != nil {
										nameStr := shared.ConvertToString(venueCity["name"])
										return &nameStr
									}
									return nil
								}(),
								"venue_lat":              venue["venue_lat"],
								"venue_long":             venue["venue_long"],
								"published":              eventData["published"],
								"status":                 eventData["status"],
								"editions_audiance_type": eventData["event_audience"],
								"edition_functionality":  eventData["functionality"],
								"edition_website":        edition["edition_website"],
								"edition_domain":         editionDomain,
								"event_followers":        esInfoMap["event_following"],
								"edition_followers":      esInfoMap["event_following"],
								"event_exhibitor":        esInfoMap["event_exhibitors"],
								"edition_exhibitor":      esInfoMap["edition_exhibitor"],
								"exhibitors_upper_bound": nil,
								"exhibitors_lower_bound": nil,
								"exhibitors_mean":        nil,
								"event_sponsor":          esInfoMap["event_totalSponsor"],
								"edition_sponsor":        esInfoMap["edition_sponsor"],
								"event_speaker":          esInfoMap["event_speakers"],
								"edition_speaker":        esInfoMap["edition_speaker"],
								"event_created":          eventData["created"],
								"edition_created":        edition["edition_created"],
								"event_hybrid":           esInfoMap["event_hybrid"],
								"isBranded": func() *uint32 {
									if eventData["brand_id"] != nil {
										// If brand_id exists, set isBranded to 1 (true)
										val := uint32(1)
										return &val
									}
									// If brand_id is null, set isBranded to 0 (false)
									val := uint32(0)
									return &val
								}(),
								"maturity":                        determineAllEventMaturity(esInfoMap["total_edition"]),
								"event_pricing":                   esInfoMap["event_pricing"],
								"event_logo":                      esInfoMap["event_logo"],
								"event_estimatedVisitors":         esInfoMap["eventEstimatedTag"],
								"event_frequency":                 esInfoMap["event_frequency"],
								"inboundScore":                    esInfoMap["inboundScore"],
								"internationalScore":              esInfoMap["internationalScore"],
								"repeatSentimentChangePercentage": esInfoMap["repeatSentimentChangePercentage"],
								"audienceZone":                    esInfoMap["audienceZone"],
								"event_avgRating":                 esInfoMap["avg_rating"],
								"version":                         1,
							}

							var currentEditionEventType interface{}
							if currentEditionIDs[eventID] == edition["edition_id"].(int64) {
								currentEditionEventType = eventData["event_type"]

								exhibitorsCountByOrganizer := edition["current_edition_exhibitors_total"]
								visitorLeads := esInfoMap["event_following"]
								eventTypeSourceID := currentEditionEventType

								var exhibitorsCount *int64
								if exhibitorsCountByOrganizer != nil {
									if val, ok := exhibitorsCountByOrganizer.(int64); ok {
										exhibitorsCount = &val
									}
								}

								var visitorLeadsInt int64 = 0
								if visitorLeads != nil {
									if val, ok := visitorLeads.(uint32); ok {
										visitorLeadsInt = int64(val)
									}
								}

								var eventTypeID *int64
								if eventTypeSourceID != nil {
									if val, ok := eventTypeSourceID.(int64); ok {
										eventTypeID = &val
									}
								}

								var upperBound, lowerBound, mean *int64

								if exhibitorsCount != nil && *exhibitorsCount >= 0 {
									upperBound = exhibitorsCount
									lowerBound = exhibitorsCount
								} else {
									if eventTypeID != nil && *eventTypeID == 1 {
										if visitorLeadsInt == 0 {
											upper := int64(100)
											lower := int64(20)
											upperBound = &upper
											lowerBound = &lower
										} else if visitorLeadsInt >= 1 && visitorLeadsInt <= 100 {
											upper := int64(500)
											lower := int64(100)
											upperBound = &upper
											lowerBound = &lower
										} else {
											upper := int64(1000)
											lower := int64(500)
											upperBound = &upper
											lowerBound = &lower
										}
									} else {
										upperBound = nil
										lowerBound = nil
									}
								}

								if upperBound != nil && lowerBound != nil {
									meanVal := (*upperBound + *lowerBound) / 2
									mean = &meanVal
								} else {
									mean = nil
								}

								if upperBound != nil {
									record["exhibitors_upper_bound"] = uint32(*upperBound)
								}
								if lowerBound != nil {
									record["exhibitors_lower_bound"] = uint32(*lowerBound)
								}
								if mean != nil {
									record["exhibitors_mean"] = uint32(*mean)
								}

							}

							if editionType != nil {
								record["edition_type"] = *editionType
							} else {
								record["edition_type"] = "NA"
							}

							if economicData != nil {
								if totalVal, ok := economicData["total"].(float64); ok {
									record["event_economic_value"] = &totalVal
								}
								if flights, ok := economicData["flights"].(float64); ok {
									record["event_economic_flights"] = &flights
								}
								if foodBeverages, ok := economicData["foodBeverages"].(float64); ok {
									record["event_economic_FoodAndBevarage"] = &foodBeverages
								}
								if transportation, ok := economicData["transportation"].(float64); ok {
									record["event_economic_Transportation"] = &transportation
								}
								if utilities, ok := economicData["utilities"].(float64); ok {
									record["event_economic_Utilities"] = &utilities
								}
								if accommodation, ok := economicData["accommodation"].(float64); ok {
									record["event_economic_Accomodation"] = &accommodation
								}

								if breakdownJSON, ok := economicData["breakdownJSON"].(string); ok {
									record["event_economic_breakdown"] = breakdownJSON
								} else {
									record["event_economic_breakdown"] = "{}"
								}

								if dayWiseJSON, ok := economicData["dayWiseJSON"].(string); ok {
									record["event_economic_dayWiseEconomicImpact"] = dayWiseJSON
								} else {
									record["event_economic_dayWiseEconomicImpact"] = "{}"
								}

								if rawJSON, ok := economicData["rawJSON"].(string); ok {
									record["event_economic_impact"] = rawJSON
								} else {
									record["event_economic_impact"] = "{}"
								}
							} else {
								record["event_economic_breakdown"] = "{}"
								record["event_economic_dayWiseEconomicImpact"] = "{}"
								record["event_economic_impact"] = "{}"
							}

							clickHouseRecords = append(clickHouseRecords, record)

							globalCountMutex.Lock()
							*totalRecordsProcessed++
							*totalRecordsInserted++
							globalCountMutex.Unlock()

							if companyID != nil && venueID != nil && cityID != nil {
								completeCount++
							} else {
								partialCount++
							}
						}
					} else {
						skippedCount++
						log.Printf("all event chunk %d: Skipping event %d - no event data found", chunkNum, eventID)
					}
				}

				// Count events with missing current editions
				eventsWithMissingCurrentEdition := 0
				for eventID := range allEvents {
					if currentEditionIDs[eventID] == 0 {
						eventsWithMissingCurrentEdition++
					}
				}

				log.Printf("all event chunk %d: Data completeness - Complete: %d, Partial: %d, Skipped: %d",
					chunkNum, completeCount, partialCount, skippedCount)
				if eventsWithMissingCurrentEdition > 0 {
					log.Printf("all event chunk %d: Warning - %d events have no current edition (event_edition is NULL)",
						chunkNum, eventsWithMissingCurrentEdition)
				}

				// Insert collected records into ClickHouse
				log.Printf("all event chunk %d: Total records collected: %d", chunkNum, len(clickHouseRecords))
				if len(clickHouseRecords) > 0 {
					log.Printf("all event chunk %d: Attempting to insert %d records into ClickHouse...", chunkNum, len(clickHouseRecords))

					insertErr := shared.RetryWithBackoff(
						func() error {
							return insertallEventDataIntoClickHouse(clickhouseConn, clickHouseRecords, config.ClickHouseWorkers, config)
						},
						3,
						fmt.Sprintf("ClickHouse insertion for chunk %d", chunkNum),
					)

					if insertErr != nil {
						log.Printf("all event chunk %d: ClickHouse insertion failed after retries: %v", chunkNum, insertErr)
						log.Printf("all event chunk %d: %d records failed to insert - consider manual retry", chunkNum, len(clickHouseRecords))
					} else {
						log.Printf("all event chunk %d: Successfully inserted %d records into ClickHouse", chunkNum, len(clickHouseRecords))
					}
				} else {
					log.Printf("all event chunk %d: No records to insert into ClickHouse", chunkNum)
				}
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

	results <- fmt.Sprintf("all event chunk %d completed successfully", chunkNum)
}

// 1. current_edition: The edition_id that matches event.event_edition (only one per event)
// 2. future_edition: All editions with start_date > current_edition start_date
// 3. past_edition: All editions with start_date < current_edition start_date
func determineAllEventEditionType(editionStartDate, currentEditionStartDate interface{}, editionID, currentEditionID int64) *string {
	// edition is the current edition, return "current_edition"
	if editionID == currentEditionID {
		editionType := "current_edition"
		return &editionType
	}

	var editionDateStr, currentDateStr string

	switch v := editionStartDate.(type) {
	case string:
		editionDateStr = v
	case []uint8:
		editionDateStr = string(v)
	default:
		return nil
	}

	switch v := currentEditionStartDate.(type) {
	case string:
		currentDateStr = v
	case []uint8:
		currentDateStr = string(v)
	default:
		return nil
	}

	editionDate, err := time.Parse("2006-01-02", editionDateStr)
	if err != nil {
		return nil
	}

	currentDate, err := time.Parse("2006-01-02", currentDateStr)
	if err != nil {
		return nil
	}

	if editionDate.After(currentDate) {
		editionType := "future_edition"
		return &editionType
	} else if editionDate.Before(currentDate) {
		editionType := "past_edition"
		return &editionType
	} else {
		editionType := "past_edition"
		return &editionType
	}
}

// new: 1 edition, growing: 2-3 editions, established: 4-8 editions, flagship: 9+ editions
func determineAllEventMaturity(totalEdition interface{}) *string {
	if totalEdition == nil {
		return nil
	}

	var editionCount uint32
	switch v := totalEdition.(type) {
	case uint32:
		editionCount = v
	case int:
		if v >= 0 {
			editionCount = uint32(v)
		} else {
			return nil
		}
	case int64:
		if v >= 0 {
			editionCount = uint32(v)
		} else {
			return nil
		}
	case float64:
		if v >= 0 && v == float64(uint32(v)) {
			editionCount = uint32(v)
		} else {
			return nil
		}
	default:
		return nil
	}

	var maturity string
	switch {
	case editionCount == 1:
		maturity = "new"
	case editionCount >= 2 && editionCount <= 3:
		maturity = "growing"
	case editionCount >= 4 && editionCount <= 8:
		maturity = "established"
	case editionCount >= 9:
		maturity = "flagship"
	default:
		return nil
	}

	return &maturity
}

func extractAllEventCityIDs(editionData []map[string]interface{}) []int64 {
	var cityIDs []int64
	seen := make(map[int64]bool)
	nullCount := 0
	zeroCount := 0

	for _, edition := range editionData {
		cityID := edition["edition_city"]
		if cityID == nil {
			nullCount++
			continue
		}

		if id, ok := cityID.(int64); ok && id > 0 {
			if !seen[id] {
				cityIDs = append(cityIDs, id)
				seen[id] = true
			}
		} else {
			zeroCount++
		}
	}

	return cityIDs
}

func fetchAllEventElasticsearchBatch(esClient *elasticsearch.Client, indexName string, eventIDs []int64) map[int64]map[string]interface{} {
	if len(eventIDs) == 0 {
		return nil
	}

	results := make(map[int64]map[string]interface{})

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"terms": map[string]interface{}{
				"id": eventIDs,
			},
		},
		"size":    len(eventIDs),
		"_source": []string{"id", "description", "exhibitors", "speakers", "totalSponsor", "following", "punchline", "frequency", "city", "hybrid", "logo", "pricing", "total_edition", "avg_rating", "eventEstimatedTag", "inboundScore", "internationalScore", "repeatSentimentChangePercentage", "audienceZone"},
	}

	queryJSON, _ := json.Marshal(query)

	// Execute search with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	searchRes, err := esClient.Search(
		esClient.Search.WithContext(ctx),
		esClient.Search.WithIndex(indexName),
		esClient.Search.WithBody(strings.NewReader(string(queryJSON))),
	)
	if err != nil {
		log.Printf("Warning: Failed to search Elasticsearch for batch: %v", err)
		return results
	}
	defer searchRes.Body.Close()

	if searchRes.IsError() {
		log.Printf("Warning: Elasticsearch search failed: %v", searchRes.Status())
		return results
	}

	// Parse response
	var result map[string]interface{}
	if err := json.NewDecoder(searchRes.Body).Decode(&result); err != nil {
		log.Printf("Warning: Failed to decode Elasticsearch response: %v", err)
		return results
	}

	hits := result["hits"].(map[string]interface{})
	hitsArray := hits["hits"].([]interface{})

	// Process each hit
	if len(hitsArray) == 0 {
		return results
	}

	for _, hit := range hitsArray {
		hitMap := hit.(map[string]interface{})
		source := hitMap["_source"].(map[string]interface{})

		// Handle id as either string or number (changed from event_id to id)
		var eventIDInt int64
		if eventIDStr, ok := source["id"].(string); ok {
			// Convert string to int64
			if parsedID, err := strconv.ParseInt(eventIDStr, 10, 64); err == nil {
				eventIDInt = parsedID
			} else {
				log.Printf("Warning: Failed to parse id string '%s': %v", eventIDStr, err)
				continue
			}
		} else if eventIDNum, ok := source["id"].(float64); ok {
			// Convert float64 to int64
			eventIDInt = int64(eventIDNum)
		} else {
			log.Printf("Warning: Unexpected id type: %T, value: %v", source["id"], source["id"])
			continue
		}

		convertStringToUInt32 := func(key string) interface{} {
			if val, exists := source[key]; exists && val != nil {
				strVal := shared.ConvertToString(val)
				if strVal != "" {
					if num, err := strconv.ParseUint(strVal, 10, 32); err == nil {
						return uint32(num)
					}
				}
			}
			return nil
		}

		convertStringToUInt8 := func(key string) interface{} {
			if val, exists := source[key]; exists && val != nil {
				// Handle float64 values directly (common from Elasticsearch)
				if floatVal, ok := val.(float64); ok {
					if floatVal >= 0 && floatVal <= 255 && floatVal == float64(uint8(floatVal)) {
						return uint8(floatVal)
					}
					return nil
				}
				// Handle other numeric types
				if intVal, ok := val.(int); ok {
					if intVal >= 0 && intVal <= 255 {
						return uint8(intVal)
					}
					return nil
				}
				// Handle string conversion as fallback
				strVal := shared.ConvertToString(val)
				if strVal != "" {
					if num, err := strconv.ParseUint(strVal, 10, 8); err == nil {
						return uint8(num)
					}
				}
			}
			return nil
		}

		var convertedTotalEdition interface{}
		if rawTotalEdition := source["total_edition"]; rawTotalEdition != nil {
			if floatVal, ok := rawTotalEdition.(float64); ok {
				if floatVal >= 0 && floatVal == float64(uint32(floatVal)) {
					convertedTotalEdition = uint32(floatVal)
				} else {
					convertedTotalEdition = nil
				}
			} else {
				convertedTotalEdition = nil
			}
		} else {
			convertedTotalEdition = nil
		}

		convertToFloat64 := func(key string) interface{} {
			if val, exists := source[key]; exists && val != nil {
				if floatVal, ok := val.(float64); ok {
					return floatVal
				}
				if intVal, ok := val.(int); ok {
					return float64(intVal)
				}
				if int64Val, ok := val.(int64); ok {
					return float64(int64Val)
				}
				strVal := shared.ConvertToString(val)
				if strVal != "" {
					if num, err := strconv.ParseFloat(strVal, 64); err == nil {
						return num
					}
				}
			}
			return nil
		}

		results[eventIDInt] = map[string]interface{}{
			"event_description":               shared.ConvertToString(source["description"]),
			"event_exhibitors":                convertStringToUInt32("exhibitors"),
			"event_speakers":                  convertStringToUInt32("speakers"),
			"event_totalSponsor":              convertStringToUInt32("totalSponsor"),
			"event_following":                 convertStringToUInt32("following"),
			"event_punchline":                 shared.ConvertToString(source["punchline"]),
			"edition_exhibitor":               convertStringToUInt32("exhibitors"),
			"edition_sponsor":                 convertStringToUInt32("totalSponsor"),
			"edition_speaker":                 convertStringToUInt32("speakers"),
			"edition_followers":               convertStringToUInt32("following"),
			"event_frequency":                 shared.ConvertToString(source["frequency"]),
			"event_hybrid":                    convertStringToUInt8("hybrid"),
			"event_logo":                      shared.ConvertToString(source["logo"]),
			"event_pricing":                   shared.ConvertToString(source["pricing"]),
			"total_edition":                   convertedTotalEdition,
			"avg_rating":                      source["avg_rating"],
			"eventEstimatedTag":               shared.ConvertToString(source["eventEstimatedTag"]),
			"inboundScore":                    convertStringToUInt32("inboundScore"),
			"internationalScore":              convertStringToUInt32("internationalScore"),
			"repeatSentimentChangePercentage": convertToFloat64("repeatSentimentChangePercentage"),
			"audienceZone":                    shared.ConvertToString(source["audienceZone"]),
		}
	}

	return results
}

func fetchAllEventElasticsearchDataForEvents(esClient *elasticsearch.Client, indexName string, eventIDs []int64) map[int64]map[string]interface{} {
	if len(eventIDs) == 0 {
		return nil
	}

	results := make(map[int64]map[string]interface{})
	batchSize := 200 //reduced batch size to prevent Elasticsearch timeouts

	expectedBatches := (len(eventIDs) + batchSize - 1) / batchSize
	resultsChan := make(chan map[int64]map[string]interface{}, expectedBatches)
	semaphore := make(chan struct{}, 5) //reduced concurrency to prevent Elasticsearch overload

	var allResults []map[int64]map[string]interface{}
	var wg sync.WaitGroup

	for i := 0; i < len(eventIDs); i += batchSize {
		end := i + batchSize
		if end > len(eventIDs) {
			end = len(eventIDs)
		}
		batch := eventIDs[i:end]

		semaphore <- struct{}{}
		wg.Add(1)

		go func(eventIDBatch []int64, batchNum int) {
			defer func() {
				<-semaphore
				wg.Done()
			}()

			// Add small delay between batches to reduce Elasticsearch pressure
			if batchNum > 0 {
				time.Sleep(100 * time.Millisecond)
			}

			// Add retry logic for failed batches
			var batchResults map[int64]map[string]interface{}
			maxRetries := 3 //increased retries
			for retry := 0; retry <= maxRetries; retry++ {
				batchResults = fetchAllEventElasticsearchBatch(esClient, indexName, eventIDBatch)
				if len(batchResults) > 0 || retry == maxRetries {
					if retry > 0 {
						log.Printf("Elasticsearch batch %d: Success after %d retries, got %d results", batchNum, retry, len(batchResults))
					}
					break
				}
				if retry < maxRetries {
					backoffTime := time.Duration(retry+1) * 3 * time.Second // Increased backoff time
					log.Printf("Elasticsearch batch %d: Retry %d/%d after %v backoff", batchNum, retry+1, maxRetries, backoffTime)
					time.Sleep(backoffTime)
				}
			}
			resultsChan <- batchResults
		}(batch, i/batchSize)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	completedBatches := 0

collectLoop:
	for completedBatches < expectedBatches {
		select {
		case batchResults := <-resultsChan:
			allResults = append(allResults, batchResults)
			completedBatches++
		case <-done:
			break collectLoop
		case <-time.After(120 * time.Second):
			log.Printf("Warning: Timeout waiting for Elasticsearch data. Completed %d/%d batches",
				completedBatches, expectedBatches)
			break collectLoop
		}
	}

	// Merge all batch results
	for _, batchResult := range allResults {
		for eventID, data := range batchResult {
			results[eventID] = data
		}
	}

	log.Printf("OK: Retrieved Elasticsearch data for %d events in %d batches", len(results), len(allResults))
	return results
}

// inserts all event data into allevent_ch
func insertallEventDataIntoClickHouse(clickhouseConn driver.Conn, records []map[string]interface{}, numWorkers int, config shared.Config) error {
	if len(records) == 0 {
		return nil
	}

	if numWorkers <= 1 {
		return insertallEventDataSingleWorker(clickhouseConn, records, config)
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
			err := insertallEventDataSingleWorker(clickhouseConn, batch, config)
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

func insertallEventDataSingleWorker(clickhouseConn driver.Conn, records []map[string]interface{}, config shared.Config) error {
	if len(records) == 0 {
		return nil
	}

	const maxBatchSize = 5000
	if len(records) > maxBatchSize {
		for i := 0; i < len(records); i += maxBatchSize {
			end := i + maxBatchSize
			if end > len(records) {
				end = len(records)
			}
			chunk := records[i:end]
			log.Printf("Inserting chunk %d-%d (%d records)", i+1, end, len(chunk))
			if err := insertallEventDataChunk(clickhouseConn, chunk, config); err != nil {
				return fmt.Errorf("failed to insert chunk %d-%d: %v", i+1, end, err)
			}
		}
		return nil
	}

	return insertallEventDataChunk(clickhouseConn, records, config)
}

func insertallEventDataChunk(clickhouseConn driver.Conn, records []map[string]interface{}, config shared.Config) error {
	if len(records) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 900*time.Second) // 15 minutes
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
			event_pricing, event_logo, event_estimatedVisitors, event_frequency, inboundScore, internationalScore, repeatSentimentChangePercentage, audienceZone,
			event_economic_FoodAndBevarage, event_economic_Transportation, event_economic_Accomodation, event_economic_Utilities, event_economic_flights, event_economic_value,
			event_economic_dayWiseEconomicImpact, event_economic_breakdown, event_economic_impact, version
		)
	`

	batch, err := clickhouseConn.PrepareBatch(ctx, insertSQL)

	maxRetries := 3
	for retryCount := 0; err != nil && retryCount < maxRetries; retryCount++ {
		log.Printf("WARNING: ClickHouse connection error (attempt %d/%d), rebuilding connection: %v", retryCount+1, maxRetries, err)
		newConn, connErr := utils.SetupNativeClickHouseConnection(config)
		if connErr != nil {
			log.Printf("ERROR: Failed to rebuild ClickHouse connection (attempt %d/%d): %v", retryCount+1, maxRetries, connErr)
			if retryCount < maxRetries-1 {
				continue
			}
			return fmt.Errorf("failed to prepare batch and rebuild connection after %d attempts: %v, %v", maxRetries, err, connErr)
		}

		clickhouseConn = newConn
		batch, err = clickhouseConn.PrepareBatch(ctx, insertSQL)
		if err != nil {
			log.Printf("ERROR: Failed to prepare batch after rebuilding connection (attempt %d/%d): %v", retryCount+1, maxRetries, err)
			continue
		}
		log.Printf("Successfully rebuilt ClickHouse connection and prepared batch after %d attempt(s)", retryCount+1)
		break
	}

	if err != nil {
		return fmt.Errorf("failed to prepare ClickHouse batch after %d retries: %v", maxRetries, err)
	}

	for _, record := range records {
		allEventRecord := convertToallEventRecord(record)

		err := batch.Append(
			allEventRecord.EventID,                         // event_id: UInt32 NOT NULL
			allEventRecord.EventUUID,                       // event_uuid: UUID NOT NULL
			allEventRecord.EventName,                       // event_name: String NOT NULL
			allEventRecord.EventAbbrName,                   // event_abbr_name: Nullable(String)
			allEventRecord.EventDescription,                // event_description: Nullable(String)
			allEventRecord.EventPunchline,                  // event_punchline: Nullable(String)
			allEventRecord.EventAvgRating,                  // event_avgRating: Nullable(Decimal(3,2))
			allEventRecord.StartDate,                       // start_date: Date NOT NULL
			allEventRecord.EndDate,                         // end_date: Date NOT NULL
			allEventRecord.EditionID,                       // edition_id: UInt32 NOT NULL
			allEventRecord.EditionCountry,                  // edition_country: LowCardinality(FixedString(2)) NOT NULL
			allEventRecord.EditionCity,                     // edition_city: UInt32 NOT NULL
			allEventRecord.EditionCityName,                 // edition_city_name: String NOT NULL
			allEventRecord.EditionCityStateID,              // edition_city_state_id: Nullable(UInt32)
			allEventRecord.EditionCityState,                // edition_city_state: LowCardinality(String) NOT NULL
			allEventRecord.EditionCityLat,                  // edition_city_lat: Float64 NOT NULL
			allEventRecord.EditionCityLong,                 // edition_city_long: Float64 NOT NULL
			allEventRecord.CompanyID,                       // company_id: Nullable(UInt32)
			allEventRecord.CompanyName,                     // company_name: Nullable(String)
			allEventRecord.CompanyDomain,                   // company_domain: Nullable(String)
			allEventRecord.CompanyWebsite,                  // company_website: Nullable(String)
			allEventRecord.CompanyCountry,                  // company_country: LowCardinality(Nullable(FixedString(2)))
			allEventRecord.CompanyState,                    // company_state: LowCardinality(Nullable(String))
			allEventRecord.CompanyCity,                     // company_city: Nullable(UInt32)
			allEventRecord.CompanyCityName,                 // company_city_name: Nullable(String)
			allEventRecord.VenueID,                         // venue_id: Nullable(UInt32)
			allEventRecord.VenueName,                       // venue_name: Nullable(String)
			allEventRecord.VenueCountry,                    // venue_country: LowCardinality(Nullable(FixedString(2)))
			allEventRecord.VenueCity,                       // venue_city: Nullable(UInt32)
			allEventRecord.VenueCityName,                   // venue_city_name: Nullable(String)
			allEventRecord.VenueLat,                        // venue_lat: Nullable(Float64)
			allEventRecord.VenueLong,                       // venue_long: Nullable(Float64)
			allEventRecord.Published,                       // published: Int8 NOT NULL
			allEventRecord.Status,                          // status: LowCardinality(FixedString(1)) NOT NULL DEFAULT 'A'
			allEventRecord.EditionsAudianceType,            // editions_audiance_type: UInt16 NOT NULL
			allEventRecord.EditionFunctionality,            // edition_functionality: LowCardinality(String) NOT NULL
			allEventRecord.EditionWebsite,                  // edition_website: Nullable(String)
			allEventRecord.EditionDomain,                   // edition_domain: Nullable(String)
			allEventRecord.EditionType,                     // edition_type: LowCardinality(Nullable(String)) DEFAULT 'NA'
			allEventRecord.EventFollowers,                  // event_followers: Nullable(UInt32)
			allEventRecord.EditionFollowers,                // edition_followers: Nullable(UInt32)
			allEventRecord.EventExhibitor,                  // event_exhibitor: Nullable(UInt32)
			allEventRecord.EditionExhibitor,                // edition_exhibitor: Nullable(UInt32)
			allEventRecord.ExhibitorsUpperBound,            // exhibitors_upper_bound: Nullable(UInt32)
			allEventRecord.ExhibitorsLowerBound,            // exhibitors_lower_bound: Nullable(UInt32)
			allEventRecord.ExhibitorsMean,                  // exhibitors_mean: Nullable(UInt32)
			allEventRecord.EventSponsor,                    // event_sponsor: Nullable(UInt32)
			allEventRecord.EditionSponsor,                  // edition_sponsor: Nullable(UInt32)
			allEventRecord.EventSpeaker,                    // event_speaker: Nullable(UInt32)
			allEventRecord.EditionSpeaker,                  // edition_speaker: Nullable(UInt32)
			allEventRecord.EventCreated,                    // event_created: DateTime NOT NULL
			allEventRecord.EditionCreated,                  // edition_created: DateTime NOT NULL
			allEventRecord.EventHybrid,                     // event_hybrid: Nullable(UInt32)
			allEventRecord.IsBranded,                       // isBranded: Nullable(UInt32)
			allEventRecord.Maturity,                        // maturity: LowCardinality(Nullable(String))
			allEventRecord.EventPricing,                    // event_pricing: LowCardinality(Nullable(String))
			allEventRecord.EventLogo,                       // event_logo: Nullable(String)
			allEventRecord.EventEstimatedVisitors,          // event_estimatedVisitors: LowCardinality(Nullable(String))
			allEventRecord.EventFrequency,                  // event_frequency: LowCardinality(Nullable(String))
			allEventRecord.InboundScore,                    // inboundScore: Nullable(UInt32)
			allEventRecord.InternationalScore,              // internationalScore: Nullable(UInt32)
			allEventRecord.RepeatSentimentChangePercentage, // repeatSentimentChangePercentage: Nullable(Float64)
			allEventRecord.AudienceZone,                    // audienceZone: LowCardinality(Nullable(String))
			allEventRecord.EventEconomicFoodAndBevarage,    // event_economic_FoodAndBevarage: Nullable(Float64)
			allEventRecord.EventEconomicTransportation,     // event_economic_Transportation: Nullable(Float64)
			allEventRecord.EventEconomicAccomodation,       // event_economic_Accomodation: Nullable(Float64)
			allEventRecord.EventEconomicUtilities,          // event_economic_Utilities: Nullable(Float64)
			allEventRecord.EventEconomicFlights,            // event_economic_flights: Nullable(Float64)
			allEventRecord.EventEconomicValue,              // event_economic_value: Nullable(Float64)
			allEventRecord.EventEconomicDayWiseImpact,      // event_economic_dayWiseEconomicImpact: JSON
			allEventRecord.EventEconomicBreakdown,          // event_economic_breakdown: JSON
			allEventRecord.EventEconomicImpact,             // event_economic_impact: JSON
			allEventRecord.Version,                         // version: UInt32 NOT NULL DEFAULT 1
		)
		if err != nil {
			log.Printf("ERROR: Failed to append record to batch: %v", err)
			log.Printf("Record data: EventID=%d, EventName=%s, EventAvgRating=%v",
				allEventRecord.EventID, allEventRecord.EventName, allEventRecord.EventAvgRating)
			return fmt.Errorf("failed to append record to batch: %v", err)
		}
	}

	if err := batch.Send(); err != nil {
		log.Printf("ERROR: Failed to send ClickHouse batch: %v", err)
		return fmt.Errorf("failed to send ClickHouse batch: %v", err)
	}

	log.Printf("OK: Successfully inserted %d all event records", len(records))
	return nil
}
