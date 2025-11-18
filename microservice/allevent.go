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

// converts a map to alleventRecord struct
func convertToalleventRecord(record map[string]interface{}) alleventRecord {
	return alleventRecord{
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
		EventUpdated:         shared.SafeConvertToDateTimeString(record["event_updated"]),
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
		ImpactScore:                     shared.SafeConvertToNullableUInt32(record["impactScore"]),
		InboundScore:                    shared.SafeConvertToNullableUInt32(record["inboundScore"]),
		InternationalScore:              shared.SafeConvertToNullableUInt32(record["internationalScore"]),
		RepeatSentimentChangePercentage: shared.SafeConvertToNullableFloat64(record["repeatSentimentChangePercentage"]),
		AudienceZone:                    shared.SafeConvertToNullableString(record["audienceZone"]),
		InboundPercentage:               shared.SafeConvertToUInt32(record["inboundPercentage"]),
		InboundAttendance:               shared.SafeConvertToUInt32(record["inboundAttendance"]),
		InternationalPercentage:         shared.SafeConvertToUInt32(record["internationalPercentage"]),
		InternationalAttendance:         shared.SafeConvertToUInt32(record["internationalAttendance"]),
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
		Keywords:                        shared.ConvertToStringArray(record["keywords"]),
		Tickets:                         shared.ConvertToStringArray(record["tickets"]),
		EventScore:                      shared.SafeConvertToNullableInt32(record["event_score"]),
		LastUpdatedAt:                   shared.SafeConvertToDateTimeString(record["last_updated_at"]),
		Version:                         shared.SafeConvertToUInt32(record["version"]),
	}
}

type alleventRecord struct {
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
	EventUpdated                    string   `ch:"event_updated"`                        // DateTime NOT NULL
	EditionCreated                  string   `ch:"edition_created"`                      // DateTime NOT NULL
	EventHybrid                     *uint8   `ch:"event_hybrid"`                         // Nullable(UInt8)
	IsBranded                       *uint32  `ch:"isBranded"`                            // Nullable(UInt32)
	Maturity                        *string  `ch:"maturity"`                             // LowCardinality(Nullable(String))
	EventPricing                    *string  `ch:"event_pricing"`                        // LowCardinality(Nullable(String))
	EventLogo                       *string  `ch:"event_logo"`                           // Nullable(String)
	EventEstimatedVisitors          *string  `ch:"event_estimatedVisitors"`              // LowCardinality(Nullable(String))
	EventFrequency                  *string  `ch:"event_frequency"`                      // LowCardinality(Nullable(String))
	ImpactScore                     *uint32  `ch:"impactScore"`                          // Nullable(UInt32)
	InboundScore                    *uint32  `ch:"inboundScore"`                         // Nullable(UInt32)
	InternationalScore              *uint32  `ch:"internationalScore"`                   // Nullable(UInt32)
	RepeatSentimentChangePercentage *float64 `ch:"repeatSentimentChangePercentage"`      // Nullable(Float64)
	AudienceZone                    *string  `ch:"audienceZone"`                         // LowCardinality(Nullable(String))
	InboundPercentage               uint32   `ch:"inboundPercentage"`                    // UInt32 NOT NULL
	InboundAttendance               uint32   `ch:"inboundAttendance"`                    // UInt32 NOT NULL
	InternationalPercentage         uint32   `ch:"internationalPercentage"`              // UInt32 NOT NULL
	InternationalAttendance         uint32   `ch:"internationalAttendance"`              // UInt32 NOT NULL
	EventEconomicFoodAndBevarage    *float64 `ch:"event_economic_FoodAndBevarage"`       // Nullable(Float64)
	EventEconomicTransportation     *float64 `ch:"event_economic_Transportation"`        // Nullable(Float64)
	EventEconomicAccomodation       *float64 `ch:"event_economic_Accomodation"`          // Nullable(Float64)
	EventEconomicUtilities          *float64 `ch:"event_economic_Utilities"`             // Nullable(Float64)
	EventEconomicFlights            *float64 `ch:"event_economic_flights"`               // Nullable(Float64)
	EventEconomicValue              *float64 `ch:"event_economic_value"`                 // Nullable(Float64)
	EventEconomicDayWiseImpact      string   `ch:"event_economic_dayWiseEconomicImpact"` // JSON
	EventEconomicBreakdown          string   `ch:"event_economic_breakdown"`             // JSON
	EventEconomicImpact             string   `ch:"event_economic_impact"`                // JSON
	Keywords                        []string `ch:"keywords"`                             // Array(String)
	Tickets                         []string `ch:"tickets"`                              // Array(String)
	EventScore                      *int32   `ch:"event_score"`                          // Nullable(Int32)
	LastUpdatedAt                   string   `ch:"last_updated_at"`                      // DateTime NOT NULL
	Version                         uint32   `ch:"version"`
}

func buildalleventMigrationData(db *sql.DB, table string, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
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

func extractalleventVenueIDs(editionData []map[string]interface{}) []int64 {
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

func fetchalleventVenueDataParallel(db *sql.DB, venueIDs []int64) []map[string]interface{} {
	if len(venueIDs) == 0 {
		return nil
	}

	batchSize := 1000
	var allVenueData []map[string]interface{}

	for i := 0; i < len(venueIDs); i += batchSize {
		end := i + batchSize
		if end > len(venueIDs) {
			end = len(venueIDs)
		}

		batch := venueIDs[i:end]
		venueData := fetchalleventVenueDataForBatch(db, batch)
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

func fetchalleventVenueDataForBatch(db *sql.DB, venueIDs []int64) []map[string]interface{} {
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
				if col == "venue_lat" || col == "venue_long" {
					if bytes, ok := val.([]byte); ok {
						if len(bytes) > 0 {
							str := string(bytes)
							if str != "" {
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

func extractalleventCompanyIDs(editionData []map[string]interface{}) []int64 {
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

func fetchalleventCompanyDataParallel(db *sql.DB, companyIDs []int64) []map[string]interface{} {
	if len(companyIDs) == 0 {
		return nil
	}

	batchSize := 1000
	var allCompanyData []map[string]interface{}

	for i := 0; i < len(companyIDs); i += batchSize {
		end := i + batchSize
		if end > len(companyIDs) {
			end = len(companyIDs)
		}

		batch := companyIDs[i:end]
		companyData := fetchalleventCompanyDataForBatch(db, batch)
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

func fetchalleventCompanyDataForBatch(db *sql.DB, companyIDs []int64) []map[string]interface{} {
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

func extractalleventIDs(batchData []map[string]interface{}) []int64 {
	var eventIDs []int64
	for _, row := range batchData {
		if id, ok := row["id"].(int64); ok {
			eventIDs = append(eventIDs, id)
		}
	}
	return eventIDs
}

func fetchallalleventDataParallel(db *sql.DB, eventIDs []int64) []map[string]interface{} {
	if len(eventIDs) == 0 {
		return nil
	}

	batchSize := 1000
	var allEditionData []map[string]interface{}

	for i := 0; i < len(eventIDs); i += batchSize {
		end := i + batchSize
		if end > len(eventIDs) {
			end = len(eventIDs)
		}

		batch := eventIDs[i:end]
		editionData := fetchallalleventDataForBatch(db, batch)
		allEditionData = append(allEditionData, editionData...)
	}

	return allEditionData
}

func fetchallalleventDataForBatch(db *sql.DB, eventIDs []int64) []map[string]interface{} {
	if len(eventIDs) == 0 {
		return nil
	}

	placeholders := make([]string, len(eventIDs))
	args := make([]interface{}, len(eventIDs))
	for i, id := range eventIDs {
		placeholders[i] = "?"
		args[i] = id
	}

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

func fetchalleventEventDataForBatch(db *sql.DB, eventIDs []int64) []map[string]interface{} {
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
		       country, published, status, event_audience, functionality, brand_id, created, modified, event_type, score 
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

type estimateData struct {
	EconomicImpact          string
	InboundPercentage       *uint32
	InboundAttendance       *uint32
	InternationalPercentage *uint32
	InternationalAttendance *uint32
}

func fetchalleventEstimateDataForBatch(db *sql.DB, eventIDs []int64) map[int64]estimateData {
	if len(eventIDs) == 0 {
		return nil
	}

	placeholders := make([]string, len(eventIDs))
	args := make([]interface{}, len(eventIDs))
	for i, id := range eventIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(`SELECT event_id, economic_impact, inbound_per, inbound_attendees, international_per, international_attendees FROM estimate WHERE event_id IN (%s)`, strings.Join(placeholders, ","))

	rows, err := db.Query(query, args...)
	if err != nil {
		log.Printf("Error fetching batch estimate data: %v", err)
		return nil
	}
	defer rows.Close()

	result := make(map[int64]estimateData)
	for rows.Next() {
		var eventID int64
		var economicImpact sql.NullString
		var inboundPer sql.NullInt64
		var inboundAttendees sql.NullInt64
		var internationalPer sql.NullInt64
		var internationalAttendees sql.NullInt64

		if err := rows.Scan(&eventID, &economicImpact, &inboundPer, &inboundAttendees, &internationalPer, &internationalAttendees); err != nil {
			continue
		}

		estimate := estimateData{}
		if economicImpact.Valid {
			estimate.EconomicImpact = economicImpact.String
		}

		if inboundPer.Valid && inboundPer.Int64 >= 0 {
			val := uint32(inboundPer.Int64)
			estimate.InboundPercentage = &val
		}

		if inboundAttendees.Valid && inboundAttendees.Int64 >= 0 {
			val := uint32(inboundAttendees.Int64)
			estimate.InboundAttendance = &val
		}

		if internationalPer.Valid && internationalPer.Int64 >= 0 {
			val := uint32(internationalPer.Int64)
			estimate.InternationalPercentage = &val
		}

		if internationalAttendees.Valid && internationalAttendees.Int64 >= 0 {
			val := uint32(internationalAttendees.Int64)
			estimate.InternationalAttendance = &val
		}

		result[eventID] = estimate
	}

	return result
}

func processalleventEconomicImpactDataParallel(estimateDataMap map[int64]estimateData) map[int64]map[string]interface{} {
	if len(estimateDataMap) == 0 {
		return nil
	}

	finalResult := make(map[int64]map[string]interface{})
	for eventID, estimate := range estimateDataMap {
		result := processalleventSingleEconomicImpact(eventID, estimate.EconomicImpact)
		for id, data := range result {
			data["inboundPercentage"] = estimate.InboundPercentage
			data["inboundAttendance"] = estimate.InboundAttendance
			data["internationalPercentage"] = estimate.InternationalPercentage
			data["internationalAttendance"] = estimate.InternationalAttendance
			finalResult[id] = data
		}
	}

	return finalResult
}

func processalleventSingleEconomicImpact(eventID int64, economicImpact string) map[int64]map[string]interface{} {
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
	total, totalBreakdown, dayWiseFormatted := formatalleventEconomicImpact(eventID, economicImpact)

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

func formatalleventEconomicImpact(_ int64, economicImpact string) (interface{}, interface{}, interface{}) {
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
	log.Println("=== Starting allevent ONLY Processing ===")

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

	log.Printf("Processing allevent data in %d chunks with chunk size: %d", config.NumChunks, chunkSize)

	// Global deduplication map - shared across all chunks
	// Using uint64 keys (eventID << 32 | editionID) instead of strings for memory efficiency
	globalUniqueRecords := make(map[uint64]bool)
	var globalMutex sync.RWMutex

	// Global counters for tracking total records processed
	var totalRecordsProcessed int64
	var totalRecordsSkipped int64
	var totalRecordsInserted int64
	var globalCountMutex sync.Mutex

	results := make(chan string, config.NumChunks)
	semaphore := make(chan struct{}, config.NumWorkers)

	for i := 0; i < config.NumChunks; i++ {
		startID := minID + (i * chunkSize)
		endID := startID + chunkSize - 1

		// last chunk to include remaining records
		if i == config.NumChunks-1 {
			endID = maxID
		}

		// delay between chunk launches
		if i > 0 {
			delay := 3 * time.Second
			log.Printf("Waiting %v before launching allevent chunk %d...", delay, i+1)
			time.Sleep(delay)
		}

		semaphore <- struct{}{}
		go func(chunkNum, start, end int) {
			defer func() { <-semaphore }()
			processalleventChunk(mysqlDB, clickhouseConn, esClient, config, start, end, chunkNum, results, globalUniqueRecords, &globalMutex, &totalRecordsProcessed, &totalRecordsSkipped, &totalRecordsInserted, &globalCountMutex)
		}(i+1, startID, endID)
	}

	for i := 0; i < config.NumChunks; i++ {
		result := <-results
		log.Printf("allevent Result: %s", result)
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

	log.Println("allevent processing completed!")
}

// processes a single chunk of allevent data
func processalleventChunk(mysqlDB *sql.DB, clickhouseConn driver.Conn, esClient *elasticsearch.Client, config shared.Config, startID, endID int, chunkNum int, results chan<- string, globalUniqueRecords map[uint64]bool, globalMutex *sync.RWMutex, totalRecordsProcessed *int64, totalRecordsSkipped *int64, totalRecordsInserted *int64, globalCountMutex *sync.Mutex) {
	log.Printf("Processing allevent chunk %d: ID range %d-%d", chunkNum, startID, endID)

	totalRecords := endID - startID + 1
	processed := 0

	offset := 0
	for {
		batchData, err := buildalleventMigrationData(mysqlDB, "event", startID, endID, config.BatchSize)
		if err != nil {
			log.Printf("allevent chunk %d batch error: %v", chunkNum, err)
			results <- fmt.Sprintf("allevent chunk %d batch error: %v", chunkNum, err)
			return
		}

		if len(batchData) == 0 {
			break
		}

		processed += len(batchData)
		progress := float64(processed) / float64(totalRecords) * 100
		log.Printf("allevent chunk %d: Retrieved %d records in batch (%.1f%% complete)", chunkNum, len(batchData), progress)

		eventIDs := extractalleventIDs(batchData)
		if len(eventIDs) > 0 {
			log.Printf("allevent chunk %d: Fetching edition data for %d events", chunkNum, len(eventIDs))

			// Fetch edition data in parallel
			startTime := time.Now()
			editionData := fetchallalleventDataParallel(mysqlDB, eventIDs)
			editionTime := time.Since(startTime)
			log.Printf("allevent chunk %d: Retrieved edition data for %d events in %v", chunkNum, len(editionData), editionTime)

			var companyData []map[string]interface{}
			if len(editionData) > 0 {
				companyIDs := extractalleventCompanyIDs(editionData)
				if len(companyIDs) > 0 {
					log.Printf("allevent chunk %d: Fetching company data for %d companies", chunkNum, len(companyIDs))
					startTime = time.Now()
					companyData = fetchalleventCompanyDataParallel(mysqlDB, companyIDs)
					companyTime := time.Since(startTime)
					log.Printf("allevent chunk %d: Retrieved company data for %d companies in %v", chunkNum, len(companyData), companyTime)
				}
			}

			// Fetch venue data for all editions
			var venueData []map[string]interface{}
			if len(editionData) > 0 {
				venueIDs := extractalleventVenueIDs(editionData)
				if len(venueIDs) > 0 {
					log.Printf("allevent chunk %d: Fetching venue data for %d venues", chunkNum, len(venueIDs))
					startTime = time.Now()
					venueData = fetchalleventVenueDataParallel(mysqlDB, venueIDs)
					venueTime := time.Since(startTime)
					log.Printf("allevent chunk %d: Retrieved venue data for %d venues in %v", chunkNum, len(venueData), venueTime)
				}
			}

			var cityData []map[string]interface{}
			if len(editionData) > 0 {
				editionCityIDs := extractalleventCityIDs(editionData)

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
					log.Printf("allevent chunk %d: Fetching city data for %d cities (edition: %d, company: %d, venue: %d)",
						chunkNum, len(allCityIDs), len(editionCityIDs), len(companyCityIDs), len(venueCityIDs))
					startTime = time.Now()
					cityData = shared.FetchCityDataParallel(mysqlDB, allCityIDs, config.NumWorkers)
					cityTime := time.Since(startTime)
					log.Printf("allevent chunk %d: Retrieved city data for %d cities in %v", chunkNum, len(cityData), cityTime)
				}
			}

			var esData map[int64]map[string]interface{}
			if len(editionData) > 0 {
				log.Printf("allevent chunk %d: Fetching Elasticsearch data for %d events in batches of 200", chunkNum, len(eventIDs))
				startTime = time.Now()
				esData = fetchalleventElasticsearchDataForEvents(esClient, config.ElasticsearchIndex, eventIDs)
				esTime := time.Since(startTime)
				log.Printf("allevent chunk %d: Retrieved Elasticsearch data for %d events in %v", chunkNum, len(esData), esTime)
			}

			if len(editionData) > 0 {
				log.Printf("allevent chunk %d: Building location_ch lookups for cities and states", chunkNum)
				startTime := time.Now()
				cityIDLookup, err := buildalleventCityIDLookupFromLocationCh(clickhouseConn)
				if err != nil {
					log.Printf("allevent chunk %d: WARNING - Failed to build city ID lookup: %v", chunkNum, err)
					cityIDLookup = make(map[string]uint32) // Empty lookup on error
				}
				stateIDLookup, err := buildalleventStateIDLookupFromLocationCh(clickhouseConn)
				if err != nil {
					log.Printf("allevent chunk %d: WARNING - Failed to build state ID lookup: %v", chunkNum, err)
					stateIDLookup = make(map[string]uint32) // Empty lookup on error
				}
				lookupTime := time.Since(startTime)
				log.Printf("allevent chunk %d: Built location_ch lookups in %v (cities: %d, states: %d)", chunkNum, lookupTime, len(cityIDLookup), len(stateIDLookup))

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

				allevents := make(map[int64][]map[string]interface{})
				currentEditionStartDates := make(map[int64]interface{})
				currentEditionIDs := make(map[int64]int64)

				editionsProcessed := 0
				editionsSkipped := 0
				for _, edition := range editionData {
					if eventID, ok := edition["event"].(int64); ok {
						allevents[eventID] = append(allevents[eventID], edition)
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
						log.Printf("allevent chunk %d: Skipping edition - invalid event ID: %v", chunkNum, edition["event"])
					}
				}
				log.Printf("allevent chunk %d: Editions processed: %d, skipped: %d", chunkNum, editionsProcessed, editionsSkipped)

				var clickHouseRecords []map[string]interface{}
				completeCount := 0
				partialCount := 0
				skippedCount := 0

				eventIDsForEditions := make([]int64, 0, len(allevents))
				for eventID := range allevents {
					eventIDsForEditions = append(eventIDsForEditions, eventID)
				}

				eventDataLookup := make(map[int64]map[string]interface{})
				if len(eventIDsForEditions) > 0 {
					log.Printf("allevent chunk %d: Fetching event data for %d events with editions", chunkNum, len(eventIDsForEditions))
					eventDataForEditions := fetchalleventEventDataForBatch(mysqlDB, eventIDsForEditions)
					for _, eventData := range eventDataForEditions {
						if eventID, ok := eventData["id"].(int64); ok {
							eventDataLookup[eventID] = eventData
						}
					}
				}

				estimateDataMap := make(map[int64]estimateData)
				if len(eventIDsForEditions) > 0 {
					estimateDataMap = fetchalleventEstimateDataForBatch(mysqlDB, eventIDsForEditions)
				}

				processedEconomicData := make(map[int64]map[string]interface{})
				if len(estimateDataMap) > 0 {
					processedEconomicData = processalleventEconomicImpactDataParallel(estimateDataMap)
				}

				// Fetch category names for events
				categoryNamesMap := make(map[int64][]string)
				if len(eventIDsForEditions) > 0 {
					log.Printf("allevent chunk %d: Fetching category names for %d events", chunkNum, len(eventIDsForEditions))
					startTime = time.Now()
					categoryNamesMap = fetchalleventCategoryNamesForEvents(mysqlDB, eventIDsForEditions)
					categoryTime := time.Since(startTime)
					log.Printf("allevent chunk %d: Retrieved category names for %d events in %v", chunkNum, len(categoryNamesMap), categoryTime)
				}

				// Fetch ticket data for events
				ticketDataMap := make(map[int64][]string)
				ticketTypeMap := make(map[int64]string)
				if len(eventIDsForEditions) > 0 {
					log.Printf("allevent chunk %d: Fetching ticket data for %d events", chunkNum, len(eventIDsForEditions))
					startTime = time.Now()
					rawTicketData := fetchalleventTicketDataForBatch(mysqlDB, eventIDsForEditions)
					if len(rawTicketData) > 0 {
						ticketDataMap = processalleventTicketData(rawTicketData)
						for _, ticket := range rawTicketData {
							eventID, ok := ticket["event"].(int64)
							if !ok {
								continue
							}
							if _, exists := ticketTypeMap[eventID]; !exists {
								ticketType := shared.SafeConvertToString(ticket["type"])
								if ticketType != "" {
									ticketTypeMap[eventID] = ticketType
								}
							}
						}
					}
					ticketTime := time.Since(startTime)
					log.Printf("allevent chunk %d: Retrieved ticket data for %d events in %v", chunkNum, len(ticketDataMap), ticketTime)
				}

				for eventID, editions := range allevents {
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
								editionDomain = shared.ExtractDomainFromWebsite(editionWebsite)
							}

							// Extract domain from company website
							var companyDomain string
							if company != nil && company["company_website"] != nil {
								companyDomain = shared.ExtractDomainFromWebsite(company["company_website"])
							}

							// Determine edition type using simplified logic
							editionType := determinealleventType(
								edition["edition_start_date"],
								currentEditionStartDates[eventID],
								edition["edition_id"].(int64),
								currentEditionIDs[eventID],
							)

							// Create unique key for deduplication (event_id + edition_id)
							// Using uint64 key (eventID << 32 | editionID) for memory efficiency
							eventIDUint32 := shared.ConvertToUInt32(eventData["id"])
							editionIDUint32 := shared.ConvertToUInt32(edition["edition_id"])
							uniqueKey := uint64(eventIDUint32)<<32 | uint64(editionIDUint32)

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

							// Get location_ch.id for edition city (query by name + country ISO)
							var editionCityLocationChID *uint32
							editionCountryISO := strings.ToUpper(shared.ConvertToString(eventData["country"]))
							if city != nil && city["name"] != nil {
								cityName := shared.ConvertToString(city["name"])
								if cityName != "" {
									cityNameStr := strings.TrimSpace(cityName)
									// First try with country ISO
									if editionCountryISO != "" && editionCountryISO != "NAN" {
										cityKeyWithISO := fmt.Sprintf("%s|%s", cityNameStr, editionCountryISO)
										if locationChID, exists := cityIDLookup[cityKeyWithISO]; exists {
											editionCityLocationChID = &locationChID
										}
									}
									// If not found with ISO, try without ISO as fallback
									if editionCityLocationChID == nil {
										cityKeyWithoutISO := cityNameStr
										if locationChID, exists := cityIDLookup[cityKeyWithoutISO]; exists {
											editionCityLocationChID = &locationChID
										}
									}
								}
							}

							// Get location_ch.id for company city (query by name + country ISO)
							var companyCityLocationChID *uint32
							if companyCity != nil && companyCity["name"] != nil {
								companyCityName := shared.ConvertToString(companyCity["name"])
								companyCountryISO := strings.ToUpper(shared.ConvertToString(company["company_country"]))
								if companyCityName != "" {
									companyCityNameStr := strings.TrimSpace(companyCityName)
									// First try with country ISO
									if companyCountryISO != "" && companyCountryISO != "NAN" {
										cityKeyWithISO := fmt.Sprintf("%s|%s", companyCityNameStr, companyCountryISO)
										if locationChID, exists := cityIDLookup[cityKeyWithISO]; exists {
											companyCityLocationChID = &locationChID
										}
									}
									// If not found with ISO, try without ISO as fallback
									if companyCityLocationChID == nil {
										cityKeyWithoutISO := companyCityNameStr
										if locationChID, exists := cityIDLookup[cityKeyWithoutISO]; exists {
											companyCityLocationChID = &locationChID
										}
									}
								}
							}

							// Get location_ch.id for venue city (query by name + country ISO)
							var venueCityLocationChID *uint32
							if venueCity != nil && venueCity["name"] != nil {
								venueCityName := shared.ConvertToString(venueCity["name"])
								venueCountryISO := strings.ToUpper(shared.ConvertToString(venue["venue_country"]))
								if venueCityName != "" {
									venueCityNameStr := strings.TrimSpace(venueCityName)
									// First try with country ISO
									if venueCountryISO != "" && venueCountryISO != "NAN" {
										cityKeyWithISO := fmt.Sprintf("%s|%s", venueCityNameStr, venueCountryISO)
										if locationChID, exists := cityIDLookup[cityKeyWithISO]; exists {
											venueCityLocationChID = &locationChID
										}
									}
									// If not found with ISO, try without ISO as fallback
									if venueCityLocationChID == nil {
										cityKeyWithoutISO := venueCityNameStr
										if locationChID, exists := cityIDLookup[cityKeyWithoutISO]; exists {
											venueCityLocationChID = &locationChID
										}
									}
								}
							}

							// Get location_ch.id for edition city state (query by name + country ISO)
							var editionCityStateLocationChID *uint32
							if city != nil && city["state"] != nil {
								stateName := shared.ConvertToString(city["state"])
								if stateName != "" {
									stateNameStr := strings.TrimSpace(stateName)
									// First try with country ISO
									if editionCountryISO != "" && editionCountryISO != "NAN" {
										stateKeyWithISO := fmt.Sprintf("%s|%s", stateNameStr, editionCountryISO)
										if locationChID, exists := stateIDLookup[stateKeyWithISO]; exists {
											editionCityStateLocationChID = &locationChID
										}
									}
									// If not found with ISO, try without ISO as fallback
									if editionCityStateLocationChID == nil {
										stateKeyWithoutISO := stateNameStr
										if locationChID, exists := stateIDLookup[stateKeyWithoutISO]; exists {
											editionCityStateLocationChID = &locationChID
										}
									}
								}
							}

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
								"edition_country":   editionCountryISO,
								"edition_city": func() interface{} {
									if editionCityLocationChID != nil {
										return uint32(*editionCityLocationChID)
									}
									return nil
								}(),
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
									if editionCityStateLocationChID != nil {
										return uint32(*editionCityStateLocationChID)
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
								"company_city": func() interface{} {
									if companyCityLocationChID != nil {
										return uint32(*companyCityLocationChID)
									}
									return nil
								}(),
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
								"venue_city": func() interface{} {
									if venueCityLocationChID != nil {
										return uint32(*venueCityLocationChID)
									}
									return nil
								}(),
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
								"event_updated":          eventData["modified"],
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
								"maturity": determinealleventMaturity(esInfoMap["total_edition"]),
								"event_pricing": func() *string {
									if ticketType, exists := ticketTypeMap[eventID]; exists && ticketType != "" {
										return &ticketType
									}
									return nil
								}(),
								"tickets": func() []string {
									if tickets, exists := ticketDataMap[eventID]; exists {
										return tickets
									}
									return []string{}
								}(),
								"event_logo":                      esInfoMap["event_logo"],
								"event_estimatedVisitors":         esInfoMap["eventEstimatedTag"],
								"event_frequency":                 esInfoMap["event_frequency"],
								"impactScore":                     esInfoMap["impactScore"],
								"inboundScore":                    esInfoMap["inboundScore"],
								"internationalScore":              esInfoMap["internationalScore"],
								"repeatSentimentChangePercentage": esInfoMap["repeatSentimentChangePercentage"],
								"audienceZone":                    esInfoMap["audienceZone"],
								"event_avgRating":                 esInfoMap["avg_rating"],
								"keywords":                        []string{},
								"event_score":                     eventData["score"],
								"last_updated_at":                 time.Now().Format("2006-01-02 15:04:05"),
								"version":                         1,
							}

							var currentEditionEventType interface{}

							if currentEditionIDs[eventID] == edition["edition_id"].(int64) {
								currentEditionEventType = eventData["event_type"]
								eventName := shared.ConvertToString(eventData["event_name"])
								eventAbbrName := shared.SafeConvertToNullableString(eventData["abbr_name"])
								eventDescription := shared.SafeConvertToNullableString(esInfoMap["event_description"])
								eventPunchline := shared.SafeConvertToNullableString(esInfoMap["event_punchline"])
								categoryNames := categoryNamesMap[eventID]

								keywords := extractalleventKeywords(eventName, eventAbbrName, eventDescription, eventPunchline, categoryNames)
								record["keywords"] = keywords

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

							var inboundPerVal, inboundAttVal, internationalPerVal, internationalAttVal uint32

							if economicData != nil {
								if inboundPer, ok := economicData["inboundPercentage"].(*uint32); ok && inboundPer != nil {
									inboundPerVal = *inboundPer
								} else if inboundPer, ok := economicData["inboundPercentage"].(uint32); ok {
									inboundPerVal = inboundPer
								}
								if inboundAtt, ok := economicData["inboundAttendance"].(*uint32); ok && inboundAtt != nil {
									inboundAttVal = *inboundAtt
								} else if inboundAtt, ok := economicData["inboundAttendance"].(uint32); ok {
									inboundAttVal = inboundAtt
								}
								if internationalPer, ok := economicData["internationalPercentage"].(*uint32); ok && internationalPer != nil {
									internationalPerVal = *internationalPer
								} else if internationalPer, ok := economicData["internationalPercentage"].(uint32); ok {
									internationalPerVal = internationalPer
								}
								if internationalAtt, ok := economicData["internationalAttendance"].(*uint32); ok && internationalAtt != nil {
									internationalAttVal = *internationalAtt
								} else if internationalAtt, ok := economicData["internationalAttendance"].(uint32); ok {
									internationalAttVal = internationalAtt
								}
							} else if estimate, exists := estimateDataMap[eventID]; exists {
								if estimate.InboundPercentage != nil {
									inboundPerVal = *estimate.InboundPercentage
								}
								if estimate.InboundAttendance != nil {
									inboundAttVal = *estimate.InboundAttendance
								}
								if estimate.InternationalPercentage != nil {
									internationalPerVal = *estimate.InternationalPercentage
								}
								if estimate.InternationalAttendance != nil {
									internationalAttVal = *estimate.InternationalAttendance
								}
							}

							record["inboundPercentage"] = inboundPerVal
							record["inboundAttendance"] = inboundAttVal
							record["internationalPercentage"] = internationalPerVal
							record["internationalAttendance"] = internationalAttVal

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
						log.Printf("allevent chunk %d: Skipping event %d - no event data found", chunkNum, eventID)
					}
				}

				// Count events with missing current editions
				eventsWithMissingCurrentEdition := 0
				for eventID := range allevents {
					if currentEditionIDs[eventID] == 0 {
						eventsWithMissingCurrentEdition++
					}
				}

				log.Printf("allevent chunk %d: Data completeness - Complete: %d, Partial: %d, Skipped: %d",
					chunkNum, completeCount, partialCount, skippedCount)
				if eventsWithMissingCurrentEdition > 0 {
					log.Printf("allevent chunk %d: Warning - %d events have no current edition (allevent is NULL)",
						chunkNum, eventsWithMissingCurrentEdition)
				}

				// Insert collected records into ClickHouse
				log.Printf("allevent chunk %d: Total records collected: %d", chunkNum, len(clickHouseRecords))
				if len(clickHouseRecords) > 0 {
					log.Printf("allevent chunk %d: Attempting to insert %d records into ClickHouse...", chunkNum, len(clickHouseRecords))

					insertErr := shared.RetryWithBackoff(
						func() error {
							return insertalleventDataIntoClickHouse(clickhouseConn, clickHouseRecords, config.ClickHouseWorkers, config)
						},
						3,
						fmt.Sprintf("ClickHouse insertion for chunk %d", chunkNum),
					)

					if insertErr != nil {
						log.Printf("allevent chunk %d: ClickHouse insertion failed after retries: %v", chunkNum, insertErr)
						log.Printf("allevent chunk %d: %d records failed to insert - consider manual retry", chunkNum, len(clickHouseRecords))
					} else {
						log.Printf("allevent chunk %d: Successfully inserted %d records into ClickHouse", chunkNum, len(clickHouseRecords))
					}
				} else {
					log.Printf("allevent chunk %d: No records to insert into ClickHouse", chunkNum)
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

	results <- fmt.Sprintf("allevent chunk %d completed successfully", chunkNum)
}

// 1. current_edition: The edition_id that matches event.allevent (only one per event)
// 2. future_edition: All editions with start_date > current_edition start_date
// 3. past_edition: All editions with start_date < current_edition start_date
func determinealleventType(editionStartDate, currentEditionStartDate interface{}, editionID, currentEditionID int64) *string {
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

// determineMaturity determines the maturity level based on total_edition count
// new: 1 edition, growing: 2-3 editions, established: 4-8 editions, flagship: 9+ editions
func determinealleventMaturity(totalEdition interface{}) *string {
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

func buildalleventCityIDLookupFromLocationCh(clickhouseConn driver.Conn) (map[string]uint32, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	query := `
		SELECT id, name, iso
		FROM location_ch
		WHERE location_type = 'CITY'
	`

	rows, err := clickhouseConn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query location_ch for cities: %v", err)
	}
	defer rows.Close()

	lookup := make(map[string]uint32)
	for rows.Next() {
		var locationChID uint32
		var cityName *string
		var countryISO *string
		if err := rows.Scan(&locationChID, &cityName, &countryISO); err != nil {
			log.Printf("Warning: Failed to scan city row: %v", err)
			continue
		}

		if cityName != nil && *cityName != "" {
			cityNameStr := strings.TrimSpace(*cityName)
			isoStr := ""
			if countryISO != nil && *countryISO != "" {
				isoStr = strings.ToUpper(strings.TrimSpace(*countryISO))
				if isoStr == "NAN" {
					isoStr = ""
				}
			}
			// Store both with and without ISO for flexible matching
			keyWithISO := fmt.Sprintf("%s|%s", cityNameStr, isoStr)
			keyWithoutISO := cityNameStr
			if isoStr != "" {
				lookup[keyWithISO] = locationChID
			}
			// Also store without ISO for fallback (only if not already exists to avoid overwriting)
			if _, exists := lookup[keyWithoutISO]; !exists {
				lookup[keyWithoutISO] = locationChID
			}
		}
	}

	log.Printf("Built city ID lookup: %d cities mapped from location_ch", len(lookup))
	return lookup, nil
}

func buildalleventStateIDLookupFromLocationCh(clickhouseConn driver.Conn) (map[string]uint32, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	query := `
		SELECT id, name, iso
		FROM location_ch
		WHERE location_type = 'STATE'
	`

	rows, err := clickhouseConn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query location_ch for states: %v", err)
	}
	defer rows.Close()

	lookup := make(map[string]uint32)
	for rows.Next() {
		var locationChID uint32
		var stateName *string
		var countryISO *string
		if err := rows.Scan(&locationChID, &stateName, &countryISO); err != nil {
			log.Printf("Warning: Failed to scan state row: %v", err)
			continue
		}

		if stateName != nil && *stateName != "" {
			stateNameStr := strings.TrimSpace(*stateName)
			isoStr := ""
			if countryISO != nil && *countryISO != "" {
				isoStr = strings.ToUpper(strings.TrimSpace(*countryISO))
				if isoStr == "NAN" {
					isoStr = ""
				}
			}
			// Store both with and without ISO for flexible matching
			keyWithISO := fmt.Sprintf("%s|%s", stateNameStr, isoStr)
			keyWithoutISO := stateNameStr
			if isoStr != "" {
				lookup[keyWithISO] = locationChID
			}
			// Also store without ISO for fallback (only if not already exists to avoid overwriting)
			if _, exists := lookup[keyWithoutISO]; !exists {
				lookup[keyWithoutISO] = locationChID
			}
		}
	}

	log.Printf("Built state ID lookup: %d states mapped from location_ch", len(lookup))
	return lookup, nil
}

// fetchalleventCategoryNamesForEvents fetches category names for given event IDs
func fetchalleventCategoryNamesForEvents(db *sql.DB, eventIDs []int64) map[int64][]string {
	if len(eventIDs) == 0 {
		return make(map[int64][]string)
	}

	placeholders := make([]string, len(eventIDs))
	args := make([]interface{}, len(eventIDs))
	for i, id := range eventIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(`
		SELECT ec.event, c.name
		FROM event_category ec
		INNER JOIN category c ON ec.category = c.id
		WHERE ec.event IN (%s)
	`, strings.Join(placeholders, ","))

	rows, err := db.Query(query, args...)
	if err != nil {
		log.Printf("Error fetching category names: %v", err)
		return make(map[int64][]string)
	}
	defer rows.Close()

	categoryMap := make(map[int64][]string)
	for rows.Next() {
		var eventID int64
		var categoryName sql.NullString
		if err := rows.Scan(&eventID, &categoryName); err != nil {
			continue
		}
		if categoryName.Valid && categoryName.String != "" {
			categoryMap[eventID] = append(categoryMap[eventID], categoryName.String)
		}
	}

	return categoryMap
}

// fetchalleventTicketDataForBatch fetches ticket data for given event IDs
func fetchalleventTicketDataForBatch(db *sql.DB, eventIDs []int64) []map[string]interface{} {
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
		SELECT 
			event, name, type, currency, price, ticket_url, created
		FROM event_ticket 
		WHERE event IN (%s)
		ORDER BY event, created
	`, strings.Join(placeholders, ","))

	rows, err := db.Query(query, args...)
	if err != nil {
		log.Printf("Error fetching ticket data: %v", err)
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

// processalleventTicketData processes raw ticket data and converts it to JSON strings
func processalleventTicketData(ticketData []map[string]interface{}) map[int64][]string {
	result := make(map[int64][]string)

	for _, ticket := range ticketData {
		eventID, ok := ticket["event"].(int64)
		if !ok {
			continue
		}

		name := shared.SafeConvertToString(ticket["name"])
		ticketType := shared.SafeConvertToString(ticket["type"])
		currency := shared.SafeConvertToNullableString(ticket["currency"])
		price := shared.SafeConvertToFloat64(ticket["price"])
		ticketURL := shared.SafeConvertToNullableString(ticket["ticket_url"])

		eventIDUint32 := shared.ConvertToUInt32(eventID)
		ticketUUID := shared.GenerateEventUUID(eventIDUint32, ticket["created"])

		ticketJSON := map[string]interface{}{
			"id":        ticketUUID,
			"name":      name,
			"type":      ticketType,
			"currency":  currency,
			"price":     price,
			"ticketUrl": ticketURL,
		}

		jsonBytes, err := json.Marshal(ticketJSON)
		if err != nil {
			log.Printf("Error marshaling ticket JSON for event %d: %v", eventID, err)
			continue
		}

		result[eventID] = append(result[eventID], string(jsonBytes))
	}

	return result
}

func extractalleventKeywords(eventName string, eventAbbrName *string, eventDescription *string, eventPunchline *string, categoryNames []string) []string {
	stopWords := map[string]bool{
		"and": true, "or": true, "but": true, "for": true, "nor": true, "so": true, "yet": true,
		"at": true, "by": true, "in": true, "of": true, "on": true, "is": true, "are": true,
		"am": true, "was": true, "were": true, "be": true, "being": true, "been": true,
		"have": true, "had": true, "has": true, "do": true, "does": true, "did": true,
		"can": true, "could": true, "may": true, "might": true, "must": true, "shall": true,
		"should": true, "to": true, "with": true, "a": true, "the": true,
	}

	var allText []string

	if eventName != "" {
		allText = append(allText, strings.ToLower(eventName))
	}

	if eventAbbrName != nil && *eventAbbrName != "" {
		allText = append(allText, strings.ToLower(*eventAbbrName))
	}

	if eventDescription != nil && *eventDescription != "" {
		allText = append(allText, strings.ToLower(*eventDescription))
	}

	if eventPunchline != nil && *eventPunchline != "" {
		allText = append(allText, strings.ToLower(*eventPunchline))
	}

	for _, catName := range categoryNames {
		if catName != "" {
			allText = append(allText, strings.ToLower(catName))
		}
	}

	if len(allText) == 0 {
		return []string{}
	}

	combinedText := strings.Join(allText, " ")

	words := strings.Fields(combinedText)

	wordMap := make(map[string]bool)
	for _, word := range words {
		word = strings.Trim(word, ".,!?;:()[]{}\"'")
		word = strings.ToLower(word)

		if len(word) <= 1 || stopWords[word] {
			continue
		}

		wordMap[word] = true
	}

	distinctWords := make([]string, 0, len(wordMap))
	for word := range wordMap {
		distinctWords = append(distinctWords, word)
	}

	return distinctWords
}

func extractalleventCityIDs(editionData []map[string]interface{}) []int64 {
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

func fetchalleventElasticsearchBatch(esClient *elasticsearch.Client, indexName string, eventIDs []int64) map[int64]map[string]interface{} {
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
		"_source": []string{"id", "description", "exhibitors", "speakers", "totalSponsor", "following", "punchline", "frequency", "city", "hybrid", "logo", "pricing", "total_edition", "avg_rating", "eventEstimatedTag", "impactScore", "inboundScore", "internationalScore", "repeatSentimentChangePercentage", "audienceZone"},
	}

	queryJSON, _ := json.Marshal(query)

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

	var result map[string]interface{}
	if err := json.NewDecoder(searchRes.Body).Decode(&result); err != nil {
		log.Printf("Warning: Failed to decode Elasticsearch response: %v", err)
		return results
	}

	hits := result["hits"].(map[string]interface{})
	hitsArray := hits["hits"].([]interface{})

	if len(hitsArray) == 0 {
		return results
	}

	for _, hit := range hitsArray {
		hitMap := hit.(map[string]interface{})
		source := hitMap["_source"].(map[string]interface{})

		var eventIDInt int64
		if eventIDStr, ok := source["id"].(string); ok {
			if parsedID, err := strconv.ParseInt(eventIDStr, 10, 64); err == nil {
				eventIDInt = parsedID
			} else {
				log.Printf("Warning: Failed to parse id string '%s': %v", eventIDStr, err)
				continue
			}
		} else if eventIDNum, ok := source["id"].(float64); ok {
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
				if floatVal, ok := val.(float64); ok {
					if floatVal >= 0 && floatVal <= 255 && floatVal == float64(uint8(floatVal)) {
						return uint8(floatVal)
					}
					return nil
				}
				if intVal, ok := val.(int); ok {
					if intVal >= 0 && intVal <= 255 {
						return uint8(intVal)
					}
					return nil
				}
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
			"impactScore":                     convertStringToUInt32("impactScore"),
			"inboundScore":                    convertStringToUInt32("inboundScore"),
			"internationalScore":              convertStringToUInt32("internationalScore"),
			"repeatSentimentChangePercentage": convertToFloat64("repeatSentimentChangePercentage"),
			"audienceZone":                    shared.ConvertToString(source["audienceZone"]),
		}
	}

	return results
}

func fetchalleventElasticsearchDataForEvents(esClient *elasticsearch.Client, indexName string, eventIDs []int64) map[int64]map[string]interface{} {
	if len(eventIDs) == 0 {
		return nil
	}

	results := make(map[int64]map[string]interface{})
	batchSize := 200

	expectedBatches := (len(eventIDs) + batchSize - 1) / batchSize
	resultsChan := make(chan map[int64]map[string]interface{}, expectedBatches)
	semaphore := make(chan struct{}, 5)

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

			if batchNum > 0 {
				time.Sleep(100 * time.Millisecond)
			}

			var batchResults map[int64]map[string]interface{}
			maxRetries := 3
			for retry := 0; retry <= maxRetries; retry++ {
				batchResults = fetchalleventElasticsearchBatch(esClient, indexName, eventIDBatch)
				if len(batchResults) > 0 || retry == maxRetries {
					if retry > 0 {
						log.Printf("Elasticsearch batch %d: Success after %d retries, got %d results", batchNum, retry, len(batchResults))
					}
					break
				}
				if retry < maxRetries {
					backoffTime := time.Duration(retry+1) * 3 * time.Second
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

	for _, batchResult := range allResults {
		for eventID, data := range batchResult {
			results[eventID] = data
		}
	}

	log.Printf("OK: Retrieved Elasticsearch data for %d events in %d batches", len(results), len(allResults))
	return results
}

func insertalleventDataIntoClickHouse(clickhouseConn driver.Conn, records []map[string]interface{}, numWorkers int, config shared.Config) error {
	if len(records) == 0 {
		return nil
	}

	if numWorkers <= 1 {
		return insertalleventDataSingleWorker(clickhouseConn, records, config)
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
			err := insertalleventDataSingleWorker(clickhouseConn, batch, config)
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

func insertalleventDataSingleWorker(clickhouseConn driver.Conn, records []map[string]interface{}, config shared.Config) error {
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
			if err := insertalleventDataChunk(clickhouseConn, chunk, config); err != nil {
				return fmt.Errorf("failed to insert chunk %d-%d: %v", i+1, end, err)
			}
		}
		return nil
	}

	return insertalleventDataChunk(clickhouseConn, records, config)
}

func insertalleventDataChunk(clickhouseConn driver.Conn, records []map[string]interface{}, config shared.Config) error {
	if len(records) == 0 {
		return nil
	}

	log.Printf("Checking ClickHouse connection health before inserting %d allevent_ch records", len(records))
	connectionCheckErr := shared.RetryWithBackoff(
		func() error {
			return shared.CheckClickHouseConnectionAlive(clickhouseConn)
		},
		3,
		"ClickHouse connection health check for allevent_ch",
	)
	if connectionCheckErr != nil {
		return fmt.Errorf("ClickHouse connection is not alive after retries: %w", connectionCheckErr)
	}
	log.Printf("ClickHouse connection is alive, proceeding with allevent_ch batch insert")

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
			event_created, event_updated, edition_created, event_hybrid, isBranded, maturity,
			event_pricing, tickets, event_logo, event_estimatedVisitors, event_frequency, impactScore, inboundScore, internationalScore, repeatSentimentChangePercentage, audienceZone,
			inboundPercentage, inboundAttendance, internationalPercentage, internationalAttendance,
			event_economic_FoodAndBevarage, event_economic_Transportation, event_economic_Accomodation, event_economic_Utilities, event_economic_flights, event_economic_value,
			event_economic_dayWiseEconomicImpact, event_economic_breakdown, event_economic_impact, keywords, event_score, last_updated_at, version
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
		alleventRecord := convertToalleventRecord(record)

		err := batch.Append(
			alleventRecord.EventID,                         // event_id: UInt32 NOT NULL
			alleventRecord.EventUUID,                       // event_uuid: UUID NOT NULL
			alleventRecord.EventName,                       // event_name: String NOT NULL
			alleventRecord.EventAbbrName,                   // event_abbr_name: Nullable(String)
			alleventRecord.EventDescription,                // event_description: Nullable(String)
			alleventRecord.EventPunchline,                  // event_punchline: Nullable(String)
			alleventRecord.EventAvgRating,                  // event_avgRating: Nullable(Decimal(3,2))
			alleventRecord.StartDate,                       // start_date: Date NOT NULL
			alleventRecord.EndDate,                         // end_date: Date NOT NULL
			alleventRecord.EditionID,                       // edition_id: UInt32 NOT NULL
			alleventRecord.EditionCountry,                  // edition_country: LowCardinality(FixedString(2)) NOT NULL
			alleventRecord.EditionCity,                     // edition_city: UInt32 NOT NULL
			alleventRecord.EditionCityName,                 // edition_city_name: String NOT NULL
			alleventRecord.EditionCityStateID,              // edition_city_state_id: Nullable(UInt32)
			alleventRecord.EditionCityState,                // edition_city_state: LowCardinality(String) NOT NULL
			alleventRecord.EditionCityLat,                  // edition_city_lat: Float64 NOT NULL
			alleventRecord.EditionCityLong,                 // edition_city_long: Float64 NOT NULL
			alleventRecord.CompanyID,                       // company_id: Nullable(UInt32)
			alleventRecord.CompanyName,                     // company_name: Nullable(String)
			alleventRecord.CompanyDomain,                   // company_domain: Nullable(String)
			alleventRecord.CompanyWebsite,                  // company_website: Nullable(String)
			alleventRecord.CompanyCountry,                  // company_country: LowCardinality(Nullable(FixedString(2)))
			alleventRecord.CompanyState,                    // company_state: LowCardinality(Nullable(String))
			alleventRecord.CompanyCity,                     // company_city: Nullable(UInt32)
			alleventRecord.CompanyCityName,                 // company_city_name: Nullable(String)
			alleventRecord.VenueID,                         // venue_id: Nullable(UInt32)
			alleventRecord.VenueName,                       // venue_name: Nullable(String)
			alleventRecord.VenueCountry,                    // venue_country: LowCardinality(Nullable(FixedString(2)))
			alleventRecord.VenueCity,                       // venue_city: Nullable(UInt32)
			alleventRecord.VenueCityName,                   // venue_city_name: Nullable(String)
			alleventRecord.VenueLat,                        // venue_lat: Nullable(Float64)
			alleventRecord.VenueLong,                       // venue_long: Nullable(Float64)
			alleventRecord.Published,                       // published: Int8 NOT NULL
			alleventRecord.Status,                          // status: LowCardinality(FixedString(1)) NOT NULL DEFAULT 'A'
			alleventRecord.EditionsAudianceType,            // editions_audiance_type: UInt16 NOT NULL
			alleventRecord.EditionFunctionality,            // edition_functionality: LowCardinality(String) NOT NULL
			alleventRecord.EditionWebsite,                  // edition_website: Nullable(String)
			alleventRecord.EditionDomain,                   // edition_domain: Nullable(String)
			alleventRecord.EditionType,                     // edition_type: LowCardinality(Nullable(String)) DEFAULT 'NA'
			alleventRecord.EventFollowers,                  // event_followers: Nullable(UInt32)
			alleventRecord.EditionFollowers,                // edition_followers: Nullable(UInt32)
			alleventRecord.EventExhibitor,                  // event_exhibitor: Nullable(UInt32)
			alleventRecord.EditionExhibitor,                // edition_exhibitor: Nullable(UInt32)
			alleventRecord.ExhibitorsUpperBound,            // exhibitors_upper_bound: Nullable(UInt32)
			alleventRecord.ExhibitorsLowerBound,            // exhibitors_lower_bound: Nullable(UInt32)
			alleventRecord.ExhibitorsMean,                  // exhibitors_mean: Nullable(UInt32)
			alleventRecord.EventSponsor,                    // event_sponsor: Nullable(UInt32)
			alleventRecord.EditionSponsor,                  // edition_sponsor: Nullable(UInt32)
			alleventRecord.EventSpeaker,                    // event_speaker: Nullable(UInt32)
			alleventRecord.EditionSpeaker,                  // edition_speaker: Nullable(UInt32)
			alleventRecord.EventCreated,                    // event_created: DateTime NOT NULL
			alleventRecord.EventUpdated,                    // event_updated: DateTime NOT NULL
			alleventRecord.EditionCreated,                  // edition_created: DateTime NOT NULL
			alleventRecord.EventHybrid,                     // event_hybrid: Nullable(UInt32)
			alleventRecord.IsBranded,                       // isBranded: Nullable(UInt32)
			alleventRecord.Maturity,                        // maturity: LowCardinality(Nullable(String))
			alleventRecord.EventPricing,                    // event_pricing: LowCardinality(Nullable(String))
			alleventRecord.Tickets,                         // tickets: Array(String)
			alleventRecord.EventLogo,                       // event_logo: Nullable(String)
			alleventRecord.EventEstimatedVisitors,          // event_estimatedVisitors: LowCardinality(Nullable(String))
			alleventRecord.EventFrequency,                  // event_frequency: LowCardinality(Nullable(String))
			alleventRecord.ImpactScore,                     // impactScore: Nullable(UInt32)
			alleventRecord.InboundScore,                    // inboundScore: Nullable(UInt32)
			alleventRecord.InternationalScore,              // internationalScore: Nullable(UInt32)
			alleventRecord.RepeatSentimentChangePercentage, // repeatSentimentChangePercentage: Nullable(Float64)
			alleventRecord.AudienceZone,                    // audienceZone: LowCardinality(Nullable(String))
			alleventRecord.InboundPercentage,               // inboundPercentage: UInt32 NOT NULL
			alleventRecord.InboundAttendance,               // inboundAttendance: UInt32 NOT NULL
			alleventRecord.InternationalPercentage,         // internationalPercentage: UInt32 NOT NULL
			alleventRecord.InternationalAttendance,         // internationalAttendance: UInt32 NOT NULL
			alleventRecord.EventEconomicFoodAndBevarage,    // event_economic_FoodAndBevarage: Nullable(Float64)
			alleventRecord.EventEconomicTransportation,     // event_economic_Transportation: Nullable(Float64)
			alleventRecord.EventEconomicAccomodation,       // event_economic_Accomodation: Nullable(Float64)
			alleventRecord.EventEconomicUtilities,          // event_economic_Utilities: Nullable(Float64)
			alleventRecord.EventEconomicFlights,            // event_economic_flights: Nullable(Float64)
			alleventRecord.EventEconomicValue,              // event_economic_value: Nullable(Float64)
			alleventRecord.EventEconomicDayWiseImpact,      // event_economic_dayWiseEconomicImpact: JSON
			alleventRecord.EventEconomicBreakdown,          // event_economic_breakdown: JSON
			alleventRecord.EventEconomicImpact,             // event_economic_impact: JSON
			alleventRecord.Keywords,                        // keywords: Nullable(String)
			alleventRecord.EventScore,                      // event_score: Nullable(Int32)
			alleventRecord.LastUpdatedAt,                   // last_updated_at: DateTime NOT NULL
			alleventRecord.Version,                         // version: UInt32 NOT NULL DEFAULT 1
		)
		if err != nil {
			log.Printf("ERROR: Failed to append record to batch: %v", err)
			log.Printf("Record data: EventID=%d, EventName=%s, EventAvgRating=%v",
				alleventRecord.EventID, alleventRecord.EventName, alleventRecord.EventAvgRating)
			return fmt.Errorf("failed to append record to batch: %v", err)
		}
	}

	if err := batch.Send(); err != nil {
		log.Printf("ERROR: Failed to send ClickHouse batch: %v", err)
		return fmt.Errorf("failed to send ClickHouse batch: %v", err)
	}

	log.Printf("OK: Successfully inserted %d allevent records", len(records))
	return nil
}
