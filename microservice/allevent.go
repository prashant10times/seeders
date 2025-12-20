package microservice

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"seeders/shared"
	"strconv"
	"strings"
	"sync"
	"time"

	"seeders/utils"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/elastic/go-elasticsearch/v6"
)

// allowed_event_types = [1,2,3,5,6,7,12,13]

// event_type_ids = {
//     1 : "9b5524b4-60f5-5478-b3f0-38e2e12e3981",
//     2 : "4de48054-46fb-5452-a23f-8aac6c00592e",
//     3 : "ad7c83a5-b8fc-5109-a159-9306848de22c",
//     5 : "e5283caa-f655-504b-8e44-49ae0edb3faa",
//     6 : "69cf1329-0c71-5dae-b7a9-838c5712bce0",
//     7 : "bffa5040-c654-5991-a1c5-0610e2c0ec74",
//     12 : "94fcb56e-2838-5d74-9092-e582d873a03e",
//     13 : "3a3609e5-56df-5a8b-ad47-c9e168eb4f59",
// }

// event_type_priority = {
//     1 : {
//         "priority" : 1,
//         "group" : "B2B"
//         },
//     2 : {
//         "priority" : 2,
//         "group" : "B2B"
//         },
//     3 : {
//         "priority" : 3,
//         "group" : "B2B"
//         },
//     5 : {
//         "priority" : 4,
//         "group" : "B2C"
//         },
//     7 : {
//         "priority" : 5,
//         "group" : "B2C"
//         },
//     6 : {
//         "priority" : 6,
//         "group" : "B2C"
//         },
//     12 : {
//         "priority" : 7,
//         "group" : "B2C"
//         },
//     13 : {
//         "priority" : 8,
//         "group" : "B2C"
//         },
// }

var eventTypeIDs map[uint32]string
var allowedEventTypeIDs = []uint32{1, 2, 3, 5, 6, 7, 12, 13}

// Event type priority and group mapping
type eventTypePriorityInfo struct {
	Priority int8
	Group    string // "B2B" or "B2C"
}

var eventTypePriority = map[uint32]eventTypePriorityInfo{
	1:  {Priority: 1, Group: "B2B"},
	2:  {Priority: 2, Group: "B2B"},
	3:  {Priority: 3, Group: "B2B"},
	5:  {Priority: 4, Group: "B2C"},
	7:  {Priority: 5, Group: "B2C"},
	6:  {Priority: 6, Group: "B2C"},
	12: {Priority: 7, Group: "B2C"},
	13: {Priority: 8, Group: "B2C"},
}

var allowedEventTypes = []uint32{1, 2, 3, 5, 6, 7, 12, 13}

// attendance_range_tag maps event type ID to a map of range strings to size labels
// Example: 1 -> {"0-1000": "NANO", "1000-5000": "MICRO", "50000+": "ULTRA"}
// Using event type IDs instead of UUIDs for better memory efficiency and simpler lookups
var attendanceRangeTag = map[uint32]map[string]string{
	1: {
		"0-1000":       "NANO",
		"1000-5000":    "MICRO",
		"5000-10000":   "SMALL",
		"10000-20000":  "MEDIUM",
		"20000-50000":  "LARGE",
		"50000-100000": "MEGA",
		"100000+":      "ULTRA",
	},
	2: {
		"0-300":      "NANO",
		"300-500":    "MICRO",
		"500-1000":   "SMALL",
		"1000-2000":  "MEDIUM",
		"2000-5000":  "LARGE",
		"5000-10000": "MEGA",
		"10000+":     "ULTRA",
	},
	3: {
		"0-300":      "NANO",
		"300-500":    "MICRO",
		"500-1000":   "SMALL",
		"1000-2000":  "MEDIUM",
		"2000-5000":  "LARGE",
		"5000-10000": "MEGA",
		"10000+":     "ULTRA",
	},
	5: {
		"0-1000":       "NANO",
		"1000-5000":    "MICRO",
		"5000-10000":   "SMALL",
		"10000-20000":  "MEDIUM",
		"20000-50000":  "LARGE",
		"50000-100000": "MEGA",
		"100000+":      "ULTRA",
	},
	6: {
		"0-1000":       "NANO",
		"1000-5000":    "MICRO",
		"5000-10000":   "SMALL",
		"10000-20000":  "MEDIUM",
		"20000-50000":  "LARGE",
		"50000-100000": "MEGA",
		"100000+":      "ULTRA",
	},
	7: {
		"0-300":      "NANO",
		"300-500":    "MICRO",
		"500-1000":   "SMALL",
		"1000-2000":  "MEDIUM",
		"2000-5000":  "LARGE",
		"5000-10000": "MEGA",
		"10000+":     "ULTRA",
	},
	12: {
		"0-300":      "NANO",
		"300-500":    "MICRO",
		"500-1000":   "SMALL",
		"1000-2000":  "MEDIUM",
		"2000-5000":  "LARGE",
		"5000-10000": "MEGA",
		"10000+":     "ULTRA",
	},
	13: {
		"0-300":      "NANO",
		"300-500":    "MICRO",
		"500-1000":   "SMALL",
		"1000-2000":  "MEDIUM",
		"2000-5000":  "LARGE",
		"5000-10000": "MEGA",
		"10000+":     "ULTRA",
	},
}

// eventTypeUUIDToID reverse lookup map: UUID -> ID (populated from eventTypeIDs)
var eventTypeUUIDToID map[string]uint32

func loadEventTypeIDsFromDB(clickhouseConn driver.Conn, config shared.Config) (map[uint32]string, error) {
	baseTableName := shared.GetClickHouseTableName("event_type_ch", config)

	log.Printf("Checking ClickHouse connection health before querying event type IDs from %s", baseTableName)
	connectionCheckErr := shared.RetryWithBackoff(
		func() error {
			return shared.CheckClickHouseConnectionAlive(clickhouseConn)
		},
		3,
	)
	if connectionCheckErr != nil {
		return nil, fmt.Errorf("ClickHouse connection is not alive after retries: %w", connectionCheckErr)
	}
	log.Printf("ClickHouse connection is alive, proceeding with event type IDs query")

	idsStr := make([]string, len(allowedEventTypeIDs))
	for i, id := range allowedEventTypeIDs {
		idsStr[i] = fmt.Sprintf("%d", id)
	}
	idsList := strings.Join(idsStr, ",")

	tableName := shared.GetTableNameWithDB(baseTableName, config)

	query := fmt.Sprintf(
		"SELECT eventtype_id, eventtype_uuid FROM %s WHERE eventtype_id IN (%s) GROUP BY eventtype_id, eventtype_uuid",
		tableName, idsList,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var rows driver.Rows
	var err error
	queryErr := shared.RetryWithBackoff(
		func() error {
			rows, err = clickhouseConn.Query(ctx, query)
			if err != nil {
				return fmt.Errorf("failed to query %s: %w", baseTableName, err)
			}
			return nil
		},
		3,
	)
	if queryErr != nil {
		return nil, queryErr
	}
	defer rows.Close()

	eventTypeMap := make(map[uint32]string, len(allowedEventTypeIDs))
	for rows.Next() {
		var eventTypeID uint32
		var eventTypeUUID string
		if err := rows.Scan(&eventTypeID, &eventTypeUUID); err != nil {
			return nil, fmt.Errorf("failed to scan event type row: %w", err)
		}
		eventTypeMap[eventTypeID] = eventTypeUUID
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating event type rows: %w", err)
	}

	missing := make([]uint32, 0, len(allowedEventTypeIDs))
	for _, id := range allowedEventTypeIDs {
		if _, found := eventTypeMap[id]; !found {
			missing = append(missing, id)
		}
	}

	if len(missing) > 0 {
		log.Printf("WARNING: Missing event type IDs in database: %v", missing)
	}

	log.Printf("Loaded %d event type UUIDs from database (expected %d)", len(eventTypeMap), len(allowedEventTypeIDs))
	return eventTypeMap, nil
}

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
		CompanyUUID:        shared.SafeConvertToString(record["company_uuid"]),
		CompanyName:        shared.SafeConvertToNullableString(record["company_name"]),
		CompanyDomain:      shared.SafeConvertToNullableString(record["company_domain"]),
		CompanyWebsite:     shared.SafeConvertToNullableString(record["company_website"]),
		CompanyLogoUrl:     shared.SafeConvertToNullableString(record["companyLogoUrl"]),
		CompanyCountry:     shared.ToUpperNullableString(shared.SafeConvertToNullableString(record["company_country"])),
		CompanyState:       shared.SafeConvertToNullableString(record["company_state"]),
		CompanyCity:        shared.SafeConvertToNullableUInt32(record["company_city"]),
		CompanyCityName: func() *string {
			if val, ok := record["company_city_name"].(*string); ok {
				return val
			}
			return nil
		}(),
		CompanyAddress: shared.SafeConvertToNullableString(record["company_address"]),
		VenueID:        shared.SafeConvertToNullableUInt32(record["venue_id"]),
		VenueName:      shared.SafeConvertToNullableString(record["venue_name"]),
		VenueCountry:   shared.ToUpperNullableString(shared.SafeConvertToNullableString(record["venue_country"])),
		VenueCity:      shared.SafeConvertToNullableUInt32(record["venue_city"]),
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
		EventEditions:        shared.SafeConvertToNullableUInt32(record["event_editions"]),
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
		EventFormat:          shared.SafeConvertToNullableString(record["event_format"]),
		IsBranded: func() *uint32 {
			if val, ok := record["isBranded"].(*uint32); ok {
				return val
			}
			return nil
		}(),
		EventBrandId:           shared.SafeConvertToNullableString(record["eventBrandId"]),
		EventSeriesId:          shared.SafeConvertToNullableString(record["eventSeriesId"]),
		Maturity:               shared.SafeConvertToNullableString(record["maturity"]),
		EventPricing:           shared.SafeConvertToNullableString(record["event_pricing"]),
		EventLogo:              shared.SafeConvertToNullableString(record["event_logo"]),
		EventEstimatedVisitors: shared.SafeConvertToNullableString(record["event_estimatedVisitors"]),
		EstimatedVisitorsMean: func() *uint32 {
			val := record["estimatedVisitorsMean"]
			if val == nil {
				return nil
			}
			if ptr, ok := val.(*uint32); ok {
				return ptr
			}
			return shared.SafeConvertToNullableUInt32(val)
		}(),
		EstimatedSize:                   shared.SafeConvertToNullableString(record["estimatedSize"]),
		EventFrequency:                  shared.SafeConvertToNullableString(record["event_frequency"]),
		ImpactScore:                     shared.SafeConvertToNullableUInt32(record["impactScore"]),
		InboundScore:                    shared.SafeConvertToNullableUInt32(record["inboundScore"]),
		InternationalScore:              shared.SafeConvertToNullableUInt32(record["internationalScore"]),
		RepeatSentimentChangePercentage: shared.SafeConvertToNullableFloat64(record["repeatSentimentChangePercentage"]),
		RepeatSentiment:                 shared.SafeConvertToNullableUInt32(record["repeatSentiment"]),
		ReputationChangePercentage:      shared.SafeConvertToNullableFloat64(record["reputationChangePercentage"]),
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
		TenTimesEventPageUrl:            shared.SafeConvertToNullableString(record["10timesEventPageUrl"]),
		Keywords:                        shared.ConvertToStringArray(record["keywords"]),
		Tickets:                         shared.ConvertToStringArray(record["tickets"]),
		Timings:                         shared.ConvertToStringArray(record["timings"]),
		EventScore:                      shared.SafeConvertToNullableInt32(record["event_score"]),
		YoYGrowth:                       shared.SafeConvertToNullableUInt32(record["yoyGrowth"]),
		FutureExpectedStartDate:         shared.SafeConvertToNullableString(record["futureExpexctedStartDate"]),
		FutureExpectedEndDate:           shared.SafeConvertToNullableString(record["futureExpexctedEndDate"]),
		PrimaryEventType:                shared.SafeConvertToNullableString(record["PrimaryEventType"]),
		VerifiedOn:                      shared.SafeConvertToNullableString(record["verifiedOn"]),
		LastUpdatedAt:                   shared.SafeConvertToDateTimeString(record["last_updated_at"]),
		Version:                         shared.SafeConvertToUInt32(record["version"]),
	}
}

type alleventRecord struct {
	EventID                         uint32   `ch:"event_id"`
	EventUUID                       string   `ch:"event_uuid"` // UUID generated from event_id + edition_created
	EventName                       string   `ch:"event_name"`
	EventAbbrName                   *string  `ch:"event_abbr_name"`
	EventDescription                *string  `ch:"event_description"`
	EventPunchline                  *string  `ch:"event_punchline"`
	EventAvgRating                  *string  `ch:"event_avgRating"`     // Nullable(Decimal(3,2))
	TenTimesEventPageUrl            *string  `ch:"10timesEventPageUrl"` // Nullable(String)
	StartDate                       string   `ch:"start_date"`          // Date NOT NULL
	EndDate                         string   `ch:"end_date"`            // Date NOT NULL
	EditionID                       uint32   `ch:"edition_id"`
	EditionCountry                  string   `ch:"edition_country"`       // LowCardinality(FixedString(2)) NOT NULL
	EditionCity                     uint32   `ch:"edition_city"`          // UInt32 NOT NULL
	EditionCityName                 string   `ch:"edition_city_name"`     // String NOT NULL
	EditionCityStateID              *uint32  `ch:"edition_city_state_id"` // Nullable(UInt32)
	EditionCityState                string   `ch:"edition_city_state"`    // LowCardinality(String) NOT NULL
	EditionCityLat                  float64  `ch:"edition_city_lat"`      // Float64 NOT NULL
	EditionCityLong                 float64  `ch:"edition_city_long"`     // Float64 NOT NULL
	CompanyID                       *uint32  `ch:"company_id"`
	CompanyUUID                     string   `ch:"company_uuid"` // UUID NOT NULL
	CompanyName                     *string  `ch:"company_name"`
	CompanyDomain                   *string  `ch:"company_domain"`
	CompanyWebsite                  *string  `ch:"company_website"`
	CompanyLogoUrl                  *string  `ch:"companyLogoUrl"` // Nullable(String)
	CompanyCountry                  *string  `ch:"company_country"`
	CompanyState                    *string  `ch:"company_state"` // LowCardinality(Nullable(String))
	CompanyCity                     *uint32  `ch:"company_city"`
	CompanyCityName                 *string  `ch:"company_city_name"`
	CompanyAddress                  *string  `ch:"company_address"` // Nullable(String)
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
	EditionType                     string   `ch:"edition_type"`   // LowCardinality(Nullable(String)) DEFAULT 'NA'
	EventEditions                   *uint32  `ch:"event_editions"` // Nullable(UInt32) - total number of editions for the event
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
	EventFormat                     *string  `ch:"event_format"`                         // LowCardinality(Nullable(String))
	IsBranded                       *uint32  `ch:"isBranded"`                            // Nullable(UInt32)
	EventBrandId                    *string  `ch:"eventBrandId"`                         // Nullable(UUID)
	EventSeriesId                   *string  `ch:"eventSeriesId"`                        // Nullable(UUID)
	Maturity                        *string  `ch:"maturity"`                             // LowCardinality(Nullable(String))
	EventPricing                    *string  `ch:"event_pricing"`                        // LowCardinality(Nullable(String))
	EventLogo                       *string  `ch:"event_logo"`                           // Nullable(String)
	EventEstimatedVisitors          *string  `ch:"event_estimatedVisitors"`              // LowCardinality(Nullable(String))
	EstimatedVisitorsMean           *uint32  `ch:"estimatedVisitorsMean"`                // Nullable(UInt32)
	EstimatedSize                   *string  `ch:"estimatedSize"`                        // LowCardinality(Nullable(String))
	EventFrequency                  *string  `ch:"event_frequency"`                      // LowCardinality(Nullable(String))
	ImpactScore                     *uint32  `ch:"impactScore"`                          // Nullable(UInt32)
	InboundScore                    *uint32  `ch:"inboundScore"`                         // Nullable(UInt32)
	InternationalScore              *uint32  `ch:"internationalScore"`                   // Nullable(UInt32)
	RepeatSentimentChangePercentage *float64 `ch:"repeatSentimentChangePercentage"`      // Nullable(Float64)
	RepeatSentiment                 *uint32  `ch:"repeatSentiment"`                      // Nullable(UInt32)
	ReputationChangePercentage      *float64 `ch:"reputationChangePercentage"`           // Nullable(Float64)
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
	Timings                         []string `ch:"timings"`                              // Array(String)
	EventScore                      *int32   `ch:"event_score"`                          // Nullable(Int32)
	YoYGrowth                       *uint32  `ch:"yoyGrowth"`                            // Nullable(UInt32)
	FutureExpectedStartDate         *string  `ch:"futureExpexctedStartDate"`             // Nullable(Date)
	FutureExpectedEndDate           *string  `ch:"futureExpexctedEndDate"`               // Nullable(Date)
	PrimaryEventType                *string  `ch:"PrimaryEventType"`                     // Nullable(UUID)
	VerifiedOn                      *string  `ch:"verifiedOn"`                           // Nullable(Date)
	LastUpdatedAt                   string   `ch:"last_updated_at"`                      // DateTime NOT NULL
	Version                         uint32   `ch:"version"`
}

func buildalleventMigrationData(db *sql.DB, table string, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT id, name as event_name, abbr_name, punchline, start_date, end_date, country, published, status, event_audience, functionality, brand_id, created FROM %s WHERE id >= %d AND id <= %d ORDER BY id, created LIMIT %d", table, startID, endID, batchSize)
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
		if companyID, ok := company["id_10x"].(int64); ok {
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
			c.id as id_10x, c.name as company_name, c.domain as company_domain, 
			c.website as company_website, c.country as company_country, 
			c.city as company_city, c.address, c.created, a.cdn_url as company_logo_url
		FROM company c
		LEFT JOIN attachment a ON c.logo = a.id
		WHERE c.id IN (%s)`, strings.Join(placeholders, ","))

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

func fetchEditionsByEditionIDs(db *sql.DB, editionIDs []int64) []map[string]interface{} {
	if len(editionIDs) == 0 {
		return nil
	}

	batchSize := 1000
	var allEditionData []map[string]interface{}

	for i := 0; i < len(editionIDs); i += batchSize {
		end := i + batchSize
		if end > len(editionIDs) {
			end = len(editionIDs)
		}

		batch := editionIDs[i:end]
		editionData := fetchEditionsByEditionIDsBatch(db, batch)
		allEditionData = append(allEditionData, editionData...)
	}

	return allEditionData
}

func fetchEditionsByEditionIDsBatch(db *sql.DB, editionIDs []int64) []map[string]interface{} {
	if len(editionIDs) == 0 {
		return nil
	}

	placeholders := make([]string, len(editionIDs))
	args := make([]interface{}, len(editionIDs))
	for i, id := range editionIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	editionQuery := fmt.Sprintf(`
		SELECT 
			event, id as edition_id, city as edition_city, 
			company_id, venue as venue_id, website as edition_website, 
			created as edition_created, start_date as edition_start_date,
			exhibitors_total, online_event as is_online
		FROM event_edition 
		WHERE id IN (%s)
		ORDER BY created`, strings.Join(placeholders, ","))

	editionRows, err := db.Query(editionQuery, args...)
	if err != nil {
		log.Printf("Error fetching edition data by edition_id: %v", err)
		return nil
	}
	defer editionRows.Close()

	columns, err := editionRows.Columns()
	if err != nil {
		return nil
	}

	eventIDSet := make(map[int64]bool)
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

		if eventID, ok := row["event"].(int64); ok {
			eventIDSet[eventID] = true
		}

		results = append(results, row)
	}

	if len(eventIDSet) > 0 {
		eventIDs := make([]int64, 0, len(eventIDSet))
		for id := range eventIDSet {
			eventIDs = append(eventIDs, id)
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
		if err == nil {
			defer currentEditionRows.Close()
			currentEditionMap := make(map[int64]int64)
			for currentEditionRows.Next() {
				var eventID int64
				var currentEditionID sql.NullInt64
				if err := currentEditionRows.Scan(&eventID, &currentEditionID); err == nil {
					if currentEditionID.Valid {
						currentEditionMap[eventID] = currentEditionID.Int64
					}
				}
			}

			for _, row := range results {
				if eventID, ok := row["event"].(int64); ok {
					if currentEditionID, exists := currentEditionMap[eventID]; exists {
						row["current_edition_id"] = currentEditionID

						if editionID, ok := row["edition_id"].(int64); ok && editionID == currentEditionID {
							row["current_edition_exhibitors_total"] = row["exhibitors_total"]
						}
					}
				}
			}
		}
	}

	return results
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
		WHERE id IN (%s)
		ORDER BY created`, strings.Join(placeholders, ","))

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
			exhibitors_total, online_event as is_online
		FROM event_edition 
		WHERE event IN (%s)
		ORDER BY created`, strings.Join(placeholders, ","))

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

		if eventID, ok := row["event"].(int64); ok {
			if currentEditionID, exists := currentEditionMap[eventID]; exists {
				row["current_edition_id"] = currentEditionID

				if editionID, ok := row["edition_id"].(int64); ok && editionID == currentEditionID {
					row["current_edition_exhibitors_total"] = row["exhibitors_total"]
				}
			}
		}

		results = append(results, row)
	}

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
		SELECT e.id, e.name as event_name, e.abbr_name, e.punchline, e.start_date, e.end_date, 
		       e.country, e.published, e.status, e.event_audience, e.functionality, e.brand_id, e.created, e.modified, e.event_type, e.score, e.url, e.multi_city, e.verified,
		       eb.id as brand_id_from_table, eb.created as brand_created
		FROM event e
		LEFT JOIN event_brands eb ON e.brand_id = eb.id
		WHERE e.id IN (%s)
		ORDER BY e.created`, strings.Join(placeholders, ","))

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

// fetchalleventEventTypesForBatch fetches event types for a batch of events from event_type_event table
func fetchalleventEventTypesForBatch(db *sql.DB, eventIDs []int64) map[int64][]uint32 {
	if len(eventIDs) == 0 {
		return nil
	}

	placeholders := make([]string, len(eventIDs))
	args := make([]interface{}, len(eventIDs))
	for i, id := range eventIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	// Build allowed event types string
	allowedTypesStr := make([]string, len(allowedEventTypes))
	for i, et := range allowedEventTypes {
		allowedTypesStr[i] = fmt.Sprintf("%d", et)
	}

	query := fmt.Sprintf(`
		SELECT ete.id, et.id as event_type_id, ete.event_id, et.created 
		FROM event_type_event ete 
		LEFT JOIN event_type et ON ete.eventtype_id = et.id 
		WHERE ete.published = 1 
		AND ete.event_id IN (%s) 
		AND et.id IN (%s)
		ORDER BY ete.id ASC
	`, strings.Join(placeholders, ","), strings.Join(allowedTypesStr, ","))

	rows, err := db.Query(query, args...)
	if err != nil {
		log.Printf("Error fetching event types: %v", err)
		return nil
	}
	defer rows.Close()

	result := make(map[int64][]uint32)
	for rows.Next() {
		var id int64
		var eventTypeID sql.NullInt64
		var eventID sql.NullInt64
		var created sql.NullString

		if err := rows.Scan(&id, &eventTypeID, &eventID, &created); err != nil {
			log.Printf("Error scanning event type row: %v", err)
			continue
		}

		if !eventID.Valid || !eventTypeID.Valid {
			continue
		}

		eventIDInt := eventID.Int64
		eventTypeIDUint := uint32(eventTypeID.Int64)

		// Check if event type is in allowed list
		isAllowed := false
		for _, allowedType := range allowedEventTypes {
			if eventTypeIDUint == allowedType {
				isAllowed = true
				break
			}
		}

		if isAllowed {
			result[eventIDInt] = append(result[eventIDInt], eventTypeIDUint)
		}
	}

	return result
}

// getAttendanceRange returns the attendance range string based on event type ID and estimated visitor mean
// use event type ID instead of UUID for better memory efficiency
func getAttendanceRange(primaryEventTypeID *uint32, estimatedVisitorMean *uint32) *string {
	if primaryEventTypeID == nil || estimatedVisitorMean == nil {
		return nil
	}

	ranges, ok := attendanceRangeTag[*primaryEventTypeID]
	if !ok {
		log.Printf("Warning: No attendance ranges found for event type ID: %d", *primaryEventTypeID)
		return nil
	}

	rangeRegex := regexp.MustCompile(`-|\+`)

	var selectedRange *string
	for rangeStr := range ranges {
		parts := rangeRegex.Split(rangeStr, -1)
		if len(parts) < 1 {
			continue
		}

		minVal, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			log.Printf("Warning: Could not parse min value '%s' in range '%s': %v", parts[0], rangeStr, err)
			continue
		}

		if strings.HasSuffix(rangeStr, "+") {
			if int64(*estimatedVisitorMean) >= minVal {
				selectedRange = &rangeStr
				break
			}
		} else if len(parts) >= 2 {
			maxVal, err := strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				log.Printf("Warning: Could not parse max value '%s' in range '%s': %v", parts[1], rangeStr, err)
				continue
			}

			visitorMeanInt := int64(*estimatedVisitorMean)
			if visitorMeanInt >= minVal && visitorMeanInt <= maxVal {
				selectedRange = &rangeStr
				break
			}
		}
	}

	return selectedRange
}

func getPrimaryEventType(eventTypes []uint32, eventAudience uint16) *string {
	if len(eventTypes) == 0 {
		return nil
	}

	var audienceGroup string
	switch eventAudience {
	case 11000:
		audienceGroup = "B2B"
	case 10100:
		audienceGroup = "B2C"
	default:
		audienceGroup = "B2B"
	}

	var validEventTypes []uint32
	for _, eventType := range eventTypes {
		if priorityInfo, ok := eventTypePriority[eventType]; ok {
			if priorityInfo.Group == audienceGroup {
				validEventTypes = append(validEventTypes, eventType)
			}
		}
	}

	if len(validEventTypes) == 0 {
		oppositeGroup := "B2C"
		if audienceGroup == "B2C" {
			oppositeGroup = "B2B"
		}
		for _, eventType := range eventTypes {
			if priorityInfo, ok := eventTypePriority[eventType]; ok {
				if priorityInfo.Group == oppositeGroup {
					validEventTypes = append(validEventTypes, eventType)
				}
			}
		}
	}

	if len(validEventTypes) == 0 {
		return nil
	}

	for i := 0; i < len(validEventTypes)-1; i++ {
		for j := i + 1; j < len(validEventTypes); j++ {
			priorityI := eventTypePriority[validEventTypes[i]].Priority
			priorityJ := eventTypePriority[validEventTypes[j]].Priority
			if priorityI > priorityJ {
				validEventTypes[i], validEventTypes[j] = validEventTypes[j], validEventTypes[i]
			}
		}
	}

	primaryEventTypeID := validEventTypes[0]
	if uuid, ok := eventTypeIDs[primaryEventTypeID]; ok {
		return &uuid
	}

	return nil
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

	if errorField, exists := economicImpactJSON["error"]; exists && errorField != nil {
		result[eventID] = processedData
		return result
	}

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
				roundedVal := math.Round(val*100) / 100
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

func cleanupOldLogFiles() {
	logFiles := []string{
		"optimize_logs.log",
		"polygon_coordinate_cases.log",
	}

	for _, logFile := range logFiles {
		if err := os.Remove(logFile); err != nil && !os.IsNotExist(err) {
			log.Printf("WARNING: Failed to remove log file %s: %v", logFile, err)
		} else if err == nil {
			log.Printf("Removed old log file: %s", logFile)
		}
	}

	logDir := "failed_batches"
	retryLogPattern := filepath.Join(logDir, "retry_log_*.txt")
	retryLogFiles, err := filepath.Glob(retryLogPattern)
	if err != nil {
		log.Printf("WARNING: Failed to glob retry log files: %v", err)
	} else {
		for _, logFile := range retryLogFiles {
			if err := os.Remove(logFile); err != nil && !os.IsNotExist(err) {
				log.Printf("WARNING: Failed to remove retry log file %s: %v", logFile, err)
			} else if err == nil {
				log.Printf("Removed old retry log file: %s", logFile)
			}
		}
	}
}

func ProcessAllEventOnly(mysqlDB *sql.DB, clickhouseConn driver.Conn, esClient *elasticsearch.Client, config shared.Config) {
	log.Println("=== Starting allevent ONLY Processing ===")

	cleanupOldLogFiles()

	var err error
	eventTypeIDs, err = loadEventTypeIDsFromDB(clickhouseConn, config)
	if err != nil {
		log.Fatalf("Failed to load event type IDs from database: %v", err)
	}
	if len(eventTypeIDs) == 0 {
		log.Fatal("No event type IDs loaded from database")
	}

	eventTypeUUIDToID = make(map[string]uint32, len(eventTypeIDs))
	for id, uuid := range eventTypeIDs {
		eventTypeUUIDToID[uuid] = id
	}

	totalRecords, minID, maxID, err := shared.GetTotalRecordsAndIDRange(mysqlDB, "event")
	if err != nil {
		log.Fatal("Failed to get total records and ID range from event:", err)
	}

	log.Printf("Total event records: %d, Min ID: %d, Max ID: %d", totalRecords, minID, maxID)

	if config.NumChunks <= 0 {
		config.NumChunks = 5 // Default to 5 chunks if not specified
	}

	chunkSize := (maxID - minID + 1) / config.NumChunks
	if chunkSize == 0 {
		chunkSize = 1
	}

	log.Printf("Processing allevent data in %d chunks with chunk size: %d", config.NumChunks, chunkSize)

	globalUniqueRecords := make(map[uint64]bool)
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

	globalCountMutex.Lock()
	log.Printf("=== FINAL SUMMARY ===")
	log.Printf("Total records processed: %d", totalRecordsProcessed)
	log.Printf("Total records skipped (duplicates): %d", totalRecordsSkipped)
	log.Printf("Total records inserted: %d", totalRecordsInserted)

	var nullEventCount int
	var invalidEventCount int
	var totalEditionsInSource int
	var validEventCount int

	err = mysqlDB.QueryRow("SELECT COUNT(*) FROM event_edition").Scan(&totalEditionsInSource)
	if err != nil {
		log.Printf("Error getting total editions count: %v", err)
	} else {
		log.Printf("Total editions in source (event_edition table): %d", totalEditionsInSource)
	}

	err = mysqlDB.QueryRow("SELECT COUNT(*) FROM event_edition WHERE event IS NULL").Scan(&nullEventCount)
	if err != nil {
		log.Printf("Error checking NULL events: %v", err)
	} else {
		log.Printf("Editions with NULL event values: %d", nullEventCount)
	}

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

	log.Println("=== Checking for failed batches to retry ===")
	log.Println("⏳ Waiting 30 seconds before starting retry phase (to allow memory to clear)...")
	time.Sleep(30 * time.Second)
	log.Println("✓ Wait complete, starting retry phase")
	retryErr := retryFailedBatchesAfterCompletion(mysqlDB, clickhouseConn, esClient, config)
	if retryErr != nil {
		log.Printf("ERROR: Failed to retry failed batches: %v", retryErr)
		log.Printf("⚠️  WARNING: Some batches still failed - optimization will be skipped")
		log.Printf("⚠️  Please rerun the script to retry failed batches before optimization")
	}
}

func HasRemainingFailedBatches() bool {
	dir := "failed_batches"
	files, err := filepath.Glob(filepath.Join(dir, "failed_batches_chunk_*.csv"))
	if err != nil {
		log.Printf("WARNING: Failed to check for remaining failed batch files: %v", err)
		return false
	}
	return len(files) > 0
}

func processalleventChunk(mysqlDB *sql.DB, clickhouseConn driver.Conn, esClient *elasticsearch.Client, config shared.Config, startID, endID int, chunkNum int, results chan<- string, globalUniqueRecords map[uint64]bool, globalMutex *sync.RWMutex, totalRecordsProcessed *int64, totalRecordsSkipped *int64, totalRecordsInserted *int64, globalCountMutex *sync.Mutex) {
	log.Printf("Processing allevent chunk %d: ID range %d-%d", chunkNum, startID, endID)

	totalRecords := endID - startID + 1
	processed := 0
	batchNumber := 0

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
		batchNumber++
		progress := float64(processed) / float64(totalRecords) * 100
		log.Printf("allevent chunk %d: Retrieved %d records in batch %d (%.1f%% complete)", chunkNum, len(batchData), batchNumber, progress)

		eventIDs := extractalleventIDs(batchData)
		if len(eventIDs) > 0 {
			log.Printf("allevent chunk %d: Fetching edition data for %d events", chunkNum, len(eventIDs))

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
				locationTableName := shared.GetClickHouseTableName("location_ch", config)
				log.Printf("allevent chunk %d: Building location lookups from %s for cities, states, and venues", chunkNum, locationTableName)
				startTime := time.Now()
				cityIDLookup, err := buildalleventCityIDLookupFromLocationCh(clickhouseConn, locationTableName)
				if err != nil {
					log.Printf("allevent chunk %d: WARNING - Failed to build city ID lookup: %v", chunkNum, err)
					cityIDLookup = make(map[string]uint32)
				}
				stateIDLookup, err := buildalleventStateIDLookupFromLocationCh(clickhouseConn, locationTableName)
				if err != nil {
					log.Printf("allevent chunk %d: WARNING - Failed to build state ID lookup: %v", chunkNum, err)
					stateIDLookup = make(map[string]uint32)
				}
				venueIDLookup, err := buildalleventVenueIDLookupFromLocationCh(clickhouseConn, locationTableName)
				if err != nil {
					log.Printf("allevent chunk %d: WARNING - Failed to build venue ID lookup: %v", chunkNum, err)
					venueIDLookup = make(map[string]uint32)
				}
				lookupTime := time.Since(startTime)
				log.Printf("allevent chunk %d: Built location lookups from %s in %v (cities: %d, states: %d, venues: %d)", chunkNum, locationTableName, lookupTime, len(cityIDLookup), len(stateIDLookup), len(venueIDLookup))

				companyLookup := make(map[int64]map[string]interface{})
				if len(companyData) > 0 {
					for _, company := range companyData {
						if companyID, ok := company["id_10x"].(int64); ok {
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

				eventTypesMap := make(map[int64][]uint32)
				if len(eventIDsForEditions) > 0 {
					log.Printf("allevent chunk %d: Fetching event types for %d events", chunkNum, len(eventIDsForEditions))
					startTime = time.Now()
					eventTypesMap = fetchalleventEventTypesForBatch(mysqlDB, eventIDsForEditions)
					eventTypeTime := time.Since(startTime)
					log.Printf("allevent chunk %d: Retrieved event types for %d events in %v", chunkNum, len(eventTypesMap), eventTypeTime)
				}

				processedEconomicData := make(map[int64]map[string]interface{})
				if len(estimateDataMap) > 0 {
					processedEconomicData = processalleventEconomicImpactDataParallel(estimateDataMap)
				}

				categoryNamesMap := make(map[int64][]string)
				if len(eventIDsForEditions) > 0 {
					log.Printf("allevent chunk %d: Fetching category names for %d events", chunkNum, len(eventIDsForEditions))
					startTime = time.Now()
					categoryNamesMap = fetchalleventCategoryNamesForEvents(mysqlDB, eventIDsForEditions)
					categoryTime := time.Since(startTime)
					log.Printf("allevent chunk %d: Retrieved category names for %d events in %v", chunkNum, len(categoryNamesMap), categoryTime)
				}

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

				timingDataMap := make(map[uint64][]string)
				if len(editionData) > 0 {
					log.Printf("allevent chunk %d: Fetching timing data for event-edition combinations", chunkNum)
					startTime = time.Now()
					timingDataMap = fetchalleventTimingDataForBatch(mysqlDB, editionData)
					timingTime := time.Since(startTime)
					log.Printf("allevent chunk %d: Retrieved timing data for %d event-edition combinations in %v", chunkNum, len(timingDataMap), timingTime)
				}

				for eventID, editions := range allevents {
					eventData := eventDataLookup[eventID]

					economicData := processedEconomicData[eventID]

					if eventData != nil {
						for _, edition := range editions {
							company, venue, city, companyCity, venueCity := resolveRelatedDataForEdition(
								edition,
								companyLookup,
								venueLookup,
								cityLookup,
							)

							esInfoMap := esData[eventID]

							editionDomain, companyDomain := extractDomainsForEdition(edition, company)

							editionType := determinealleventType(
								edition["edition_start_date"],
								currentEditionStartDates[eventID],
								edition["edition_id"].(int64),
								currentEditionIDs[eventID],
							)

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

							editionCountryISO := strings.ToUpper(shared.ConvertToString(eventData["country"]))

							editionCityLocationChID, companyCityLocationChID, venueCityLocationChID,
								editionCityStateLocationChID, venueLocationChID := computeAllLocationIDs(
								city,
								company,
								companyCity,
								venue,
								venueCity,
								editionCountryISO,
								cityIDLookup,
								stateIDLookup,
								venueIDLookup,
							)

							record := buildAlleventRecord(
								eventData,
								edition,
								company,
								venue,
								city,
								companyCity,
								venueCity,
								esInfoMap,
								economicData,
								estimateDataMap[eventID],
								eventTypesMap,
								categoryNamesMap,
								ticketDataMap,
								ticketTypeMap,
								timingDataMap,
								eventID,
								editionType,
								currentEditionIDs[eventID],
								editions,
								editionCountryISO,
								editionDomain,
								companyDomain,
								editionCityLocationChID,
								companyCityLocationChID,
								venueCityLocationChID,
								editionCityStateLocationChID,
								venueLocationChID,
							)

							clickHouseRecords = append(clickHouseRecords, record)

							globalCountMutex.Lock()
							*totalRecordsProcessed++
							*totalRecordsInserted++
							globalCountMutex.Unlock()

							if company != nil && venue != nil && city != nil {
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

				log.Printf("allevent chunk %d: Total records collected: %d", chunkNum, len(clickHouseRecords))
				if len(clickHouseRecords) > 0 {
					log.Printf("allevent chunk %d: Attempting to insert %d records into ClickHouse...", chunkNum, len(clickHouseRecords))

					insertErr := insertalleventDataIntoClickHouse(clickhouseConn, clickHouseRecords, config.ClickHouseWorkers, config)

					if insertErr != nil {
						log.Printf("allevent chunk %d: ClickHouse insertion failed for batch %d, writing %d records to CSV for retry: %v", chunkNum, batchNumber, len(clickHouseRecords), insertErr)
						if err := writeFailedBatchToCSV(clickHouseRecords, batchNumber, chunkNum); err != nil {
							log.Printf("ERROR: Failed to write failed batch to CSV: %v", err)
						} else {
							log.Printf("allevent chunk %d: Successfully wrote batch %d to CSV for retry", chunkNum, batchNumber)
						}
					} else {
						log.Printf("allevent chunk %d: Successfully inserted %d records into ClickHouse", chunkNum, len(clickHouseRecords))
					}
				} else {
					log.Printf("allevent chunk %d: No records to insert into ClickHouse", chunkNum)
				}
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

func buildalleventCityIDLookupFromLocationCh(clickhouseConn driver.Conn, locationTableName string) (map[string]uint32, error) {
	log.Printf("Checking ClickHouse connection health before querying cities from %s", locationTableName)
	connectionCheckErr := shared.RetryWithBackoff(
		func() error {
			return shared.CheckClickHouseConnectionAlive(clickhouseConn)
		},
		3,
	)
	if connectionCheckErr != nil {
		return nil, fmt.Errorf("ClickHouse connection is not alive after retries: %w", connectionCheckErr)
	}
	log.Printf("ClickHouse connection is alive, proceeding with cities query")

	query := fmt.Sprintf(`
		SELECT id, name, iso
		FROM %s
		WHERE location_type = 'CITY'
	`, locationTableName)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var rows driver.Rows
	var err error
	queryErr := shared.RetryWithBackoff(
		func() error {
			rows, err = clickhouseConn.Query(ctx, query)
			if err != nil {
				return fmt.Errorf("failed to query %s for cities: %v", locationTableName, err)
			}
			return nil
		},
		3,
	)
	if queryErr != nil {
		return nil, queryErr
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
			keyWithISO := fmt.Sprintf("%s|%s", cityNameStr, isoStr)
			keyWithoutISO := cityNameStr
			if isoStr != "" {
				lookup[keyWithISO] = locationChID
			}
			if _, exists := lookup[keyWithoutISO]; !exists {
				lookup[keyWithoutISO] = locationChID
			}
		}
	}

	log.Printf("Built city ID lookup: %d cities mapped from %s", len(lookup), locationTableName)
	return lookup, nil
}

func buildalleventStateIDLookupFromLocationCh(clickhouseConn driver.Conn, locationTableName string) (map[string]uint32, error) {
	log.Printf("Checking ClickHouse connection health before querying states from %s", locationTableName)
	connectionCheckErr := shared.RetryWithBackoff(
		func() error {
			return shared.CheckClickHouseConnectionAlive(clickhouseConn)
		},
		3,
	)
	if connectionCheckErr != nil {
		return nil, fmt.Errorf("ClickHouse connection is not alive after retries: %w", connectionCheckErr)
	}
	log.Printf("ClickHouse connection is alive, proceeding with states query")

	query := fmt.Sprintf(`
		SELECT id, name, iso
		FROM %s
		WHERE location_type = 'STATE'
	`, locationTableName)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var rows driver.Rows
	var err error
	queryErr := shared.RetryWithBackoff(
		func() error {
			rows, err = clickhouseConn.Query(ctx, query)
			if err != nil {
				return fmt.Errorf("failed to query %s for states: %v", locationTableName, err)
			}
			return nil
		},
		3,
	)
	if queryErr != nil {
		return nil, queryErr
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
			keyWithISO := fmt.Sprintf("%s|%s", stateNameStr, isoStr)
			keyWithoutISO := stateNameStr
			if isoStr != "" {
				lookup[keyWithISO] = locationChID
			}
			if _, exists := lookup[keyWithoutISO]; !exists {
				lookup[keyWithoutISO] = locationChID
			}
		}
	}

	log.Printf("Built state ID lookup: %d states mapped from %s", len(lookup), locationTableName)
	return lookup, nil
}

func buildalleventVenueIDLookupFromLocationCh(clickhouseConn driver.Conn, locationTableName string) (map[string]uint32, error) {
	log.Printf("Checking ClickHouse connection health before querying venues from %s", locationTableName)
	connectionCheckErr := shared.RetryWithBackoff(
		func() error {
			return shared.CheckClickHouseConnectionAlive(clickhouseConn)
		},
		3,
	)
	if connectionCheckErr != nil {
		return nil, fmt.Errorf("ClickHouse connection is not alive after retries: %w", connectionCheckErr)
	}
	log.Printf("ClickHouse connection is alive, proceeding with venues query")

	query := fmt.Sprintf(`
		SELECT id, name, iso
		FROM %s
		WHERE location_type = 'VENUE'
	`, locationTableName)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var rows driver.Rows
	var err error
	queryErr := shared.RetryWithBackoff(
		func() error {
			rows, err = clickhouseConn.Query(ctx, query)
			if err != nil {
				return fmt.Errorf("failed to query %s for venues: %v", locationTableName, err)
			}
			return nil
		},
		3,
	)
	if queryErr != nil {
		return nil, queryErr
	}
	defer rows.Close()

	lookup := make(map[string]uint32)
	for rows.Next() {
		var locationChID uint32
		var venueName *string
		var countryISO *string
		if err := rows.Scan(&locationChID, &venueName, &countryISO); err != nil {
			log.Printf("Warning: Failed to scan venue row: %v", err)
			continue
		}

		if venueName != nil && *venueName != "" {
			venueNameStr := strings.TrimSpace(*venueName)
			isoStr := ""
			if countryISO != nil && *countryISO != "" {
				isoStr = strings.ToUpper(strings.TrimSpace(*countryISO))
				if isoStr == "NAN" {
					isoStr = ""
				}
			}
			keyWithISO := fmt.Sprintf("%s|%s", venueNameStr, isoStr)
			keyWithoutISO := venueNameStr
			if isoStr != "" {
				lookup[keyWithISO] = locationChID
			}
			if _, exists := lookup[keyWithoutISO]; !exists {
				lookup[keyWithoutISO] = locationChID
			}
		}
	}

	log.Printf("Built venue ID lookup: %d venues mapped from %s", len(lookup), locationTableName)
	return lookup, nil
}

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
		ticketUUID := shared.GenerateUUIDFromString(fmt.Sprintf("%d-%s", eventIDUint32, shared.ConvertToString(ticket["created"])))

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

func sanitizeInvalidJSONEscapes(jsonStr string) string {
	var result strings.Builder
	result.Grow(len(jsonStr))

	validEscapes := map[byte]bool{
		'"':  true, // \"
		'\\': true, // \\
		'/':  true, // \/
		'b':  true, // \b
		'f':  true, // \f
		'n':  true, // \n
		'r':  true, // \r
		't':  true, // \t
		'u':  true, // \uXXXX
	}

	bytes := []byte(jsonStr)
	for i := 0; i < len(bytes); i++ {
		if bytes[i] == '\\' && i+1 < len(bytes) {
			nextChar := bytes[i+1]
			if validEscapes[nextChar] {
				result.WriteByte(bytes[i])
				result.WriteByte(nextChar)
				i++
				if nextChar == 'u' && i+4 < len(bytes) {
					allHex := true
					for j := i + 1; j <= i+4 && j < len(bytes); j++ {
						c := bytes[j]
						if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
							allHex = false
							break
						}
					}
					if allHex {
						result.Write(bytes[i+1 : i+5])
						i += 4
					}
				}
			} else {
				result.WriteByte(nextChar)
				i++
			}
		} else {
			result.WriteByte(bytes[i])
		}
	}

	return result.String()
}

func fetchalleventTimingDataForBatch(db *sql.DB, editionData []map[string]interface{}) map[uint64][]string {
	result := make(map[uint64][]string)
	eventEditionPairs := make(map[uint64]bool)
	var eventIDs []int64
	var editionIDs []int64

	for _, edition := range editionData {
		eventID, eventOK := edition["event"].(int64)
		editionID, editionOK := edition["edition_id"].(int64)
		if eventOK && editionOK {
			eventIDUint32 := shared.ConvertToUInt32(eventID)
			editionIDUint32 := shared.ConvertToUInt32(editionID)
			key := uint64(eventIDUint32)<<32 | uint64(editionIDUint32)
			if !eventEditionPairs[key] {
				eventEditionPairs[key] = true
				eventIDs = append(eventIDs, eventID)
				editionIDs = append(editionIDs, editionID)
			}
		}
	}

	if len(eventEditionPairs) == 0 {
		return result
	}

	batchSize := 500
	for i := 0; i < len(eventIDs); i += batchSize {
		end := i + batchSize
		if end > len(eventIDs) {
			end = len(eventIDs)
		}

		batchEventIDs := eventIDs[i:end]
		batchEditionIDs := editionIDs[i:end]

		var conditions []string
		var args []interface{}
		for j := 0; j < len(batchEventIDs); j++ {
			conditions = append(conditions, "(event = ? AND event_edition = ?)")
			args = append(args, batchEventIDs[j], batchEditionIDs[j])
		}

		query := fmt.Sprintf(`
			SELECT event, event_edition, value
			FROM event_data
			WHERE title = 'timing'
			AND (%s)
		`, strings.Join(conditions, " OR "))

		rows, err := db.Query(query, args...)
		if err != nil {
			log.Printf("Error fetching timing data batch %d-%d: %v", i+1, end, err)
			continue
		}

		func() {
			defer rows.Close()
			for rows.Next() {
				var eventID sql.NullInt64
				var eventEdition sql.NullInt64
				var value sql.NullString

				if err := rows.Scan(&eventID, &eventEdition, &value); err != nil {
					log.Printf("Error scanning timing data: %v", err)
					continue
				}

				if !eventID.Valid || !eventEdition.Valid {
					continue
				}

				eventIDUint32 := shared.ConvertToUInt32(eventID.Int64)
				editionIDUint32 := shared.ConvertToUInt32(eventEdition.Int64)
				key := uint64(eventIDUint32)<<32 | uint64(editionIDUint32)

				if value.Valid && value.String != "" {
					jsonStr := value.String
					jsonStr = strings.ReplaceAll(jsonStr, ":NULL", ":null")
					jsonStr = strings.ReplaceAll(jsonStr, ": NULL", ": null")
					jsonStr = strings.ReplaceAll(jsonStr, ",NULL", ",null")
					jsonStr = strings.ReplaceAll(jsonStr, ", NULL", ", null")
					jsonStr = strings.ReplaceAll(jsonStr, "[NULL", "[null")
					jsonStr = strings.ReplaceAll(jsonStr, "[ NULL", "[ null")
					jsonStr = strings.ReplaceAll(jsonStr, "NULL]", "null]")
					jsonStr = strings.ReplaceAll(jsonStr, " NULL]", " null]")
					jsonStr = sanitizeInvalidJSONEscapes(jsonStr)

					var timingData interface{}
					if err := json.Unmarshal([]byte(jsonStr), &timingData); err != nil {
						log.Printf("Error parsing timing JSON for event %d, edition %d: %v. Original value: %s, Sanitized: %s", eventID.Int64, eventEdition.Int64, err, value.String, jsonStr)
						result[key] = []string{}
						continue
					}

					var timings []string
					switch v := timingData.(type) {
					case []interface{}:
						for _, item := range v {
							if str, ok := item.(string); ok {
								timings = append(timings, str)
							} else if itemMap, ok := item.(map[string]interface{}); ok {
								if jsonBytes, err := json.Marshal(itemMap); err == nil {
									timings = append(timings, string(jsonBytes))
								} else {
									timings = append(timings, shared.ConvertToString(item))
								}
							} else {
								timings = append(timings, shared.ConvertToString(item))
							}
						}
					case []string:
						timings = v
					case map[string]interface{}:
						if jsonBytes, err := json.Marshal(v); err == nil {
							timings = []string{string(jsonBytes)}
						} else {
							log.Printf("Error marshaling timing map to JSON for event %d, edition %d: %v", eventID.Int64, eventEdition.Int64, err)
							timings = []string{}
						}
					case string:
						timings = []string{v}
					default:
						if jsonBytes, err := json.Marshal(v); err == nil {
							timings = []string{string(jsonBytes)}
						} else {
							timings = []string{shared.ConvertToString(v)}
						}
					}

					result[key] = timings
				} else {
					result[key] = []string{}
				}
			}
		}()
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
		"_source": []string{"id", "description", "exhibitors", "speakers", "totalSponsor", "following", "punchline", "frequency", "city", "hybrid", "logo", "pricing", "total_edition", "avg_rating", "eventEstimatedTag", "impactScore", "inboundScore", "internationalScore", "repeatSentimentChangePercentage", "repeatSentiment", "reputationSentiment", "audienceZone", "yoyGrowth", "futureExpexctedStartDate", "futureExpexctedEndDate", "finalEstimate", "highEstimate", "lowEstimate"},
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
				if floatVal, ok := val.(float64); ok {
					if floatVal >= 0 && floatVal == float64(uint32(floatVal)) {
						return uint32(floatVal)
					}
					return nil
				}
				if intVal, ok := val.(int); ok {
					if intVal >= 0 {
						return uint32(intVal)
					}
					return nil
				}
				if int64Val, ok := val.(int64); ok {
					if int64Val >= 0 && int64Val <= math.MaxUint32 {
						return uint32(int64Val)
					}
					return nil
				}
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
			"event_city":                      source["city"],
			"event_logo":                      shared.ConvertToString(source["logo"]),
			"event_pricing":                   shared.ConvertToString(source["pricing"]),
			"total_edition":                   convertedTotalEdition,
			"avg_rating":                      source["avg_rating"],
			"eventEstimatedTag":               shared.ConvertToString(source["eventEstimatedTag"]),
			"impactScore":                     convertStringToUInt32("impactScore"),
			"inboundScore":                    convertStringToUInt32("inboundScore"),
			"internationalScore":              convertStringToUInt32("internationalScore"),
			"repeatSentimentChangePercentage": convertToFloat64("repeatSentimentChangePercentage"),
			"repeatSentiment":                 convertStringToUInt32("repeatSentiment"),
			"reputationChangePercentage":      convertToFloat64("reputationSentiment"),
			"audienceZone":                    shared.ConvertToString(source["audienceZone"]),
			"yoyGrowth":                       convertStringToUInt32("yoyGrowth"),
			"futureExpexctedStartDate":        source["futureExpexctedStartDate"],
			"futureExpexctedEndDate":          source["futureExpexctedEndDate"],
			"finalEstimate":                   source["finalEstimate"],
			"highEstimate":                    source["highEstimate"],
			"lowEstimate":                     source["lowEstimate"],
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
	var wg sync.WaitGroup

	workersLaunched := 0
	for i := 0; i < numWorkers; i++ {
		start := i * batchSize
		end := start + batchSize
		if end > len(records) {
			end = len(records)
		}
		if start >= len(records) {
			break
		}

		workersLaunched++
		semaphore <- struct{}{}
		wg.Add(1)
		go func(start, end int) {
			defer func() {
				<-semaphore
				wg.Done()
			}()
			batch := records[start:end]
			err := insertalleventDataSingleWorker(clickhouseConn, batch, config)
			results <- err
		}(start, end)
	}

	for i := 0; i < workersLaunched; i++ {
		if err := <-results; err != nil {
			wg.Wait()
			return err
		}
	}

	wg.Wait()

	return nil
}

func insertalleventDataSingleWorker(clickhouseConn driver.Conn, records []map[string]interface{}, config shared.Config) error {
	if len(records) == 0 {
		return nil
	}

	const maxBatchSize = 10000
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

	connectionCheckErr := shared.RetryWithBackoff(
		func() error {
			return shared.CheckClickHouseConnectionAlive(clickhouseConn)
		},
		3,
	)
	if connectionCheckErr != nil {
		return fmt.Errorf("ClickHouse connection is not alive after retries: %w", connectionCheckErr)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 900*time.Second)
	defer cancel()

	insertSQL := `
		INSERT INTO allevent_temp (
			event_id, event_uuid, event_name, event_abbr_name, event_description, event_punchline, event_avgRating, 10timesEventPageUrl,
			start_date, end_date,
			edition_id, edition_country, edition_city, edition_city_name, edition_city_state_id, edition_city_state, edition_city_lat, edition_city_long,
			company_id, company_uuid, company_name, company_domain, company_website, companyLogoUrl, company_country, company_state, company_city, company_city_name, company_address,
			venue_id, venue_name, venue_country, venue_city, venue_city_name, venue_lat, venue_long,
			published, status, editions_audiance_type, edition_functionality, edition_website, edition_domain,
			edition_type, event_editions, event_format, event_followers, edition_followers, event_exhibitor, edition_exhibitor,
			exhibitors_upper_bound, exhibitors_lower_bound, exhibitors_mean,
			event_sponsor, edition_sponsor, event_speaker, edition_speaker,
			event_created, event_updated, edition_created, event_hybrid, isBranded, eventBrandId, eventSeriesId, maturity,
			event_pricing, tickets, timings, event_logo, event_estimatedVisitors, estimatedVisitorsMean, estimatedSize, event_frequency, impactScore, inboundScore, internationalScore, repeatSentimentChangePercentage, repeatSentiment, reputationChangePercentage, audienceZone,
			inboundPercentage, inboundAttendance, internationalPercentage, internationalAttendance,
			event_economic_FoodAndBevarage, event_economic_Transportation, event_economic_Accomodation, event_economic_Utilities, event_economic_flights, event_economic_value,
			event_economic_dayWiseEconomicImpact, event_economic_breakdown, event_economic_impact, keywords, event_score, yoyGrowth, futureExpexctedStartDate, futureExpexctedEndDate, PrimaryEventType, verifiedOn, last_updated_at, version
		)
	`

	var batch driver.Batch
	var err error
	maxRetries := 3
	for retryCount := 0; retryCount < maxRetries; retryCount++ {
		if retryCount > 0 {
			log.Printf("Checking ClickHouse connection health before retry %d/%d", retryCount+1, maxRetries)
			connectionCheckErr := shared.RetryWithBackoff(
				func() error {
					return shared.CheckClickHouseConnectionAlive(clickhouseConn)
				},
				3,
			)
			if connectionCheckErr != nil {
				log.Printf("WARNING: ClickHouse connection health check failed on retry %d: %v", retryCount+1, connectionCheckErr)
			}
		}

		batch, err = clickhouseConn.PrepareBatch(ctx, insertSQL)
		if err == nil {
			break
		}

		if retryCount < maxRetries-1 {
			log.Printf("WARNING: ClickHouse PrepareBatch error (attempt %d/%d), rebuilding connection: %v", retryCount+1, maxRetries, err)
			newConn, connErr := utils.SetupNativeClickHouseConnection(config)
			if connErr != nil {
				log.Printf("ERROR: Failed to rebuild ClickHouse connection (attempt %d/%d): %v", retryCount+1, maxRetries, connErr)
				continue
			}

			clickhouseConn = newConn
			log.Printf("Successfully rebuilt ClickHouse connection, retrying PrepareBatch (attempt %d/%d)", retryCount+2, maxRetries)
		}
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
			alleventRecord.TenTimesEventPageUrl,            // 10timesEventPageUrl: Nullable(String)
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
			alleventRecord.CompanyUUID,                     // company_uuid: UUID NOT NULL
			alleventRecord.CompanyName,                     // company_name: Nullable(String)
			alleventRecord.CompanyDomain,                   // company_domain: Nullable(String)
			alleventRecord.CompanyWebsite,                  // company_website: Nullable(String)
			alleventRecord.CompanyLogoUrl,                  // companyLogoUrl: Nullable(String)
			alleventRecord.CompanyCountry,                  // company_country: LowCardinality(Nullable(FixedString(2)))
			alleventRecord.CompanyState,                    // company_state: LowCardinality(Nullable(String))
			alleventRecord.CompanyCity,                     // company_city: Nullable(UInt32)
			alleventRecord.CompanyCityName,                 // company_city_name: Nullable(String)
			alleventRecord.CompanyAddress,                  // company_address: Nullable(String)
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
			alleventRecord.EventEditions,                   // event_editions: Nullable(UInt32)
			alleventRecord.EventFormat,                     // event_format: LowCardinality(Nullable(String))
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
			alleventRecord.EventHybrid,                     // event_hybrid: Nullable(UInt8)
			alleventRecord.IsBranded,                       // isBranded: Nullable(UInt32)
			alleventRecord.EventBrandId,                    // eventBrandId: Nullable(UUID)
			alleventRecord.EventSeriesId,                   // eventSeriesId: Nullable(UUID)
			alleventRecord.Maturity,                        // maturity: LowCardinality(Nullable(String))
			alleventRecord.EventPricing,                    // event_pricing: LowCardinality(Nullable(String))
			alleventRecord.Tickets,                         // tickets: Array(String)
			alleventRecord.Timings,                         // timings: Array(String)
			alleventRecord.EventLogo,                       // event_logo: Nullable(String)
			alleventRecord.EventEstimatedVisitors,          // event_estimatedVisitors: LowCardinality(Nullable(String))
			alleventRecord.EstimatedVisitorsMean,           // estimatedVisitorsMean: Nullable(UInt32)
			alleventRecord.EstimatedSize,                   // estimatedSize: LowCardinality(Nullable(String))
			alleventRecord.EventFrequency,                  // event_frequency: LowCardinality(Nullable(String))
			alleventRecord.ImpactScore,                     // impactScore: Nullable(UInt32)
			alleventRecord.InboundScore,                    // inboundScore: Nullable(UInt32)
			alleventRecord.InternationalScore,              // internationalScore: Nullable(UInt32)
			alleventRecord.RepeatSentimentChangePercentage, // repeatSentimentChangePercentage: Nullable(Float64)
			alleventRecord.RepeatSentiment,                 // repeatSentiment: Nullable(UInt32)
			alleventRecord.ReputationChangePercentage,      // reputationChangePercentage: Nullable(Float64)
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
			alleventRecord.YoYGrowth,                       // yoyGrowth: Nullable(UInt32)
			alleventRecord.FutureExpectedStartDate,         // futureExpexctedStartDate: Nullable(Date)
			alleventRecord.FutureExpectedEndDate,           // futureExpexctedEndDate: Nullable(Date)
			alleventRecord.PrimaryEventType,                // PrimaryEventType: Nullable(UUID)
			alleventRecord.VerifiedOn,                      // verifiedOn: Nullable(Date)
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

type EventEditionPair struct {
	EventID   uint32
	EditionID uint32
	BatchNum  int
}

func isMemoryLimitError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "MEMORY_LIMIT_EXCEEDED") ||
		strings.Contains(errStr, "memory limit exceeded")
}

func extractChunkNumFromFilename(filename string) int {
	base := filepath.Base(filename)
	parts := strings.Split(base, "_")
	if len(parts) >= 4 && parts[0] == "failed" && parts[1] == "batches" && parts[2] == "chunk" {
		if chunkNum, err := strconv.Atoi(strings.TrimSuffix(parts[3], ".csv")); err == nil {
			return chunkNum
		}
	}
	return -1
}

func writeFailedBatchToCSV(records []map[string]interface{}, batchNum int, chunkNum int) error {
	dir := "failed_batches"
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	filepath := filepath.Join(dir, fmt.Sprintf("failed_batches_chunk_%d.csv", chunkNum))

	file, err := os.OpenFile(filepath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filepath, err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	fileInfo, err := file.Stat()
	if err == nil && fileInfo.Size() == 0 {
		if err := writer.Write([]string{"event_id", "edition_id", "batch_num"}); err != nil {
			return fmt.Errorf("failed to write CSV header: %w", err)
		}
	}

	for _, record := range records {
		eventID := shared.SafeConvertToUInt32(record["event_id"])
		editionID := shared.SafeConvertToUInt32(record["edition_id"])

		row := []string{
			strconv.FormatUint(uint64(eventID), 10),
			strconv.FormatUint(uint64(editionID), 10),
			strconv.Itoa(batchNum),
		}

		if err := writer.Write(row); err != nil {
			return fmt.Errorf("failed to write CSV row: %w", err)
		}
	}

	log.Printf("Written %d failed pairs to %s (batch %d)", len(records), filepath, batchNum)
	return nil
}

func readCSVGroupedByBatch(filename string) (map[int][]EventEditionPair, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filename, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV: %w", err)
	}

	if len(records) == 0 {
		return make(map[int][]EventEditionPair), nil
	}

	startIdx := 0
	if len(records) > 0 && len(records[0]) > 0 && records[0][0] == "event_id" {
		startIdx = 1
	}

	pairsByBatch := make(map[int][]EventEditionPair)

	for i := startIdx; i < len(records); i++ {
		if len(records[i]) < 3 {
			continue
		}

		eventID, err1 := strconv.ParseUint(records[i][0], 10, 32)
		editionID, err2 := strconv.ParseUint(records[i][1], 10, 32)
		batchNum, err3 := strconv.Atoi(records[i][2])

		if err1 != nil || err2 != nil || err3 != nil {
			log.Printf("WARNING: Skipping invalid row in %s: %v", filename, records[i])
			continue
		}

		pair := EventEditionPair{
			EventID:   uint32(eventID),
			EditionID: uint32(editionID),
			BatchNum:  batchNum,
		}

		pairsByBatch[batchNum] = append(pairsByBatch[batchNum], pair)
	}

	return pairsByBatch, nil
}

func checkExistenceInClickHouse(clickhouseConn driver.Conn, pairs []EventEditionPair, config shared.Config) (map[uint64]bool, error) {
	if len(pairs) == 0 {
		return make(map[uint64]bool), nil
	}

	tableName := shared.GetTableNameWithDB("allevent_temp", config)

	batchSize := 10000
	existingPairs := make(map[uint64]bool)

	for i := 0; i < len(pairs); i += batchSize {
		end := i + batchSize
		if end > len(pairs) {
			end = len(pairs)
		}

		batch := pairs[i:end]
		tuples := make([]string, len(batch))
		for j, pair := range batch {
			tuples[j] = fmt.Sprintf("(%d, %d)", pair.EventID, pair.EditionID)
		}

		query := fmt.Sprintf(
			"SELECT event_id, edition_id FROM %s WHERE (event_id, edition_id) IN (%s)",
			tableName,
			strings.Join(tuples, ","),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		rows, err := clickhouseConn.Query(ctx, query)
		cancel()

		if err != nil {
			return nil, fmt.Errorf("failed to query existence: %w", err)
		}

		for rows.Next() {
			var eventID uint32
			var editionID uint32
			if err := rows.Scan(&eventID, &editionID); err != nil {
				rows.Close()
				return nil, fmt.Errorf("failed to scan row: %w", err)
			}
			key := uint64(eventID)<<32 | uint64(editionID)
			existingPairs[key] = true
		}
		rows.Close()
	}

	return existingPairs, nil
}

func checkExistenceByDuplicateColumns(clickhouseConn driver.Conn, records []map[string]interface{}, config shared.Config) (map[string]bool, error) {
	if len(records) == 0 {
		return make(map[string]bool), nil
	}

	tableName := shared.GetTableNameWithDB("allevent_temp", config)
	existingRecords := make(map[string]bool)
	batchSize := 1000

	for i := 0; i < len(records); i += batchSize {
		end := i + batchSize
		if end > len(records) {
			end = len(records)
		}

		batch := records[i:end]
		tuples := make([]string, 0, len(batch))

		for _, record := range batch {
			eventID := shared.ConvertToUInt32(record["event_id"])
			editionID := shared.ConvertToUInt32(record["edition_id"])
			published := shared.ConvertToUInt32(record["published"])
			status := shared.ConvertToUInt32(record["status"])
			editionType := shared.ConvertToUInt32(record["edition_type"])

			tuple := fmt.Sprintf("(%d, %d, %d, %d, %d)", published, status, editionType, eventID, editionID)
			tuples = append(tuples, tuple)
		}

		if len(tuples) == 0 {
			continue
		}

		query := fmt.Sprintf(
			"SELECT toString(published), toString(status), toString(edition_type), toString(event_id), toString(edition_id) FROM %s WHERE (published, status, edition_type, event_id, edition_id) IN (%s)",
			tableName,
			strings.Join(tuples, ","),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		rows, err := clickhouseConn.Query(ctx, query)
		cancel()

		if err != nil {
			return nil, fmt.Errorf("failed to query existence by duplicate columns: %w", err)
		}

		for rows.Next() {
			var publishedStr, statusStr, editionTypeStr, eventIDStr, editionIDStr string
			if err := rows.Scan(&publishedStr, &statusStr, &editionTypeStr, &eventIDStr, &editionIDStr); err != nil {
				rows.Close()
				return nil, fmt.Errorf("failed to scan row: %w", err)
			}
			key := fmt.Sprintf("%s|%s|%s|%s|%s", publishedStr, statusStr, editionTypeStr, eventIDStr, editionIDStr)
			existingRecords[key] = true
		}
		rows.Close()
	}

	return existingRecords, nil
}

func rebuildRecordsForFailedBatch(
	mysqlDB *sql.DB,
	clickhouseConn driver.Conn,
	esClient *elasticsearch.Client,
	pairs []EventEditionPair,
	config shared.Config,
) ([]map[string]interface{}, error) {
	if len(pairs) == 0 {
		return nil, nil
	}

	eventIDSet := make(map[int64]bool)
	editionIDSet := make(map[int64]bool)
	pairMap := make(map[uint64]EventEditionPair)

	for _, pair := range pairs {
		eventIDSet[int64(pair.EventID)] = true
		editionIDSet[int64(pair.EditionID)] = true
		key := uint64(pair.EventID)<<32 | uint64(pair.EditionID)
		pairMap[key] = pair
	}

	eventIDs := make([]int64, 0, len(eventIDSet))
	for id := range eventIDSet {
		eventIDs = append(eventIDs, id)
	}

	editionIDs := make([]int64, 0, len(editionIDSet))
	for id := range editionIDSet {
		editionIDs = append(editionIDs, id)
	}

	log.Printf("Rebuilding records for %d event-edition pairs (%d unique events, %d unique editions)", len(pairs), len(eventIDs), len(editionIDs))

	expectedPairsMap := make(map[uint64]bool)
	for _, pair := range pairs {
		key := uint64(pair.EventID)<<32 | uint64(pair.EditionID)
		expectedPairsMap[key] = true
	}

	sampleSize := 10
	if len(editionIDs) < sampleSize {
		sampleSize = len(editionIDs)
	}
	log.Printf("Fetching editions by edition_id for %d edition IDs (sample: %v)", len(editionIDs), editionIDs[:sampleSize])
	editionData := fetchEditionsByEditionIDs(mysqlDB, editionIDs)
	log.Printf("Fetched %d editions from MySQL for %d edition IDs", len(editionData), len(editionIDs))

	if len(editionData) > 0 {
		sampleSize2 := 10
		if len(editionData) < sampleSize2 {
			sampleSize2 = len(editionData)
		}
		fetchedEditionIDs := make([]int64, 0, sampleSize2)
		for _, ed := range editionData[:sampleSize2] {
			edID := int64(shared.ConvertToUInt32(ed["edition_id"]))
			fetchedEditionIDs = append(fetchedEditionIDs, edID)
		}
		log.Printf("Sample fetched edition IDs: %v", fetchedEditionIDs)
	} else {
		log.Printf("WARNING: No editions fetched! Check if edition IDs exist in MySQL")
	}

	filteredEditionData := editionData

	actualEventIDs := make([]int64, 0)
	actualEventIDSet := make(map[int64]bool)
	for _, edition := range filteredEditionData {
		if eventIDVal, ok := edition["event"]; ok {
			eventID := int64(shared.ConvertToUInt32(eventIDVal))
			if eventID > 0 && !actualEventIDSet[eventID] {
				actualEventIDs = append(actualEventIDs, eventID)
				actualEventIDSet[eventID] = true
			}
		}
	}
	log.Printf("Extracted %d unique event IDs from fetched editions", len(actualEventIDs))

	companyIDs := extractalleventCompanyIDs(filteredEditionData)
	venueIDs := extractalleventVenueIDs(filteredEditionData)
	editionCityIDs := extractalleventCityIDs(filteredEditionData)

	companyData := fetchalleventCompanyDataParallel(mysqlDB, companyIDs)
	venueData := fetchalleventVenueDataParallel(mysqlDB, venueIDs)

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

	cityData := shared.FetchCityDataParallel(mysqlDB, allCityIDs, config.NumWorkers)

	esData := fetchalleventElasticsearchDataForEvents(esClient, config.ElasticsearchIndex, actualEventIDs)

	locationTableName := shared.GetClickHouseTableName("location_ch", config)
	cityIDLookup, err := buildalleventCityIDLookupFromLocationCh(clickhouseConn, locationTableName)
	if err != nil {
		log.Printf("WARNING - Failed to build city ID lookup: %v", err)
		cityIDLookup = make(map[string]uint32)
	}
	stateIDLookup, err := buildalleventStateIDLookupFromLocationCh(clickhouseConn, locationTableName)
	if err != nil {
		log.Printf("WARNING - Failed to build state ID lookup: %v", err)
		stateIDLookup = make(map[string]uint32)
	}
	venueIDLookup, err := buildalleventVenueIDLookupFromLocationCh(clickhouseConn, locationTableName)
	if err != nil {
		log.Printf("WARNING - Failed to build venue ID lookup: %v", err)
		venueIDLookup = make(map[string]uint32)
	}

	companyLookup := make(map[int64]map[string]interface{})
	for _, company := range companyData {
		if companyID, ok := company["id_10x"].(int64); ok {
			companyLookup[companyID] = company
		}
	}

	venueLookup := make(map[int64]map[string]interface{})
	for _, venue := range venueData {
		if venueID, ok := venue["id"].(int64); ok {
			venueLookup[venueID] = venue
		}
	}

	cityLookup := make(map[int64]map[string]interface{})
	for _, city := range cityData {
		if cityID, ok := city["id"].(int64); ok {
			cityLookup[cityID] = city
		}
	}

	eventDataLookup := make(map[int64]map[string]interface{})
	eventDataForEditions := fetchalleventEventDataForBatch(mysqlDB, actualEventIDs)
	for _, eventData := range eventDataForEditions {
		if eventID, ok := eventData["id"].(int64); ok {
			eventDataLookup[eventID] = eventData
		}
	}

	estimateDataMap := fetchalleventEstimateDataForBatch(mysqlDB, actualEventIDs)
	eventTypesMap := fetchalleventEventTypesForBatch(mysqlDB, actualEventIDs)
	categoryNamesMap := fetchalleventCategoryNamesForEvents(mysqlDB, actualEventIDs)
	rawTicketData := fetchalleventTicketDataForBatch(mysqlDB, actualEventIDs)
	ticketDataMap := processalleventTicketData(rawTicketData)
	ticketTypeMap := make(map[int64]string)
	for _, ticket := range rawTicketData {
		if eventID, ok := ticket["event"].(int64); ok {
			if _, exists := ticketTypeMap[eventID]; !exists {
				ticketType := shared.SafeConvertToString(ticket["type"])
				if ticketType != "" {
					ticketTypeMap[eventID] = ticketType
				}
			}
		}
	}
	timingDataMap := fetchalleventTimingDataForBatch(mysqlDB, filteredEditionData)
	processedEconomicData := processalleventEconomicImpactDataParallel(estimateDataMap)

	allevents := make(map[int64][]map[string]interface{})
	currentEditionStartDates := make(map[int64]interface{})
	currentEditionIDs := make(map[int64]int64)

	for _, edition := range filteredEditionData {
		eventID := int64(shared.ConvertToUInt32(edition["event"]))
		if eventID > 0 {
			allevents[eventID] = append(allevents[eventID], edition)
			if currentEditionID, exists := edition["current_edition_id"]; exists {
				editionID := int64(shared.ConvertToUInt32(edition["edition_id"]))
				currentEditionIDInt64 := int64(shared.ConvertToUInt32(currentEditionID))
				if currentEditionIDInt64 == editionID {
					currentEditionStartDates[eventID] = edition["edition_start_date"]
					currentEditionIDs[eventID] = editionID
				}
			}
		}
	}

	log.Printf("Grouped %d editions into %d events", len(filteredEditionData), len(allevents))

	records := make([]map[string]interface{}, 0, len(pairs))

	for _, pair := range pairs {
		expectedEventID := int64(pair.EventID)
		editionID := int64(pair.EditionID)

		var targetEdition map[string]interface{}
		var actualEventID int64

		for _, edition := range filteredEditionData {
			editionIDUint32 := shared.ConvertToUInt32(edition["edition_id"])
			edID := int64(editionIDUint32)

			if edID == editionID {
				targetEdition = edition
				if eventIDVal, ok := edition["event"]; ok {
					actualEventID = int64(shared.ConvertToUInt32(eventIDVal))
				}
				break
			}
		}

		if targetEdition == nil {
			log.Printf("WARNING: Could not find edition %d (expected event %d) in %d fetched editions, skipping", editionID, expectedEventID, len(filteredEditionData))
			foundInRequest := false
			for _, reqID := range editionIDs {
				if reqID == editionID {
					foundInRequest = true
					break
				}
			}
			if !foundInRequest {
				log.Printf("  ERROR: Edition %d was not even in the fetch request!", editionID)
			} else {
				log.Printf("  Edition %d was in fetch request but not returned by MySQL query", editionID)
			}
			continue
		}

		eventData := eventDataLookup[actualEventID]
		if eventData == nil {
			log.Printf("WARNING: Could not find event data for event %d (edition %d belongs to this event), skipping", actualEventID, editionID)
			continue
		}

		company, venue, city, companyCity, venueCity := resolveRelatedDataForEdition(
			targetEdition,
			companyLookup,
			venueLookup,
			cityLookup,
		)

		esInfoMap := esData[actualEventID]
		if esInfoMap == nil {
			esInfoMap = make(map[string]interface{})
		}

		editionDomain, companyDomain := extractDomainsForEdition(targetEdition, company)

		editionType := determinealleventType(
			targetEdition["edition_start_date"],
			currentEditionStartDates[actualEventID],
			editionID,
			currentEditionIDs[actualEventID],
		)

		editionCountryISO := strings.ToUpper(shared.ConvertToString(eventData["country"]))

		editionCityLocationChID, companyCityLocationChID, venueCityLocationChID,
			editionCityStateLocationChID, venueLocationChID := computeAllLocationIDs(
			city,
			company,
			companyCity,
			venue,
			venueCity,
			editionCountryISO,
			cityIDLookup,
			stateIDLookup,
			venueIDLookup,
		)

		record := buildAlleventRecord(
			eventData,
			targetEdition,
			company,
			venue,
			city,
			companyCity,
			venueCity,
			esInfoMap,
			processedEconomicData[actualEventID],
			estimateDataMap[actualEventID],
			eventTypesMap,
			categoryNamesMap,
			ticketDataMap,
			ticketTypeMap,
			timingDataMap,
			actualEventID,
			editionType,
			currentEditionIDs[actualEventID],
			allevents[actualEventID],
			editionCountryISO,
			editionDomain,
			companyDomain,
			editionCityLocationChID,
			companyCityLocationChID,
			venueCityLocationChID,
			editionCityStateLocationChID,
			venueLocationChID,
		)

		records = append(records, record)
	}

	log.Printf("Rebuilt %d records from %d pairs", len(records), len(pairs))
	return records, nil
}

func resolveRelatedDataForEdition(
	edition map[string]interface{},
	companyLookup map[int64]map[string]interface{},
	venueLookup map[int64]map[string]interface{},
	cityLookup map[int64]map[string]interface{},
) (
	company map[string]interface{},
	venue map[string]interface{},
	city map[string]interface{},
	companyCity map[string]interface{},
	venueCity map[string]interface{},
) {
	companyID := edition["company_id"]
	venueID := edition["venue_id"]
	cityID := edition["edition_city"]

	if companyID != nil {
		if c, exists := companyLookup[companyID.(int64)]; exists {
			company = c
		}
	}

	if venueID != nil {
		if v, exists := venueLookup[venueID.(int64)]; exists {
			venue = v
		}
	}

	if cityID != nil {
		if c, exists := cityLookup[cityID.(int64)]; exists {
			city = c
		}
	}

	if company != nil && company["company_city"] != nil {
		if companyCityID, ok := company["company_city"].(int64); ok {
			if c, exists := cityLookup[companyCityID]; exists {
				companyCity = c
			}
		}
	}

	if venue != nil && venue["venue_city"] != nil {
		if venueCityID, ok := venue["venue_city"].(int64); ok {
			if c, exists := cityLookup[venueCityID]; exists {
				venueCity = c
			}
		}
	}

	return company, venue, city, companyCity, venueCity
}

func computeAllLocationIDs(
	city map[string]interface{},
	company map[string]interface{},
	companyCity map[string]interface{},
	venue map[string]interface{},
	venueCity map[string]interface{},
	editionCountryISO string,
	cityIDLookup map[string]uint32,
	stateIDLookup map[string]uint32,
	venueIDLookup map[string]uint32,
) (
	editionCityLocationChID *uint32,
	companyCityLocationChID *uint32,
	venueCityLocationChID *uint32,
	editionCityStateLocationChID *uint32,
	venueLocationChID *uint32,
) {
	if city != nil && city["name"] != nil {
		cityName := shared.ConvertToString(city["name"])
		if cityName != "" {
			cityNameStr := strings.TrimSpace(cityName)
			if editionCountryISO != "" && editionCountryISO != "NAN" {
				cityKeyWithISO := fmt.Sprintf("%s|%s", cityNameStr, editionCountryISO)
				if locationChID, exists := cityIDLookup[cityKeyWithISO]; exists {
					editionCityLocationChID = &locationChID
				}
			}
			if editionCityLocationChID == nil {
				cityKeyWithoutISO := cityNameStr
				if locationChID, exists := cityIDLookup[cityKeyWithoutISO]; exists {
					editionCityLocationChID = &locationChID
				}
			}
		}
	}

	// Company city location ID
	if companyCity != nil && companyCity["name"] != nil {
		companyCityName := shared.ConvertToString(companyCity["name"])
		companyCountryISO := strings.ToUpper(shared.ConvertToString(company["company_country"]))
		if companyCityName != "" {
			companyCityNameStr := strings.TrimSpace(companyCityName)
			if companyCountryISO != "" && companyCountryISO != "NAN" {
				cityKeyWithISO := fmt.Sprintf("%s|%s", companyCityNameStr, companyCountryISO)
				if locationChID, exists := cityIDLookup[cityKeyWithISO]; exists {
					companyCityLocationChID = &locationChID
				}
			}
			if companyCityLocationChID == nil {
				cityKeyWithoutISO := companyCityNameStr
				if locationChID, exists := cityIDLookup[cityKeyWithoutISO]; exists {
					companyCityLocationChID = &locationChID
				}
			}
		}
	}

	// Venue city location ID
	if venueCity != nil && venueCity["name"] != nil {
		venueCityName := shared.ConvertToString(venueCity["name"])
		venueCountryISO := strings.ToUpper(shared.ConvertToString(venue["venue_country"]))
		if venueCityName != "" {
			venueCityNameStr := strings.TrimSpace(venueCityName)
			if venueCountryISO != "" && venueCountryISO != "NAN" {
				cityKeyWithISO := fmt.Sprintf("%s|%s", venueCityNameStr, venueCountryISO)
				if locationChID, exists := cityIDLookup[cityKeyWithISO]; exists {
					venueCityLocationChID = &locationChID
				}
			}
			if venueCityLocationChID == nil {
				cityKeyWithoutISO := venueCityNameStr
				if locationChID, exists := cityIDLookup[cityKeyWithoutISO]; exists {
					venueCityLocationChID = &locationChID
				}
			}
		}
	}

	// Edition city state location ID
	if city != nil && city["state"] != nil {
		stateName := shared.ConvertToString(city["state"])
		if stateName != "" {
			stateNameStr := strings.TrimSpace(stateName)
			if editionCountryISO != "" && editionCountryISO != "NAN" {
				stateKeyWithISO := fmt.Sprintf("%s|%s", stateNameStr, editionCountryISO)
				if locationChID, exists := stateIDLookup[stateKeyWithISO]; exists {
					editionCityStateLocationChID = &locationChID
				}
			}
			if editionCityStateLocationChID == nil {
				stateKeyWithoutISO := stateNameStr
				if locationChID, exists := stateIDLookup[stateKeyWithoutISO]; exists {
					editionCityStateLocationChID = &locationChID
				}
			}
		}
	}

	// Venue location ID
	if venue != nil && venue["venue_name"] != nil {
		venueName := shared.ConvertToString(venue["venue_name"])
		venueCountryISO := strings.ToUpper(shared.ConvertToString(venue["venue_country"]))
		if venueName != "" {
			venueNameStr := strings.TrimSpace(venueName)
			if venueCountryISO != "" && venueCountryISO != "NAN" {
				venueKeyWithISO := fmt.Sprintf("%s|%s", venueNameStr, venueCountryISO)
				if locationChID, exists := venueIDLookup[venueKeyWithISO]; exists {
					venueLocationChID = &locationChID
				}
			}
			if venueLocationChID == nil {
				venueKeyWithoutISO := venueNameStr
				if locationChID, exists := venueIDLookup[venueKeyWithoutISO]; exists {
					venueLocationChID = &locationChID
				}
			}
		}
	}

	return editionCityLocationChID, companyCityLocationChID, venueCityLocationChID, editionCityStateLocationChID, venueLocationChID
}

// extractDomainsForEdition extracts edition and company domains
// This centralizes the domain extraction logic
func extractDomainsForEdition(
	edition map[string]interface{},
	company map[string]interface{},
) (editionDomain string, companyDomain string) {
	editionWebsite := edition["edition_website"]
	if editionWebsite != nil {
		editionDomain = shared.ExtractDomainFromWebsite(editionWebsite)
	}

	if company != nil && company["company_website"] != nil {
		companyDomain = shared.ExtractDomainFromWebsite(company["company_website"])
	}

	return editionDomain, companyDomain
}

// buildAlleventRecord builds a complete allevent record from all the necessary data
// This is a reusable function that can be called from both processalleventChunk and rebuildRecordsForFailedBatch
func buildAlleventRecord(
	eventData map[string]interface{},
	edition map[string]interface{},
	company map[string]interface{},
	venue map[string]interface{},
	city map[string]interface{},
	companyCity map[string]interface{},
	venueCity map[string]interface{},
	esInfoMap map[string]interface{},
	economicData map[string]interface{},
	estimate estimateData,
	eventTypesMap map[int64][]uint32,
	categoryNamesMap map[int64][]string,
	ticketDataMap map[int64][]string,
	ticketTypeMap map[int64]string,
	timingDataMap map[uint64][]string,
	eventID int64,
	editionType *string,
	currentEditionID int64,
	allEditions []map[string]interface{},
	editionCountryISO string,
	editionDomain string,
	companyDomain string,
	editionCityLocationChID *uint32,
	companyCityLocationChID *uint32,
	venueCityLocationChID *uint32,
	editionCityStateLocationChID *uint32,
	venueLocationChID *uint32,
) map[string]interface{} {
	record := map[string]interface{}{
		"event_id":          eventData["id"],
		"event_uuid":        shared.GenerateUUIDFromString(fmt.Sprintf("%d-%s", shared.ConvertToUInt32(eventData["id"]), shared.ConvertToString(edition["edition_created"]))),
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
		"company_id":        company["id_10x"],
		"company_uuid": func() string {
			if company != nil {
				if companyID, ok := company["id_10x"].(int64); ok && companyID > 0 {
					created := company["created"]
					createdStr := shared.ConvertToString(created)
					if createdStr != "" {
						idInputString := fmt.Sprintf("%d-%s", companyID, createdStr)
						return shared.GenerateUUIDFromString(idInputString)
					}
				}
			}
			if companyID := edition["company_id"]; companyID != nil {
				var id int64
				var ok bool
				if id, ok = companyID.(int64); !ok {
					if idVal, ok2 := companyID.(int); ok2 {
						id = int64(idVal)
						ok = true
					} else if idVal, ok2 := companyID.(uint32); ok2 {
						id = int64(idVal)
						ok = true
					}
				}
				if ok && id > 0 {
					createdStr := shared.ConvertToString(edition["edition_created"])
					if createdStr == "" {
						createdStr = shared.ConvertToString(edition["start_date"])
					}
					if createdStr == "" {
						createdStr = "1970-01-01 00:00:00"
					}
					idInputString := fmt.Sprintf("%d-%s", id, createdStr)
					return shared.GenerateUUIDFromString(idInputString)
				}
			}
			eventIDStr := shared.ConvertToString(eventData["id"])
			editionIDStr := shared.ConvertToString(edition["edition_id"])
			idInputString := fmt.Sprintf("company-%s-%s", eventIDStr, editionIDStr)
			return shared.GenerateUUIDFromString(idInputString)
		}(),
		"company_name":    company["company_name"],
		"company_domain":  companyDomain,
		"company_website": company["company_website"],
		"companyLogoUrl":  company["company_logo_url"],
		"company_country": strings.ToUpper(shared.ConvertToString(company["company_country"])),
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
		"company_address": func() *string {
			if company != nil && company["address"] != nil {
				addressStr := shared.ConvertToString(company["address"])
				if strings.TrimSpace(addressStr) == "" {
					return nil
				}
				return &addressStr
			}
			return nil
		}(),
		"venue_id": func() interface{} {
			if venueLocationChID != nil {
				return uint32(*venueLocationChID)
			}
			return nil
		}(),
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
				val := uint32(1)
				return &val
			}
			val := uint32(0)
			return &val
		}(),
		"eventBrandId": func() *string {
			brandId := eventData["brand_id_from_table"]
			brandCreated := eventData["brand_created"]
			if brandId != nil && brandCreated != nil {
				brandIdStr := shared.ConvertToString(brandId)
				brandCreatedStr := shared.ConvertToString(brandCreated)
				if brandIdStr != "" && brandCreatedStr != "" {
					uuidInput := fmt.Sprintf("%s-%s", brandIdStr, brandCreatedStr)
					uuid := shared.GenerateUUIDFromString(uuidInput)
					return &uuid
				}
			}
			return nil
		}(),
		"eventSeriesId": func() *string {
			isSeries := eventData["multi_city"]
			eventName := eventData["event_name"]

			var isSeriesInt int
			if isSeries != nil {
				if val, ok := isSeries.(int64); ok {
					isSeriesInt = int(val)
				} else if val, ok := isSeries.(int); ok {
					isSeriesInt = val
				} else if val, ok := isSeries.(uint32); ok {
					isSeriesInt = int(val)
				}
			}

			if isSeriesInt == 1 && eventName != nil {
				eventNameStr := shared.ConvertToString(eventName)
				if eventNameStr != "" {
					uuid := shared.GenerateUUIDFromString(eventNameStr)
					return &uuid
				}
			}
			return nil
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
		"timings": func() []string {
			editionIDUint32 := shared.ConvertToUInt32(edition["edition_id"])
			eventIDUint32 := shared.ConvertToUInt32(eventID)
			key := uint64(eventIDUint32)<<32 | uint64(editionIDUint32)
			if timings, exists := timingDataMap[key]; exists {
				return timings
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
		"repeatSentiment":                 esInfoMap["repeatSentiment"],
		"reputationChangePercentage":      esInfoMap["reputationChangePercentage"],
		"audienceZone":                    esInfoMap["audienceZone"],
		"event_avgRating":                 esInfoMap["avg_rating"],
		"10timesEventPageUrl":             eventData["url"],
		"keywords":                        []string{},
		"event_score":                     eventData["score"],
		"yoyGrowth":                       esInfoMap["yoyGrowth"],
		"futureExpexctedStartDate":        esInfoMap["futureExpexctedStartDate"],
		"futureExpexctedEndDate":          esInfoMap["futureExpexctedEndDate"],
		"PrimaryEventType": func() *string {
			eventTypes := eventTypesMap[eventID]
			eventAudience := shared.SafeConvertToUInt16(eventData["event_audience"])
			result := getPrimaryEventType(eventTypes, eventAudience)
			return result
		}(),
		"verifiedOn": func() *string {
			verified := eventData["verified"]
			if verified != nil {
				verifiedStr := shared.ConvertToString(verified)
				if len(verifiedStr) >= 10 {
					datePart := verifiedStr[:10]
					return &datePart
				}
			}
			return nil
		}(),
		"estimatedVisitorsMean": func() *uint32 {
			if finalEstimate := esInfoMap["finalEstimate"]; finalEstimate != nil {
				if finalEstimateStr, ok := finalEstimate.(string); ok && finalEstimateStr != "" {
					if finalEstimateFloat, err := strconv.ParseFloat(finalEstimateStr, 64); err == nil {
						result := uint32(finalEstimateFloat)
						return &result
					}
				} else if finalEstimateFloat, ok := finalEstimate.(float64); ok {
					result := uint32(finalEstimateFloat)
					return &result
				} else if finalEstimateInt, ok := finalEstimate.(int64); ok {
					result := uint32(finalEstimateInt)
					return &result
				} else if finalEstimateInt, ok := finalEstimate.(int); ok {
					result := uint32(finalEstimateInt)
					return &result
				}
			}

			highEstimate := esInfoMap["highEstimate"]
			lowEstimate := esInfoMap["lowEstimate"]

			var highVal, lowVal float64
			highValid := false
			lowValid := false

			if highEstimate != nil {
				if highStr, ok := highEstimate.(string); ok && highStr != "" {
					if val, err := strconv.ParseFloat(highStr, 64); err == nil {
						highVal = val
						highValid = true
					}
				} else if val, ok := highEstimate.(float64); ok {
					highVal = val
					highValid = true
				} else if val, ok := highEstimate.(int64); ok {
					highVal = float64(val)
					highValid = true
				} else if val, ok := highEstimate.(int); ok {
					highVal = float64(val)
					highValid = true
				}
			}

			if lowEstimate != nil {
				if lowStr, ok := lowEstimate.(string); ok && lowStr != "" {
					if val, err := strconv.ParseFloat(lowStr, 64); err == nil {
						lowVal = val
						lowValid = true
					}
				} else if val, ok := lowEstimate.(float64); ok {
					lowVal = val
					lowValid = true
				} else if val, ok := lowEstimate.(int64); ok {
					lowVal = float64(val)
					lowValid = true
				} else if val, ok := lowEstimate.(int); ok {
					lowVal = float64(val)
					lowValid = true
				}
			}

			if highValid && lowValid {
				mean := uint32((highVal + lowVal) / 2)
				return &mean
			}

			return nil
		}(),
		"estimatedSize": func() *string {
			eventTypes := eventTypesMap[eventID]
			eventAudience := shared.SafeConvertToUInt16(eventData["event_audience"])
			primaryEventTypeUUID := getPrimaryEventType(eventTypes, eventAudience)

			var primaryEventTypeID *uint32
			if primaryEventTypeUUID != nil {
				if id, ok := eventTypeUUIDToID[*primaryEventTypeUUID]; ok {
					primaryEventTypeID = &id
				}
			}

			var estimatedVisitorMean *uint32
			if finalEstimate := esInfoMap["finalEstimate"]; finalEstimate != nil {
				if finalEstimateStr, ok := finalEstimate.(string); ok && finalEstimateStr != "" {
					if finalEstimateFloat, err := strconv.ParseFloat(finalEstimateStr, 64); err == nil {
						result := uint32(finalEstimateFloat)
						estimatedVisitorMean = &result
					}
				} else if finalEstimateFloat, ok := finalEstimate.(float64); ok {
					result := uint32(finalEstimateFloat)
					estimatedVisitorMean = &result
				} else if finalEstimateInt, ok := finalEstimate.(int64); ok {
					result := uint32(finalEstimateInt)
					estimatedVisitorMean = &result
				} else if finalEstimateInt, ok := finalEstimate.(int); ok {
					result := uint32(finalEstimateInt)
					estimatedVisitorMean = &result
				}
			}

			if estimatedVisitorMean == nil {
				highEstimate := esInfoMap["highEstimate"]
				lowEstimate := esInfoMap["lowEstimate"]

				var highVal, lowVal float64
				highValid := false
				lowValid := false

				if highEstimate != nil {
					if highStr, ok := highEstimate.(string); ok && highStr != "" {
						if val, err := strconv.ParseFloat(highStr, 64); err == nil {
							highVal = val
							highValid = true
						}
					} else if val, ok := highEstimate.(float64); ok {
						highVal = val
						highValid = true
					} else if val, ok := highEstimate.(int64); ok {
						highVal = float64(val)
						highValid = true
					} else if val, ok := highEstimate.(int); ok {
						highVal = float64(val)
						highValid = true
					}
				}

				if lowEstimate != nil {
					if lowStr, ok := lowEstimate.(string); ok && lowStr != "" {
						if val, err := strconv.ParseFloat(lowStr, 64); err == nil {
							lowVal = val
							lowValid = true
						}
					} else if val, ok := lowEstimate.(float64); ok {
						lowVal = val
						lowValid = true
					} else if val, ok := lowEstimate.(int64); ok {
						lowVal = float64(val)
						lowValid = true
					} else if val, ok := lowEstimate.(int); ok {
						lowVal = float64(val)
						lowValid = true
					}
				}

				if highValid && lowValid {
					mean := uint32((highVal + lowVal) / 2)
					estimatedVisitorMean = &mean
				}
			}

			return getAttendanceRange(primaryEventTypeID, estimatedVisitorMean)
		}(),
		"last_updated_at": time.Now().Format("2006-01-02 15:04:05"),
		"version":         1,
	}

	// Post-processing for current editions
	var currentEditionEventType interface{}
	editionIDUint32 := shared.ConvertToUInt32(edition["edition_id"])
	if currentEditionID > 0 && int64(currentEditionID) == int64(editionIDUint32) {
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

	// Set edition_type
	if editionType != nil {
		record["edition_type"] = *editionType
	} else {
		record["edition_type"] = "NA"
	}

	// Set event_editions count for current editions
	if editionType != nil && *editionType == "current_edition" {
		if len(allEditions) > 0 {
			eventEditionsCount := uint32(len(allEditions))
			record["event_editions"] = eventEditionsCount
		} else {
			record["event_editions"] = nil
		}
	} else {
		record["event_editions"] = nil
	}

	// Determine event_format
	var eventFormat string
	fieldHybrid := esInfoMap["event_hybrid"]
	fieldCity := esInfoMap["event_city"]
	var isOnline interface{}
	if edition["is_online"] != nil {
		isOnline = edition["is_online"]
	}

	if fieldHybrid != nil {
		if h, ok := fieldHybrid.(uint8); ok && h == 1 {
			eventFormat = "HYBRID"
		} else if h, ok := fieldHybrid.(*uint8); ok && h != nil && *h == 1 {
			eventFormat = "HYBRID"
		}
	}

	if eventFormat == "" {
		var cityStr string
		var isOnlineStr string

		if fieldCity != nil {
			cityStr = shared.ConvertToString(fieldCity)
		}
		if isOnline != nil {
			isOnlineStr = shared.ConvertToString(isOnline)
		}

		if cityStr == "1" || isOnlineStr == "1" {
			eventFormat = "ONLINE"
		} else {
			eventFormat = "OFFLINE"
		}
	}

	record["event_format"] = &eventFormat

	// Add economic impact data
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

	// Add inbound/international data
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
	} else {
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

	return record
}

func retryFailedBatchesAfterCompletion(
	mysqlDB *sql.DB,
	clickhouseConn driver.Conn,
	esClient *elasticsearch.Client,
	config shared.Config,
) error {
	logDir := "failed_batches"
	logFile := filepath.Join(logDir, fmt.Sprintf("retry_log_%s.txt", time.Now().Format("20060102_150405")))
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Printf("WARNING: Failed to create log directory: %v", err)
		logFile = ""
	}

	var logWriter *os.File
	if logFile != "" {
		var err error
		logWriter, err = os.Create(logFile)
		if err != nil {
			log.Printf("WARNING: Failed to create retry log file: %v", err)
			logWriter = nil
		} else {
			defer logWriter.Close()
			logWriter.WriteString(fmt.Sprintf("=== Retry Failed Batches Log - Started at %s ===\n\n", time.Now().Format("2006-01-02 15:04:05")))
		}
	}

	logToFile := func(format string, args ...interface{}) {
		msg := fmt.Sprintf(format, args...)
		log.Print(msg)
		if logWriter != nil {
			logWriter.WriteString(msg + "\n")
		}
	}

	dir := "failed_batches"

	maxAttempts := 3
	backoffDuration := 2 * time.Minute

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		files, err := filepath.Glob(filepath.Join(dir, "failed_batches_chunk_*.csv"))
		if err != nil {
			logToFile("ERROR: Failed to glob failed batch files: %v", err)
			return fmt.Errorf("failed to glob failed batch files: %w", err)
		}

		if len(files) == 0 {
			if attempt == 1 {
				logToFile("No failed batches to retry")
			} else {
				logToFile("✓ All failed batches successfully processed!")
			}
			return nil
		}

		logToFile("")
		logToFile("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		logToFile("=== Retry Attempt %d/%d ===", attempt, maxAttempts)
		logToFile("Found %d failed batch files to process", len(files))
		if logFile != "" {
			logToFile("Log file: %s", logFile)
		}
		logToFile("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

		totalSkipped := 0
		totalRetried := 0
		totalFailed := 0

		for _, file := range files {
			logToFile("")
			logToFile("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
			logToFile("Processing failed batch file: %s", file)
			logToFile("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

			pairsByBatch, err := readCSVGroupedByBatch(file)
			if err != nil {
				logToFile("ERROR: Failed to read %s: %v", file, err)
				continue
			}

			logToFile("Found %d batches in file", len(pairsByBatch))

			fileProcessedSuccessfully := true

			for batchNum, pairs := range pairsByBatch {
				logToFile("")
				logToFile("  → Processing batch %d with %d pairs...", batchNum, len(pairs))

				logToFile("    Checking existence in allevent_temp for batch %d (%d pairs)...", batchNum, len(pairs))
				existingPairs, err := checkExistenceInClickHouse(clickhouseConn, pairs, config)
				if err != nil {
					logToFile("    ERROR: Failed to check existence for batch %d: %v", batchNum, err)
					totalFailed += len(pairs)
					fileProcessedSuccessfully = false
					continue
				}

				missingPairs := make([]EventEditionPair, 0)
				for _, pair := range pairs {
					key := uint64(pair.EventID)<<32 | uint64(pair.EditionID)
					if !existingPairs[key] {
						missingPairs = append(missingPairs, pair)
					}
				}

				if len(existingPairs) == len(pairs) {
					logToFile("    ✓ All %d pairs already exist in allevent_temp, no insert needed for batch %d", len(pairs), batchNum)
					totalSkipped += len(pairs)
					continue
				} else if len(existingPairs) > 0 {
					logToFile("    Found %d/%d pairs exist, %d pairs missing - will insert missing pairs only",
						len(existingPairs), len(pairs), len(missingPairs))
					totalSkipped += len(existingPairs)
				} else {
					logToFile("    No pairs exist in allevent_temp, will insert all %d pairs", len(pairs))
				}

				if len(missingPairs) == 0 {
					continue
				}

				rebuildBatchSize := 5000
				insertSuccess := true
				failedSubBatches := make([]map[string]interface{}, 0)
				backoffDuration := 30 * time.Second

				optimizeConfigs := shared.GetTableOptimizeConfigs()
				alleventOptimizeConfig, hasConfig := optimizeConfigs["allevent_ch"]

				if hasConfig && alleventOptimizeConfig.PartitionExpression == "" {
					createStatement, err := shared.GetTableCreateStatement(clickhouseConn, alleventOptimizeConfig.TempTableName, config)
					if err == nil {
						partitionExpr, err := shared.ExtractPartitionExpression(createStatement)
						if err == nil {
							alleventOptimizeConfig.PartitionExpression = partitionExpr
						}
					}
				}

				logToFile("    Processing %d missing pairs in batches of %d (rebuild → insert → verify → optimize)...", len(missingPairs), rebuildBatchSize)

				for i := 0; i < len(missingPairs); i += rebuildBatchSize {
					end := i + rebuildBatchSize
					if end > len(missingPairs) {
						end = len(missingPairs)
					}
					missingPairsBatch := missingPairs[i:end]

					logToFile("      Processing missing pairs batch %d-%d (%d pairs): rebuilding records...", i+1, end, len(missingPairsBatch))

					records, err := rebuildRecordsForFailedBatch(
						mysqlDB,
						clickhouseConn,
						esClient,
						missingPairsBatch,
						config,
					)
					if err != nil {
						logToFile("        ERROR: Failed to rebuild records for batch %d-%d: %v", i+1, end, err)
						totalFailed += len(missingPairsBatch)
						fileProcessedSuccessfully = false
						continue
					}

					if len(records) == 0 {
						logToFile("        WARNING: No records rebuilt for batch %d-%d (expected %d records) - possible data loss", i+1, end, len(missingPairsBatch))
						totalFailed += len(missingPairsBatch)
						fileProcessedSuccessfully = false
						continue
					}

					if len(records) < len(missingPairsBatch) {
						logToFile("        WARNING: Only rebuilt %d/%d records for batch %d-%d - %d records missing", len(records), len(missingPairsBatch), i+1, end, len(missingPairsBatch)-len(records))
						totalFailed += len(missingPairsBatch) - len(records)
						fileProcessedSuccessfully = false
					}

					logToFile("        ✓ Rebuilt %d records", len(records))

					logToFile("        Checking existence in allevent_temp using duplicate detection columns (published, status, edition_type, event_id, edition_id)...")
					existingBeforeInsert, err := checkExistenceByDuplicateColumns(clickhouseConn, records, config)
					if err != nil {
						logToFile("        WARNING: Failed to check existence before insert: %v, proceeding with insert anyway", err)
					} else {
						stillMissingRecords := make([]map[string]interface{}, 0)
						existingCount := 0

						for _, record := range records {
							eventID := shared.ConvertToUInt32(record["event_id"])
							editionID := shared.ConvertToUInt32(record["edition_id"])
							published := shared.ConvertToUInt32(record["published"])
							status := shared.ConvertToUInt32(record["status"])
							editionType := shared.ConvertToUInt32(record["edition_type"])

							key := fmt.Sprintf("%d|%d|%d|%d|%d", published, status, editionType, eventID, editionID)

							if !existingBeforeInsert[key] {
								stillMissingRecords = append(stillMissingRecords, record)
							} else {
								existingCount++
							}
						}

						stillMissingPairs := make([]EventEditionPair, 0, len(stillMissingRecords))
						for _, record := range stillMissingRecords {
							eventID := shared.ConvertToUInt32(record["event_id"])
							editionID := shared.ConvertToUInt32(record["edition_id"])
							stillMissingPairs = append(stillMissingPairs, EventEditionPair{
								EventID:   eventID,
								EditionID: editionID,
							})
						}

						if existingCount > 0 {
							logToFile("        Found %d/%d records already exist (using duplicate detection columns), will insert only %d missing records", existingCount, len(records), len(stillMissingRecords))
							totalSkipped += existingCount

							if len(stillMissingRecords) == 0 {
								logToFile("        ✓ All records already exist, skipping insert for this batch")
								if i+rebuildBatchSize < len(missingPairs) {
									logToFile("        ⏳ Waiting %v before processing next batch...", backoffDuration)
									time.Sleep(backoffDuration)
									logToFile("        ✓ Wait complete, continuing with next batch")
								}
								continue
							}

							records = stillMissingRecords
							missingPairsBatch = stillMissingPairs
						} else {
							logToFile("        ✓ All %d records confirmed missing, proceeding with insert", len(records))
						}
					}

					logToFile("        Inserting %d records...", len(records))

					attemptCount := 0
					insertErr := shared.RetryWithBackoff(
						func() error {
							if attemptCount > 0 {
								now := time.Now().Format("2006-01-02 15:04:05")
								for j := range records {
									records[j]["last_updated_at"] = now
								}
								logToFile("          Retrying insert for batch %d-%d (attempt %d)", i+1, end, attemptCount+1)
							}
							attemptCount++
							return insertalleventDataChunk(clickhouseConn, records, config)
						},
						3, // 3 retries for transient errors
					)

					if insertErr != nil {
						logToFile("        ✗ ERROR: Failed to insert batch %d-%d after retries: %v", i+1, end, insertErr)
						insertSuccess = false
						totalFailed += len(records)

						if isMemoryLimitError(insertErr) {
							logToFile("          Memory error detected, writing back to CSV for retry")
							chunkNum := extractChunkNumFromFilename(file)
							if chunkNum >= 0 {
								retryBatchNum := -batchNum*10000 - i
								if writeErr := writeFailedBatchToCSV(records, retryBatchNum, chunkNum); writeErr != nil {
									logToFile("          ERROR: Failed to write failed batch to CSV: %v", writeErr)
								} else {
									logToFile("          ✓ Successfully wrote failed batch %d-%d back to CSV (batchNum: %d)", i+1, end, retryBatchNum)
								}
							}
						}

						failedSubBatches = append(failedSubBatches, records...)
						fileProcessedSuccessfully = false
						continue
					}

					logToFile("        ✓ Successfully inserted batch %d-%d (%d records)", i+1, end, len(records))
					totalRetried += len(records)

					logToFile("        Double-checking: Verifying inserted records exist in allevent_temp...")
					verifyPairs := make([]EventEditionPair, 0, len(records))
					for _, record := range records {
						eventID, ok1 := record["event_id"].(uint32)
						editionID, ok2 := record["edition_id"].(uint32)
						if ok1 && ok2 {
							verifyPairs = append(verifyPairs, EventEditionPair{
								EventID:   eventID,
								EditionID: editionID,
							})
						}
					}

					if len(verifyPairs) > 0 {
						existingAfterInsert, err := checkExistenceInClickHouse(clickhouseConn, verifyPairs, config)
						if err != nil {
							logToFile("        WARNING: Failed to verify inserted records: %v", err)
						} else {
							missingAfterInsert := 0
							for _, pair := range verifyPairs {
								key := uint64(pair.EventID)<<32 | uint64(pair.EditionID)
								if !existingAfterInsert[key] {
									missingAfterInsert++
									logToFile("        ✗ WARNING: Record (event_id=%d, edition_id=%d) not found after insert!", pair.EventID, pair.EditionID)
								}
							}
							if missingAfterInsert == 0 {
								logToFile("        ✓ Double-check passed: All %d records confirmed in allevent_temp", len(verifyPairs))
							} else {
								logToFile("        ✗ WARNING: Double-check failed: %d/%d records missing after insert", missingAfterInsert, len(verifyPairs))
								totalFailed += missingAfterInsert
								fileProcessedSuccessfully = false
							}
						}
					}

					if hasConfig && alleventOptimizeConfig.PartitionExpression != "" {
						logToFile("        ⏳ Waiting 30 seconds before checking for duplicates (to allow ClickHouse merge process)...")
						time.Sleep(30 * time.Second)
						logToFile("        Checking for duplicate partitions after insert...")

						duplicatePartitions, err := shared.GetPartitionsWithDuplicates(clickhouseConn, alleventOptimizeConfig, config)
						if err != nil {
							logToFile("        WARNING: Failed to check for duplicates: %v", err)
						} else if len(duplicatePartitions) > 0 {
							logToFile("        Found %d partitions with duplicates, verifying they still exist (to avoid false positives)...", len(duplicatePartitions))
							time.Sleep(20 * time.Second)
							verifiedDuplicates, verifyErr := shared.GetPartitionsWithDuplicates(clickhouseConn, alleventOptimizeConfig, config)
							if verifyErr != nil {
								logToFile("        WARNING: Failed to verify duplicates: %v, proceeding with optimization anyway", verifyErr)
								verifiedDuplicates = duplicatePartitions
							} else if len(verifiedDuplicates) == 0 {
								logToFile("        ✓ Duplicates were automatically merged by ClickHouse, no optimization needed")
							} else {
								verifiedPartitionMap := make(map[string]bool)
								for _, p := range verifiedDuplicates {
									verifiedPartitionMap[p] = true
								}

								partitionsToOptimize := make([]string, 0)
								for _, partition := range duplicatePartitions {
									if verifiedPartitionMap[partition] {
										partitionsToOptimize = append(partitionsToOptimize, partition)
									}
								}

								if len(partitionsToOptimize) > 0 {
									logToFile("        Verified %d partitions still have duplicates, optimizing on priority...", len(partitionsToOptimize))
									for _, partition := range partitionsToOptimize {
										logToFile("          Optimizing partition: %s", partition)
										optimizeErr := shared.OptimizeTablePartition(clickhouseConn, alleventOptimizeConfig.TempTableName, partition, config, "optimize_logs.log")
										if optimizeErr != nil {
											logToFile("          WARNING: Failed to optimize partition %s: %v", partition, optimizeErr)
										} else {
											logToFile("          ✓ Successfully optimized partition %s", partition)
										}
									}
								} else {
									logToFile("        ✓ All duplicates were automatically merged, no optimization needed")
								}
							}
						} else {
							logToFile("        ✓ No duplicate partitions found")
						}
					}

					if i+rebuildBatchSize < len(missingPairs) {
						logToFile("        ⏳ Waiting %v before processing next batch...", backoffDuration)
						time.Sleep(backoffDuration)
						logToFile("        ✓ Wait complete, continuing with next batch")
					}
				}

				if insertSuccess {
					logToFile("  ✓ Successfully processed batch %d", batchNum)
				} else if len(failedSubBatches) > 0 {
					logToFile("  ⚠ Batch %d partially failed - %d records failed to insert", batchNum, len(failedSubBatches))
				}
			}

			if fileProcessedSuccessfully {
				if err := os.Remove(file); err != nil {
					logToFile("  WARNING: Failed to remove successfully processed file %s: %v", file, err)
				} else {
					logToFile("  ✓ Successfully removed processed file: %s (all batches succeeded)", file)
				}
			} else {
				logToFile("  ⚠ Keeping file %s for retry (some batches failed or had issues)", file)
				logToFile("     → This file will be processed again on next script run")
			}
		}

		logToFile("")
		logToFile("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		logToFile("=== Retry Attempt %d/%d Summary ===", attempt, maxAttempts)
		logToFile("Total pairs skipped (already exist): %d", totalSkipped)
		logToFile("Total records retried and inserted: %d", totalRetried)
		logToFile("Total records failed: %d", totalFailed)
		logToFile("=== Attempt %d completed at %s ===", attempt, time.Now().Format("2006-01-02 15:04:05"))

		remainingFiles, err := filepath.Glob(filepath.Join(dir, "failed_batches_chunk_*.csv"))
		if err != nil {
			logToFile("WARNING: Failed to check for remaining failed batch files: %v", err)
		} else if len(remainingFiles) == 0 {
			logToFile("")
			logToFile("✓ All failed batches successfully processed - no remaining files")
			if logFile != "" {
				logToFile("Full log saved to: %s", logFile)
			}
			return nil
		} else {
			logToFile("")
			logToFile("⚠️  %d failed batch file(s) still remain after attempt %d:", len(remainingFiles), attempt)
			for _, file := range remainingFiles {
				logToFile("   - %s", file)
			}

			if attempt < maxAttempts {
				logToFile("")
				logToFile("⏳ Waiting %v before retry attempt %d/%d...", backoffDuration, attempt+1, maxAttempts)
				time.Sleep(backoffDuration)
				logToFile("✓ Backoff complete, starting next retry attempt")
			} else {
				logToFile("")
				logToFile("⚠️  Maximum retry attempts (%d) reached", maxAttempts)
				logToFile("   → These files will be processed on the next script run")
				logToFile("   → Optimization will be skipped until all batches are successfully inserted")
				if logFile != "" {
					logToFile("Full log saved to: %s", logFile)
				}
				return fmt.Errorf("failed batches still remain after %d attempts: %d file(s) need to be processed", maxAttempts, len(remainingFiles))
			}
		}
	}

	return nil
}
