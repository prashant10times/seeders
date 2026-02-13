package microservice

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"seeders/shared"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

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
var allowedEventTypeIDs = []uint32{1, 2, 3, 5, 6, 7, 12, 13, 14}

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
	14: {Priority: 9, Group: "B2C"},
}

var allowedEventTypes = []uint32{1, 2, 3, 5, 6, 7, 12, 13, 14}

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
	14: {
		"0-1000":       "NANO",
		"1000-5000":    "MICRO",
		"5000-10000":   "SMALL",
		"10000-20000":  "MEDIUM",
		"20000-50000":  "LARGE",
		"50000-100000": "MEGA",
		"100000+":      "ULTRA",
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

func decodeBase64Date(value interface{}) string {
	str := shared.SafeConvertToString(value)
	if str == "" {
		return "1970-01-01"
	}
	decoded, err := base64.StdEncoding.DecodeString(str)
	if err == nil {
		decodedStr := string(decoded)
		if matched, _ := regexp.MatchString(`^\d{4}-\d{2}-\d{2}$`, decodedStr); matched {
			return decodedStr
		}
		return decodedStr
	}
	return str
}

func decodeBase64NullableDate(value interface{}) *string {
	if value == nil {
		return nil
	}
	if strPtr, ok := value.(*string); ok {
		if strPtr == nil || *strPtr == "" {
			return nil
		}
		decoded, err := base64.StdEncoding.DecodeString(*strPtr)
		if err == nil {
			decodedStr := string(decoded)
			if matched, _ := regexp.MatchString(`^\d{4}-\d{2}-\d{2}$`, decodedStr); matched {
				return &decodedStr
			}
			return &decodedStr
		}
		return strPtr
	}
	str := shared.SafeConvertToString(value)
	if str == "" {
		return nil
	}
	decoded, err := base64.StdEncoding.DecodeString(str)
	if err == nil {
		decodedStr := string(decoded)
		if matched, _ := regexp.MatchString(`^\d{4}-\d{2}-\d{2}$`, decodedStr); matched {
			return &decodedStr
		}
		return &decodedStr
	}
	return &str
}

func parseDateLenient(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, fmt.Errorf("empty date string")
	}
	datePart := s
	if len(s) >= 10 {
		datePart = s[:10]
	}
	return time.Parse("2006-01-02", datePart)
}

func decodeBase64DateTime(value interface{}) string {
	str := shared.SafeConvertToString(value)
	if str == "" {
		return "1970-01-01 00:00:00"
	}
	decoded, err := base64.StdEncoding.DecodeString(str)
	if err == nil {
		decodedStr := string(decoded)
		if matched, _ := regexp.MatchString(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$`, decodedStr); matched {
			return decodedStr
		}
		return decodedStr
	}
	return str
}

func decodeBase64StringIfNeeded(value interface{}) string {
	str := shared.SafeConvertToString(value)
	if str == "" {
		return ""
	}

	hasPadding := strings.HasSuffix(str, "=") || strings.HasSuffix(str, "==")
	isMultipleOf4 := len(str)%4 == 0
	if !hasPadding || !isMultipleOf4 || len(str) < 16 {
		return str
	}

	decoded, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return str
	}

	if !utf8.Valid(decoded) {
		return str
	}

	decodedStr := string(decoded)
	if len(decodedStr) == 0 {
		return str
	}

	printableCount := 0
	for _, r := range decodedStr {
		if r >= 32 && r <= 126 {
			printableCount++
		}
	}

	if float64(printableCount)/float64(len(decodedStr)) >= 0.8 {
		return decodedStr
	}

	return str
}

func decodeBase64NullableStringIfNeeded(value interface{}) *string {
	if value == nil {
		return nil
	}
	str := shared.SafeConvertToString(value)
	if str == "" {
		return nil
	}

	hasPadding := strings.HasSuffix(str, "=") || strings.HasSuffix(str, "==")
	isMultipleOf4 := len(str)%4 == 0
	if !hasPadding || !isMultipleOf4 || len(str) < 16 {
		return &str
	}

	decoded, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return &str
	}

	if !utf8.Valid(decoded) {
		return &str
	}

	decodedStr := string(decoded)
	if len(decodedStr) == 0 {
		return &str
	}

	printableCount := 0
	for _, r := range decodedStr {
		if r >= 32 && r <= 126 {
			printableCount++
		}
	}

	if float64(printableCount)/float64(len(decodedStr)) >= 0.8 {
		return &decodedStr
	}
	return &str
}

// normalizePricingValue normalizes pricing values: converts to lowercase and replaces "free-paid" with "free_and_paid"
func normalizePricingValue(value interface{}) *string {
	if value == nil {
		return nil
	}
	decoded := decodeBase64NullableStringIfNeeded(value)
	if decoded == nil {
		return nil
	}
	str := *decoded
	if str == "" {
		return nil
	}
	str = strings.ToLower(str)
	str = strings.ReplaceAll(str, "free-paid", "free_and_paid")

	if str == "not_available" {
		return nil
	}
	return &str
}

func decodeValueWithBase64Support(value interface{}, fieldType string) interface{} {
	if value == nil {
		return nil
	}

	str, ok := value.(string)
	if !ok {
		str = shared.SafeConvertToString(value)
		if str == "" {
			return value
		}
	}

	switch fieldType {
	case "date":
		return decodeBase64Date(value)
	case "datetime":
		return decodeBase64DateTime(value)
	case "nullable_date":
		return decodeBase64NullableDate(value)
	}

	hasPadding := strings.HasSuffix(str, "=") || strings.HasSuffix(str, "==")
	isMultipleOf4 := len(str)%4 == 0

	if len(str) < 16 {
		if !hasPadding || !isMultipleOf4 {
			return value
		}
	} else {
		if !hasPadding && !isMultipleOf4 {
			return value
		}
	}

	base64Chars := "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/="
	isBase64Like := true
	for _, r := range str {
		if !strings.ContainsRune(base64Chars, r) {
			isBase64Like = false
			break
		}
	}

	if !isBase64Like {
		return value
	}

	decoded, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		missingPadding := (4 - len(str)%4) % 4
		if missingPadding > 0 {
			paddedStr := str + strings.Repeat("=", missingPadding)
			decoded, err = base64.StdEncoding.DecodeString(paddedStr)
		}
		if err != nil {
			return value
		}
	}

	if !utf8.Valid(decoded) {
		return value
	}

	decodedStr := string(decoded)
	if len(decodedStr) == 0 {
		return value
	}

	printableCount := 0
	for _, r := range decodedStr {
		if r >= 32 && r <= 126 {
			printableCount++
		}
	}

	if float64(printableCount)/float64(len(decodedStr)) >= 0.8 {
		return decodedStr
	}

	return value
}

// converts a map to alleventRecord struct
func convertToalleventRecord(record map[string]interface{}) alleventRecord {
	decodeStr := func(key string) string {
		val := decodeValueWithBase64Support(record[key], "string")
		return shared.SafeConvertToString(val)
	}
	decodeNullableStr := func(key string) *string {
		val := decodeValueWithBase64Support(record[key], "string")
		return shared.SafeConvertToNullableString(val)
	}

	return alleventRecord{
		EventID:            shared.SafeConvertToUInt32(record["event_id"]),
		EventUUID:          decodeStr("event_uuid"),
		EventName:          decodeStr("event_name"),
		EventAbbrName:      decodeNullableStr("event_abbr_name"),
		EventDescription:   decodeNullableStr("event_description"),
		EventPunchline:     decodeNullableStr("event_punchline"),
		StartDate:          decodeBase64Date(record["start_date"]),
		EndDate:            decodeBase64Date(record["end_date"]),
		EditionID:          shared.SafeConvertToUInt32(record["edition_id"]),
		EditionUUID:        shared.GenerateUUIDFromString(fmt.Sprintf("%d-%s", shared.SafeConvertToUInt32(record["edition_id"]), decodeBase64DateTime(record["edition_created"]))),
		EditionCountry:     strings.ToUpper(decodeStr("edition_country")),
		EditionCity:        shared.SafeConvertToUInt32(record["edition_city"]),
		EditionCityName:    decodeStr("edition_city_name"),
		EditionCityStateID: shared.SafeConvertToNullableUInt32(record["edition_city_state_id"]),
		EditionCityState:   decodeStr("edition_city_state"),
		EditionCityLat:     shared.SafeConvertToFloat64(record["edition_city_lat"]),
		EditionCityLong:    shared.SafeConvertToFloat64(record["edition_city_long"]),
		CompanyID:          shared.SafeConvertToNullableUInt32(record["company_id"]),
		CompanyUUID:        decodeNullableStr("company_uuid"),
		CompanyPublished:   shared.SafeConvertToNullableInt8(record["companyPublished"]),
		CompanyName:        decodeNullableStr("company_name"),
		CompanyDomain:      decodeNullableStr("company_domain"),
		CompanyWebsite:     decodeNullableStr("company_website"),
		CompanyLogoUrl:     decodeNullableStr("companyLogoUrl"),
		CompanyCountry:     shared.ToUpperNullableString(decodeNullableStr("company_country")),
		CompanyState:       decodeNullableStr("company_state"),
		CompanyCity:        shared.SafeConvertToNullableUInt32(record["company_city"]),
		CompanyCityName:    decodeNullableStr("company_city_name"),
		CompanyAddress:     decodeNullableStr("company_address"),
		VenueID:            shared.SafeConvertToNullableUInt32(record["venue_id"]),
		VenueName:          decodeNullableStr("venue_name"),
		VenueCountry:       shared.ToUpperNullableString(decodeNullableStr("venue_country")),
		VenueCity:          shared.SafeConvertToNullableUInt32(record["venue_city"]),
		VenueCityName:      decodeNullableStr("venue_city_name"),
		VenueLat:           shared.SafeConvertToNullableFloat64(record["venue_lat"]),
		VenueLong:          shared.SafeConvertToNullableFloat64(record["venue_long"]),
		Published:          shared.SafeConvertToInt8(record["published"]),
		Status: func() string {
			status := shared.SafeConvertToStatusString(record["status"])
			// FixedString(1) requires exactly 1 character - handle edge cases
			status = strings.TrimSpace(status)
			if status == "" || status == "null" || len(status) == 0 {
				return "A" // Default as per schema
			}
			// Take only first character if longer than 1
			if len(status) > 1 {
				return string(status[0])
			}
			return status
		}(),
		EditionsAudianceType: func() uint16 {
			decoded := decodeStr("editions_audiance_type")
			if decoded == "" {
				return 0
			}
			// Try to parse as uint16
			if val, err := strconv.ParseUint(decoded, 10, 16); err == nil {
				return uint16(val)
			}
			return shared.SafeConvertToUInt16(record["editions_audiance_type"])
		}(),
		EditionFunctionality:   decodeStr("edition_functionality"),
		EditionWebsite:         decodeNullableStr("edition_website"),
		EditionDomain:          decodeNullableStr("edition_domain"),
		EditionType:            *shared.SafeConvertToNullableString(record["edition_type"]),
		EventEditions:          shared.SafeConvertToNullableUInt32(record["event_editions"]),
		EventFollowers:         shared.SafeConvertToNullableUInt32(record["event_followers"]),
		EditionFollowers:       shared.SafeConvertToNullableUInt32(record["edition_followers"]),
		EventExhibitor:         shared.SafeConvertToNullableUInt32(record["event_exhibitor"]),
		EditionExhibitor:       shared.SafeConvertToNullableUInt32(record["edition_exhibitor"]),
		ExhibitorsUpperBound:   shared.SafeConvertToNullableUInt32(record["exhibitors_upper_bound"]),
		ExhibitorsLowerBound:   shared.SafeConvertToNullableUInt32(record["exhibitors_lower_bound"]),
		ExhibitorsMean:         shared.SafeConvertToNullableUInt32(record["exhibitors_mean"]),
		EventSponsor:           shared.SafeConvertToNullableUInt32(record["event_sponsor"]),
		EditionSponsor:         shared.SafeConvertToNullableUInt32(record["edition_sponsor"]),
		EventSpeaker:           shared.SafeConvertToNullableUInt32(record["event_speaker"]),
		EditionSpeaker:         shared.SafeConvertToNullableUInt32(record["edition_speaker"]),
		EventCreated:           decodeBase64DateTime(record["event_created"]),
		EventUpdated:           decodeBase64DateTime(record["event_updated"]),
		EditionCreated:         decodeBase64DateTime(record["edition_created"]),
		EventHybrid:            shared.SafeConvertToNullableUInt8(record["event_hybrid"]),
		EventFormat:            shared.SafeConvertToNullableString(record["event_format"]),
		IsBranded:              shared.SafeConvertToNullableUInt32(record["isBranded"]),
		EventBrandId:           decodeNullableStr("eventBrandId"),
		EventSeriesId:          decodeNullableStr("eventSeriesId"),
		Maturity:               decodeNullableStr("maturity"),
		EventPricing:           decodeNullableStr("event_pricing"),
		EventLogo:              decodeNullableStr("event_logo"),
		EventEstimatedVisitors: decodeNullableStr("event_estimatedVisitors"),
		EstimatedVisitorsMean: func() *uint32 {
			val := shared.SafeConvertToNullableUInt32(record["estimatedVisitorsMean"])
			if val == nil {
				zero := uint32(0)
				return &zero
			}
			return val
		}(),
		EstimatedSize:      decodeNullableStr("estimatedSize"),
		EventFrequency:     decodeNullableStr("event_frequency"),
		ImpactScore:        shared.SafeConvertToNullableUInt32(record["impactScore"]),
		InboundScore:       shared.SafeConvertToNullableUInt32(record["inboundScore"]),
		InternationalScore: shared.SafeConvertToNullableUInt32(record["internationalScore"]),
		RepeatSentimentChangePercentage: func() *float64 {
			val := shared.SafeConvertToNullableFloat64(record["repeatSentimentChangePercentage"])
			if val == nil {
				// Default to 0 if nil
				zero := float64(0.0)
				return &zero
			}
			// If value is 0.0, return 0.0
			return val
		}(),
		RepeatSentiment:              shared.SafeConvertToNullableUInt32(record["repeatSentiment"]),
		ReputationChangePercentage:   shared.SafeConvertToNullableFloat64(record["reputationChangePercentage"]),
		AudienceZone:                 decodeNullableStr("audienceZone"),
		InboundPercentage:            shared.SafeConvertToUInt32(record["inboundPercentage"]),
		InboundAttendance:            shared.SafeConvertToUInt32(record["inboundAttendance"]),
		InternationalPercentage:      shared.SafeConvertToUInt32(record["internationalPercentage"]),
		InternationalAttendance:      shared.SafeConvertToUInt32(record["internationalAttendance"]),
		EventEconomicFoodAndBevarage: shared.SafeConvertToNullableFloat64(record["event_economic_FoodAndBevarage"]),
		EventEconomicTransportation:  shared.SafeConvertToNullableFloat64(record["event_economic_Transportation"]),
		EventEconomicAccomodation:    shared.SafeConvertToNullableFloat64(record["event_economic_Accomodation"]),
		EventEconomicUtilities:       shared.SafeConvertToNullableFloat64(record["event_economic_Utilities"]),
		EventEconomicFlights:         shared.SafeConvertToNullableFloat64(record["event_economic_flights"]),
		EventEconomicValue:           shared.SafeConvertToNullableFloat64(record["event_economic_value"]),
		EventEconomicDayWiseImpact:   decodeStr("event_economic_dayWiseEconomicImpact"),
		EventEconomicBreakdown:       decodeStr("event_economic_breakdown"),
		EventEconomicImpact:          decodeStr("event_economic_impact"),
		EventAvgRating:               shared.SafeConvertFloat64ToDecimalString(record["event_avgRating"]),
		TenTimesEventPageUrl:         decodeNullableStr("10timesEventPageUrl"),
		Keywords:                     shared.ConvertToStringArray(record["keywords"]),
		Tickets:                      shared.ConvertToStringArray(record["tickets"]),
		Timings:                      shared.ConvertToStringArray(record["timings"]),
		EventScore:                   shared.SafeConvertToNullableInt32(record["event_score"]),
		YoYGrowth:                    shared.SafeConvertToNullableUInt32(record["yoyGrowth"]),
		FutureExpectedStartDate:      decodeBase64NullableDate(record["futureExpexctedStartDate"]),
		FutureExpectedEndDate:        decodeBase64NullableDate(record["futureExpexctedEndDate"]),
		PredictionScore: func() *int32 {
			if val, exists := record["predictionScore"]; exists && val != nil {
				if scoreInt32Ptr, ok := val.(*int32); ok {
					return scoreInt32Ptr
				}
				if scoreIntPtr, ok := val.(*int); ok {
					scoreInt32 := int32(*scoreIntPtr)
					return &scoreInt32
				}
				if scoreInt64Ptr, ok := val.(*int64); ok {
					scoreInt32 := int32(*scoreInt64Ptr)
					return &scoreInt32
				}
				if scoreFloatPtr, ok := val.(*float64); ok {
					scoreInt32 := int32(*scoreFloatPtr)
					return &scoreInt32
				}
				if scoreStrPtr, ok := val.(*string); ok && *scoreStrPtr != "" {
					if scoreVal, err := strconv.ParseInt(*scoreStrPtr, 10, 32); err == nil {
						scoreInt32 := int32(scoreVal)
						return &scoreInt32
					}
				}
				if scoreInt32, ok := val.(int32); ok {
					return &scoreInt32
				} else if scoreInt, ok := val.(int); ok {
					scoreInt32 := int32(scoreInt)
					return &scoreInt32
				} else if scoreInt64, ok := val.(int64); ok {
					scoreInt32 := int32(scoreInt64)
					return &scoreInt32
				} else if scoreFloat, ok := val.(float64); ok {
					scoreInt32 := int32(scoreFloat)
					return &scoreInt32
				} else if scoreStr, ok := val.(string); ok && scoreStr != "" {
					if scoreVal, err := strconv.ParseInt(scoreStr, 10, 32); err == nil {
						scoreInt32 := int32(scoreVal)
						return &scoreInt32
					}
				}
			}
			// Default to 0
			scoreInt32 := int32(0)
			return &scoreInt32
		}(),
		PrimaryEventType: decodeNullableStr("PrimaryEventType"),
		VerifiedOn:       decodeBase64NullableDate(record["verifiedOn"]),
		LastUpdatedAt:    decodeBase64DateTime(record["last_updated_at"]),
		Version:          shared.SafeConvertToUInt32(record["version"]),
	}
}

type alleventRecord struct {
	EventID                         uint32   `ch:"event_id"`
	EventUUID                       string   `ch:"event_uuid"` // UUID generated from event_id + event_created
	EventName                       string   `ch:"event_name"`
	EventAbbrName                   *string  `ch:"event_abbr_name"`
	EventDescription                *string  `ch:"event_description"`
	EventPunchline                  *string  `ch:"event_punchline"`
	EventAvgRating                  *string  `ch:"event_avgRating"`     // Nullable(Decimal(3,2))
	TenTimesEventPageUrl            *string  `ch:"10timesEventPageUrl"` // Nullable(String)
	StartDate                       string   `ch:"start_date"`          // Date NOT NULL
	EndDate                         string   `ch:"end_date"`            // Date NOT NULL
	EditionID                       uint32   `ch:"edition_id"`
	EditionUUID                     string   `ch:"edition_uuid"`          // UUID generated from edition_id + edition_created
	EditionCountry                  string   `ch:"edition_country"`       // LowCardinality(FixedString(2)) NOT NULL
	EditionCity                     uint32   `ch:"edition_city"`          // UInt32 NOT NULL
	EditionCityName                 string   `ch:"edition_city_name"`     // String NOT NULL
	EditionCityStateID              *uint32  `ch:"edition_city_state_id"` // Nullable(UInt32)
	EditionCityState                string   `ch:"edition_city_state"`    // LowCardinality(String) NOT NULL
	EditionCityLat                  float64  `ch:"edition_city_lat"`      // Float64 NOT NULL
	EditionCityLong                 float64  `ch:"edition_city_long"`     // Float64 NOT NULL
	CompanyID                       *uint32  `ch:"company_id"`
	CompanyUUID                     *string  `ch:"company_uuid"` // Nullable(UUID)
	CompanyPublished                *int8    `ch:"companyPublished"`
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
	PredictionScore                 *int32   `ch:"predictionScore"`                      // Nullable(Int32) DEFAULT 0
	PrimaryEventType                *string  `ch:"PrimaryEventType"`                     // Nullable(UUID)
	VerifiedOn                      *string  `ch:"verifiedOn"`                           // Nullable(Date)
	LastUpdatedAt                   string   `ch:"last_updated_at"`                      // DateTime NOT NULL
	Version                         uint32   `ch:"version"`
}

type dayWiseEconomicImpactRecord struct {
	EventID       uint32  `ch:"event_id"`        // UInt32
	Date          string  `ch:"date"`            // Date
	Metric        string  `ch:"metric"`          // LowCardinality(String)
	Value         float64 `ch:"value"`           // Float64
	LastUpdatedAt string  `ch:"last_updated_at"` // DateTime
}

func buildalleventMigrationData(db *sql.DB, table string, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT id, name as event_name, abbr_name, punchline, start_date, end_date, country, published, status, event_audience, functionality, brand_id, created FROM %s WHERE id >= %d AND id <= %d ORDER BY id, end_date LIMIT %d", table, startID, endID, batchSize)
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
			c.city as company_city, c.address, c.created, c.published as company_published, a.cdn_url as company_logo_url
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
			end_date as edition_end_date,
			exhibitors_total, online_event as is_online
		FROM event_edition 
		WHERE id IN (%s)
		ORDER BY end_date`, strings.Join(placeholders, ","))

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
		ORDER BY end_date`, strings.Join(placeholders, ","))

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
			end_date as edition_end_date,
			exhibitors_total, online_event as is_online
		FROM event_edition 
		WHERE event IN (%s)
		ORDER BY end_date`, strings.Join(placeholders, ","))

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
		ORDER BY e.end_date`, strings.Join(placeholders, ","))

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

func fetchalleventPredictedDatesForBatch(db *sql.DB, eventIDs []int64) map[int64]map[string]interface{} {
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
			ee.id,
			ee.event,
			ee.start_date,
			ee.end_date,
			100 AS score
		FROM event_edition ee
		JOIN (
			SELECT 
				ee.event,
				MIN(ee.id) AS min_id
			FROM event_edition ee
			JOIN event e ON ee.event = e.id
			WHERE ee.event IN (%s)
			AND ee.start_date > e.start_date
			AND ee.start_date > e.end_date
			AND ee.start_date > NOW()
			GROUP BY ee.event
		) x ON ee.event = x.event AND ee.id = x.min_id`, strings.Join(placeholders, ","))

	rows, err := db.Query(query, args...)
	if err != nil {
		log.Printf("Error fetching predicted dates data: %v", err)
		return nil
	}
	defer rows.Close()

	results := make(map[int64]map[string]interface{})
	for rows.Next() {
		var editionID int64
		var eventID int64
		var startDate sql.NullString
		var endDate sql.NullString
		var score int

		if err := rows.Scan(&editionID, &eventID, &startDate, &endDate, &score); err != nil {
			log.Printf("Error scanning predicted dates row: %v", err)
			continue
		}

		if startDate.Valid && endDate.Valid {
			results[eventID] = map[string]interface{}{
				"start_date": startDate.String,
				"end_date":   endDate.String,
				"score":      score,
			}
		}
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
		processedData["breakdownJSON"] = "{}"
		processedData["dayWiseJSON"] = "{}"
		result[eventID] = processedData
		return result
	}

	if errorField, exists := economicImpactJSON["error"]; exists && errorField != nil {
		processedData["breakdownJSON"] = "{}"
		processedData["dayWiseJSON"] = "{}"
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

	if len(dayWise) == 0 {
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

func convertDayWiseDataToRows(
	eventID int64,
	dayWiseFormatted map[string]map[string]interface{},
	lastUpdatedAt string,
) []dayWiseEconomicImpactRecord {

	if len(dayWiseFormatted) == 0 {
		return nil
	}

	var records []dayWiseEconomicImpactRecord
	var eventIDUint uint32
	if eventID >= 0 {
		eventIDUint = uint32(eventID)
	}

	for dateStr, dayData := range dayWiseFormatted {
		if breakdownTyped, ok := dayData["breakdown"].(map[string]float64); ok {
			for metric, value := range breakdownTyped {
				record := dayWiseEconomicImpactRecord{
					EventID:       eventIDUint,
					Date:          dateStr,
					Metric:        metric,
					Value:         value,
					LastUpdatedAt: lastUpdatedAt,
				}
				records = append(records, record)
			}
			continue
		}

		if breakdownIface, ok := dayData["breakdown"].(map[string]interface{}); ok {
			for metric, v := range breakdownIface {
				var value float64
				switch val := v.(type) {
				case float64:
					value = val
				case int:
					value = float64(val)
				case int64:
					value = float64(val)
				case float32:
					value = float64(val)
				case string:
					if parsed, err := strconv.ParseFloat(val, 64); err == nil {
						value = parsed
					} else {
						continue
					}
				default:
					continue
				}

				record := dayWiseEconomicImpactRecord{
					EventID:       eventIDUint,
					Date:          dateStr,
					Metric:        metric,
					Value:         value,
					LastUpdatedAt: lastUpdatedAt,
				}
				records = append(records, record)
			}
		}

	}

	return records
}

func aggregateDayWiseRecordsFromEconomicData(
	processedEconomicData map[int64]map[string]interface{},
	lastUpdatedAt string,
) []dayWiseEconomicImpactRecord {

	var allRecords []dayWiseEconomicImpactRecord
	skippedEmpty := 0
	skippedNil := 0
	skippedParseError := 0
	processedSuccess := 0

	for eventID, economicData := range processedEconomicData {
		if economicData == nil {
			skippedNil++
			continue
		}

		dayWiseJSONStr, ok := economicData["dayWiseJSON"].(string)
		if !ok || dayWiseJSONStr == "" || dayWiseJSONStr == "{}" {
			skippedEmpty++
			continue
		}

		var dayWiseFormatted map[string]map[string]interface{}
		if err := json.Unmarshal([]byte(dayWiseJSONStr), &dayWiseFormatted); err != nil {
			log.Printf("WARNING: Failed to parse dayWiseJSON for event_id %d: %v", eventID, err)
			skippedParseError++
			continue
		}

		records := convertDayWiseDataToRows(eventID, dayWiseFormatted, lastUpdatedAt)
		if len(records) > 0 {
			allRecords = append(allRecords, records...)
			processedSuccess++
		}
	}

	log.Printf("Generated %d day-wise economic impact records from %d events (processed: %d, skipped: nil=%d, empty=%d, parse_error=%d)",
		len(allRecords), len(processedEconomicData), processedSuccess, skippedNil, skippedEmpty, skippedParseError)

	return allRecords
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
	retryErr := retryFailedBatchesAfterCompletion(clickhouseConn, config)
	if retryErr != nil {
		log.Printf("ERROR: Failed to retry failed batches: %v", retryErr)
		log.Printf("⚠️  WARNING: Some batches still failed - optimization will be skipped")
		log.Printf("⚠️  Please rerun the script to retry failed batches before optimization")
	}
}

func HasRemainingFailedBatches() bool {
	dir := "failed_batches"
	files, err := filepath.Glob(filepath.Join(dir, "failed_batches_chunk_*_batch_*.json"))
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
				log.Printf("allevent chunk %d: Building location lookups from %s using id_10x for cities, states, and venues", chunkNum, locationTableName)
				startTime := time.Now()
				cityIDLookup, err := buildCityIDLookupFromId10x(clickhouseConn, locationTableName)
				if err != nil {
					log.Printf("allevent chunk %d: WARNING - Failed to build city ID lookup: %v", chunkNum, err)
					cityIDLookup = make(map[string]uint32)
				}
				stateIDLookup, err := buildStateIDLookupFromId10x(clickhouseConn, locationTableName)
				if err != nil {
					log.Printf("allevent chunk %d: WARNING - Failed to build state ID lookup: %v", chunkNum, err)
					stateIDLookup = make(map[string]uint32)
				}
				// Build state UUID lookup for direct state_uuid to id mapping
				stateUUIDLookup, err := buildStateIDLookupFromUUID(clickhouseConn, locationTableName)
				if err != nil {
					log.Printf("allevent chunk %d: WARNING - Failed to build state UUID lookup: %v", chunkNum, err)
					stateUUIDLookup = make(map[string]uint32)
				}
				// Build city to state_uuid lookup
				cityStateUUIDLookup, err := buildCityStateUUIDLookup(clickhouseConn, locationTableName)
				if err != nil {
					log.Printf("allevent chunk %d: WARNING - Failed to build city state UUID lookup: %v", chunkNum, err)
					cityStateUUIDLookup = make(map[string]string)
				}
				venueIDLookup, err := buildVenueIDLookupFromId10x(clickhouseConn, locationTableName)
				if err != nil {
					log.Printf("allevent chunk %d: WARNING - Failed to build venue ID lookup: %v", chunkNum, err)
					venueIDLookup = make(map[string]uint32)
				}
				lookupTime := time.Since(startTime)
				log.Printf("allevent chunk %d: Built location lookups from %s in %v (cities: %d, states: %d, stateUUIDs: %d, cityStateUUIDs: %d, venues: %d)", chunkNum, locationTableName, lookupTime, len(cityIDLookup), len(stateIDLookup), len(stateUUIDLookup), len(cityStateUUIDLookup), len(venueIDLookup))

				// Debug: Print one example value from each lookup map
				if len(cityIDLookup) > 0 {
					for k, v := range cityIDLookup {
						log.Printf("allevent chunk %d: City lookup example - id_10x: %s -> location_id: %d", chunkNum, k, v)
						break
					}
				}
				if len(stateIDLookup) > 0 {
					for k, v := range stateIDLookup {
						log.Printf("allevent chunk %d: State lookup example - id_10x: %s -> location_id: %d", chunkNum, k, v)
						break
					}
				}
				if len(stateUUIDLookup) > 0 {
					for k, v := range stateUUIDLookup {
						log.Printf("allevent chunk %d: State UUID lookup example - state_uuid: %s -> location_id: %d", chunkNum, k, v)
						break
					}
				}
				if len(cityStateUUIDLookup) > 0 {
					for k, v := range cityStateUUIDLookup {
						log.Printf("allevent chunk %d: City state UUID lookup example - city_id_10x: %s -> state_uuid: %s", chunkNum, k, v)
						break
					}
				}
				if len(venueIDLookup) > 0 {
					for k, v := range venueIDLookup {
						log.Printf("allevent chunk %d: Venue lookup example - id_10x: %s -> location_id: %d", chunkNum, k, v)
						break
					}
				}

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
				predictedDatesMap := make(map[int64]map[string]interface{})
				if len(eventIDsForEditions) > 0 {
					estimateDataMap = fetchalleventEstimateDataForBatch(mysqlDB, eventIDsForEditions)
					predictedDatesMap = fetchalleventPredictedDatesForBatch(mysqlDB, eventIDsForEditions)
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

				if len(processedEconomicData) > 0 {
					log.Printf("allevent chunk %d: Processing day-wise economic impact data for %d events", chunkNum, len(processedEconomicData))
					lastUpdatedAt := time.Now().Format("2006-01-02 15:04:05")
					log.Printf("allevent chunk %d: Aggregating day-wise economic impact data for %d events", chunkNum, len(processedEconomicData))
					dayWiseRecords := aggregateDayWiseRecordsFromEconomicData(
						processedEconomicData,
						lastUpdatedAt,
					)

					if len(dayWiseRecords) > 0 {
						log.Printf("allevent chunk %d: Inserting %d day-wise records into event_daywiseEconomicImpact_temp", chunkNum, len(dayWiseRecords))

						if err := insertDayWiseEconomicImpactWithRetry(
							clickhouseConn,
							dayWiseRecords,
						); err != nil {
							log.Printf("ERROR: Failed to insert day-wise records for chunk %d: %v", chunkNum, err)
						} else {
							log.Printf("allevent chunk %d: Successfully inserted %d day-wise economic impact records", chunkNum, len(dayWiseRecords))
						}
					} else {
						log.Printf("allevent chunk %d: No day-wise records to insert", chunkNum)
					}
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
				if len(eventIDsForEditions) > 0 {
					log.Printf("allevent chunk %d: Fetching ticket data for %d events", chunkNum, len(eventIDsForEditions))
					startTime = time.Now()
					rawTicketData := fetchalleventTicketDataForBatch(mysqlDB, eventIDsForEditions)
					if len(rawTicketData) > 0 {
						ticketDataMap = processalleventTicketData(rawTicketData)
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

				// Sort eventIDs by end_date to maintain insertion order
				sortedEventIDs := make([]int64, 0, len(allevents))
				for eventID := range allevents {
					sortedEventIDs = append(sortedEventIDs, eventID)
				}

				// Sort by end_date from eventDataLookup
				sort.Slice(sortedEventIDs, func(i, j int) bool {
					eventIDI := sortedEventIDs[i]
					eventIDJ := sortedEventIDs[j]

					eventDataI := eventDataLookup[eventIDI]
					eventDataJ := eventDataLookup[eventIDJ]

					if eventDataI == nil || eventDataJ == nil {
						// If event data is missing, maintain original order
						return eventIDI < eventIDJ
					}

					endDateI := shared.ConvertToString(eventDataI["end_date"])
					endDateJ := shared.ConvertToString(eventDataJ["end_date"])

					if endDateI == "" || endDateJ == "" {
						// If end_date is missing, maintain original order
						return eventIDI < eventIDJ
					}

					// Parse dates and compare
					dateI, errI := time.Parse("2006-01-02", endDateI)
					dateJ, errJ := time.Parse("2006-01-02", endDateJ)

					if errI != nil || errJ != nil {
						// If parsing fails, maintain original order
						return eventIDI < eventIDJ
					}

					// Sort by end_date ascending (earliest first)
					if dateI.Equal(dateJ) {
						// If same end_date, sort by eventID for consistency
						return eventIDI < eventIDJ
					}
					return dateI.Before(dateJ)
				})

				for _, eventID := range sortedEventIDs {
					editions := allevents[eventID]
					eventData := eventDataLookup[eventID]

					economicData := processedEconomicData[eventID]

					if eventData != nil {
						// Sort editions within this event by edition_end_date to ensure correct order
						sort.Slice(editions, func(i, j int) bool {
							editionI := editions[i]
							editionJ := editions[j]

							endDateIStr := shared.SafeConvertToString(editionI["edition_end_date"])
							endDateJStr := shared.SafeConvertToString(editionJ["edition_end_date"])

							if endDateIStr == "" || endDateJStr == "" {
								// If end_date is missing, maintain original order
								return false
							}

							dateI, errI := time.Parse("2006-01-02", endDateIStr)
							dateJ, errJ := time.Parse("2006-01-02", endDateJStr)

							if errI != nil || errJ != nil {
								return false
							}

							if dateI.Equal(dateJ) {
								editionIDI := shared.ConvertToUInt32(editionI["edition_id"])
								editionIDJ := shared.ConvertToUInt32(editionJ["edition_id"])
								return editionIDI < editionIDJ
							}
							return dateI.Before(dateJ)
						})

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
								companyCity,
								venue,
								venueCity,
								editionCountryISO,
								cityIDLookup,
								stateIDLookup,
								stateUUIDLookup,
								cityStateUUIDLookup,
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
								predictedDatesMap[eventID],
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
					log.Printf("allevent chunk %d: Attempting to insert %d records into ClickHouse (will be split into chunks of max 10,000)...", chunkNum, len(clickHouseRecords))

					insertErr := insertalleventDataIntoClickHouse(clickhouseConn, clickHouseRecords, config.ClickHouseWorkers, config)

					if insertErr != nil {
						log.Printf("allevent chunk %d: ClickHouse insertion failed for batch %d, checking which records already exist before writing to JSON: %v", chunkNum, batchNumber, insertErr)

						// Convert records to pairs for existence check
						checkPairs := make([]EventEditionPair, 0, len(clickHouseRecords))
						for _, record := range clickHouseRecords {
							eventID := shared.ConvertToUInt32(record["event_id"])
							editionID := shared.ConvertToUInt32(record["edition_id"])
							checkPairs = append(checkPairs, EventEditionPair{
								EventID:   eventID,
								EditionID: editionID,
							})
						}

						// Check which records already exist in the database
						// Use FINAL=true to see unmerged records (critical: some records may have been inserted but not merged yet)
						existingPairs, err := checkExistenceInClickHouseWithRetry(clickhouseConn, checkPairs, config, true, nil)
						if err != nil {
							log.Printf("allevent chunk %d: WARNING - Failed to check existence before writing to JSON: %v, writing all records", chunkNum, err)
							// If check fails, write all records (safer to retry than skip)
							if err := writeFailedBatchToJSON(clickHouseRecords, batchNumber, chunkNum); err != nil {
								log.Printf("ERROR: Failed to write failed batch to JSON: %v", err)
							} else {
								log.Printf("allevent chunk %d: Successfully wrote batch %d to JSON for retry (existence check failed, wrote all %d records)", chunkNum, batchNumber, len(clickHouseRecords))
							}
						} else {
							// Filter out records that already exist
							missingRecords := make([]map[string]interface{}, 0)
							existingCount := 0
							for _, record := range clickHouseRecords {
								eventID := shared.ConvertToUInt32(record["event_id"])
								editionID := shared.ConvertToUInt32(record["edition_id"])
								key := uint64(eventID)<<32 | uint64(editionID)
								if !existingPairs[key] {
									missingRecords = append(missingRecords, record)
								} else {
									existingCount++
								}
							}

							if existingCount > 0 {
								log.Printf("allevent chunk %d: Found %d/%d records already exist in database, will write only %d missing records", chunkNum, existingCount, len(clickHouseRecords), len(missingRecords))
							}

							if len(missingRecords) > 0 {
								// Write JSON with full record data for efficient retry
								if err := writeFailedBatchToJSON(missingRecords, batchNumber, chunkNum); err != nil {
									log.Printf("ERROR: Failed to write failed batch to JSON: %v", err)
								} else {
									log.Printf("allevent chunk %d: Successfully wrote %d missing records to JSON for retry", chunkNum, len(missingRecords))
								}
							} else {
								log.Printf("allevent chunk %d: All %d records already exist in database, skipping file write", chunkNum, len(clickHouseRecords))
							}
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

// buildCityIDLookupFromId10x builds a lookup map from id_10x (format: "city-{id}") to ClickHouse location id
func buildCityIDLookupFromId10x(clickhouseConn driver.Conn, locationTableName string) (map[string]uint32, error) {
	log.Printf("Building city ID lookup from id_10x in %s", locationTableName)
	connectionCheckErr := shared.RetryWithBackoff(
		func() error {
			return shared.CheckClickHouseConnectionAlive(clickhouseConn)
		},
		3,
	)
	if connectionCheckErr != nil {
		return nil, fmt.Errorf("ClickHouse connection is not alive after retries: %w", connectionCheckErr)
	}

	query := fmt.Sprintf(`
		SELECT id, id_10x
		FROM %s
		WHERE location_type = 'CITY' AND id_10x IS NOT NULL
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
		var id10x *string
		if err := rows.Scan(&locationChID, &id10x); err != nil {
			log.Printf("Warning: Failed to scan city row: %v", err)
			continue
		}

		if id10x != nil && *id10x != "" {
			lookup[*id10x] = locationChID
		}
	}

	log.Printf("Built city ID lookup from id_10x: %d cities mapped from %s", len(lookup), locationTableName)
	return lookup, nil
}

// buildStateIDLookupFromId10x builds a lookup map from id_10x (format: "state-{id}-{ISO}") to ClickHouse location id
func buildStateIDLookupFromId10x(clickhouseConn driver.Conn, locationTableName string) (map[string]uint32, error) {
	log.Printf("Building state ID lookup from id_10x in %s", locationTableName)
	connectionCheckErr := shared.RetryWithBackoff(
		func() error {
			return shared.CheckClickHouseConnectionAlive(clickhouseConn)
		},
		3,
	)
	if connectionCheckErr != nil {
		return nil, fmt.Errorf("ClickHouse connection is not alive after retries: %w", connectionCheckErr)
	}

	query := fmt.Sprintf(`
		SELECT id, id_10x
		FROM %s
		WHERE location_type = 'STATE' AND id_10x IS NOT NULL
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
		var id10x *string
		if err := rows.Scan(&locationChID, &id10x); err != nil {
			log.Printf("Warning: Failed to scan state row: %v", err)
			continue
		}

		if id10x != nil && *id10x != "" {
			lookup[*id10x] = locationChID
		}
	}

	log.Printf("Built state ID lookup from id_10x: %d states mapped from %s", len(lookup), locationTableName)
	return lookup, nil
}

// buildStateIDLookupFromUUID builds a lookup map from state_uuid to ClickHouse location id
func buildStateIDLookupFromUUID(clickhouseConn driver.Conn, locationTableName string) (map[string]uint32, error) {
	log.Printf("Building state ID lookup from state_uuid in %s", locationTableName)
	connectionCheckErr := shared.RetryWithBackoff(
		func() error {
			return shared.CheckClickHouseConnectionAlive(clickhouseConn)
		},
		3,
	)
	if connectionCheckErr != nil {
		return nil, fmt.Errorf("ClickHouse connection is not alive after retries: %w", connectionCheckErr)
	}

	query := fmt.Sprintf(`
		SELECT id, id_uuid
		FROM %s
		WHERE location_type = 'STATE' AND id_uuid IS NOT NULL
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
		var stateUUID *string
		if err := rows.Scan(&locationChID, &stateUUID); err != nil {
			log.Printf("Warning: Failed to scan state row: %v", err)
			continue
		}

		if stateUUID != nil && *stateUUID != "" {
			lookup[*stateUUID] = locationChID
		}
	}

	log.Printf("Built state ID lookup from state_uuid: %d states mapped from %s", len(lookup), locationTableName)
	return lookup, nil
}

// buildCityStateUUIDLookup builds a lookup map from city id_10x to state_uuid
func buildCityStateUUIDLookup(clickhouseConn driver.Conn, locationTableName string) (map[string]string, error) {
	log.Printf("Building city to state_uuid lookup in %s", locationTableName)
	connectionCheckErr := shared.RetryWithBackoff(
		func() error {
			return shared.CheckClickHouseConnectionAlive(clickhouseConn)
		},
		3,
	)
	if connectionCheckErr != nil {
		return nil, fmt.Errorf("ClickHouse connection is not alive after retries: %w", connectionCheckErr)
	}

	query := fmt.Sprintf(`
		SELECT id_10x, state_uuid
		FROM %s
		WHERE location_type = 'CITY' AND id_10x IS NOT NULL AND state_uuid IS NOT NULL
	`, locationTableName)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var rows driver.Rows
	var err error
	queryErr := shared.RetryWithBackoff(
		func() error {
			rows, err = clickhouseConn.Query(ctx, query)
			if err != nil {
				return fmt.Errorf("failed to query %s for city state_uuid: %v", locationTableName, err)
			}
			return nil
		},
		3,
	)
	if queryErr != nil {
		return nil, queryErr
	}
	defer rows.Close()

	lookup := make(map[string]string)
	for rows.Next() {
		var cityID10x *string
		var stateUUID *string
		if err := rows.Scan(&cityID10x, &stateUUID); err != nil {
			log.Printf("Warning: Failed to scan city state_uuid row: %v", err)
			continue
		}

		if cityID10x != nil && *cityID10x != "" && stateUUID != nil && *stateUUID != "" {
			lookup[*cityID10x] = *stateUUID
		}
	}

	log.Printf("Built city to state_uuid lookup: %d cities mapped from %s", len(lookup), locationTableName)
	return lookup, nil
}

// buildVenueIDLookupFromId10x builds a lookup map from id_10x (format: "venue-{id}") to ClickHouse location id
func buildVenueIDLookupFromId10x(clickhouseConn driver.Conn, locationTableName string) (map[string]uint32, error) {
	log.Printf("Building venue ID lookup from id_10x in %s", locationTableName)
	connectionCheckErr := shared.RetryWithBackoff(
		func() error {
			return shared.CheckClickHouseConnectionAlive(clickhouseConn)
		},
		3,
	)
	if connectionCheckErr != nil {
		return nil, fmt.Errorf("ClickHouse connection is not alive after retries: %w", connectionCheckErr)
	}

	query := fmt.Sprintf(`
		SELECT id, id_10x
		FROM %s
		WHERE location_type = 'VENUE' AND id_10x IS NOT NULL
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
		var id10x *string
		if err := rows.Scan(&locationChID, &id10x); err != nil {
			log.Printf("Warning: Failed to scan venue row: %v", err)
			continue
		}

		if id10x != nil && *id10x != "" {
			lookup[*id10x] = locationChID
		}
	}

	log.Printf("Built venue ID lookup from id_10x: %d venues mapped from %s", len(lookup), locationTableName)
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
	eventIDStrings := make([]string, len(eventIDs))
	for i, id := range eventIDs {
		eventIDStrings[i] = strconv.FormatInt(id, 10)
	}

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"terms": map[string]interface{}{
				"id": eventIDStrings,
			},
		},
		"size":    len(eventIDs),
		"_source": []string{"id", "description", "exhibitors", "speakers", "totalSponsor", "following", "punchline", "frequency", "city", "hybrid", "logo", "pricing", "total_edition", "avg_rating", "eventEstimatedTag", "impactScore", "inboundScore", "internationalScore", "repeatSentimentChangePercentage", "repeatSentiment", "reputationSentiment", "audienceZone", "yoyGrowth", "pred_startDate", "pred_endDate", "pred_score", "finalEstimate", "highEstimate", "lowEstimate"},
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

		// Check if pred_startDate is not more than 3 months from now
		// If so, set pred_startDate, pred_endDate, and pred_score to nil
		var predStartDate interface{}
		var predEndDate interface{}
		var predScore interface{}

		now := time.Now()
		threeMonthsFromNow := now.AddDate(0, 3, 0)

		shouldInvalidate := false

		// Check pred_endDate: invalidate if less than 3 months from now
		// if predEndDateRaw := source["pred_endDate"]; predEndDateRaw != nil {
		// 	if predEndDateStr, ok := predEndDateRaw.(string); ok && predEndDateStr != "" {
		// 		if predEndDateParsed, err := time.Parse("2006-01-02", predEndDateStr); err == nil {
		// 			if predEndDateParsed.Before(threeMonthsFromNow) {
		// 				shouldInvalidate = true
		// 			}
		// 		}
		// 	}
		// }

		// Check pred_startDate: invalidate if not more than 3 months from now
		if !shouldInvalidate {
			if predStartDateRaw := source["pred_startDate"]; predStartDateRaw != nil {
				if predStartDateStr, ok := predStartDateRaw.(string); ok && predStartDateStr != "" {
					if predStartDateParsed, err := time.Parse("2006-01-02", predStartDateStr); err == nil {
						if !predStartDateParsed.After(threeMonthsFromNow) {
							shouldInvalidate = true
						}
					} else {
						shouldInvalidate = true
					}
				} else {
					shouldInvalidate = true
				}
			} else {
				shouldInvalidate = true
			}
		}

		if shouldInvalidate {
			predStartDate = nil
			predEndDate = nil
			predScore = nil
		} else {
			predStartDate = source["pred_startDate"]
			predEndDate = source["pred_endDate"]
			predScore = source["pred_score"]
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
			"pred_startDate":                  predStartDate,
			"pred_endDate":                    predEndDate,
			"pred_score":                      predScore,
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
			edition_id, edition_uuid, edition_country, edition_city, edition_city_name, edition_city_state_id, edition_city_state, edition_city_lat, edition_city_long,
			company_id, company_uuid, companyPublished, company_name, company_domain, company_website, companyLogoUrl, company_country, company_state, company_city, company_city_name, company_address,
			venue_id, venue_name, venue_country, venue_city, venue_city_name, venue_lat, venue_long,
			published, status, editions_audiance_type, edition_functionality, edition_website, edition_domain,
			edition_type, event_editions, event_format, event_followers, edition_followers, event_exhibitor, edition_exhibitor,
			exhibitors_upper_bound, exhibitors_lower_bound, exhibitors_mean,
			event_sponsor, edition_sponsor, event_speaker, edition_speaker,
			event_created, event_updated, edition_created, event_hybrid, isBranded, eventBrandId, eventSeriesId, maturity,
			event_pricing, tickets, timings, event_logo, event_estimatedVisitors, estimatedVisitorsMean, estimatedSize, event_frequency, impactScore, inboundScore, internationalScore, repeatSentimentChangePercentage, repeatSentiment, reputationChangePercentage, audienceZone,
			inboundPercentage, inboundAttendance, internationalPercentage, internationalAttendance,
			event_economic_FoodAndBevarage, event_economic_Transportation, event_economic_Accomodation, event_economic_Utilities, event_economic_flights, event_economic_value,
			event_economic_dayWiseEconomicImpact, event_economic_breakdown, event_economic_impact, keywords, event_score, yoyGrowth, futureExpexctedStartDate, futureExpexctedEndDate, predictionScore, PrimaryEventType, verifiedOn, last_updated_at, version
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
			alleventRecord.EditionUUID,                     // edition_uuid: UUID NOT NULL
			alleventRecord.EditionCountry,                  // edition_country: LowCardinality(FixedString(2)) NOT NULL
			alleventRecord.EditionCity,                     // edition_city: UInt32 NOT NULL
			alleventRecord.EditionCityName,                 // edition_city_name: String NOT NULL
			alleventRecord.EditionCityStateID,              // edition_city_state_id: Nullable(UInt32)
			alleventRecord.EditionCityState,                // edition_city_state: LowCardinality(String) NOT NULL
			alleventRecord.EditionCityLat,                  // edition_city_lat: Float64 NOT NULL
			alleventRecord.EditionCityLong,                 // edition_city_long: Float64 NOT NULL
			alleventRecord.CompanyID,                       // company_id: Nullable(UInt32)
			alleventRecord.CompanyUUID,                     // company_uuid: Nullable(UUID)
			alleventRecord.CompanyPublished,                // companyPublished: Nullable(Int8)
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
			alleventRecord.PredictionScore,                 // predictionScore: Nullable(Int32) DEFAULT 0
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

func insertDayWiseEconomicImpactBatch(
	clickhouseConn driver.Conn,
	records []dayWiseEconomicImpactRecord,
) error {

	if len(records) == 0 {
		return nil
	}

	ctx := context.Background()
	insertSQL := `
		INSERT INTO event_daywiseEconomicImpact_temp (
			event_id,
			date,
			metric,
			value,
			last_updated_at
		)
	`

	maxRetries := 3
	var batch driver.Batch
	var err error

	for retryCount := 0; retryCount < maxRetries; retryCount++ {
		batch, err = clickhouseConn.PrepareBatch(ctx, insertSQL)
		if err == nil {
			break
		}

		if retryCount < maxRetries-1 {
			log.Printf("WARNING: ClickHouse PrepareBatch error for daywise table (attempt %d/%d): %v",
				retryCount+1, maxRetries, err)
			time.Sleep(2 * time.Second)
		}
	}

	if err != nil {
		return fmt.Errorf("failed to prepare batch for daywise table after %d retries: %v", maxRetries, err)
	}

	for _, record := range records {
		err := batch.Append(
			record.EventID,
			record.Date,
			record.Metric,
			record.Value,
			record.LastUpdatedAt,
		)

		if err != nil {
			return fmt.Errorf("failed to append record to batch (event_id=%d, date=%s, metric=%s): %v",
				record.EventID, record.Date, record.Metric, err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch for daywise table: %v", err)
	}

	log.Printf("OK: Successfully inserted %d day-wise economic impact records", len(records))
	return nil
}

func insertDayWiseEconomicImpactWithRetry(
	clickhouseConn driver.Conn,
	records []dayWiseEconomicImpactRecord,
) error {

	if len(records) == 0 {
		return nil
	}

	maxRetries := 3
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		lastErr = insertDayWiseEconomicImpactBatch(clickhouseConn, records)

		if lastErr == nil {
			return nil
		}

		log.Printf("Attempt %d/%d failed for day-wise insert: %v", attempt, maxRetries, lastErr)

		if attempt < maxRetries && len(records) > 1000 {
			batchSize := 1000
			if attempt == maxRetries-1 {
				batchSize = 500
			}

			log.Printf("Splitting %d day-wise records into batches of %d", len(records), batchSize)

			successCount := 0
			failCount := 0

			for i := 0; i < len(records); i += batchSize {
				end := i + batchSize
				if end > len(records) {
					end = len(records)
				}

				smallBatch := records[i:end]
				if err := insertDayWiseEconomicImpactBatch(clickhouseConn, smallBatch); err != nil {
					log.Printf("Small day-wise batch %d-%d failed: %v", i, end, err)
					failCount++
					lastErr = err
				} else {
					successCount++
				}
			}

			if failCount == 0 {
				log.Printf("All day-wise small batches succeeded (%d batches)", successCount)
				return nil
			}

			log.Printf("Partial success for day-wise: %d succeeded, %d failed", successCount, failCount)
		}

		if attempt < maxRetries {
			time.Sleep(5 * time.Second)
		}
	}

	return fmt.Errorf("failed to insert day-wise records after %d attempts: %v", maxRetries, lastErr)
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

func extractChunkNumFromJSONFilename(filename string) int {
	base := filepath.Base(filename)
	parts := strings.Split(base, "_")
	if len(parts) >= 5 && parts[0] == "failed" && parts[1] == "batches" && parts[2] == "chunk" {
		if chunkNum, err := strconv.Atoi(parts[3]); err == nil {
			return chunkNum
		}
	}
	return -1
}

func extractBatchNumFromJSONFilename(filename string) int {
	base := filepath.Base(filename)
	parts := strings.Split(base, "_")
	// Handle both patterns:
	// - failed_batches_chunk_%d_batch_%d.json
	// - failed_batches_chunk_%d_batch_%d_part_%d.json
	if len(parts) >= 6 && parts[0] == "failed" && parts[1] == "batches" && parts[2] == "chunk" && parts[4] == "batch" {
		// parts[5] could be "5.json" or "5" (if followed by part number)
		batchNumStr := parts[5]
		// Remove .json extension if present
		batchNumStr = strings.TrimSuffix(batchNumStr, ".json")
		if batchNum, err := strconv.Atoi(batchNumStr); err == nil {
			return batchNum
		}
	}
	return -1
}

func writeFailedBatchToJSON(records []map[string]interface{}, batchNum int, chunkNum int) error {
	const maxFileSize = 20 * 1024 * 1024 // 20MB limit
	dir := "failed_batches"
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	var currentFile *os.File
	var currentWriter *bufio.Writer
	var currentFilePath string
	var currentFileSize int64
	var partNum int
	var totalRecordsWritten int
	var invalidJSONFile *os.File
	var invalidJSONWriter *bufio.Writer
	var invalidJSONCount int

	createNewFile := func() error {
		if currentWriter != nil {
			if err := currentWriter.Flush(); err != nil {
				return fmt.Errorf("failed to flush buffer: %w", err)
			}
		}
		if currentFile != nil {
			if err := currentFile.Close(); err != nil {
				return fmt.Errorf("failed to close file: %w", err)
			}
		}

		if partNum == 0 {
			currentFilePath = filepath.Join(dir, fmt.Sprintf("failed_batches_chunk_%d_batch_%d.json", chunkNum, batchNum))
		} else {
			currentFilePath = filepath.Join(dir, fmt.Sprintf("failed_batches_chunk_%d_batch_%d_part_%d.json", chunkNum, batchNum, partNum))
		}

		var err error
		currentFile, err = os.Create(currentFilePath)
		if err != nil {
			return fmt.Errorf("failed to create JSON file %s: %w", currentFilePath, err)
		}
		// Use a larger buffer (1MB) to handle large JSON records efficiently
		currentWriter = bufio.NewWriterSize(currentFile, 1024*1024)
		currentFileSize = 0
		partNum++
		return nil
	}

	ensureInvalidJSONFile := func() error {
		if invalidJSONFile == nil {
			invalidJSONPath := filepath.Join(dir, fmt.Sprintf("invalid_json_chunk_%d_batch_%d.json", chunkNum, batchNum))
			var err error
			invalidJSONFile, err = os.OpenFile(invalidJSONPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				return fmt.Errorf("failed to create invalid JSON file %s: %w", invalidJSONPath, err)
			}
			invalidJSONWriter = bufio.NewWriter(invalidJSONFile)
		}
		return nil
	}

	if err := createNewFile(); err != nil {
		return err
	}
	defer func() {
		if currentWriter != nil {
			currentWriter.Flush()
		}
		if currentFile != nil {
			currentFile.Close()
		}
		if invalidJSONWriter != nil {
			invalidJSONWriter.Flush()
		}
		if invalidJSONFile != nil {
			invalidJSONFile.Close()
		}
	}()

	for _, record := range records {
		jsonData, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("failed to marshal record to JSON: %w", err)
		}

		if !json.Valid(jsonData) {
			if err := ensureInvalidJSONFile(); err != nil {
				return err
			}
			if _, err := invalidJSONWriter.Write(jsonData); err != nil {
				return fmt.Errorf("failed to write invalid JSON to file: %w", err)
			}
			if _, err := invalidJSONWriter.WriteString("\n"); err != nil {
				return fmt.Errorf("failed to write newline to invalid JSON file: %w", err)
			}
			invalidJSONCount++
			log.Printf("WARNING: Invalid JSON detected for batch %d (full record written to invalid_json file)", batchNum)
			continue
		}

		recordSize := int64(len(jsonData) + 1)

		// Check if this single record exceeds the file size limit
		if recordSize > maxFileSize {
			log.Printf("WARNING: Record size (%d bytes) exceeds file size limit (%d bytes) for batch %d. Writing to new file.", recordSize, maxFileSize, batchNum)
			// If current file has data, close it first
			if currentFileSize > 0 {
				if err := createNewFile(); err != nil {
					return err
				}
			}
		} else {
			// Check if adding this record would exceed the limit (only if record fits in one file)
			// Use safety margin: if we're within 1MB of limit, create new file to prevent any edge cases
			safetyMargin := int64(1024 * 1024) // 1MB safety margin
			if currentFileSize > 0 {
				// Only check actual file size if we're getting close to the limit (within 2MB)
				// This avoids expensive stat() calls for every record
				if currentFileSize+recordSize > maxFileSize-(2*safetyMargin) {
					// Flush buffer first to get accurate file size on disk
					if err := currentWriter.Flush(); err != nil {
						return fmt.Errorf("failed to flush buffer before size check: %w", err)
					}
					// Get actual file size on disk for accurate checking
					fileInfo, err := currentFile.Stat()
					if err != nil {
						return fmt.Errorf("failed to get file size: %w", err)
					}
					actualFileSize := fileInfo.Size()

					// Update currentFileSize to match actual disk size
					currentFileSize = actualFileSize

					// Check with safety margin: create new file if we're close to limit
					if actualFileSize+recordSize > maxFileSize-safetyMargin {
						log.Printf("File size limit approaching (%d bytes, adding %d bytes would exceed %d bytes), creating new file part %d for batch %d", actualFileSize, recordSize, maxFileSize-safetyMargin, partNum, batchNum)
						if err := createNewFile(); err != nil {
							return err
						}
					}
				} else if currentFileSize+recordSize > maxFileSize {
					// Quick check: if in-memory size would exceed limit, create new file
					log.Printf("File size limit reached (%d bytes), creating new file part %d for batch %d", currentFileSize, partNum, batchNum)
					if err := createNewFile(); err != nil {
						return err
					}
				}
			}
		}

		// Write the complete record atomically in a single operation
		// Combine jsonData + newline into one write for true atomicity
		recordWithNewline := append(jsonData, '\n')

		// For very large records, write directly to file to avoid buffer issues
		if recordSize > 1024*1024 { // If record > 1MB, write directly
			if err := currentWriter.Flush(); err != nil {
				return fmt.Errorf("failed to flush buffer before large record write: %w", err)
			}
			// Single atomic write: record + newline together
			if _, err := currentFile.Write(recordWithNewline); err != nil {
				return fmt.Errorf("failed to write large record to file: %w", err)
			}
			// Sync to disk for large records to ensure data is persisted
			if err := currentFile.Sync(); err != nil {
				return fmt.Errorf("failed to sync large record to disk: %w", err)
			}
		} else {
			// For smaller records, use buffered write (single atomic operation)
			if _, err := currentWriter.Write(recordWithNewline); err != nil {
				return fmt.Errorf("failed to write record to file: %w", err)
			}
			// Flush immediately after each record to prevent truncation if process is interrupted
			if err := currentWriter.Flush(); err != nil {
				return fmt.Errorf("failed to flush after writing record: %w", err)
			}
		}
		currentFileSize += recordSize
		totalRecordsWritten++
	}

	if err := currentWriter.Flush(); err != nil {
		return fmt.Errorf("failed to flush buffer: %w", err)
	}
	// Sync to disk to ensure all data is persisted
	if currentFile != nil {
		if err := currentFile.Sync(); err != nil {
			return fmt.Errorf("failed to sync file to disk: %w", err)
		}
	}

	if invalidJSONWriter != nil {
		if err := invalidJSONWriter.Flush(); err != nil {
			return fmt.Errorf("failed to flush invalid JSON buffer: %w", err)
		}
	}

	if partNum > 1 {
		log.Printf("Written %d failed records across %d files (batch %d, chunk %d)", totalRecordsWritten, partNum, batchNum, chunkNum)
	} else {
		log.Printf("Written %d failed records to %s (batch %d)", totalRecordsWritten, currentFilePath, batchNum)
	}
	if invalidJSONCount > 0 {
		log.Printf("WARNING: %d invalid JSON records written to invalid_json_chunk_%d_batch_%d.json", invalidJSONCount, chunkNum, batchNum)
	}
	return nil
}

func readFailedBatchFromJSON(filepath string) ([]map[string]interface{}, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to open JSON file %s: %w", filepath, err)
	}
	defer file.Close()

	var records []map[string]interface{}
	scanner := bufio.NewScanner(file)

	// Increase buffer size to handle large JSON records (default is 64KB)
	// Set to 10MB to handle very large records
	const maxCapacity = 50 * 1024 * 1024 // 50MB
	buf := make([]byte, 0, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	lineNum := 0

	for scanner.Scan() {
		lineNum++
		rawLine := scanner.Text()
		line := strings.TrimSpace(rawLine)
		if line == "" {
			continue
		}

		// Debug: Check if json.Valid passes but Unmarshal fails (indicates trailing data)
		isValid := json.Valid([]byte(line))

		var record map[string]interface{}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			errMsg := err.Error()

			// If json.Valid passed but Unmarshal failed, there's trailing data after valid JSON
			if isValid && strings.Contains(errMsg, "end of file expected") {
				// Find where valid JSON ends and trailing data begins
				decoder := json.NewDecoder(strings.NewReader(line))
				var temp map[string]interface{}
				decodeErr := decoder.Decode(&temp)
				if decodeErr == nil {
					// Valid JSON was decoded, check what's left
					remaining, _ := decoder.Token()
					log.Printf("ERROR: Line %d in %s has trailing data after valid JSON", lineNum, filepath)
					log.Printf("  Valid JSON decoded successfully (%d keys)", len(temp))
					log.Printf("  Trailing data: %v", remaining)
					log.Printf("  Line length: %d bytes", len(line))
					firstChars := 200
					if len(line) < firstChars {
						firstChars = len(line)
					}
					log.Printf("  First 200 chars: %s", line[:firstChars])
					lastChars := 200
					if len(line) < lastChars {
						lastChars = len(line)
					}
					startIdx := len(line) - lastChars
					if startIdx < 0 {
						startIdx = 0
					}
					log.Printf("  Last 200 chars: %s", line[startIdx:])

					// Try to find the end of valid JSON
					for i := len(line) - 1; i >= 0; i-- {
						testLine := line[:i+1]
						if json.Valid([]byte(testLine)) {
							if json.Unmarshal([]byte(testLine), &temp) == nil {
								log.Printf("  Valid JSON ends at position %d (trailing: %d bytes)", i+1, len(line)-i-1)
								if i+1 < len(line) {
									log.Printf("  Trailing content: %q", line[i+1:])
								}
								break
							}
						}
					}
				}
				return nil, fmt.Errorf("line %d has trailing data after valid JSON: %w", lineNum, err)
			}

			// Handle other JSON errors
			if strings.Contains(errMsg, "unexpected end of JSON input") {
				log.Printf("WARNING: Truncated JSON record on line %d in file %s", lineNum, filepath)
				if len(line) > 100 {
					log.Printf("  Last 100 chars: ...%s", line[len(line)-100:])
				} else {
					log.Printf("  Line content: %s", line)
				}
				return nil, fmt.Errorf("truncated JSON on line %d: %w", lineNum, err)
			}

			return nil, fmt.Errorf("failed to unmarshal JSON on line %d: %w", lineNum, err)
		}

		if !isValid {
			log.Printf("WARNING: json.Valid failed but Unmarshal succeeded on line %d (unusual)", lineNum)
		}

		records = append(records, record)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read JSON file %s: %w", filepath, err)
	}

	return records, nil
}

var queryLogFile *os.File
var queryLogMutex sync.Mutex

func initQueryLogFile() error {
	queryLogMutex.Lock()
	defer queryLogMutex.Unlock()

	if queryLogFile != nil {
		return nil
	}

	logDir := "failed_batches"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %w", err)
	}

	logFile := filepath.Join(logDir, fmt.Sprintf("existence_check_queries_%s.log", time.Now().Format("20060102_150405")))
	file, err := os.Create(logFile)
	if err != nil {
		return fmt.Errorf("failed to create query log file: %w", err)
	}

	queryLogFile = file
	file.WriteString(fmt.Sprintf("=== Existence Check Queries Log - Started at %s ===\n\n", time.Now().Format("2006-01-02 15:04:05")))
	return nil
}

func logQueryToFile(query string, pairs []EventEditionPair, foundCount int) {
	if foundCount == len(pairs) {
		return
	}

	queryLogMutex.Lock()
	defer queryLogMutex.Unlock()

	if queryLogFile == nil {
		if err := initQueryLogFile(); err != nil {
			return
		}
	}

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	queryLogFile.WriteString(fmt.Sprintf("[%s] Checking %d pairs, Found %d existing\n", timestamp, len(pairs), foundCount))

	maxQueryLen := 1000
	if len(query) > maxQueryLen {
		queryPreview := query[:maxQueryLen] + "... [TRUNCATED, " + fmt.Sprintf("%d", len(query)-maxQueryLen) + " chars omitted] ..." + query[len(query)-200:]
		queryLogFile.WriteString(fmt.Sprintf("QUERY (truncated):\n%s\n\n", queryPreview))
	} else {
		queryLogFile.WriteString(fmt.Sprintf("QUERY:\n%s\n\n", query))
	}

	sampleSize := 10
	if len(pairs) < sampleSize {
		sampleSize = len(pairs)
	}
	queryLogFile.WriteString(fmt.Sprintf("PAIRS BEING CHECKED (showing first %d of %d):\n", sampleSize, len(pairs)))
	for i := 0; i < sampleSize; i++ {
		queryLogFile.WriteString(fmt.Sprintf("  (%d, %d)\n", pairs[i].EventID, pairs[i].EditionID))
	}
	if len(pairs) > sampleSize {
		queryLogFile.WriteString(fmt.Sprintf("  ... (%d more pairs)\n", len(pairs)-sampleSize))
	}
	queryLogFile.WriteString("\n")
}

func checkExistenceInClickHouse(clickhouseConn driver.Conn, pairs []EventEditionPair, config shared.Config) (map[uint64]bool, error) {
	return checkExistenceInClickHouseWithRetry(clickhouseConn, pairs, config, false, nil)
}

func checkExistenceInClickHouseWithRetry(clickhouseConn driver.Conn, pairs []EventEditionPair, config shared.Config, useFinal bool, logToFileFunc func(string, ...interface{})) (map[uint64]bool, error) {
	if len(pairs) == 0 {
		return make(map[uint64]bool), nil
	}

	tableName := shared.GetTableNameWithDB("allevent_temp", config)

	batchSize := 500 // Reduced from 1000 to 500 for better FINAL query performance (FINAL forces merges which can be slow)
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

		// Use FINAL keyword to force merge and see all data including unmerged parts
		// This ensures we see data that was just inserted but hasn't been merged yet
		finalKeyword := ""
		if useFinal {
			finalKeyword = "FINAL"
		}

		query := fmt.Sprintf(
			"SELECT event_id, edition_id FROM %s %s WHERE (event_id, edition_id) IN (%s)",
			tableName,
			finalKeyword,
			strings.Join(tuples, ","),
		)

		// Log FULL query to retry log file if available
		if logToFileFunc != nil {
			logToFileFunc("        EXISTENCE CHECK QUERY [batch %d/%d, useFinal=%v]:", i/batchSize+1, (len(pairs)+batchSize-1)/batchSize, useFinal)
			logToFileFunc("        %s", query)
			logToFileFunc("        Checking %d pairs in this batch", len(tuples))
		}

		// Log the query with sample pairs for debugging duplicates
		if i == 0 {
			// Log first batch with details
			sampleSize := 10
			if len(tuples) < sampleSize {
				sampleSize = len(tuples)
			}
			sampleTuples := tuples[:sampleSize]
			log.Printf("EXISTENCE CHECK QUERY [batch %d/%d, useFinal=%v]: SELECT event_id, edition_id FROM %s %s WHERE (event_id, edition_id) IN (%s%s) [checking %d pairs total]",
				i/batchSize+1,
				(len(pairs)+batchSize-1)/batchSize,
				useFinal,
				tableName,
				finalKeyword,
				strings.Join(sampleTuples, ", "),
				func() string {
					if len(tuples) > sampleSize {
						return fmt.Sprintf(", ... (%d more)", len(tuples)-sampleSize)
					}
					return ""
				}(),
				len(tuples),
			)
		} else {
			// Log concise summary for subsequent batches
			log.Printf("EXISTENCE CHECK QUERY [batch %d/%d, useFinal=%v]: Checking %d pairs", i/batchSize+1, (len(pairs)+batchSize-1)/batchSize, useFinal, len(tuples))
		}

		// Retry logic to account for merge delays
		var rows driver.Rows
		var err error
		var foundCount int
		var scanDuration time.Duration
		maxRetries := 3
		querySuccessful := false

		for retry := 0; retry < maxRetries; retry++ {
			if retry > 0 {
				// Wait 10 seconds on failure before retry
				if logToFileFunc != nil {
					logToFileFunc("        ⏳ Waiting 10 seconds before existence check retry %d/%d...", retry+1, maxRetries)
				}
				time.Sleep(10 * time.Second)
			}

			// Log query start time for performance tracking
			queryStartTime := time.Now()
			if logToFileFunc != nil {
				logToFileFunc("        Executing query [batch %d/%d]...", i/batchSize+1, (len(pairs)+batchSize-1)/batchSize)
			}

			// Create context with timeout - keep it alive during row scanning
			// ClickHouse driver may execute query lazily during rows.Next()
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			rows, err = clickhouseConn.Query(ctx, query)

			if err != nil {
				cancel() // Cancel immediately on error
				queryDuration := time.Since(queryStartTime)
				if logToFileFunc != nil {
					logToFileFunc("        Query failed after %v: %v", queryDuration, err)
				}
				if retry < maxRetries-1 {
					log.Printf("WARNING: Failed to query existence (attempt %d/%d) after %v, retrying: %v", retry+1, maxRetries, queryDuration, err)
				}
				continue // Retry
			}

			// Query() returned successfully, but actual execution might happen during rows.Next()
			// Time the row scanning phase (this is where the actual query execution happens)
			scanStartTime := time.Now()
			foundCount = 0
			for rows.Next() {
				var eventID uint32
				var editionID uint32
				if err := rows.Scan(&eventID, &editionID); err != nil {
					cancel() // Cancel context before closing
					rows.Close()
					return nil, fmt.Errorf("failed to scan row: %w", err)
				}
				key := uint64(eventID)<<32 | uint64(editionID)
				existingPairs[key] = true
				foundCount++
			}
			scanDuration = time.Since(scanStartTime)
			totalDuration := time.Since(queryStartTime)

			// Check for errors during iteration
			if err := rows.Err(); err != nil {
				cancel()
				rows.Close()
				return nil, fmt.Errorf("error during row iteration: %w", err)
			}

			rows.Close()
			cancel() // Cancel context after all operations complete

			if logToFileFunc != nil {
				logToFileFunc("        Query + Scan completed in %v (scan: %v, query setup: %v)", totalDuration, scanDuration, scanStartTime.Sub(queryStartTime))
			}

			// Successfully completed query and scanning
			querySuccessful = true
			break
		}

		if !querySuccessful {
			return nil, fmt.Errorf("failed to query existence after %d retries: %w", maxRetries, err)
		}

		// Log results to retry log file if available
		if logToFileFunc != nil {
			logToFileFunc("        EXISTENCE CHECK RESULT [batch %d/%d]: Found %d/%d pairs already exist in database",
				i/batchSize+1,
				(len(pairs)+batchSize-1)/batchSize,
				foundCount,
				len(tuples),
			)
		}

		// Query is already logged via log.Printf above - no need to log to file

		// Log results for debugging - print summary for all batches
		log.Printf("EXISTENCE CHECK RESULT [batch %d/%d]: Found %d/%d pairs already exist in database",
			i/batchSize+1,
			(len(pairs)+batchSize-1)/batchSize,
			foundCount,
			len(tuples),
		)
		if i == 0 {
			// Log sample of found pairs (first 5)
			if foundCount > 0 {
				sampleCount := 0
				samplePairs := make([]string, 0, 5)
				for _, pair := range batch {
					if sampleCount >= 5 {
						break
					}
					key := uint64(pair.EventID)<<32 | uint64(pair.EditionID)
					if existingPairs[key] {
						samplePairs = append(samplePairs, fmt.Sprintf("(%d,%d)", pair.EventID, pair.EditionID))
						sampleCount++
					}
				}
				if len(samplePairs) > 0 {
					sampleMsg := fmt.Sprintf("EXISTENCE CHECK SAMPLE: Found pairs: %s%s",
						strings.Join(samplePairs, ", "),
						func() string {
							if foundCount > len(samplePairs) {
								return fmt.Sprintf(" ... (%d more)", foundCount-len(samplePairs))
							}
							return ""
						}(),
					)
					log.Print(sampleMsg)
					if logToFileFunc != nil {
						logToFileFunc("        %s", sampleMsg)
					}
				}
			}
		}

		// Log batch completion
		if logToFileFunc != nil {
			logToFileFunc("        Batch %d/%d completed, continuing to next batch...", i/batchSize+1, (len(pairs)+batchSize-1)/batchSize)
		}
	}

	if logToFileFunc != nil {
		logToFileFunc("        All batches completed for existence check")
	}

	return existingPairs, nil
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
	cityIDLookup, err := buildCityIDLookupFromId10x(clickhouseConn, locationTableName)
	if err != nil {
		log.Printf("WARNING - Failed to build city ID lookup: %v", err)
		cityIDLookup = make(map[string]uint32)
	}
	stateIDLookup, err := buildStateIDLookupFromId10x(clickhouseConn, locationTableName)
	if err != nil {
		log.Printf("WARNING - Failed to build state ID lookup: %v", err)
		stateIDLookup = make(map[string]uint32)
	}
	// Build state UUID lookup for direct state_uuid to id mapping
	stateUUIDLookup, err := buildStateIDLookupFromUUID(clickhouseConn, locationTableName)
	if err != nil {
		log.Printf("WARNING - Failed to build state UUID lookup: %v", err)
		stateUUIDLookup = make(map[string]uint32)
	}
	// Build city to state_uuid lookup
	cityStateUUIDLookup, err := buildCityStateUUIDLookup(clickhouseConn, locationTableName)
	if err != nil {
		log.Printf("WARNING - Failed to build city state UUID lookup: %v", err)
		cityStateUUIDLookup = make(map[string]string)
	}
	venueIDLookup, err := buildVenueIDLookupFromId10x(clickhouseConn, locationTableName)
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
	predictedDatesMap := fetchalleventPredictedDatesForBatch(mysqlDB, actualEventIDs)
	rawTicketData := fetchalleventTicketDataForBatch(mysqlDB, actualEventIDs)
	ticketDataMap := processalleventTicketData(rawTicketData)
	timingDataMap := fetchalleventTimingDataForBatch(mysqlDB, filteredEditionData)
	processedEconomicData := processalleventEconomicImpactDataParallel(estimateDataMap)

	if len(processedEconomicData) > 0 {
		log.Printf("Processing day-wise economic impact data for %d events in batch", len(processedEconomicData))
		lastUpdatedAt := time.Now().Format("2006-01-02 15:04:05")

		dayWiseRecords := aggregateDayWiseRecordsFromEconomicData(
			processedEconomicData,
			lastUpdatedAt,
		)

		if len(dayWiseRecords) > 0 {
			log.Printf("Inserting %d day-wise records into event_daywiseEconomicImpact_temp for batch", len(dayWiseRecords))

			if err := insertDayWiseEconomicImpactWithRetry(
				clickhouseConn,
				dayWiseRecords,
			); err != nil {
				log.Printf("WARNING: Failed to insert day-wise records for batch: %v", err)
			} else {
				log.Printf("Successfully inserted %d day-wise economic impact records for batch", len(dayWiseRecords))
			}
		} else {
			log.Printf("No day-wise records to insert for this batch")
		}
	}

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
			companyCity,
			venue,
			venueCity,
			editionCountryISO,
			cityIDLookup,
			stateIDLookup,
			stateUUIDLookup,
			cityStateUUIDLookup,
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
			predictedDatesMap[actualEventID],
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
	companyCity map[string]interface{},
	venue map[string]interface{},
	venueCity map[string]interface{},
	editionCountryISO string,
	cityIDLookup map[string]uint32,
	stateIDLookup map[string]uint32,
	stateUUIDLookup map[string]uint32,
	cityStateUUIDLookup map[string]string,
	venueIDLookup map[string]uint32,
) (
	editionCityLocationChID *uint32,
	companyCityLocationChID *uint32,
	venueCityLocationChID *uint32,
	editionCityStateLocationChID *uint32,
	venueLocationChID *uint32,
) {
	// Edition city location ID - using id_10x format: "city-{id}"
	if city != nil && city["id"] != nil {
		if cityID, ok := city["id"].(int64); ok && cityID > 0 {
			id10x := fmt.Sprintf("city-%d", cityID)
			if locationChID, exists := cityIDLookup[id10x]; exists {
				editionCityLocationChID = &locationChID

				// Edition city state location ID - using state_uuid from city record
				if stateUUID, exists := cityStateUUIDLookup[id10x]; exists && stateUUID != "" {
					if locationChID, exists := stateUUIDLookup[stateUUID]; exists {
						editionCityStateLocationChID = &locationChID
					}
				}
			}
		}
	}

	// Company city location ID - using id_10x format: "city-{id}"
	if companyCity != nil && companyCity["id"] != nil {
		if companyCityID, ok := companyCity["id"].(int64); ok && companyCityID > 0 {
			id10x := fmt.Sprintf("city-%d", companyCityID)
			if locationChID, exists := cityIDLookup[id10x]; exists {
				companyCityLocationChID = &locationChID
			}
		}
	}

	// Venue city location ID - using id_10x format: "city-{id}"
	if venueCity != nil && venueCity["id"] != nil {
		if venueCityID, ok := venueCity["id"].(int64); ok && venueCityID > 0 {
			id10x := fmt.Sprintf("city-%d", venueCityID)
			if locationChID, exists := cityIDLookup[id10x]; exists {
				venueCityLocationChID = &locationChID
			}
		}
	}

	// Fallback: Edition city state location ID - using id_10x format: "state-{id}-{ISO}" (if state_uuid lookup failed)
	if editionCityStateLocationChID == nil && city != nil && city["state_id"] != nil && editionCountryISO != "" && editionCountryISO != "NAN" {
		if stateID, ok := city["state_id"].(int64); ok && stateID > 0 {
			countryISOUpper := strings.ToUpper(strings.TrimSpace(editionCountryISO))
			id10x := fmt.Sprintf("state-%d-%s", stateID, countryISOUpper)
			if locationChID, exists := stateIDLookup[id10x]; exists {
				editionCityStateLocationChID = &locationChID
			}
		}
	}

	// Venue location ID - using id_10x format: "venue-{id}"
	if venue != nil {
		// Try to get venue ID from different possible fields
		var venueID int64
		var found bool

		if venueIDVal, ok := venue["id"].(int64); ok && venueIDVal > 0 {
			venueID = venueIDVal
			found = true
		} else if venueIDVal, ok := venue["venue_id"].(int64); ok && venueIDVal > 0 {
			venueID = venueIDVal
			found = true
		}

		if found {
			id10x := fmt.Sprintf("venue-%d", venueID)
			if locationChID, exists := venueIDLookup[id10x]; exists {
				venueLocationChID = &locationChID
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
	dbPredictedDates map[string]interface{},
) map[string]interface{} {
	record := map[string]interface{}{
		"event_id":          eventData["id"],
		"event_uuid":        shared.GenerateUUIDFromString(fmt.Sprintf("%d-%s", shared.ConvertToUInt32(eventData["id"]), decodeBase64DateTime(eventData["created"]))),
		"event_name":        decodeBase64StringIfNeeded(eventData["event_name"]),
		"event_abbr_name":   decodeBase64NullableStringIfNeeded(eventData["abbr_name"]),
		"event_description": decodeBase64NullableStringIfNeeded(esInfoMap["event_description"]),
		"event_punchline":   decodeBase64NullableStringIfNeeded(esInfoMap["event_punchline"]),
		"start_date":        decodeBase64Date(edition["edition_start_date"]),
		"end_date":          decodeBase64Date(edition["edition_end_date"]),
		"edition_id":        edition["edition_id"],
		"edition_uuid":      shared.GenerateUUIDFromString(fmt.Sprintf("%d-%s", shared.ConvertToUInt32(edition["edition_id"]), decodeBase64DateTime(edition["edition_created"]))),
		"edition_country":   editionCountryISO,
		"edition_city": func() interface{} {
			if editionCityLocationChID != nil {
				return uint32(*editionCityLocationChID)
			}
			return nil
		}(),
		"edition_city_name": decodeBase64StringIfNeeded(city["name"]),
		"edition_city_state": func() string {
			if city != nil && city["state"] != nil {
				stateStr := decodeBase64StringIfNeeded(city["state"])
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
		"company_uuid": func() *string {
			if company != nil {
				if companyID, ok := company["id_10x"].(int64); ok && companyID > 0 {
					created := company["created"]
					createdStr := shared.ConvertToString(created)
					if createdStr != "" {
						idInputString := fmt.Sprintf("%d-%s", companyID, createdStr)
						uuidStr := shared.GenerateUUIDFromString(idInputString)
						return &uuidStr
					}
				}
			}
			return nil
		}(),
		"companyPublished": func() interface{} {
			if company != nil {
				return company["company_published"]
			}
			return nil
		}(),
		"company_name":    decodeBase64NullableStringIfNeeded(company["company_name"]),
		"company_domain":  companyDomain,
		"company_website": decodeBase64NullableStringIfNeeded(company["company_website"]),
		"companyLogoUrl":  decodeBase64NullableStringIfNeeded(company["company_logo_url"]),
		"company_country": func() *string {
			country := decodeBase64NullableStringIfNeeded(company["company_country"])
			if country != nil {
				upper := strings.ToUpper(*country)
				return &upper
			}
			return nil
		}(),
		"company_state": func() *string {
			if companyCity != nil && companyCity["state"] != nil {
				stateStr := decodeBase64NullableStringIfNeeded(companyCity["state"])
				if stateStr != nil && strings.TrimSpace(*stateStr) != "" {
					return stateStr
				}
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
				return decodeBase64NullableStringIfNeeded(companyCity["name"])
			}
			return nil
		}(),
		"company_address": func() *string {
			if company != nil && company["address"] != nil {
				addressStr := decodeBase64NullableStringIfNeeded(company["address"])
				if addressStr != nil && strings.TrimSpace(*addressStr) != "" {
					return addressStr
				}
			}
			return nil
		}(),
		"venue_id": func() interface{} {
			if venueLocationChID != nil {
				return uint32(*venueLocationChID)
			}
			return nil
		}(),
		"venue_name": decodeBase64NullableStringIfNeeded(venue["venue_name"]),
		"venue_country": func() *string {
			country := decodeBase64NullableStringIfNeeded(venue["venue_country"])
			if country != nil {
				upper := strings.ToUpper(*country)
				return &upper
			}
			return nil
		}(),
		"venue_city": func() interface{} {
			if venueCityLocationChID != nil {
				return uint32(*venueCityLocationChID)
			}
			return nil
		}(),
		"venue_city_name": func() *string {
			if venueCity != nil && venueCity["name"] != nil {
				return decodeBase64NullableStringIfNeeded(venueCity["name"])
			}
			return nil
		}(),
		"venue_lat":  venue["venue_lat"],
		"venue_long": venue["venue_long"],
		"published":  eventData["published"],
		// "status":                 eventData["status"],
		"status": func() string {
			allowedStatus := []string{"C", "P", "U"}
			statusStr := strings.TrimSpace(shared.ConvertToString(eventData["status"]))
			if statusStr == "" {
				return "A"
			}
			ch := strings.ToUpper(statusStr)[:1]
			if slices.Contains(allowedStatus, ch) {
				return ch
			}
			return "A"
		}(),
		"editions_audiance_type": eventData["event_audience"],
		"edition_functionality":  eventData["functionality"],
		"edition_website":        decodeBase64NullableStringIfNeeded(edition["edition_website"]),
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
		"event_created":          decodeBase64DateTime(eventData["created"]),
		"event_updated":          decodeBase64DateTime(eventData["modified"]),
		"edition_created":        decodeBase64DateTime(edition["edition_created"]),
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
		"maturity":      determinealleventMaturity(esInfoMap["total_edition"]),
		"event_pricing": normalizePricingValue(esInfoMap["event_pricing"]),
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
		"event_logo":                      decodeBase64NullableStringIfNeeded(esInfoMap["event_logo"]),
		"event_estimatedVisitors":         decodeBase64NullableStringIfNeeded(esInfoMap["eventEstimatedTag"]),
		"event_frequency":                 decodeBase64NullableStringIfNeeded(esInfoMap["event_frequency"]),
		"impactScore":                     esInfoMap["impactScore"],
		"inboundScore":                    esInfoMap["inboundScore"],
		"internationalScore":              esInfoMap["internationalScore"],
		"repeatSentimentChangePercentage": esInfoMap["repeatSentimentChangePercentage"],
		"repeatSentiment":                 esInfoMap["repeatSentiment"],
		"reputationChangePercentage":      esInfoMap["reputationChangePercentage"],
		"audienceZone":                    decodeBase64NullableStringIfNeeded(esInfoMap["audienceZone"]),
		"event_avgRating":                 esInfoMap["avg_rating"],
		"10timesEventPageUrl":             decodeBase64NullableStringIfNeeded(eventData["url"]),
		"keywords":                        []string{},
		"event_score":                     eventData["score"],
		"yoyGrowth":                       esInfoMap["yoyGrowth"],
		"futureExpexctedStartDate": func() *string {
			startDateStr := decodeBase64Date(edition["edition_start_date"])
			endDateStr := decodeBase64Date(edition["edition_end_date"])
			esPredStartDate := decodeBase64NullableDate(esInfoMap["pred_startDate"])
			esPredEndDate := decodeBase64NullableDate(esInfoMap["pred_endDate"])

			// if (db_pred_data_exist is None AND ES predictions are invalidated)
			if dbPredictedDates == nil && esPredStartDate != nil && esPredEndDate != nil && startDateStr != "" && endDateStr != "" {
				startDate, err1 := parseDateLenient(startDateStr)
				endDate, err2 := parseDateLenient(endDateStr)
				predStartDate, err3 := parseDateLenient(*esPredStartDate)
				predEndDate, err4 := parseDateLenient(*esPredEndDate)
				if err1 == nil && err2 == nil && err3 == nil && err4 == nil {
					now := time.Now()
					if startDate.After(predStartDate) && endDate.After(predEndDate) && (predEndDate.Before(now) || predEndDate.Equal(now)) {
						// ES predictions were wrong - invalidate them
						return nil
					}
				}
			}
			// elif db_pred_data_exist is not None (use DB prediction - highest priority)
			if dbPredictedDates != nil {
				if dbStartDate, ok := dbPredictedDates["start_date"].(string); ok && dbStartDate != "" {
					return &dbStartDate
				}
			}
			// elif db_pred_data_exist is None AND ES predictions are valid future predictions
			if dbPredictedDates == nil && esPredStartDate != nil && esPredEndDate != nil && startDateStr != "" && endDateStr != "" {
				startDate, err1 := parseDateLenient(startDateStr)
				endDate, err2 := parseDateLenient(endDateStr)
				predStartDate, err3 := parseDateLenient(*esPredStartDate)
				predEndDate, err4 := parseDateLenient(*esPredEndDate)
				if err1 == nil && err2 == nil && err3 == nil && err4 == nil {
					now := time.Now()
					if startDate.Before(predStartDate) && endDate.Before(predEndDate) && (predEndDate.After(now) || predEndDate.Equal(now)) {
						// Keep ES prediction: pred_start_date = pred_start_date
						return esPredStartDate
					}
				}
			}
			// else (set to None)
			return nil
		}(),
		"futureExpexctedEndDate": func() *string {
			startDateStr := decodeBase64Date(edition["edition_start_date"])
			endDateStr := decodeBase64Date(edition["edition_end_date"])
			// Parse ES predictions
			esPredStartDate := decodeBase64NullableDate(esInfoMap["pred_startDate"])
			esPredEndDate := decodeBase64NullableDate(esInfoMap["pred_endDate"])

			// if (db_pred_data_exist is None AND ES predictions are invalidated)
			if dbPredictedDates == nil && esPredStartDate != nil && esPredEndDate != nil && startDateStr != "" && endDateStr != "" {
				startDate, err1 := parseDateLenient(startDateStr)
				endDate, err2 := parseDateLenient(endDateStr)
				predStartDate, err3 := parseDateLenient(*esPredStartDate)
				predEndDate, err4 := parseDateLenient(*esPredEndDate)
				if err1 == nil && err2 == nil && err3 == nil && err4 == nil {
					now := time.Now()
					if startDate.After(predStartDate) && endDate.After(predEndDate) && (predEndDate.Before(now) || predEndDate.Equal(now)) {
						// ES predictions were wrong - invalidate them
						return nil
					}
				}
			}
			// elif db_pred_data_exist is not None (use DB prediction - highest priority)
			if dbPredictedDates != nil {
				if dbEndDate, ok := dbPredictedDates["end_date"].(string); ok && dbEndDate != "" {
					return &dbEndDate
				}
			}
			// elif db_pred_data_exist is None AND ES predictions are valid future predictions
			if dbPredictedDates == nil && esPredStartDate != nil && esPredEndDate != nil && startDateStr != "" && endDateStr != "" {
				startDate, err1 := parseDateLenient(startDateStr)
				endDate, err2 := parseDateLenient(endDateStr)
				predStartDate, err3 := parseDateLenient(*esPredStartDate)
				predEndDate, err4 := parseDateLenient(*esPredEndDate)
				if err1 == nil && err2 == nil && err3 == nil && err4 == nil {
					now := time.Now()
					if startDate.Before(predStartDate) && endDate.Before(predEndDate) && (predEndDate.After(now) || predEndDate.Equal(now)) {
						// Keep ES prediction: pred_end_date = pred_end_date
						return esPredEndDate
					}
				}
			}
			// else (set to None)
			return nil
		}(),
		"predictionScore": func() *int32 {
			// Decision logic for prediction score
			startDateStr := decodeBase64Date(edition["edition_start_date"])
			endDateStr := decodeBase64Date(edition["edition_end_date"])
			// Parse ES predictions
			esPredStartDate := decodeBase64NullableDate(esInfoMap["pred_startDate"])
			esPredEndDate := decodeBase64NullableDate(esInfoMap["pred_endDate"])
			esPredScore := esInfoMap["pred_score"]

			// if (db_pred_data_exist is None AND ES predictions are invalidated)
			if dbPredictedDates == nil && esPredStartDate != nil && esPredEndDate != nil && startDateStr != "" && endDateStr != "" {
				startDate, err1 := parseDateLenient(startDateStr)
				endDate, err2 := parseDateLenient(endDateStr)
				predStartDate, err3 := parseDateLenient(*esPredStartDate)
				predEndDate, err4 := parseDateLenient(*esPredEndDate)
				if err1 == nil && err2 == nil && err3 == nil && err4 == nil {
					now := time.Now()
					if startDate.After(predStartDate) && endDate.After(predEndDate) && (predEndDate.Before(now) || predEndDate.Equal(now)) {
						// ES predictions were wrong - explicitly set to 0
						score := int32(0)
						return &score
					}
				}
			}
			// elif db_pred_data_exist is not None (use DB prediction, score = 100 - highest priority)
			if dbPredictedDates != nil {
				if dbScore, ok := dbPredictedDates["score"].(int); ok {
					score := int32(dbScore)
					return &score
				}
			}
			// elif db_pred_data_exist is None AND ES predictions are valid future predictions
			if dbPredictedDates == nil && esPredStartDate != nil && esPredEndDate != nil && startDateStr != "" && endDateStr != "" {
				startDate, err1 := parseDateLenient(startDateStr)
				endDate, err2 := parseDateLenient(endDateStr)
				predStartDate, err3 := parseDateLenient(*esPredStartDate)
				predEndDate, err4 := parseDateLenient(*esPredEndDate)
				if err1 == nil && err2 == nil && err3 == nil && err4 == nil {
					now := time.Now()
					if startDate.Before(predStartDate) && endDate.Before(predEndDate) && (predEndDate.After(now) || predEndDate.Equal(now)) {
						// Keep ES prediction: pred_score = pred_score
						if esPredScore != nil {
							if scoreFloat, ok := esPredScore.(float64); ok {
								score := int32(scoreFloat)
								return &score
							} else if scoreInt, ok := esPredScore.(int); ok {
								score := int32(scoreInt)
								return &score
							} else if scoreInt64, ok := esPredScore.(int64); ok {
								score := int32(scoreInt64)
								return &score
							} else if scoreStr, ok := esPredScore.(string); ok && scoreStr != "" {
								if scoreVal, err := strconv.ParseInt(scoreStr, 10, 32); err == nil {
									score := int32(scoreVal)
									return &score
								}
							}
						}
					}
				}
			}
			// else (set to 0)
			score := int32(0)
			return &score
		}(),
		"PrimaryEventType": func() *string {
			eventTypes := eventTypesMap[eventID]
			eventAudience := shared.SafeConvertToUInt16(eventData["event_audience"])
			result := getPrimaryEventType(eventTypes, eventAudience)
			return result
		}(),
		"verifiedOn": func() *string {
			verified := eventData["verified"]
			if verified != nil {
				verifiedStr := decodeBase64StringIfNeeded(verified)
				if len(verifiedStr) >= 10 {
					datePart := verifiedStr[:10]
					return &datePart
				}
			}
			return nil
		}(),
		"estimatedVisitorsMean": func() *uint32 {
			if esInfoMap == nil {
				zero := uint32(0)
				return &zero
			}

			convertToUInt32 := func(val interface{}) (*uint32, bool) {
				if val == nil {
					return nil, false
				}

				switch v := val.(type) {
				case uint32:
					return &v, true
				case uint64:
					if v <= math.MaxUint32 {
						result := uint32(v)
						return &result, true
					}
				case int64:
					if v >= 0 && v <= math.MaxUint32 {
						result := uint32(v)
						return &result, true
					}
				case int:
					if v >= 0 && v <= math.MaxUint32 {
						result := uint32(v)
						return &result, true
					}
				case int32:
					if v >= 0 {
						result := uint32(v)
						return &result, true
					}
				case float64:
					if v >= 0 && v <= math.MaxUint32 && v == math.Trunc(v) {
						result := uint32(v)
						return &result, true
					}
				case float32:
					if v >= 0 && v <= math.MaxUint32 && float64(v) == math.Trunc(float64(v)) {
						result := uint32(v)
						return &result, true
					}
				case string:
					v = strings.TrimSpace(v)
					if v != "" {
						if uintVal, err := strconv.ParseUint(v, 10, 32); err == nil {
							result := uint32(uintVal)
							return &result, true
						}
						if floatVal, err := strconv.ParseFloat(v, 64); err == nil {
							if floatVal >= 0 && floatVal <= math.MaxUint32 && floatVal == math.Trunc(floatVal) {
								result := uint32(floatVal)
								return &result, true
							}
						}
					}
				case []byte:
					if len(v) > 0 {
						strVal := strings.TrimSpace(string(v))
						if strVal != "" {
							if uintVal, err := strconv.ParseUint(strVal, 10, 32); err == nil {
								result := uint32(uintVal)
								return &result, true
							}
							if floatVal, err := strconv.ParseFloat(strVal, 64); err == nil {
								if floatVal >= 0 && floatVal <= math.MaxUint32 && floatVal == math.Trunc(floatVal) {
									result := uint32(floatVal)
									return &result, true
								}
							}
						}
					}
				}
				return nil, false
			}

			// Try finalEstimate first
			if finalEstimate := esInfoMap["finalEstimate"]; finalEstimate != nil {
				if result, ok := convertToUInt32(finalEstimate); ok {
					return result
				}
			}

			// Fallback to calculating mean from highEstimate and lowEstimate
			highEstimate := esInfoMap["highEstimate"]
			lowEstimate := esInfoMap["lowEstimate"]

			var highVal, lowVal float64
			highValid := false
			lowValid := false

			convertToFloat64 := func(val interface{}) (float64, bool) {
				if val == nil {
					return 0, false
				}

				switch v := val.(type) {
				case float64:
					return v, true
				case float32:
					return float64(v), true
				case int64:
					return float64(v), true
				case int:
					return float64(v), true
				case int32:
					return float64(v), true
				case uint32:
					return float64(v), true
				case uint64:
					return float64(v), true
				case string:
					v = strings.TrimSpace(v)
					if v != "" {
						if floatVal, err := strconv.ParseFloat(v, 64); err == nil {
							return floatVal, true
						}
						if uintVal, err := strconv.ParseUint(v, 10, 64); err == nil {
							return float64(uintVal), true
						}
					}
				case []byte:
					// Handle byte arrays (sometimes JSON unmarshaling returns []byte for strings)
					if len(v) > 0 {
						strVal := strings.TrimSpace(string(v))
						if strVal != "" {
							if floatVal, err := strconv.ParseFloat(strVal, 64); err == nil {
								return floatVal, true
							}
							if uintVal, err := strconv.ParseUint(strVal, 10, 64); err == nil {
								return float64(uintVal), true
							}
						}
					}
				}
				return 0, false
			}

			if highVal, highValid = convertToFloat64(highEstimate); !highValid {
				highValid = false
			}

			if lowVal, lowValid = convertToFloat64(lowEstimate); !lowValid {
				lowValid = false
			}

			if highValid && lowValid {
				mean := uint32((highVal + lowVal) / 2)
				return &mean
			}

			// Default to 0 if no valid estimate found
			zero := uint32(0)
			return &zero
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

	if fieldHybrid != nil {
		if h, ok := fieldHybrid.(uint8); ok && h == 1 {
			eventFormat = "HYBRID"
		} else if h, ok := fieldHybrid.(*uint8); ok && h != nil && *h == 1 {
			eventFormat = "HYBRID"
		}
	}

	if eventFormat == "" {
		cityStr := shared.ConvertToString(fieldCity)
		if cityStr == "1" {
			eventFormat = "ONLINE"
		} else {
			eventFormat = "OFFLINE"
		}
	}

	// Validate event_format - only allow OFFLINE, ONLINE, or HYBRID
	validFormats := map[string]bool{
		"OFFLINE": true,
		"ONLINE":  true,
		"HYBRID":  true,
	}
	if !validFormats[eventFormat] {
		record["event_format"] = nil
	} else {
		record["event_format"] = &eventFormat
	}

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

		if dayWiseJSON, ok := economicData["dayWiseJSON"].(string); ok && dayWiseJSON != "" {
			record["event_economic_dayWiseEconomicImpact"] = dayWiseJSON
		} else if rawJSON, ok := economicData["rawJSON"].(string); ok && rawJSON != "" {
			record["event_economic_dayWiseEconomicImpact"] = rawJSON
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

// processFailedBatchInsert handles the insert logic for records that are already built
// Returns (success bool, failedRecords []map[string]interface{})
func processFailedBatchInsert(
	clickhouseConn driver.Conn,
	records []map[string]interface{},
	config shared.Config,
	logToFile func(string, ...interface{}),
) (bool, []map[string]interface{}) {
	if len(records) == 0 {
		return true, nil
	}

	logToFile("        Inserting %d records...", len(records))

	// Custom retry logic with 10-second timeout on failure
	maxRetries := 3
	attemptCount := 0
	var insertErr error
	for attemptCount < maxRetries {
		if attemptCount > 0 {
			now := time.Now().Format("2006-01-02 15:04:05")
			for j := range records {
				records[j]["last_updated_at"] = now
			}
			logToFile("          ⏳ Waiting 10 seconds before retry %d/%d...", attemptCount+1, maxRetries)
			time.Sleep(10 * time.Second)
			logToFile("          ✓ Wait complete, retrying insert (attempt %d)", attemptCount+1)
		}
		attemptCount++

		// Progressive batch splitting: attempt 2 uses 1000, attempt 3 uses 500
		if attemptCount == 2 && len(records) > 1000 {
			logToFile("          Second retry attempt: Splitting %d records into batches of 1000...", len(records))
			retryBatchSize := 1000
			successfulBatches := 0
			failedBatches := 0
			totalSmallBatches := (len(records) + retryBatchSize - 1) / retryBatchSize

			for batchStart := 0; batchStart < len(records); batchStart += retryBatchSize {
				batchEnd := batchStart + retryBatchSize
				if batchEnd > len(records) {
					batchEnd = len(records)
				}
				smallBatch := records[batchStart:batchEnd]
				smallBatchNum := (batchStart / retryBatchSize) + 1
				logToFile("          Inserting small batch %d/%d (%d records) on retry attempt 2...", smallBatchNum, totalSmallBatches, len(smallBatch))
				smallBatchErr := insertalleventDataChunk(clickhouseConn, smallBatch, config)
				if smallBatchErr != nil {
					failedBatches++
					insertErr = smallBatchErr
					logToFile("          ✗ Small batch %d/%d failed: %v", smallBatchNum, totalSmallBatches, smallBatchErr)
				} else {
					successfulBatches++
					logToFile("          ✓ Small batch %d/%d inserted successfully", smallBatchNum, totalSmallBatches)
				}
			}

			if successfulBatches == totalSmallBatches {
				insertErr = nil
				logToFile("          ✓ All %d small batches inserted successfully on retry attempt 2", totalSmallBatches)
			} else if successfulBatches > 0 {
				logToFile("          ⚠ Partial success: %d/%d small batches succeeded, %d failed", successfulBatches, totalSmallBatches, failedBatches)
			} else {
				logToFile("          ✗ All %d small batches failed on retry attempt 2", totalSmallBatches)
			}
		} else if attemptCount == maxRetries && len(records) > 500 {
			logToFile("          Last retry attempt: Splitting %d records into batches of 500...", len(records))
			lastRetryBatchSize := 500
			successfulBatches := 0
			failedBatches := 0
			totalSmallBatches := (len(records) + lastRetryBatchSize - 1) / lastRetryBatchSize

			for batchStart := 0; batchStart < len(records); batchStart += lastRetryBatchSize {
				batchEnd := batchStart + lastRetryBatchSize
				if batchEnd > len(records) {
					batchEnd = len(records)
				}
				smallBatch := records[batchStart:batchEnd]
				smallBatchNum := (batchStart / lastRetryBatchSize) + 1
				logToFile("          Inserting small batch %d/%d (%d records) on final retry attempt...", smallBatchNum, totalSmallBatches, len(smallBatch))
				smallBatchErr := insertalleventDataChunk(clickhouseConn, smallBatch, config)
				if smallBatchErr != nil {
					failedBatches++
					insertErr = smallBatchErr
					logToFile("          ✗ Small batch %d/%d failed: %v", smallBatchNum, totalSmallBatches, smallBatchErr)
				} else {
					successfulBatches++
					logToFile("          ✓ Small batch %d/%d inserted successfully", smallBatchNum, totalSmallBatches)
				}
			}

			if successfulBatches == totalSmallBatches {
				insertErr = nil
				logToFile("          ✓ All %d small batches inserted successfully on final retry", totalSmallBatches)
			} else if successfulBatches > 0 {
				logToFile("          ⚠ Partial success: %d/%d small batches succeeded, %d failed", successfulBatches, totalSmallBatches, failedBatches)
			} else {
				logToFile("          ✗ All %d small batches failed on final retry attempt", totalSmallBatches)
			}
		} else {
			insertErr = insertalleventDataChunk(clickhouseConn, records, config)
		}

		if insertErr == nil {
			break
		}
		if attemptCount < maxRetries {
			logToFile("          ✗ Insert attempt %d/%d failed: %v", attemptCount, maxRetries, insertErr)
		}
	}

	if insertErr != nil {
		logToFile("        ✗ ERROR: Failed to insert after retries: %v", insertErr)
		return false, records
	}

	logToFile("        ✓ Successfully inserted %d records", len(records))

	// Verify inserted records
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
		existingAfterInsert, err := checkExistenceInClickHouseWithRetry(clickhouseConn, verifyPairs, config, true, logToFile)
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
				return false, records
			}
		}
	}

	return true, nil
}

func retryFailedBatchesAfterCompletion(
	clickhouseConn driver.Conn,
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
			defer func() {
				if logWriter != nil {
					logWriter.Sync()
					logWriter.Close()
				}
			}()
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

	// Helper function to ensure log file is flushed and synced before returning
	ensureLogFlushed := func() {
		if logWriter != nil {
			logWriter.Sync() // Ensure data is written to disk
		}
	}

	dir := "failed_batches"

	maxAttempts := 3

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		jsonFiles, err := filepath.Glob(filepath.Join(dir, "failed_batches_chunk_*_batch_*.json"))
		if err != nil {
			logToFile("ERROR: Failed to glob JSON files: %v", err)
			return fmt.Errorf("failed to glob failed batch files: %w", err)
		}

		chunkFiles := make(map[int][]string)

		for _, jsonFile := range jsonFiles {
			chunkNum := extractChunkNumFromJSONFilename(jsonFile)
			if chunkNum >= 0 {
				chunkFiles[chunkNum] = append(chunkFiles[chunkNum], jsonFile)
			}
		}

		if len(chunkFiles) == 0 {
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
		logToFile("Found %d chunk(s) with failed batches to process", len(chunkFiles))
		if logFile != "" {
			logToFile("Log file: %s", logFile)
		}
		logToFile("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

		totalSkipped := 0
		totalRetried := 0
		totalFailed := 0

		for chunkNum, jsonFiles := range chunkFiles {
			// Sort JSON files by batch number to ensure correct processing order
			sort.Slice(jsonFiles, func(i, j int) bool {
				batchNumI := extractBatchNumFromJSONFilename(jsonFiles[i])
				batchNumJ := extractBatchNumFromJSONFilename(jsonFiles[j])
				return batchNumI < batchNumJ
			})

			logToFile("")
			logToFile("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
			logToFile("Processing chunk %d: %d JSON file(s)", chunkNum, len(jsonFiles))
			logToFile("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

			allFailedRecords := make([]map[string]interface{}, 0)

			for _, jsonFile := range jsonFiles {
				batchNum := extractBatchNumFromJSONFilename(jsonFile)
				logToFile("")
				logToFile("  → Processing JSON file: %s (batch %d)", filepath.Base(jsonFile), batchNum)

				records, err := readFailedBatchFromJSON(jsonFile)
				if err != nil {
					logToFile("    ERROR: Failed to read JSON file: %v", err)
					totalFailed += 1
					continue
				}

				if len(records) == 0 {
					logToFile("    File is empty, removing it...")
					if err := os.Remove(jsonFile); err != nil {
						logToFile("    WARNING: Failed to remove empty JSON file: %v", err)
					} else {
						logToFile("    ✓ Removed empty JSON file")
					}
					continue
				}

				logToFile("    Loaded %d records from JSON", len(records))

				// Sort records by end_date to ensure correct insertion order (safety measure)
				sort.Slice(records, func(i, j int) bool {
					recordI := records[i]
					recordJ := records[j]

					endDateIStr := shared.SafeConvertToString(recordI["end_date"])
					endDateJStr := shared.SafeConvertToString(recordJ["end_date"])

					if endDateIStr == "" || endDateJStr == "" {
						return false
					}

					dateI, errI := time.Parse("2006-01-02", endDateIStr)
					dateJ, errJ := time.Parse("2006-01-02", endDateJStr)

					if errI != nil || errJ != nil {
						return false
					}
					return dateI.Before(dateJ)
				})

				checkPairs := make([]EventEditionPair, 0, len(records))
				for _, record := range records {
					eventID := shared.ConvertToUInt32(record["event_id"])
					editionID := shared.ConvertToUInt32(record["edition_id"])
					checkPairs = append(checkPairs, EventEditionPair{
						EventID:   eventID,
						EditionID: editionID,
					})
				}

				logToFile("    Checking existence in allevent_temp for %d records...", len(records))
				existingPairs, err := checkExistenceInClickHouseWithRetry(clickhouseConn, checkPairs, config, true, logToFile)
				if err != nil {
					logToFile("    ERROR: Failed to check existence: %v", err)
					totalFailed += len(records)
					continue
				}

				missingRecords := make([]map[string]interface{}, 0)
				for _, record := range records {
					eventID := shared.ConvertToUInt32(record["event_id"])
					editionID := shared.ConvertToUInt32(record["edition_id"])
					key := uint64(eventID)<<32 | uint64(editionID)
					if !existingPairs[key] {
						missingRecords = append(missingRecords, record)
					}
				}

				if len(existingPairs) == len(records) {
					logToFile("    ✓ All %d records already exist, skipping insert", len(records))
					totalSkipped += len(records)
					if err := os.Remove(jsonFile); err != nil {
						logToFile("    WARNING: Failed to remove processed JSON file: %v", err)
					} else {
						logToFile("    ✓ Removed processed JSON file")
					}
					continue
				} else if len(existingPairs) > 0 {
					logToFile("    Found %d/%d records exist, %d records missing", len(existingPairs), len(records), len(missingRecords))
					totalSkipped += len(existingPairs)
				}

				if len(missingRecords) == 0 {
					continue
				}

				insertSuccess, failedRecords := processFailedBatchInsert(clickhouseConn, missingRecords, config, logToFile)
				if insertSuccess {
					totalRetried += len(missingRecords)
					if err := os.Remove(jsonFile); err != nil {
						logToFile("    WARNING: Failed to remove processed JSON file: %v", err)
					} else {
						logToFile("    ✓ Removed processed JSON file")
					}
				} else {
					totalFailed += len(failedRecords)
					allFailedRecords = append(allFailedRecords, failedRecords...)
				}
			}

			if len(allFailedRecords) > 0 {
				logToFile("")
				logToFile("  Writing %d failed records back to JSON...", len(allFailedRecords))
				retryBatchNum := 1
				if err := writeFailedBatchToJSON(allFailedRecords, retryBatchNum, chunkNum); err != nil {
					logToFile("  WARNING: Failed to write failed records to JSON: %v", err)
				} else {
					logToFile("  ✓ Successfully wrote %d failed records to JSON", len(allFailedRecords))
				}
			}
		}

		logToFile("")
		logToFile("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		logToFile("=== Retry Attempt %d/%d Summary ===", attempt, maxAttempts)
		logToFile("Total pairs skipped (already exist): %d", totalSkipped)
		logToFile("Total records retried and inserted: %d", totalRetried)
		logToFile("Total records failed: %d", totalFailed)
		logToFile("=== Attempt %d completed at %s ===", attempt, time.Now().Format("2006-01-02 15:04:05"))

		remainingJSONFiles, err := filepath.Glob(filepath.Join(dir, "failed_batches_chunk_*_batch_*.json"))
		if err != nil {
			logToFile("WARNING: Failed to check for remaining failed batch files: %v", err)
		}
		remainingFiles := len(remainingJSONFiles)
		if remainingFiles == 0 {
			logToFile("")
			logToFile("✓ All failed batches successfully processed - no remaining files")
			if logFile != "" {
				logToFile("Full log saved to: %s", logFile)
			}
			ensureLogFlushed()
			return nil
		} else {
			logToFile("")
			logToFile("⚠️  %d failed batch file(s) still remain after attempt %d:", remainingFiles, attempt)
			for _, file := range remainingJSONFiles {
				logToFile("   - %s", file)
			}

			if attempt < maxAttempts {
				logToFile("")
				logToFile("⏳ Waiting 10 seconds before retry attempt %d/%d...", attempt+1, maxAttempts)
				time.Sleep(10 * time.Second)
				logToFile("✓ Wait complete, starting next retry attempt")
			} else {
				logToFile("")
				logToFile("⚠️  Maximum retry attempts (%d) reached", maxAttempts)
				logToFile("   → These files will be processed on the next script run")
				logToFile("   → Optimization will be skipped until all batches are successfully inserted")
				if logFile != "" {
					logToFile("Full log saved to: %s", logFile)
				}
				ensureLogFlushed()
				return fmt.Errorf("failed batches still remain after %d attempts: %d file(s) need to be processed", maxAttempts, remainingFiles)
			}
		}
	}

	ensureLogFlushed()
	return nil
}
