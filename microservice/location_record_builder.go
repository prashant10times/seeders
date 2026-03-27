package microservice

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"seeders/shared"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

func buildCountryLocationRecord(row map[string]interface{}, currentID uint32, now time.Time, countryCoordsLookup map[string]CountryCoordinatesMap) (LocationChRecord, bool) {
	countryID := shared.SafeConvertToString(row["id_10x"])
	created := shared.SafeConvertToString(row["created"])
	if countryID == "" || created == "" {
		return LocationChRecord{}, false
	}

	normalizedID := normalizeNFC(strings.TrimSpace(countryID))
	isoUpper := strings.ToUpper(normalizedID)
	id10x := fmt.Sprintf("country-%s", isoUpper)
	idUUID := shared.GenerateUUIDFromString(fmt.Sprintf("%s-%s", strings.ToUpper(countryID), created))

	name := shared.SafeConvertToNullableString(row["name"])
	alias := shared.SafeConvertToNullableString(row["alias"])
	phonecode := shared.SafeConvertToNullableString(row["phonecode"])
	currency := shared.SafeConvertToNullableString(row["currency"])
	continent := shared.SafeConvertToNullableString(row["continent"])
	published := shared.SafeConvertToInt8(row["published"])
	slug := shared.SafeConvertToNullableString(row["slug"])

	var regions []string
	if r := shared.SafeConvertToString(row["regions"]); r != "" {
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

	var latPtr, lonPtr *float64
	if coords, ok := countryCoordsLookup[isoUpper]; ok {
		latPtr = coords.Latitude
		lonPtr = coords.Longitude
	}

	isoPtr := (*string)(nil)
	if isoUpper != "" {
		isoPtr = &isoUpper
	}

	return LocationChRecord{
		IDUUID:        idUUID,
		ID:            currentID,
		ID10x:         id10x,
		LocationType:  "COUNTRY",
		Name:          name,
		Alias:         alias,
		Slug:          slug,
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
	}, true
}

func buildStateLocationRecord(row map[string]interface{}, currentID uint32, now time.Time, countryUUIDLookup, countryNameLookup map[string]string, seenStates map[string]bool) (LocationChRecord, bool) {
	stateName := shared.SafeConvertToString(row["id_10x"])
	countryISO := shared.SafeConvertToString(row["countryId"])
	if stateName == "" || countryISO == "" {
		return LocationChRecord{}, false
	}

	countryISOUpper := strings.ToUpper(strings.TrimSpace(countryISO))
	stateID := shared.SafeConvertToUInt32(row["id"])
	id10x := fmt.Sprintf("state-%d-%s", stateID, countryISOUpper)
	if seenStates != nil {
		if seenStates[id10x] {
			return LocationChRecord{}, false
		}
		seenStates[id10x] = true
	}

	idUUID := shared.GenerateUUIDFromString(normalizeNFC(id10x))
	name := shared.SafeConvertToNullableString(row["name"])
	alias := shared.SafeConvertToNullableString(row["alias"])
	slug := shared.SafeConvertToNullableString(row["slug"])
	geometry := shared.SafeConvertToNullableString(row["geometry"])
	published := shared.SafeConvertToInt8(row["published"])

	var latPtr, lonPtr *float64
	if geometry != nil && *geometry != "" {
		latPtr, lonPtr = parseGeometryLatLng(*geometry)
	}

	var countryUUID *string
	if uuid, ok := countryUUIDLookup[countryISOUpper]; ok {
		countryUUID = &uuid
	}

	var countryNameValue *string
	if cname, ok := countryNameLookup[countryISOUpper]; ok {
		caser := cases.Title(language.English)
		nameTitle := caser.String(strings.ToLower(cname))
		countryNameValue = &nameTitle
	}

	var isoPtr *string
	if countryISOUpper != "NAN" {
		isoPtr = &countryISOUpper
	}

	return LocationChRecord{
		IDUUID:        idUUID,
		ID:            currentID,
		ID10x:         id10x,
		LocationType:  "STATE",
		Name:          name,
		Alias:         alias,
		Slug:          slug,
		Geometry:      geometry,
		Latitude:      latPtr,
		Longitude:     lonPtr,
		Published:     published,
		ISO:           isoPtr,
		CountryUUID:   countryUUID,
		CountryName:   countryNameValue,
		LastUpdatedAt: now,
		Version:       1,
	}, true
}

func buildCityLocationRecord(row map[string]interface{}, currentID uint32, now time.Time, countryUUIDLookup, countryNameLookup map[string]string) (LocationChRecord, bool) {
	cityID := shared.SafeConvertToString(row["id_10x"])
	created := shared.SafeConvertToString(row["created"])
	if cityID == "" || created == "" {
		return LocationChRecord{}, false
	}

	normalizedID := normalizeNFC(cityID)
	idUUID := shared.GenerateUUIDFromString(fmt.Sprintf("%s-%s", normalizedID, created))
	id10x := fmt.Sprintf("city-%s", normalizedID)

	name := shared.SafeConvertToNullableString(row["name"])
	alias := shared.SafeConvertToNullableString(row["alias"])
	slug := shared.SafeConvertToNullableString(row["slug"])
	stateName := shared.SafeConvertToNullableString(row["state_name"])
	countrySourceID := shared.SafeConvertToString(row["countryId"])
	latitude := shared.SafeConvertToNullableFloat64(row["latitude"])
	longitude := shared.SafeConvertToNullableFloat64(row["longitude"])
	utcOffset := shared.SafeConvertToNullableString(row["utc_offset"])
	timezone := shared.SafeConvertToNullableString(row["timezone"])
	published := shared.SafeConvertToInt8(row["published"])

	var stateUUID *string
	if stateName != nil && *stateName != "" && countrySourceID != "" {
		stateID := shared.SafeConvertToUInt32(row["state_id"])
		if stateID > 0 {
			countryISOUpper := strings.ToUpper(strings.TrimSpace(countrySourceID))
			uuid := shared.GenerateUUIDFromString(normalizeNFC(fmt.Sprintf("state-%d-%s", stateID, countryISOUpper)))
			stateUUID = &uuid
		}
	}

	var countryUUID *string
	var countryNameValue *string
	if countrySourceID != "" {
		countryISOUpper := strings.ToUpper(strings.TrimSpace(countrySourceID))
		if uuid, ok := countryUUIDLookup[countryISOUpper]; ok {
			countryUUID = &uuid
		}
		if cname, ok := countryNameLookup[countryISOUpper]; ok {
			caser := cases.Title(language.English)
			nameTitle := caser.String(strings.ToLower(cname))
			countryNameValue = &nameTitle
		}
	}

	var isoPtr *string
	if countrySourceID != "" {
		countryISOUpper := strings.ToUpper(strings.TrimSpace(countrySourceID))
		if countryISOUpper != "NAN" {
			isoPtr = &countryISOUpper
		}
	}

	return LocationChRecord{
		IDUUID:        idUUID,
		ID:            currentID,
		ID10x:         id10x,
		LocationType:  "CITY",
		Name:          name,
		Alias:         alias,
		Slug:          slug,
		Latitude:      latitude,
		Longitude:     longitude,
		UTCOffset:     utcOffset,
		Timezone:      timezone,
		Published:     published,
		ISO:           isoPtr,
		StateUUID:     stateUUID,
		StateName:     stateName,
		CountryUUID:   countryUUID,
		CountryName:   countryNameValue,
		LastUpdatedAt: now,
		Version:       1,
	}, true
}

func buildVenueLocationRecord(row map[string]interface{}, currentID uint32, now time.Time, countryUUIDLookup, countryNameLookup, cityUUIDLookup, stateUUIDLookup map[string]string) (LocationChRecord, bool) {
	venueID := shared.SafeConvertToString(row["id_10x"])
	created := shared.SafeConvertToString(row["created"])
	if venueID == "" || created == "" {
		return LocationChRecord{}, false
	}

	normalizedID := normalizeNFC(venueID)
	idUUID := shared.GenerateUUIDFromString(fmt.Sprintf("%s-%s", normalizedID, created))
	id10x := fmt.Sprintf("venue-%s", normalizedID)

	name := shared.SafeConvertToNullableString(row["name"])
	address := shared.SafeConvertToNullableString(row["address"])
	slug := shared.SafeConvertToNullableString(row["slug"])
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

	if address != nil && *address != "" {
		cleanAddr := strings.ReplaceAll(*address, "\n", " ")
		address = &cleanAddr
	}

	var cityUUID *string
	var cityID *uint32
	if cityIDStr != "" {
		if cityIDVal, err := strconv.ParseUint(cityIDStr, 10, 32); err == nil {
			cityIDUint32 := uint32(cityIDVal)
			cityID = &cityIDUint32
		}
		if uuid, ok := cityUUIDLookup[cityIDStr]; ok {
			cityUUID = &uuid
		}
	}

	var stateUUID *string
	if stateName != nil && *stateName != "" && countrySourceID != "" {
		stateID := shared.SafeConvertToUInt32(row["state_id"])
		if stateID > 0 {
			countryISOUpper := strings.ToUpper(strings.TrimSpace(countrySourceID))
			lookupKey := fmt.Sprintf("%d-%s", stateID, countryISOUpper)
			if uuid, ok := stateUUIDLookup[lookupKey]; ok {
				stateUUID = &uuid
			}
		}
	}

	var countryUUID *string
	var countryNameValue *string
	if countrySourceID != "" {
		countryISOUpper := strings.ToUpper(strings.TrimSpace(countrySourceID))
		if uuid, ok := countryUUIDLookup[countryISOUpper]; ok {
			countryUUID = &uuid
		}
		if cname, ok := countryNameLookup[countryISOUpper]; ok {
			caser := cases.Title(language.English)
			nameTitle := caser.String(strings.ToLower(cname))
			countryNameValue = &nameTitle
		}
	}

	var isoPtr *string
	if countrySourceID != "" {
		countryISOUpper := strings.ToUpper(strings.TrimSpace(countrySourceID))
		if countryISOUpper != "NAN" {
			isoPtr = &countryISOUpper
		}
	}

	return LocationChRecord{
		IDUUID:        idUUID,
		ID:            currentID,
		ID10x:         id10x,
		LocationType:  "VENUE",
		Name:          name,
		Address:       address,
		Slug:          slug,
		Latitude:      latitude,
		Longitude:     longitude,
		Website:       website,
		Postalcode:    postalcode,
		Published:     published,
		ISO:           isoPtr,
		StateUUID:     stateUUID,
		StateName:     stateName,
		CountryUUID:   countryUUID,
		CountryName:   countryNameValue,
		CityID:        cityID,
		CityUUID:      cityUUID,
		CityName:      cityName,
		CityLatitude:  cityLat,
		CityLongitude: cityLong,
		LastUpdatedAt: now,
		Version:       1,
	}, true
}

func buildSubVenueLocationRecord(row map[string]interface{}, currentID uint32, now time.Time, venueUUIDLookup map[string]string) (LocationChRecord, bool) {
	subVenueID := shared.SafeConvertToString(row["id_10x"])
	created := shared.SafeConvertToString(row["created"])
	if subVenueID == "" || created == "" {
		return LocationChRecord{}, false
	}

	normalizedID := normalizeNFC(subVenueID)
	idUUID := shared.GenerateUUIDFromString(fmt.Sprintf("%s-%s", normalizedID, created))
	id10x := fmt.Sprintf("sub_venue-%s", normalizedID)

	name := shared.SafeConvertToNullableString(row["name"])
	area := shared.SafeConvertToNullableFloat64(row["area"])
	published := shared.SafeConvertToInt8(row["published"])
	venueIDStr := shared.SafeConvertToString(row["venueId"])

	if name != nil {
		cleanedName := normalizeNFC(*name)
		cleanedName = strings.ReplaceAll(cleanedName, "\\", "")
		cleanedName = strings.ReplaceAll(cleanedName, "'", "''")
		name = &cleanedName
	}

	var venueUUID *string
	if venueIDStr != "" {
		if uuid, found := venueUUIDLookup[venueIDStr]; found {
			venueUUID = &uuid
		}
	}

	return LocationChRecord{
		IDUUID:        idUUID,
		ID:            currentID,
		ID10x:         id10x,
		LocationType:  "SUB_VENUE",
		Name:          name,
		Area:          area,
		Published:     published,
		VenueUUID:     venueUUID,
		LastUpdatedAt: now,
		Version:       1,
	}, true
}
