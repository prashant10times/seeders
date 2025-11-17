package microservice

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"seeders/shared"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type AlertChRecord struct {
	ID               string  `ch:"id"`
	SourceID         string  `ch:"sourceId"`
	CurrentEpisodeID string  `ch:"currentEpisodeId"`
	Type             string  `ch:"type"`
	Name             string  `ch:"name"`
	Description      *string `ch:"description"`
	Level            string  `ch:"level"`
	StartDate        string  `ch:"startDate"`
	EndDate          string  `ch:"endDate"`
	LastModified     string  `ch:"lastModified"`
	Created          string  `ch:"created"`
	OriginLongitude  float64 `ch:"originLongitude"`
	OriginLatitude   float64 `ch:"originLatitude"`
}

type AlertMetadata struct {
	AlertID         string
	GeometryLink    string
	AlertType       string
	AlertLevel      string
	StartDate       string
	EndDate         string
	OriginLongitude float64
	OriginLatitude  float64
	AlertCountry    string
	GDACSearchURL   string
}

type LocationPolygonRecord struct {
	TableName        string    `ch:"tableName"`
	TableID          string    `ch:"tableId"`
	Polygon          string    `ch:"polygon"`
	PolygonSourceURL *string   `ch:"polygonSourceUrl"`
	CreatedAt        time.Time `ch:"createdAt"`
	UpdatedAt        time.Time `ch:"updatedAt"`
	Version          uint32    `ch:"version"`
}

type EventTypeChRecord struct {
	EventTypeID    uint32   `ch:"eventtype_id"`
	EventTypeUUID  string   `ch:"eventtype_uuid"`
	EventID        uint32   `ch:"event_id"`
	Published      int8     `ch:"published"`
	Name           string   `ch:"name"`
	Slug           string   `ch:"slug"`
	EventAudience  *uint16  `ch:"event_audience"`
	EventGroupType *string  `ch:"eventGroupType"`
	Groups         []string `ch:"groups"`
	Priority       *int8    `ch:"priority"`
	Created        string   `ch:"created"`
	Version        uint32   `ch:"version"`
	AlertID        *string  `ch:"alert_id"`
	AlertLevel     *string  `ch:"alert_level"`
	AlertType      *string  `ch:"alert_type"`
	AlertStartDate *string  `ch:"alert_start_date"`
	AlertEndDate   *string  `ch:"alert_end_date"`
}

type PolygonErrorLogger struct {
	mu      sync.Mutex
	errors  []PolygonError
	logFile *os.File
	logger  *log.Logger
}

type PolygonError struct {
	AlertID          string
	GDACSearchURL    string
	PolygonSourceURL string
}

var polygonErrorLogger *PolygonErrorLogger

func InitPolygonErrorLogger() error {
	logFileName := "polygon_coordinate_cases.log"
	file, err := os.Create(logFileName)
	if err != nil {
		return fmt.Errorf("failed to create polygon error log file: %w", err)
	}

	polygonErrorLogger = &PolygonErrorLogger{
		errors:  make([]PolygonError, 0),
		logFile: file,
		logger:  log.New(file, "", log.LstdFlags),
	}

	polygonErrorLogger.logger.Println("Format: alertId -> gdacSearchURL -> polygonSourceURL")

	return nil
}

func LogPolygonError(alertID, gdacSearchURL, polygonSourceURL string) {
	if polygonErrorLogger == nil {
		return
	}

	polygonErrorLogger.mu.Lock()
	defer polygonErrorLogger.mu.Unlock()

	polygonErr := PolygonError{
		AlertID:          alertID,
		GDACSearchURL:    gdacSearchURL,
		PolygonSourceURL: polygonSourceURL,
	}

	polygonErrorLogger.errors = append(polygonErrorLogger.errors, polygonErr)
	polygonErrorLogger.logger.Printf("%s -> %s -> %s", alertID, gdacSearchURL, polygonSourceURL)
}

func FinalizePolygonErrorLogger() error {
	if polygonErrorLogger == nil {
		return nil
	}

	polygonErrorLogger.mu.Lock()
	defer polygonErrorLogger.mu.Unlock()

	totalCount := len(polygonErrorLogger.errors)
	logFileName := polygonErrorLogger.logFile.Name()

	polygonErrorLogger.logger.Println("")
	polygonErrorLogger.logger.Println("=== Summary ===")
	polygonErrorLogger.logger.Printf("Total errors: %d", totalCount)

	if err := polygonErrorLogger.logFile.Close(); err != nil {
		return fmt.Errorf("failed to close polygon error log file: %w", err)
	}

	if totalCount > 0 {
		log.Printf("Polygon error log written to %s with %d total errors", logFileName, totalCount)
	}

	return nil
}

func InsertAlertsChDataSingleWorker(clickhouseConn driver.Conn, alertRecords []AlertChRecord) error {
	if len(alertRecords) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	batch, err := clickhouseConn.PrepareBatch(ctx, `
		INSERT INTO testing_db.alerts_ch (
			id, sourceId, currentEpisodeId, type, name, description, level, startDate, endDate, lastModified, created, originLongitude, originLatitude
		)
	`)
	if err != nil {
		log.Printf("ERROR: Failed to prepare ClickHouse batch for alerts_ch: %v", err)
		return fmt.Errorf("failed to prepare ClickHouse batch: %v", err)
	}

	for _, record := range alertRecords {
		err := batch.Append(
			record.ID,               // id: UUID
			record.SourceID,         // sourceId: String
			record.CurrentEpisodeID, // currentEpisodeId: String
			record.Type,             // type: LowCardinality(String)
			record.Name,             // name: String
			record.Description,      // description: Nullable(String)
			record.Level,            // level: LowCardinality(String)
			record.StartDate,        // startDate: Date
			record.EndDate,          // endDate: Date
			record.LastModified,     // lastModified: DateTime
			record.Created,          // created: DateTime
			record.OriginLongitude,  // originLongitude: Float64
			record.OriginLatitude,   // originLatitude: Float64
		)
		if err != nil {
			log.Printf("ERROR: Failed to append record to batch: %v", err)
			log.Printf("Record data: ID=%s, SourceID=%s, Type=%s, Name=%s, Level=%s, StartDate=%s, EndDate=%s",
				record.ID, record.SourceID, record.Type, record.Name, record.Level, record.StartDate, record.EndDate)
			return fmt.Errorf("failed to append record to batch: %v", err)
		}
	}

	if err := batch.Send(); err != nil {
		log.Printf("ERROR: Failed to send ClickHouse batch: %v", err)
		return fmt.Errorf("failed to send ClickHouse batch: %v", err)
	}

	log.Printf("OK: Successfully inserted %d alerts_ch records", len(alertRecords))
	return nil
}

var alertTypeMap = map[string]string{
	"TC": "CYCLONES",
	"EQ": "EARTHQUAKES",
	"FL": "FLOODS",
	"TS": "TSUNAMIS",
	"VO": "VOLCANIC_ERUPTIONS",
	"WF": "FOREST_FIRES",
	"DR": "DROUGHTS",
}

var alertLevelMap = map[string]string{
	"Green":  "GREEN",
	"Orange": "ORANGE",
	"Red":    "RED",
}

var alertBufferAreas = map[string]map[string]struct {
	Level  string
	Radius int
}{
	"EARTHQUAKES": {
		"RED":    {"ORANGE", 100},
		"ORANGE": {"GREEN", 50},
		"GREEN":  {"GREEN", 10},
	},
	"TSUNAMIS": {
		"RED":    {"ORANGE", 100},
		"ORANGE": {"GREEN", 50},
		"GREEN":  {"GREEN", 10},
	},
	"VOLCANIC_ERUPTIONS": {
		"RED":    {"ORANGE", 100},
		"ORANGE": {"GREEN", 30},
		"GREEN":  {"GREEN", 10},
	},
}

var alertRecoveryDurations = map[string]map[string]int{
	"CYCLONES": {
		"GREEN":  2,
		"ORANGE": 4,
		"RED":    10,
	},
	"EARTHQUAKES": {
		"GREEN":  2,
		"ORANGE": 5,
		"RED":    20,
	},
	"FLOODS": {
		"GREEN":  2,
		"ORANGE": 5,
		"RED":    20,
	},
	"TSUNAMIS": {
		"GREEN":  1,
		"ORANGE": 4,
		"RED":    10,
	},
	"VOLCANIC_ERUPTIONS": {
		"GREEN":  2,
		"ORANGE": 6,
		"RED":    20,
	},
	"FOREST_FIRES": {
		"GREEN":  1,
		"ORANGE": 5,
		"RED":    10,
	},
}

var gdacCountries = []string{
	"Afghanistan", "Albania", "Algeria", "American Samoa", "Andorra", "Angola", "Anguilla", "Antarctica", "Antigua & Barbuda", "Argentina", "Armenia", "Aruba", "Australia", "Austria", "Azerbaijan", "Bahrain", "Baker I.", "Bangladesh", "Barbados", "Belarus", "Belgium", "Belize", "Benin", "Bermuda", "Bhutan", "Bolivia", "Bosnia & Herzegovina",
	"Botswana", "Bouvet I.", "Brazil", "British Indian Ocean Territory", "British Virgin Is.", "Brunei", "Bulgaria", "Burkina Faso", "Burundi", "Cambodia", "Cameroon", "Canada", "Cape Verde", "Cayman Is.", "Central African Republic", "Chad", "Chile", "China", "Christmas I.", "Cocos Is.", "Colombia", "Comoros", "Cook Is.", "Costa Rica", "Cote d'Ivoire", "Croatia", "Cuba", "Cyprus", "Czech Republic", "Democratic Republic of Congo", "Denmark", "Djibouti", "Dominica", "Dominican Republic", "Ecuador", "Egypt", "El Salvador", "Equatorial Guinea", "Eritrea", "Estonia", "Eswatini", "Ethiopia", "Falkland Is.", "Faroe Is.", "Fiji", "Finland", "France", "French Guiana", "French Polynesia", "French Southern & Antarctic Lands", "Gabon", "Gaza Strip", "Georgia", "Germany", "Ghana", "Gibraltar", "Glorioso I.", "Greece", "Greenland", "Grenada", "Guadeloupe", "Guam", "Guatemala", "Guernsey", "Guinea", "Guinea-Bissau", "Guyana", "Haiti", "Heard I. & McDonald Is.", "Honduras", "Howland I.", "Hungary", "Iceland", "India", "Indonesia", "Iraq", "Ireland", "Islamic Republic of Iran", "Isle of Man", "Israel", "Italy", "Jamaica", "Jan Mayen", "Japan", "Jarvis I.", "Jersey", "Johnston Atoll", "Jordan", "Juan De Nova I.", "Kazakhstan", "Kenya", "Kiribati", "Kosovo", "Kuwait", "Kyrgyzstan", "Laos", "Latvia", "Lebanon", "Lesotho", "Liberia", "Libya", "Liechtenstein", "Lithuania", "Luxembourg", "Madagascar", "Malawi", "Malaysia", "Maldives", "Mali", "Malta", "Marshall Is.", "Martinique", "Mauritania", "Mauritius", "Mayotte", "Mexico", "Micronesia", "Midway Is.", "Moldova", "Monaco", "Mongolia", "Montenegro", "Montserrat", "Morocco", "Mozambique", "Myanmar", "Namibia", "Nauru", "Nepal", "Netherlands", "Netherlands Antilles", "New Caledonia", "New Zealand", "Nicaragua", "Niger", "Nigeria", "Niue", "Norfolk I.", "North Korea", "Northern Mariana Is.", "Norway", "Oman", "Pakistan", "Palau", "Panama", "Papua New Guinea", "Paraguay", "Peru", "Philippines", "Pitcairn Is.", "Poland", "Portugal", "Puerto Rico", "Qatar", "Republic of Congo", "Reunion", "Romania", "Russia", "Rwanda", "Samoa", "San Marino", "Sao Tome & Principe", "Saudi Arabia", "Senegal", "Serbia", "Seychelles", "Sierra Leone", "Singapore", "Slovakia", "Slovenia", "Solomon Is.", "Somalia", "South Africa", "South Georgia & the South Sandwich Is.", "South Korea", "South Sudan", "Spain", "Sri Lanka", "St. Helena", "St. Kitts & Nevis", "St. Lucia", "St. Pierre & Miquelon", "St. Vincent & the Grenadines", "Sudan", "Suriname", "Svalbard", "Sweden", "Switzerland", "Syria", "Taiwan", "Tajikistan", "Tanzania", "Thailand", "The Bahamas", "The Gambia", "The Republic of North Macedonia", "Timor-Leste", "Togo", "Tokelau", "Tonga", "Trinidad & Tobago", "Tunisia", "Türkiye", "Turkmenistan", "Turks & Caicos Is.", "Tuvalu", "Uganda", "Ukraine", "United Arab Emirates", "United Kingdom", "United States", "Uruguay", "Uzbekistan", "Vanuatu", "Vatican City", "Venezuela", "Vietnam", "Virgin Is.", "Wake I.", "Wallis & Futuna", "West Bank", "Western Sahara", "Yemen", "Zambia", "Zimbabwe",
}

type GDACFeature struct {
	Type       string                 `json:"type"`
	Properties map[string]interface{} `json:"properties"`
	Geometry   GDACGeometry           `json:"geometry"`
}

type GDACGeometry struct {
	Type        string      `json:"type"`
	Coordinates interface{} `json:"coordinates"`
}

type GDACResponse struct {
	Type     string        `json:"type"`
	Features []GDACFeature `json:"features"`
}

type FetchAlertRequest struct {
	Type     *string
	Country  *string
	Level    *string
	FromDate *string
	ToDate   *string
}

func makeRequest(client *http.Client, url string, maxRetries int, backoffFactor int) ([]byte, int, error) {
	var lastErr error
	var lastStatusCode int
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			waitTime := time.Duration(backoffFactor) * time.Duration(1<<uint(attempt-1)) * time.Second
			log.Printf("Retrying request to %s (attempt %d/%d) after %v", url, attempt+1, maxRetries, waitTime)
			time.Sleep(waitTime)
		}

		resp, err := client.Get(url)
		if err != nil {
			lastErr = fmt.Errorf("request failed: %w", err)
			continue
		}
		defer resp.Body.Close()

		statusCode := resp.StatusCode
		if statusCode == 204 {
			return nil, statusCode, nil
		}

		if statusCode == 200 {
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				lastErr = fmt.Errorf("failed to read response body: %w", err)
				continue
			}
			return body, statusCode, nil
		}

		lastStatusCode = statusCode
		if statusCode == 403 {
			lastErr = fmt.Errorf("rate limited (403)")
			log.Printf("Rate limited (403) for %s, will retry", url)
			continue
		}

		if statusCode >= 500 {
			lastErr = fmt.Errorf("server error: %d", statusCode)
			log.Printf("Server error (%d) for %s, will retry", statusCode, url)
			continue
		}

		lastErr = fmt.Errorf("unexpected status code: %d", statusCode)
		return nil, statusCode, lastErr
	}

	return nil, lastStatusCode, fmt.Errorf("max retries exceeded: %w", lastErr)
}

func fetchAlerts(gdacBaseURL, gdacEndpoint string, req FetchAlertRequest) (string, FetchAlertRequest, error) {
	params := url.Values{}
	if req.Type != nil {
		params.Add("eventlist", *req.Type)
	}
	if req.Country != nil {
		params.Add("country", *req.Country)
	}
	if req.Level != nil {
		params.Add("alertlevel", *req.Level)
	}
	if req.FromDate != nil {
		params.Add("fromDate", *req.FromDate)
	}
	if req.ToDate != nil {
		params.Add("toDate", *req.ToDate)
	}

	url := fmt.Sprintf("%s%s?%s", gdacBaseURL, gdacEndpoint, params.Encode())
	return url, req, nil
}

func parseAlertFeature(feature GDACFeature, now time.Time, country string) (*AlertChRecord, *AlertMetadata, error) {
	props := feature.Properties

	var sourceID string
	if eventIDVal, exists := props["eventid"]; exists && eventIDVal != nil {
		switch v := eventIDVal.(type) {
		case string:
			sourceID = v
		case float64:
			sourceID = fmt.Sprintf("%.0f", v)
		case int:
			sourceID = fmt.Sprintf("%d", v)
		case int64:
			sourceID = fmt.Sprintf("%d", v)
		default:
			sourceID = fmt.Sprintf("%v", v)
		}
	}
	if sourceID == "" {
		return nil, nil, fmt.Errorf("missing or invalid eventid")
	}
	id := shared.GenerateUUIDFromString(sourceID)

	var currentEpisodeID string
	if episodeIDVal, exists := props["episodeid"]; exists && episodeIDVal != nil {
		switch v := episodeIDVal.(type) {
		case string:
			currentEpisodeID = v
		case float64:
			currentEpisodeID = fmt.Sprintf("%.0f", v)
		case int:
			currentEpisodeID = fmt.Sprintf("%d", v)
		case int64:
			currentEpisodeID = fmt.Sprintf("%d", v)
		default:
			currentEpisodeID = fmt.Sprintf("%v", v)
		}
	}

	eventType, ok := props["eventtype"].(string)
	if !ok {
		return nil, nil, fmt.Errorf("missing or invalid eventtype")
	}
	alertType, exists := alertTypeMap[eventType]
	if !exists {
		return nil, nil, fmt.Errorf("unknown alert type: %s", eventType)
	}

	if alertType == "DROUGHTS" {
		return nil, nil, nil
	}

	alertLevelStr, ok := props["alertlevel"].(string)
	if !ok {
		return nil, nil, fmt.Errorf("missing or invalid alertlevel")
	}
	alertLevel, exists := alertLevelMap[alertLevelStr]
	if !exists {
		return nil, nil, fmt.Errorf("unknown alert level: %s", alertLevelStr)
	}

	name, ok := props["name"].(string)
	if !ok {
		return nil, nil, fmt.Errorf("missing or invalid name")
	}

	var description *string
	if desc, ok := props["htmldescription"].(string); ok && desc != "" {
		description = &desc
	}

	fromDateStr, ok := props["fromdate"].(string)
	if !ok {
		return nil, nil, fmt.Errorf("missing or invalid fromdate")
	}
	startDate := strings.Split(fromDateStr, "T")[0]

	toDateStr, ok := props["todate"].(string)
	if !ok {
		return nil, nil, fmt.Errorf("missing or invalid todate")
	}
	endDate := strings.Split(toDateStr, "T")[0]

	var lastModified string
	if dateModifiedStr, ok := props["datemodified"].(string); ok && dateModifiedStr != "" {
		if t, err := time.Parse(time.RFC3339, dateModifiedStr); err == nil {
			lastModified = t.Format("2006-01-02 15:04:05")
		} else if t, err := time.Parse("2006-01-02T15:04:05", dateModifiedStr); err == nil {
			lastModified = t.Format("2006-01-02 15:04:05")
		} else {
			lastModified = strings.Replace(dateModifiedStr, "T", " ", 1)
			if idx := strings.Index(lastModified, "+"); idx != -1 {
				lastModified = lastModified[:idx]
			}
			if idx := strings.Index(lastModified, "Z"); idx != -1 {
				lastModified = lastModified[:idx]
			}
			lastModified = strings.TrimSpace(lastModified)
		}
	}
	if lastModified == "" {
		lastModified = now.Format("2006-01-02 15:04:05")
	}

	var longitude, latitude float64
	coords, ok := feature.Geometry.Coordinates.([]interface{})
	if !ok || len(coords) < 2 {
		return nil, nil, fmt.Errorf("invalid geometry coordinates")
	}

	if lon, ok := coords[0].(float64); ok {
		longitude = lon
	} else {
		return nil, nil, fmt.Errorf("invalid longitude")
	}

	if lat, ok := coords[1].(float64); ok {
		latitude = lat
	} else {
		return nil, nil, fmt.Errorf("invalid latitude")
	}

	var geometryLink string
	if urlsVal, exists := props["url"]; exists && urlsVal != nil {
		if urlsMap, ok := urlsVal.(map[string]interface{}); ok {
			if geomLink, ok := urlsMap["geometry"].(string); ok {
				geometryLink = geomLink
			}
		}
	}

	created := now.Format("2006-01-02 15:04:05")

	alertRecord := &AlertChRecord{
		ID:               id,
		SourceID:         sourceID,
		CurrentEpisodeID: currentEpisodeID,
		Type:             alertType,
		Name:             name,
		Description:      description,
		Level:            alertLevel,
		StartDate:        startDate,
		EndDate:          endDate,
		LastModified:     lastModified,
		Created:          created,
		OriginLongitude:  longitude,
		OriginLatitude:   latitude,
	}

	metadata := &AlertMetadata{
		AlertID:         id,
		GeometryLink:    geometryLink,
		AlertType:       alertType,
		AlertLevel:      alertLevel,
		StartDate:       startDate,
		EndDate:         endDate,
		OriginLongitude: longitude,
		OriginLatitude:  latitude,
		AlertCountry:    country,
	}

	return alertRecord, metadata, nil
}

func getDateRange(startDate, endDate string) [][]string {
	if startDate == endDate {
		return [][]string{{startDate, endDate}}
	}

	start, _ := time.Parse("2006-01-02", startDate)
	end, _ := time.Parse("2006-01-02", endDate)
	middle := start.Add(end.Sub(start) / 2)
	middleStr := middle.Format("2006-01-02")
	nextDayStr := middle.AddDate(0, 0, 1).Format("2006-01-02")

	return [][]string{{startDate, middleStr}, {nextDayStr, endDate}}
}

func generateRequests(payload FetchAlertRequest, searchLevel int) []FetchAlertRequest {
	var requests []FetchAlertRequest

	if searchLevel == 1 {
		requests = append(requests, payload)
	} else if searchLevel == 2 {
		for alertTypeCode := range alertTypeMap {
			if alertTypeCode == "DR" {
				continue
			}
			req := payload
			req.Type = &alertTypeCode
			requests = append(requests, req)
		}
	} else if searchLevel == 3 {
		dateRanges := getDateRange(*payload.FromDate, *payload.ToDate)
		for _, dateRange := range dateRanges {
			req := payload
			fromDate := dateRange[0]
			toDate := dateRange[1]
			req.FromDate = &fromDate
			req.ToDate = &toDate
			requests = append(requests, req)
		}
	}

	return requests
}

func getAlerts(httpClient *http.Client, gdacBaseURL, gdacEndpoint string, payloads []FetchAlertRequest, searchLevel int) ([]GDACFeature, error) {
	var allFeatures []GDACFeature
	pending := []FetchAlertRequest{}

	log.Printf("Fetching at search level: %d", searchLevel)

	for len(payloads) > 0 {
		var requests []FetchAlertRequest
		for _, payload := range payloads {
			generated := generateRequests(payload, searchLevel)
			requests = append(requests, generated...)
		}

		type result struct {
			features   []GDACFeature
			payload    FetchAlertRequest
			statusCode int
			err        error
		}

		resultsChan := make(chan result, len(requests))
		semaphore := make(chan struct{}, 10)
		var wg sync.WaitGroup

		for _, req := range requests {
			wg.Add(1)
			semaphore <- struct{}{}

			go func(request FetchAlertRequest) {
				defer func() {
					<-semaphore
					wg.Done()
				}()

				url, _, err := fetchAlerts(gdacBaseURL, gdacEndpoint, request)
				if err != nil {
					resultsChan <- result{err: err}
					return
				}

				body, statusCode, err := makeRequest(httpClient, url, 3, 2)
				if err != nil {
					if statusCode == 403 {
						resultsChan <- result{payload: request, statusCode: 403}
						return
					}
					resultsChan <- result{err: err, statusCode: statusCode}
					return
				}

				if statusCode == 204 || body == nil {
					resultsChan <- result{features: []GDACFeature{}, statusCode: statusCode}
					return
				}

				var response GDACResponse
				if err := json.Unmarshal(body, &response); err != nil {
					resultsChan <- result{err: err}
					return
				}

				resultsChan <- result{features: response.Features, statusCode: statusCode}
			}(req)
		}

		go func() {
			wg.Wait()
			close(resultsChan)
		}()

		payloads = []FetchAlertRequest{}

		for res := range resultsChan {
			if res.err != nil {
				log.Printf("ERROR: Request failed: %v", res.err)
				continue
			}

			if res.statusCode == 403 {
				payloads = append(payloads, res.payload)
				continue
			}

			if res.statusCode != 200 {
				continue
			}

			features := res.features
			numAlerts := len(features)

			if numAlerts >= 100 && res.payload.FromDate != nil && res.payload.ToDate != nil && *res.payload.FromDate != *res.payload.ToDate {
				log.Printf("More than 100 alerts found, will split: %d alerts", numAlerts)
				pending = append(pending, res.payload)
			} else {
				allFeatures = append(allFeatures, features...)
			}
		}

		if len(payloads) > 0 {
			log.Printf("Rate limit exceeded. Waiting 10 seconds before retrying...")
			time.Sleep(10 * time.Second)
		}

		if len(pending) > 0 {
			nextSearchLevel := searchLevel + 1
			if nextSearchLevel > 3 {
				nextSearchLevel = 3
			}
			fetchedFeatures, err := getAlerts(httpClient, gdacBaseURL, gdacEndpoint, pending, nextSearchLevel)
			if err != nil {
				return nil, err
			}
			allFeatures = append(allFeatures, fetchedFeatures...)
			pending = []FetchAlertRequest{}
		}
	}

	seen := make(map[string]bool)
	var uniqueFeatures []GDACFeature
	for _, feature := range allFeatures {
		key := fmt.Sprintf("%v", feature.Properties)
		if !seen[key] {
			seen[key] = true
			uniqueFeatures = append(uniqueFeatures, feature)
		}
	}

	return uniqueFeatures, nil
}

type CountryData struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Shortname string `json:"shortname"`
	Altname   string `json:"altname"`
}

func GetValidCountries() ([]string, error) {
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	jsonPath := filepath.Join(dir, "Country_data.json")

	data, err := os.ReadFile(jsonPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read Country_data.json: %w", err)
	}

	var countries []CountryData
	if err := json.Unmarshal(data, &countries); err != nil {
		return nil, fmt.Errorf("failed to parse Country_data.json: %w", err)
	}

	countryNames := make(map[string]bool)
	for _, country := range countries {
		if country.Altname != "" {
			countryNames[strings.ToUpper(country.Altname)] = true
		}
	}

	log.Printf("Loaded %d countries from Country_data.json", len(countries))
	log.Printf("Total GDAC countries: %d", len(gdacCountries))

	var validCountries []string
	for _, gdacCountry := range gdacCountries {
		if countryNames[strings.ToUpper(gdacCountry)] {
			validCountries = append(validCountries, gdacCountry)
		}
	}

	log.Printf("Valid countries: %d", len(validCountries))
	return validCountries, nil
}

type GeoJSONFeature struct {
	Type       string                 `json:"type"`
	Bbox       []float64              `json:"bbox,omitempty"`
	Properties map[string]interface{} `json:"properties"`
	Geometry   GeoJSONGeometry        `json:"geometry"`
}

type GeoJSONGeometry struct {
	Type        string      `json:"type"`
	Coordinates interface{} `json:"coordinates"`
}

type GeoJSON struct {
	Type     string           `json:"type"`
	Bbox     []float64        `json:"bbox,omitempty"`
	Features []GeoJSONFeature `json:"features"`
}

func getValidFeatureClassesGeoJson(geojsonMap map[string]interface{}) (map[string]interface{}, error) {
	if geojsonMap == nil {
		return map[string]interface{}{
			"type":     "FeatureCollection",
			"features": []interface{}{},
		}, nil
	}

	validClasses := map[string]bool{
		"Poly_Orange": true,
		"Poly_Red":    true,
		"Poly_Green":  true,
		"Poly_area":   true,
		"Poly_Circle": true,
		"Poly_Global": true,
	}

	featuresRaw, ok := geojsonMap["features"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid GeoJSON: missing or invalid features array")
	}

	validFeatures := []interface{}{}
	for _, featRaw := range featuresRaw {
		feature, ok := featRaw.(map[string]interface{})
		if !ok {
			continue
		}

		geometry, ok := feature["geometry"].(map[string]interface{})
		if !ok {
			continue
		}
		geomType, _ := geometry["type"].(string)
		if geomType != "Polygon" && geomType != "MultiPolygon" {
			continue
		}

		properties, ok := feature["properties"].(map[string]interface{})
		if !ok {
			continue
		}
		classVal, ok := properties["Class"]
		if !ok {
			continue
		}
		class, ok := classVal.(string)
		if !ok {
			continue
		}
		if !validClasses[class] {
			continue
		}

		validFeatures = append(validFeatures, feature)
	}

	sort.Slice(validFeatures, func(i, j int) bool {
		featI, _ := validFeatures[i].(map[string]interface{})
		featJ, _ := validFeatures[j].(map[string]interface{})
		propsI, _ := featI["properties"].(map[string]interface{})
		propsJ, _ := featJ["properties"].(map[string]interface{})
		classI := getClassValue(propsI["Class"])
		classJ := getClassValue(propsJ["Class"])
		return classI < classJ
	})

	geojsonMap["features"] = validFeatures

	return geojsonMap, nil
}

func getClassValue(class interface{}) int {
	if classStr, ok := class.(string); ok {
		switch classStr {
		case "Poly_Green":
			return 0
		case "Poly_Orange":
			return 1
		default:
			return 2
		}
	}
	return 2
}

func InsertLocationPolygonsChDataSingleWorker(clickhouseConn driver.Conn, polygonRecords []LocationPolygonRecord) error {
	if len(polygonRecords) == 0 {
		return nil
	}

	for i, record := range polygonRecords {
		if record.TableID == "" {
			return fmt.Errorf("record %d has empty TableID", i)
		}
		if record.Polygon == "" {
			return fmt.Errorf("record %d has empty Polygon", i)
		}
		if len(record.Polygon) > 100*1024*1024 {
			return fmt.Errorf("record %d has polygon too large: %d bytes", i, len(record.Polygon))
		}
	}

	log.Printf("Checking ClickHouse connection health before inserting %d location_polygons_ch records", len(polygonRecords))
	connectionCheckErr := shared.RetryWithBackoff(
		func() error {
			return shared.CheckClickHouseConnectionAlive(clickhouseConn)
		},
		3,
		"ClickHouse connection health check",
	)
	if connectionCheckErr != nil {
		return fmt.Errorf("ClickHouse connection is not alive after retries: %w", connectionCheckErr)
	}
	log.Printf("ClickHouse connection is alive, proceeding with batch insert")

	insertErr := shared.RetryWithBackoff(
		func() error {
			return insertLocationPolygonsBatch(clickhouseConn, polygonRecords)
		},
		3,
		"location_polygons_ch batch insert",
	)

	if insertErr != nil {
		return fmt.Errorf("failed to insert location_polygons_ch batch after retries: %w", insertErr)
	}

	log.Printf("OK: Successfully inserted %d location_polygons_ch records", len(polygonRecords))
	return nil
}

func insertLocationPolygonsBatch(clickhouseConn driver.Conn, polygonRecords []LocationPolygonRecord) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	log.Printf("Preparing ClickHouse batch for %d location_polygons_ch records", len(polygonRecords))

	batch, err := clickhouseConn.PrepareBatch(ctx, `
		INSERT INTO testing_db.location_polygons_ch (
			tableName, tableId, polygon, polygonSourceUrl, createdAt, updatedAt, version
		)
	`)
	if err != nil {
		log.Printf("ERROR: Failed to prepare ClickHouse batch for location_polygons_ch: %v", err)
		log.Printf("ERROR: Attempting to insert %d records", len(polygonRecords))
		if len(polygonRecords) > 0 {
			log.Printf("ERROR: First record TableID: %s, Polygon length: %d",
				polygonRecords[0].TableID, len(polygonRecords[0].Polygon))
			if len(polygonRecords[0].TableID) != 36 || !strings.Contains(polygonRecords[0].TableID, "-") {
				log.Printf("ERROR: TableID may not be in valid UUID format: %s", polygonRecords[0].TableID)
			}
		}
		if strings.Contains(err.Error(), "EOF") {
			return fmt.Errorf("connection error (table may not exist or connection dropped): %v. Please verify table 'testing_db.location_polygons_ch' exists", err)
		}
		return fmt.Errorf("failed to prepare ClickHouse batch: %v", err)
	}

	for i, record := range polygonRecords {
		err := batch.Append(
			record.TableName,        // tableName: String
			record.TableID,          // tableId: UUID (as string)
			record.Polygon,          // polygon: String (GeoJSON)
			record.PolygonSourceURL, // polygonSourceUrl: Nullable(String)
			record.CreatedAt,        // createdAt: DateTime
			record.UpdatedAt,        // updatedAt: DateTime
			record.Version,          // version: UInt32
		)
		if err != nil {
			log.Printf("ERROR: Failed to append polygon record %d to batch: %v", i, err)
			log.Printf("ERROR: Record TableID: %s, Polygon length: %d", record.TableID, len(record.Polygon))
			return fmt.Errorf("failed to append record %d to batch: %v", i, err)
		}
	}

	log.Printf("Sending ClickHouse batch with %d location_polygons_ch records", len(polygonRecords))
	if err := batch.Send(); err != nil {
		log.Printf("ERROR: Failed to send ClickHouse batch: %v", err)
		return fmt.Errorf("failed to send ClickHouse batch: %v", err)
	}

	return nil
}

func fetchAndProcessPolygon(httpClient *http.Client, alertID, geometryLink string) (*LocationPolygonRecord, error) {
	body, statusCode, err := makeRequest(httpClient, geometryLink, 3, 2)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch polygon: %w", err)
	}
	if statusCode != 200 || body == nil {
		return nil, fmt.Errorf("failed to fetch polygon: status code %d", statusCode)
	}

	var geojsonMap map[string]interface{}
	if err := json.Unmarshal(body, &geojsonMap); err != nil {
		return nil, fmt.Errorf("failed to parse GeoJSON: %w", err)
	}

	validGeoJSONMap, err := getValidFeatureClassesGeoJson(geojsonMap)
	if err != nil {
		return nil, fmt.Errorf("failed to filter GeoJSON features: %w", err)
	}

	polygonJSON, err := json.Marshal(validGeoJSONMap)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal GeoJSON: %w", err)
	}

	now := time.Now()
	var sourceURL *string
	if geometryLink != "" {
		sourceURL = &geometryLink
	}

	return &LocationPolygonRecord{
		TableName:        "Alerts",
		TableID:          alertID,
		Polygon:          string(polygonJSON),
		PolygonSourceURL: sourceURL,
		CreatedAt:        now,
		UpdatedAt:        now,
		Version:          1,
	}, nil
}

func processPolygons(clickhouseConn driver.Conn, httpClient *http.Client, metadataMap map[string]*AlertMetadata, semaphore chan struct{}) error {
	type GeometryLink struct {
		AlertID      string
		GeometryLink string
		Metadata     *AlertMetadata
	}

	geometryLinks := []GeometryLink{}
	for alertID, meta := range metadataMap {
		if meta.GeometryLink != "" {
			geometryLinks = append(geometryLinks, GeometryLink{
				AlertID:      alertID,
				GeometryLink: meta.GeometryLink,
				Metadata:     meta,
			})
		}
	}

	if len(geometryLinks) == 0 {
		log.Printf("No geometry links to process")
		return nil
	}

	log.Printf("Processing %d geometry links in chunks of 20", len(geometryLinks))

	chunkSize := 20
	var allPolygons []LocationPolygonRecord
	var mu sync.Mutex
	var wg sync.WaitGroup

	requestCounter := 0
	var counterMu sync.Mutex

	var alertEventTypeID uint32
	maxEventTypeID, err := getMaxEventTypeIDForAlerts(clickhouseConn)
	if err != nil {
		log.Printf("ERROR: Failed to get max eventtype_id: %v", err)
		alertEventTypeID = 10
	} else {
		alertEventTypeID = maxEventTypeID + 10
		log.Printf("Using eventtype_id %d for all alert records (max in DB: %d, added buffer: 10)", alertEventTypeID, maxEventTypeID)
	}

	for i := 0; i < len(geometryLinks); i += chunkSize {
		end := i + chunkSize
		if end > len(geometryLinks) {
			end = len(geometryLinks)
		}
		chunk := geometryLinks[i:end]
		chunkNum := (i / chunkSize) + 1

		log.Printf("Processing chunk %d (%d geometry links)", chunkNum, len(chunk))

		type PolygonWithMetadata struct {
			Polygon  *LocationPolygonRecord
			Metadata *AlertMetadata
		}
		var chunkPolygonsWithMetadata []PolygonWithMetadata
		var chunkPolygonsMu sync.Mutex

		for _, geomLink := range chunk {
			wg.Add(1)
			semaphore <- struct{}{}

			go func(link GeometryLink) {
				defer func() {
					<-semaphore
					wg.Done()
				}()

				counterMu.Lock()
				requestCounter++
				currentCount := requestCounter
				shouldSleep := currentCount%10 == 0 && currentCount > 0
				counterMu.Unlock()

				if shouldSleep {
					log.Printf("Rate limiting: sleeping for 1 second after %d requests", currentCount)
					time.Sleep(1 * time.Second)
				}

				polygon, err := fetchAndProcessPolygon(httpClient, link.AlertID, link.GeometryLink)
				if err != nil {
					log.Printf("ERROR: Failed to fetch/process polygon for alert %s: %v", link.AlertID, err)
					return
				}

				mu.Lock()
				allPolygons = append(allPolygons, *polygon)
				mu.Unlock()

				chunkPolygonsMu.Lock()
				chunkPolygonsWithMetadata = append(chunkPolygonsWithMetadata, PolygonWithMetadata{
					Polygon:  polygon,
					Metadata: link.Metadata,
				})
				chunkPolygonsMu.Unlock()
			}(geomLink)
		}

		wg.Wait()

		if len(allPolygons) > 0 {
			chunkPolygons := allPolygons
			allPolygons = []LocationPolygonRecord{}

			if err := InsertLocationPolygonsChDataSingleWorker(clickhouseConn, chunkPolygons); err != nil {
				log.Printf("ERROR: Failed to insert polygon chunk %d: %v", chunkNum, err)
				return fmt.Errorf("failed to insert polygon chunk: %w", err)
			}
			log.Printf("Inserted %d polygons for chunk %d", len(chunkPolygons), chunkNum)
		}

		for _, pg := range chunkPolygonsWithMetadata {
			metadata := pg.Metadata
			polygonGeoJSON := pg.Polygon.Polygon

			eventTypeRecords, err := processPolygonFeaturesAndMapEvents(clickhouseConn, polygonGeoJSON, metadata, alertEventTypeID)
			if err != nil {
				log.Printf("ERROR: Failed to map events for alert %s (polygon): %v", metadata.AlertID, err)
				continue
			}

			if len(eventTypeRecords) > 0 {
				if err := InsertEventTypeChDataSingleWorker(clickhouseConn, eventTypeRecords); err != nil {
					log.Printf("ERROR: Failed to insert event_type_ch records for alert %s (polygon): %v", metadata.AlertID, err)
					continue
				}
				log.Printf("Inserted %d event_type_ch records for alert %s (polygon processed)", len(eventTypeRecords), metadata.AlertID)
			}
		}
	}

	return nil
}

func getMaxEventTypeIDForAlerts(clickhouseConn driver.Conn) (uint32, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	query := "SELECT MAX(eventtype_id) FROM testing_db.event_type_ch"
	row := clickhouseConn.QueryRow(ctx, query)

	var maxID uint32
	err := row.Scan(&maxID)
	if err != nil {
		if err.Error() == "sql: no rows in result set" || strings.Contains(err.Error(), "no rows") {
			log.Println("No existing records in event_type_ch, starting from 0")
			return 0, nil
		}
		return 0, fmt.Errorf("failed to get max eventtype_id: %v", err)
	}

	return maxID, nil
}

func extractPointsFromRing(ring interface{}) []struct {
	Lon, Lat float64
} {
	ringArr, ok := ring.([]interface{})
	if !ok {
		return nil
	}

	var rawPoints []struct {
		Lon, Lat float64
	}
	for _, point := range ringArr {
		pointArr, ok := point.([]interface{})
		if !ok || len(pointArr) < 2 {
			continue
		}
		lon, ok1 := pointArr[0].(float64)
		lat, ok2 := pointArr[1].(float64)
		if !ok1 || !ok2 {
			continue
		}
		rawPoints = append(rawPoints, struct {
			Lon, Lat float64
		}{Lon: lon, Lat: lat})
	}
	return rawPoints
}

func validatePoints(rawPoints []struct {
	Lon, Lat float64
}) bool {
	if len(rawPoints) == 0 {
		return false
	}

	if len(rawPoints) > 1 {
		firstPoint := rawPoints[0]
		allSame := true
		for i := 1; i < len(rawPoints); i++ {
			if rawPoints[i].Lon != firstPoint.Lon || rawPoints[i].Lat != firstPoint.Lat {
				allSame = false
				break
			}
		}
		if allSame {
			return false
		}
	}
	return true
}

func formatPointsToClickHouse(rawPoints []struct {
	Lon, Lat float64
}) string {
	points := make([]string, len(rawPoints))
	for i, p := range rawPoints {
		points[i] = fmt.Sprintf("(%f,%f)", p.Lon, p.Lat)
	}
	return "[[" + strings.Join(points, ",") + "]]"
}

func convertRingToClickHousePolygon(ring interface{}) (string, bool) {
	rawPoints := extractPointsFromRing(ring)
	if !validatePoints(rawPoints) {
		return "", false
	}
	return formatPointsToClickHouse(rawPoints), true
}

func convertFeatureToClickHousePolygon(feature GeoJSONFeature, alertID string, polygonSourceURL string, gdacSearchURL string) (string, error) {
	switch feature.Geometry.Type {
	case "Polygon":
		coords, ok := feature.Geometry.Coordinates.([]interface{})
		if !ok || len(coords) == 0 {
			return "", fmt.Errorf("invalid polygon coordinates")
		}

		if polygonStr, valid := convertRingToClickHousePolygon(coords[0]); valid {
			return polygonStr, nil
		}

		if alertID != "" {
			if polygonSourceURL != "" && gdacSearchURL != "" {
				log.Printf("WARNING: First ring (outer boundary) is invalid for alertID: %s, skipping polygon (same as PostGIS would return empty results), polygonSourceURL: %s, gdacSearchURL: %s", alertID, polygonSourceURL, gdacSearchURL)
			} else if polygonSourceURL != "" {
				log.Printf("WARNING: First ring (outer boundary) is invalid for alertID: %s, skipping polygon (same as PostGIS would return empty results), polygonSourceURL: %s", alertID, polygonSourceURL)
			} else {
				log.Printf("WARNING: First ring (outer boundary) is invalid for alertID: %s, skipping polygon (same as PostGIS would return empty results)", alertID)
			}
			LogPolygonError(alertID, gdacSearchURL, polygonSourceURL)
		}
		return "", fmt.Errorf("first ring (outer boundary) is invalid - skipping polygon (same as PostGIS behavior)")
	case "MultiPolygon":
		coords, ok := feature.Geometry.Coordinates.([]interface{})
		if !ok || len(coords) == 0 {
			return "", fmt.Errorf("invalid multipolygon coordinates")
		}
		polygon, ok := coords[0].([]interface{})
		if !ok || len(polygon) == 0 {
			return "", fmt.Errorf("empty multipolygon")
		}
		ring, ok := polygon[0].([]interface{})
		if !ok || len(ring) == 0 {
			return "", fmt.Errorf("empty multipolygon ring")
		}

		rawPoints := extractPointsFromRing(ring)
		if len(rawPoints) == 0 {
			return "", fmt.Errorf("no valid points in multipolygon")
		}

		if !validatePoints(rawPoints) {
			if alertID != "" {
				log.Printf("ERROR: Invalid multipolygon detected - all %d coordinates are identical (%f, %f) for alertID: %s", len(rawPoints), rawPoints[0].Lon, rawPoints[0].Lat, alertID)
				LogPolygonError(alertID, gdacSearchURL, polygonSourceURL)
			} else {
				log.Printf("ERROR: Invalid multipolygon detected - all %d coordinates are identical (%f, %f)", len(rawPoints), rawPoints[0].Lon, rawPoints[0].Lat)
			}
			return "", fmt.Errorf("invalid multipolygon: all coordinates are identical (%f, %f)", rawPoints[0].Lon, rawPoints[0].Lat)
		}

		return formatPointsToClickHouse(rawPoints), nil
	default:
		return "", fmt.Errorf("feature is not a Polygon or MultiPolygon (type: %s)", feature.Geometry.Type)
	}
}

func convertMultiPolygonToClickHouse(feature GeoJSONFeature, alertID string, _ string, _ string) ([]string, error) {
	if feature.Geometry.Type != "MultiPolygon" {
		return nil, fmt.Errorf("feature is not a MultiPolygon (type: %s)", feature.Geometry.Type)
	}

	coords, ok := feature.Geometry.Coordinates.([]interface{})
	if !ok || len(coords) == 0 {
		return nil, fmt.Errorf("invalid multipolygon coordinates")
	}

	var polygonStrs []string
	for polyIdx, polygonRaw := range coords {
		polygon, ok := polygonRaw.([]interface{})
		if !ok || len(polygon) == 0 {
			continue
		}

		ring, ok := polygon[0].([]interface{})
		if !ok || len(ring) == 0 {
			continue
		}

		rawPoints := extractPointsFromRing(ring)
		if len(rawPoints) == 0 {
			continue
		}

		if !validatePoints(rawPoints) {
			if alertID != "" {
				log.Printf("WARNING: Invalid polygon %d in MultiPolygon - all coordinates identical (%f, %f) for alertID: %s", polyIdx, rawPoints[0].Lon, rawPoints[0].Lat, alertID)
			}
			continue
		}

		polygonStr := formatPointsToClickHouse(rawPoints)
		polygonStrs = append(polygonStrs, polygonStr)
	}

	if len(polygonStrs) == 0 {
		return nil, fmt.Errorf("no valid polygons in MultiPolygon")
	}

	return polygonStrs, nil
}

func queryEventsWithinPolygon(clickhouseConn driver.Conn, isCircle bool, centerLat, centerLon float64, radiusMeters int, polygonStr string, startDate, endDate string, lastEventID uint32) ([]uint32, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var query string
	if isCircle {
		query = fmt.Sprintf(`
			SELECT event_id
			FROM testing_db.allevent_ch
			WHERE venue_lat IS NOT NULL
				AND venue_long IS NOT NULL
				AND greatCircleDistance(venue_lat, venue_long, %f, %f) <= %d
				AND start_date <= '%s'
				AND end_date >= '%s'
				AND event_id > %d
			ORDER BY event_id
			LIMIT 1000
		`, centerLat, centerLon, radiusMeters, endDate, startDate, lastEventID)
	} else {
		query = fmt.Sprintf(`
			SELECT event_id
			FROM testing_db.allevent_ch
			WHERE venue_lat IS NOT NULL
				AND venue_long IS NOT NULL
				AND pointInPolygon((toFloat64OrDefault(venue_long, 0.0), toFloat64OrDefault(venue_lat, 0.0)), %s)
				AND start_date <= '%s'
				AND end_date >= '%s'
				AND event_id > %d
			ORDER BY event_id
			LIMIT 1000
		`, polygonStr, endDate, startDate, lastEventID)
	}

	rows, err := clickhouseConn.Query(ctx, query)
	if err != nil {
		errStr := err.Error()

		if strings.Contains(errStr, "Polygon is not valid") || strings.Contains(errStr, "wrong topological dimension") || strings.Contains(errStr, "degenerate") {
			return nil, fmt.Errorf("INVALID_POLYGON: %w", err)
		}

		if strings.Contains(errStr, "Max query size exceeded") || strings.Contains(errStr, "max_query_size") {
			return nil, fmt.Errorf("polygon too large for ClickHouse query (max_query_size limit): %w. Consider increasing ClickHouse max_query_size setting or using a different approach", err)
		}
		return nil, fmt.Errorf("failed to query events: %w", err)
	}
	defer rows.Close()

	var eventIDs []uint32
	for rows.Next() {
		var eventID uint32
		if err := rows.Scan(&eventID); err != nil {
			log.Printf("ERROR: Failed to scan event_id: %v", err)
			continue
		}
		eventIDs = append(eventIDs, eventID)
	}

	return eventIDs, nil
}

func processPolygonFeaturesAndMapEvents(clickhouseConn driver.Conn, polygonGeoJSON string, metadata *AlertMetadata, alertEventTypeID uint32) ([]EventTypeChRecord, error) {
	var geojson GeoJSON
	if err := json.Unmarshal([]byte(polygonGeoJSON), &geojson); err != nil {
		return nil, fmt.Errorf("failed to parse polygon GeoJSON: %w", err)
	}

	alertEventTypeUUID := "37bb8e91-4fc8-45de-a072-be24fbca2a64"
	alertEventTypeName := "Alert"
	alertEventTypeSlug := "alert"
	alertEventGroupType := "UNSCHEDULED"

	now := time.Now()
	createdStr := now.Format("2006-01-02 15:04:05")

	var allEventTypeRecords []EventTypeChRecord
	affectedEvents := make(map[uint32]struct {
		AlertID    string
		AlertLevel string
		AlertType  string
		StartDate  string
		EndDate    string
	})

	for _, feature := range geojson.Features {
		properties := feature.Properties
		classVal, ok := properties["Class"]
		if !ok {
			continue
		}
		class, ok := classVal.(string)
		if !ok {
			continue
		}

		var featureAlertLevel string
		if class == "Poly_Orange" || class == "Poly_Red" || class == "Poly_Green" {
			parts := strings.Split(class, "_")
			if len(parts) > 1 {
				featureAlertLevel = strings.ToUpper(parts[1])
			}
		}
		if featureAlertLevel == "" {
			featureAlertLevel = metadata.AlertLevel
		}

		recoveryDays := 0
		if recoveryMap, exists := alertRecoveryDurations[metadata.AlertType]; exists {
			if days, exists := recoveryMap[featureAlertLevel]; exists {
				recoveryDays = days
			}
		}
		alertEndDate, _ := time.Parse("2006-01-02", metadata.EndDate)
		featureEndDateWithRecovery := alertEndDate.AddDate(0, 0, recoveryDays).Format("2006-01-02")

		var bufferAlertLevel string
		var bufferRadiusKm int
		if class == "Poly_Circle" {
			if bufferMap, exists := alertBufferAreas[metadata.AlertType]; exists {
				if buffer, exists := bufferMap[metadata.AlertLevel]; exists {
					bufferAlertLevel = buffer.Level
					bufferRadiusKm = buffer.Radius
				}
			}
		}

		var bufferEndDateWithRecovery string
		if class == "Poly_Circle" && bufferAlertLevel != "" {
			recoveryDaysBuffer := 0
			if recoveryMap, exists := alertRecoveryDurations[metadata.AlertType]; exists {
				if days, exists := recoveryMap[bufferAlertLevel]; exists {
					recoveryDaysBuffer = days
				}
			}
			bufferEndDateWithRecovery = alertEndDate.AddDate(0, 0, recoveryDaysBuffer).Format("2006-01-02")
		}

		if class != "Poly_Circle" {
			if feature.Geometry.Type != "Polygon" && feature.Geometry.Type != "MultiPolygon" {
				log.Printf("WARNING: Skipping feature with non-polygon geometry type: %s (class: %s)", feature.Geometry.Type, class)
				continue
			}
		}

		lastEventID := uint32(0)
		addBuffer := true

		for {
			var eventIDs []uint32
			var err error

			if class == "Poly_Circle" {
				radiusKm := 0
				if polygonLabel, ok := properties["polygonlabel"].(string); ok {
					parts := strings.Split(polygonLabel, "km")
					if len(parts) > 0 {
						if r, err := strconv.Atoi(strings.TrimSpace(parts[0])); err == nil {
							radiusKm = r
						}
					}
				}

				radiusMeters := radiusKm * 1000
				if addBuffer && bufferRadiusKm > 0 {
					radiusMeters = (radiusKm + bufferRadiusKm) * 1000
				}

				endDate := featureEndDateWithRecovery
				if addBuffer && bufferAlertLevel != "" && bufferEndDateWithRecovery != "" {
					endDate = bufferEndDateWithRecovery
				}

				eventIDs, err = queryEventsWithinPolygon(clickhouseConn, true, metadata.OriginLatitude, metadata.OriginLongitude, radiusMeters, "", metadata.StartDate, endDate, lastEventID)
				if err != nil {
					log.Printf("ERROR: Failed to query events for circle: %v", err)
					break
				}
			} else {
				var allEventIDs []uint32
				var polygonsToQuery []string

				if feature.Geometry.Type == "MultiPolygon" {
					polygonStrs, err := convertMultiPolygonToClickHouse(feature, metadata.AlertID, metadata.GeometryLink, metadata.GDACSearchURL)
					if err != nil {
						log.Printf("WARNING: Failed to convert MultiPolygon (same as PostGIS would return empty): %v (class: %s, alertID: %s, polygonSourceURL: %s, gdacSearchURL: %s)", err, class, metadata.AlertID, metadata.GeometryLink, metadata.GDACSearchURL)
						LogPolygonError(metadata.AlertID, metadata.GDACSearchURL, metadata.GeometryLink)
						break
					}
					polygonsToQuery = polygonStrs
				} else {
					polygonStr, err := convertFeatureToClickHousePolygon(feature, metadata.AlertID, metadata.GeometryLink, metadata.GDACSearchURL)
					if err != nil {
						log.Printf("WARNING: Failed to convert polygon (same as PostGIS would return empty): %v (geometry type: %s, class: %s, alertID: %s, polygonSourceURL: %s, gdacSearchURL: %s)", err, feature.Geometry.Type, class, metadata.AlertID, metadata.GeometryLink, metadata.GDACSearchURL)
						LogPolygonError(metadata.AlertID, metadata.GDACSearchURL, metadata.GeometryLink)
						break
					}
					polygonsToQuery = []string{polygonStr}
				}

				endDate := featureEndDateWithRecovery
				if addBuffer && bufferAlertLevel != "" && bufferEndDateWithRecovery != "" {
					endDate = bufferEndDateWithRecovery
				}

				for _, polygonStr := range polygonsToQuery {
					polygonEventIDs, err := queryEventsWithinPolygon(clickhouseConn, false, 0, 0, 0, polygonStr, metadata.StartDate, endDate, lastEventID)
					if err != nil {
						errStr := err.Error()
						if strings.Contains(errStr, "INVALID_POLYGON:") {
							log.Printf("WARNING: Invalid polygon detected (same as PostGIS would return empty) - skipping this polygon (class: %s, alertID: %s, polygonSourceURL: %s, gdacSearchURL: %s)", class, metadata.AlertID, metadata.GeometryLink, metadata.GDACSearchURL)
							LogPolygonError(metadata.AlertID, metadata.GDACSearchURL, metadata.GeometryLink)
							continue
						}
						log.Printf("ERROR: Failed to query events for polygon: %v", err)
						continue
					}
					allEventIDs = append(allEventIDs, polygonEventIDs...)
				}

				seen := make(map[uint32]bool)
				var uniqueEventIDs []uint32
				for _, id := range allEventIDs {
					if !seen[id] {
						seen[id] = true
						uniqueEventIDs = append(uniqueEventIDs, id)
					}
				}
				eventIDs = uniqueEventIDs
			}

			for _, eventID := range eventIDs {
				alertLevel := featureAlertLevel
				endDate := featureEndDateWithRecovery

				if addBuffer && bufferAlertLevel != "" {
					alertLevel = bufferAlertLevel
					if bufferEndDateWithRecovery != "" {
						endDate = bufferEndDateWithRecovery
					}
				}

				affectedEvents[eventID] = struct {
					AlertID    string
					AlertLevel string
					AlertType  string
					StartDate  string
					EndDate    string
				}{
					AlertID:    metadata.AlertID,
					AlertLevel: alertLevel,
					AlertType:  metadata.AlertType,
					StartDate:  metadata.StartDate,
					EndDate:    endDate,
				}
			}

			if len(eventIDs) > 0 {
				lastEventID = eventIDs[len(eventIDs)-1]
			}

			if addBuffer && class == "Poly_Circle" && len(eventIDs) < 1000 {
				addBuffer = false
				continue
			}

			if len(eventIDs) < 1000 {
				break
			}
		}
	}

	for eventID, alertData := range affectedEvents {
		alertID := alertData.AlertID
		alertLevel := alertData.AlertLevel
		alertType := alertData.AlertType
		alertStartDate := alertData.StartDate
		alertEndDate := alertData.EndDate

		record := EventTypeChRecord{
			EventTypeID:    alertEventTypeID,
			EventTypeUUID:  alertEventTypeUUID,
			EventID:        eventID,
			Published:      1,
			Name:           alertEventTypeName,
			Slug:           alertEventTypeSlug,
			EventAudience:  nil,
			EventGroupType: &alertEventGroupType,
			Groups:         []string{},
			Priority:       nil,
			Created:        createdStr,
			Version:        1,
			AlertID:        &alertID,
			AlertLevel:     &alertLevel,
			AlertType:      &alertType,
			AlertStartDate: &alertStartDate,
			AlertEndDate:   &alertEndDate,
		}
		allEventTypeRecords = append(allEventTypeRecords, record)
	}

	return allEventTypeRecords, nil
}

func InsertEventTypeChDataSingleWorker(clickhouseConn driver.Conn, eventTypeRecords []EventTypeChRecord) error {
	if len(eventTypeRecords) == 0 {
		return nil
	}

	log.Printf("Checking ClickHouse connection health before inserting %d event_type_ch records", len(eventTypeRecords))
	connectionCheckErr := shared.RetryWithBackoff(
		func() error {
			return shared.CheckClickHouseConnectionAlive(clickhouseConn)
		},
		3,
		"ClickHouse connection health check for event_type_ch",
	)
	if connectionCheckErr != nil {
		return fmt.Errorf("ClickHouse connection is not alive after retries: %w", connectionCheckErr)
	}
	log.Printf("ClickHouse connection is alive, proceeding with event_type_ch batch insert")

	insertErr := shared.RetryWithBackoff(
		func() error {
			return insertEventTypeChBatch(clickhouseConn, eventTypeRecords)
		},
		3,
		"event_type_ch batch insert",
	)

	if insertErr != nil {
		return fmt.Errorf("failed to insert event_type_ch batch after retries: %w", insertErr)
	}

	log.Printf("OK: Successfully inserted %d event_type_ch records with alert data", len(eventTypeRecords))
	return nil
}

func insertEventTypeChBatch(clickhouseConn driver.Conn, eventTypeRecords []EventTypeChRecord) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	log.Printf("Preparing ClickHouse batch for %d event_type_ch records", len(eventTypeRecords))

	batch, err := clickhouseConn.PrepareBatch(ctx, `
		INSERT INTO testing_db.event_type_ch (
			eventtype_id, eventtype_uuid, event_id, published, name, slug, event_audience, eventGroupType, groups, priority, created, version,
			alert_id, alert_level, alert_type, alert_start_date, alert_end_date
		)
	`)
	if err != nil {
		log.Printf("ERROR: Failed to prepare ClickHouse batch for event_type_ch: %v", err)
		if strings.Contains(err.Error(), "EOF") {
			return fmt.Errorf("connection error (table may not exist or connection dropped): %v. Please verify table 'testing_db.event_type_ch' exists with alert fields", err)
		}
		return fmt.Errorf("failed to prepare ClickHouse batch: %v", err)
	}

	for _, record := range eventTypeRecords {
		err := batch.Append(
			record.EventTypeID,    // eventtype_id: UInt32
			record.EventTypeUUID,  // eventtype_uuid: UUID
			record.EventID,        // event_id: UInt32
			record.Published,      // published: Int8
			record.Name,           // name: LowCardinality(String)
			record.Slug,           // slug: String
			record.EventAudience,  // event_audience: Nullable(UInt16)
			record.EventGroupType, // eventGroupType: LowCardinality(Nullable(String))
			record.Groups,         // groups: Array(String)
			record.Priority,       // priority: Nullable(Int8)
			record.Created,        // created: DateTime
			record.Version,        // version: UInt32
			record.AlertID,        // alert_id: Nullable(UUID)
			record.AlertLevel,     // alert_level: LowCardinality(Nullable(String))
			record.AlertType,      // alert_type: LowCardinality(Nullable(String))
			record.AlertStartDate, // alert_start_date: Nullable(Date)
			record.AlertEndDate,   // alert_end_date: Nullable(Date)
		)
		if err != nil {
			log.Printf("ERROR: Failed to append event_type record to batch: %v", err)
			return fmt.Errorf("failed to append record to batch: %v", err)
		}
	}

	log.Printf("Sending ClickHouse batch with %d event_type_ch records", len(eventTypeRecords))
	if err := batch.Send(); err != nil {
		log.Printf("ERROR: Failed to send ClickHouse batch: %v", err)
		return fmt.Errorf("failed to send ClickHouse batch: %v", err)
	}

	return nil
}

func ProcessAlertsFromAPI(clickhouseConn driver.Conn, gdacBaseURL, gdacEndpoint string, validCountries []string) error {
	startTime := time.Now()
	log.Printf("Starting alert processing from GDAC API")

	// Initialize polygon error logger
	if err := InitPolygonErrorLogger(); err != nil {
		log.Printf("WARNING: Failed to initialize polygon error logger: %v", err)
	}
	defer func() {
		if err := FinalizePolygonErrorLogger(); err != nil {
			log.Printf("WARNING: Failed to finalize polygon error logger: %v", err)
		}
	}()

	endDate := time.Now()
	startDate := endDate.AddDate(0, 0, -365)
	startDateStr := startDate.Format("2006-01-02")
	endDateStr := endDate.Format("2006-01-02")

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
			log.Printf("Fetching alerts for %s", countryName)

			alertLevel := "Green;Orange;Red"
			payload := FetchAlertRequest{
				Country:  &countryName,
				Level:    &alertLevel,
				FromDate: &startDateStr,
				ToDate:   &endDateStr,
			}

			// Build the initial GDAC search URL for this country
			initialGDACSearchURL, _, err := fetchAlerts(gdacBaseURL, gdacEndpoint, payload)
			if err != nil {
				log.Printf("ERROR: Failed to build GDAC search URL for %s: %v", countryName, err)
				return
			}

			features, err := getAlerts(httpClient, gdacBaseURL, gdacEndpoint, []FetchAlertRequest{payload}, 1)
			if err != nil {
				log.Printf("ERROR: Failed to fetch alerts for %s: %v", countryName, err)
				return
			}

			log.Printf("Fetched %d alerts for %s", len(features), countryName)

			now := time.Now()
			var countryAlerts []AlertChRecord
			countryMetadata := make(map[string]*AlertMetadata)

			for _, feature := range features {
				alert, metadata, err := parseAlertFeature(feature, now, countryName)
				if err != nil {
					log.Printf("ERROR: Failed to parse alert feature: %v", err)
					continue
				}
				if alert != nil && metadata != nil {
					// Store the GDAC search URL that fetched this alert
					metadata.GDACSearchURL = initialGDACSearchURL
					countryAlerts = append(countryAlerts, *alert)
					if metadata.GeometryLink != "" {
						countryMetadata[metadata.AlertID] = metadata
					}
				}
			}

			mu.Lock()
			allAlerts = append(allAlerts, countryAlerts...)
			for alertID, meta := range countryMetadata {
				allMetadata[alertID] = meta
			}
			mu.Unlock()

			countryEnd := time.Now()
			log.Printf("Processed %d alerts for %s in %v", len(countryAlerts), countryName, countryEnd.Sub(countryStart))
		}(country)
	}

	wg.Wait()

	log.Printf("Total alerts fetched: %d", len(allAlerts))

	batchSize := 1000
	for i := 0; i < len(allAlerts); i += batchSize {
		end := i + batchSize
		if end > len(allAlerts) {
			end = len(allAlerts)
		}
		batch := allAlerts[i:end]

		if err := InsertAlertsChDataSingleWorker(clickhouseConn, batch); err != nil {
			log.Printf("ERROR: Failed to insert alert batch: %v", err)
			return fmt.Errorf("failed to insert alert batch: %w", err)
		}
	}

	endTime := time.Now()
	log.Printf("Successfully processed and inserted %d alerts in %v", len(allAlerts), endTime.Sub(startTime))

	if len(allMetadata) > 0 {
		log.Printf("Starting polygon processing for %d alerts with geometry links", len(allMetadata))
		if err := processPolygons(clickhouseConn, httpClient, allMetadata, semaphore); err != nil {
			log.Printf("ERROR: Failed to process polygons: %v", err)
			return fmt.Errorf("failed to process polygons: %w", err)
		}
		log.Printf("Completed polygon processing")
	}

	return nil
}
