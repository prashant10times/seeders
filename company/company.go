package company

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"seeders/shared"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/elastic/go-elasticsearch/v6"
)

// CREATE TABLE testing_db.allCompany_ch                         ↴│
//    │↳(                                                             ↴│
//    │↳    `id` UInt32,                                              ↴│
//    │↳    `uuid` UUID,                                              ↴│
//    │↳    `name` Nullable(String),                                  ↴│
//    │↳    `address` Nullable(String),                               ↴│
//    │↳    `description` Nullable(String),                           ↴│
//    │↳    `logo` Nullable(String),                                  ↴│
//    │↳    `companyCity` Nullable(UInt32),                           ↴│
//    │↳    `companyCityName` Nullable(String),                       ↴│
//    │↳    `companyCityPlaceId` Nullable(String),                    ↴│
//    │↳    `companyCityUrl` Nullable(String),                         ↴│
//    │↳    `pincode` Nullable(String),                               ↴│
//    │↳    `companyState` Nullable(UInt32),                           ↴│
//    │↳    `companyStateName` Nullable(String),                       ↴│
//    │↳    `companyCountry` LowCardinality(Nullable(FixedString(2))), ↴│
//    │↳    `companyCountryName` LowCardinality(Nullable(String)),    ↴│
//    │↳    `companyCountryUrl` Nullable(String),                      ↴│
// 	  │↳    `countryZone` Nullable(String),                           ↴│
//    │↳    `companyWebsite` Nullable(String),                         ↴│
//    │↳    `companyDomain` Nullable(String),                          ↴│
//    │↳    `companyTwitter` Nullable(String),                         ↴│
//    │↳    `companyLinkedin` Nullable(String),                        ↴│
//    │↳    `companyInstagram` Nullable(String),                       ↴│
//    │↳    `companyFacebook` Nullable(String),                        ↴│
//    │↳    `companyYoutube` Nullable(String),                         ↴│
//    │↳    `companyPublished` Int8,                                   ↴│
//    │↳    `created` DateTime,                                        ↴│
//    │↳    `modified` DateTime,                                       ↴│
//    │↳    `totalEvents` UInt32,                                      ↴│
//    │↳    `totalLeads` UInt32,                                       ↴│
//    │↳    `score` UInt32,                                            ↴│
//    │↳    `onboarded` DateTime,                                      ↴│
//    │↳    `firstLogin` DateTime,                                     ↴│
//    │↳    `lastLogin` DateTime,                                      ↴│
//    │↳    `lastEventAdded` DateTime,                                 ↴│
//    │↳    `lastUpdatedAt` DateTime                                    ↴│
//    │↳)                                                              ↴│
//    │↳ENGINE = ReplacingMergeTree(lastUpdatedAt)                    ↴│
//    │↳PARTITION BY toYYYYMM(created)                                 ↴│
//    │↳ORDER BY (id, uuid)                                            ↴│
//    │↳SETTINGS index_granularity = 8192

// CompanyRecord holds one row for the allCompany_ch table (subset of columns seeded from SQL).
type CompanyRecord struct {
	ID                  uint32  `ch:"id"`
	UUID                string  `ch:"uuid"`
	Name                *string `ch:"name"`
	Address             *string `ch:"address"`
	Description         *string `ch:"description"`
	Logo                *string `ch:"logo"`
	CompanyCity         *uint32 `ch:"companyCity"`
	CompanyCityName     *string `ch:"companyCityName"`
	CompanyCityPlaceID  *string `ch:"companyCityPlaceId"`
	CompanyCityURL      *string `ch:"companyCityUrl"`
	Pincode             *string `ch:"pincode"`
	CompanyState        *uint32 `ch:"companyState"`
	CompanyStateName    *string `ch:"companyStateName"`
	CompanyCountry      *string `ch:"companyCountry"`
	CompanyCountryName  *string `ch:"companyCountryName"`
	CompanyCountryURL   *string `ch:"companyCountryUrl"`
	CountryZone         *string `ch:"countryZone"`
	CompanyWebsite      *string `ch:"companyWebsite"`
	CompanyDomain       *string `ch:"companyDomain"`
	CompanyTwitter      *string `ch:"companyTwitter"`
	CompanyLinkedin     *string `ch:"companyLinkedin"`
	CompanyInstagram    *string `ch:"companyInstagram"`
	CompanyFacebook     *string `ch:"companyFacebook"`
	CompanyYoutube      *string `ch:"companyYoutube"`
	CompanyPublished    int8    `ch:"companyPublished"`
	Created             string  `ch:"created"`
	Modified            string  `ch:"modified"`
	TotalEvents         uint32  `ch:"totalEvents"`
	TotalEventsB2B      uint32  `ch:"totalEventsB2B"`
	TotalEventsB2C      uint32  `ch:"totalEventsB2C"`
	TotalLeads          uint32  `ch:"totalLeads"`
	Score               uint32  `ch:"score"`
	FutureExpectedUpcomingEventCount uint32  `ch:"futureExpectedUpcomingEventCount"`
	CompanyCurrentIntent             []string `ch:"companyCurrentIntent"`
	TopEventCountryZone              *string  `ch:"topEventCountryZone"`
	Onboarded           string  `ch:"onboarded"`
	FirstLogin          string  `ch:"firstLogin"`
	LastLogin           string  `ch:"lastLogin"`
	LastEventAdded      string  `ch:"lastEventAdded"`
	LastUpdatedAt       string  `ch:"lastUpdatedAt"`
	// From ES interest_data_v2
	UpcomingEvents                uint32 `ch:"upcomingEvents"`
	UpcomingEventsB2B              uint32 `ch:"upcomingEventsB2B"`
	UpcomingEventsB2C              uint32 `ch:"upcomingEventsB2C"`
	TotalExhibit                   uint32 `ch:"totalExhibit"`
	TotalExhibitB2B                uint32 `ch:"totalExhibitB2B"`
	TotalExhibitB2C                uint32 `ch:"totalExhibitB2C"`
	TotalSponsor                   uint32 `ch:"totalSponsor"`
	TotalSponsorB2B                uint32 `ch:"totalSponsorB2B"`
	TotalSponsorB2C                uint32 `ch:"totalSponsorB2C"`
	TotalSpeaker                   uint32 `ch:"totalSpeaker"`
	TotalSpeakerB2B                uint32 `ch:"totalSpeakerB2B"`
	TotalSpeakerB2C                uint32 `ch:"totalSpeakerB2C"`
	LinkedEvent                    uint32 `ch:"linkedEvent"`
	LinkedEventB2B                 uint32 `ch:"linkedEventB2B"`
	LinkedEventB2C                 uint32 `ch:"linkedEventB2C"`
	Following                      uint32 `ch:"following"`
	CompanyLinkedEvents            uint32 `ch:"companyLinkedEvents"`
	CompanyLinkedEventUpcoming     uint32 `ch:"companyLinkedEventUpcoming"`
	CompanyEventFocusAsOrganizer   uint32 `ch:"companyEventFocusAsOrganizer"`
	CompanyEventFocusAsSpeaker     uint32 `ch:"companyEventFocusAsSpeaker"`
	CompanyEventFocusAsSponsor     uint32 `ch:"companyEventFocusAsSponsor"`
	CompanyEventFocusAsExhibitor   uint32 `ch:"companyEventFocusAsExhibitor"`
	CompanyParticipationFreqB2B    int8 `ch:"companyParticipationFrequencyB2B"` // Enum8('unknown'=0,'very_low'=1,...,'very_high'=5)
	CompanyParticipationFreqB2C    int8 `ch:"companyParticipationFrequencyB2C"`
	CompanyParticipationActivenessB2B string `ch:"companyParticipationActivenessB2B"`
	CompanyParticipationActivenessB2C string `ch:"companyParticipationActivenessB2C"`
}

// ProcessCompanyOnly seeds allCompany_ch from MySQL company table and ES company index (with city, country, attachment joins).
func ProcessCompanyOnly(mysqlDB *sql.DB, clickhouseConn driver.Conn, esClient *elasticsearch.Client, config shared.Config) {
	log.Println("=== Starting COMPANY Processing ===")

	totalRecords, minID, maxID, err := shared.GetTotalRecordsAndIDRange(mysqlDB, "company")
	if err != nil {
		log.Fatal("Failed to get total records and ID range from company:", err)
	}

	log.Printf("Total company records: %d, Min ID: %d, Max ID: %d", totalRecords, minID, maxID)

	if config.NumChunks <= 0 {
		config.NumChunks = 5
	}

	chunkSize := (maxID - minID + 1) / config.NumChunks
	if chunkSize == 0 {
		chunkSize = 1
	}

	log.Printf("Processing company data in %d chunks with chunk size: %d", config.NumChunks, chunkSize)

	results := make(chan string, config.NumChunks)
	semaphore := make(chan struct{}, config.NumWorkers)

	for i := 0; i < config.NumChunks; i++ {
		startID := minID + (i * chunkSize)
		endID := startID + chunkSize - 1
		if i == config.NumChunks-1 {
			endID = maxID
		}
		if i > 0 {
			time.Sleep(3 * time.Second)
		}

		semaphore <- struct{}{}
		go func(chunkNum, start, end int) {
			defer func() { <-semaphore }()
			processCompanyChunk(mysqlDB, clickhouseConn, esClient, config, start, end, chunkNum, results)
		}(i+1, startID, endID)
	}

	for i := 0; i < config.NumChunks; i++ {
		result := <-results
		log.Printf("Company Result: %s", result)
	}

	log.Println("Company processing completed!")
}

func processCompanyChunk(mysqlDB *sql.DB, clickhouseConn driver.Conn, esClient *elasticsearch.Client, config shared.Config, startID, endID int, chunkNum int, results chan<- string) {
	log.Printf("Processing company chunk %d: ID range %d-%d", chunkNum, startID, endID)

	totalRecords := endID - startID + 1
	processed := 0
	start := startID

	for {
		batchData, err := buildCompanyMigrationData(mysqlDB, start, endID, config.BatchSize)
		if err != nil {
			results <- fmt.Sprintf("Company chunk %d batch error: %v", chunkNum, err)
			return
		}

		if len(batchData) == 0 {
			break
		}

		processed += len(batchData)
		progress := float64(processed) / float64(totalRecords) * 100
		log.Printf("Company chunk %d: Retrieved %d records in batch (%.1f%% complete)", chunkNum, len(batchData), progress)

		companyIDs := extractCompanyIDs(batchData)
		var esData map[int64]map[string]interface{}
		if esClient != nil && config.CompanyIndex != "" && len(companyIDs) > 0 {
			esData = fetchCompanyInterestFromES(esClient, config.CompanyIndex, companyIDs)
			log.Printf("Company chunk %d: ES %s hit %d/%d companies", chunkNum, config.CompanyIndex, len(esData), len(companyIDs))
		}
		var esDataV1 map[int64]map[string]interface{}
		if esClient != nil && config.CompanyV1Index != "" && len(companyIDs) > 0 {
			esDataV1 = fetchCompanyV1FromES(esClient, config.CompanyV1Index, companyIDs)
			log.Printf("Company chunk %d: ES %s (company_v1) hit %d/%d companies", chunkNum, config.CompanyV1Index, len(esDataV1), len(companyIDs))
		}

		var companyRecords []CompanyRecord
		now := time.Now().Format("2006-01-02 15:04:05")

		for _, row := range batchData {
			rec := mapRowToCompanyRecord(row, now, esData, esDataV1)
			companyRecords = append(companyRecords, rec)
		}

		if len(companyRecords) > 0 {
			insertErr := shared.RetryWithBackoff(
				func() error {
					return insertCompanyDataIntoClickHouse(clickhouseConn, companyRecords, config.ClickHouseWorkers)
				},
				3,
			)
			if insertErr != nil {
				log.Printf("Company chunk %d: Insert failed after retries: %v", chunkNum, insertErr)
				results <- fmt.Sprintf("Company chunk %d: Failed to insert %d records", chunkNum, len(companyRecords))
				return
			}
			log.Printf("Company chunk %d: Inserted %d records into allCompany_temp", chunkNum, len(companyRecords))
		}

		if len(batchData) > 0 {
			if lastID, ok := batchData[len(batchData)-1]["id"].(int64); ok {
				start = int(lastID) + 1
			}
		}

		if len(batchData) < config.BatchSize {
			break
		}
	}

	results <- fmt.Sprintf("Company chunk %d: Completed successfully", chunkNum)
}

// buildCompanyMigrationData fetches company rows with JOINs to attachment, city, country.
func buildCompanyMigrationData(db *sql.DB, startID, endID int, batchSize int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT 
			c.id,
			c.name,
			c.address,
			c.profile,
			c.city,
			c.pincode,
			c.country,
			c.website,
			c.domain,
			c.twitter_id,
			c.linkedin_id,
			c.instagram_id,
			c.facebook_id,
			c.youtube_id,
			c.published,
			c.created,
			c.modified,
			c.total_events,
			c.total_leads,
			c.company_score,
			c.onboard_date,
			c.first_login,
			c.last_login,
			c.last_event_added,
			a.cdn_url AS logo_url,
			ct.name AS city_name,
			ct.place_id AS city_place_id,
			ct.url AS city_url,
			ct.state AS city_state,
			co.id AS country_code,
			co.name AS country_name,
			co.url AS country_url,
			co.zone AS country_zone
		FROM company c
		LEFT JOIN attachment a ON c.logo = a.id
		LEFT JOIN city ct ON c.city = ct.id
		LEFT JOIN country co ON c.country = co.id
		WHERE c.id >= %d AND c.id <= %d
		ORDER BY c.id
		LIMIT %d`, startID, endID, batchSize)

	log.Printf("[SQL Company] %s", strings.TrimSpace(query))
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
			if val != nil {
				row[col] = val
			}
		}
		results = append(results, row)
	}

	return results, nil
}

func mapRowToCompanyRecord(row map[string]interface{}, lastUpdatedAt string, esData, esDataV1 map[int64]map[string]interface{}) CompanyRecord {
	id := shared.ConvertToUInt32(row["id"])
	created := shared.SafeClickHouseDateTimeString(row["created"])
	uuid := shared.GenerateUUIDFromString(fmt.Sprintf("%d-%s", id, created))

	rec := CompanyRecord{
		ID:               id,
		UUID:             uuid,
		Name:             shared.ConvertToStringPtr(row["name"]),
		Address:          shared.ConvertToStringPtr(row["address"]),
		Description:      shared.ConvertToStringPtr(row["profile"]),
		Logo:             shared.ConvertToStringPtr(row["logo_url"]),
		CompanyCity:      shared.ConvertToUInt32Ptr(row["city"]),
		CompanyCityName:  shared.ConvertToStringPtr(row["city_name"]),
		CompanyCityPlaceID: shared.ConvertToStringPtr(row["city_place_id"]),
		CompanyCityURL:   shared.ConvertToStringPtr(row["city_url"]),
		Pincode:          shared.ConvertToStringPtr(row["pincode"]),
		CompanyState:     nil, // keep empty as per spec
		CompanyStateName: shared.ConvertToStringPtr(row["city_state"]),
		CompanyCountry:   toFixedString2(shared.ConvertToStringPtr(row["country_code"])),
		CompanyCountryName: shared.ConvertToStringPtr(row["country_name"]),
		CompanyCountryURL:  shared.ConvertToStringPtr(row["country_url"]),
		CountryZone:      shared.ConvertToStringPtr(row["country_zone"]),
		CompanyWebsite:   shared.ConvertToStringPtr(row["website"]),
		CompanyDomain:    shared.ConvertToStringPtr(row["domain"]),
		CompanyTwitter:   shared.ConvertToStringPtr(row["twitter_id"]),
		CompanyLinkedin:  shared.ConvertToStringPtr(row["linkedin_id"]),
		CompanyInstagram: shared.ConvertToStringPtr(row["instagram_id"]),
		CompanyFacebook:  shared.ConvertToStringPtr(row["facebook_id"]),
		CompanyYoutube:   shared.ConvertToStringPtr(row["youtube_id"]),
		CompanyPublished: shared.SafeConvertToInt8(row["published"]),
		Created:          created,
		Modified:         shared.SafeClickHouseDateTimeString(row["modified"]),
		TotalEvents:     shared.ConvertToUInt32(row["total_events"]),
		TotalLeads:      shared.ConvertToUInt32(row["total_leads"]),
		Score:           shared.ConvertToUInt32(row["company_score"]),
		Onboarded:       shared.SafeClickHouseDateTimeString(row["onboard_date"]),
		FirstLogin:      shared.SafeClickHouseDateTimeString(row["first_login"]),
		LastLogin:       shared.SafeClickHouseDateTimeString(row["last_login"]),
		LastEventAdded:  shared.SafeClickHouseDateTimeString(row["last_event_added"]),
		LastUpdatedAt:    lastUpdatedAt,
	}

	if esData != nil {
		if companyID, ok := row["id"].(int64); ok {
			if es, exists := esData[companyID]; exists {
				mergeESInterestIntoRecord(&rec, es)
			}
		}
	}
	if esDataV1 != nil {
		if companyID, ok := row["id"].(int64); ok {
			if v1, exists := esDataV1[companyID]; exists {
				mergeCompanyV1IntoRecord(&rec, v1)
			}
		}
	}

	return rec
}

func extractCompanyIDs(batchData []map[string]interface{}) []int64 {
	seen := make(map[int64]bool)
	var ids []int64
	for _, row := range batchData {
		if v, ok := row["id"]; ok && v != nil {
			var id int64
			switch x := v.(type) {
			case int64:
				id = x
			case int32:
				id = int64(x)
			case int:
				id = int64(x)
			case float64:
				id = int64(x)
			default:
				continue
			}
			if id > 0 && !seen[id] {
				seen[id] = true
				ids = append(ids, id)
			}
		}
	}
	return ids
}

// fetchCompanyInterestFromES fetches company interest/aggregate data from ES index (e.g. interest_data_v2) by company IDs.
func fetchCompanyInterestFromES(esClient *elasticsearch.Client, indexName string, companyIDs []int64) map[int64]map[string]interface{} {
	if len(companyIDs) == 0 {
		return nil
	}
	const batchSize = 200
	results := make(map[int64]map[string]interface{})
	for i := 0; i < len(companyIDs); i += batchSize {
		end := i + batchSize
		if end > len(companyIDs) {
			end = len(companyIDs)
		}
		batch := companyIDs[i:end]
		batched := fetchCompanyInterestESBatch(esClient, indexName, batch)
		for k, v := range batched {
			results[k] = v
		}
	}
	return results
}

func fetchCompanyInterestESBatch(esClient *elasticsearch.Client, indexName string, companyIDs []int64) map[int64]map[string]interface{} {
	results := make(map[int64]map[string]interface{})
	idStrs := make([]string, len(companyIDs))
	for i, id := range companyIDs {
		idStrs[i] = strconv.FormatInt(id, 10)
	}
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"terms": map[string]interface{}{
				"company_id": idStrs,
			},
		},
		"size": len(companyIDs),
		"_source": []string{
			"company_id",
			"company_upcomingOrganized", "company_upcomingOrganizedB2b", "company_upcomingOrganizedB2c",
			"company_totalExhibited", "company_totalExhibitedB2b", "company_totalExhibitedB2c",
			"company_totalSponsor", "company_totalSponsorB2b", "company_totalSponsorB2c",
			"company_totalSpeaker", "company_totalSpeakerB2b", "company_totalSpeakerB2c",
			"company_linkedEvents", "company_linkedEventsB2b", "company_linkedEventsB2c",
			"company_follower",
			"company_linkedEventUpcoming",
			"company_event_focus_as_organizer", "company_event_focus_as_speaker",
			"company_event_focus_as_sponsor", "company_event_focus_as_exhibitor",
			"company_participation_frequency_b2b", "company_participation_frequency_b2c",
			"company_participation_activeness_b2b", "company_participation_activeness_b2c",
			"company_curEdiPartType",
		},
	}
	queryJSON, _ := json.Marshal(query)
	log.Printf("[ES Company] index=%s query=%s", indexName, string(queryJSON))
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	searchRes, err := esClient.Search(
		esClient.Search.WithContext(ctx),
		esClient.Search.WithIndex(indexName),
		esClient.Search.WithBody(strings.NewReader(string(queryJSON))),
	)
	if err != nil {
		log.Printf("Warning: ES search interest_data_v2 failed: %v", err)
		return results
	}
	defer searchRes.Body.Close()
	if searchRes.IsError() {
		log.Printf("Warning: ES search interest_data_v2 status: %s", searchRes.Status())
		return results
	}
	var body map[string]interface{}
	if err := json.NewDecoder(searchRes.Body).Decode(&body); err != nil {
		log.Printf("Warning: ES interest_data_v2 decode failed: %v", err)
		return results
	}
	hits, _ := body["hits"].(map[string]interface{})
	hitsArray, _ := hits["hits"].([]interface{})
	for _, hit := range hitsArray {
		hitMap, _ := hit.(map[string]interface{})
		source, _ := hitMap["_source"].(map[string]interface{})
		companyID := esSourceToCompanyID(source)
		if companyID > 0 {
			results[companyID] = source
		}
	}
	return results
}

func esSourceToCompanyID(source map[string]interface{}) int64 {
	v, ok := source["company_id"]
	if !ok || v == nil {
		return 0
	}
	switch x := v.(type) {
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	case float64:
		return int64(x)
	case int64:
		return x
	case int:
		return int64(x)
	default:
		return 0
	}
}

func esUint32(source map[string]interface{}, key string) uint32 {
	v, ok := source[key]
	if !ok || v == nil {
		return 0
	}
	switch x := v.(type) {
	case float64:
		if x >= 0 && x == float64(uint32(x)) {
			return uint32(x)
		}
	case int:
		if x >= 0 {
			return uint32(x)
		}
	case int64:
		if x >= 0 && x <= 0xFFFFFFFF {
			return uint32(x)
		}
	case string:
		n, _ := strconv.ParseUint(x, 10, 32)
		return uint32(n)
	}
	return 0
}

func esString(source map[string]interface{}, key string) string {
	v, ok := source[key]
	if !ok || v == nil {
		return ""
	}
	return shared.ConvertToString(v)
}

// participationFreqToEnum8 maps ES string to Enum8('unknown'=0,'very_low'=1,...,'very_high'=5). Returns 0 for unknown/empty.
func participationFreqToEnum8(s string) int8 {
	switch strings.TrimSpace(strings.ToLower(s)) {
	case "very_low":
		return 1
	case "low":
		return 2
	case "medium":
		return 3
	case "high":
		return 4
	case "very_high":
		return 5
	default:
		return 0 // unknown
	}
}

func mergeESInterestIntoRecord(rec *CompanyRecord, es map[string]interface{}) {
	rec.UpcomingEvents = esUint32(es, "company_upcomingOrganized")
	rec.UpcomingEventsB2B = esUint32(es, "company_upcomingOrganizedB2b")
	rec.UpcomingEventsB2C = esUint32(es, "company_upcomingOrganizedB2c")
	rec.TotalExhibit = esUint32(es, "company_totalExhibited")
	rec.TotalExhibitB2B = esUint32(es, "company_totalExhibitedB2b")
	rec.TotalExhibitB2C = esUint32(es, "company_totalExhibitedB2c")
	rec.TotalSponsor = esUint32(es, "company_totalSponsor")
	rec.TotalSponsorB2B = esUint32(es, "company_totalSponsorB2b")
	rec.TotalSponsorB2C = esUint32(es, "company_totalSponsorB2c")
	rec.TotalSpeaker = esUint32(es, "company_totalSpeaker")
	rec.TotalSpeakerB2B = esUint32(es, "company_totalSpeakerB2b")
	rec.TotalSpeakerB2C = esUint32(es, "company_totalSpeakerB2c")
	rec.LinkedEvent = esUint32(es, "company_linkedEvents")
	rec.LinkedEventB2B = esUint32(es, "company_linkedEventsB2b")
	rec.LinkedEventB2C = esUint32(es, "company_linkedEventsB2c")
	rec.Following = esUint32(es, "company_follower")
	rec.CompanyLinkedEvents = esUint32(es, "company_linkedEvents")
	rec.CompanyLinkedEventUpcoming = esUint32(es, "company_linkedEventUpcoming")
	rec.CompanyEventFocusAsOrganizer = esUint32(es, "company_event_focus_as_organizer")
	rec.CompanyEventFocusAsSpeaker = esUint32(es, "company_event_focus_as_speaker")
	rec.CompanyEventFocusAsSponsor = esUint32(es, "company_event_focus_as_sponsor")
	rec.CompanyEventFocusAsExhibitor = esUint32(es, "company_event_focus_as_exhibitor")
	rec.CompanyParticipationFreqB2B = participationFreqToEnum8(esString(es, "company_participation_frequency_b2b"))
	rec.CompanyParticipationFreqB2C = participationFreqToEnum8(esString(es, "company_participation_frequency_b2c"))
	rec.CompanyParticipationActivenessB2B = esString(es, "company_participation_activeness_b2b")
	rec.CompanyParticipationActivenessB2C = esString(es, "company_participation_activeness_b2c")
	rec.CompanyCurrentIntent = esStringSlice(es, "company_curEdiPartType")
}

// fetchCompanyV1FromES fetches company_v1 data (total_organized_b2b, total_organized_b2c, ft_ex_upe) by company id; company_v1 index uses "id" for company id.
func fetchCompanyV1FromES(esClient *elasticsearch.Client, indexName string, companyIDs []int64) map[int64]map[string]interface{} {
	if len(companyIDs) == 0 {
		return nil
	}
	const batchSize = 200
	results := make(map[int64]map[string]interface{})
	for i := 0; i < len(companyIDs); i += batchSize {
		end := i + batchSize
		if end > len(companyIDs) {
			end = len(companyIDs)
		}
		batch := companyIDs[i:end]
		batched := fetchCompanyV1ESBatch(esClient, indexName, batch)
		for k, v := range batched {
			results[k] = v
		}
	}
	return results
}

func fetchCompanyV1ESBatch(esClient *elasticsearch.Client, indexName string, companyIDs []int64) map[int64]map[string]interface{} {
	results := make(map[int64]map[string]interface{})
	idStrs := make([]string, len(companyIDs))
	for i, id := range companyIDs {
		idStrs[i] = strconv.FormatInt(id, 10)
	}
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"terms": map[string]interface{}{
				"id": idStrs,
			},
		},
		"size":  len(companyIDs),
		"_source": []string{"id", "total_organized_b2b", "total_organized_b2c", "ft_ex_upe"},
	}
	queryJSON, _ := json.Marshal(query)
	log.Printf("[ES company_v1] index=%s query=%s", indexName, string(queryJSON))
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	searchRes, err := esClient.Search(
		esClient.Search.WithContext(ctx),
		esClient.Search.WithIndex(indexName),
		esClient.Search.WithBody(strings.NewReader(string(queryJSON))),
	)
	if err != nil {
		log.Printf("Warning: ES search company_v1 failed: %v", err)
		return results
	}
	defer searchRes.Body.Close()
	if searchRes.IsError() {
		log.Printf("Warning: ES search company_v1 status: %s", searchRes.Status())
		return results
	}
	var body map[string]interface{}
	if err := json.NewDecoder(searchRes.Body).Decode(&body); err != nil {
		log.Printf("Warning: ES company_v1 decode failed: %v", err)
		return results
	}
	hits, _ := body["hits"].(map[string]interface{})
	hitsArray, _ := hits["hits"].([]interface{})
	for _, hit := range hitsArray {
		hitMap, _ := hit.(map[string]interface{})
		source, _ := hitMap["_source"].(map[string]interface{})
		companyID := esSourceToCompanyIDV1(source)
		if companyID > 0 {
			results[companyID] = source
		}
	}
	return results
}

// esSourceToCompanyIDV1 reads company id from company_v1 source where the field is "id".
func esSourceToCompanyIDV1(source map[string]interface{}) int64 {
	v, ok := source["id"]
	if !ok || v == nil {
		return 0
	}
	switch x := v.(type) {
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	case float64:
		return int64(x)
	case int64:
		return x
	case int:
		return int64(x)
	default:
		return 0
	}
}

func mergeCompanyV1IntoRecord(rec *CompanyRecord, v1 map[string]interface{}) {
	rec.TotalEventsB2B = esUint32(v1, "total_organized_b2b")
	rec.TotalEventsB2C = esUint32(v1, "total_organized_b2c")
	rec.FutureExpectedUpcomingEventCount = esUint32(v1, "ft_ex_upe")
}

// esStringSlice returns []string from an ES _source field (array of strings or single string).
func esStringSlice(source map[string]interface{}, key string) []string {
	v, ok := source[key]
	if !ok || v == nil {
		return nil
	}
	switch x := v.(type) {
	case []interface{}:
		out := make([]string, 0, len(x))
		for _, item := range x {
			out = append(out, shared.ConvertToString(item))
		}
		return out
	case []string:
		return x
	case string:
		if x == "" {
			return nil
		}
		return []string{x}
	default:
		return nil
	}
}

// toFixedString2 returns a 2-char country code for LowCardinality(FixedString(2)), or nil.
func toFixedString2(s *string) *string {
	if s == nil || *s == "" {
		return nil
	}
	trimmed := strings.TrimSpace(*s)
	if len(trimmed) >= 2 {
		two := trimmed[:2]
		return &two
	}
	return s
}

func insertCompanyDataIntoClickHouse(clickhouseConn driver.Conn, records []CompanyRecord, numWorkers int) error {
	if len(records) == 0 {
		return nil
	}
	if numWorkers <= 1 {
		return insertCompanyDataSingleWorker(clickhouseConn, records)
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
		go func(s, e int) {
			defer func() { <-semaphore }()
			results <- insertCompanyDataSingleWorker(clickhouseConn, records[s:e])
		}(start, end)
	}

	for i := 0; i < numWorkers && i*batchSize < len(records); i++ {
		if err := <-results; err != nil {
			return err
		}
	}
	return nil
}

func insertCompanyDataSingleWorker(clickhouseConn driver.Conn, records []CompanyRecord) error {
	if len(records) == 0 {
		return nil
	}

	connectionCheckErr := shared.RetryWithBackoff(
		func() error { return shared.CheckClickHouseConnectionAlive(clickhouseConn) },
		3,
	)
	if connectionCheckErr != nil {
		return fmt.Errorf("ClickHouse connection is not alive after retries: %w", connectionCheckErr)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	batch, err := clickhouseConn.PrepareBatch(ctx, `
		INSERT INTO allCompany_temp (
			id, uuid, name, address, description, logo,
			companyCity, companyCityName, companyCityPlaceId, companyCityUrl,
			pincode, companyState, companyStateName,
			companyCountry, companyCountryName, companyCountryUrl, countryZone,
			companyWebsite, companyDomain, companyTwitter, companyLinkedin,
			companyInstagram, companyFacebook, companyYoutube,
			companyPublished, created, modified,
			totalEvents, totalEventsB2B, totalEventsB2C, totalLeads, score, onboarded,
			futureExpectedUpcomingEventCount, companyCurrentIntent, topEventCountryZone,
			firstLogin, lastLogin, lastEventAdded, lastUpdatedAt,
			upcomingEvents, upcomingEventsB2B, upcomingEventsB2C,
			totalExhibit, totalExhibitB2B, totalExhibitB2C,
			totalSponsor, totalSponsorB2B, totalSponsorB2C,
			totalSpeaker, totalSpeakerB2B, totalSpeakerB2C,
			linkedEvent, linkedEventB2B, linkedEventB2C, following,
			companyLinkedEvents, companyLinkedEventUpcoming,
			companyEventFocusAsOrganizer, companyEventFocusAsSpeaker, companyEventFocusAsSponsor, companyEventFocusAsExhibitor,
			companyParticipationFrequencyB2B, companyParticipationFrequencyB2C,
			companyParticipationActivenessB2B, companyParticipationActivenessB2C
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare ClickHouse batch: %v", err)
	}

	for _, r := range records {
		companyCurrentIntent := r.CompanyCurrentIntent
		if companyCurrentIntent == nil {
			companyCurrentIntent = []string{}
		}
		err := batch.Append(
			r.ID, r.UUID, r.Name, r.Address, r.Description, r.Logo,
			r.CompanyCity, r.CompanyCityName, r.CompanyCityPlaceID, r.CompanyCityURL,
			r.Pincode, r.CompanyState, r.CompanyStateName,
			r.CompanyCountry, r.CompanyCountryName, r.CompanyCountryURL, r.CountryZone,
			r.CompanyWebsite, r.CompanyDomain, r.CompanyTwitter, r.CompanyLinkedin,
			r.CompanyInstagram, r.CompanyFacebook, r.CompanyYoutube,
			r.CompanyPublished, r.Created, r.Modified,
			r.TotalEvents, r.TotalEventsB2B, r.TotalEventsB2C, r.TotalLeads, r.Score, r.Onboarded,
			r.FutureExpectedUpcomingEventCount, companyCurrentIntent, r.TopEventCountryZone,
			r.FirstLogin, r.LastLogin, r.LastEventAdded, r.LastUpdatedAt,
			r.UpcomingEvents, r.UpcomingEventsB2B, r.UpcomingEventsB2C,
			r.TotalExhibit, r.TotalExhibitB2B, r.TotalExhibitB2C,
			r.TotalSponsor, r.TotalSponsorB2B, r.TotalSponsorB2C,
			r.TotalSpeaker, r.TotalSpeakerB2B, r.TotalSpeakerB2C,
			r.LinkedEvent, r.LinkedEventB2B, r.LinkedEventB2C, r.Following,
			r.CompanyLinkedEvents, r.CompanyLinkedEventUpcoming,
			r.CompanyEventFocusAsOrganizer, r.CompanyEventFocusAsSpeaker, r.CompanyEventFocusAsSponsor, r.CompanyEventFocusAsExhibitor,
			r.CompanyParticipationFreqB2B, r.CompanyParticipationFreqB2C,
			r.CompanyParticipationActivenessB2B, r.CompanyParticipationActivenessB2C,
		)
		if err != nil {
			return fmt.Errorf("failed to append record to batch: %v", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send ClickHouse batch: %v", err)
	}

	log.Printf("OK: Inserted %d company records into allCompany_temp", len(records))
	return nil
}
