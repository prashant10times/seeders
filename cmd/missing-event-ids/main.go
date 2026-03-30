package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
)

type Env struct {
	DBUser     string `envconfig:"DB_USER" required:"true"`
	DBPassword string `envconfig:"DB_PASSWORD" required:"true"`
	DBHost     string `envconfig:"DB_HOST" required:"true"`
	DBPort     int    `envconfig:"DB_PORT" required:"true"`
	DBName     string `envconfig:"DB_NAME" required:"true"`

	ClickHouseHost       string `envconfig:"CLICKHOUSE_HOST" required:"true"`
	ClickHousePort       string `envconfig:"CLICKHOUSE_PORT" required:"true"`
	ClickHouseNativePort string `envconfig:"CLICKHOUSE_NATIVE_PORT" required:"true"`
	ClickHouseDB         string `envconfig:"CLICKHOUSE_DB" required:"true"`
	ClickHouseUser       string `envconfig:"CLICKHOUSE_USER" required:"true"`
	ClickHousePassword   string `envconfig:"CLICKHOUSE_PASSWORD" required:"true"`
}

func parseCSVCompanies(csv string) ([]string, error) {
	if strings.TrimSpace(csv) == "" {
		return nil, fmt.Errorf("companies list is empty")
	}
	parts := strings.Split(csv, ",")
	out := make([]string, 0, len(parts))
	seen := map[string]bool{}
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if _, err := strconv.ParseInt(p, 10, 64); err != nil {
			return nil, fmt.Errorf("invalid company id %q (expected number)", p)
		}
		if !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no valid company ids parsed from %q", csv)
	}
	return out, nil
}

func openMySQL(e Env) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=true",
		e.DBUser, e.DBPassword, e.DBHost, e.DBPort, e.DBName,
	)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(15 * time.Minute)
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(25)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func openClickHouse(e Env) (clickhouse.Conn, error) {
	return clickhouse.Open(&clickhouse.Options{
		Addr: []string{e.ClickHouseHost + ":" + e.ClickHousePort},
		Auth: clickhouse.Auth{
			Database: e.ClickHouseDB,
			Username: e.ClickHouseUser,
			Password: e.ClickHousePassword,
		},
		Protocol: clickhouse.HTTP,
		Settings: clickhouse.Settings{
			"max_execution_time": 900,
			"receive_timeout":    900,
			"send_timeout":       900,
		},
		DialTimeout: 30 * time.Second,
		ReadTimeout: 900 * time.Second,
	})
}

func escapeSQLStringLiteral(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func clickHouseEventIDsQuery(companyID string, limit, offset int) string {
	companyID = escapeSQLStringLiteral(companyID)
	return fmt.Sprintf(`
WITH filtered_company_events AS (
	SELECT DISTINCT event_id
	FROM (
		SELECT DISTINCT event_id
		FROM testing_db.event_visitors_ch
		WHERE user_company_id IN ('%s') AND published IN (1, 2)
		UNION ALL
		SELECT DISTINCT event_id
		FROM testing_db.event_speaker_ch
		WHERE user_company_id IN ('%s') AND published IN (1, 2)
		UNION ALL
		SELECT DISTINCT event_id
		FROM testing_db.event_exhibitor_ch
		WHERE company_id IS NOT NULL AND company_id IN ('%s') AND published IN (1, 2)
		UNION ALL
		SELECT DISTINCT event_id
		FROM testing_db.event_sponsors_ch
		WHERE company_id IS NOT NULL AND company_id IN ('%s') AND published IN (1, 2)
		UNION ALL
		SELECT DISTINCT event_id
		FROM testing_db.allevent_ch
		WHERE company_id IS NOT NULL AND company_id IN ('%s') AND edition_type = 'current_edition'
	)
),
preFilterEvent AS (
	SELECT
		e.event_id,
		e.edition_id,
		any(e.venue_id)         AS venue_id,
		any(e.edition_country)  AS edition_country
	FROM testing_db.allevent_ch AS e
	WHERE e.published IN ('2','4','1')
		AND e.edition_type = 'current_edition'
		AND e.edition_functionality IN ('open')
		AND e.editions_audiance_type IN (0,10100,11000)
		AND e.event_id IN (SELECT event_id FROM filtered_company_events)
	GROUP BY e.event_id, e.edition_id
)
SELECT DISTINCT event_id
FROM preFilterEvent
ORDER BY event_id
LIMIT %d OFFSET %d
`, companyID, companyID, companyID, companyID, companyID, limit, offset)
}

func buildMySQLMissingQuery(inputIDs []int64) (string, []any) {
	var b strings.Builder
	args := make([]any, 0, len(inputIDs)+10)

	b.WriteString("SELECT input_ids.event_id\nFROM (\n")
	for i, id := range inputIDs {
		if i == 0 {
			b.WriteString("    SELECT ? AS event_id\n")
		} else {
			b.WriteString("    UNION ALL SELECT ?\n")
		}
		args = append(args, id)
	}
	b.WriteString(") AS input_ids\nWHERE input_ids.event_id NOT IN (\n")
	b.WriteString("    SELECT event_id\n    FROM (\n")
	b.WriteString("        SELECT event AS event_id\n        FROM event_speaker\n        WHERE company_id IN (?) AND published IN (1,2)\n")
	b.WriteString("        UNION\n")
	b.WriteString("        SELECT event\n        FROM event_visitor\n        WHERE company IN (?) AND published IN (1,2)\n")
	b.WriteString("        UNION\n")
	b.WriteString("        SELECT event_id\n        FROM event_exhibitor\n        WHERE company_id IN (?) AND published IN (1,2)\n")
	b.WriteString("        UNION\n")
	b.WriteString("        SELECT event_id\n        FROM event_sponsors\n        WHERE company_id IN (?) AND published IN (1,2)\n")
	b.WriteString("        UNION\n")
	b.WriteString("        SELECT e.id AS event_id\n")
	b.WriteString("        FROM event_edition ee\n")
	b.WriteString("        JOIN event e ON ee.id = e.event_edition\n")
	b.WriteString("        WHERE ee.company_id IN (?)\n")
	b.WriteString("          AND (e.status IS NULL OR e.status IN ('P','C','U','A'))\n")
	b.WriteString("          AND e.published IN (1,2)\n")
	b.WriteString("          AND e.functionality = 'open'\n")
	b.WriteString("    ) AS unioned_events\n")
	b.WriteString(")\n")

	return b.String(), args
}

func formatSQLValue(v any) string {
	switch t := v.(type) {
	case nil:
		return "NULL"
	case int:
		return strconv.FormatInt(int64(t), 10)
	case int32:
		return strconv.FormatInt(int64(t), 10)
	case int64:
		return strconv.FormatInt(t, 10)
	case uint32:
		return strconv.FormatUint(uint64(t), 10)
	case uint64:
		return strconv.FormatUint(t, 10)
	case string:
		return "'" + escapeSQLStringLiteral(t) + "'"
	default:
		// Best-effort for logging only.
		return "'" + escapeSQLStringLiteral(fmt.Sprintf("%v", t)) + "'"
	}
}

func renderMySQLQueryForLog(query string, args []any) string {
	// Logging-only interpolation: replaces each '?' with formatted arg.
	// Do NOT use the result for execution.
	if len(args) == 0 || !strings.Contains(query, "?") {
		return query
	}
	var b strings.Builder
	b.Grow(len(query) + len(args)*4)
	argIdx := 0
	for i := 0; i < len(query); i++ {
		if query[i] == '?' && argIdx < len(args) {
			b.WriteString(formatSQLValue(args[argIdx]))
			argIdx++
			continue
		}
		b.WriteByte(query[i])
	}
	if argIdx < len(args) {
		b.WriteString("\n-- WARNING: not all args were consumed by placeholder replacement\n")
	}
	return b.String()
}

func main() {
	var (
		envFile   = flag.String("env-file", ".env.missing_event_ids", "path to env file for this script")
		companies = flag.String("companies", "1104480,6491,36271,1128748", "comma-separated MySQL company ids")
		batchSize = flag.Int("batch", 1000, "clickhouse limit per page")
		outDir    = flag.String("out-dir", "logs", "directory for output logs")
	)
	flag.Parse()

	if err := godotenv.Load(*envFile); err != nil {
		log.Fatalf("failed to load env file %q: %v", *envFile, err)
	}

	var e Env
	if err := envconfig.Process("", &e); err != nil {
		log.Fatalf("failed to process env vars: %v", err)
	}

	companyIDs, err := parseCSVCompanies(*companies)
	if err != nil {
		log.Fatal(err)
	}

	if err := os.MkdirAll(*outDir, 0755); err != nil {
		log.Fatalf("failed to create out dir %q: %v", *outDir, err)
	}

	mysqlDB, err := openMySQL(e)
	if err != nil {
		log.Fatalf("mysql connection failed: %v", err)
	}
	defer mysqlDB.Close()

	ch, err := openClickHouse(e)
	if err != nil {
		log.Fatalf("clickhouse connection failed: %v", err)
	}

	for _, companyID := range companyIDs {
		logPath := filepath.Join(*outDir, fmt.Sprintf("missing_event_ids_%s.log", companyID))
		f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			log.Fatalf("failed to open log file %q: %v", logPath, err)
		}

		logger := log.New(f, "", log.LstdFlags)
		logger.Printf("company_id=%s", companyID)
		logger.Printf("started_at=%s", time.Now().Format(time.RFC3339))
		log.Printf("processing company_id=%s -> %s", companyID, logPath)

		offset := 0
		totalMissing := 0
		seenMissing := map[int64]struct{}{}
		allMissing := make([]int64, 0, 128)
		for {
			chQuery := clickHouseEventIDsQuery(companyID, *batchSize, offset)
			logger.Printf("----- CLICKHOUSE QUERY START (company_id=%s offset=%d limit=%d) -----\n%s\n----- CLICKHOUSE QUERY END -----", companyID, offset, *batchSize, strings.TrimSpace(chQuery))

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			rows, err := ch.Query(ctx, chQuery)
			cancel()
			if err != nil {
				logger.Printf("ERROR clickhouse query failed (offset=%d, limit=%d): %v", offset, *batchSize, err)
				break
			}

			batchIDs := make([]int64, 0, *batchSize)
			for rows.Next() {
				var id uint32
				if err := rows.Scan(&id); err != nil {
					logger.Printf("ERROR clickhouse scan failed (offset=%d): %v", offset, err)
					_ = rows.Close()
					batchIDs = nil // force stop for this company
					goto companyDone
				}
				if uint64(id) > math.MaxInt64 {
					logger.Printf("ERROR event_id overflow (offset=%d): %d", offset, id)
					_ = rows.Close()
					batchIDs = nil
					goto companyDone
				}
				batchIDs = append(batchIDs, int64(id))
			}
			_ = rows.Close()

			if len(batchIDs) == 0 {
				logger.Printf("no more clickhouse rows; stopping at offset=%d", offset)
				break
			}

			query, args := buildMySQLMissingQuery(batchIDs)
			companyIDNum, _ := strconv.ParseInt(companyID, 10, 64)
			args = append(args, companyIDNum, companyIDNum, companyIDNum, companyIDNum, companyIDNum)

			logger.Printf("----- MYSQL QUERY TEMPLATE START (offset=%d batch=%d) -----\n%s\n----- MYSQL QUERY TEMPLATE END -----", offset, len(batchIDs), strings.TrimSpace(query))
			logger.Printf("----- MYSQL QUERY FINAL START (offset=%d batch=%d) -----\n%s\n----- MYSQL QUERY FINAL END -----", offset, len(batchIDs), strings.TrimSpace(renderMySQLQueryForLog(query, args)))

			ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Minute)
			mysqlRows, err := mysqlDB.QueryContext(ctx2, query, args...)
			cancel2()
			if err != nil {
				logger.Printf("ERROR mysql query failed (offset=%d, batch=%d): %v", offset, len(batchIDs), err)
				break
			}

			missing := make([]int64, 0)
			for mysqlRows.Next() {
				var id int64
				if err := mysqlRows.Scan(&id); err != nil {
					logger.Printf("ERROR mysql scan failed: %v", err)
					break
				}
				missing = append(missing, id)
			}
			_ = mysqlRows.Close()

			if len(missing) > 0 {
				logger.Printf("offset=%d limit=%d missing_count=%d", offset, *batchSize, len(missing))
				for _, id := range missing {
					logger.Printf("missing_event_id=%d", id)
					if _, ok := seenMissing[id]; !ok {
						seenMissing[id] = struct{}{}
						allMissing = append(allMissing, id)
					}
				}
				totalMissing += len(missing)
			} else {
				logger.Printf("offset=%d limit=%d missing_count=0", offset, *batchSize)
			}

			offset += *batchSize
		}

	companyDone:
		if len(allMissing) > 0 {
			parts := make([]string, 0, len(allMissing))
			for _, id := range allMissing {
				parts = append(parts, strconv.FormatInt(id, 10))
			}
			logger.Printf("missing_event_ids_csv=%s", strings.Join(parts, ","))
		} else {
			logger.Printf("missing_event_ids_csv=")
		}
		logger.Printf("finished_at=%s total_missing=%d", time.Now().Format(time.RFC3339), totalMissing)
		_ = f.Close()
		log.Printf("completed company_id=%s total_missing=%d", companyID, totalMissing)
	}
}
