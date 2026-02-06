package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/klauspost/compress/gzip"
	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"
)

type Config struct {
	DatabaseURL                     string
	BaseURL                         string
	DataDir                         string
	SQLDir                          string
	DatasetDate                     string
	SchemaVersion                   int
	EnablePGSearch                  bool
	RunETL                          bool
	ForceDownload                   bool
	KeepStaging                     bool
	LoadBatchSize                   int
	BatchSize                       int
	MaxActors                       int
	MaxProducers                    int
	MaxWriters                      int
	MaxDirectors                    int
	MaxParallelWorkers              string
	WorkMem                         string
	MaintenanceWorkMem              string
	Port                            string
	ReaderBufferSize                int
	DownloadConcurrency             int
	MinNumVotes                     int
	DBMaxWalSize                    string
	DBMinWalSize                    string
	DBCheckpointTimeout             string
	DBCheckpointCompletionTarget    string
	DBWalCompression                string
	DBMaxParallelWorkers            string
	DBMaxParallelMaintenanceWorkers string
	ScheduleEnabled                 bool
	PollInterval                    time.Duration
	BootstrapBlocking               bool
	ForceRebuild                    bool
	SwapLockTimeout                 string
}

type TitleBasics struct {
	Tconst         string   `json:"tconst"`
	TitleType      string   `json:"titleType"`
	PrimaryTitle   string   `json:"primaryTitle"`
	OriginalTitle  string   `json:"originalTitle"`
	IsAdult        bool     `json:"isAdult"`
	StartYear      *int     `json:"startYear"`
	EndYear        *int     `json:"endYear"`
	RuntimeMinutes *int     `json:"runtimeMinutes"`
	Genres         []string `json:"genres"`
}

type Rating struct {
	AverageRating *float64 `json:"averageRating"`
	NumVotes      *int     `json:"numVotes"`
}

type Aka struct {
	Title           string   `json:"title"`
	Language        string   `json:"language,omitempty"`
	Types           []string `json:"types,omitempty"`
	Attributes      []string `json:"attributes,omitempty"`
	IsOriginalTitle bool     `json:"isOriginalTitle"`
	Ordering        int      `json:"ordering,omitempty"`
}

type CastMember struct {
	Nconst     string   `json:"nconst"`
	Name       string   `json:"name"`
	Category   string   `json:"category"`
	Characters []string `json:"characters"`
}

type CrewMember struct {
	Nconst string `json:"nconst"`
	Name   string `json:"name"`
}

type Producer struct {
	Nconst   string `json:"nconst"`
	Name     string `json:"name"`
	Category string `json:"category"`
}

type TitleCrew struct {
	Directors []CrewMember `json:"directors"`
	Writers   []CrewMember `json:"writers"`
	Producers []Producer   `json:"producers"`
}

type Episode struct {
	Tconst        string   `json:"tconst"`
	EpisodeNumber *int     `json:"episodeNumber"`
	PrimaryTitle  string   `json:"primaryTitle"`
	StartYear     *int     `json:"startYear"`
	AverageRating *float64 `json:"averageRating"`
	NumVotes      *int     `json:"numVotes"`
}

type Season struct {
	SeasonNumber *int      `json:"seasonNumber"`
	Episodes     []Episode `json:"episodes"`
}

type TitleData struct {
	Basics   TitleBasics      `json:"basics"`
	Akas     map[string][]Aka `json:"akas"`
	Ratings  Rating           `json:"ratings"`
	Cast     []CastMember     `json:"cast"`
	Crew     TitleCrew        `json:"crew"`
	Episodes []Season         `json:"episodes"`
}

type principalRow struct {
	Tconst     string
	Ordering   int
	Nconst     string
	Category   string
	Characters string
}

type crewLists struct {
	Directors []string
	Writers   []string
}

type seasonKey struct {
	HasValue bool
	Value    int
}

type imdbFileState struct {
	FileName      string
	LastModified  time.Time
	ContentLength int64
}

var imdbDatasetFiles = []string{
	"title.basics.tsv.gz",
	"title.akas.tsv.gz",
	"title.ratings.tsv.gz",
	"title.principals.tsv.gz",
	"title.crew.tsv.gz",
	"title.episode.tsv.gz",
	"name.basics.tsv.gz",
}

const etlSchedulerLockKey int64 = 573901235911

func envString(key, def string) string {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return def
	}
	return val
}

func envBool(key string, def bool) bool {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return def
	}
	switch strings.ToLower(val) {
	case "1", "true", "yes", "y":
		return true
	case "0", "false", "no", "n":
		return false
	default:
		return def
	}
}

func envInt(key string, def int) int {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return def
	}
	parsed, err := strconv.Atoi(val)
	if err != nil {
		return def
	}
	return parsed
}

func envDuration(key string, def time.Duration) time.Duration {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return def
	}
	dur, err := time.ParseDuration(val)
	if err != nil {
		return def
	}
	return dur
}

func resolveSQLDir(envVal string) (string, error) {
	if envVal != "" {
		return envVal, nil
	}
	if _, err := os.Stat("etl"); err == nil {
		return "etl", nil
	}
	if _, err := os.Stat("../etl"); err == nil {
		return "../etl", nil
	}
	return "", errors.New("ETL SQL dir not found; set ETL_SQL_DIR")
}

func redactURL(raw string) string {
	schemeIdx := strings.Index(raw, "://")
	if schemeIdx == -1 {
		return raw
	}
	at := strings.LastIndex(raw, "@")
	if at == -1 {
		return raw
	}
	creds := raw[schemeIdx+3 : at]
	parts := strings.SplitN(creds, ":", 2)
	if len(parts) != 2 {
		return raw
	}
	return raw[:schemeIdx+3] + parts[0] + ":<redacted>@" + raw[at+1:]
}

func parseInt(raw string) *int {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "\\N" {
		return nil
	}
	val, err := strconv.Atoi(raw)
	if err != nil {
		return nil
	}
	return &val
}

func parseFloat(raw string) *float64 {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "\\N" {
		return nil
	}
	val, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return nil
	}
	return &val
}

func splitList(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "\\N" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func parseCharacters(raw string) []string {
	if raw == "" || raw == "\\N" {
		return nil
	}
	var out []string
	if err := json.Unmarshal([]byte(raw), &out); err == nil {
		return out
	}
	return []string{raw}
}

func normalizeCharacters(raw string) []string {
	chars := parseCharacters(raw)
	if len(chars) == 0 {
		return nil
	}
	out := make([]string, 0, len(chars))
	for _, ch := range chars {
		if ch == "" {
			continue
		}
		parts := strings.Split(ch, ",")
		if len(parts) == 1 {
			ch = strings.TrimSpace(ch)
			if ch != "" {
				out = append(out, ch)
			}
			continue
		}
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			out = append(out, part)
		}
	}
	return dedupeCharacters(out)
}

func toJSONB(v any) (json.RawMessage, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(data), nil
}

func intOrNil(val *int) any {
	if val == nil {
		return nil
	}
	return *val
}

func floatOrNil(val *float64) any {
	if val == nil {
		return nil
	}
	return *val
}

func dedupeStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func fieldByIndex(line string, index int) (string, bool) {
	if index < 0 {
		return "", false
	}
	start := 0
	for i := 0; i <= len(line); i++ {
		if i == len(line) || line[i] == '\t' {
			if index == 0 {
				return line[start:i], true
			}
			index--
			start = i + 1
		}
	}
	return "", false
}

func parseTconstID(raw string) (uint32, bool) {
	raw = strings.TrimSpace(raw)
	if len(raw) < 3 {
		return 0, false
	}
	if (raw[0] != 't' && raw[0] != 'T') || (raw[1] != 't' && raw[1] != 'T') {
		return 0, false
	}
	var val uint64
	for i := 2; i < len(raw); i++ {
		ch := raw[i]
		if ch < '0' || ch > '9' {
			return 0, false
		}
		val = val*10 + uint64(ch-'0')
		if val > uint64(^uint32(0)) {
			return 0, false
		}
	}
	return uint32(val), true
}

func parseNconstID(raw string) (uint32, bool) {
	raw = strings.TrimSpace(raw)
	if len(raw) < 3 {
		return 0, false
	}
	if (raw[0] != 'n' && raw[0] != 'N') || (raw[1] != 'm' && raw[1] != 'M') {
		return 0, false
	}
	var val uint64
	for i := 2; i < len(raw); i++ {
		ch := raw[i]
		if ch < '0' || ch > '9' {
			return 0, false
		}
		val = val*10 + uint64(ch-'0')
		if val > uint64(^uint32(0)) {
			return 0, false
		}
	}
	return uint32(val), true
}

func akaTypeRank(aka Aka) int {
	for _, typ := range aka.Types {
		if strings.EqualFold(typ, "imdbDisplay") {
			return 0
		}
	}
	return 1
}

func akaOrderingOrMax(ordering int) int {
	if ordering > 0 {
		return ordering
	}
	return int(^uint(0) >> 1)
}

func sameTitle(a string, b string) bool {
	a = strings.TrimSpace(a)
	b = strings.TrimSpace(b)
	if a == "" || b == "" {
		return false
	}
	return strings.EqualFold(a, b)
}

func filterAkas(akas map[string][]Aka, primaryTitle string, originalTitle string) map[string][]Aka {
	if len(akas) == 0 {
		return akas
	}
	if strings.TrimSpace(primaryTitle) == "" && strings.TrimSpace(originalTitle) == "" {
		return akas
	}
	out := make(map[string][]Aka, len(akas))
	for region, list := range akas {
		filtered := make([]Aka, 0, len(list))
		for _, aka := range list {
			if sameTitle(aka.Title, primaryTitle) || sameTitle(aka.Title, originalTitle) {
				continue
			}
			filtered = append(filtered, aka)
		}
		if len(filtered) > 0 {
			out[region] = filtered
		}
	}
	return out
}

func seasonKeyFromPtr(val *int) seasonKey {
	if val == nil {
		return seasonKey{}
	}
	return seasonKey{HasValue: true, Value: *val}
}

func clampNonNegative(val int) int {
	if val < 0 {
		return 0
	}
	return val
}

func clampPositive(val int, def int) int {
	if val <= 0 {
		return def
	}
	return val
}

func waitForDB(ctx context.Context, url string, logger *log.Logger) (*pgxpool.Pool, error) {
	const (
		maxAttempts = 10
		backoff     = 5 * time.Second
	)
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		pool, err := pgxpool.New(ctx, url)
		if err == nil {
			pingErr := pool.Ping(ctx)
			if pingErr == nil {
				return pool, nil
			}
			pool.Close()
			lastErr = pingErr
		} else {
			lastErr = err
		}
		logger.Printf("db: not ready (attempt %d/%d): %v", attempt, maxAttempts, lastErr)
		time.Sleep(backoff)
	}
	return nil, fmt.Errorf("db: not ready after retries: %w", lastErr)
}
func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)

	cfg, err := loadConfig()
	if err != nil {
		logger.Fatalf("config: %v", err)
	}

	logger.Printf("api: starting with DATABASE_URL=%s", redactURL(cfg.DatabaseURL))

	ctx := context.Background()
	pool, err := waitForDB(ctx, cfg.DatabaseURL, logger)
	if err != nil {
		logger.Fatalf("%v", err)
	}
	defer pool.Close()

	if cfg.RunETL {
		if cfg.ScheduleEnabled {
			needsBootstrap, err := shouldRunBlockingBootstrap(ctx, pool)
			if err != nil {
				logger.Fatalf("etl bootstrap check failed: %v", err)
			}
			if needsBootstrap && cfg.BootstrapBlocking {
				logger.Printf("etl: bootstrap run started (titles table is empty)")
				if err := runScheduledRebuildCycle(ctx, pool, cfg, logger); err != nil {
					logger.Fatalf("etl bootstrap failed: %v", err)
				}
			} else if needsBootstrap {
				logger.Printf("etl: bootstrap run deferred to scheduler (ETL_BOOTSTRAP_BLOCKING=false)")
			}
			startETLScheduler(ctx, pool, cfg, logger)
		} else {
			if err := runETL(ctx, pool, cfg, logger); err != nil {
				logger.Fatalf("etl failed: %v", err)
			}
		}
	} else {
		logger.Printf("etl: skipped (RUN_ETL=false)")
	}

	if err := runServer(pool, cfg, logger); err != nil {
		logger.Fatalf("api: %v", err)
	}
}

func loadConfig() (Config, error) {
	cfg := Config{}
	cfg.DatabaseURL = envString("DATABASE_URL", "")
	if cfg.DatabaseURL == "" {
		return cfg, errors.New("DATABASE_URL is required")
	}

	cfg.BaseURL = envString("IMDB_BASE_URL", "https://datasets.imdbws.com")
	cfg.DataDir = envString("IMDB_DATA_DIR", "/data")
	cfg.DatasetDate = envString("DATASET_DATE", time.Now().UTC().Format("2006-01-02"))
	cfg.SchemaVersion = envInt("SCHEMA_VERSION", 1)
	cfg.EnablePGSearch = envBool("ENABLE_PG_SEARCH", true)
	cfg.RunETL = envBool("RUN_ETL", true)
	cfg.ForceDownload = envBool("IMDB_FORCE_DOWNLOAD", false)
	cfg.KeepStaging = envBool("ETL_KEEP_STAGING", false)
	cfg.LoadBatchSize = clampPositive(envInt("ETL_LOAD_BATCH_SIZE", 10000), 10000)
	cfg.BatchSize = clampPositive(envInt("ETL_BATCH_SIZE", 10000), 10000)
	cfg.MaxActors = clampNonNegative(envInt("ETL_MAX_ACTORS", 10))
	cfg.MaxProducers = clampNonNegative(envInt("ETL_MAX_PRODUCERS", 1))
	cfg.MaxWriters = clampNonNegative(envInt("ETL_MAX_WRITERS", 1))
	cfg.MaxDirectors = clampNonNegative(envInt("ETL_MAX_DIRECTORS", 1))
	cfg.MaxParallelWorkers = strings.TrimSpace(os.Getenv("ETL_MAX_PARALLEL_WORKERS"))
	cfg.WorkMem = strings.TrimSpace(os.Getenv("ETL_WORK_MEM"))
	cfg.MaintenanceWorkMem = strings.TrimSpace(os.Getenv("ETL_MAINTENANCE_WORK_MEM"))
	cfg.ReaderBufferSize = clampPositive(envInt("ETL_READER_BUFFER", 256*1024), 256*1024)
	cfg.DownloadConcurrency = clampPositive(envInt("ETL_DOWNLOAD_CONCURRENCY", 3), 3)
	cfg.MinNumVotes = clampNonNegative(envInt("ETL_MIN_NUMVOTES", 1))
	cfg.DBMaxWalSize = strings.TrimSpace(os.Getenv("ETL_DB_MAX_WAL_SIZE"))
	cfg.DBMinWalSize = strings.TrimSpace(os.Getenv("ETL_DB_MIN_WAL_SIZE"))
	cfg.DBCheckpointTimeout = strings.TrimSpace(os.Getenv("ETL_DB_CHECKPOINT_TIMEOUT"))
	cfg.DBCheckpointCompletionTarget = strings.TrimSpace(os.Getenv("ETL_DB_CHECKPOINT_COMPLETION_TARGET"))
	cfg.DBWalCompression = strings.TrimSpace(os.Getenv("ETL_DB_WAL_COMPRESSION"))
	cfg.DBMaxParallelWorkers = strings.TrimSpace(os.Getenv("ETL_DB_MAX_PARALLEL_WORKERS"))
	cfg.DBMaxParallelMaintenanceWorkers = strings.TrimSpace(os.Getenv("ETL_DB_MAX_PARALLEL_MAINTENANCE_WORKERS"))
	cfg.ScheduleEnabled = envBool("ETL_SCHEDULE_ENABLED", true)
	cfg.PollInterval = envDuration("ETL_POLL_INTERVAL", time.Hour)
	if cfg.PollInterval <= 0 {
		return cfg, errors.New("ETL_POLL_INTERVAL must be greater than 0")
	}
	cfg.BootstrapBlocking = envBool("ETL_BOOTSTRAP_BLOCKING", true)
	cfg.ForceRebuild = envBool("IMDB_FORCE_REBUILD", false)
	cfg.SwapLockTimeout = envString("ETL_SWAP_LOCK_TIMEOUT", "30s")
	if strings.Contains(cfg.SwapLockTimeout, "'") || strings.Contains(cfg.SwapLockTimeout, ";") {
		return cfg, errors.New("ETL_SWAP_LOCK_TIMEOUT contains invalid character")
	}
	cfg.Port = envString("PORT", "8000")

	sqlDir, err := resolveSQLDir(os.Getenv("ETL_SQL_DIR"))
	if err != nil {
		return cfg, err
	}
	cfg.SQLDir = sqlDir

	return cfg, nil
}

func runETL(ctx context.Context, pool *pgxpool.Pool, cfg Config, logger *log.Logger) error {
	logger.Printf("etl: starting")
	logger.Printf(
		"etl: config base_url=%s data_dir=%s sql_dir=%s dataset_date=%s schema_version=%d search=%v force_download=%v load_batch=%d build_batch=%d reader_buf=%d download_workers=%d min_votes=%d",
		cfg.BaseURL, cfg.DataDir, cfg.SQLDir, cfg.DatasetDate, cfg.SchemaVersion, cfg.EnablePGSearch, cfg.ForceDownload, cfg.LoadBatchSize, cfg.BatchSize,
		cfg.ReaderBufferSize, cfg.DownloadConcurrency, cfg.MinNumVotes,
	)

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("data dir: %w", err)
	}

	if err := downloadDatasets(ctx, cfg, logger); err != nil {
		return err
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire conn: %w", err)
	}
	if err := applySystemSettings(ctx, conn, cfg, logger); err != nil {
		conn.Release()
		return err
	}
	conn.Release()

	if err := runSQLFile(ctx, pool, cfg, logger, "schema.sql"); err != nil {
		return err
	}

	if err := runSQLFile(ctx, pool, cfg, logger, "staging.sql"); err != nil {
		return err
	}

	if err := loadStagingInBatches(ctx, pool, cfg, logger); err != nil {
		return err
	}

	scripts := []string{
		"index_staging.sql",
		"episodes_enriched.sql",
		"filter_titles.sql",
		"create_next.sql",
	}
	for _, script := range scripts {
		if err := runSQLFile(ctx, pool, cfg, logger, script); err != nil {
			return err
		}
	}

	if err := buildTitlesInBatches(ctx, pool, cfg, logger); err != nil {
		return err
	}

	postScripts := []string{
		"discover_next.sql",
		"indexes_next.sql",
		"analyze.sql",
	}
	if cfg.EnablePGSearch {
		postScripts = append(postScripts, "pg_search_next.sql")
	}
	postScripts = append(postScripts,
		"swap.sql",
		"analyze_final.sql",
	)
	if cfg.KeepStaging {
		logger.Printf("etl: cleanup skipped (ETL_KEEP_STAGING=true)")
	} else {
		postScripts = append(postScripts, "cleanup.sql")
	}
	for _, script := range postScripts {
		if err := runSQLFile(ctx, pool, cfg, logger, script); err != nil {
			return err
		}
	}

	logger.Printf("etl: finished")
	return nil
}

func runSQLFile(ctx context.Context, pool *pgxpool.Pool, cfg Config, logger *log.Logger, filename string) error {
	path := filepath.Join(cfg.SQLDir, filename)
	sqlBytes, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", filename, err)
	}

	sqlText := string(sqlBytes)
	sqlText = strings.ReplaceAll(sqlText, "{{data_dir}}", cfg.DataDir)
	sqlText = strings.ReplaceAll(sqlText, "{{dataset_date}}", cfg.DatasetDate)
	sqlText = strings.ReplaceAll(sqlText, "{{schema_version}}", strconv.Itoa(cfg.SchemaVersion))
	sqlText = strings.ReplaceAll(sqlText, "{{min_num_votes}}", strconv.Itoa(cfg.MinNumVotes))
	sqlText = strings.ReplaceAll(sqlText, "{{swap_lock_timeout}}", cfg.SwapLockTimeout)

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Release()

	if err := applyETLSettings(ctx, conn, cfg); err != nil {
		return err
	}
	if err := logETLSettings(ctx, conn, logger); err != nil {
		return err
	}

	logger.Printf("etl: running %s", filename)
	start := time.Now()
	if _, err := conn.Exec(ctx, sqlText); err != nil {
		return fmt.Errorf("%s failed: %w", filename, err)
	}
	logger.Printf("etl: finished %s in %s", filename, time.Since(start).Truncate(time.Millisecond))
	return nil
}

func applyETLSettings(ctx context.Context, conn *pgxpool.Conn, cfg Config) error {
	if cfg.MaxParallelWorkers != "" {
		if _, err := strconv.Atoi(cfg.MaxParallelWorkers); err != nil {
			return fmt.Errorf("ETL_MAX_PARALLEL_WORKERS invalid: %w", err)
		}
		if _, err := conn.Exec(ctx, "SET max_parallel_workers_per_gather = "+cfg.MaxParallelWorkers); err != nil {
			return fmt.Errorf("set max_parallel_workers_per_gather: %w", err)
		}
	}

	if cfg.WorkMem != "" {
		if strings.Contains(cfg.WorkMem, "'") {
			return errors.New("ETL_WORK_MEM contains invalid character")
		}
		if _, err := conn.Exec(ctx, "SET work_mem = '"+cfg.WorkMem+"'"); err != nil {
			return fmt.Errorf("set work_mem: %w", err)
		}
	}

	if cfg.MaintenanceWorkMem != "" {
		if strings.Contains(cfg.MaintenanceWorkMem, "'") {
			return errors.New("ETL_MAINTENANCE_WORK_MEM contains invalid character")
		}
		if _, err := conn.Exec(ctx, "SET maintenance_work_mem = '"+cfg.MaintenanceWorkMem+"'"); err != nil {
			return fmt.Errorf("set maintenance_work_mem: %w", err)
		}
	}

	return nil
}

func logETLSettings(ctx context.Context, conn *pgxpool.Conn, logger *log.Logger) error {
	var workMem, maintenanceMem, mpwg string
	err := conn.QueryRow(ctx, `
SELECT current_setting('work_mem'),
       current_setting('maintenance_work_mem'),
       current_setting('max_parallel_workers_per_gather')`).Scan(&workMem, &maintenanceMem, &mpwg)
	if err != nil {
		return fmt.Errorf("read etl settings: %w", err)
	}
	logger.Printf("etl: session settings work_mem=%s maintenance_work_mem=%s max_parallel_workers_per_gather=%s", workMem, maintenanceMem, mpwg)
	return nil
}

func applySystemSettings(ctx context.Context, conn *pgxpool.Conn, cfg Config, logger *log.Logger) error {
	setting := func(name string, value string) error {
		if value == "" {
			return nil
		}
		if strings.Contains(value, "'") || strings.Contains(value, ";") {
			return fmt.Errorf("%s contains invalid character", name)
		}
		_, err := conn.Exec(ctx, fmt.Sprintf("ALTER SYSTEM SET %s = '%s'", name, value))
		if err != nil {
			return fmt.Errorf("alter system %s: %w", name, err)
		}
		return nil
	}

	if err := setting("max_wal_size", cfg.DBMaxWalSize); err != nil {
		return err
	}
	if err := setting("min_wal_size", cfg.DBMinWalSize); err != nil {
		return err
	}
	if err := setting("checkpoint_timeout", cfg.DBCheckpointTimeout); err != nil {
		return err
	}
	if err := setting("checkpoint_completion_target", cfg.DBCheckpointCompletionTarget); err != nil {
		return err
	}
	if err := setting("wal_compression", cfg.DBWalCompression); err != nil {
		return err
	}
	if err := setting("max_parallel_workers", cfg.DBMaxParallelWorkers); err != nil {
		return err
	}
	if err := setting("max_parallel_maintenance_workers", cfg.DBMaxParallelMaintenanceWorkers); err != nil {
		return err
	}

	if _, err := conn.Exec(ctx, "SELECT pg_reload_conf()"); err != nil {
		return fmt.Errorf("reload conf: %w", err)
	}
	logger.Printf("etl: applied postgres settings via ALTER SYSTEM")
	return nil
}

func startETLScheduler(ctx context.Context, pool *pgxpool.Pool, cfg Config, logger *log.Logger) {
	logger.Printf("etl: scheduler enabled poll_interval=%s", cfg.PollInterval)
	go func() {
		if err := runScheduledRebuildCycle(ctx, pool, cfg, logger); err != nil {
			logger.Printf("etl: scheduler cycle failed: %v", err)
		}

		ticker := time.NewTicker(cfg.PollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := runScheduledRebuildCycle(ctx, pool, cfg, logger); err != nil {
					logger.Printf("etl: scheduler cycle failed: %v", err)
				}
			}
		}
	}()
}

func runScheduledRebuildCycle(ctx context.Context, pool *pgxpool.Pool, cfg Config, logger *log.Logger) error {
	manifest, err := probeIMDbFileStates(ctx, cfg)
	if err != nil {
		return fmt.Errorf("probe imdb headers: %w", err)
	}

	prevState, err := loadSourceState(ctx, pool)
	if err != nil {
		return fmt.Errorf("load etl source state: %w", err)
	}

	if cfg.ForceRebuild {
		logger.Printf("etl: force rebuild enabled (IMDB_FORCE_REBUILD=true)")
	}
	if !cfg.ForceRebuild {
		changedCount, allUpdated := compareStateSets(prevState, manifest)
		if len(prevState) == 0 {
			logger.Printf("etl: no previous source state; full rebuild required")
		} else if !allUpdated {
			logger.Printf("etl: skip rebuild changed_files=%d/%d (requires all files changed)", changedCount, len(imdbDatasetFiles))
			return nil
		}
	}

	lockConn, locked, err := acquireETLSchedulerLock(ctx, pool)
	if err != nil {
		return fmt.Errorf("acquire etl lock: %w", err)
	}
	if !locked {
		logger.Printf("etl: skip rebuild (lock held by another instance)")
		return nil
	}
	defer releaseETLSchedulerLock(lockConn)

	if !cfg.ForceRebuild {
		prevState, err = loadSourceState(ctx, pool)
		if err != nil {
			return fmt.Errorf("reload etl source state: %w", err)
		}
		changedCount, allUpdated := compareStateSets(prevState, manifest)
		if len(prevState) > 0 && !allUpdated {
			logger.Printf("etl: skip rebuild after lock changed_files=%d/%d", changedCount, len(imdbDatasetFiles))
			return nil
		}
	}

	rebuildCfg := cfg
	rebuildCfg.DatasetDate = datasetDateFromState(manifest)
	rebuildCfg.ForceDownload = true

	logger.Printf("etl: scheduled rebuild triggered dataset_date=%s", rebuildCfg.DatasetDate)
	if err := runETL(ctx, pool, rebuildCfg, logger); err != nil {
		return err
	}

	if err := saveSourceState(ctx, pool, manifest); err != nil {
		return fmt.Errorf("persist source state: %w", err)
	}
	logger.Printf("etl: scheduled rebuild completed")
	return nil
}

func shouldRunBlockingBootstrap(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	hasRows, err := hasAnyTitles(ctx, pool)
	if err != nil {
		return false, err
	}
	return !hasRows, nil
}

func hasAnyTitles(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	hasTable, err := tableExists(ctx, pool, "public.titles")
	if err != nil {
		return false, err
	}
	if !hasTable {
		return false, nil
	}

	var exists bool
	err = pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM titles LIMIT 1)`).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func tableExists(ctx context.Context, pool *pgxpool.Pool, qualifiedName string) (bool, error) {
	var exists bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, qualifiedName).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

func probeIMDbFileStates(ctx context.Context, cfg Config) (map[string]imdbFileState, error) {
	baseURL := strings.TrimRight(cfg.BaseURL, "/")
	client := &http.Client{Timeout: 30 * time.Second}
	state := make(map[string]imdbFileState, len(imdbDatasetFiles))
	for _, file := range imdbDatasetFiles {
		url := baseURL + "/" + file
		req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
		if err != nil {
			return nil, fmt.Errorf("%s: build request: %w", file, err)
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("%s: head request: %w", file, err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("%s: head status %s", file, resp.Status)
		}

		lastModifiedRaw := strings.TrimSpace(resp.Header.Get("Last-Modified"))
		if lastModifiedRaw == "" {
			return nil, fmt.Errorf("%s: missing Last-Modified header on 200 response", file)
		}

		contentLengthRaw := strings.TrimSpace(resp.Header.Get("Content-Length"))
		if contentLengthRaw == "" {
			return nil, fmt.Errorf("%s: missing Content-Length header on 200 response", file)
		}

		lastModified, err := http.ParseTime(lastModifiedRaw)
		if err != nil {
			return nil, fmt.Errorf("%s: parse Last-Modified: %w", file, err)
		}

		contentLength, err := strconv.ParseInt(contentLengthRaw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%s: parse Content-Length: %w", file, err)
		}
		if contentLength < 0 {
			return nil, fmt.Errorf("%s: invalid Content-Length %d", file, contentLength)
		}

		state[file] = imdbFileState{
			FileName:      file,
			LastModified:  lastModified.UTC(),
			ContentLength: contentLength,
		}
	}
	return state, nil
}

func compareStateSets(previous map[string]imdbFileState, current map[string]imdbFileState) (int, bool) {
	if len(previous) == 0 || len(current) == 0 {
		return 0, false
	}
	changedCount := 0
	for _, file := range imdbDatasetFiles {
		prev, okPrev := previous[file]
		curr, okCurr := current[file]
		if !okPrev || !okCurr {
			return changedCount, false
		}
		if prev.LastModified.Equal(curr.LastModified) && prev.ContentLength == curr.ContentLength {
			continue
		}
		changedCount++
	}
	return changedCount, changedCount == len(imdbDatasetFiles)
}

func datasetDateFromState(state map[string]imdbFileState) string {
	if len(state) == 0 {
		return time.Now().UTC().Format("2006-01-02")
	}
	maxTime := time.Time{}
	for _, file := range imdbDatasetFiles {
		entry, ok := state[file]
		if !ok {
			continue
		}
		if entry.LastModified.After(maxTime) {
			maxTime = entry.LastModified
		}
	}
	if maxTime.IsZero() {
		maxTime = time.Now().UTC()
	}
	return maxTime.UTC().Format("2006-01-02")
}

func loadSourceState(ctx context.Context, pool *pgxpool.Pool) (map[string]imdbFileState, error) {
	exists, err := tableExists(ctx, pool, "public.etl_source_state")
	if err != nil {
		return nil, err
	}
	if !exists {
		return map[string]imdbFileState{}, nil
	}

	rows, err := pool.Query(ctx, `
SELECT file_name, last_modified, content_length
FROM etl_source_state`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	state := make(map[string]imdbFileState)
	for rows.Next() {
		var (
			fileName      string
			lastModified  time.Time
			contentLength int64
		)
		if err := rows.Scan(&fileName, &lastModified, &contentLength); err != nil {
			return nil, err
		}
		state[fileName] = imdbFileState{
			FileName:      fileName,
			LastModified:  lastModified.UTC(),
			ContentLength: contentLength,
		}
	}
	return state, rows.Err()
}

func saveSourceState(ctx context.Context, pool *pgxpool.Pool, state map[string]imdbFileState) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	for _, file := range imdbDatasetFiles {
		entry, ok := state[file]
		if !ok {
			return fmt.Errorf("missing source state for %s", file)
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO etl_source_state (file_name, last_modified, content_length, updated_at)
VALUES ($1, $2, $3, now())
ON CONFLICT (file_name) DO UPDATE
SET last_modified = EXCLUDED.last_modified,
    content_length = EXCLUDED.content_length,
    updated_at = now()`,
			entry.FileName,
			entry.LastModified,
			entry.ContentLength,
		); err != nil {
			return err
		}
	}

	if _, err := tx.Exec(ctx, `DELETE FROM etl_source_state WHERE NOT (file_name = ANY($1))`, imdbDatasetFiles); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func acquireETLSchedulerLock(ctx context.Context, pool *pgxpool.Pool) (*pgxpool.Conn, bool, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, false, err
	}

	var locked bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", etlSchedulerLockKey).Scan(&locked); err != nil {
		conn.Release()
		return nil, false, err
	}

	if !locked {
		conn.Release()
		return nil, false, nil
	}
	return conn, true, nil
}

func releaseETLSchedulerLock(conn *pgxpool.Conn) {
	if conn == nil {
		return
	}
	_, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", etlSchedulerLockKey)
	conn.Release()
}

func downloadDatasets(ctx context.Context, cfg Config, logger *log.Logger) error {
	baseURL := strings.TrimRight(cfg.BaseURL, "/")
	workerCount := clampPositive(cfg.DownloadConcurrency, 3)
	if workerCount > len(imdbDatasetFiles) {
		workerCount = len(imdbDatasetFiles)
	}

	jobs := make(chan string, len(imdbDatasetFiles))
	for _, file := range imdbDatasetFiles {
		jobs <- file
	}
	close(jobs)

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for file := range jobs {
				if workerCtx.Err() != nil {
					return
				}
				logger.Printf("etl: download %s", file)
				if err := downloadFile(workerCtx, baseURL, cfg.DataDir, file, cfg.ForceDownload, logger); err != nil {
					select {
					case errCh <- err:
					default:
					}
					cancel()
					return
				}
			}
		}()
	}

	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
	}
	if workerCtx.Err() != nil && !errors.Is(workerCtx.Err(), context.Canceled) {
		return workerCtx.Err()
	}
	return nil
}

func downloadFile(ctx context.Context, baseURL, dataDir, filename string, force bool, logger *log.Logger) error {
	dest := filepath.Join(dataDir, filename)
	if !force {
		if info, err := os.Stat(dest); err == nil && info.Size() > 0 {
			logger.Printf("etl: skip %s (already downloaded)", filename)
			return nil
		}
	}

	url := baseURL + "/" + filename
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("download %s: %w", filename, err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("download %s: %w", filename, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download %s: status %s", filename, resp.Status)
	}

	tmp := dest + ".tmp"
	if err := os.RemoveAll(tmp); err != nil {
		return fmt.Errorf("download %s: %w", filename, err)
	}

	out, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("download %s: %w", filename, err)
	}

	if _, err := io.Copy(out, resp.Body); err != nil {
		out.Close()
		return fmt.Errorf("download %s: %w", filename, err)
	}

	if err := out.Close(); err != nil {
		return fmt.Errorf("download %s: %w", filename, err)
	}

	if err := os.Rename(tmp, dest); err != nil {
		return fmt.Errorf("download %s: %w", filename, err)
	}

	return nil
}

type tsvLoadSpec struct {
	File      string
	Table     string
	Columns   []string
	Types     []tsvColumnType
	Indexes   []int
	Filter    func([]string) bool
	FilterRaw func(string) bool
	OnRecord  func([]string)
}

type tsvColumnType uint8

const (
	tsvColumnText tsvColumnType = iota
	tsvColumnInt
	tsvColumnFloat
	tsvColumnBool01
)

func loadStagingInBatches(ctx context.Context, pool *pgxpool.Pool, cfg Config, logger *log.Logger) error {
	datasetDate, err := time.Parse("2006-01-02", cfg.DatasetDate)
	if err != nil {
		return fmt.Errorf("dataset_date: %w", err)
	}
	datasetYear := datasetDate.Year()

	typeAllowlist := make(map[uint32]struct{}, 11000000)
	currentYearSet := make(map[uint32]struct{}, 250000)
	votedSet := make(map[uint32]struct{}, 4000000)
	var finalAllowlist map[uint32]struct{}
	nameAllowlist := make(map[uint32]struct{}, 4000000)

	allowedTitleTypeRaw := func(line string) bool {
		titleType, ok := fieldByIndex(line, 1)
		if !ok {
			return false
		}
		switch titleType {
		case "movie", "tvMovie", "tvSeries", "tvMiniSeries", "tvSpecial", "tvEpisode":
			return true
		default:
			return false
		}
	}

	allowlistFilter := func(line string, index int, allow map[uint32]struct{}) bool {
		if len(allow) == 0 {
			return false
		}
		raw, ok := fieldByIndex(line, index)
		if !ok {
			return false
		}
		id, ok := parseTconstID(raw)
		if !ok {
			return false
		}
		_, ok = allow[id]
		return ok
	}

	allowedPrincipalCategoryRaw := func(line string) bool {
		category, ok := fieldByIndex(line, 3)
		if !ok {
			return false
		}
		switch category {
		case "actor", "actress",
			"producer", "executive_producer", "associate_producer", "co_producer", "line_producer":
			return true
		default:
			return false
		}
	}

	addNconst := func(raw string) {
		if raw == "" || raw == "\\N" {
			return
		}
		if id, ok := parseNconstID(raw); ok {
			nameAllowlist[id] = struct{}{}
		}
	}

	addNconstList := func(raw string) {
		if raw == "" || raw == "\\N" {
			return
		}
		for _, part := range strings.Split(raw, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			addNconst(part)
		}
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Release()

	basicsSpec := tsvLoadSpec{
		File:  "title.basics.tsv.gz",
		Table: "stg_title_basics",
		Columns: []string{
			"tconst", "titletype", "primarytitle", "originaltitle", "isadult", "startyear", "endyear", "runtimeminutes", "genres",
		},
		Types: []tsvColumnType{
			tsvColumnText, tsvColumnText, tsvColumnText, tsvColumnText,
			tsvColumnBool01, tsvColumnInt, tsvColumnInt, tsvColumnInt, tsvColumnText,
		},
		FilterRaw: allowedTitleTypeRaw,
		OnRecord: func(record []string) {
			if len(record) == 0 {
				return
			}
			id, ok := parseTconstID(record[0])
			if !ok {
				return
			}
			typeAllowlist[id] = struct{}{}
			if len(record) > 5 {
				if year := parseInt(record[5]); year != nil && *year == datasetYear {
					currentYearSet[id] = struct{}{}
				}
			}
		},
	}
	path := filepath.Join(cfg.DataDir, basicsSpec.File)
	if err := loadTSVToTable(ctx, conn, path, basicsSpec.Table, basicsSpec.Columns, basicsSpec.Types, basicsSpec.Indexes, cfg.LoadBatchSize, cfg.ReaderBufferSize, basicsSpec.Filter, basicsSpec.FilterRaw, basicsSpec.OnRecord, logger); err != nil {
		return err
	}
	logger.Printf("etl: allowlist type=%d current_year=%d", len(typeAllowlist), len(currentYearSet))

	ratingsSpec := tsvLoadSpec{
		File:  "title.ratings.tsv.gz",
		Table: "stg_title_ratings",
		Columns: []string{
			"tconst", "averagerating", "numvotes",
		},
		Types: []tsvColumnType{
			tsvColumnText, tsvColumnFloat, tsvColumnInt,
		},
		FilterRaw: func(line string) bool {
			return allowlistFilter(line, 0, typeAllowlist)
		},
		OnRecord: func(record []string) {
			if len(record) < 3 {
				return
			}
			id, ok := parseTconstID(record[0])
			if !ok {
				return
			}
			if _, ok := typeAllowlist[id]; !ok {
				return
			}
			votes := parseInt(record[2])
			if votes != nil && *votes >= cfg.MinNumVotes {
				votedSet[id] = struct{}{}
			}
		},
	}
	path = filepath.Join(cfg.DataDir, ratingsSpec.File)
	if err := loadTSVToTable(ctx, conn, path, ratingsSpec.Table, ratingsSpec.Columns, ratingsSpec.Types, ratingsSpec.Indexes, cfg.LoadBatchSize, cfg.ReaderBufferSize, ratingsSpec.Filter, ratingsSpec.FilterRaw, ratingsSpec.OnRecord, logger); err != nil {
		return err
	}
	logger.Printf("etl: allowlist voted=%d min_votes=%d", len(votedSet), cfg.MinNumVotes)

	finalAllowlist = make(map[uint32]struct{}, len(currentYearSet)+len(votedSet))
	for id := range currentYearSet {
		finalAllowlist[id] = struct{}{}
	}
	for id := range votedSet {
		finalAllowlist[id] = struct{}{}
	}
	logger.Printf("etl: allowlist final=%d", len(finalAllowlist))

	typeAllowlist = nil
	currentYearSet = nil
	votedSet = nil

	specs := []tsvLoadSpec{
		{
			File:  "title.akas.tsv.gz",
			Table: "stg_title_akas",
			Columns: []string{
				"titleid", "ordering", "title", "region", "language", "types", "attributes", "isoriginaltitle",
			},
			Types: []tsvColumnType{
				tsvColumnText, tsvColumnInt, tsvColumnText, tsvColumnText,
				tsvColumnText, tsvColumnText, tsvColumnText, tsvColumnBool01,
			},
			FilterRaw: func(line string) bool {
				return allowlistFilter(line, 0, finalAllowlist)
			},
		},
		{
			File:  "title.principals.tsv.gz",
			Table: "stg_title_principals",
			Columns: []string{
				"tconst", "ordering", "nconst", "category", "characters",
			},
			Types: []tsvColumnType{
				tsvColumnText, tsvColumnInt, tsvColumnText, tsvColumnText, tsvColumnText,
			},
			Indexes: []int{0, 1, 2, 3, 5},
			FilterRaw: func(line string) bool {
				if !allowlistFilter(line, 0, finalAllowlist) {
					return false
				}
				return allowedPrincipalCategoryRaw(line)
			},
			OnRecord: func(record []string) {
				if len(record) > 2 {
					addNconst(record[2])
				}
			},
		},
		{
			File:  "title.crew.tsv.gz",
			Table: "stg_title_crew",
			Columns: []string{
				"tconst", "directors", "writers",
			},
			Types: []tsvColumnType{
				tsvColumnText, tsvColumnText, tsvColumnText,
			},
			FilterRaw: func(line string) bool {
				return allowlistFilter(line, 0, finalAllowlist)
			},
			OnRecord: func(record []string) {
				if len(record) > 1 {
					addNconstList(record[1])
				}
				if len(record) > 2 {
					addNconstList(record[2])
				}
			},
		},
		{
			File:  "title.episode.tsv.gz",
			Table: "stg_title_episode",
			Columns: []string{
				"tconst", "parenttconst", "seasonnumber", "episodenumber",
			},
			Types: []tsvColumnType{
				tsvColumnText, tsvColumnText, tsvColumnInt, tsvColumnInt,
			},
			FilterRaw: func(line string) bool {
				return allowlistFilter(line, 1, finalAllowlist)
			},
		},
		{
			File:  "name.basics.tsv.gz",
			Table: "stg_name_basics",
			Columns: []string{
				"nconst", "primaryname",
			},
			Types: []tsvColumnType{
				tsvColumnText, tsvColumnText,
			},
			Indexes: []int{0, 1},
			FilterRaw: func(line string) bool {
				raw, ok := fieldByIndex(line, 0)
				if !ok {
					return false
				}
				id, ok := parseNconstID(raw)
				if !ok {
					return false
				}
				_, ok = nameAllowlist[id]
				return ok
			},
		},
	}

	for _, spec := range specs {
		path := filepath.Join(cfg.DataDir, spec.File)
		if err := loadTSVToTable(ctx, conn, path, spec.Table, spec.Columns, spec.Types, spec.Indexes, cfg.LoadBatchSize, cfg.ReaderBufferSize, spec.Filter, spec.FilterRaw, spec.OnRecord, logger); err != nil {
			return err
		}
	}

	logger.Printf("etl: allowlist names=%d", len(nameAllowlist))

	finalAllowlist = nil
	nameAllowlist = nil
	runtime.GC()
	return nil
}

func loadTSVToTable(ctx context.Context, conn *pgxpool.Conn, path, table string, columns []string, columnTypes []tsvColumnType, indexes []int, batchSize int, readerBuffer int, filter func([]string) bool, filterRaw func(string) bool, onRecord func([]string), logger *log.Logger) error {
	_ = batchSize
	start := time.Now()
	logger.Printf("etl: load %s -> %s", filepath.Base(path), table)

	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer file.Close()

	gz, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("gzip %s: %w", path, err)
	}
	defer gz.Close()

	var reader *bufio.Reader
	if readerBuffer > 0 {
		reader = bufio.NewReaderSize(gz, readerBuffer)
	} else {
		reader = bufio.NewReader(gz)
	}

	source := newTSVCopySource(reader, columns, columnTypes, indexes, filter, filterRaw, onRecord, logger, table, start)
	if _, err := conn.CopyFrom(ctx, pgx.Identifier{table}, columns, source); err != nil {
		return fmt.Errorf("copy %s: %w", table, err)
	}
	if err := source.Err(); err != nil {
		return err
	}

	logger.Printf(
		"etl: finished %s scanned=%d inserted=%d skipped=%d in %s",
		table,
		source.Scanned(),
		source.Inserted(),
		source.Skipped(),
		time.Since(start).Truncate(time.Millisecond),
	)
	return nil
}

type tsvCopySource struct {
	reader      *bufio.Reader
	columns     []string
	columnTypes []tsvColumnType
	indexes     []int
	filter      func([]string) bool
	filterRaw   func(string) bool
	onRecord    func([]string)
	logger      *log.Logger
	table       string
	start       time.Time

	headerRead bool
	sourceLen  int
	maxIndex   int
	row        []any
	err        error
	fullRecord []string
	record     []string

	scanned  int
	inserted int
	skipped  int

	logEvery int
}

func newTSVCopySource(reader *bufio.Reader, columns []string, columnTypes []tsvColumnType, indexes []int, filter func([]string) bool, filterRaw func(string) bool, onRecord func([]string), logger *log.Logger, table string, start time.Time) *tsvCopySource {
	types := columnTypes
	if len(types) != len(columns) {
		types = make([]tsvColumnType, len(columns))
		for i := range types {
			types[i] = tsvColumnText
		}
	}
	maxIdx := -1
	for _, idx := range indexes {
		if idx > maxIdx {
			maxIdx = idx
		}
	}
	return &tsvCopySource{
		reader:      reader,
		columns:     columns,
		columnTypes: types,
		indexes:     indexes,
		filter:      filter,
		filterRaw:   filterRaw,
		onRecord:    onRecord,
		logger:      logger,
		table:       table,
		start:       start,
		maxIndex:    maxIdx,
		row:         make([]any, len(columns)),
		logEvery:    500000,
	}
}

func trimLineEnding(line string) string {
	if len(line) == 0 {
		return line
	}
	if line[len(line)-1] == '\n' {
		line = line[:len(line)-1]
	}
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	return line
}

func countTSVColumns(line string) int {
	if line == "" {
		return 0
	}
	count := 1
	for i := 0; i < len(line); i++ {
		if line[i] == '\t' {
			count++
		}
	}
	return count
}

func parseTSVRecord(line string, expected int, dst []string) []string {
	if expected <= 0 {
		return dst[:0]
	}
	if cap(dst) < expected {
		dst = make([]string, expected)
	} else {
		dst = dst[:expected]
		for i := range dst {
			dst[i] = ""
		}
	}

	field := 0
	start := 0
	for i := 0; i <= len(line); i++ {
		if i < len(line) && line[i] != '\t' {
			continue
		}
		if field == expected-1 {
			dst[field] = line[start:]
			return dst
		}
		dst[field] = line[start:i]
		field++
		start = i + 1
		if field >= expected {
			return dst
		}
	}
	return dst
}

func convertTSVValue(raw string, typ tsvColumnType) any {
	switch typ {
	case tsvColumnText:
		return raw
	case tsvColumnInt:
		if raw == "" || raw == "\\N" {
			return nil
		}
		val, err := strconv.Atoi(raw)
		if err != nil {
			return nil
		}
		return val
	case tsvColumnFloat:
		if raw == "" || raw == "\\N" {
			return nil
		}
		val, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return nil
		}
		return val
	case tsvColumnBool01:
		if raw == "" || raw == "\\N" {
			return nil
		}
		if raw == "1" {
			return true
		}
		if raw == "0" {
			return false
		}
		return nil
	default:
		return raw
	}
}

func (s *tsvCopySource) readLine() (string, error) {
	line, err := s.reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	if errors.Is(err, io.EOF) && len(line) == 0 {
		return "", io.EOF
	}
	return trimLineEnding(line), err
}

func (s *tsvCopySource) Next() bool {
	if s.err != nil {
		return false
	}
	for {
		line, err := s.readLine()
		if errors.Is(err, io.EOF) && line == "" {
			return false
		}
		if err != nil && !errors.Is(err, io.EOF) {
			s.err = err
			return false
		}
		if line == "" {
			if errors.Is(err, io.EOF) {
				return false
			}
			continue
		}

		if !s.headerRead {
			s.sourceLen = countTSVColumns(line)
			if len(s.indexes) == 0 && s.sourceLen != len(s.columns) {
				s.logger.Printf("etl: warning %s header columns=%d expected=%d", s.table, s.sourceLen, len(s.columns))
			}
			if len(s.indexes) > 0 && s.maxIndex >= s.sourceLen {
				s.logger.Printf("etl: warning %s header columns=%d max_index=%d", s.table, s.sourceLen, s.maxIndex)
			}
			s.headerRead = true
			if errors.Is(err, io.EOF) {
				return false
			}
			continue
		}

		s.scanned++
		if s.filterRaw != nil && !s.filterRaw(line) {
			s.skipped++
			if errors.Is(err, io.EOF) {
				return false
			}
			continue
		}

		s.fullRecord = parseTSVRecord(line, s.sourceLen, s.fullRecord)

		record := s.fullRecord
		if len(s.indexes) > 0 {
			if cap(s.record) < len(s.columns) {
				s.record = make([]string, len(s.columns))
			} else {
				s.record = s.record[:len(s.columns)]
			}
			for i, idx := range s.indexes {
				val := ""
				if idx >= 0 && idx < len(s.fullRecord) {
					val = s.fullRecord[idx]
				}
				s.record[i] = val
			}
			record = s.record
		}

		if s.filter != nil && !s.filter(record) {
			s.skipped++
			if errors.Is(err, io.EOF) {
				return false
			}
			continue
		}

		if s.onRecord != nil {
			s.onRecord(record)
		}

		for i := range s.columns {
			s.row[i] = convertTSVValue(record[i], s.columnTypes[i])
		}
		s.inserted++

		if s.scanned%s.logEvery == 0 {
			s.logger.Printf("etl: %s scanned=%d inserted=%d skipped=%d elapsed=%s", s.table, s.scanned, s.inserted, s.skipped, time.Since(s.start).Truncate(time.Second))
		}

		return true
	}
}

func (s *tsvCopySource) Values() ([]any, error) {
	return s.row, nil
}

func (s *tsvCopySource) Err() error {
	return s.err
}

func (s *tsvCopySource) Scanned() int {
	return s.scanned
}

func (s *tsvCopySource) Inserted() int {
	return s.inserted
}

func (s *tsvCopySource) Skipped() int {
	return s.skipped
}

type queryer interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

type searchCopyRow struct {
	Tconst        string
	TitleType     string
	StartYear     *int
	PrimaryTitle  string
	OriginalTitle string
	AkaTitles     []string
}

type titlesNextCopySource struct {
	cfg              Config
	datasetDate      time.Time
	tconsts          []string
	basics           map[string]TitleBasics
	ratings          map[string]Rating
	akasByTitle      map[string]map[string][]Aka
	actorsByTitle    map[string][]principalRow
	producersByTitle map[string][]principalRow
	crewByTitle      map[string]crewLists
	episodesByTitle  map[string][]Season
	names            map[string]string
	searchRows       *[]searchCopyRow

	idx      int
	inserted int
	row      []any
	err      error
}

func newTitlesNextCopySource(
	cfg Config,
	datasetDate time.Time,
	tconsts []string,
	basics map[string]TitleBasics,
	ratings map[string]Rating,
	akasByTitle map[string]map[string][]Aka,
	actorsByTitle map[string][]principalRow,
	producersByTitle map[string][]principalRow,
	crewByTitle map[string]crewLists,
	episodesByTitle map[string][]Season,
	names map[string]string,
	searchRows *[]searchCopyRow,
) *titlesNextCopySource {
	return &titlesNextCopySource{
		cfg:              cfg,
		datasetDate:      datasetDate,
		tconsts:          tconsts,
		basics:           basics,
		ratings:          ratings,
		akasByTitle:      akasByTitle,
		actorsByTitle:    actorsByTitle,
		producersByTitle: producersByTitle,
		crewByTitle:      crewByTitle,
		episodesByTitle:  episodesByTitle,
		names:            names,
		searchRows:       searchRows,
		row:              make([]any, 14),
	}
}

func (s *titlesNextCopySource) Next() bool {
	if s.err != nil {
		return false
	}
	for s.idx < len(s.tconsts) {
		tconst := s.tconsts[s.idx]
		s.idx++

		basic, ok := s.basics[tconst]
		if !ok {
			continue
		}

		rating := s.ratings[tconst]
		akas := s.akasByTitle[tconst]
		if akas == nil {
			akas = map[string][]Aka{}
		} else {
			akas = filterAkas(akas, basic.PrimaryTitle, basic.OriginalTitle)
			if akas == nil {
				akas = map[string][]Aka{}
			}
		}

		cast := buildCast(s.actorsByTitle[tconst], s.names)
		if cast == nil {
			cast = []CastMember{}
		}

		producers := buildProducers(s.producersByTitle[tconst], s.names)
		if producers == nil {
			producers = []Producer{}
		}

		crewLists := s.crewByTitle[tconst]
		directors := buildCrewMembers(crewLists.Directors, s.names)
		writers := buildCrewMembers(crewLists.Writers, s.names)
		if directors == nil {
			directors = []CrewMember{}
		}
		if writers == nil {
			writers = []CrewMember{}
		}

		episodes := s.episodesByTitle[tconst]
		if episodes == nil {
			episodes = []Season{}
		}

		data := TitleData{
			Basics:  basic,
			Akas:    akas,
			Ratings: rating,
			Cast:    cast,
			Crew: TitleCrew{
				Directors: directors,
				Writers:   writers,
				Producers: producers,
			},
			Episodes: episodes,
		}

		jsonb, err := toJSONB(data)
		if err != nil {
			s.err = fmt.Errorf("jsonb %s: %w", tconst, err)
			return false
		}

		s.row[0] = tconst
		s.row[1] = basic.TitleType
		s.row[2] = basic.PrimaryTitle
		s.row[3] = basic.OriginalTitle
		s.row[4] = intOrNil(basic.StartYear)
		s.row[5] = intOrNil(basic.EndYear)
		s.row[6] = basic.IsAdult
		s.row[7] = intOrNil(basic.RuntimeMinutes)
		s.row[8] = basic.Genres
		s.row[9] = floatOrNil(rating.AverageRating)
		s.row[10] = intOrNil(rating.NumVotes)
		s.row[11] = jsonb
		s.row[12] = s.datasetDate
		s.row[13] = s.cfg.SchemaVersion

		if basic.TitleType != "tvepisode" {
			akaTitles := make([]string, 0)
			for _, regionEntries := range akas {
				for _, aka := range regionEntries {
					akaTitles = append(akaTitles, aka.Title)
				}
			}
			akaTitles = dedupeStrings(akaTitles)
			*s.searchRows = append(*s.searchRows, searchCopyRow{
				Tconst:        tconst,
				TitleType:     basic.TitleType,
				StartYear:     basic.StartYear,
				PrimaryTitle:  basic.PrimaryTitle,
				OriginalTitle: basic.OriginalTitle,
				AkaTitles:     akaTitles,
			})
		}

		s.inserted++
		return true
	}
	return false
}

func (s *titlesNextCopySource) Values() ([]any, error) {
	return s.row, nil
}

func (s *titlesNextCopySource) Err() error {
	return s.err
}

func (s *titlesNextCopySource) Inserted() int {
	return s.inserted
}

type searchRowsCopySource struct {
	rows     []searchCopyRow
	idx      int
	inserted int
	row      []any
}

func newSearchRowsCopySource(rows []searchCopyRow) *searchRowsCopySource {
	return &searchRowsCopySource{
		rows: rows,
		row:  make([]any, 6),
	}
}

func (s *searchRowsCopySource) Next() bool {
	if s.idx >= len(s.rows) {
		return false
	}
	item := s.rows[s.idx]
	s.idx++

	s.row[0] = item.Tconst
	s.row[1] = item.TitleType
	s.row[2] = intOrNil(item.StartYear)
	s.row[3] = item.PrimaryTitle
	s.row[4] = item.OriginalTitle
	s.row[5] = item.AkaTitles

	s.inserted++
	return true
}

func (s *searchRowsCopySource) Values() ([]any, error) {
	return s.row, nil
}

func (s *searchRowsCopySource) Err() error {
	return nil
}

func (s *searchRowsCopySource) Inserted() int {
	return s.inserted
}

func buildTitlesInBatches(ctx context.Context, pool *pgxpool.Pool, cfg Config, logger *log.Logger) error {
	datasetDate, err := time.Parse("2006-01-02", cfg.DatasetDate)
	if err != nil {
		return fmt.Errorf("dataset_date: %w", err)
	}

	last := ""
	batchNum := 0
	for {
		tconsts, err := fetchTconstBatch(ctx, pool, last, cfg.BatchSize)
		if err != nil {
			return err
		}
		if len(tconsts) == 0 {
			break
		}

		batchNum++
		logger.Printf("etl: batch %d tconsts=%d", batchNum, len(tconsts))

		conn, err := pool.Acquire(ctx)
		if err != nil {
			return fmt.Errorf("acquire conn: %w", err)
		}
		err = buildAndInsertBatch(ctx, conn, cfg, tconsts, datasetDate, logger)
		conn.Release()
		if err != nil {
			return err
		}

		last = tconsts[len(tconsts)-1]
	}

	logger.Printf("etl: batch build complete (%d batches)", batchNum)
	return nil
}

func fetchTconstBatch(ctx context.Context, pool *pgxpool.Pool, last string, limit int) ([]string, error) {
	rows, err := pool.Query(ctx, `
SELECT tconst
FROM title_filter
WHERE tconst > $1
ORDER BY tconst
LIMIT $2`, last, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]string, 0, limit)
	for rows.Next() {
		var tconst string
		if err := rows.Scan(&tconst); err != nil {
			return nil, err
		}
		out = append(out, tconst)
	}
	return out, rows.Err()
}

func buildAndInsertBatch(ctx context.Context, conn *pgxpool.Conn, cfg Config, tconsts []string, datasetDate time.Time, logger *log.Logger) error {
	batchStart := time.Now()

	stepStart := time.Now()
	basics, err := fetchBasics(ctx, conn, tconsts)
	if err != nil {
		return err
	}
	basicsDur := time.Since(stepStart)

	seriesTconsts := make([]string, 0, len(basics))
	for _, basic := range basics {
		if basic.TitleType == "tvseries" || basic.TitleType == "tvminiseries" {
			seriesTconsts = append(seriesTconsts, basic.Tconst)
		}
	}

	stepStart = time.Now()
	ratings, err := fetchRatings(ctx, conn, tconsts)
	if err != nil {
		return err
	}
	ratingsDur := time.Since(stepStart)

	stepStart = time.Now()
	akasByTitle, err := fetchAkas(ctx, conn, tconsts)
	if err != nil {
		return err
	}
	akasDur := time.Since(stepStart)

	stepStart = time.Now()
	principals, err := fetchPrincipals(ctx, conn, tconsts, cfg.MaxActors, cfg.MaxProducers)
	if err != nil {
		return err
	}
	principalsDur := time.Since(stepStart)

	actorsByTitle := make(map[string][]principalRow)
	producersByTitle := make(map[string][]principalRow)
	for _, row := range principals {
		if row.Category == "actor" || row.Category == "actress" {
			actorsByTitle[row.Tconst] = append(actorsByTitle[row.Tconst], row)
		} else {
			producersByTitle[row.Tconst] = append(producersByTitle[row.Tconst], row)
		}
	}

	if cfg.MaxActors == 0 {
		actorsByTitle = map[string][]principalRow{}
	}
	if cfg.MaxProducers == 0 {
		producersByTitle = map[string][]principalRow{}
	}

	for tconst, list := range actorsByTitle {
		if len(list) > cfg.MaxActors {
			actorsByTitle[tconst] = list[:cfg.MaxActors]
		}
	}
	for tconst, list := range producersByTitle {
		if len(list) > cfg.MaxProducers {
			producersByTitle[tconst] = list[:cfg.MaxProducers]
		}
	}

	stepStart = time.Now()
	crewByTitle, err := fetchCrew(ctx, conn, tconsts)
	if err != nil {
		return err
	}
	crewDur := time.Since(stepStart)

	for tconst, lists := range crewByTitle {
		if cfg.MaxDirectors == 0 {
			lists.Directors = nil
		} else if len(lists.Directors) > cfg.MaxDirectors {
			lists.Directors = lists.Directors[:cfg.MaxDirectors]
		}
		if cfg.MaxWriters == 0 {
			lists.Writers = nil
		} else if len(lists.Writers) > cfg.MaxWriters {
			lists.Writers = lists.Writers[:cfg.MaxWriters]
		}
		crewByTitle[tconst] = lists
	}

	nconstSet := make(map[string]struct{})
	for _, list := range actorsByTitle {
		for _, row := range list {
			nconstSet[row.Nconst] = struct{}{}
		}
	}
	for _, list := range producersByTitle {
		for _, row := range list {
			nconstSet[row.Nconst] = struct{}{}
		}
	}
	for _, list := range crewByTitle {
		for _, nconst := range list.Directors {
			nconstSet[nconst] = struct{}{}
		}
		for _, nconst := range list.Writers {
			nconstSet[nconst] = struct{}{}
		}
	}

	nconsts := make([]string, 0, len(nconstSet))
	for nconst := range nconstSet {
		nconsts = append(nconsts, nconst)
	}

	stepStart = time.Now()
	names, err := fetchNames(ctx, conn, nconsts)
	if err != nil {
		return err
	}
	namesDur := time.Since(stepStart)

	stepStart = time.Now()
	episodesByTitle, err := fetchEpisodes(ctx, conn, seriesTconsts)
	if err != nil {
		return err
	}
	episodesDur := time.Since(stepStart)

	searchRows := make([]searchCopyRow, 0, len(tconsts))

	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	var copyTitlesDur time.Duration
	titlesSource := newTitlesNextCopySource(
		cfg,
		datasetDate,
		tconsts,
		basics,
		ratings,
		akasByTitle,
		actorsByTitle,
		producersByTitle,
		crewByTitle,
		episodesByTitle,
		names,
		&searchRows,
	)
	stepStart = time.Now()
	_, err = tx.CopyFrom(
		ctx,
		pgx.Identifier{"titles_next"},
		[]string{
			"tconst",
			"title_type",
			"primary_title",
			"original_title",
			"start_year",
			"end_year",
			"is_adult",
			"runtime_minutes",
			"genres",
			"average_rating",
			"num_votes",
			"data",
			"dataset_date",
			"schema_version",
		},
		titlesSource,
	)
	if err != nil {
		return fmt.Errorf("copy titles_next: %w", err)
	}
	if err := titlesSource.Err(); err != nil {
		return err
	}
	copyTitlesDur = time.Since(stepStart)
	buildDur := copyTitlesDur

	var copySearchDur time.Duration
	if len(searchRows) > 0 {
		searchSource := newSearchRowsCopySource(searchRows)
		stepStart = time.Now()
		_, err = tx.CopyFrom(
			ctx,
			pgx.Identifier{"title_search_next"},
			[]string{
				"tconst",
				"title_type",
				"start_year",
				"primary_title",
				"original_title",
				"aka_titles",
			},
			searchSource,
		)
		if err != nil {
			return fmt.Errorf("copy title_search_next: %w", err)
		}
		copySearchDur = time.Since(stepStart)
	}

	stepStart = time.Now()
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit batch: %w", err)
	}
	commitDur := time.Since(stepStart)

	logger.Printf("etl: batch inserted titles=%d search=%d", titlesSource.Inserted(), len(searchRows))
	logger.Printf(
		"etl: batch timings basics=%s ratings=%s akas=%s principals=%s crew=%s names=%s episodes=%s build=%s copy_titles=%s copy_search=%s commit=%s total=%s",
		basicsDur.Truncate(time.Millisecond),
		ratingsDur.Truncate(time.Millisecond),
		akasDur.Truncate(time.Millisecond),
		principalsDur.Truncate(time.Millisecond),
		crewDur.Truncate(time.Millisecond),
		namesDur.Truncate(time.Millisecond),
		episodesDur.Truncate(time.Millisecond),
		buildDur.Truncate(time.Millisecond),
		copyTitlesDur.Truncate(time.Millisecond),
		copySearchDur.Truncate(time.Millisecond),
		commitDur.Truncate(time.Millisecond),
		time.Since(batchStart).Truncate(time.Millisecond),
	)
	return nil
}

func fetchBasics(ctx context.Context, q queryer, tconsts []string) (map[string]TitleBasics, error) {
	rows, err := q.Query(ctx, `
SELECT tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres
FROM stg_title_basics
WHERE tconst = ANY($1)`, tconsts)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]TitleBasics, len(tconsts))
	for rows.Next() {
		var (
			tconst        string
			titleType     string
			primaryTitle  string
			originalTitle string
			isAdult       pgtype.Bool
			startYear     pgtype.Int4
			endYear       pgtype.Int4
			runtime       pgtype.Int4
			genres        string
		)
		if err := rows.Scan(&tconst, &titleType, &primaryTitle, &originalTitle, &isAdult, &startYear, &endYear, &runtime, &genres); err != nil {
			return nil, err
		}

		titleType = strings.ToLower(strings.TrimSpace(titleType))
		primaryTitle = strings.TrimSpace(primaryTitle)
		originalTitle = strings.TrimSpace(originalTitle)
		out[tconst] = TitleBasics{
			Tconst:         tconst,
			TitleType:      titleType,
			PrimaryTitle:   primaryTitle,
			OriginalTitle:  originalTitle,
			IsAdult:        isAdult.Valid && isAdult.Bool,
			StartYear:      intPtrFromPg(startYear),
			EndYear:        intPtrFromPg(endYear),
			RuntimeMinutes: intPtrFromPg(runtime),
			Genres:         splitList(genres),
		}
	}
	return out, rows.Err()
}

func fetchRatings(ctx context.Context, q queryer, tconsts []string) (map[string]Rating, error) {
	rows, err := q.Query(ctx, `
SELECT tconst, averageRating, numVotes
FROM stg_title_ratings
WHERE tconst = ANY($1)`, tconsts)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]Rating, len(tconsts))
	for rows.Next() {
		var (
			tconst string
			avg    pgtype.Float8
			votes  pgtype.Int4
		)
		if err := rows.Scan(&tconst, &avg, &votes); err != nil {
			return nil, err
		}
		out[tconst] = Rating{
			AverageRating: floatPtrFromPg(avg),
			NumVotes:      intPtrFromPg(votes),
		}
	}
	return out, rows.Err()
}

func fetchAkas(ctx context.Context, q queryer, tconsts []string) (map[string]map[string][]Aka, error) {
	rows, err := q.Query(ctx, `
SELECT titleId, ordering, title, region, language, types, attributes, isOriginalTitle
FROM stg_title_akas
WHERE titleId = ANY($1)
ORDER BY titleId, ordering`, tconsts)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]map[string][]Aka)
	for rows.Next() {
		var (
			titleId         string
			ordering        pgtype.Int4
			title           string
			region          string
			language        string
			types           string
			attributes      string
			isOriginalTitle pgtype.Bool
		)
		if err := rows.Scan(&titleId, &ordering, &title, &region, &language, &types, &attributes, &isOriginalTitle); err != nil {
			return nil, err
		}

		region = strings.TrimSpace(nullIfNA(region))
		if region == "" {
			region = "GLOBAL"
		}
		orderVal := 0
		if ordering.Valid {
			orderVal = int(ordering.Int32)
		}

		aka := Aka{
			Title:           title,
			Language:        strings.TrimSpace(nullIfNA(language)),
			Types:           splitList(types),
			Attributes:      splitList(attributes),
			IsOriginalTitle: isOriginalTitle.Valid && isOriginalTitle.Bool,
			Ordering:        orderVal,
		}

		if _, ok := out[titleId]; !ok {
			out[titleId] = make(map[string][]Aka)
		}
		out[titleId][region] = append(out[titleId][region], aka)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	for _, byRegion := range out {
		for region, entries := range byRegion {
			sort.SliceStable(entries, func(i, j int) bool {
				a := entries[i]
				b := entries[j]
				aRank := akaTypeRank(a)
				bRank := akaTypeRank(b)
				if aRank != bRank {
					return aRank < bRank
				}
				if a.IsOriginalTitle != b.IsOriginalTitle {
					return a.IsOriginalTitle
				}
				aOrd := akaOrderingOrMax(a.Ordering)
				bOrd := akaOrderingOrMax(b.Ordering)
				if aOrd != bOrd {
					return aOrd < bOrd
				}
				return strings.ToLower(a.Title) < strings.ToLower(b.Title)
			})
			byRegion[region] = entries
		}
	}

	return out, nil
}

func fetchPrincipals(ctx context.Context, q queryer, tconsts []string, maxActors int, maxProducers int) ([]principalRow, error) {
	if maxActors <= 0 && maxProducers <= 0 {
		return []principalRow{}, nil
	}

	rows, err := q.Query(ctx, `
WITH ranked AS (
  SELECT tconst, ordering, nconst, category, characters,
         CASE WHEN category IN ('actor','actress') THEN 1 ELSE 0 END AS is_actor,
         row_number() OVER (
           PARTITION BY tconst, CASE WHEN category IN ('actor','actress') THEN 1 ELSE 0 END
           ORDER BY ordering
         ) AS rn
  FROM stg_title_principals
  WHERE tconst = ANY($1)
    AND category IN (
      'actor','actress',
      'producer','executive_producer','associate_producer','co_producer','line_producer'
    )
)
SELECT tconst, ordering, nconst, category, characters
FROM ranked
WHERE (is_actor = 1 AND rn <= $2)
   OR (is_actor = 0 AND rn <= $3)
ORDER BY tconst, ordering`, tconsts, maxActors, maxProducers)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]principalRow, 0)
	for rows.Next() {
		var (
			tconst     string
			ordering   pgtype.Int4
			nconst     string
			category   string
			characters string
		)
		if err := rows.Scan(&tconst, &ordering, &nconst, &category, &characters); err != nil {
			return nil, err
		}
		ord := 0
		if ordering.Valid {
			ord = int(ordering.Int32)
		}
		out = append(out, principalRow{
			Tconst:     tconst,
			Ordering:   ord,
			Nconst:     nconst,
			Category:   category,
			Characters: characters,
		})
	}
	return out, rows.Err()
}

func fetchCrew(ctx context.Context, q queryer, tconsts []string) (map[string]crewLists, error) {
	rows, err := q.Query(ctx, `
SELECT tconst, directors, writers
FROM stg_title_crew
WHERE tconst = ANY($1)`, tconsts)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]crewLists)
	for rows.Next() {
		var tconst, directors, writers string
		if err := rows.Scan(&tconst, &directors, &writers); err != nil {
			return nil, err
		}
		out[tconst] = crewLists{
			Directors: splitList(directors),
			Writers:   splitList(writers),
		}
	}
	return out, rows.Err()
}

func fetchNames(ctx context.Context, q queryer, nconsts []string) (map[string]string, error) {
	if len(nconsts) == 0 {
		return map[string]string{}, nil
	}

	rows, err := q.Query(ctx, `
SELECT nconst, primaryName
FROM stg_name_basics
WHERE nconst = ANY($1)`, nconsts)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]string, len(nconsts))
	for rows.Next() {
		var nconst, name string
		if err := rows.Scan(&nconst, &name); err != nil {
			return nil, err
		}
		out[nconst] = name
	}
	return out, rows.Err()
}

func fetchEpisodes(ctx context.Context, q queryer, tconsts []string) (map[string][]Season, error) {
	if len(tconsts) == 0 {
		return map[string][]Season{}, nil
	}

	rows, err := q.Query(ctx, `
SELECT parent_tconst, tconst, season_number, episode_number,
       primary_title, start_year, average_rating, num_votes
FROM stg_episode_enriched
WHERE parent_tconst = ANY($1)
ORDER BY parent_tconst, season_number NULLS LAST, episode_number NULLS LAST, tconst`, tconsts)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	byParent := make(map[string]map[seasonKey]*Season)
	for rows.Next() {
		var (
			parentTconst string
			tconst       string
			seasonNum    pgtype.Int4
			episodeNum   pgtype.Int4
			primaryTitle string
			startYear    pgtype.Int4
			avgRating    pgtype.Float8
			numVotes     pgtype.Int4
		)
		if err := rows.Scan(&parentTconst, &tconst, &seasonNum, &episodeNum, &primaryTitle, &startYear, &avgRating, &numVotes); err != nil {
			return nil, err
		}

		if _, ok := byParent[parentTconst]; !ok {
			byParent[parentTconst] = make(map[seasonKey]*Season)
		}

		key := seasonKeyFromPtr(intPtrFromPg(seasonNum))
		season := byParent[parentTconst][key]
		if season == nil {
			season = &Season{SeasonNumber: intPtrFromPg(seasonNum), Episodes: []Episode{}}
			byParent[parentTconst][key] = season
		}

		season.Episodes = append(season.Episodes, Episode{
			Tconst:        tconst,
			EpisodeNumber: intPtrFromPg(episodeNum),
			PrimaryTitle:  primaryTitle,
			StartYear:     intPtrFromPg(startYear),
			AverageRating: floatPtrFromPg(avgRating),
			NumVotes:      intPtrFromPg(numVotes),
		})
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make(map[string][]Season, len(byParent))
	for parent, seasonsMap := range byParent {
		seasons := make([]Season, 0, len(seasonsMap))
		for _, season := range seasonsMap {
			seasons = append(seasons, *season)
		}
		sort.Slice(seasons, func(i, j int) bool {
			if seasons[i].SeasonNumber == nil && seasons[j].SeasonNumber == nil {
				return false
			}
			if seasons[i].SeasonNumber == nil {
				return false
			}
			if seasons[j].SeasonNumber == nil {
				return true
			}
			return *seasons[i].SeasonNumber < *seasons[j].SeasonNumber
		})
		out[parent] = seasons
	}

	return out, nil
}

func buildCast(rows []principalRow, names map[string]string) []CastMember {
	if len(rows) == 0 {
		return nil
	}
	cast := make([]CastMember, 0, len(rows))
	index := make(map[string]int, len(rows))
	for _, row := range rows {
		key := row.Nconst + "|" + row.Category
		chars := normalizeCharacters(row.Characters)
		if idx, ok := index[key]; ok {
			if len(chars) > 0 {
				cast[idx].Characters = mergeDedupStrings(cast[idx].Characters, chars)
			}
			continue
		}
		cast = append(cast, CastMember{
			Nconst:     row.Nconst,
			Name:       names[row.Nconst],
			Category:   row.Category,
			Characters: chars,
		})
		index[key] = len(cast) - 1
	}
	return cast
}

func mergeDedupStrings(base []string, add []string) []string {
	if len(add) == 0 {
		return base
	}
	if len(base) == 0 {
		return add
	}
	combined := make([]string, 0, len(base)+len(add))
	combined = append(combined, base...)
	combined = append(combined, add...)
	return dedupeCharacters(combined)
}

func dedupeCharacters(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		key := normalizeCharacterKey(value)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, value)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func normalizeCharacterKey(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(value))
	space := false
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			space = false
			continue
		}
		if !space {
			b.WriteByte(' ')
			space = true
		}
	}
	return strings.TrimSpace(b.String())
}

func buildProducers(rows []principalRow, names map[string]string) []Producer {
	if len(rows) == 0 {
		return nil
	}
	out := make([]Producer, 0, len(rows))
	for _, row := range rows {
		out = append(out, Producer{
			Nconst:   row.Nconst,
			Name:     names[row.Nconst],
			Category: row.Category,
		})
	}
	return out
}

func buildCrewMembers(nconsts []string, names map[string]string) []CrewMember {
	if len(nconsts) == 0 {
		return nil
	}
	out := make([]CrewMember, 0, len(nconsts))
	for _, nconst := range nconsts {
		out = append(out, CrewMember{
			Nconst: nconst,
			Name:   names[nconst],
		})
	}
	return out
}

func nullIfNA(val string) string {
	val = strings.TrimSpace(val)
	if val == "\\N" {
		return ""
	}
	return val
}
func intPtrFromPg(val pgtype.Int4) *int {
	if !val.Valid {
		return nil
	}
	v := int(val.Int32)
	return &v
}

func floatPtrFromPg(val pgtype.Float8) *float64 {
	if !val.Valid {
		return nil
	}
	v := val.Float64
	return &v
}

func parseBoolParam(val string) bool {
	switch strings.ToLower(strings.TrimSpace(val)) {
	case "1", "true", "yes", "y":
		return true
	default:
		return false
	}
}

func runServer(pool *pgxpool.Pool, cfg Config, logger *log.Logger) error {
	e := echo.New()
	e.Use(middleware.Recover())
	e.Use(middleware.RequestLogger())

	e.GET("/health", func(c *echo.Context) error {
		return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
	})

	e.GET("/titles/:tconst", getTitleHandler(pool))
	e.GET("/search", searchHandler(pool, cfg.EnablePGSearch))
	e.GET("/discover", discoverHandler(pool))

	addr := ":" + cfg.Port
	logger.Printf("api: listening on %s", addr)
	return e.Start(addr)
}

type SearchItem struct {
	Tconst        string   `json:"tconst"`
	TitleType     string   `json:"titleType"`
	StartYear     *int     `json:"startYear"`
	PrimaryTitle  string   `json:"primaryTitle"`
	OriginalTitle string   `json:"originalTitle"`
	AkaTitles     []string `json:"akaTitles"`
	Score         float64  `json:"score"`
}

type DiscoverItem struct {
	Tconst        string   `json:"tconst"`
	TitleType     string   `json:"titleType"`
	PrimaryTitle  string   `json:"primaryTitle"`
	OriginalTitle string   `json:"originalTitle"`
	StartYear     *int     `json:"startYear"`
	EndYear       *int     `json:"endYear"`
	Genres        []string `json:"genres"`
	AverageRating *float64 `json:"averageRating"`
	NumVotes      *int     `json:"numVotes"`
}

type DiscoverResponse struct {
	Items []DiscoverItem `json:"items"`
	Meta  DiscoverMeta   `json:"meta"`
}

type DiscoverMeta struct {
	Sort           string         `json:"sort"`
	Limit          int            `json:"limit"`
	HasMore        bool           `json:"hasMore"`
	NextCursor     *string        `json:"nextCursor,omitempty"`
	AppliedFilters DiscoverFilter `json:"appliedFilters"`
}

type DiscoverFilter struct {
	Type      string   `json:"type"`
	Genres    []string `json:"genres"`
	YearFrom  *int     `json:"yearFrom,omitempty"`
	YearTo    *int     `json:"yearTo,omitempty"`
	MinVotes  *int     `json:"minVotes,omitempty"`
	MinRating *float64 `json:"minRating,omitempty"`
}

type discoverSort string

const (
	discoverSortPopular  discoverSort = "popular"
	discoverSortTopRated discoverSort = "top_rated"
	discoverSortNewest   discoverSort = "newest"
	discoverSortOldest   discoverSort = "oldest"
)

type discoverCursor struct {
	Sort        discoverSort `json:"sort"`
	Tconst      string       `json:"tconst"`
	VotesKey    *int         `json:"votesKey,omitempty"`
	YearKey     *int         `json:"yearKey,omitempty"`
	RatingKey   *float64     `json:"ratingKey,omitempty"`
	Fingerprint string       `json:"fingerprint"`
}

func getTitleHandler(pool *pgxpool.Pool) echo.HandlerFunc {
	return func(c *echo.Context) error {
		tconst := c.Param("tconst")
		if tconst == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "missing tconst"})
		}

		ctx := c.Request().Context()
		var data []byte
		err := pool.QueryRow(ctx, `SELECT data FROM titles WHERE tconst = $1`, tconst).Scan(&data)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return c.NoContent(http.StatusNotFound)
			}
			return err
		}

		return c.Blob(http.StatusOK, "application/json", data)
	}
}

func searchHandler(pool *pgxpool.Pool, enabled bool) echo.HandlerFunc {
	return func(c *echo.Context) error {
		if !enabled {
			return c.JSON(http.StatusNotImplemented, map[string]string{"error": "search is disabled"})
		}

		query := strings.TrimSpace(c.QueryParam("query"))
		if query == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "query is required"})
		}

		titleType := strings.ToLower(strings.TrimSpace(c.QueryParam("type")))
		var typeList []string
		switch titleType {
		case "series":
			typeList = []string{"tvseries", "tvminiseries", "tvspecial"}
		case "movies":
			typeList = []string{"movie", "tvmovie"}
		default:
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "type must be 'series' or 'movies'"})
		}

		limit := 20
		if raw := c.QueryParam("limit"); raw != "" {
			if parsed, err := strconv.Atoi(raw); err == nil {
				limit = parsed
			}
		}
		if limit < 1 {
			limit = 1
		}
		if limit > 50 {
			limit = 50
		}

		sql := fmt.Sprintf(`
SELECT tconst, title_type, start_year, primary_title, original_title, aka_titles,
       pdb.score(tconst) AS score
FROM title_search
WHERE title_type = ANY($2)
  AND (
    primary_title @@@ pdb.match($1, distance => 1)
    OR original_title @@@ pdb.match($1, distance => 1)
    OR aka_titles @@@ pdb.match($1, distance => 1)
  )
ORDER BY score DESC
LIMIT $3`)

		ctx := c.Request().Context()
		rows, err := pool.Query(ctx, sql, query, typeList, limit)
		if err != nil {
			return err
		}
		defer rows.Close()

		results := make([]SearchItem, 0, limit)
		for rows.Next() {
			var (
				tconst        string
				titleType     string
				startYear     pgtype.Int4
				primaryTitle  string
				originalTitle string
				akaTitles     []string
				score         float64
			)
			if err := rows.Scan(&tconst, &titleType, &startYear, &primaryTitle, &originalTitle, &akaTitles, &score); err != nil {
				return err
			}
			if akaTitles == nil {
				akaTitles = []string{}
			}
			results = append(results, SearchItem{
				Tconst:        tconst,
				TitleType:     titleType,
				StartYear:     intPtrFromPg(startYear),
				PrimaryTitle:  primaryTitle,
				OriginalTitle: originalTitle,
				AkaTitles:     akaTitles,
				Score:         score,
			})
		}
		if err := rows.Err(); err != nil {
			return err
		}

		return c.JSON(http.StatusOK, results)
	}
}

func parseDiscoverSort(raw string) (discoverSort, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "popular":
		return discoverSortPopular, nil
	case "top_rated":
		return discoverSortTopRated, nil
	case "newest":
		return discoverSortNewest, nil
	case "oldest":
		return discoverSortOldest, nil
	default:
		return "", errors.New("invalid sort")
	}
}

func parseDiscoverGenres(c *echo.Context) ([]string, error) {
	rawValues := append([]string{}, c.QueryParams()["genres"]...)
	rawValues = append(rawValues, c.QueryParams()["genre"]...)

	seen := map[string]struct{}{}
	genres := make([]string, 0, 3)
	for _, raw := range rawValues {
		for _, part := range strings.Split(raw, ",") {
			g := strings.ToLower(strings.TrimSpace(part))
			if g == "" {
				continue
			}
			if _, ok := seen[g]; ok {
				continue
			}
			seen[g] = struct{}{}
			genres = append(genres, g)
		}
	}
	if len(genres) > 3 {
		return nil, errors.New("max 3 genres are allowed")
	}
	sort.Strings(genres)
	return genres, nil
}

func encodeDiscoverCursor(cur discoverCursor) (string, error) {
	data, err := json.Marshal(cur)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func decodeDiscoverCursor(raw string) (discoverCursor, error) {
	var cur discoverCursor
	data, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return cur, err
	}
	if err := json.Unmarshal(data, &cur); err != nil {
		return cur, err
	}
	if cur.Tconst == "" || cur.Sort == "" || cur.Fingerprint == "" {
		return cur, errors.New("invalid cursor payload")
	}
	return cur, nil
}

func discoverFilterFingerprint(
	sortMode discoverSort,
	typeGroup string,
	genres []string,
	yearFrom *int,
	yearTo *int,
	minVotes *int,
	minRating *float64,
) string {
	yearFromToken := ""
	if yearFrom != nil {
		yearFromToken = strconv.Itoa(*yearFrom)
	}
	yearToToken := ""
	if yearTo != nil {
		yearToToken = strconv.Itoa(*yearTo)
	}
	minVotesToken := ""
	if minVotes != nil {
		minVotesToken = strconv.Itoa(*minVotes)
	}
	minRatingToken := ""
	if minRating != nil {
		minRatingToken = strconv.FormatFloat(*minRating, 'f', 4, 64)
	}
	return strings.Join([]string{
		string(sortMode),
		typeGroup,
		strings.Join(genres, ","),
		yearFromToken,
		yearToToken,
		minVotesToken,
		minRatingToken,
	}, "|")
}

func discoverOrderClause(sortMode discoverSort) string {
	switch sortMode {
	case discoverSortTopRated:
		return "COALESCE(d.average_rating, -1)::float8 DESC, COALESCE(d.num_votes, -1) DESC, d.tconst DESC"
	case discoverSortNewest:
		return "COALESCE(d.start_year, -1) DESC, COALESCE(d.num_votes, -1) DESC, d.tconst DESC"
	case discoverSortOldest:
		return "COALESCE(d.start_year, 2147483647) ASC, COALESCE(d.num_votes, -1) DESC, d.tconst DESC"
	default:
		return "COALESCE(d.num_votes, -1) DESC, d.tconst DESC"
	}
}

func discoverHandler(pool *pgxpool.Pool) echo.HandlerFunc {
	return func(c *echo.Context) error {
		titleType := strings.ToLower(strings.TrimSpace(c.QueryParam("type")))
		yearFromRaw := strings.TrimSpace(c.QueryParam("year_from"))
		yearToRaw := strings.TrimSpace(c.QueryParam("year_to"))
		sortMode, err := parseDiscoverSort(c.QueryParam("sort"))
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "sort must be one of: popular, top_rated, newest, oldest"})
		}

		var typeGroup string
		switch titleType {
		case "series":
			typeGroup = "series"
		case "movies":
			typeGroup = "movies"
		default:
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "type must be 'series' or 'movies'"})
		}

		genres, err := parseDiscoverGenres(c)
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}

		var yearFrom *int
		if yearFromRaw != "" {
			parsed, err := strconv.Atoi(yearFromRaw)
			if err != nil {
				return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid year_from"})
			}
			yearFrom = &parsed
		}

		var yearTo *int
		if yearToRaw != "" {
			parsed, err := strconv.Atoi(yearToRaw)
			if err != nil {
				return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid year_to"})
			}
			yearTo = &parsed
		}
		if yearFrom != nil && yearTo != nil && *yearFrom > *yearTo {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "year_from must be <= year_to"})
		}

		var minVotes *int
		if raw := strings.TrimSpace(c.QueryParam("min_votes")); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil || parsed < 0 {
				return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid min_votes"})
			}
			minVotes = &parsed
		}

		var minRating *float64
		if raw := strings.TrimSpace(c.QueryParam("min_rating")); raw != "" {
			parsed, err := strconv.ParseFloat(raw, 64)
			if err != nil || parsed < 0 || parsed > 10 {
				return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid min_rating"})
			}
			minRating = &parsed
		}

		limit := 20
		if raw := c.QueryParam("limit"); raw != "" {
			if parsed, err := strconv.Atoi(raw); err == nil {
				limit = parsed
			}
		}
		if limit < 1 {
			limit = 1
		}
		if limit > 50 {
			limit = 50
		}

		fingerprint := discoverFilterFingerprint(sortMode, typeGroup, genres, yearFrom, yearTo, minVotes, minRating)
		var cursor *discoverCursor
		if raw := strings.TrimSpace(c.QueryParam("cursor")); raw != "" {
			parsed, err := decodeDiscoverCursor(raw)
			if err != nil {
				return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid cursor"})
			}
			if parsed.Sort != sortMode {
				return c.JSON(http.StatusBadRequest, map[string]string{"error": "cursor sort does not match requested sort"})
			}
			if parsed.Fingerprint != fingerprint {
				return c.JSON(http.StatusBadRequest, map[string]string{"error": "cursor does not match requested filters"})
			}
			cursor = &parsed
		}

		args := make([]any, 0, 16)
		args = append(args, typeGroup)
		param := 2
		where := "WHERE d.type_group = $1"

		if yearFrom != nil {
			where += fmt.Sprintf(" AND d.start_year >= $%d", param)
			args = append(args, *yearFrom)
			param++
		}
		if yearTo != nil {
			where += fmt.Sprintf(" AND d.start_year <= $%d", param)
			args = append(args, *yearTo)
			param++
		}
		if minVotes != nil {
			where += fmt.Sprintf(" AND COALESCE(d.num_votes, 0) >= $%d", param)
			args = append(args, *minVotes)
			param++
		}
		if minRating != nil {
			where += fmt.Sprintf(" AND COALESCE(d.average_rating, 0)::float8 >= $%d", param)
			args = append(args, *minRating)
			param++
		}
		for _, genre := range genres {
			where += fmt.Sprintf(` AND EXISTS (
    SELECT 1
    FROM discover_genre dg
    WHERE dg.type_group = d.type_group
      AND dg.tconst = d.tconst
      AND dg.genre = $%d
)`, param)
			args = append(args, genre)
			param++
		}

		if cursor != nil {
			switch sortMode {
			case discoverSortTopRated:
				if cursor.RatingKey == nil || cursor.VotesKey == nil {
					return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid cursor"})
				}
				where += fmt.Sprintf(" AND (COALESCE(d.average_rating, -1)::float8, COALESCE(d.num_votes, -1), d.tconst) < ($%d, $%d, $%d)", param, param+1, param+2)
				args = append(args, *cursor.RatingKey, *cursor.VotesKey, cursor.Tconst)
				param += 3
			case discoverSortNewest:
				if cursor.YearKey == nil || cursor.VotesKey == nil {
					return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid cursor"})
				}
				where += fmt.Sprintf(" AND (COALESCE(d.start_year, -1), COALESCE(d.num_votes, -1), d.tconst) < ($%d, $%d, $%d)", param, param+1, param+2)
				args = append(args, *cursor.YearKey, *cursor.VotesKey, cursor.Tconst)
				param += 3
			case discoverSortOldest:
				if cursor.YearKey == nil || cursor.VotesKey == nil {
					return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid cursor"})
				}
				where += fmt.Sprintf(" AND (COALESCE(d.start_year, 2147483647) > $%d OR (COALESCE(d.start_year, 2147483647) = $%d AND (COALESCE(d.num_votes, -1) < $%d OR (COALESCE(d.num_votes, -1) = $%d AND d.tconst < $%d))))", param, param, param+1, param+1, param+2)
				args = append(args, *cursor.YearKey, *cursor.VotesKey, cursor.Tconst)
				param += 3
			default:
				if cursor.VotesKey == nil {
					return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid cursor"})
				}
				where += fmt.Sprintf(" AND (COALESCE(d.num_votes, -1), d.tconst) < ($%d, $%d)", param, param+1)
				args = append(args, *cursor.VotesKey, cursor.Tconst)
				param += 2
			}
		}

		sqlLimit := limit + 1
		args = append(args, sqlLimit)
		orderClause := discoverOrderClause(sortMode)
		sql := fmt.Sprintf(`
SELECT d.tconst, d.title_type, d.primary_title, d.original_title, d.start_year, d.end_year,
       d.genres, d.average_rating::float8, d.num_votes,
       COALESCE(d.start_year, -1) AS sort_year_desc,
       COALESCE(d.start_year, 2147483647) AS sort_year_asc,
       COALESCE(d.num_votes, -1) AS sort_votes,
       COALESCE(d.average_rating, -1)::float8 AS sort_rating
FROM discover_core d
%s
ORDER BY %s
LIMIT $%d`, where, orderClause, param)

		ctx := c.Request().Context()
		rows, err := pool.Query(ctx, sql, args...)
		if err != nil {
			return err
		}
		defer rows.Close()

		results := make([]DiscoverItem, 0, sqlLimit)
		rowCursors := make([]discoverCursor, 0, sqlLimit)
		for rows.Next() {
			var (
				tconst        string
				ttype         string
				primaryTitle  string
				originalTitle string
				startYear     pgtype.Int4
				endYear       pgtype.Int4
				genresArr     []string
				avgRating     pgtype.Float8
				numVotes      pgtype.Int4
				sortYearDesc  int32
				sortYearAsc   int32
				sortVotes     int32
				sortRating    float64
			)
			if err := rows.Scan(
				&tconst,
				&ttype,
				&primaryTitle,
				&originalTitle,
				&startYear,
				&endYear,
				&genresArr,
				&avgRating,
				&numVotes,
				&sortYearDesc,
				&sortYearAsc,
				&sortVotes,
				&sortRating,
			); err != nil {
				return err
			}
			if genresArr == nil {
				genresArr = []string{}
			}
			item := DiscoverItem{
				Tconst:        tconst,
				TitleType:     ttype,
				PrimaryTitle:  primaryTitle,
				OriginalTitle: originalTitle,
				StartYear:     intPtrFromPg(startYear),
				EndYear:       intPtrFromPg(endYear),
				Genres:        genresArr,
				AverageRating: floatPtrFromPg(avgRating),
				NumVotes:      intPtrFromPg(numVotes),
			}
			results = append(results, item)

			cur := discoverCursor{
				Sort:        sortMode,
				Tconst:      tconst,
				Fingerprint: fingerprint,
			}
			voteKey := int(sortVotes)
			cur.VotesKey = &voteKey
			switch sortMode {
			case discoverSortTopRated:
				ratingKey := sortRating
				cur.RatingKey = &ratingKey
			case discoverSortNewest:
				yearKey := int(sortYearDesc)
				cur.YearKey = &yearKey
			case discoverSortOldest:
				yearKey := int(sortYearAsc)
				cur.YearKey = &yearKey
			}
			rowCursors = append(rowCursors, cur)
		}
		if err := rows.Err(); err != nil {
			return err
		}

		hasMore := len(results) > limit
		if hasMore {
			results = results[:limit]
			rowCursors = rowCursors[:limit]
		}

		var nextCursor *string
		if hasMore && len(rowCursors) > 0 {
			encoded, err := encodeDiscoverCursor(rowCursors[len(rowCursors)-1])
			if err != nil {
				return err
			}
			nextCursor = &encoded
		}

		resp := DiscoverResponse{
			Items: results,
			Meta: DiscoverMeta{
				Sort:       string(sortMode),
				Limit:      limit,
				HasMore:    hasMore,
				NextCursor: nextCursor,
				AppliedFilters: DiscoverFilter{
					Type:      titleType,
					Genres:    genres,
					YearFrom:  yearFrom,
					YearTo:    yearTo,
					MinVotes:  minVotes,
					MinRating: minRating,
				},
			},
		}

		return c.JSON(http.StatusOK, resp)
	}
}
