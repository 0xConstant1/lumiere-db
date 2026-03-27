package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	etlcore "lumiere-api/internal/etl"
)

func Load() (etlcore.Config, error) {
	cfg := etlcore.Config{}
	cfg.DatabaseURL = envString("DATABASE_URL", "")
	if cfg.DatabaseURL == "" {
		return cfg, errors.New("DATABASE_URL is required")
	}

	cfg.BaseURL = envString("IMDB_BASE_URL", "https://datasets.imdbws.com")
	cfg.DataDir = envString("IMDB_DATA_DIR", "/data")
	cfg.DatasetDate = envString("DATASET_DATE", time.Now().UTC().Format("2006-01-02"))
	var err error

	cfg.SchemaVersion, err = envInt("SCHEMA_VERSION", 1)
	if err != nil {
		return cfg, err
	}
	cfg.EnablePGSearch, err = envBool("ENABLE_PG_SEARCH", true)
	if err != nil {
		return cfg, err
	}
	cfg.RunETL, err = envBool("RUN_ETL", true)
	if err != nil {
		return cfg, err
	}
	cfg.ForceDownload, err = envBool("IMDB_FORCE_DOWNLOAD", false)
	if err != nil {
		return cfg, err
	}
	cfg.KeepStaging, err = envBool("ETL_KEEP_STAGING", false)
	if err != nil {
		return cfg, err
	}
	loadBatchSize, err := envInt("ETL_LOAD_BATCH_SIZE", 10000)
	if err != nil {
		return cfg, err
	}
	cfg.LoadBatchSize = clampPositive(loadBatchSize, 10000)
	batchSize, err := envInt("ETL_BATCH_SIZE", 10000)
	if err != nil {
		return cfg, err
	}
	cfg.BatchSize = clampPositive(batchSize, 10000)
	maxActors, err := envInt("ETL_MAX_ACTORS", 10)
	if err != nil {
		return cfg, err
	}
	cfg.MaxActors = clampNonNegative(maxActors)
	maxProducers, err := envInt("ETL_MAX_PRODUCERS", 1)
	if err != nil {
		return cfg, err
	}
	cfg.MaxProducers = clampNonNegative(maxProducers)
	maxWriters, err := envInt("ETL_MAX_WRITERS", 1)
	if err != nil {
		return cfg, err
	}
	cfg.MaxWriters = clampNonNegative(maxWriters)
	maxDirectors, err := envInt("ETL_MAX_DIRECTORS", 1)
	if err != nil {
		return cfg, err
	}
	cfg.MaxDirectors = clampNonNegative(maxDirectors)
	cfg.MaxParallelWorkers = strings.TrimSpace(os.Getenv("ETL_MAX_PARALLEL_WORKERS"))
	cfg.WorkMem = strings.TrimSpace(os.Getenv("ETL_WORK_MEM"))
	cfg.MaintenanceWorkMem = strings.TrimSpace(os.Getenv("ETL_MAINTENANCE_WORK_MEM"))
	readerBufferSize, err := envInt("ETL_READER_BUFFER", 256*1024)
	if err != nil {
		return cfg, err
	}
	cfg.ReaderBufferSize = clampPositive(readerBufferSize, 256*1024)
	downloadConcurrency, err := envInt("ETL_DOWNLOAD_CONCURRENCY", 3)
	if err != nil {
		return cfg, err
	}
	cfg.DownloadConcurrency = clampPositive(downloadConcurrency, 3)
	minNumVotes, err := envInt("ETL_MIN_NUMVOTES", 1)
	if err != nil {
		return cfg, err
	}
	cfg.MinNumVotes = clampNonNegative(minNumVotes)
	cfg.DBMaxWalSize = strings.TrimSpace(os.Getenv("ETL_DB_MAX_WAL_SIZE"))
	cfg.DBMinWalSize = strings.TrimSpace(os.Getenv("ETL_DB_MIN_WAL_SIZE"))
	cfg.DBCheckpointTimeout = strings.TrimSpace(os.Getenv("ETL_DB_CHECKPOINT_TIMEOUT"))
	cfg.DBCheckpointCompletionTarget = strings.TrimSpace(os.Getenv("ETL_DB_CHECKPOINT_COMPLETION_TARGET"))
	cfg.DBWalCompression = strings.TrimSpace(os.Getenv("ETL_DB_WAL_COMPRESSION"))
	cfg.DBMaxParallelWorkers = strings.TrimSpace(os.Getenv("ETL_DB_MAX_PARALLEL_WORKERS"))
	cfg.DBMaxParallelMaintenanceWorkers = strings.TrimSpace(os.Getenv("ETL_DB_MAX_PARALLEL_MAINTENANCE_WORKERS"))
	cfg.ScheduleEnabled, err = envBool("ETL_SCHEDULE_ENABLED", true)
	if err != nil {
		return cfg, err
	}
	cfg.PollInterval, err = envDuration("ETL_POLL_INTERVAL", time.Hour)
	if err != nil {
		return cfg, err
	}
	if cfg.PollInterval <= 0 {
		return cfg, errors.New("ETL_POLL_INTERVAL must be greater than 0")
	}
	cfg.BootstrapBlocking, err = envBool("ETL_BOOTSTRAP_BLOCKING", true)
	if err != nil {
		return cfg, err
	}
	cfg.ForceRebuild, err = envBool("IMDB_FORCE_REBUILD", false)
	if err != nil {
		return cfg, err
	}
	cfg.SwapLockTimeout = envString("ETL_SWAP_LOCK_TIMEOUT", "30s")
	if strings.Contains(cfg.SwapLockTimeout, "'") || strings.Contains(cfg.SwapLockTimeout, ";") {
		return cfg, errors.New("ETL_SWAP_LOCK_TIMEOUT contains invalid character")
	}
	cfg.Port = envString("PORT", "8000")
	cfg.CORSAllowOrigins, err = envCSV("CORS_ALLOW_ORIGINS", []string{
		"http://localhost:5173",
		"http://127.0.0.1:5173",
	})
	if err != nil {
		return cfg, err
	}
	for _, origin := range cfg.CORSAllowOrigins {
		if origin == "*" {
			return cfg, errors.New("CORS_ALLOW_ORIGINS must not contain '*'")
		}
		if !strings.HasPrefix(origin, "http://") && !strings.HasPrefix(origin, "https://") {
			return cfg, fmt.Errorf("CORS_ALLOW_ORIGINS has invalid origin %q", origin)
		}
	}

	sqlDir, err := resolveSQLDir(os.Getenv("ETL_SQL_DIR"))
	if err != nil {
		return cfg, err
	}
	cfg.SQLDir = sqlDir

	return cfg, nil
}

func envString(key, def string) string {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return def
	}
	return val
}

func envBool(key string, def bool) (bool, error) {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return def, nil
	}
	switch strings.ToLower(val) {
	case "1", "true", "yes", "y":
		return true, nil
	case "0", "false", "no", "n":
		return false, nil
	default:
		return false, fmt.Errorf("%s has invalid boolean value %q", key, val)
	}
}

func envInt(key string, def int) (int, error) {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return def, nil
	}
	parsed, err := strconv.Atoi(val)
	if err != nil {
		return 0, fmt.Errorf("%s has invalid integer value %q", key, val)
	}
	return parsed, nil
}

func envDuration(key string, def time.Duration) (time.Duration, error) {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return def, nil
	}
	dur, err := time.ParseDuration(val)
	if err != nil {
		return 0, fmt.Errorf("%s has invalid duration value %q", key, val)
	}
	return dur, nil
}

func envCSV(key string, def []string) ([]string, error) {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		out := make([]string, len(def))
		copy(out, def)
		return out, nil
	}

	parts := strings.Split(val, ",")
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		entry := strings.TrimSpace(part)
		if entry == "" {
			return nil, fmt.Errorf("%s contains an empty value", key)
		}
		if _, ok := seen[entry]; ok {
			continue
		}
		seen[entry] = struct{}{}
		out = append(out, entry)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%s must contain at least one value", key)
	}
	return out, nil
}

func resolveSQLDir(envVal string) (string, error) {
	envVal = strings.TrimSpace(envVal)
	if envVal != "" {
		info, err := os.Stat(envVal)
		if err != nil {
			return "", fmt.Errorf("ETL_SQL_DIR path %q is invalid: %w", envVal, err)
		}
		if !info.IsDir() {
			return "", fmt.Errorf("ETL_SQL_DIR path %q is not a directory", envVal)
		}
		return envVal, nil
	}
	if info, err := os.Stat("etl"); err == nil && info.IsDir() {
		return "etl", nil
	}
	return "", errors.New("ETL SQL dir not found at ./etl; set ETL_SQL_DIR")
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
