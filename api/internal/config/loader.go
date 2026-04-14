package config

import (
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	etlcore "lumiere-api/internal/etl"
)

func Load() (Config, error) {
	cfg := Config{}
	cfg.DatabaseURL = envString("DATABASE_URL", "")
	if cfg.DatabaseURL == "" {
		return cfg, errors.New("DATABASE_URL is required")
	}

	cfg.ETL.Runtime.BaseURL = envString("IMDB_BASE_URL", "https://datasets.imdbws.com")
	cfg.ETL.Runtime.DataDir = envString("IMDB_DATA_DIR", "/data")
	cfg.ETL.Runtime.DatasetDate = envString("DATASET_DATE", time.Now().UTC().Format("2006-01-02"))
	var err error

	cfg.ETL.Runtime.SchemaVersion, err = envInt("SCHEMA_VERSION", 1)
	if err != nil {
		return cfg, err
	}
	cfg.EnablePGSearch, err = envBool("ENABLE_PG_SEARCH", true)
	if err != nil {
		return cfg, err
	}
	cfg.ETL.Runtime.EnablePGSearch = cfg.EnablePGSearch
	cfg.ETL.Enabled, err = envBool("RUN_ETL", true)
	if err != nil {
		return cfg, err
	}
	cfg.ETL.Runtime.ForceDownload, err = envBool("IMDB_FORCE_DOWNLOAD", false)
	if err != nil {
		return cfg, err
	}
	cfg.ETL.Runtime.KeepStaging, err = envBool("ETL_KEEP_STAGING", false)
	if err != nil {
		return cfg, err
	}
	loadBatchSize, err := envInt("ETL_LOAD_BATCH_SIZE", 10000)
	if err != nil {
		return cfg, err
	}
	cfg.ETL.Runtime.LoadBatchSize = clampPositive(loadBatchSize, 10000)
	batchSize, err := envInt("ETL_BATCH_SIZE", 10000)
	if err != nil {
		return cfg, err
	}
	cfg.ETL.Runtime.BatchSize = clampPositive(batchSize, 10000)
	maxActors, err := envInt("ETL_MAX_ACTORS", 10)
	if err != nil {
		return cfg, err
	}
	cfg.ETL.Runtime.MaxActors = clampNonNegative(maxActors)
	maxProducers, err := envInt("ETL_MAX_PRODUCERS", 1)
	if err != nil {
		return cfg, err
	}
	cfg.ETL.Runtime.MaxProducers = clampNonNegative(maxProducers)
	maxWriters, err := envInt("ETL_MAX_WRITERS", 1)
	if err != nil {
		return cfg, err
	}
	cfg.ETL.Runtime.MaxWriters = clampNonNegative(maxWriters)
	maxDirectors, err := envInt("ETL_MAX_DIRECTORS", 1)
	if err != nil {
		return cfg, err
	}
	cfg.ETL.Runtime.MaxDirectors = clampNonNegative(maxDirectors)
	cfg.ETL.Runtime.MaxParallelWorkers, err = envOptionalInt("ETL_MAX_PARALLEL_WORKERS")
	if err != nil {
		return cfg, err
	}
	cfg.ETL.Runtime.WorkMem, err = envOptionalMemorySize("ETL_WORK_MEM")
	if err != nil {
		return cfg, err
	}
	cfg.ETL.Runtime.MaintenanceWorkMem, err = envOptionalMemorySize("ETL_MAINTENANCE_WORK_MEM")
	if err != nil {
		return cfg, err
	}
	readerBufferSize, err := envInt("ETL_READER_BUFFER", 256*1024)
	if err != nil {
		return cfg, err
	}
	cfg.ETL.Runtime.ReaderBufferSize = clampPositive(readerBufferSize, 256*1024)
	downloadConcurrency, err := envInt("ETL_DOWNLOAD_CONCURRENCY", 3)
	if err != nil {
		return cfg, err
	}
	cfg.ETL.Runtime.DownloadConcurrency = clampPositive(downloadConcurrency, 3)
	minNumVotes, err := envInt("ETL_MIN_NUMVOTES", 1)
	if err != nil {
		return cfg, err
	}
	cfg.ETL.Runtime.MinNumVotes = clampNonNegative(minNumVotes)
	cfg.ETL.ScheduleEnabled, err = envBool("ETL_SCHEDULE_ENABLED", true)
	if err != nil {
		return cfg, err
	}
	cfg.ETL.Runtime.PollInterval, err = envDuration("ETL_POLL_INTERVAL", time.Hour)
	if err != nil {
		return cfg, err
	}
	if cfg.ETL.Runtime.PollInterval <= 0 {
		return cfg, errors.New("ETL_POLL_INTERVAL must be greater than 0")
	}
	cfg.ETL.BootstrapBlocking, err = envBool("ETL_BOOTSTRAP_BLOCKING", true)
	if err != nil {
		return cfg, err
	}
	cfg.ETL.Runtime.ForceRebuild, err = envBool("IMDB_FORCE_REBUILD", false)
	if err != nil {
		return cfg, err
	}
	cfg.ETL.Runtime.SwapLockTimeout, err = envDuration("ETL_SWAP_LOCK_TIMEOUT", 30*time.Second)
	if err != nil {
		return cfg, err
	}
	if cfg.ETL.Runtime.SwapLockTimeout <= 0 {
		return cfg, errors.New("ETL_SWAP_LOCK_TIMEOUT must be greater than 0")
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
	cfg.ETL.Runtime.SQLDir = sqlDir

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

func envOptionalInt(key string) (*int, error) {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return nil, nil
	}
	parsed, err := strconv.Atoi(val)
	if err != nil {
		return nil, fmt.Errorf("%s has invalid integer value %q", key, val)
	}
	if parsed < 0 {
		return nil, fmt.Errorf("%s must be greater than or equal to 0", key)
	}
	return &parsed, nil
}

func envOptionalMemorySize(key string) (etlcore.MemorySize, error) {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return 0, nil
	}
	size, err := parseMemorySize(val)
	if err != nil {
		return 0, fmt.Errorf("%s has invalid memory value %q: %w", key, val, err)
	}
	return size, nil
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

func parseMemorySize(raw string) (etlcore.MemorySize, error) {
	raw = strings.TrimSpace(raw)
	idx := 0
	for idx < len(raw) && raw[idx] >= '0' && raw[idx] <= '9' {
		idx++
	}
	if idx == 0 {
		return 0, errors.New("missing numeric size")
	}

	value, err := strconv.ParseInt(raw[:idx], 10, 64)
	if err != nil {
		return 0, err
	}
	if value <= 0 {
		return 0, errors.New("size must be greater than 0")
	}

	unit := strings.ToLower(strings.TrimSpace(raw[idx:]))
	multiplier, ok := memoryUnitMultiplier(unit)
	if !ok {
		return 0, fmt.Errorf("unsupported unit %q", unit)
	}
	if value > math.MaxInt64/multiplier {
		return 0, errors.New("size overflows int64")
	}
	return etlcore.MemorySize(value * multiplier), nil
}

func memoryUnitMultiplier(unit string) (int64, bool) {
	switch unit {
	case "", "k", "kb", "kib":
		return 1024, true
	case "b":
		return 1, true
	case "m", "mb", "mib":
		return 1024 * 1024, true
	case "g", "gb", "gib":
		return 1024 * 1024 * 1024, true
	case "t", "tb", "tib":
		return 1024 * 1024 * 1024 * 1024, true
	default:
		return 0, false
	}
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
