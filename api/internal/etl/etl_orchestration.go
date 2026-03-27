package etl

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

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
	unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = conn.Exec(unlockCtx, "SELECT pg_advisory_unlock($1)", etlSchedulerLockKey)
	conn.Release()
}

func downloadDatasets(ctx context.Context, cfg Config, logger *log.Logger) error {
	baseURL := strings.TrimRight(cfg.BaseURL, "/")
	workerCount := min(clampPositive(cfg.DownloadConcurrency, 3), len(imdbDatasetFiles))

	jobs := make(chan string, len(imdbDatasetFiles))
	for _, file := range imdbDatasetFiles {
		jobs <- file
	}
	close(jobs)

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	for range workerCount {
		wg.Go(func() {
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
		})
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
	const downloadRequestTimeout = 2 * time.Hour

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

	client := &http.Client{Timeout: downloadRequestTimeout}
	resp, err := client.Do(req)
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
	cleanupTmp := true
	defer func() {
		if cleanupTmp {
			_ = os.Remove(tmp)
		}
	}()

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

	if err := os.Remove(dest); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("download %s: remove existing destination: %w", filename, err)
	}
	if err := os.Rename(tmp, dest); err != nil {
		return fmt.Errorf("download %s: %w", filename, err)
	}
	cleanupTmp = false

	return nil
}
