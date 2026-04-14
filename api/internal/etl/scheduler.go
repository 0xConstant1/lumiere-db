package etl

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type imdbFileState struct {
	FileName      string
	LastModified  time.Time
	ContentLength int64
}

const (
	etlSchedulerLockKey        int64         = 573901235911
	sourceStateTable                         = "etl_source_state"
	pendingStateTable                        = "etl_pending_source_state"
	manifestStabilityDelay     time.Duration = 30 * time.Second
	manifestStabilityMaxChecks               = 10
)

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
	if err := ensureSchedulerStateTables(ctx, pool); err != nil {
		return fmt.Errorf("ensure scheduler state tables: %w", err)
	}

	manifest, err := probeIMDbFileStates(ctx, cfg)
	if err != nil {
		return fmt.Errorf("probe imdb headers: %w", err)
	}

	hasTitles, err := hasAnyTitles(ctx, pool)
	if err != nil {
		return fmt.Errorf("check titles: %w", err)
	}

	if cfg.ForceRebuild {
		logger.Printf("etl: force rebuild enabled (IMDB_FORCE_REBUILD=true)")
		return rebuildFromManifest(ctx, pool, cfg, logger, manifest)
	}

	if !hasTitles {
		stableManifest, err := waitForStableManifest(ctx, cfg, logger, manifest)
		if err != nil {
			return err
		}
		return rebuildFromManifest(ctx, pool, cfg, logger, stableManifest)
	}

	prevState, err := loadSourceState(ctx, pool)
	if err != nil {
		return fmt.Errorf("load etl source state: %w", err)
	}
	if manifestsEqual(prevState, manifest) {
		if err := clearPendingSourceState(ctx, pool); err != nil {
			return fmt.Errorf("clear pending source state: %w", err)
		}
		logger.Printf("etl: no upstream manifest change detected")
		return nil
	}

	pendingState, err := loadPendingSourceState(ctx, pool)
	if err != nil {
		return fmt.Errorf("load pending source state: %w", err)
	}
	if !manifestsEqual(pendingState, manifest) {
		if err := savePendingSourceState(ctx, pool, manifest); err != nil {
			return fmt.Errorf("save pending source state: %w", err)
		}
		logger.Printf("etl: manifest change detected; waiting for the next poll to confirm stability")
		return nil
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

	latestManifest, err := probeIMDbFileStates(ctx, cfg)
	if err != nil {
		return fmt.Errorf("re-probe imdb headers: %w", err)
	}
	if !manifestsEqual(manifest, latestManifest) {
		if err := savePendingSourceState(ctx, pool, latestManifest); err != nil {
			return fmt.Errorf("update pending source state: %w", err)
		}
		logger.Printf("etl: manifest changed again while waiting on the lock; waiting for another stable poll")
		return nil
	}

	prevState, err = loadSourceState(ctx, pool)
	if err != nil {
		return fmt.Errorf("reload etl source state: %w", err)
	}
	if manifestsEqual(prevState, latestManifest) {
		if err := clearPendingSourceState(ctx, pool); err != nil {
			return fmt.Errorf("clear pending source state: %w", err)
		}
		logger.Printf("etl: manifest already processed by another instance")
		return nil
	}

	return rebuildFromManifestLocked(ctx, pool, cfg, logger, latestManifest)
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

func manifestsEqual(left map[string]imdbFileState, right map[string]imdbFileState) bool {
	if len(left) != len(right) {
		return false
	}
	for _, file := range imdbDatasetFiles {
		leftEntry, okLeft := left[file]
		rightEntry, okRight := right[file]
		if !okLeft || !okRight {
			return false
		}
		if !leftEntry.LastModified.Equal(rightEntry.LastModified) {
			return false
		}
		if leftEntry.ContentLength != rightEntry.ContentLength {
			return false
		}
	}
	return true
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

func ensureSchedulerStateTables(ctx context.Context, pool *pgxpool.Pool) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS etl_source_state (
  file_name TEXT PRIMARY KEY,
  last_modified TIMESTAMPTZ NOT NULL,
  content_length BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`,
		`CREATE TABLE IF NOT EXISTS etl_pending_source_state (
  file_name TEXT PRIMARY KEY,
  last_modified TIMESTAMPTZ NOT NULL,
  content_length BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`,
	}
	for _, stmt := range statements {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func waitForStableManifest(ctx context.Context, cfg Config, logger *log.Logger, manifest map[string]imdbFileState) (map[string]imdbFileState, error) {
	current := manifest
	for attempt := 1; attempt <= manifestStabilityMaxChecks; attempt++ {
		logger.Printf(
			"etl: bootstrap manifest observed; waiting %s for stability check (%d/%d)",
			manifestStabilityDelay,
			attempt,
			manifestStabilityMaxChecks,
		)

		timer := time.NewTimer(manifestStabilityDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("wait for stable imdb manifest: %w", ctx.Err())
		case <-timer.C:
		}

		nextManifest, err := probeIMDbFileStates(ctx, cfg)
		if err != nil {
			return nil, fmt.Errorf("re-probe imdb headers: %w", err)
		}
		if manifestsEqual(current, nextManifest) {
			logger.Printf("etl: imdb manifest is stable")
			return nextManifest, nil
		}

		logger.Printf("etl: imdb manifest still changing; delaying bootstrap rebuild")
		current = nextManifest
	}

	return nil, errors.New("imdb manifest did not stabilize before rebuild")
}

func rebuildFromManifest(ctx context.Context, pool *pgxpool.Pool, cfg Config, logger *log.Logger, manifest map[string]imdbFileState) error {
	lockConn, locked, err := acquireETLSchedulerLock(ctx, pool)
	if err != nil {
		return fmt.Errorf("acquire etl lock: %w", err)
	}
	if !locked {
		logger.Printf("etl: skip rebuild (lock held by another instance)")
		return nil
	}
	defer releaseETLSchedulerLock(lockConn)

	return rebuildFromManifestLocked(ctx, pool, cfg, logger, manifest)
}

func rebuildFromManifestLocked(ctx context.Context, pool *pgxpool.Pool, cfg Config, logger *log.Logger, manifest map[string]imdbFileState) error {
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
	if err := clearPendingSourceState(ctx, pool); err != nil {
		return fmt.Errorf("clear pending source state: %w", err)
	}
	logger.Printf("etl: scheduled rebuild completed")
	return nil
}
