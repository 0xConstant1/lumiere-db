package etl

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

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
		return fmt.Errorf("download datasets: %w", err)
	}

	if err := runSQLScripts(ctx, pool, cfg, logger, "schema.sql", "staging.sql"); err != nil {
		return err
	}
	if err := loadStagingInBatches(ctx, pool, cfg, logger); err != nil {
		return fmt.Errorf("load staging data: %w", err)
	}

	preBuildScripts := []string{
		"index_staging.sql",
		"episodes_enriched.sql",
		"filter_titles.sql",
		"create_next.sql",
	}
	if err := runSQLScripts(ctx, pool, cfg, logger, preBuildScripts...); err != nil {
		return err
	}

	if err := buildTitlesInBatches(ctx, pool, cfg, logger); err != nil {
		return fmt.Errorf("build titles: %w", err)
	}

	postBuildScripts := []string{
		"discover_next.sql",
		"indexes_next.sql",
		"analyze.sql",
	}
	if cfg.EnablePGSearch {
		postBuildScripts = append(postBuildScripts, "pg_search_next.sql")
	}
	postBuildScripts = append(postBuildScripts, "swap.sql", "analyze_final.sql")
	if cfg.KeepStaging {
		logger.Printf("etl: cleanup skipped (ETL_KEEP_STAGING=true)")
	} else {
		postBuildScripts = append(postBuildScripts, "cleanup.sql")
	}
	if err := runSQLScripts(ctx, pool, cfg, logger, postBuildScripts...); err != nil {
		return err
	}

	logger.Printf("etl: finished")
	return nil
}

func runSQLScripts(ctx context.Context, pool *pgxpool.Pool, cfg Config, logger *log.Logger, scripts ...string) error {
	for _, script := range scripts {
		if err := runSQLFile(ctx, pool, cfg, logger, script); err != nil {
			return fmt.Errorf("run %s: %w", script, err)
		}
	}
	return nil
}
