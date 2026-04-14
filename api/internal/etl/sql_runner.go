package etl

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

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
	var workMem, maintenanceMem, maxParallelWorkers string
	err := conn.QueryRow(ctx, `
SELECT current_setting('work_mem'),
       current_setting('maintenance_work_mem'),
       current_setting('max_parallel_workers_per_gather')`).Scan(&workMem, &maintenanceMem, &maxParallelWorkers)
	if err != nil {
		return fmt.Errorf("read etl settings: %w", err)
	}
	logger.Printf("etl: session settings work_mem=%s maintenance_work_mem=%s max_parallel_workers_per_gather=%s", workMem, maintenanceMem, maxParallelWorkers)
	return nil
}
