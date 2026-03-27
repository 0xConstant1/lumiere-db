package etl

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
)

func Run(ctx context.Context, pool *pgxpool.Pool, cfg Config, logger *log.Logger) error {
	return runETL(ctx, pool, cfg, logger)
}

func StartScheduler(ctx context.Context, pool *pgxpool.Pool, cfg Config, logger *log.Logger) {
	startETLScheduler(ctx, pool, cfg, logger)
}

func RunScheduledRebuildCycle(ctx context.Context, pool *pgxpool.Pool, cfg Config, logger *log.Logger) error {
	return runScheduledRebuildCycle(ctx, pool, cfg, logger)
}

func ShouldRunBlockingBootstrap(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	return shouldRunBlockingBootstrap(ctx, pool)
}
