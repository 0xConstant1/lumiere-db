package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	configapi "lumiere-api/internal/config"
	dbapi "lumiere-api/internal/db"
	etlcore "lumiere-api/internal/etl"
	serverapi "lumiere-api/internal/http/server"
)

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := configapi.Load()
	if err != nil {
		logger.Fatalf("config: %v", err)
	}

	logger.Printf("api: starting with DATABASE_URL=%s", dbapi.RedactURL(cfg.DatabaseURL))

	pool, err := dbapi.ConnectWithRetry(ctx, cfg.DatabaseURL, logger)
	if err != nil {
		logger.Fatalf("%v", err)
	}
	defer pool.Close()

	etlCfg := cfg.ETL.Runtime
	if cfg.ETL.Enabled {
		if cfg.ETL.ScheduleEnabled {
			needsBootstrap, err := etlcore.ShouldRunBlockingBootstrap(ctx, pool)
			if err != nil {
				logger.Fatalf("etl bootstrap check failed: %v", err)
			}
			if needsBootstrap && cfg.ETL.BootstrapBlocking {
				logger.Printf("etl: bootstrap run started (titles table is empty)")
				if err := etlcore.RunScheduledRebuildCycle(ctx, pool, etlCfg, logger); err != nil {
					logger.Fatalf("etl bootstrap failed: %v", err)
				}
			} else if needsBootstrap {
				logger.Printf("etl: bootstrap run deferred to scheduler (ETL_BOOTSTRAP_BLOCKING=false)")
			}
			etlcore.StartScheduler(ctx, pool, etlCfg, logger)
		} else {
			if err := etlcore.Run(ctx, pool, etlCfg, logger); err != nil {
				logger.Fatalf("etl failed: %v", err)
			}
		}
	} else {
		logger.Printf("etl: skipped (RUN_ETL=false)")
	}

	if err := serverapi.Start(ctx, pool, cfg.Port, cfg.EnablePGSearch, cfg.CORSAllowOrigins, logger); err != nil {
		logger.Fatalf("api: %v", err)
	}
}
