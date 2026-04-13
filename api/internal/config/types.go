package config

import (
	"time"

	etlcore "lumiere-api/internal/etl"
)

type Config struct {
	DatabaseURL      string
	Port             string
	EnablePGSearch   bool
	CORSAllowOrigins []string
	ETL              ETLConfig
}

type ETLConfig struct {
	Enabled             bool
	ScheduleEnabled     bool
	BootstrapBlocking   bool
	BaseURL             string
	DataDir             string
	SQLDir              string
	DatasetDate         string
	SchemaVersion       int
	EnablePGSearch      bool
	ForceDownload       bool
	KeepStaging         bool
	LoadBatchSize       int
	BatchSize           int
	MaxActors           int
	MaxProducers        int
	MaxWriters          int
	MaxDirectors        int
	MaxParallelWorkers  string
	WorkMem             string
	MaintenanceWorkMem  string
	ReaderBufferSize    int
	DownloadConcurrency int
	MinNumVotes         int
	PollInterval        time.Duration
	ForceRebuild        bool
	SwapLockTimeout     string
}

func (c ETLConfig) Core() etlcore.Config {
	return etlcore.Config{
		BaseURL:             c.BaseURL,
		DataDir:             c.DataDir,
		SQLDir:              c.SQLDir,
		DatasetDate:         c.DatasetDate,
		SchemaVersion:       c.SchemaVersion,
		EnablePGSearch:      c.EnablePGSearch,
		ForceDownload:       c.ForceDownload,
		KeepStaging:         c.KeepStaging,
		LoadBatchSize:       c.LoadBatchSize,
		BatchSize:           c.BatchSize,
		MaxActors:           c.MaxActors,
		MaxProducers:        c.MaxProducers,
		MaxWriters:          c.MaxWriters,
		MaxDirectors:        c.MaxDirectors,
		MaxParallelWorkers:  c.MaxParallelWorkers,
		WorkMem:             c.WorkMem,
		MaintenanceWorkMem:  c.MaintenanceWorkMem,
		ReaderBufferSize:    c.ReaderBufferSize,
		DownloadConcurrency: c.DownloadConcurrency,
		MinNumVotes:         c.MinNumVotes,
		PollInterval:        c.PollInterval,
		ForceRebuild:        c.ForceRebuild,
		SwapLockTimeout:     c.SwapLockTimeout,
	}
}
