package etl

import "time"

type Config struct {
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
