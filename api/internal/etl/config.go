package etl

import (
	"strconv"
	"time"
)

type MemorySize int64

func (m MemorySize) isZero() bool {
	return m == 0
}

func (m MemorySize) postgresValue() string {
	kib := (int64(m) + 1023) / 1024
	return strconv.FormatInt(kib, 10) + "kB"
}

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
	MaxParallelWorkers  *int
	WorkMem             MemorySize
	MaintenanceWorkMem  MemorySize
	ReaderBufferSize    int
	DownloadConcurrency int
	MinNumVotes         int
	PollInterval        time.Duration
	ForceRebuild        bool
	SwapLockTimeout     time.Duration
}
