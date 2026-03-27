package etl

import "time"

type Config struct {
	DatabaseURL                     string
	BaseURL                         string
	DataDir                         string
	SQLDir                          string
	DatasetDate                     string
	SchemaVersion                   int
	EnablePGSearch                  bool
	RunETL                          bool
	ForceDownload                   bool
	KeepStaging                     bool
	LoadBatchSize                   int
	BatchSize                       int
	MaxActors                       int
	MaxProducers                    int
	MaxWriters                      int
	MaxDirectors                    int
	MaxParallelWorkers              string
	WorkMem                         string
	MaintenanceWorkMem              string
	Port                            string
	CORSAllowOrigins                []string
	ReaderBufferSize                int
	DownloadConcurrency             int
	MinNumVotes                     int
	DBMaxWalSize                    string
	DBMinWalSize                    string
	DBCheckpointTimeout             string
	DBCheckpointCompletionTarget    string
	DBWalCompression                string
	DBMaxParallelWorkers            string
	DBMaxParallelMaintenanceWorkers string
	ScheduleEnabled                 bool
	PollInterval                    time.Duration
	BootstrapBlocking               bool
	ForceRebuild                    bool
	SwapLockTimeout                 string
}
