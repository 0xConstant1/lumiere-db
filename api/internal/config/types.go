package config

import etlcore "lumiere-api/internal/etl"

type Config struct {
	DatabaseURL      string
	Port             string
	EnablePGSearch   bool
	CORSAllowOrigins []string
	ETL              ETLConfig
}

type ETLConfig struct {
	Runtime           etlcore.Config
	Enabled           bool
	ScheduleEnabled   bool
	BootstrapBlocking bool
}
