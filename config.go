package pgexporter

import (
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

const (
	// The value of "type" key in configuration.
	typeStr = "pg"
	// The stability level of the exporter.
	stability = component.StabilityLevelBeta
)

// Config defines configuration for the InfluxDB exporter.
type Config struct {
	exporterhelper.QueueSettings `mapstructure:"sending_queue"`
	exporterhelper.RetrySettings `mapstructure:"retry_on_failure"`
	Dsn                          string `mapstructure:"dsn"`
}

func (cfg *Config) Validate() error {
	return nil
}
