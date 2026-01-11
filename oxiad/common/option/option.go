package option

import (
	"fmt"

	"go.uber.org/multierr"
)

const (
	DefaultMetricsPort = 8080
	DefaultLogLevel    = "info"
)

type ObservabilityOptions struct {
	Metric MetricOptions `yaml:"metric" json:"metric"`
	Log    LogOptions    `yaml:"log" json:"log"`
}

func (ob *ObservabilityOptions) WithDefault() {
	ob.Metric.WithDefault()
	ob.Log.WithDefault()
}

func (ob *ObservabilityOptions) Validate() error {
	return multierr.Combine(
		ob.Metric.Validate(),
		ob.Log.Validate())
}

type MetricOptions struct {
	Enabled     *bool  `yaml:"enabled" json:"enabled"`
	BindAddress string `yaml:"bindAddress" json:"bindAddress"`
}

func (mo *MetricOptions) IsEnabled() bool {
	// the default value is true
	if mo.Enabled == nil {
		return true
	}
	return *mo.Enabled
}

func (mo *MetricOptions) WithDefault() {
	if mo.BindAddress == "" {
		mo.BindAddress = fmt.Sprintf("0.0.0.0:%d", DefaultMetricsPort)
	}
}

func (mo *MetricOptions) Validate() error {
	return nil
}

type LogOptions struct {
	Level string `yaml:"level" json:"level"`
}

func (lo *LogOptions) WithDefault() {
	if lo.Level == "" {
		lo.Level = DefaultLogLevel
	}
}

func (lo *LogOptions) Validate() error {
	return nil
}
