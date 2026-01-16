// Copyright 2023-2026 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	Metric MetricOptions `yaml:"metric,omitempty" json:"metric,omitempty" jsonschema:"description=Metric configuration for observability"`
	Log    LogOptions    `yaml:"log,omitempty" json:"log,omitempty" jsonschema:"description=Log configuration for observability"`
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
	Enabled     *bool  `yaml:"enabled" json:"enabled" jsonschema:"description=Enable metrics collection,default=true"`
	BindAddress string `yaml:"bindAddress,omitempty" json:"bindAddress,omitempty" jsonschema:"description=Bind address for metrics server,example=0.0.0.0:8080,format=hostname"`
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

func (*MetricOptions) Validate() error {
	return nil
}

type LogOptions struct {
	Level string `yaml:"level,omitempty" json:"level,omitempty" jsonschema:"description=Log level,example=info,default=info"`
}

func (lo *LogOptions) WithDefault() {
	if lo.Level == "" {
		lo.Level = DefaultLogLevel
	}
}

func (*LogOptions) Validate() error {
	return nil
}
