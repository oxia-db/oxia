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

func (*MetricOptions) Validate() error {
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

func (*LogOptions) Validate() error {
	return nil
}
