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
	"go.uber.org/multierr"

	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
)

type Options struct {
	Cluster       ClusterOptions                    `yaml:"cluster,omitempty" json:"cluster,omitempty" jsonschema:"description=Deprecated cluster configuration settings"`
	Server        ServerOptions                     `yaml:"server" json:"server" jsonschema:"description=Server configuration for public and internal endpoints"`
	Controller    ControllerOptions                 `yaml:"controller,omitempty" json:"controller,omitempty" jsonschema:"description=Controller configuration for cluster management"`
	Metadata      MetadataOptions                   `yaml:"metadata" json:"metadata" jsonschema:"description=Metadata provider configuration"`
	Observability commonoption.ObservabilityOptions `yaml:"observability" json:"observability" jsonschema:"description=Observability configuration for metrics and tracing"`
}

func (op *Options) WithDefault() {
	op.Cluster.WithDefault()
	op.Server.WithDefault()
	op.Controller.WithDefault()
	op.Metadata.WithDefault()
	_ = op.Metadata.ApplyLegacyClusterConfigPath(op.Cluster.ConfigPath)
	op.Observability.WithDefault()
}

func (op *Options) Validate() error {
	return multierr.Combine(
		op.Cluster.Validate(),
		op.Server.Validate(),
		op.Controller.Validate(),
		op.Metadata.ApplyLegacyClusterConfigPath(op.Cluster.ConfigPath),
		op.Metadata.Validate(),
		op.Observability.Validate())
}

func NewDefaultOptions() *Options {
	options := Options{}
	options.WithDefault()
	return &options
}
