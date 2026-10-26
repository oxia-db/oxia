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

// Options is the configuration of a coordinator, as read from the file passed
// with --sconfig, or built from the command line flags. The coordinator applies
// the defaults of the options that are not set (see WithDefault), then
// validates them (see Validate). It reloads the file when it changes.
//
// A minimal configuration that keeps the cluster configuration and status in
// local files:
//
//	server:
//	  public:
//	    bindAddress: 0.0.0.0:6651
//	  internal:
//	    bindAddress: 0.0.0.0:6649
//	metadata:
//	  providerName: file
//	  file:
//	    dir: ./data
type Options struct {
	// Cluster holds the deprecated cluster settings, still accepted for
	// compatibility. Use Metadata instead.
	Cluster ClusterOptions `yaml:"cluster,omitempty" json:"cluster,omitempty" jsonschema:"description=Deprecated: use metadata instead. Cluster settings still accepted for compatibility"`
	// Server configures the public and internal gRPC servers of the
	// coordinator.
	Server ServerOptions `yaml:"server" json:"server" jsonschema:"description=Public and internal gRPC servers of the coordinator"`
	// Controller configures the connections of the coordinator to the data
	// servers.
	Controller ControllerOptions `yaml:"controller,omitempty" json:"controller,omitempty" jsonschema:"description=Connections of the coordinator to the data servers"`
	// Metadata configures where the coordinator keeps the cluster
	// configuration and status, and how the coordinators that share them elect
	// their leader.
	Metadata MetadataOptions `yaml:"metadata" json:"metadata" jsonschema:"description=Where the coordinator keeps the cluster configuration and status"`
	// Observability configures the metrics and the logs of the coordinator.
	Observability commonoption.ObservabilityOptions `yaml:"observability" json:"observability" jsonschema:"description=Metrics and logs of the coordinator"`
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

// NewDefaultOptions returns the options of a coordinator with their defaults.
func NewDefaultOptions() *Options {
	options := Options{}
	options.WithDefault()
	return &options
}
