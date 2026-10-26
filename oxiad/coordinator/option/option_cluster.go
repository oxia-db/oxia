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

// ClusterOptions holds the deprecated cluster settings, still accepted for
// compatibility: set the location of the cluster configuration in the
// metadata options instead (see MetadataOptions.ApplyLegacyClusterConfigPath).
type ClusterOptions struct {
	// ConfigPath is the location of the cluster configuration: the path of a
	// file, or configmap:<namespace>/<name> for a Kubernetes ConfigMap. It is
	// moved into the options of the metadata provider, and it must not
	// conflict with them. It is deprecated: set metadata.file.configName, or
	// metadata.kubernetes.namespace and metadata.kubernetes.configName.
	ConfigPath string `yaml:"configPath,omitempty" json:"configPath,omitempty" jsonschema:"description=Deprecated: use metadata.file.configName or metadata.kubernetes.configName. Path of the cluster configuration file\\, or configmap:<namespace>/<name> for a ConfigMap,example=./configs/cluster.yaml"`
}

func (*ClusterOptions) WithDefault() {
}

func (*ClusterOptions) Validate() error {
	return nil
}
