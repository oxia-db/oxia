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

type ClusterOptions struct {
	ConfigPath string `yaml:"configPath,omitempty" json:"configPath,omitempty" jsonschema:"description=Deprecated path to the cluster configuration file,example=./configs/cluster.yaml"`
}

func (*ClusterOptions) WithDefault() {
}

func (*ClusterOptions) Validate() error {
	return nil
}
