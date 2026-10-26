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

// RaftMetadata configures the raft provider, which replicates the cluster
// configuration and status in a Raft group of the coordinators. Each
// coordinator is a member of the group, and the leader of the group manages
// the cluster.
type RaftMetadata struct {
	// BootstrapNodes are the Raft addresses of the coordinators that form the
	// group, including this one. They are used only when this coordinator
	// starts without any Raft state: a coordinator that has some keeps the
	// members it recorded.
	BootstrapNodes []string `yaml:"bootstrapNodes" json:"bootstrapNodes" jsonschema:"description=Raft addresses of the coordinators that form the group\\, including this one. Used only when the coordinator starts without any Raft state"`
	// Address is the host:port of this coordinator in the Raft group. It also
	// identifies the coordinator in the group, so it must be stable across
	// restarts.
	Address string `yaml:"address" json:"address" jsonschema:"description=host:port of this coordinator in the Raft group. Must be stable across restarts,example=localhost:8080"`
	// DataDir is the directory of the Raft log and snapshots. The coordinator
	// keeps them in a subdirectory named after its address.
	DataDir string `yaml:"dataDir" json:"dataDir" jsonschema:"description=Directory of the Raft log and snapshots,example=./data/raft"`
}
