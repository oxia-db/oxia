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

type RaftMetadata struct {
	BootstrapNodes []string `yaml:"bootstrapNodes" json:"bootstrapNodes" jsonschema:"description=List of bootstrap nodes for Raft cluster initialization"`
	Address        string   `yaml:"address" json:"address" jsonschema:"description=Address of this node in the Raft cluster,example=localhost:8080"`
	DataDir        string   `yaml:"dataDir" json:"dataDir" jsonschema:"description=Directory for Raft metadata storage,example=./data/raft"`
}
