// Copyright 2023-2025 The Oxia Authors
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

package model

type Server struct {
	// Name is the unique identification for clusters
	Name *string `json:"name" yaml:"name"`

	// Public is the endpoint that is advertised to clients
	Public string `json:"public" yaml:"public"`

	// Internal is the endpoint for server->server RPCs
	Internal string `json:"internal" yaml:"internal"`
}

type ServerMetadata struct {
	// Labels represents a key-value map to store metadata associated with a server.
	Labels map[string]string `json:"labels" yaml:"labels"`
}

func (sv *Server) GetIdentifier() string {
	if sv.Name == nil {
		return sv.Internal
	}
	return *sv.Name
}
