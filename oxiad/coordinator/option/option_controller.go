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
	"github.com/oxia-db/oxia/common/security"
)

// ControllerOptions configures the connections of the coordinator to the data
// servers, through which it assigns their shards and elects the leaders.
type ControllerOptions struct {
	// TLS secures the connections of the coordinator to the internal servers
	// of the data servers, as their client: set it when the data servers
	// enable TLS on their internal server. It is disabled by default: it is
	// enabled when enabled is true, or, when enabled is unset, when a
	// certificate file is set.
	TLS security.TLSOptions `yaml:"tls,omitempty" json:"tls,omitempty" jsonschema:"description=Client TLS of the connections to the internal servers of the data servers. Disabled by default"`
}

func (co *ControllerOptions) WithDefault() {
	co.TLS.WithDefault()
}

func (co *ControllerOptions) Validate() error {
	return co.TLS.Validate()
}
