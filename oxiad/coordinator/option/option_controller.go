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

	"github.com/oxia-db/oxia/common/security"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
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
	// StepDownTimeout is how long the coordinator leader tolerates being unable to reach a majority of the data
	// servers before it gives up the leadership, so that a coordinator that can reach them takes over. The leadership
	// only tells that the leader reaches the metadata store, e.g. the Kubernetes API server, not the data servers: a
	// leader cut off from them keeps its lease and nobody replaces the shard leaders it can't reach. It is also the
	// time a new leader is given to reach the data servers before it counts. A short timeout recovers sooner, but
	// hands the leadership over when a majority of the data servers fail for real. It applies to a cluster of 3 data
	// servers or more, and is disabled by default (zero).
	StepDownTimeout commonoption.Duration `yaml:"stepDownTimeout,omitempty" json:"stepDownTimeout,omitempty" jsonschema:"description=How long the coordinator leader tolerates being unable to reach a majority of the data servers before it gives up the leadership. Disabled by default"`
}

func (co *ControllerOptions) WithDefault() {
	co.TLS.WithDefault()
}

func (co *ControllerOptions) Validate() error {
	err := co.TLS.Validate()
	if co.StepDownTimeout < 0 {
		err = multierr.Append(err, fmt.Errorf("controller.stepDownTimeout must not be negative, got %s", co.StepDownTimeout.ToDuration()))
	}
	return err
}
