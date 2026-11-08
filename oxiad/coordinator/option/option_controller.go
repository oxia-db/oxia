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
	"errors"
	"fmt"

	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/common/security"
	"github.com/oxia-db/oxia/oxiad/common/health"
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
	// HealthCheck sets how the coordinator decides that a data server failed.
	HealthCheck HealthCheckOptions `yaml:"healthCheck,omitempty" json:"healthCheck,omitempty" jsonschema:"description=How the coordinator decides that a data server failed"`
}

func (co *ControllerOptions) WithDefault() {
	co.TLS.WithDefault()
	co.HealthCheck.WithDefault()
}

func (co *ControllerOptions) Validate() error {
	return multierr.Combine(co.TLS.Validate(), co.HealthCheck.Validate())
}

// HealthCheckOptions sets how the coordinator probes the data servers. A data
// server that fails failureThreshold consecutive probes is declared failed, and
// the coordinator elects new leaders for its shards: the time to detect a data
// server that stops answering is up to failureThreshold * probeTimeout. A low
// threshold or timeout fails over faster, but declares a data server that is
// only slow, e.g. one under heavy load, failed, which moves its load to the
// others. The defaults favour stability.
type HealthCheckOptions struct {
	// ProbeInterval is the time between two probes of a data server.
	ProbeInterval commonoption.Duration `yaml:"probeInterval,omitempty" json:"probeInterval,omitempty" jsonschema:"description=Time between two probes of a data server. Defaults to 2s"`
	// ProbeTimeout is the time a probe waits for the answer before it fails.
	ProbeTimeout commonoption.Duration `yaml:"probeTimeout,omitempty" json:"probeTimeout,omitempty" jsonschema:"description=Time a probe waits for the answer before it fails. Defaults to 5s"`
	// FailureThreshold is the number of consecutive failed probes after which
	// the data server is declared failed.
	FailureThreshold int `yaml:"failureThreshold,omitempty" json:"failureThreshold,omitempty" jsonschema:"description=Consecutive failed probes after which a data server is declared failed. Defaults to 3"`
}

func (ho *HealthCheckOptions) WithDefault() {
	if ho.ProbeInterval == 0 {
		ho.ProbeInterval = commonoption.Duration(health.DefaultCheckPolicy.ProbeInterval)
	}
	if ho.ProbeTimeout == 0 {
		ho.ProbeTimeout = commonoption.Duration(health.DefaultCheckPolicy.ProbeTimeout)
	}
	if ho.FailureThreshold == 0 {
		ho.FailureThreshold = health.DefaultCheckPolicy.FailureThreshold
	}
}

func (ho *HealthCheckOptions) Validate() error {
	var err error
	if ho.ProbeInterval < 0 {
		err = multierr.Append(err, fmt.Errorf("controller.healthCheck.probeInterval must be positive, got %s", ho.ProbeInterval.ToDuration()))
	}
	if ho.ProbeTimeout < 0 {
		err = multierr.Append(err, fmt.Errorf("controller.healthCheck.probeTimeout must be positive, got %s", ho.ProbeTimeout.ToDuration()))
	}
	if ho.FailureThreshold < 0 {
		err = multierr.Append(err, errors.New("controller.healthCheck.failureThreshold must be positive"))
	}
	return err
}

// Policy returns the health check policy of the options, which must have their
// defaults applied.
func (ho *HealthCheckOptions) Policy() health.CheckPolicy {
	return health.CheckPolicy{
		ProbeInterval:    ho.ProbeInterval.ToDuration(),
		ProbeTimeout:     ho.ProbeTimeout.ToDuration(),
		FailureThreshold: ho.FailureThreshold,
	}
}
