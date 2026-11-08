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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/oxiad/common/health"
)

func TestHealthCheckOptionsPolicyDefaultsToTheDefaultCheckPolicy(t *testing.T) {
	opts := readOptions(t, `
metadata:
  providerName: memory
`)

	require.NoError(t, opts.Validate())
	require.Equal(t, health.DefaultCheckPolicy, opts.Controller.HealthCheck.Policy())
}

func TestHealthCheckOptionsPolicyReadsTheFile(t *testing.T) {
	opts := readOptions(t, `
controller:
  healthCheck:
    probeInterval: 1s
    probeTimeout: 2s
    failureThreshold: 3
metadata:
  providerName: memory
`)

	require.NoError(t, opts.Validate())
	require.Equal(t, health.CheckPolicy{ProbeInterval: time.Second, ProbeTimeout: 2 * time.Second, FailureThreshold: 3},
		opts.Controller.HealthCheck.Policy())
}

// A field that the file leaves out keeps its default.
func TestHealthCheckOptionsPolicyKeepsTheDefaultOfAnUnsetField(t *testing.T) {
	opts := readOptions(t, `
controller:
  healthCheck:
    probeTimeout: 2s
metadata:
  providerName: memory
`)

	require.NoError(t, opts.Validate())
	require.Equal(t, health.CheckPolicy{
		ProbeInterval:    health.DefaultCheckPolicy.ProbeInterval,
		ProbeTimeout:     2 * time.Second,
		FailureThreshold: health.DefaultCheckPolicy.FailureThreshold,
	}, opts.Controller.HealthCheck.Policy())
}

func TestHealthCheckOptionsValidateRejectsNegativeValues(t *testing.T) {
	for name, tt := range map[string]struct {
		yaml string
		err  string
	}{
		"probe-interval":    {"probeInterval: -1s", "controller.healthCheck.probeInterval must be positive"},
		"probe-timeout":     {"probeTimeout: -1s", "controller.healthCheck.probeTimeout must be positive"},
		"failure-threshold": {"failureThreshold: -1", "controller.healthCheck.failureThreshold must be positive"},
	} {
		t.Run(name, func(t *testing.T) {
			opts := readOptions(t, "controller:\n  healthCheck:\n    "+tt.yaml+"\nmetadata:\n  providerName: memory\n")

			require.ErrorContains(t, opts.Validate(), tt.err)
		})
	}
}
