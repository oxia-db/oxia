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
)

func TestControllerOptionsStepDownTimeoutIsDisabledByDefault(t *testing.T) {
	opts := readOptions(t, `
metadata:
  providerName: memory
`)

	require.NoError(t, opts.Validate())
	require.Zero(t, opts.Controller.StepDownTimeout)
}

func TestControllerOptionsStepDownTimeoutReadsTheFile(t *testing.T) {
	opts := readOptions(t, `
controller:
  stepDownTimeout: 15s
metadata:
  providerName: memory
`)

	require.NoError(t, opts.Validate())
	require.Equal(t, 15*time.Second, opts.Controller.StepDownTimeout.ToDuration())
}

func TestControllerOptionsValidateRejectsNegativeStepDownTimeout(t *testing.T) {
	opts := readOptions(t, `
controller:
  stepDownTimeout: -1s
metadata:
  providerName: memory
`)

	require.ErrorContains(t, opts.Validate(), "controller.stepDownTimeout must not be negative")
}
