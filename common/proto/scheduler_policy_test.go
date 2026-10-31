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

package proto

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSchedulerPolicyValidate(t *testing.T) {
	var unset *SchedulerPolicy
	require.NoError(t, unset.Validate())
	require.NoError(t, (&SchedulerPolicy{Interval: "10s", MoveTimeout: "1m", MoveBackoff: "1s", Tolerance: 0.1}).Validate())

	for _, tc := range []struct {
		policy   *SchedulerPolicy
		expected string
	}{
		{&SchedulerPolicy{Interval: "x"}, "interval must be a positive duration"},
		{&SchedulerPolicy{Interval: "-1s"}, "interval must be a positive duration"},
		{&SchedulerPolicy{MoveTimeout: "0s"}, "moveTimeout must be a positive duration"},
		{&SchedulerPolicy{MoveBackoff: "x"}, "moveBackoff must be a positive duration"},
		{&SchedulerPolicy{StableTime: "-1m"}, "stableTime must be a positive duration"},
		{&SchedulerPolicy{Tolerance: -0.1}, "tolerance must be"},
		{&SchedulerPolicy{Tolerance: 1}, "tolerance must be"},
	} {
		require.ErrorContains(t, tc.policy.Validate(), tc.expected)
	}
}

func TestClusterConfigurationValidateSchedulerPolicy(t *testing.T) {
	require.ErrorContains(t, (&ClusterConfiguration{
		SchedulerPolicy: &SchedulerPolicy{Interval: "x"},
	}).Validate(), "invalid schedulerPolicy")
}
