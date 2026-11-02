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

package model

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/proto"
)

func TestNewSplitPolicyTakesTheDefaults(t *testing.T) {
	assert.Equal(t, SplitPolicy{MaxShardSizeMB: 1024, MaxThroughputOps: 10000, Stabilization: time.Minute,
		Cooldown: 5 * time.Minute, MaxShardsPerNamespace: 64, CollectionInterval: 30 * time.Second}, NewSplitPolicy(nil))
}

func TestNewSplitPolicyTakesTheConfiguration(t *testing.T) {
	assert.Equal(t, SplitPolicy{Enabled: true, MaxShardSizeMB: 1, MaxThroughputOps: 2, Cooldown: time.Second,
		MaxShardsPerNamespace: 3, CollectionInterval: 200 * time.Millisecond},
		NewSplitPolicy(&proto.ClusterConfiguration{AutoSplit: &proto.AutoSplitConfig{Enabled: true, MaxShardSizeMb: 1,
			MaxThroughputOps: 2, StabilizationPeriod: "0s", CooldownPeriod: "1s", MaxShardsPerNamespace: 3,
			CollectionInterval: "200ms"}}))
}

func TestNewSplitPolicyKeepsAnExplicitZeroWindow(t *testing.T) {
	split := NewSplitPolicy(&proto.ClusterConfiguration{AutoSplit: &proto.AutoSplitConfig{StabilizationPeriod: "0s",
		CooldownPeriod: "0s"}})

	assert.Zero(t, split.Stabilization)
	assert.Zero(t, split.Cooldown)
}

func TestNewSplitPolicyKeepsTheSafetyWindowsOfAnInvalidConfig(t *testing.T) {
	split := NewSplitPolicy(&proto.ClusterConfiguration{AutoSplit: &proto.AutoSplitConfig{StabilizationPeriod: "x",
		CooldownPeriod: "y"}})

	assert.Equal(t, time.Minute, split.Stabilization)
	assert.Equal(t, 5*time.Minute, split.Cooldown)
}
