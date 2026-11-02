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

func TestNewPolicyTakesTheDefaults(t *testing.T) {
	assert.Equal(t, Policy{
		Interval: 30 * time.Second,
		Split: SplitPolicy{MaxShardSizeMB: 1024, MaxThroughputOps: 10000, Stabilization: time.Minute,
			Cooldown: 5 * time.Minute, MaxShardsPerNamespace: 64, CollectionInterval: 30 * time.Second},
	}, NewPolicy(nil))
}

func TestNewPolicyTakesTheConfiguration(t *testing.T) {
	assert.Equal(t, Policy{
		Interval: time.Second,
		Split: SplitPolicy{Enabled: true, MaxShardSizeMB: 1, MaxThroughputOps: 2, Cooldown: time.Second,
			MaxShardsPerNamespace: 3, CollectionInterval: 200 * time.Millisecond},
	}, NewPolicy(&proto.ClusterConfiguration{
		LoadBalancer: &proto.LoadBalancer{ScheduleInterval: "1s"},
		AutoSplit: &proto.AutoSplitConfig{Enabled: true, MaxShardSizeMb: 1, MaxThroughputOps: 2,
			StabilizationPeriod: "0s", CooldownPeriod: "1s", MaxShardsPerNamespace: 3, CollectionInterval: "200ms"},
	}))
}

func TestPolicyGetRoundInterval(t *testing.T) {
	assert.Equal(t, 30*time.Second, NewPolicy(nil).GetRoundInterval())

	split := &proto.AutoSplitConfig{Enabled: true, CollectionInterval: "1s"}
	assert.Equal(t, time.Second, NewPolicy(&proto.ClusterConfiguration{AutoSplit: split}).GetRoundInterval())
}
