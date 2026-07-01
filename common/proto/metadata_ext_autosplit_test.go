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
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAutoSplitConfig_StabilizationPeriod(t *testing.T) {
	c := &AutoSplitConfig{StabilizationPeriod: "2m"}
	d, err := c.GetStabilizationPeriodDuration()
	assert.NoError(t, err)
	assert.Equal(t, 2*time.Minute, d)
	assert.Equal(t, 2*time.Minute, c.GetStabilizationPeriodDurationOrDefault())
}

func TestAutoSplitConfig_StabilizationPeriod_Default(t *testing.T) {
	assert.Equal(t, 1*time.Minute, (*AutoSplitConfig)(nil).GetStabilizationPeriodDurationOrDefault())
	assert.Equal(t, 1*time.Minute, (&AutoSplitConfig{}).GetStabilizationPeriodDurationOrDefault())
}

func TestAutoSplitConfig_CooldownPeriod(t *testing.T) {
	c := &AutoSplitConfig{CooldownPeriod: "10m"}
	d, err := c.GetCooldownPeriodDuration()
	assert.NoError(t, err)
	assert.Equal(t, 10*time.Minute, d)
	assert.Equal(t, 10*time.Minute, c.GetCooldownPeriodDurationOrDefault())
}

func TestAutoSplitConfig_CooldownPeriod_Default(t *testing.T) {
	assert.Equal(t, 5*time.Minute, (*AutoSplitConfig)(nil).GetCooldownPeriodDurationOrDefault())
	assert.Equal(t, 5*time.Minute, (&AutoSplitConfig{}).GetCooldownPeriodDurationOrDefault())
}

func TestAutoSplitConfig_MaxShardSizeMBOrDefault(t *testing.T) {
	assert.Equal(t, uint32(1024), (*AutoSplitConfig)(nil).GetMaxShardSizeMBOrDefault())
	assert.Equal(t, uint32(1024), (&AutoSplitConfig{}).GetMaxShardSizeMBOrDefault())
	assert.Equal(t, uint32(512), (&AutoSplitConfig{MaxShardSizeMb: 512}).GetMaxShardSizeMBOrDefault())
}

func TestAutoSplitConfig_MaxThroughputOpsOrDefault(t *testing.T) {
	assert.Equal(t, uint32(10000), (*AutoSplitConfig)(nil).GetMaxThroughputOpsOrDefault())
	assert.Equal(t, uint32(10000), (&AutoSplitConfig{}).GetMaxThroughputOpsOrDefault())
	assert.Equal(t, uint32(5000), (&AutoSplitConfig{MaxThroughputOps: 5000}).GetMaxThroughputOpsOrDefault())
}

func TestAutoSplitConfig_MaxShardsPerNamespaceOrDefault(t *testing.T) {
	assert.Equal(t, uint32(64), (*AutoSplitConfig)(nil).GetMaxShardsPerNamespaceOrDefault())
	assert.Equal(t, uint32(64), (&AutoSplitConfig{}).GetMaxShardsPerNamespaceOrDefault())
	assert.Equal(t, uint32(128), (&AutoSplitConfig{MaxShardsPerNamespace: 128}).GetMaxShardsPerNamespaceOrDefault())
}

func TestClusterConfiguration_GetAutoSplitWithDefaults(t *testing.T) {
	t.Run("nil config", func(t *testing.T) {
		as := (*ClusterConfiguration)(nil).GetAutoSplitWithDefaults()
		assert.False(t, as.GetEnabled())
		assert.Equal(t, uint32(1024), as.GetMaxShardSizeMb())
		assert.Equal(t, uint32(10000), as.GetMaxThroughputOps())
		assert.Equal(t, "1m", as.GetStabilizationPeriod())
		assert.Equal(t, "5m", as.GetCooldownPeriod())
		assert.Equal(t, uint32(64), as.GetMaxShardsPerNamespace())
	})

	t.Run("partial override", func(t *testing.T) {
		cc := &ClusterConfiguration{
			AutoSplit: &AutoSplitConfig{
				Enabled:               true,
				MaxShardSizeMb:        2048,
				MaxShardsPerNamespace: 32,
			},
		}
		as := cc.GetAutoSplitWithDefaults()
		assert.True(t, as.GetEnabled())
		assert.Equal(t, uint32(2048), as.GetMaxShardSizeMb())
		assert.Equal(t, uint32(10000), as.GetMaxThroughputOps())
		assert.Equal(t, "1m", as.GetStabilizationPeriod())
		assert.Equal(t, "5m", as.GetCooldownPeriod())
		assert.Equal(t, uint32(32), as.GetMaxShardsPerNamespace())
	})
}

func TestAutoSplitConfig_CollectionInterval(t *testing.T) {
	c := &AutoSplitConfig{CollectionInterval: "500ms"}
	d, err := c.GetCollectionIntervalDuration()
	assert.NoError(t, err)
	assert.Equal(t, 500*time.Millisecond, d)
	assert.Equal(t, 500*time.Millisecond, c.GetCollectionIntervalDurationOrDefault())
}

func TestAutoSplitConfig_CollectionInterval_Default(t *testing.T) {
	assert.Equal(t, 30*time.Second, (*AutoSplitConfig)(nil).GetCollectionIntervalDurationOrDefault())
	assert.Equal(t, 30*time.Second, (&AutoSplitConfig{}).GetCollectionIntervalDurationOrDefault())
	assert.Equal(t, 30*time.Second, (&AutoSplitConfig{CollectionInterval: "bad"}).GetCollectionIntervalDurationOrDefault())
	// A non-positive interval would panic time.NewTicker; it must default.
	assert.Equal(t, 30*time.Second, (&AutoSplitConfig{CollectionInterval: "-1s"}).GetCollectionIntervalDurationOrDefault())
	assert.Equal(t, 30*time.Second, (&AutoSplitConfig{CollectionInterval: "0s"}).GetCollectionIntervalDurationOrDefault())
}

func TestClusterConfiguration_Validate_AutoSplitDurations(t *testing.T) {
	base := func() *ClusterConfiguration {
		return &ClusterConfiguration{
			Namespaces: []*Namespace{{Name: "default", InitialShardCount: 1, ReplicationFactor: 1}},
			Servers:    []*DataServerIdentity{{Public: "s1:6648", Internal: "s1:6649"}},
		}
	}

	t.Run("valid durations pass", func(t *testing.T) {
		cc := base()
		cc.AutoSplit = &AutoSplitConfig{Enabled: true, StabilizationPeriod: "1m", CooldownPeriod: "5m"}
		assert.NoError(t, cc.Validate())
	})

	t.Run("empty durations pass", func(t *testing.T) {
		cc := base()
		cc.AutoSplit = &AutoSplitConfig{Enabled: true}
		assert.NoError(t, cc.Validate())
	})

	t.Run("malformed stabilizationPeriod is rejected", func(t *testing.T) {
		cc := base()
		cc.AutoSplit = &AutoSplitConfig{Enabled: true, StabilizationPeriod: "1minute"}
		assert.ErrorContains(t, cc.Validate(), "stabilizationPeriod")
	})

	t.Run("malformed cooldownPeriod is rejected", func(t *testing.T) {
		cc := base()
		cc.AutoSplit = &AutoSplitConfig{Enabled: true, CooldownPeriod: "5min"}
		assert.ErrorContains(t, cc.Validate(), "cooldownPeriod")
	})

	t.Run("malformed collectionInterval is rejected", func(t *testing.T) {
		cc := base()
		cc.AutoSplit = &AutoSplitConfig{Enabled: true, CollectionInterval: "200"}
		assert.ErrorContains(t, cc.Validate(), "collectionInterval")
	})

	t.Run("negative collectionInterval is rejected", func(t *testing.T) {
		cc := base()
		cc.AutoSplit = &AutoSplitConfig{Enabled: true, CollectionInterval: "-1s"}
		assert.ErrorContains(t, cc.Validate(), "collectionInterval")
	})
}
