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
	"time"

	"github.com/oxia-db/oxia/common/proto"
)

// SplitPolicy is the auto-split of the cluster configuration that a round
// applies, with its defaults. See proto.AutoSplitConfig for each field.
type SplitPolicy struct {
	Enabled               bool
	MaxShardSizeMB        uint32
	MaxThroughputOps      uint32
	Stabilization         time.Duration
	Cooldown              time.Duration
	MaxShardsPerNamespace int
	CollectionInterval    time.Duration
}

// NewSplitPolicy resolves the auto-split of cluster: a field left unset takes
// its default. An explicit 0s stabilization or cooldown is kept; an invalid
// one takes the default, so that it doesn't turn the safety window off.
func NewSplitPolicy(cluster *proto.ClusterConfiguration) SplitPolicy {
	config := cluster.GetAutoSplitWithDefaults()
	stabilization, err := config.GetStabilizationPeriodDuration()
	if err != nil {
		stabilization = config.GetStabilizationPeriodDurationOrDefault()
	}
	cooldown, err := config.GetCooldownPeriodDuration()
	if err != nil {
		cooldown = config.GetCooldownPeriodDurationOrDefault()
	}
	return SplitPolicy{
		Enabled:               config.GetEnabled(),
		MaxShardSizeMB:        config.GetMaxShardSizeMBOrDefault(),
		MaxThroughputOps:      config.GetMaxThroughputOpsOrDefault(),
		Stabilization:         stabilization,
		Cooldown:              cooldown,
		MaxShardsPerNamespace: int(config.GetMaxShardsPerNamespaceOrDefault()),
		CollectionInterval:    config.GetCollectionIntervalDurationOrDefault(),
	}
}
