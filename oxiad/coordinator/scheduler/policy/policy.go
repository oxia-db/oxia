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

// Package policy resolves the load balancer and the auto-split of the cluster
// configuration, with their defaults.
package policy

import (
	"time"

	"github.com/oxia-db/oxia/common/proto"
)

// Policy is the scheduler policy that a round applies.
type Policy struct {
	// Interval is the schedule interval of the load balancer configuration.
	Interval time.Duration
	Split    Split
}

// New resolves the policy of the configuration: a field left unset takes its
// default.
func New(cluster *proto.ClusterConfiguration) Policy {
	return Policy{
		Interval: cluster.GetLoadBalancer().GetScheduleIntervalDurationOrDefault(),
		Split:    newSplit(cluster.GetAutoSplitWithDefaults()),
	}
}

// GetRoundInterval returns the time until the next round when nothing triggers
// one: the shard stats are sampled once per collection interval of the
// auto-split, and a round follows each sample.
func (p Policy) GetRoundInterval() time.Duration {
	if p.Split.Enabled {
		return min(p.Interval, p.Split.CollectionInterval)
	}
	return p.Interval
}
