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

// Package collector collects the snapshot of a round: the metadata of the
// cluster, and the stats of the shards from their leaders, for the splits.
package collector

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/proto"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/model/stats"
	"github.com/oxia-db/oxia/oxiad/coordinator/scheduler/policy"
)

const (
	statusTimeout = 5 * time.Second
	// parallelism bounds the status requests in flight.
	parallelism = 16
)

// Collector collects the snapshot of each round, and the stats of its shards
// from their leaders, at most once per collection interval of the auto-split.
// It keeps the samples that a rate and the stabilization period need.
//
// The op counters of a shard are cumulative in the process of its leader: a
// throughput is measured between two samples from the same leader only, and
// is dropped when the leader changes or its counters go backwards, after a
// restart.
//
// A Collector is not safe for concurrent use: the scheduler loop owns it.
type Collector struct {
	ctx      context.Context
	logger   *slog.Logger
	metadata coordmetadata.Metadata
	running  func(name string) bool
	rpc      rpc.ControllerRPCProvider
	// sampled is the time of the last sample, and overshoots its result.
	sampled    time.Time
	overshoots map[int64]float64
	filtering  map[int64]bool
	trackers   map[int64]*tracker
	// lastSplit is the time the last split started: no shard is over its
	// threshold until the cooldown from it ends.
	lastSplit time.Time

	evaluationsCounter metric.Counter
}

// tracker holds the last sample of a shard.
type tracker struct {
	// leader is the data server the sample came from.
	leader         string
	stats          *proto.ShardStats
	sampled        time.Time
	throughputOps  float64
	exceedingSince time.Time
}

// New returns a Collector that reads metadata, whether each data server is
// running with running, and the stats of the shards with provider, until ctx
// is done.
func New(ctx context.Context, metadata coordmetadata.Metadata, running func(name string) bool,
	provider rpc.ControllerRPCProvider) *Collector {
	return &Collector{
		ctx:      ctx,
		logger:   slog.With(slog.String("component", "snapshot-collector")),
		metadata: metadata,
		running:  running,
		rpc:      provider,
		trackers: make(map[int64]*tracker),
		evaluationsCounter: metric.NewCounter("oxia_coordinator_autosplit_evaluations_total",
			"Number of threshold evaluation cycles", "count", map[string]any{}),
	}
}

// Collect returns the snapshot of the cluster from the metadata, with the data
// servers that are down, so that the plan moves nothing that needs them; the
// dispatch checks them again before it sends an action. It doesn't sample the stats of the shards: see Sample. It returns the error of
// a read that failed.
func (c *Collector) Collect() (*stats.ClusterStats, error) {
	dataServers, errDataServers := c.metadata.ListDataServer()
	namespaces, errNamespaces := c.metadata.ListNamespace()
	statuses, errStatuses := c.metadata.ListNamespaceStatus()
	if err := errors.Join(errDataServers, errNamespaces, errStatuses); err != nil {
		return nil, err
	}
	s := stats.New(dataServers, namespaces, statuses)
	for name, dataServer := range s.DataServers {
		dataServer.Down = !c.running(name)
	}
	return s, nil
}

// Sample sets the stats of the shards of s against p at now, sampled again
// if the collection interval passed since the last sample. A shard without a
// leader, or whose leader doesn't answer, has no stats; no shard is over its
// threshold until the cooldown of the last split ends. A shard that still
// filters the records of its parent, or of a namespace at its maximum of
// shards, isn't splittable. It forgets the samples if the auto-split is
// disabled.
//
// The splits come after the other ops, so the owner samples only once
// nothing else moves: a shard sampled again after a gap of more than two
// collection intervals starts its stabilization over.
func (c *Collector) Sample(s *stats.ClusterStats, p policy.Split, now time.Time) {
	c.sample(s, p, now)
	cooling := now.Sub(c.lastSplit) < p.Cooldown
	for _, ns := range s.Namespaces {
		// A shard being deleted still counts, until it is gone
		full := len(ns.Shards)+ns.DeletingShards >= p.MaxShardsPerNamespace
		for id, shard := range ns.Shards {
			shard.Splittable = shard.Splittable && !full && !c.filtering[id]
			if !cooling {
				shard.Overshoot = c.overshoots[id]
			}
		}
	}
}

// SetLastSplit records that a split started at now: the cooldown runs from it.
func (c *Collector) SetLastSplit(now time.Time) {
	c.lastSplit = now
}

// sample samples the stats of the shards of s again, if the collection
// interval of p passed since the last sample. It forgets the samples if the
// auto-split is disabled.
func (c *Collector) sample(s *stats.ClusterStats, p policy.Split, now time.Time) {
	if !p.Enabled {
		c.sampled, c.overshoots, c.filtering = time.Time{}, nil, nil
		clear(c.trackers)
		return
	}
	if !c.sampled.IsZero() && now.Sub(c.sampled) < p.CollectionInterval {
		return
	}
	c.sampled = now
	c.evaluationsCounter.Inc()

	leaders := make(map[int64]string)
	for _, ns := range s.Namespaces {
		for id, shard := range ns.Shards {
			leaders[id] = shard.Leader
		}
	}
	for id := range c.trackers {
		if _, ok := leaders[id]; !ok {
			delete(c.trackers, id)
		}
	}
	c.overshoots, c.filtering = make(map[int64]float64), make(map[int64]bool)
	for id, shardStats := range c.fetch(s, leaders) {
		c.overshoots[id] = c.overshoot(id, leaders[id], shardStats, p, now)
		c.filtering[id] = shardStats.GetSplitFilterPending()
	}
}

// fetch reads the stats of each shard from its leader, in parallel. It leaves
// out the shards without a known leader, and those whose leader doesn't
// answer.
func (c *Collector) fetch(s *stats.ClusterStats, leaders map[int64]string) map[int64]*proto.ShardStats {
	var mu sync.Mutex
	var wg sync.WaitGroup
	results := make(map[int64]*proto.ShardStats, len(leaders))
	limit := make(chan struct{}, parallelism)
	for id, leader := range leaders {
		dataServer := s.DataServers[leader]
		if dataServer == nil {
			continue
		}
		identity := dataServer.GetIdentity()
		limit <- struct{}{}
		wg.Go(func() {
			defer func() { <-limit }()
			ctx, cancel := context.WithTimeout(c.ctx, statusTimeout)
			defer cancel()
			response, err := c.rpc.GetStatus(ctx, identity, &proto.GetStatusRequest{Shard: id})
			if err != nil {
				c.logger.Debug("Failed to read the stats of a shard", slog.Int64("shard", id), slog.Any("error", err))
				return
			}
			mu.Lock()
			defer mu.Unlock()
			results[id] = response.GetShardStats()
		})
	}
	wg.Wait()
	return results
}

// overshoot records the sample of shard, and returns how many times its
// threshold the shard has stayed over for the stabilization period, or 0.
func (c *Collector) overshoot(shard int64, leader string, shardStats *proto.ShardStats, p policy.Split,
	now time.Time) float64 {
	t, ok := c.trackers[shard]
	if !ok || now.Sub(t.sampled) > 2*p.CollectionInterval {
		t = &tracker{}
		c.trackers[shard] = t
	}
	if t.leader != leader {
		t.throughputOps = 0
	} else if t.stats != nil && now.After(t.sampled) {
		previous := t.stats.GetReadOpsTotal() + t.stats.GetWriteOpsTotal()
		current := shardStats.GetReadOpsTotal() + shardStats.GetWriteOpsTotal()
		if current >= previous {
			t.throughputOps = float64(current-previous) / now.Sub(t.sampled).Seconds()
		} else {
			t.throughputOps = 0
		}
	}
	t.leader, t.stats, t.sampled = leader, shardStats, now

	sizeMB := float64(shardStats.GetDbSizeBytes()) / (1024 * 1024)
	overshoot := max(sizeMB/float64(p.MaxShardSizeMB), t.throughputOps/float64(p.MaxThroughputOps))
	if overshoot <= 1 {
		t.exceedingSince = time.Time{}
		return 0
	}
	if t.exceedingSince.IsZero() {
		t.exceedingSince = now
	}
	if now.Sub(t.exceedingSince) < p.Stabilization {
		return 0
	}
	return overshoot
}
