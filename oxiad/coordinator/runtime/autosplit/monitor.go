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

package autosplit

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/oxia-db/oxia/common/metric"
	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/common/proto"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/controller"
)

type StatusGetter interface {
	GetStatus(ctx context.Context, node *proto.DataServerIdentity, req *proto.GetStatusRequest) (*proto.GetStatusResponse, error)
}

const DefaultCollectionInterval = 30 * time.Second

type shardKey struct {
	namespace string
	shardId   int64
}

type shardStatsTracker struct {
	lastStats      *proto.ShardStats
	lastSampleTime time.Time
	throughputOps  float64
	exceedingSince time.Time
}

type Monitor struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	metadata           coordmetadata.Metadata
	rpc                StatusGetter
	splitter           controller.ShardSplitter
	collectionInterval time.Duration

	mu                 sync.Mutex
	trackers           map[shardKey]*shardStatsTracker
	lastSplitCompleted time.Time

	evaluationsCounter metric.Counter
	initiatedCounter   metric.Counter

	logger *slog.Logger
}

func NewMonitor(
	metadata coordmetadata.Metadata,
	rpcProvider StatusGetter,
	splitter controller.ShardSplitter,
	collectionInterval time.Duration,
) *Monitor {
	if collectionInterval == 0 {
		collectionInterval = DefaultCollectionInterval
	}
	labels := map[string]any{}
	ctx, cancel := context.WithCancel(context.Background())
	return &Monitor{
		ctx:                ctx,
		cancel:             cancel,
		metadata:           metadata,
		rpc:                rpcProvider,
		splitter:           splitter,
		collectionInterval: collectionInterval,
		trackers:           make(map[shardKey]*shardStatsTracker),
		evaluationsCounter: metric.NewCounter("oxia_coordinator_autosplit_evaluations_total",
			"Number of threshold evaluation cycles", "count", labels),
		initiatedCounter: metric.NewCounter("oxia_coordinator_autosplit_splits_initiated_total",
			"Auto-splits started", "count", labels),
		logger: slog.With(slog.String("component", "auto-split-monitor")),
	}
}

func (m *Monitor) Start() {
	m.wg.Go(func() {
		process.DoWithLabels(m.ctx, map[string]string{
			"component": "auto-split-monitor",
		}, m.run)
	})
}

func (m *Monitor) Close() {
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()
}

func (m *Monitor) run() {
	ticker := time.NewTicker(m.collectionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.evaluate()
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *Monitor) evaluate() {
	config := m.metadata.GetConfig().UnsafeBorrow()
	autoSplit := config.GetAutoSplitWithDefaults()

	if !autoSplit.GetEnabled() {
		m.mu.Lock()
		clear(m.trackers)
		m.mu.Unlock()
		return
	}

	m.evaluationsCounter.Inc()

	namespaces := m.metadata.ListNamespaceStatus()

	if m.anySplitInProgress(namespaces) {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	cooldown, _ := autoSplit.GetCooldownPeriodDuration()
	if !m.lastSplitCompleted.IsZero() && now.Sub(m.lastSplitCompleted) < cooldown {
		return
	}

	type candidate struct {
		key            shardKey
		overshootRatio float64
	}
	var best *candidate

	activeShards := make(map[shardKey]struct{})

	for ns, borrowedNs := range namespaces {
		nsStatus := borrowedNs.UnsafeBorrow()

		shardCount := uint32(len(nsStatus.GetShards()))
		if shardCount >= autoSplit.GetMaxShardsPerNamespaceOrDefault() {
			m.logger.Warn("Namespace at max shard count, skipping",
				slog.String("namespace", ns),
				slog.Uint64("shards", uint64(shardCount)),
				slog.Uint64("max", uint64(autoSplit.GetMaxShardsPerNamespaceOrDefault())),
			)
			continue
		}

		for shardId, meta := range nsStatus.GetShards() {
			key := shardKey{namespace: ns, shardId: shardId}
			activeShards[key] = struct{}{}

			if meta.GetStatusOrDefault() != proto.ShardStatusSteadyState {
				continue
			}
			if meta.Leader == nil {
				continue
			}
			if meta.Split != nil {
				continue
			}
			if len(meta.PendingDeleteShardNodes) > 0 {
				continue
			}

			stats := m.collectStats(key, meta.Leader)
			if stats == nil {
				continue
			}

			ratio := m.computeOvershoot(key, stats, autoSplit, now)
			if ratio <= 1.0 {
				continue
			}

			if best == nil || ratio > best.overshootRatio {
				best = &candidate{key: key, overshootRatio: ratio}
			}
		}
	}

	m.pruneTrackers(activeShards)

	if best == nil {
		return
	}

	m.logger.Info("Initiating auto-split",
		slog.String("namespace", best.key.namespace),
		slog.Int64("shard", best.key.shardId),
		slog.Float64("overshoot-ratio", best.overshootRatio),
	)

	leftChild, rightChild, err := m.splitter.InitiateSplit(best.key.namespace, best.key.shardId, nil)
	if err != nil {
		m.logger.Warn("Auto-split initiation failed",
			slog.String("namespace", best.key.namespace),
			slog.Int64("shard", best.key.shardId),
			slog.Any("error", err),
		)
		return
	}

	m.initiatedCounter.Inc()
	m.lastSplitCompleted = now

	m.logger.Info("Auto-split initiated",
		slog.String("namespace", best.key.namespace),
		slog.Int64("parent-shard", best.key.shardId),
		slog.Int64("left-child", leftChild),
		slog.Int64("right-child", rightChild),
	)
}

func (m *Monitor) collectStats(key shardKey, leader *proto.DataServerIdentity) *proto.ShardStats {
	ctx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
	defer cancel()

	resp, err := m.rpc.GetStatus(ctx, leader, &proto.GetStatusRequest{Shard: key.shardId})
	if err != nil {
		m.logger.Debug("Failed to collect stats",
			slog.String("namespace", key.namespace),
			slog.Int64("shard", key.shardId),
			slog.Any("error", err),
		)
		return nil
	}
	return resp.GetShardStats()
}

func (m *Monitor) computeOvershoot(key shardKey, stats *proto.ShardStats, config *proto.AutoSplitConfig, now time.Time) float64 {
	tracker, exists := m.trackers[key]
	if !exists {
		tracker = &shardStatsTracker{}
		m.trackers[key] = tracker
	}

	if tracker.lastStats != nil {
		elapsed := now.Sub(tracker.lastSampleTime).Seconds()
		if elapsed > 0 {
			prevTotal := tracker.lastStats.GetReadOpsTotal() + tracker.lastStats.GetWriteOpsTotal()
			currTotal := stats.GetReadOpsTotal() + stats.GetWriteOpsTotal()
			if currTotal >= prevTotal {
				tracker.throughputOps = float64(currTotal-prevTotal) / elapsed
			}
		}
	}

	tracker.lastStats = stats
	tracker.lastSampleTime = now

	maxSizeMB := float64(config.GetMaxShardSizeMBOrDefault())
	maxThroughput := float64(config.GetMaxThroughputOpsOrDefault())
	stabilization, _ := config.GetStabilizationPeriodDuration()

	sizeMB := float64(stats.GetDbSizeBytes()) / (1024 * 1024)
	var sizeRatio, throughputRatio float64

	if maxSizeMB > 0 {
		sizeRatio = sizeMB / maxSizeMB
	}
	if maxThroughput > 0 {
		throughputRatio = tracker.throughputOps / maxThroughput
	}

	overshoot := max(sizeRatio, throughputRatio)
	if overshoot <= 1.0 {
		tracker.exceedingSince = time.Time{}
		return 0
	}

	if tracker.exceedingSince.IsZero() {
		tracker.exceedingSince = now
	}

	if now.Sub(tracker.exceedingSince) < stabilization {
		return 0
	}

	return overshoot
}

func (m *Monitor) anySplitInProgress(namespaces map[string]commonobject.Borrowed[*proto.NamespaceStatus]) bool {
	for _, borrowedNs := range namespaces {
		ns := borrowedNs.UnsafeBorrow()
		for _, meta := range ns.GetShards() {
			if meta.Split != nil && len(meta.Split.ChildShardIds) > 0 {
				return true
			}
		}
	}
	return false
}

func (m *Monitor) pruneTrackers(active map[shardKey]struct{}) {
	for key := range m.trackers {
		if _, ok := active[key]; !ok {
			delete(m.trackers, key)
		}
	}
}
