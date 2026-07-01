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
	"cmp"
	"context"
	"log/slog"
	"slices"
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
	GetStatus(ctx context.Context, node *proto.DataServerIdentity,
		req *proto.GetStatusRequest) (*proto.GetStatusResponse, error)
}

const (
	DefaultCollectionInterval = 30 * time.Second
	statusCollectionTimeout   = 5 * time.Second
)

type shardKey struct {
	namespace string
	shardId   int64
}

type shardStatsTracker struct {
	// leader identifies the server the last sample came from. Op counters are
	// per-process cumulative, so a rate is only meaningful between two samples
	// from the same leader.
	leader         string
	lastStats      *proto.ShardStats
	lastSampleTime time.Time
	throughputOps  float64
	exceedingSince time.Time
}

type candidate struct {
	key            shardKey
	overshootRatio float64
}

type Monitor struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	metadata           coordmetadata.Metadata
	rpc                StatusGetter
	splitter           controller.ShardSplitter
	collectionInterval time.Duration

	mu       sync.Mutex
	trackers map[shardKey]*shardStatsTracker
	// lastSplitInitiated is the time the most recent split was started (not
	// finished). The cooldown window is measured from initiation.
	lastSplitInitiated time.Time

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
	if collectionInterval <= 0 {
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
	cooldown := m.cooldownPeriod(autoSplit)
	if !m.lastSplitInitiated.IsZero() && now.Sub(m.lastSplitInitiated) < cooldown {
		return
	}

	candidates := m.collectCandidates(namespaces, autoSplit, now)
	if len(candidates) == 0 {
		return
	}

	// Most overloaded shard first.
	slices.SortFunc(candidates, func(a, b candidate) int {
		return cmp.Compare(b.overshootRatio, a.overshootRatio)
	})

	// Try candidates in order. If InitiateSplit rejects the worst offender
	// (e.g. an ensemble-selection failure), fall through to the next so a
	// single unsplittable shard cannot starve the rest of the namespace.
	for _, c := range candidates {
		m.logger.Info("Initiating auto-split",
			slog.String("namespace", c.key.namespace),
			slog.Int64("shard", c.key.shardId),
			slog.Float64("overshoot-ratio", c.overshootRatio),
		)

		leftChild, rightChild, err := m.splitter.InitiateSplit(c.key.namespace, c.key.shardId, nil)
		if err != nil {
			m.logger.Warn("Auto-split initiation failed, trying next candidate",
				slog.String("namespace", c.key.namespace),
				slog.Int64("shard", c.key.shardId),
				slog.Any("error", err),
			)
			continue
		}

		m.initiatedCounter.Inc()
		m.lastSplitInitiated = now

		m.logger.Info("Auto-split initiated",
			slog.String("namespace", c.key.namespace),
			slog.Int64("parent-shard", c.key.shardId),
			slog.Int64("left-child", leftChild),
			slog.Int64("right-child", rightChild),
		)
		return
	}
}

// collectCandidates samples every steady-state shard, updates the per-shard
// trackers, prunes trackers for shards that no longer exist, and returns the
// shards whose overshoot ratio (after stabilization) exceeds 1.
func (m *Monitor) collectCandidates(
	namespaces map[string]commonobject.Borrowed[*proto.NamespaceStatus],
	autoSplit *proto.AutoSplitConfig,
	now time.Time,
) []candidate {
	var candidates []candidate
	activeShards := make(map[shardKey]struct{})
	maxShards := autoSplit.GetMaxShardsPerNamespaceOrDefault()

	for ns, borrowedNs := range namespaces {
		nsStatus := borrowedNs.UnsafeBorrow()

		// Record every existing shard as active before any skip, so a namespace
		// that is temporarily skipped (e.g. at the shard-count cap) does not
		// lose the stabilization history of its shards to pruneTrackers.
		for shardId := range nsStatus.GetShards() {
			activeShards[shardKey{namespace: ns, shardId: shardId}] = struct{}{}
		}

		if uint32(len(nsStatus.GetShards())) >= maxShards {
			m.logger.Warn("Namespace at max shard count, skipping",
				slog.String("namespace", ns),
				slog.Int("shards", len(nsStatus.GetShards())),
				slog.Uint64("max", uint64(maxShards)),
			)
			continue
		}

		for shardId, meta := range nsStatus.GetShards() {
			if c, ok := m.shardCandidate(shardKey{namespace: ns, shardId: shardId}, meta, autoSplit, now); ok {
				candidates = append(candidates, c)
			}
		}
	}

	m.pruneTrackers(activeShards)
	return candidates
}

// shardCandidate samples one shard and reports whether it is a split candidate
// (steady-state, splittable, and over the threshold past its stabilization
// window). It updates the shard's tracker as a side effect.
func (m *Monitor) shardCandidate(key shardKey, meta *proto.ShardMetadata,
	autoSplit *proto.AutoSplitConfig, now time.Time) (candidate, bool) {
	if meta.GetStatusOrDefault() != proto.ShardStatusSteadyState {
		return candidate{}, false
	}
	if meta.Leader == nil {
		return candidate{}, false
	}
	if meta.Split != nil {
		return candidate{}, false
	}
	if len(meta.PendingDeleteShardNodes) > 0 {
		return candidate{}, false
	}
	// Skip shards whose hash range is too small to split; InitiateSplit would
	// reject them and they must not block other candidates.
	if !splittable(meta) {
		return candidate{}, false
	}

	stats := m.collectStats(key, meta.Leader)
	if stats == nil {
		return candidate{}, false
	}

	ratio := m.computeOvershoot(key, meta.Leader, stats, autoSplit, now)
	if ratio <= 1.0 {
		return candidate{}, false
	}
	return candidate{key: key, overshootRatio: ratio}, true
}

// splittable reports whether the shard's hash range is wide enough to split,
// mirroring the guard in runtime.InitiateSplit.
func splittable(meta *proto.ShardMetadata) bool {
	r := meta.GetInt32HashRange()
	return r.GetMax()-r.GetMin() >= 1
}

func (m *Monitor) collectStats(key shardKey, leader *proto.DataServerIdentity) *proto.ShardStats {
	ctx, cancel := context.WithTimeout(m.ctx, statusCollectionTimeout)
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

func (m *Monitor) computeOvershoot(key shardKey, leader *proto.DataServerIdentity,
	stats *proto.ShardStats, config *proto.AutoSplitConfig, now time.Time) float64 {
	tracker, exists := m.trackers[key]
	if !exists {
		tracker = &shardStatsTracker{}
		m.trackers[key] = tracker
	}

	leaderID := leader.GetNameOrDefault()

	switch {
	case tracker.leader != leaderID:
		// Leader changed (or first sample): the cumulative counters belong to a
		// different process, so drop the rate until we have two same-leader
		// samples rather than computing a spurious cross-leader delta.
		tracker.throughputOps = 0
	case tracker.lastStats != nil:
		elapsed := now.Sub(tracker.lastSampleTime).Seconds()
		if elapsed > 0 {
			prevTotal := tracker.lastStats.GetReadOpsTotal() + tracker.lastStats.GetWriteOpsTotal()
			currTotal := stats.GetReadOpsTotal() + stats.GetWriteOpsTotal()
			if currTotal >= prevTotal {
				tracker.throughputOps = float64(currTotal-prevTotal) / elapsed
			} else {
				// Counters went backwards: the leader kept its identity but its
				// process restarted, so there is no meaningful rate across the
				// reset. Drop the stale value instead of carrying it forward.
				tracker.throughputOps = 0
			}
		}
	default:
		// Same leader, first sample for this tracker: no rate to compute yet.
	}

	tracker.leader = leaderID
	tracker.lastStats = stats
	tracker.lastSampleTime = now

	maxSizeMB := float64(config.GetMaxShardSizeMBOrDefault())
	maxThroughput := float64(config.GetMaxThroughputOpsOrDefault())
	stabilization := m.stabilizationPeriod(config)

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

// cooldownPeriod returns the configured cooldown. A malformed duration string
// must not silently disable the safety window, so on a parse error it falls
// back to the default instead of zero. An explicit "0s" remains valid.
func (m *Monitor) cooldownPeriod(config *proto.AutoSplitConfig) time.Duration {
	d, err := config.GetCooldownPeriodDuration()
	if err != nil {
		m.logger.Warn("Invalid cooldown_period, using default", slog.Any("error", err))
		return config.GetCooldownPeriodDurationOrDefault()
	}
	return d
}

// stabilizationPeriod mirrors cooldownPeriod: a parse error falls back to the
// default rather than silently disabling the stabilization window.
func (m *Monitor) stabilizationPeriod(config *proto.AutoSplitConfig) time.Duration {
	d, err := config.GetStabilizationPeriodDuration()
	if err != nil {
		m.logger.Warn("Invalid stabilization_period, using default", slog.Any("error", err))
		return config.GetStabilizationPeriodDurationOrDefault()
	}
	return d
}

func (*Monitor) anySplitInProgress(namespaces map[string]commonobject.Borrowed[*proto.NamespaceStatus]) bool {
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
