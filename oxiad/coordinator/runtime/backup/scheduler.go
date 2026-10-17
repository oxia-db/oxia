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

// Package backup is the part of the shard backups that runs on the
// coordinator: the scheduler that keeps every shard's backup within the
// policy's interval, through the backup replica it assigns to each shard,
// and that has the expired backups of each namespace deleted. See
// docs/backups.md.
package backup

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
	"github.com/oxia-db/oxia/oxiad/backup"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
)

const (
	// rpcTimeout bounds each call of a round, so that an unresponsive data
	// server doesn't stall the round.
	rpcTimeout = 5 * time.Second
	// roundWorkers is how many shards a round handles at once.
	roundWorkers = 8
	// failureBackoff is the delay before the next request to a shard whose
	// last one failed, doubled on each failure up to the interval.
	failureBackoff = 10 * time.Second
	// goneRetention is how long the scheduler remembers a shard that no
	// longer exists, for the shards that take over its range to inherit its
	// last backup time.
	goneRetention = 2 * time.Hour
)

// RPC is what the scheduler needs of the data servers.
type RPC interface {
	GetStatus(ctx context.Context, node *proto.DataServerIdentity,
		req *proto.GetStatusRequest) (*proto.GetStatusResponse, error)
	BackupShard(ctx context.Context, node *proto.DataServerIdentity,
		req *proto.BackupShardRequest) (*proto.BackupShardResponse, error)
	ExpireBackups(ctx context.Context, node *proto.DataServerIdentity,
		req *proto.ExpireBackupsRequest) (*proto.ExpireBackupsResponse, error)
}

// Options configures a Scheduler. Metadata, RPC and Available are required.
type Options struct {
	Metadata coordmetadata.Metadata
	RPC      RPC
	// Available reports whether the data server of the given name is
	// running.
	Available func(name string) bool
	// Clock is the time source, for the tests.
	Clock  func() time.Time
	Logger *slog.Logger
}

// Scheduler runs the backup policy of the cluster. Its state is owned by
// its round loop; RequestBackup borrows it under mu to mark shards due.
type Scheduler struct {
	metadata  coordmetadata.Metadata
	rpc       RPC
	available func(name string) bool
	clock     func() time.Time
	log       *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	// wake makes the loop run a round now
	wake chan struct{}

	mu         sync.Mutex
	shards     map[shardKey]*shardState
	gone       map[shardKey]goneShard
	namespaces map[string]*cleanupState
	// cleanupTurn rotates the data servers the cleanups go to
	cleanupTurn int

	requests  metric.Counter
	completed metric.Counter
	failures  metric.Counter
	moves     metric.Counter
}

type shardKey struct {
	namespace string
	shard     int64
}

// shardState is what the scheduler knows of a shard. The backup times come
// from the records of the backup replica, through its responses.
type shardState struct {
	key       shardKey
	hashRange backup.Range
	// lastBackup is when the shard's state was last captured: the time of
	// the last backup of its replica, or the one it inherited from the shard
	// that held its range before. Zero until known.
	lastBackup time.Time
	// nextDue is when the shard needs a request next
	nextDue time.Time
	// forced makes the next request take a backup whatever the policy says
	forced bool
	// unavailableSince is since when the backup replica can't take a due
	// backup, zero when it can
	unavailableSince time.Time
	failures         int
	ageGauge         metric.Gauge
}

// goneShard is a shard that no longer exists: a split parent, or a shard
// of a deleted namespace.
type goneShard struct {
	hashRange  backup.Range
	lastBackup time.Time
	at         time.Time
}

// New returns a scheduler, which Start runs and Close stops.
func New(options Options) *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Scheduler{
		metadata:   options.Metadata,
		rpc:        options.RPC,
		available:  options.Available,
		clock:      options.Clock,
		log:        options.Logger,
		ctx:        ctx,
		cancel:     cancel,
		wake:       make(chan struct{}, 1),
		shards:     map[shardKey]*shardState{},
		gone:       map[shardKey]goneShard{},
		namespaces: map[string]*cleanupState{},
	}
	if s.clock == nil {
		s.clock = time.Now
	}
	if s.log == nil {
		s.log = slog.With(slog.String("component", "backup-scheduler"))
	}
	labels := map[string]any{}
	s.requests = metric.NewCounter("oxia_coordinator_backup_requests",
		"The number of backup requests sent to the data servers", "count", labels)
	s.completed = metric.NewCounter("oxia_coordinator_backups_completed",
		"The number of shard backups the data servers reported completed", "count", labels)
	s.failures = metric.NewCounter("oxia_coordinator_backup_failures",
		"The number of backup requests that failed", "count", labels)
	s.moves = metric.NewCounter("oxia_coordinator_backup_replica_moves",
		"The number of times the backup replica of a shard moved", "count", labels)
	return s
}

// Start runs the rounds until Close.
func (s *Scheduler) Start() {
	s.wg.Go(func() {
		process.DoWithLabels(s.ctx, map[string]string{"component": "backup-scheduler"}, s.run)
	})
}

// Close stops the scheduler and waits for its round.
func (s *Scheduler) Close() {
	s.cancel()
	s.wg.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.forget(nil)
}

// RequestBackup makes the backups of the shards of a namespace, or of one of
// its shards, or of every namespace when it is empty, due now, and returns
// how many. The backups run whatever the policy says, as soon as their
// replicas can take them.
func (s *Scheduler) RequestBackup(namespace string, shard *int64) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for key, state := range s.shards {
		if (namespace != "" && key.namespace != namespace) || (shard != nil && key.shard != *shard) {
			continue
		}
		state.forced = true
		state.nextDue = s.clock()
		count++
	}
	select {
	case s.wake <- struct{}{}:
	default:
	}
	return count
}

func (s *Scheduler) run() {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-timer.C:
			timer.Reset(s.round())
		case <-s.wake:
			// The next tick runs a round now
			timer.Reset(0)
		}
	}
}

// round runs one check of the policy, and returns the delay to the next.
func (s *Scheduler) round() time.Duration {
	borrowedConfig, err := s.metadata.GetConfig()
	if err != nil {
		s.log.Warn("Failed to read the configuration", slog.Any("error", err))
		return (*proto.BackupConfig)(nil).GetScheduleIntervalDurationOrDefault()
	}
	config := borrowedConfig.UnsafeBorrow()
	policy := config.GetBackupWithDefaults()
	interval := policy.GetScheduleIntervalDurationOrDefault()
	if !policy.GetEnabled() {
		s.mu.Lock()
		s.forget(nil)
		s.mu.Unlock()
		return interval
	}
	namespaces, err := s.metadata.ListNamespaceStatus()
	if err != nil {
		s.log.Warn("Failed to list the namespaces", slog.Any("error", err))
		return interval
	}
	configs := map[string]*proto.Namespace{}
	for _, ns := range config.GetNamespaces() {
		configs[ns.GetName()] = ns
	}
	covered := coveredNamespaces(namespaces, configs)
	now := s.clock()
	due := s.reconcile(covered, now, policy)
	s.dispatch(due, covered, configs, policy, now)
	s.cleanups(configs, policy, now)
	return interval
}

// coveredNamespaces drops the namespaces that opted out of the policy
// with backup: false in their configuration: their shards take no
// backups, forced or not, and report no age. Their cleanups still run,
// so the backups taken before they opted out expire.
func coveredNamespaces(namespaces map[string]commonobject.Borrowed[*proto.NamespaceStatus],
	configs map[string]*proto.Namespace) map[string]commonobject.Borrowed[*proto.NamespaceStatus] {
	covered := make(map[string]commonobject.Borrowed[*proto.NamespaceStatus], len(namespaces))
	for name, status := range namespaces {
		if config := configs[name]; config != nil && config.Backup != nil && !config.GetBackup() {
			continue
		}
		covered[name] = status
	}
	return covered
}

// reconcile updates the shard states from the metadata, and returns the
// shards that are due, most overdue first.
func (s *Scheduler) reconcile(namespaces map[string]commonobject.Borrowed[*proto.NamespaceStatus], now time.Time,
	policy *proto.BackupConfig) []*shardState {
	s.mu.Lock()
	defer s.mu.Unlock()
	live := map[shardKey]*proto.ShardMetadata{}
	for name, borrowed := range namespaces {
		for shard, meta := range borrowed.UnsafeBorrow().GetShards() {
			if eligibleMetadata(meta) {
				live[shardKey{namespace: name, shard: shard}] = meta
			}
		}
	}
	for key, state := range s.shards {
		if _, ok := live[key]; !ok {
			s.gone[key] = goneShard{hashRange: state.hashRange, lastBackup: state.lastBackup, at: now}
			s.forget(&key)
		}
	}
	for key, at := range s.gone {
		if now.Sub(at.at) > goneRetention {
			delete(s.gone, key)
		}
	}
	interval := policy.GetIntervalDurationOrDefault()
	var due []*shardState
	for key, meta := range live {
		state, ok := s.shards[key]
		if !ok {
			state = s.newShardState(key, meta, now, interval)
			s.shards[key] = state
		}
		if !state.nextDue.After(now) {
			due = append(due, state)
		}
	}
	slices.SortFunc(due, byPriority)
	return due
}

// eligibleMetadata reports whether a shard takes backups at all: a shard
// being deleted doesn't, nor a split child that isn't serving yet.
func eligibleMetadata(meta *proto.ShardMetadata) bool {
	if meta.GetStatusOrDefault() == proto.ShardStatusDeleting {
		return false
	}
	return meta.GetSplit() == nil || len(meta.GetSplit().GetChildShardIds()) > 0
}

// newShardState starts tracking a shard. A shard that takes over the range
// of a gone shard, e.g. a split child, inherits its last backup time: the
// range stays covered by the parent's backup until the child's first one.
func (s *Scheduler) newShardState(key shardKey, meta *proto.ShardMetadata, now time.Time,
	interval time.Duration) *shardState {
	state := &shardState{key: key, hashRange: rangeOf(meta.GetInt32HashRange()), nextDue: now}
	for _, gone := range s.gone {
		if gone.hashRange.Covers(state.hashRange) && gone.lastBackup.After(state.lastBackup) {
			state.lastBackup = gone.lastBackup
		}
	}
	if !state.lastBackup.IsZero() {
		state.nextDue = state.lastBackup.Add(interval)
	}
	labels := metric.LabelsForShard(key.namespace, key.shard)
	state.ageGauge = metric.NewGauge("oxia_coordinator_backup_age",
		"The seconds since the state of the shard was last backed up", "s", labels,
		func() int64 { return s.ageSeconds(state) })
	return state
}

// byPriority orders the due shards: the forced ones first, then the longest
// due, then by key.
func byPriority(a, b *shardState) int {
	if a.forced != b.forced {
		if a.forced {
			return -1
		}
		return 1
	}
	return cmp.Or(a.nextDue.Compare(b.nextDue), cmp.Compare(a.key.namespace, b.key.namespace),
		cmp.Compare(a.key.shard, b.key.shard))
}

// ageSeconds is the age of the last backup of a shard, for its gauge: -1
// while it has none.
func (s *Scheduler) ageSeconds(state *shardState) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if state.lastBackup.IsZero() {
		return -1
	}
	return int64(s.clock().Sub(state.lastBackup).Seconds())
}

// forget drops the state of a shard, or of every shard when key is nil.
// The caller holds mu.
func (s *Scheduler) forget(key *shardKey) {
	for k, state := range s.shards {
		if key == nil || k == *key {
			state.ageGauge.Unregister()
			delete(s.shards, k)
		}
	}
	if key == nil {
		clear(s.gone)
		clear(s.namespaces)
	}
}

func rangeOf(r *proto.HashRange) backup.Range {
	if r == nil {
		return backup.FullRange
	}
	return backup.Range{Min: r.GetMin(), Max: r.GetMax()}
}
