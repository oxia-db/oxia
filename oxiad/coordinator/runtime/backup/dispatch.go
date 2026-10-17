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

package backup

import (
	"cmp"
	"context"
	"log/slog"
	"slices"
	"sync"
	"time"

	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
)

// This file owns the requests of a round to the backup replicas of the due
// shards, and the assignment of the backup replicas.

// roleCounts counts the backup replica roles of each data server, by name,
// to spread the roles evenly. A round shares it between its workers, which
// hold its lock from choosing a replica to storing the assignment: two
// shards assigned at once don't both pick the least loaded server.
type roleCounts struct {
	sync.Mutex
	counts map[string]int
}

func countRoles(namespaces map[string]commonobject.Borrowed[*proto.NamespaceStatus]) *roleCounts {
	roles := &roleCounts{counts: map[string]int{}}
	for _, borrowed := range namespaces {
		for _, meta := range borrowed.UnsafeBorrow().GetShards() {
			if replica := meta.GetBackupReplica(); replica != nil {
				roles.counts[replica.GetNameOrDefault()]++
			}
		}
	}
	return roles
}

// move records a change of replica. The caller holds the lock.
func (r *roleCounts) move(from, to *proto.DataServerIdentity) {
	if from != nil {
		r.counts[from.GetNameOrDefault()]--
	}
	r.counts[to.GetNameOrDefault()]++
}

// count returns the roles of a server. The caller holds the lock.
func (r *roleCounts) count(server *proto.DataServerIdentity) int {
	return r.counts[server.GetNameOrDefault()]
}

// dispatch sends a request to the backup replica of every due shard, with
// roundWorkers shards at a time.
func (s *Scheduler) dispatch(due []*shardState, namespaces map[string]commonobject.Borrowed[*proto.NamespaceStatus],
	configs map[string]*proto.Namespace, policy *proto.BackupConfig, now time.Time) {
	roles := countRoles(namespaces)
	slots := make(chan struct{}, roundWorkers)
	var wg sync.WaitGroup
	for _, state := range due {
		if s.ctx.Err() != nil {
			break
		}
		meta := namespaces[state.key.namespace].UnsafeBorrow().GetShards()[state.key.shard]
		// A slot frees within the timeout of the calls of a worker
		slots <- struct{}{}
		wg.Go(func() {
			defer func() { <-slots }()
			s.dispatchShard(state, meta, configs[state.key.namespace], policy, roles, now)
		})
	}
	wg.Wait()
}

// dispatchShard asks the backup replica of a shard for the backup its
// policy wants, if the shard is eligible: in steady state with a leader, with
// a backup replica that is available and caught up. An ineligible shard
// stays due, and is tried again on the next round.
func (s *Scheduler) dispatchShard(state *shardState, meta *proto.ShardMetadata, config *proto.Namespace,
	policy *proto.BackupConfig, roles *roleCounts, now time.Time) {
	log := s.log.With(slog.String("namespace", state.key.namespace), slog.Int64("shard", state.key.shard))
	if meta.GetStatusOrDefault() != proto.ShardStatusSteadyState || meta.GetLeader() == nil {
		return
	}
	replica := s.ensureReplica(state, meta, roles, log)
	if replica == nil {
		return
	}
	if !s.replicaReady(state.key.shard, meta, replica) {
		s.replicaUnavailable(state, meta, replica, roles, policy, now, log)
		return
	}
	s.setUnavailableSince(state, time.Time{})

	ctx, cancel := context.WithTimeout(s.ctx, rpcTimeout)
	defer cancel()
	s.mu.Lock()
	forced := state.forced
	s.mu.Unlock()
	res, err := s.rpc.BackupShard(ctx, replica, &proto.BackupShardRequest{
		Namespace:       state.key.namespace,
		Shard:           state.key.shard,
		Policy:          policyProto(policy),
		Force:           forced,
		NamespaceConfig: config,
		HashRange:       meta.GetInt32HashRange(),
	})
	s.requests.Inc()
	s.applyResponse(state, res, err, policy, now, log.With(slog.String("replica", replica.GetNameOrDefault())))
}

// ensureReplica returns the backup replica of the shard, assigning one when
// the shard has none, or when its replica left the ensemble. It returns nil
// when no member of the ensemble is available, or when the assignment
// couldn't be stored: the shard is tried again on the next round.
func (s *Scheduler) ensureReplica(state *shardState, meta *proto.ShardMetadata, roles *roleCounts,
	log *slog.Logger) *proto.DataServerIdentity {
	current := meta.GetBackupReplica()
	if current != nil && inEnsemble(current, meta.GetEnsemble()) {
		return current
	}
	roles.Lock()
	defer roles.Unlock()
	replica := s.chooseReplica(meta, roles, nil)
	if replica == nil {
		log.Debug("No data server available to take the backups of the shard")
		return nil
	}
	if !s.assignReplica(state, meta, replica, roles, log) {
		return nil
	}
	s.setUnavailableSince(state, time.Time{})
	return replica
}

// replicaUnavailable records that the backup replica can't take a due
// backup, and moves the role to another member once that lasted half an
// interval: the replica is down, or far behind.
func (s *Scheduler) replicaUnavailable(state *shardState, meta *proto.ShardMetadata, replica *proto.DataServerIdentity,
	roles *roleCounts, policy *proto.BackupConfig, now time.Time, log *slog.Logger) {
	s.mu.Lock()
	since := state.unavailableSince
	if since.IsZero() {
		since = now
		state.unavailableSince = now
	}
	s.mu.Unlock()
	if now.Sub(since) < policy.GetIntervalDurationOrDefault()/2 {
		return
	}
	roles.Lock()
	defer roles.Unlock()
	other := s.chooseReplica(meta, roles, replica)
	if other == nil {
		return
	}
	log.Info("Moving the backup replica of the shard", slog.String("from", replica.GetNameOrDefault()),
		slog.String("to", other.GetNameOrDefault()), slog.Duration("unavailable-for", now.Sub(since)))
	if s.assignReplica(state, meta, other, roles, log) {
		s.moves.Inc()
		s.setUnavailableSince(state, time.Time{})
	}
}

func (s *Scheduler) setUnavailableSince(state *shardState, since time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state.unavailableSince = since
}

// chooseReplica picks the backup replica of a shard among the available
// members of its ensemble but except: a follower rather than the leader,
// and the data server with the fewest backup roles. The caller holds the
// lock of roles.
func (s *Scheduler) chooseReplica(meta *proto.ShardMetadata, roles *roleCounts,
	except *proto.DataServerIdentity) *proto.DataServerIdentity {
	candidates := make([]*proto.DataServerIdentity, 0, len(meta.GetEnsemble()))
	for _, member := range meta.GetEnsemble() {
		if except != nil && member.GetNameOrDefault() == except.GetNameOrDefault() {
			continue
		}
		if s.available(member.GetNameOrDefault()) {
			candidates = append(candidates, member)
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	leader := meta.GetLeader().GetNameOrDefault()
	slices.SortFunc(candidates, func(a, b *proto.DataServerIdentity) int {
		aLeads, bLeads := a.GetNameOrDefault() == leader, b.GetNameOrDefault() == leader
		if aLeads != bLeads {
			if aLeads {
				return 1
			}
			return -1
		}
		return cmp.Or(cmp.Compare(roles.count(a), roles.count(b)),
			cmp.Compare(a.GetNameOrDefault(), b.GetNameOrDefault()))
	})
	return candidates[0]
}

// assignReplica stores the backup replica in the shard's metadata, unless
// the member left the ensemble meanwhile. The caller holds the lock of
// roles.
func (s *Scheduler) assignReplica(state *shardState, meta *proto.ShardMetadata, replica *proto.DataServerIdentity,
	roles *roleCounts, log *slog.Logger) bool {
	previous := meta.GetBackupReplica()
	err := s.metadata.PatchNamespaceStatus(state.key.namespace, func(status *proto.NamespaceStatus) bool {
		current, ok := status.GetShards()[state.key.shard]
		if !ok || !inEnsemble(replica, current.GetEnsemble()) {
			return false
		}
		current.BackupReplica = replica
		return true
	})
	if err != nil {
		log.Warn("Failed to store the backup replica of the shard", slog.Any("error", err))
		return false
	}
	roles.move(previous, replica)
	log.Info("Assigned the backup replica of the shard", slog.String("replica", replica.GetNameOrDefault()))
	return true
}

// replicaReady reports whether the backup replica can take a backup now:
// it is available, and it is the leader, or a member that has applied
// everything the leader had committed when asked. A follower reports FENCED
// until the first entry it receives after the election: its database is as
// complete as a FOLLOWER's.
func (s *Scheduler) replicaReady(shard int64, meta *proto.ShardMetadata, replica *proto.DataServerIdentity) bool {
	if !s.available(replica.GetNameOrDefault()) {
		return false
	}
	if replica.GetNameOrDefault() == meta.GetLeader().GetNameOrDefault() {
		return true
	}
	ctx, cancel := context.WithTimeout(s.ctx, rpcTimeout)
	defer cancel()
	req := &proto.GetStatusRequest{Shard: shard}
	leaderStatus, err := s.rpc.GetStatus(ctx, meta.GetLeader(), req)
	if err != nil {
		return false
	}
	replicaStatus, err := s.rpc.GetStatus(ctx, replica, req)
	if err != nil {
		return false
	}
	switch replicaStatus.GetStatus() {
	case proto.ServingStatus_FOLLOWER, proto.ServingStatus_FENCED:
		return replicaStatus.GetCommitOffset() >= leaderStatus.GetCommitOffset()
	default:
		return false
	}
}

// applyResponse updates the state of a shard from the response of its
// replica.
func (s *Scheduler) applyResponse(state *shardState, res *proto.BackupShardResponse, err error,
	policy *proto.BackupConfig, now time.Time, log *slog.Logger) {
	s.mu.Lock()
	defer s.mu.Unlock()
	interval := policy.GetIntervalDurationOrDefault()
	if err != nil {
		s.failRequest(state, interval, now)
		log.Warn("Backup request failed", slog.Any("error", err))
		return
	}
	if last := res.GetLastBackup(); last != nil {
		if at := time.UnixMilli(last.GetTimestampMillis()).UTC(); at.After(state.lastBackup) {
			state.lastBackup = at
		}
	}
	switch res.GetState() {
	case proto.BackupJobState_BACKUP_JOB_STATE_NOT_NEEDED:
		state.forced = false
		state.failures = 0
		state.nextDue = now.Add(interval)
		if next := state.lastBackup.Add(interval); next.After(now) {
			state.nextDue = next
		}
	case proto.BackupJobState_BACKUP_JOB_STATE_DONE:
		state.forced = false
		state.failures = 0
		state.nextDue = state.lastBackup.Add(interval)
		s.completed.Inc()
		log.Info("Backup completed", slog.String("backup-id", res.GetLastBackup().GetBackupId()),
			slog.Int64("commit-offset", res.GetLastBackup().GetCommitOffset()),
			slog.Uint64("uploaded-bytes", res.GetLastBackup().GetUploadedBytes()))
	case proto.BackupJobState_BACKUP_JOB_STATE_FAILED:
		s.failRequest(state, interval, now)
		log.Warn("Backup failed", slog.String("error", res.GetError()))
	case proto.BackupJobState_BACKUP_JOB_STATE_STARTED, proto.BackupJobState_BACKUP_JOB_STATE_RUNNING,
		proto.BackupJobState_BACKUP_JOB_STATE_BUSY:
		// Asked again on the next round
		state.nextDue = now
	default:
		s.failRequest(state, interval, now)
		log.Warn("Unexpected backup state", slog.Any("state", res.GetState()))
	}
}

// failRequest delays the next request of a shard after a failure, doubling
// the delay on each failure in a row up to the interval. The caller holds
// mu.
func (s *Scheduler) failRequest(state *shardState, interval time.Duration, now time.Time) {
	state.failures++
	state.nextDue = now.Add(backoff(state.failures, interval))
	s.failures.Inc()
}

func backoff(failures int, limit time.Duration) time.Duration {
	delay := failureBackoff
	for i := 1; i < failures && delay < limit; i++ {
		delay *= 2
	}
	return min(delay, limit)
}

func inEnsemble(server *proto.DataServerIdentity, ensemble []*proto.DataServerIdentity) bool {
	for _, member := range ensemble {
		if member.GetNameOrDefault() == server.GetNameOrDefault() {
			return true
		}
	}
	return false
}

func policyProto(policy *proto.BackupConfig) *proto.BackupPolicy {
	return &proto.BackupPolicy{
		Destination:            policy.GetDestination(),
		IntervalMillis:         policy.GetIntervalDurationOrDefault().Milliseconds(),
		RetentionMillis:        policy.GetRetentionDurationOrDefault().Milliseconds(),
		MaxConcurrentPerServer: policy.GetMaxConcurrentPerServerOrDefault(),
		Compression:            policy.GetCompression(),
	}
}
