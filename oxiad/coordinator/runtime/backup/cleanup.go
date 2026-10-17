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
	"context"
	"log/slog"
	"slices"
	"time"

	"github.com/oxia-db/oxia/common/proto"
)

// This file owns the cleanups of the namespaces: once per cleanup interval,
// a data server deletes the expired backups of each namespace.

// cleanupState is what the scheduler knows of the cleanup of a namespace.
type cleanupState struct {
	nextCleanup time.Time
	// server is the data server running the cleanup, while one does
	server   *proto.DataServerIdentity
	failures int
}

// cleanupRequest is a cleanup request a round sends.
type cleanupRequest struct {
	namespace string
	state     *cleanupState
	server    *proto.DataServerIdentity
}

// cleanups sends the cleanup requests that are due. The first cleanup of a
// namespace runs at the start of the scheduler: a cleanup is cheap next to
// a missed one. The lock is not held across the requests: the states of
// the namespaces are only changed by the round loop.
func (s *Scheduler) cleanups(configs map[string]*proto.Namespace, policy *proto.BackupConfig, now time.Time) {
	for _, req := range s.dueCleanups(configs, now) {
		s.cleanup(req, policy, now)
	}
}

// dueCleanups returns the cleanups to request in this round, with the
// server each one goes to.
func (s *Scheduler) dueCleanups(configs map[string]*proto.Namespace, now time.Time) []cleanupRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	for name := range s.namespaces {
		if _, ok := configs[name]; !ok {
			delete(s.namespaces, name)
		}
	}
	servers := s.availableServers()
	names := make([]string, 0, len(configs))
	for name := range configs {
		names = append(names, name)
	}
	slices.Sort(names)
	var due []cleanupRequest
	for _, name := range names {
		state, ok := s.namespaces[name]
		if !ok {
			state = &cleanupState{nextCleanup: now}
			s.namespaces[name] = state
		}
		if state.nextCleanup.After(now) {
			continue
		}
		server := state.server
		if server == nil {
			if len(servers) == 0 {
				continue
			}
			server = servers[s.cleanupTurn%len(servers)]
			s.cleanupTurn++
		}
		due = append(due, cleanupRequest{namespace: name, state: state, server: server})
	}
	return due
}

// availableServers returns the running data servers, by name. The caller
// holds mu.
func (s *Scheduler) availableServers() []*proto.DataServerIdentity {
	all, err := s.metadata.ListDataServer()
	if err != nil {
		s.log.Warn("Failed to list the data servers", slog.Any("error", err))
		return nil
	}
	servers := make([]*proto.DataServerIdentity, 0, len(all))
	for name, borrowed := range all {
		if s.available(name) {
			servers = append(servers, borrowed.UnsafeBorrow().GetIdentity())
		}
	}
	slices.SortFunc(servers, func(a, b *proto.DataServerIdentity) int {
		return cmpStrings(a.GetNameOrDefault(), b.GetNameOrDefault())
	})
	return servers
}

func cmpStrings(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// cleanup asks a data server for the cleanup of a namespace, and schedules
// the next request from its answer.
func (s *Scheduler) cleanup(req cleanupRequest, policy *proto.BackupConfig, now time.Time) {
	log := s.log.With(slog.String("namespace", req.namespace), slog.String("server", req.server.GetNameOrDefault()))
	ctx, cancel := context.WithTimeout(s.ctx, rpcTimeout)
	defer cancel()
	res, err := s.rpc.ExpireBackups(ctx, req.server, &proto.ExpireBackupsRequest{
		Namespace: req.namespace,
		Policy:    policyProto(policy),
	})
	cleanupInterval := policy.GetCleanupIntervalDurationOrDefault()
	s.mu.Lock()
	defer s.mu.Unlock()
	state := req.state
	if err != nil {
		state.failures++
		state.server = nil
		state.nextCleanup = now.Add(backoff(state.failures, cleanupInterval))
		log.Warn("Backup cleanup request failed", slog.Any("error", err))
		return
	}
	switch res.GetState() {
	case proto.BackupJobState_BACKUP_JOB_STATE_STARTED, proto.BackupJobState_BACKUP_JOB_STATE_RUNNING:
		state.server = req.server
		state.nextCleanup = now
	case proto.BackupJobState_BACKUP_JOB_STATE_DONE:
		state.server = nil
		state.failures = 0
		state.nextCleanup = now.Add(cleanupInterval)
		log.Info("Backup cleanup completed", slog.Uint64("backups-expired", uint64(res.GetBackupsExpired())),
			slog.Uint64("files-deleted", uint64(res.GetFilesDeleted())), slog.Uint64("bytes-deleted", res.GetBytesDeleted()))
	case proto.BackupJobState_BACKUP_JOB_STATE_BUSY:
		// Another server gets the next request
		state.server = nil
		state.nextCleanup = now.Add(policy.GetScheduleIntervalDurationOrDefault())
	default:
		state.failures++
		state.server = nil
		state.nextCleanup = now.Add(backoff(state.failures, cleanupInterval))
		log.Warn("Backup cleanup failed", slog.String("error", res.GetError()), slog.Any("state", res.GetState()))
	}
}
