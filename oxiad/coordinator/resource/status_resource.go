// Copyright 2023-2025 The Oxia Authors
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

package resource

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/util"
)

type EnsembleSupplier func(namespaceConfig *model.NamespaceConfig, status *model.ClusterStatus) ([]model.Server, error)

type StatusResource interface {
	Load() *model.ClusterStatus

	ApplyChanges(config *model.ClusterConfig, ensembleSupplier EnsembleSupplier) (*model.ClusterStatus, map[int64]string, []int64)

	Update(newStatus *model.ClusterStatus)

	UpdateShardMetadata(namespace string, shard int64, shardMetadata model.ShardMetadata)

	DeleteShardMetadata(namespace string, shard int64)

	IsReady(clusterConfig *model.ClusterConfig) bool

	// ChangeNotify returns a channel that is closed when the next status
	// change occurs. Callers should capture this channel BEFORE checking
	// the condition they are waiting for, to avoid missing a change:
	//
	//	for {
	//	    ch := statusResource.ChangeNotify()
	//	    if condition(statusResource.Load()) {
	//	        return
	//	    }
	//	    select {
	//	    case <-ch:
	//	    case <-ctx.Done():
	//	        return ctx.Err()
	//	    }
	//	}
	ChangeNotify() <-chan struct{}
}

var _ StatusResource = &status{}

type status struct {
	*slog.Logger
	metadata metadata.Provider

	lock             sync.RWMutex
	current          *model.ClusterStatus
	currentVersionID metadata.Version
	changeCh         chan struct{}
}

// handleStoreError handles errors from metadata.Store().
// ErrMetadataBadVersion is treated as permanent — retrying with a
// re-read version could overwrite valid data written by a new leader.
// The LeadershipLostCh will trigger a full coordinator restart with
// clean state.
func (*status) handleStoreError(err error) error {
	if errors.Is(err, metadata.ErrMetadataBadVersion) {
		return backoff.Permanent(err)
	}
	return err
}

// notifyChange wakes all goroutines waiting on ChangeNotify.
// Must be called while holding s.lock for writing.
func (s *status) notifyChange() {
	close(s.changeCh)
	s.changeCh = make(chan struct{})
}

func (s *status) ChangeNotify() <-chan struct{} {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.changeCh
}

// WaitForCondition blocks until condition returns true for the current
// status, using event-driven notifications instead of time-based polling.
// It triggers triggerFn (if non-nil) each iteration to drive progress.
func WaitForCondition(ctx context.Context, sr StatusResource, triggerFn func(), condition func(*model.ClusterStatus) bool) error {
	for {
		ch := sr.ChangeNotify()
		if condition(sr.Load()) {
			return nil
		}
		if triggerFn != nil {
			triggerFn()
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *status) loadWithInitSlow() {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.current != nil {
		return
	}
	_ = backoff.RetryNotify(func() error {
		clusterStatus, version, err := s.metadata.Get()
		if err != nil {
			return err
		}
		s.current = clusterStatus
		s.currentVersionID = version
		return nil
	}, backoff.NewExponentialBackOff(), func(err error, duration time.Duration) {
		s.Warn(
			"failed to load status, retrying later",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
	if s.current == nil {
		s.current = &model.ClusterStatus{}
	}
}

func (s *status) Load() *model.ClusterStatus {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if s.current == nil {
		s.lock.RUnlock()
		s.loadWithInitSlow()
		s.lock.RLock()
	}
	return s.current
}

func (s *status) ApplyChanges(config *model.ClusterConfig, ensembleSupplier EnsembleSupplier) (*model.ClusterStatus, map[int64]string, []int64) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.current == nil {
		s.lock.Unlock()
		s.loadWithInitSlow()
		s.lock.Lock()
	}
	newStatus := s.current.Clone()
	shardsToAdd, shardsToDelete := util.ApplyClusterChanges(config, newStatus, ensembleSupplier)
	if len(shardsToAdd) == 0 && len(shardsToDelete) == 0 {
		return newStatus, shardsToAdd, shardsToDelete
	}
	_ = backoff.RetryNotify(func() error {
		versionID, err := s.metadata.Store(newStatus, s.currentVersionID)
		if err != nil {
			return s.handleStoreError(err)
		}
		s.current = newStatus
		s.currentVersionID = versionID
		return nil
	}, backoff.NewExponentialBackOff(), func(err error, duration time.Duration) {
		s.Warn(
			"failed to apply cluster changes, retrying later",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
	s.notifyChange()
	return newStatus, shardsToAdd, shardsToDelete
}

func (s *status) Update(newStatus *model.ClusterStatus) {
	s.lock.Lock()
	defer s.lock.Unlock()
	_ = backoff.RetryNotify(func() error {
		versionID, err := s.metadata.Store(newStatus, s.currentVersionID)
		if err != nil {
			return s.handleStoreError(err)
		}
		s.current = newStatus
		s.currentVersionID = versionID
		return nil
	}, backoff.NewExponentialBackOff(), func(err error, duration time.Duration) {
		s.Warn(
			"failed to update status, retrying later",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
	s.notifyChange()
}

func (s *status) UpdateShardMetadata(namespace string, shard int64, shardMetadata model.ShardMetadata) {
	s.lock.Lock()
	defer s.lock.Unlock()

	clonedStatus := s.current.Clone()
	ns, exist := clonedStatus.Namespaces[namespace]
	if !exist {
		return
	}
	ns.Shards[shard] = shardMetadata.Clone()
	_ = backoff.RetryNotify(func() error {
		versionID, err := s.metadata.Store(clonedStatus, s.currentVersionID)
		if err != nil {
			return s.handleStoreError(err)
		}
		s.current = clonedStatus
		s.currentVersionID = versionID
		return nil
	}, backoff.NewExponentialBackOff(), func(err error, duration time.Duration) {
		s.Warn(
			"failed to update shard metadata, retrying later",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
	s.notifyChange()
}

func (s *status) DeleteShardMetadata(namespace string, shard int64) {
	s.lock.Lock()
	defer s.lock.Unlock()

	clonedStatus := s.current.Clone()
	ns, exist := clonedStatus.Namespaces[namespace]
	if !exist {
		return
	}
	delete(ns.Shards, shard)
	if len(ns.Shards) == 0 {
		delete(clonedStatus.Namespaces, namespace)
	}
	_ = backoff.RetryNotify(func() error {
		versionID, err := s.metadata.Store(clonedStatus, s.currentVersionID)
		if err != nil {
			return s.handleStoreError(err)
		}
		s.current = clonedStatus
		s.currentVersionID = versionID
		return nil
	}, backoff.NewExponentialBackOff(), func(err error, duration time.Duration) {
		s.Warn(
			"failed to delete shard metadata, retrying later",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
	s.notifyChange()
}

func (s *status) IsReady(clusterConfig *model.ClusterConfig) bool {
	currentStatus := s.Load()
	for _, namespace := range clusterConfig.Namespaces {
		count := namespace.InitialShardCount
		name := namespace.Name
		namespaceStatus, ok := currentStatus.Namespaces[name]
		if !ok {
			return false
		}
		if len(namespaceStatus.Shards) != int(count) {
			return false
		}
		for _, shard := range namespaceStatus.Shards {
			if shard.Status != model.ShardStatusSteadyState {
				return false
			}
		}
	}
	return true
}

func NewStatusResource(meta metadata.Provider) StatusResource {
	s := status{
		Logger: slog.With(
			slog.String("component", "status-resource"),
		),
		lock:             sync.RWMutex{},
		metadata:         meta,
		currentVersionID: metadata.NotExists,
		current:          nil,
		changeCh:         make(chan struct{}),
	}
	s.Load()
	return &s
}
