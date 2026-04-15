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

	ApplyChanges(config *model.ClusterConfig, ensembleSupplier EnsembleSupplier) (*model.ClusterStatus, map[int64]string, []int64, error)

	Update(newStatus *model.ClusterStatus) error

	UpdateShardMetadata(namespace string, shard int64, shardMetadata model.ShardMetadata) error

	DeleteShardMetadata(namespace string, shard int64) error

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
	ctx      context.Context

	lock             sync.RWMutex
	current          *model.ClusterStatus
	currentVersionID metadata.Version
	loaded           bool
	changeCh         chan struct{}
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

// ensureLoaded loads the status from the metadata store if not already loaded.
// Caller must hold s.lock for writing.
func (s *status) ensureLoaded() error {
	if s.loaded {
		return nil
	}
	err := backoff.RetryNotify(func() error {
		clusterStatus, version, err := s.metadata.Get()
		if err != nil {
			return err
		}
		s.current = clusterStatus
		s.currentVersionID = version
		s.loaded = true
		return nil
	}, backoff.WithContext(backoff.NewExponentialBackOff(), s.ctx), func(err error, duration time.Duration) {
		s.Warn(
			"failed to load status, retrying later",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
	return err
}

func (s *status) Load() *model.ClusterStatus {
	s.lock.RLock()
	if s.loaded {
		defer s.lock.RUnlock()
		if s.current == nil {
			return model.NewClusterStatus()
		}
		return s.current.Clone()
	}
	s.lock.RUnlock()

	s.lock.Lock()
	defer s.lock.Unlock()
	if err := s.ensureLoaded(); err != nil {
		panic(err)
	}
	if s.current == nil {
		return model.NewClusterStatus()
	}
	return s.current.Clone()
}

func (s *status) ApplyChanges(config *model.ClusterConfig, ensembleSupplier EnsembleSupplier) (*model.ClusterStatus, map[int64]string, []int64, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if err := s.ensureLoaded(); err != nil {
		return nil, nil, nil, err
	}
	current := s.current
	if current == nil {
		current = model.NewClusterStatus()
	}
	newStatus := current.Clone()
	shardsToAdd, shardsToDelete := util.ApplyClusterChanges(config, newStatus, ensembleSupplier)
	if len(shardsToAdd) == 0 && len(shardsToDelete) == 0 {
		return newStatus, shardsToAdd, shardsToDelete, nil
	}
	err := backoff.RetryNotify(func() error {
		versionID, err := s.metadata.Store(newStatus, s.currentVersionID)
		if err != nil {
			return err
		}
		s.current = newStatus.Clone()
		s.currentVersionID = versionID
		return nil
	}, backoff.WithContext(backoff.NewExponentialBackOff(), s.ctx), func(err error, duration time.Duration) {
		s.Warn(
			"failed to apply cluster changes, retrying later",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
	if err != nil {
		return newStatus, shardsToAdd, shardsToDelete, err
	}
	s.notifyChange()
	return newStatus, shardsToAdd, shardsToDelete, nil
}

func (s *status) Update(newStatus *model.ClusterStatus) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if err := s.ensureLoaded(); err != nil {
		return err
	}
	err := backoff.RetryNotify(func() error {
		versionID, err := s.metadata.Store(newStatus, s.currentVersionID)
		if err != nil {
			return err
		}
		s.current = newStatus.Clone()
		s.currentVersionID = versionID
		return nil
	}, backoff.WithContext(backoff.NewExponentialBackOff(), s.ctx), func(err error, duration time.Duration) {
		s.Warn(
			"failed to update status, retrying later",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
	if err != nil {
		return err
	}
	s.notifyChange()
	return nil
}

func (s *status) UpdateShardMetadata(namespace string, shard int64, shardMetadata model.ShardMetadata) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if err := s.ensureLoaded(); err != nil {
		return err
	}
	if s.current == nil {
		return nil
	}
	_, exist := s.current.Namespaces[namespace]
	if !exist {
		return nil
	}
	clonedStatus := s.current.Clone()
	namespaceStatus := clonedStatus.Namespaces[namespace]
	namespaceStatus.Shards[shard] = shardMetadata.Clone()
	clonedStatus.Namespaces[namespace] = namespaceStatus
	err := backoff.RetryNotify(func() error {
		versionID, err := s.metadata.Store(clonedStatus, s.currentVersionID)
		if err != nil {
			return err
		}
		s.current = clonedStatus.Clone()
		s.currentVersionID = versionID
		return nil
	}, backoff.WithContext(backoff.NewExponentialBackOff(), s.ctx), func(err error, duration time.Duration) {
		s.Warn(
			"failed to update shard metadata, retrying later",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
	if err != nil {
		return err
	}
	s.notifyChange()
	return nil
}

func (s *status) DeleteShardMetadata(namespace string, shard int64) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if err := s.ensureLoaded(); err != nil {
		return err
	}
	if s.current == nil {
		return nil
	}
	_, exist := s.current.Namespaces[namespace]
	if !exist {
		return nil
	}
	clonedStatus := s.current.Clone()
	namespaceStatus := clonedStatus.Namespaces[namespace]
	delete(namespaceStatus.Shards, shard)
	if len(namespaceStatus.Shards) == 0 {
		delete(clonedStatus.Namespaces, namespace)
	} else {
		clonedStatus.Namespaces[namespace] = namespaceStatus
	}
	err := backoff.RetryNotify(func() error {
		versionID, err := s.metadata.Store(clonedStatus, s.currentVersionID)
		if err != nil {
			return err
		}
		s.current = clonedStatus.Clone()
		s.currentVersionID = versionID
		return nil
	}, backoff.WithContext(backoff.NewExponentialBackOff(), s.ctx), func(err error, duration time.Duration) {
		s.Warn(
			"failed to delete shard metadata, retrying later",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
	if err != nil {
		return err
	}
	s.notifyChange()
	return nil
}

func (s *status) IsReady(clusterConfig *model.ClusterConfig) bool {
	currentStatus := s.Load()
	if currentStatus == nil {
		return false
	}
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
	s, err := NewStatusResourceWithError(meta)
	if err != nil {
		panic(err)
	}
	return s
}

func NewStatusResourceWithError(meta metadata.Provider) (StatusResource, error) {
	return newStatusResource(context.Background(), meta)
}

func NewStatusResourceWithErrorContext(ctx context.Context, meta metadata.Provider) (StatusResource, error) {
	return newStatusResource(ctx, meta)
}

func newStatusResource(ctx context.Context, meta metadata.Provider) (StatusResource, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	s := status{
		Logger: slog.With(
			slog.String("component", "status-resource"),
		),
		lock:             sync.RWMutex{},
		metadata:         meta,
		ctx:              ctx,
		currentVersionID: metadata.NotExists,
		current:          nil,
		changeCh:         make(chan struct{}),
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	if err := s.ensureLoaded(); err != nil {
		return nil, err
	}
	return &s, nil
}
