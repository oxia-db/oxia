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
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"

	oxiatime "github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
)

type StatusResource interface {
	io.Closer

	Load() *model.ClusterStatus

	LoadWithVersion() (*model.ClusterStatus, metadata.Version)

	Swap(newStatus *model.ClusterStatus, version metadata.Version) bool

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

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	lock             sync.RWMutex
	current          *model.ClusterStatus
	currentVersionID metadata.Version
	changeCh         chan struct{}
	leaseWatch       chan struct{}
	leaseWatchOnce   sync.Once
}

func (s *status) Close() error {
	s.cancel()
	s.wg.Wait()
	return nil
}

// guardedStore wraps metadata.Store and triggers a coordinator restart on
// ErrMetadataBadVersion, which is a permanent error that cannot self-heal.
func (s *status) guardedStore(cs *model.ClusterStatus, expectedVersion metadata.Version) (metadata.Version, error) {
	v, err := s.metadata.Store(cs, expectedVersion)
	if errors.Is(err, metadata.ErrMetadataBadVersion) {
		slog.Error("Metadata version mismatch, triggering coordinator revalidation",
			slog.Any("error", err))
		s.broadcastLeaseMightChanged()
	}
	return v, err
}

// watchProviderLease monitors the provider's leadership-lost signal
// and forwards it to leaseWatch.
func (s *status) watchProviderLease(providerLeaseWatch <-chan struct{}) {
	select {
	case <-providerLeaseWatch:
		slog.Warn("Lease lost from provider")
		s.broadcastLeaseMightChanged()
	case <-s.ctx.Done():
	}
}

// broadcastLeaseMightChanged closes leaseWatch to trigger a coordinator restart.
func (s *status) broadcastLeaseMightChanged() {
	s.leaseWatchOnce.Do(func() {
		close(s.leaseWatch)
	})
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
	}, oxiatime.NewBackOff(s.ctx), func(err error, duration time.Duration) {
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
	current, _ := s.LoadWithVersion()
	return current
}

func (s *status) LoadWithVersion() (*model.ClusterStatus, metadata.Version) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if s.current == nil {
		s.lock.RUnlock()
		s.loadWithInitSlow()
		s.lock.RLock()
	}
	return s.current, s.currentVersionID
}

func (s *status) Swap(newStatus *model.ClusterStatus, version metadata.Version) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.currentVersionID != version {
		return false
	}
	err := backoff.RetryNotify(func() error {
		versionID, err := s.guardedStore(newStatus, s.currentVersionID)
		if errors.Is(err, metadata.ErrMetadataBadVersion) {
			return backoff.Permanent(err)
		}
		if err != nil {
			return err
		}
		s.current = newStatus
		s.currentVersionID = versionID
		return nil
	}, oxiatime.NewBackOff(s.ctx), func(err error, duration time.Duration) {
		s.Warn(
			"failed to swap status, retrying later",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
	if err == nil {
		s.notifyChange()
	}
	return err == nil
}

func (s *status) Update(newStatus *model.ClusterStatus) {
	s.lock.Lock()
	defer s.lock.Unlock()
	_ = backoff.RetryNotify(func() error {
		versionID, err := s.guardedStore(newStatus, s.currentVersionID)
		if err != nil {
			return err
		}
		s.current = newStatus
		s.currentVersionID = versionID
		return nil
	}, oxiatime.NewBackOff(s.ctx), func(err error, duration time.Duration) {
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
		versionID, err := s.guardedStore(clonedStatus, s.currentVersionID)
		if err != nil {
			return err
		}
		s.current = clonedStatus
		s.currentVersionID = versionID
		return nil
	}, oxiatime.NewBackOff(s.ctx), func(err error, duration time.Duration) {
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
		versionID, err := s.guardedStore(clonedStatus, s.currentVersionID)
		if err != nil {
			return err
		}
		s.current = clonedStatus
		s.currentVersionID = versionID
		return nil
	}, oxiatime.NewBackOff(s.ctx), func(err error, duration time.Duration) {
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

func NewStatusResource(ctx context.Context, meta metadata.Provider) (StatusResource, <-chan struct{}, error) {
	ctx, cancel := context.WithCancel(ctx)
	leaseWatch := make(chan struct{})
	s := &status{
		Logger: slog.With(
			slog.String("component", "status-resource"),
		),
		ctx:              ctx,
		cancel:           cancel,
		metadata:         meta,
		currentVersionID: metadata.NotExists,
		changeCh:         make(chan struct{}),
		leaseWatch:       leaseWatch,
	}

	providerLeaseWatch, err := meta.WaitToBecomeLeader()
	if err != nil {
		cancel()
		return nil, nil, fmt.Errorf("failed to become leader: %w", err)
	}

	if providerLeaseWatch != nil {
		s.wg.Go(func() {
			s.watchProviderLease(providerLeaseWatch)
		})
	}

	s.Load()
	return s, leaseWatch, nil
}
