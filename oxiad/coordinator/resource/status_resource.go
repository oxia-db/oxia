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

	"github.com/oxia-db/oxia/common/option"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
)

// Versioned pairs a value with its metadata version,
// enabling callers to perform CAS writes.
type Versioned[T any] struct {
	Data    T
	Version metadata.Version
}

type StatusResource interface {
	Get() *Versioned[*model.ClusterStatus]

	GetData() *model.ClusterStatus

	GetShard(namespace string, shard int64) option.Option[Versioned[model.ShardMetadata]]

	Swap(state *Versioned[*model.ClusterStatus]) error

	Update(state *Versioned[*model.ClusterStatus]) error

	UpdateShard(namespace string, shard int64, state *Versioned[*model.ShardMetadata]) (metadata.Version, error)

	DeleteShard(namespace string, shard int64, version metadata.Version) error

	IsReady(clusterConfig *model.ClusterConfig) bool

	// ChangeNotify returns a channel that is closed when the next status
	// change occurs. Callers should capture this channel BEFORE checking
	// the condition they are waiting for, to avoid missing a change:
	//
	//	for {
	//	    ch := statusResource.ChangeNotify()
	//	    state := statusResource.Get()
	//	    if condition(state.Data) {
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
		state := sr.Get()
		if condition(state.Data) {
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

func (s *status) loadFromProvider() {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.current != nil {
		return
	}
	clusterStatus, version, err := s.metadata.Get()
	if err != nil {
		s.Warn("failed to load status from metadata provider", slog.Any("error", err))
		s.current = &model.ClusterStatus{}
		return
	}
	s.current = clusterStatus
	s.currentVersionID = version
	if s.current == nil {
		s.current = &model.ClusterStatus{}
	}
}

func (s *status) Get() *Versioned[*model.ClusterStatus] {
	s.lock.RLock()
	if s.current == nil {
		s.lock.RUnlock()
		s.loadFromProvider()
		s.lock.RLock()
	}
	defer s.lock.RUnlock()
	return &Versioned[*model.ClusterStatus]{
		Data:    s.current,
		Version: s.currentVersionID,
	}
}

func (s *status) GetData() *model.ClusterStatus {
	return s.Get().Data
}

func (s *status) GetShard(namespace string, shard int64) option.Option[Versioned[model.ShardMetadata]] {
	s.lock.RLock()
	if s.current == nil {
		s.lock.RUnlock()
		s.loadFromProvider()
		s.lock.RLock()
	}
	defer s.lock.RUnlock()
	if s.current == nil {
		return option.None[Versioned[model.ShardMetadata]]()
	}
	ns, exist := s.current.Namespaces[namespace]
	if !exist {
		return option.None[Versioned[model.ShardMetadata]]()
	}
	meta, exist := ns.Shards[shard]
	if !exist {
		return option.None[Versioned[model.ShardMetadata]]()
	}
	return option.Some(Versioned[model.ShardMetadata]{
		Data:    meta.Clone(),
		Version: s.currentVersionID,
	})
}

func (s *status) Swap(state *Versioned[*model.ClusterStatus]) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.currentVersionID != state.Version {
		return metadata.ErrMetadataBadVersion
	}
	versionID, err := s.metadata.Store(state.Data, s.currentVersionID)
	if err != nil {
		return err
	}
	s.current = state.Data
	s.currentVersionID = versionID
	s.notifyChange()
	return nil
}

func (s *status) Update(state *Versioned[*model.ClusterStatus]) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.currentVersionID != state.Version {
		return metadata.ErrMetadataBadVersion
	}
	versionID, err := s.metadata.Store(state.Data, s.currentVersionID)
	if err != nil {
		return err
	}
	s.current = state.Data
	s.currentVersionID = versionID
	s.notifyChange()
	return nil
}

func (s *status) UpdateShard(namespace string, shard int64, state *Versioned[*model.ShardMetadata]) (metadata.Version, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.currentVersionID != state.Version {
		return "", metadata.ErrMetadataBadVersion
	}
	clonedStatus := s.current.Clone()
	ns, exist := clonedStatus.Namespaces[namespace]
	if !exist {
		return "", nil
	}
	ns.Shards[shard] = state.Data.Clone()
	versionID, err := s.metadata.Store(clonedStatus, s.currentVersionID)
	if err != nil {
		return "", err
	}
	s.current = clonedStatus
	s.currentVersionID = versionID
	s.notifyChange()
	return versionID, nil
}

func (s *status) DeleteShard(namespace string, shard int64,
	version metadata.Version,
) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.currentVersionID != version {
		return metadata.ErrMetadataBadVersion
	}
	clonedStatus := s.current.Clone()
	ns, exist := clonedStatus.Namespaces[namespace]
	if !exist {
		return nil
	}
	delete(ns.Shards, shard)
	if len(ns.Shards) == 0 {
		delete(clonedStatus.Namespaces, namespace)
	}
	versionID, err := s.metadata.Store(clonedStatus, s.currentVersionID)
	if err != nil {
		return err
	}
	s.current = clonedStatus
	s.currentVersionID = versionID
	s.notifyChange()
	return nil
}

func (s *status) IsReady(clusterConfig *model.ClusterConfig) bool {
	state := s.Get()
	for _, namespace := range clusterConfig.Namespaces {
		count := namespace.InitialShardCount
		name := namespace.Name
		namespaceStatus, ok := state.Data.Namespaces[name]
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
	s.Get()
	return &s
}
