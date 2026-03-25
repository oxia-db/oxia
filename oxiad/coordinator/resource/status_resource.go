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

	"github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
)

type StatusResource interface {
	Load() *model.ClusterStatus

	LoadWithVersion() (*model.ClusterStatus, metadata.Version)

	Swap(newStatus *model.ClusterStatus, version metadata.Version) (bool, error)

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
	clusterStatus, version, err := s.metadata.Get()
	if err != nil {
		s.Warn(
			"failed to load status",
			slog.Any("error", err),
		)
	}
	if clusterStatus == nil {
		s.current = &model.ClusterStatus{}
	} else {
		s.current = clusterStatus
		s.currentVersionID = version
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

func (s *status) Swap(newStatus *model.ClusterStatus, version metadata.Version) (bool, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.currentVersionID != version {
		return false, nil
	}
	versionID, err := s.metadata.Store(newStatus, s.currentVersionID)
	if err != nil {
		return false, err
	}
	s.current = newStatus
	s.currentVersionID = versionID
	s.notifyChange()
	return true, nil
}

func (s *status) Update(newStatus *model.ClusterStatus) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	versionID, err := s.metadata.Store(newStatus, s.currentVersionID)
	if err != nil {
		return err
	}
	s.current = newStatus
	s.currentVersionID = versionID
	s.notifyChange()
	return nil
}

func (s *status) UpdateShardMetadata(namespace string, shard int64, shardMetadata model.ShardMetadata) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	clonedStatus := s.current.Clone()
	ns, exist := clonedStatus.Namespaces[namespace]
	if !exist {
		return nil
	}
	ns.Shards[shard] = shardMetadata.Clone()
	versionID, err := s.metadata.Store(clonedStatus, s.currentVersionID)
	if err != nil {
		return err
	}
	s.current = clonedStatus
	s.currentVersionID = versionID
	s.notifyChange()
	return nil
}

func (s *status) DeleteShardMetadata(namespace string, shard int64) error {
	s.lock.Lock()
	defer s.lock.Unlock()

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
