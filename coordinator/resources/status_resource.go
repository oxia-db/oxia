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

package resources

import (
	"log/slog"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/oxia-db/oxia/coordinator/metadata"
	"github.com/oxia-db/oxia/coordinator/model"
)

type StatusResource interface {
	Load() *model.ClusterStatus

	LoadWithVersion() (*model.ClusterStatus, metadata.Version)

	Swap(newStatus *model.ClusterStatus, version metadata.Version) bool

	Update(newStatus *model.ClusterStatus)

	UpdateShardMetadata(namespace string, shard int64, shardMetadata model.ShardMetadata)

	DeleteShardMetadata(namespace string, shard int64)
}

var _ StatusResource = &status{}

type status struct {
	*slog.Logger
	metadata metadata.Provider

	lock             sync.RWMutex
	current          *model.ClusterStatus
	currentVersionID metadata.Version
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
	_ = backoff.RetryNotify(func() error {
		versionID, err := s.metadata.Store(newStatus, s.currentVersionID)
		if err != nil {
			return err
		}
		s.current = newStatus
		s.currentVersionID = versionID
		return nil
	}, backoff.NewExponentialBackOff(), func(err error, duration time.Duration) {
		s.Warn(
			"failed to swap status, retrying later",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
	return true
}

func (s *status) Update(newStatus *model.ClusterStatus) {
	s.lock.Lock()
	defer s.lock.Unlock()
	_ = backoff.RetryNotify(func() error {
		versionID, err := s.metadata.Store(newStatus, s.currentVersionID)
		if err != nil {
			return err
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
}

func (s *status) UpdateShardMetadata(namespace string, shard int64, shardMetadata model.ShardMetadata) {
	s.lock.Lock()
	defer s.lock.Unlock()

	clonedStatus := s.current.Clone()
	ns, exist := clonedStatus.Namespaces[namespace]
	if !exist {
		return
	}
	ns.Shards[shard] = shardMetadata
	_ = backoff.RetryNotify(func() error {
		versionID, err := s.metadata.Store(clonedStatus, s.currentVersionID)
		if err != nil {
			return err
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
			return err
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
}

func NewStatusResource(meta metadata.Provider) StatusResource {
	s := status{
		lock:             sync.RWMutex{},
		metadata:         meta,
		currentVersionID: metadata.NotExists,
		current:          nil,
	}
	s.Load()
	return &s
}
