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

package metadata

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"

	commonobject "github.com/oxia-db/oxia/common/object"
	commonproto "github.com/oxia-db/oxia/common/proto"
	oxiatime "github.com/oxia-db/oxia/common/time"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
)

type Metadata interface {
	io.Closer

	GetStatus() commonobject.Borrowed[*commonproto.ClusterStatus]
	UpdateStatus(newStatus *commonproto.ClusterStatus)
	ReserveShardIDs(count uint32) int64

	CreateNamespaceStatus(name string, status *commonproto.NamespaceStatus) bool
	ListNamespaceStatus() map[string]commonobject.Borrowed[*commonproto.NamespaceStatus]
	GetNamespaceStatus(namespace string) (commonobject.Borrowed[*commonproto.NamespaceStatus], bool)
	DeleteNamespaceStatus(name string) commonobject.Borrowed[*commonproto.NamespaceStatus]
	GetNamespace(namespace string) (commonobject.Borrowed[*commonproto.Namespace], bool)

	UpdateShardStatus(namespace string, shard int64, shardMetadata *commonproto.ShardMetadata)
	DeleteShardStatus(namespace string, shard int64)

	GetConfig() commonobject.Borrowed[*commonproto.ClusterConfiguration]
	SubscribeConfig() *commonwatch.Receiver[provider.Versioned[*commonproto.ClusterConfiguration]]
	GetLoadBalancer() commonobject.Borrowed[*commonproto.LoadBalancer]

	CreateDataServer(dataServer *commonproto.DataServer) error
	ListDataServer() map[string]commonobject.Borrowed[*commonproto.DataServer]
	GetDataServer(name string) (commonobject.Borrowed[*commonproto.DataServer], bool)
}

type EnsembleSupplier func(
	namespaceConfig *commonproto.Namespace,
	status *commonproto.ClusterStatus,
) ([]*commonproto.DataServerIdentity, error)

type coordinatorMetadata struct {
	logger *slog.Logger
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	statusProvider provider.Provider[*commonproto.ClusterStatus]
	statusLock     sync.Mutex
	changeChLock   sync.RWMutex
	changeCh       chan struct{}

	configProvider provider.Provider[*commonproto.ClusterConfiguration]
	configLock     sync.Mutex
}

func newMetadata(ctx context.Context, statusProvider provider.Provider[*commonproto.ClusterStatus], configProvider provider.Provider[*commonproto.ClusterConfiguration]) Metadata {
	metadataCtx, cancel := context.WithCancel(ctx)
	m := &coordinatorMetadata{
		logger:         slog.With(slog.String("component", "coordinator-metadata")),
		ctx:            metadataCtx,
		cancel:         cancel,
		statusProvider: statusProvider,
		changeCh:       make(chan struct{}),
		configProvider: configProvider,
	}

	m.doStatusRecovery()
	return m
}

func (m *coordinatorMetadata) computeStatus(fn func(*commonproto.ClusterStatus, metadatacommon.Version) (*commonproto.ClusterStatus, bool)) error {
	m.statusLock.Lock()
	defer m.statusLock.Unlock()

	current := m.statusProvider.Watch().Load()
	next, changed := fn(metadatacommon.ClusterStatusCodec.Clone(current.Value), current.Version)
	if !changed {
		return nil
	}

	_, err := m.statusProvider.Store(provider.Versioned[*commonproto.ClusterStatus]{
		Value:   next,
		Version: current.Version,
	})
	if errors.Is(err, metadatacommon.ErrBadVersion) {
		panic(err)
	}
	return err
}

func (m *coordinatorMetadata) computeConfig(fn func(*commonproto.ClusterConfiguration, metadatacommon.Version) (*commonproto.ClusterConfiguration, bool)) error {
	m.configLock.Lock()
	defer m.configLock.Unlock()

	current := m.configProvider.Watch().Load()
	next, changed := fn(metadatacommon.ClusterConfigCodec.Clone(current.Value), current.Version)
	if !changed {
		return nil
	}

	if err := next.Validate(); err != nil {
		return err
	}

	_, err := m.configProvider.Store(provider.Versioned[*commonproto.ClusterConfiguration]{
		Value:   next,
		Version: current.Version,
	})
	return err
}

func (m *coordinatorMetadata) Close() error {
	m.cancel()
	m.wg.Wait()
	return nil
}

func (m *coordinatorMetadata) notifyStatusChange() {
	m.changeChLock.Lock()
	defer m.changeChLock.Unlock()
	close(m.changeCh)
	m.changeCh = make(chan struct{})
}

func (m *coordinatorMetadata) doStatusRecovery() {
	status := m.statusProvider.Watch().Load().Value
	if status.GetInstanceId() == "" {
		_ = backoff.RetryNotify(func() error {
			return m.computeStatus(func(status *commonproto.ClusterStatus, _ metadatacommon.Version) (*commonproto.ClusterStatus, bool) {
				if status.GetInstanceId() != "" {
					return status, false
				}
				status.InstanceId = uuid.NewString()
				return status, true
			})
		}, oxiatime.NewBackOff(m.ctx), func(err error, duration time.Duration) {
			m.logger.Warn(
				"failed to initialize instance id",
				slog.Any("error", err),
				slog.Duration("retry-after", duration),
			)
		})
		m.notifyStatusChange()
	}
}

func (m *coordinatorMetadata) GetStatus() commonobject.Borrowed[*commonproto.ClusterStatus] {
	return commonobject.Borrow(m.statusProvider.Watch().Load().Value)
}

func (m *coordinatorMetadata) UpdateStatus(newStatus *commonproto.ClusterStatus) {
	_ = backoff.RetryNotify(func() error {
		return m.computeStatus(func(_ *commonproto.ClusterStatus, _ metadatacommon.Version) (*commonproto.ClusterStatus, bool) {
			return newStatus, true
		})
	}, oxiatime.NewBackOff(m.ctx), func(err error, duration time.Duration) {
		m.logger.Warn(
			"failed to update status",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
	m.notifyStatusChange()
}

func (m *coordinatorMetadata) ReserveShardIDs(count uint32) int64 {
	var base int64
	_ = backoff.RetryNotify(func() error {
		return m.computeStatus(func(status *commonproto.ClusterStatus, _ metadatacommon.Version) (*commonproto.ClusterStatus, bool) {
			base = status.GetShardIdGenerator()
			status.ShardIdGenerator += int64(count)
			return status, true
		})
	}, oxiatime.NewBackOff(m.ctx), func(err error, duration time.Duration) {
		m.logger.Warn(
			"failed to reserve shard ids",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
	m.notifyStatusChange()
	return base
}

func (m *coordinatorMetadata) CreateNamespaceStatus(name string, status *commonproto.NamespaceStatus) bool {
	created := false
	_ = backoff.RetryNotify(func() error {
		return m.computeStatus(func(clusterStatus *commonproto.ClusterStatus, _ metadatacommon.Version) (*commonproto.ClusterStatus, bool) {
			if clusterStatus.Namespaces == nil {
				clusterStatus.Namespaces = map[string]*commonproto.NamespaceStatus{}
			}
			if _, exists := clusterStatus.Namespaces[name]; exists {
				return clusterStatus, false
			}
			clusterStatus.Namespaces[name] = status
			created = true
			return clusterStatus, true
		})
	}, oxiatime.NewBackOff(m.ctx), func(err error, duration time.Duration) {
		m.logger.Warn(
			"failed to create namespace status",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
	if created {
		m.notifyStatusChange()
	}
	return created
}

func (m *coordinatorMetadata) ListNamespaceStatus() map[string]commonobject.Borrowed[*commonproto.NamespaceStatus] {
	status := m.statusProvider.Watch().Load().Value
	namespaces := make(map[string]commonobject.Borrowed[*commonproto.NamespaceStatus], len(status.GetNamespaces()))
	for name, status := range status.GetNamespaces() {
		namespaces[name] = commonobject.Borrow(status)
	}
	return namespaces
}

func (m *coordinatorMetadata) GetNamespaceStatus(namespace string) (commonobject.Borrowed[*commonproto.NamespaceStatus], bool) {
	status := m.statusProvider.Watch().Load().Value
	namespaceStatus, exists := status.GetNamespaces()[namespace]
	if !exists {
		return commonobject.Borrowed[*commonproto.NamespaceStatus]{}, false
	}
	return commonobject.Borrow(namespaceStatus), true
}

func (m *coordinatorMetadata) DeleteNamespaceStatus(name string) commonobject.Borrowed[*commonproto.NamespaceStatus] {
	var namespaceStatus *commonproto.NamespaceStatus
	changed := false
	_ = backoff.RetryNotify(func() error {
		return m.computeStatus(func(clusterStatus *commonproto.ClusterStatus, _ metadatacommon.Version) (*commonproto.ClusterStatus, bool) {
			ns, exists := clusterStatus.Namespaces[name]
			if !exists {
				return clusterStatus, false
			}
			namespaceStatus = ns
			for shardID, shardMetadata := range ns.Shards {
				if shardMetadata.Status != commonproto.ShardStatusDeleting {
					shardMetadata.Status = commonproto.ShardStatusDeleting
					ns.Shards[shardID] = shardMetadata
					changed = true
				}
			}
			return clusterStatus, changed
		})
	}, oxiatime.NewBackOff(m.ctx), func(err error, duration time.Duration) {
		m.logger.Warn(
			"failed to mark namespace deleting",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
	if changed {
		m.notifyStatusChange()
	}
	return commonobject.Borrow(namespaceStatus)
}

func (m *coordinatorMetadata) UpdateShardStatus(namespace string, shard int64, shardMetadata *commonproto.ShardMetadata) {
	changed := false
	_ = backoff.RetryNotify(func() error {
		return m.computeStatus(func(clusterStatus *commonproto.ClusterStatus, _ metadatacommon.Version) (*commonproto.ClusterStatus, bool) {
			ns, exist := clusterStatus.Namespaces[namespace]
			if !exist {
				return clusterStatus, false
			}
			ns.Shards[shard] = shardMetadata
			changed = true
			return clusterStatus, true
		})
	}, oxiatime.NewBackOff(m.ctx), func(err error, duration time.Duration) {
		m.logger.Warn(
			"failed to update shard metadata",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
	if changed {
		m.notifyStatusChange()
	}
}

func (m *coordinatorMetadata) DeleteShardStatus(namespace string, shard int64) {
	changed := false
	_ = backoff.RetryNotify(func() error {
		return m.computeStatus(func(clusterStatus *commonproto.ClusterStatus, _ metadatacommon.Version) (*commonproto.ClusterStatus, bool) {
			ns, exist := clusterStatus.Namespaces[namespace]
			if !exist {
				return clusterStatus, false
			}
			if _, exists := ns.Shards[shard]; !exists {
				return clusterStatus, false
			}
			delete(ns.Shards, shard)
			if len(ns.Shards) == 0 {
				delete(clusterStatus.Namespaces, namespace)
			}
			changed = true
			return clusterStatus, true
		})
	}, oxiatime.NewBackOff(m.ctx), func(err error, duration time.Duration) {
		m.logger.Warn(
			"failed to delete shard metadata",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
	if changed {
		m.notifyStatusChange()
	}
}

func (m *coordinatorMetadata) statusChangeNotify() <-chan struct{} {
	m.changeChLock.RLock()
	defer m.changeChLock.RUnlock()
	return m.changeCh
}

func (m *coordinatorMetadata) GetConfig() commonobject.Borrowed[*commonproto.ClusterConfiguration] {
	return commonobject.Borrow(m.configProvider.Watch().Load().Value)
}

func (m *coordinatorMetadata) SubscribeConfig() *commonwatch.Receiver[provider.Versioned[*commonproto.ClusterConfiguration]] {
	return m.configProvider.Watch().Subscribe()
}

func (m *coordinatorMetadata) GetLoadBalancer() commonobject.Borrowed[*commonproto.LoadBalancer] {
	return commonobject.Borrow(m.GetConfig().UnsafeBorrow().GetLoadBalancerWithDefaults())
}

func (m *coordinatorMetadata) CreateDataServer(dataServer *commonproto.DataServer) error {
	name := dataServer.GetIdentity().GetName()
	var createErr error

	if err := m.computeConfig(func(config *commonproto.ClusterConfiguration, _ metadatacommon.Version) (*commonproto.ClusterConfiguration, bool) {
		if _, exists := config.GetDataServer(name); exists {
			createErr = metadatacommon.ErrAlreadyExists
			return nil, false
		}

		config.Servers = append(config.Servers, dataServer.GetIdentity())
		if metadata := dataServer.GetMetadata(); metadata != nil {
			if config.ServerMetadata == nil {
				config.ServerMetadata = map[string]*commonproto.DataServerMetadata{}
			}
			config.ServerMetadata[name] = metadata
		}

		return config, true
	}); err != nil {
		return err
	}
	return createErr
}

func (m *coordinatorMetadata) ListDataServer() map[string]commonobject.Borrowed[*commonproto.DataServer] {
	config := m.GetConfig().UnsafeBorrow()
	dataServers := make(map[string]commonobject.Borrowed[*commonproto.DataServer], len(config.GetServers()))
	for _, server := range config.GetServers() {
		name := server.GetNameOrDefault()
		identity := server
		if server.GetName() == "" {
			identity = &commonproto.DataServerIdentity{
				Name:     &name,
				Public:   server.GetPublic(),
				Internal: server.GetInternal(),
			}
		}
		dataServer := &commonproto.DataServer{
			Identity: identity,
			Metadata: &commonproto.DataServerMetadata{},
		}
		if value, found := config.GetServerMetadata()[name]; found {
			dataServer.Metadata = value
		}
		dataServers[name] = commonobject.Borrow(dataServer)
	}
	return dataServers
}

func (m *coordinatorMetadata) GetNamespace(namespace string) (commonobject.Borrowed[*commonproto.Namespace], bool) {
	for _, ns := range m.GetConfig().UnsafeBorrow().GetNamespaces() {
		if ns.GetName() == namespace {
			return commonobject.Borrow(ns), true
		}
	}
	return commonobject.Borrowed[*commonproto.Namespace]{}, false
}

func (m *coordinatorMetadata) GetDataServer(name string) (commonobject.Borrowed[*commonproto.DataServer], bool) {
	value, ok := m.GetConfig().UnsafeBorrow().GetDataServer(name)
	if !ok {
		return commonobject.Borrowed[*commonproto.DataServer]{}, false
	}
	return commonobject.Borrow(value), true
}

func WaitForCondition(ctx context.Context, metadata Metadata, triggerFn func(), condition func(*commonproto.ClusterStatus) bool) error {
	notifier, ok := metadata.(interface{ statusChangeNotify() <-chan struct{} })
	if !ok {
		return errors.New("metadata does not support status change notifications")
	}
	for {
		ch := notifier.statusChangeNotify()
		if condition(metadata.GetStatus().UnsafeBorrow()) {
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
