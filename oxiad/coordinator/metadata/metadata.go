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

package metadata

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	gproto "google.golang.org/protobuf/proto"

	commonobject "github.com/oxia-db/oxia/common/object"
	commonproto "github.com/oxia-db/oxia/common/proto"
	oxiatime "github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/oxiad/common/cache"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
)

type Metadata interface {
	io.Closer

	WaitToBecomeLeader() (lost <-chan struct{}, err error)
	GetSelf() (*commonproto.Coordinator, error)
	GetLeader() (*commonproto.Coordinator, error)
	GetInstanceID() (string, error)
	AllocateShardIDs(count uint32) (int64, error)

	CreateNamespaceStatus(name string, status *commonproto.NamespaceStatus) error
	ListNamespaceStatus() (map[string]commonobject.Borrowed[*commonproto.NamespaceStatus], error)
	GetNamespaceStatus(namespace string) (commonobject.Borrowed[*commonproto.NamespaceStatus], bool, error)
	DeleteNamespaceStatus(name string) commonobject.Borrowed[*commonproto.NamespaceStatus]

	GetShardStatus(namespace string, shard int64) (commonobject.Borrowed[*commonproto.ShardMetadata], bool)
	UpdateShardStatus(namespace string, shard int64, shardMetadata *commonproto.ShardMetadata) error
	// UpdateShardStatuses applies update to the shards of a namespace, as they
	// are in the current status, and stores the result in a single status
	// write: no reader sees some of the changes without the others, and no
	// concurrent update, e.g. a leader election, is reverted. update may run
	// more than once, each time on a fresh copy, and returns false to leave
	// the status unchanged, in which case UpdateShardStatuses returns nil. It
	// may delete shards: a namespace left without shards is deleted, as by
	// DeleteShardStatus.
	UpdateShardStatuses(namespace string, update func(shards map[int64]*commonproto.ShardMetadata) bool) error
	DeleteShardStatus(namespace string, shard int64) error

	GetConfig() commonobject.Borrowed[*commonproto.ClusterConfiguration]
	SubscribeConfig() *cache.Subscription[provider.Versioned[*commonproto.ClusterConfiguration]]
	GetLoadBalancer() commonobject.Borrowed[*commonproto.LoadBalancer]

	CreateNamespace(namespace *commonproto.Namespace) error
	PatchNamespace(namespace *commonproto.Namespace) (*commonproto.Namespace, error)
	DeleteNamespace(name string) (*commonproto.Namespace, error)
	ListNamespace() map[string]commonobject.Borrowed[*commonproto.Namespace]
	GetNamespace(namespace string) (commonobject.Borrowed[*commonproto.Namespace], bool)

	CreateDataServer(dataServer *commonproto.DataServer) error
	PatchDataServer(dataServer *commonproto.DataServer) (*commonproto.DataServer, error)
	DeleteDataServer(name string) (*commonproto.DataServer, error)
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

	configProvider provider.Provider[*commonproto.ClusterConfiguration]
	configLock     sync.Mutex
	name           string
}

func newMetadata(
	ctx context.Context,
	statusProvider provider.Provider[*commonproto.ClusterStatus],
	configProvider provider.Provider[*commonproto.ClusterConfiguration],
	name string,
) Metadata {
	metadataCtx, cancel := context.WithCancel(ctx)
	m := &coordinatorMetadata{
		logger:         slog.With(slog.String("component", "coordinator-metadata")),
		ctx:            metadataCtx,
		cancel:         cancel,
		statusProvider: statusProvider,
		configProvider: configProvider,
		name:           name,
	}
	return m
}

func (m *coordinatorMetadata) computeStatus(fn func(*commonproto.ClusterStatus, metadatacommon.Version) (*commonproto.ClusterStatus, bool, error)) error {
	m.statusLock.Lock()
	defer m.statusLock.Unlock()

	current, err := m.statusProvider.Load()
	if err != nil {
		return err
	}
	next, changed, err := fn(metadatacodec.ClusterStatusCodec.Clone(current.Value), current.Version)
	if err != nil || !changed {
		return err
	}

	_, err = m.statusProvider.Store(provider.Versioned[*commonproto.ClusterStatus]{
		Value:   next,
		Version: current.Version,
	})
	if errors.Is(err, metadatacommon.ErrBadVersion) {
		panic(err)
	}
	return err
}

func (m *coordinatorMetadata) computeConfig(fn func(*commonproto.ClusterConfiguration, metadatacommon.Version) (*commonproto.ClusterConfiguration, error)) error {
	m.configLock.Lock()
	defer m.configLock.Unlock()

	current, err := m.configProvider.Load()
	if err != nil {
		return err
	}
	next, err := fn(metadatacodec.ClusterConfigCodec.Clone(current.Value), current.Version)
	if err != nil {
		return err
	}

	if err := next.Validate(); err != nil {
		return err
	}

	_, err = m.configProvider.Store(provider.Versioned[*commonproto.ClusterConfiguration]{
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

func (m *coordinatorMetadata) GetInstanceID() (string, error) {
	status, err := m.statusProvider.Load()
	if err != nil {
		return "", err
	}
	return status.Value.GetInstanceId(), nil
}

func (m *coordinatorMetadata) GetSelf() (*commonproto.Coordinator, error) {
	config, err := m.configProvider.Load()
	if err != nil {
		return nil, err
	}
	coordinator, ok := config.Value.GetCoordinator(m.name)
	if !ok {
		return nil, fmt.Errorf("coordinator %q not found in cluster configuration", m.name)
	}
	return coordinator.CloneVT(), nil
}

func (m *coordinatorMetadata) GetLeader() (*commonproto.Coordinator, error) {
	name, err := m.statusProvider.GetLeaderName()
	if err != nil {
		return nil, err
	}
	config, err := m.configProvider.Load()
	if err != nil {
		return nil, err
	}
	coordinator, ok := config.Value.GetCoordinator(name)
	if !ok {
		return nil, fmt.Errorf("coordinator %q not found in cluster configuration", name)
	}
	return coordinator.CloneVT(), nil
}

func (m *coordinatorMetadata) WaitToBecomeLeader() (<-chan struct{}, error) {
	m.logger.Info("Waiting to become leader")
	leadershipLost, err := m.statusProvider.WaitToBecomeLeader()
	if err != nil {
		return nil, fmt.Errorf("failed to wait in becoming leader: %w", err)
	}
	m.logger.Info("This coordinator is now leader")
	// The snapshots were loaded before the leadership, and the previous
	// leader may have changed them since.
	if err := m.statusProvider.Reload(); err != nil {
		return nil, fmt.Errorf("failed to reload the cluster status: %w", err)
	}
	if err := m.configProvider.Reload(); err != nil {
		return nil, fmt.Errorf("failed to reload the cluster configuration: %w", err)
	}
	// A coordinator that lost the leadership while reloading must not write.
	select {
	case <-leadershipLost:
		return nil, errors.New("lost the leadership while reloading the metadata")
	default:
	}
	// Initialize the instance id of a new cluster. The reloads just succeeded,
	// so a failed write fails the takeover rather than being retried.
	if err := m.computeStatus(func(status *commonproto.ClusterStatus, _ metadatacommon.Version) (*commonproto.ClusterStatus, bool, error) {
		if status.GetInstanceId() != "" {
			return status, false, nil
		}
		status.InstanceId = uuid.NewString()
		return status, true, nil
	}); err != nil {
		return nil, fmt.Errorf("failed to initialize the instance id: %w", err)
	}
	return leadershipLost, nil
}

func (m *coordinatorMetadata) AllocateShardIDs(count uint32) (int64, error) {
	var base int64
	if err := m.computeStatus(func(status *commonproto.ClusterStatus, _ metadatacommon.Version) (*commonproto.ClusterStatus, bool, error) {
		base = status.GetShardIdGenerator()
		status.ShardIdGenerator += int64(count)
		return status, true, nil
	}); err != nil {
		return 0, err
	}
	return base, nil
}

func (m *coordinatorMetadata) CreateNamespaceStatus(name string, status *commonproto.NamespaceStatus) error {
	return m.computeStatus(func(clusterStatus *commonproto.ClusterStatus, _ metadatacommon.Version) (*commonproto.ClusterStatus, bool, error) {
		if clusterStatus.Namespaces == nil {
			clusterStatus.Namespaces = map[string]*commonproto.NamespaceStatus{}
		}
		if _, exists := clusterStatus.Namespaces[name]; exists {
			return clusterStatus, false, fmt.Errorf("%w: namespace %q", metadatacommon.ErrAlreadyExists, name)
		}
		clusterStatus.Namespaces[name] = status
		return clusterStatus, true, nil
	})
}

func (m *coordinatorMetadata) ListNamespaceStatus() (map[string]commonobject.Borrowed[*commonproto.NamespaceStatus], error) {
	status, err := m.statusProvider.Load()
	if err != nil {
		return nil, err
	}
	namespaces := make(map[string]commonobject.Borrowed[*commonproto.NamespaceStatus], len(status.Value.GetNamespaces()))
	for name, status := range status.Value.GetNamespaces() {
		namespaces[name] = commonobject.Borrow(status)
	}
	return namespaces, nil
}

func (m *coordinatorMetadata) GetNamespaceStatus(namespace string) (commonobject.Borrowed[*commonproto.NamespaceStatus], bool, error) {
	status, err := m.statusProvider.Load()
	if err != nil {
		return commonobject.Borrowed[*commonproto.NamespaceStatus]{}, false, err
	}
	namespaceStatus, exists := status.Value.GetNamespaces()[namespace]
	if !exists {
		return commonobject.Borrowed[*commonproto.NamespaceStatus]{}, false, nil
	}
	return commonobject.Borrow(namespaceStatus), true, nil
}

func (m *coordinatorMetadata) DeleteNamespaceStatus(name string) commonobject.Borrowed[*commonproto.NamespaceStatus] {
	var namespaceStatus *commonproto.NamespaceStatus
	changed := false
	err := backoff.RetryNotify(func() error {
		return m.computeStatus(func(clusterStatus *commonproto.ClusterStatus, _ metadatacommon.Version) (*commonproto.ClusterStatus, bool, error) {
			ns, exists := clusterStatus.Namespaces[name]
			if !exists {
				return clusterStatus, false, nil
			}
			namespaceStatus = ns
			for shardID, shardMetadata := range ns.Shards {
				if shardMetadata.Status != commonproto.ShardStatusDeleting {
					shardMetadata.Status = commonproto.ShardStatusDeleting
					ns.Shards[shardID] = shardMetadata
					changed = true
				}
			}
			return clusterStatus, changed, nil
		})
	}, oxiatime.NewBackOff(m.ctx), func(err error, duration time.Duration) {
		m.logger.Warn(
			"failed to mark namespace deleting",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
	if err != nil {
		return commonobject.Borrowed[*commonproto.NamespaceStatus]{}
	}
	return commonobject.Borrow(namespaceStatus)
}

func (m *coordinatorMetadata) GetShardStatus(namespace string, shard int64) (commonobject.Borrowed[*commonproto.ShardMetadata], bool) {
	status, err := backoff.RetryNotifyWithData(m.statusProvider.Load, oxiatime.NewBackOff(m.ctx), func(err error, duration time.Duration) {
		m.logger.Warn(
			"failed to load the cluster status",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
	if err != nil {
		return commonobject.Borrowed[*commonproto.ShardMetadata]{}, false
	}
	namespaceStatus, exists := status.Value.GetNamespaces()[namespace]
	if !exists {
		return commonobject.Borrowed[*commonproto.ShardMetadata]{}, false
	}
	shardStatus, exists := namespaceStatus.GetShards()[shard]
	if !exists {
		return commonobject.Borrowed[*commonproto.ShardMetadata]{}, false
	}
	return commonobject.Borrow(shardStatus), true
}

func (m *coordinatorMetadata) UpdateShardStatus(namespace string, shard int64, shardMetadata *commonproto.ShardMetadata) error {
	if shardMetadata != nil {
		shardMetadata = gproto.Clone(shardMetadata).(*commonproto.ShardMetadata) //nolint:revive
	}
	shardExists := true
	err := backoff.RetryNotify(func() error {
		return m.computeStatus(func(clusterStatus *commonproto.ClusterStatus, _ metadatacommon.Version) (*commonproto.ClusterStatus, bool, error) {
			ns, exist := clusterStatus.Namespaces[namespace]
			if !exist {
				shardExists = false
				return clusterStatus, false, nil
			}
			if _, exist = ns.Shards[shard]; !exist {
				shardExists = false
				return clusterStatus, false, nil
			}
			ns.Shards[shard] = shardMetadata
			return clusterStatus, true, nil
		})
	}, oxiatime.NewBackOff(m.ctx), func(err error, duration time.Duration) {
		m.logger.Warn(
			"failed to update shard metadata",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
	if err != nil {
		return err
	}
	if !shardExists {
		return fmt.Errorf("%w: shard %d of namespace %q", metadatacommon.ErrNotFound, shard, namespace)
	}
	return nil
}

func (m *coordinatorMetadata) UpdateShardStatuses(
	namespace string,
	update func(shards map[int64]*commonproto.ShardMetadata) bool,
) error {
	namespaceExists := true
	err := backoff.RetryNotify(func() error {
		return m.computeStatus(func(clusterStatus *commonproto.ClusterStatus, _ metadatacommon.Version) (*commonproto.ClusterStatus, bool, error) {
			ns, exist := clusterStatus.Namespaces[namespace]
			namespaceExists = exist
			updated := exist && update(ns.Shards)
			if updated && len(ns.Shards) == 0 {
				delete(clusterStatus.Namespaces, namespace)
			}
			return clusterStatus, updated, nil
		})
	}, oxiatime.NewBackOff(m.ctx), func(err error, duration time.Duration) {
		m.logger.Warn(
			"failed to update shards metadata",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
	if err != nil {
		return err
	}
	if !namespaceExists {
		return fmt.Errorf("%w: namespace %q", metadatacommon.ErrNotFound, namespace)
	}
	return nil
}

func (m *coordinatorMetadata) DeleteShardStatus(namespace string, shard int64) error {
	return backoff.RetryNotify(func() error {
		return m.computeStatus(func(clusterStatus *commonproto.ClusterStatus, _ metadatacommon.Version) (*commonproto.ClusterStatus, bool, error) {
			ns, exist := clusterStatus.Namespaces[namespace]
			if !exist {
				return clusterStatus, false, nil
			}
			if _, exists := ns.Shards[shard]; !exists {
				return clusterStatus, false, nil
			}
			delete(ns.Shards, shard)
			if len(ns.Shards) == 0 {
				delete(clusterStatus.Namespaces, namespace)
			}
			return clusterStatus, true, nil
		})
	}, oxiatime.NewBackOff(m.ctx), func(err error, duration time.Duration) {
		m.logger.Warn(
			"failed to delete shard metadata",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
}

func (m *coordinatorMetadata) GetConfig() commonobject.Borrowed[*commonproto.ClusterConfiguration] {
	config, err := backoff.RetryNotifyWithData(m.configProvider.Load, oxiatime.NewBackOff(m.ctx), func(err error, duration time.Duration) {
		m.logger.Warn(
			"failed to load the cluster configuration",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
	if err != nil {
		return commonobject.Borrowed[*commonproto.ClusterConfiguration]{}
	}
	return commonobject.Borrow(config.Value)
}

func (m *coordinatorMetadata) SubscribeConfig() *cache.Subscription[provider.Versioned[*commonproto.ClusterConfiguration]] {
	return m.configProvider.Subscribe()
}

func (m *coordinatorMetadata) GetLoadBalancer() commonobject.Borrowed[*commonproto.LoadBalancer] {
	return commonobject.Borrow(m.GetConfig().UnsafeBorrow().GetLoadBalancerWithDefaults())
}

func (m *coordinatorMetadata) CreateNamespace(namespace *commonproto.Namespace) error {
	name := namespace.GetName()

	return m.computeConfig(func(config *commonproto.ClusterConfiguration, _ metadatacommon.Version) (*commonproto.ClusterConfiguration, error) {
		for _, existing := range config.GetNamespaces() {
			if existing.GetName() == name {
				return nil, metadatacommon.ErrAlreadyExists
			}
		}
		if namespace.GetReplicationFactor() > uint32(len(config.GetServers())) {
			return nil, fmt.Errorf("%w: namespace %q has replicationFactor=%d but only %d servers are configured",
				metadatacommon.ErrFailedPrecondition, name, namespace.GetReplicationFactor(), len(config.GetServers()))
		}

		config.Namespaces = append(config.Namespaces, namespace)
		return config, nil
	})
}

func (m *coordinatorMetadata) PatchNamespace(desiredNamespace *commonproto.Namespace) (*commonproto.Namespace, error) {
	var updated *commonproto.Namespace
	if err := m.computeConfig(func(config *commonproto.ClusterConfiguration, _ metadatacommon.Version) (*commonproto.ClusterConfiguration, error) {
		for _, namespace := range config.GetNamespaces() {
			if namespace.GetName() != desiredNamespace.GetName() {
				continue
			}
			if replicationFactor := desiredNamespace.GetReplicationFactor(); replicationFactor != 0 {
				if replicationFactor > uint32(len(config.GetServers())) {
					return nil, fmt.Errorf("%w: namespace %q has replicationFactor=%d but only %d servers are configured",
						metadatacommon.ErrFailedPrecondition, namespace.GetName(), replicationFactor, len(config.GetServers()))
				}
				namespace.ReplicationFactor = replicationFactor
			}
			if desiredNamespace.NotificationsEnabled != nil {
				notificationsEnabled := desiredNamespace.GetNotificationsEnabled()
				namespace.NotificationsEnabled = &notificationsEnabled
			}

			updated = namespace
			return config, nil
		}
		return nil, metadatacommon.ErrNotFound
	}); err != nil {
		return nil, err
	}

	return updated, nil
}

func (m *coordinatorMetadata) DeleteNamespace(name string) (*commonproto.Namespace, error) {
	var deleted *commonproto.Namespace
	if err := m.computeConfig(func(config *commonproto.ClusterConfiguration, _ metadatacommon.Version) (*commonproto.ClusterConfiguration, error) {
		for i, namespace := range config.GetNamespaces() {
			if namespace.GetName() != name {
				continue
			}

			deleted = namespace
			config.Namespaces = append(config.Namespaces[:i], config.Namespaces[i+1:]...)
			return config, nil
		}
		return nil, metadatacommon.ErrNotFound
	}); err != nil {
		return nil, err
	}

	return deleted, nil
}

func (m *coordinatorMetadata) CreateDataServer(dataServer *commonproto.DataServer) error {
	name := dataServer.GetIdentity().GetName()

	return m.computeConfig(func(config *commonproto.ClusterConfiguration, _ metadatacommon.Version) (*commonproto.ClusterConfiguration, error) {
		if _, exists := config.GetDataServer(name); exists {
			return nil, metadatacommon.ErrAlreadyExists
		}

		config.Servers = append(config.Servers, dataServer.GetIdentity())
		if metadata := dataServer.GetMetadata(); metadata != nil {
			if config.ServerMetadata == nil {
				config.ServerMetadata = map[string]*commonproto.DataServerMetadata{}
			}
			config.ServerMetadata[name] = metadata
		}

		return config, nil
	})
}

func (m *coordinatorMetadata) PatchDataServer(desireDataServer *commonproto.DataServer) (*commonproto.DataServer, error) {
	var updated *commonproto.DataServer
	if err := m.computeConfig(func(config *commonproto.ClusterConfiguration, _ metadatacommon.Version) (*commonproto.ClusterConfiguration, error) {
		for _, existID := range config.GetServers() {
			if existID.GetNameOrDefault() != desireDataServer.GetNameOrDefault() {
				continue
			}
			if public := desireDataServer.GetIdentity().GetPublic(); public != "" {
				existID.Public = public
			}
			if internal := desireDataServer.GetIdentity().GetInternal(); internal != "" {
				existID.Internal = internal
			}
			if config.ServerMetadata == nil {
				config.ServerMetadata = map[string]*commonproto.DataServerMetadata{}
			}
			var dsMeta *commonproto.DataServerMetadata
			var ok bool
			if dsMeta, ok = config.ServerMetadata[existID.GetNameOrDefault()]; !ok {
				dsMeta = &commonproto.DataServerMetadata{}
			}
			if desireDataServer.Metadata != nil {
				if desireDataServer.Metadata.Labels != nil {
					dsMeta.Labels = desireDataServer.Metadata.Labels
				}
				config.ServerMetadata[existID.GetNameOrDefault()] = dsMeta
			}
			updated = &commonproto.DataServer{
				Identity: existID,
				Metadata: dsMeta,
			}
			return config, nil
		}
		return nil, metadatacommon.ErrNotFound
	}); err != nil {
		return nil, err
	}

	return updated, nil
}

func (m *coordinatorMetadata) DeleteDataServer(name string) (*commonproto.DataServer, error) {
	var deleted *commonproto.DataServer
	if err := m.computeConfig(func(config *commonproto.ClusterConfiguration, _ metadatacommon.Version) (*commonproto.ClusterConfiguration, error) {
		for i, identity := range config.GetServers() {
			if identity.GetNameOrDefault() != name {
				continue
			}

			dataServer, _ := config.GetDataServer(name)
			deleted = dataServer
			remainingServerCount := len(config.GetServers()) - 1
			for _, namespace := range config.GetNamespaces() {
				if uint64(namespace.GetReplicationFactor()) > uint64(remainingServerCount) {
					return nil, fmt.Errorf("%w: cannot delete data server %q because namespace %q replicationFactor=%d exceeds remaining data servers=%d",
						metadatacommon.ErrFailedPrecondition,
						name,
						namespace.GetName(),
						namespace.GetReplicationFactor(),
						remainingServerCount)
				}
			}
			config.Servers = append(config.Servers[:i], config.Servers[i+1:]...)
			if config.ServerMetadata != nil {
				delete(config.ServerMetadata, name)
			}
			return config, nil
		}
		return nil, metadatacommon.ErrNotFound
	}); err != nil {
		return nil, err
	}

	return deleted, nil
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
	ns, exists := m.ListNamespace()[namespace]
	return ns, exists
}

func (m *coordinatorMetadata) ListNamespace() map[string]commonobject.Borrowed[*commonproto.Namespace] {
	configNamespaces := m.GetConfig().UnsafeBorrow().GetNamespaces()
	namespaces := make(map[string]commonobject.Borrowed[*commonproto.Namespace], len(configNamespaces))
	for _, namespace := range configNamespaces {
		namespaces[namespace.GetName()] = commonobject.Borrow(namespace)
	}
	return namespaces
}

func (m *coordinatorMetadata) GetDataServer(name string) (commonobject.Borrowed[*commonproto.DataServer], bool) {
	value, ok := m.GetConfig().UnsafeBorrow().GetDataServer(name)
	if !ok {
		return commonobject.Borrowed[*commonproto.DataServer]{}, false
	}
	return commonobject.Borrow(value), true
}
