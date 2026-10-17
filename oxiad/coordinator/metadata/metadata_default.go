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

type defaultMetadata struct {
	name   string
	logger *slog.Logger
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	configProvider provider.Provider[*commonproto.ClusterConfiguration]
	configLock     sync.Mutex

	statusProvider provider.Provider[*commonproto.ClusterStatus]
	statusLock     sync.Mutex
	// leadershipLost is the channel WaitToBecomeLeader returned: closed once
	// this coordinator lost the leadership. Guarded by statusLock.
	leadershipLost <-chan struct{}
}

func newDefaultMetadata(
	ctx context.Context,
	statusProvider provider.Provider[*commonproto.ClusterStatus],
	configProvider provider.Provider[*commonproto.ClusterConfiguration],
	name string,
) Metadata {
	metadataCtx, cancel := context.WithCancel(ctx)
	m := &defaultMetadata{
		name:           name,
		logger:         slog.With(slog.String("component", "coordinator-metadata")),
		ctx:            metadataCtx,
		cancel:         cancel,
		configProvider: configProvider,
		statusProvider: statusProvider,
	}
	return m
}

func (m *defaultMetadata) computeStatus(fn func(*commonproto.ClusterStatus, metadatacommon.Version) (*commonproto.ClusterStatus, bool, error)) error {
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
		// Another coordinator wrote the status. The version check refused
		// this write, so nothing is lost: the write fails, and its caller
		// retries over the fresh state, or stops once this coordinator learns
		// that it lost the leadership. A new leader can write before that
		// happens, during a handover.
		if !m.stoppedLeadingLocked() {
			m.logger.Error("Cluster status written by another coordinator while this one leads",
				slog.Any("error", err))
		}
		return fmt.Errorf("cluster status written by another coordinator: %w", err)
	}
	return err
}

// stoppedLeadingLocked reports whether this coordinator lost the leadership or
// is closing. The caller holds statusLock.
func (m *defaultMetadata) stoppedLeadingLocked() bool {
	if m.ctx.Err() != nil {
		return true
	}
	if m.leadershipLost == nil {
		return false
	}
	select {
	case <-m.leadershipLost:
		return true
	default:
		return false
	}
}

func (m *defaultMetadata) computeConfig(fn func(*commonproto.ClusterConfiguration, metadatacommon.Version) (*commonproto.ClusterConfiguration, error)) error {
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

func (m *defaultMetadata) Close() error {
	m.cancel()
	m.wg.Wait()
	return nil
}

func (m *defaultMetadata) GetInstanceID() (string, error) {
	status, err := m.statusProvider.Load()
	if err != nil {
		return "", err
	}
	return status.Value.GetInstanceId(), nil
}

func (m *defaultMetadata) GetSelf() (*commonproto.Coordinator, error) {
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

func (m *defaultMetadata) GetLeader() (*commonproto.Coordinator, error) {
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

func (m *defaultMetadata) WaitToBecomeLeader() (<-chan struct{}, error) {
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
	if err := m.checkConfigNotDeleted(); err != nil {
		return nil, err
	}
	m.statusLock.Lock()
	m.leadershipLost = leadershipLost
	m.statusLock.Unlock()
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

// checkConfigNotDeleted fails if the configuration is missing while the status
// has namespaces. The status only gets a namespace from a configuration, so
// this one was deleted: reconciling it as empty would drain every data server
// and delete every namespace. Unlike what a provider remembers of the
// documents it read, the status survives a restart.
func (m *defaultMetadata) checkConfigNotDeleted() error {
	config, err := m.configProvider.Load()
	if err != nil {
		return err
	}
	if config.Version != metadatacommon.NotExists {
		return nil
	}
	status, err := m.statusProvider.Load()
	if err != nil {
		return err
	}
	if len(status.Value.GetNamespaces()) > 0 {
		return errors.New("the cluster configuration is missing, but the cluster status has namespaces: " +
			"restore the configuration")
	}
	return nil
}

func (m *defaultMetadata) AllocateShardIDs(count uint32) (int64, error) {
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

func (m *defaultMetadata) CreateNamespaceStatus(namespace string, status *commonproto.NamespaceStatus) error {
	return m.computeStatus(func(clusterStatus *commonproto.ClusterStatus, _ metadatacommon.Version) (*commonproto.ClusterStatus, bool, error) {
		if clusterStatus.Namespaces == nil {
			clusterStatus.Namespaces = map[string]*commonproto.NamespaceStatus{}
		}
		if _, exists := clusterStatus.Namespaces[namespace]; exists {
			return clusterStatus, false, fmt.Errorf("%w: namespace %q", metadatacommon.ErrAlreadyExists, namespace)
		}
		clusterStatus.Namespaces[namespace] = status
		return clusterStatus, true, nil
	})
}

func (m *defaultMetadata) ListNamespaceStatus() (map[string]commonobject.Borrowed[*commonproto.NamespaceStatus], error) {
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

func (m *defaultMetadata) GetNamespaceStatus(namespace string) (commonobject.Borrowed[*commonproto.NamespaceStatus], bool, error) {
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

func (m *defaultMetadata) PatchNamespaceStatus(
	namespace string,
	modify func(status *commonproto.NamespaceStatus) bool,
) error {
	namespaceExists := true
	if err := m.computeStatus(func(clusterStatus *commonproto.ClusterStatus, _ metadatacommon.Version) (*commonproto.ClusterStatus, bool, error) {
		ns, exist := clusterStatus.Namespaces[namespace]
		namespaceExists = exist
		updated := exist && modify(ns)
		if updated && len(ns.Shards) == 0 {
			delete(clusterStatus.Namespaces, namespace)
		}
		return clusterStatus, updated, nil
	}); err != nil {
		return err
	}
	if !namespaceExists {
		return fmt.Errorf("%w: namespace %q", metadatacommon.ErrNotFound, namespace)
	}
	return nil
}

func (m *defaultMetadata) DeleteNamespaceStatus(namespace string) commonobject.Borrowed[*commonproto.NamespaceStatus] {
	var namespaceStatus *commonproto.NamespaceStatus
	changed := false
	err := backoff.RetryNotify(func() error {
		return m.computeStatus(func(clusterStatus *commonproto.ClusterStatus, _ metadatacommon.Version) (*commonproto.ClusterStatus, bool, error) {
			ns, exists := clusterStatus.Namespaces[namespace]
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

func (m *defaultMetadata) GetShardStatus(namespace string, shard int64) (commonobject.Borrowed[*commonproto.ShardMetadata], bool, error) {
	status, err := m.statusProvider.Load()
	if err != nil {
		return commonobject.Borrowed[*commonproto.ShardMetadata]{}, false, err
	}
	namespaceStatus, exists := status.Value.GetNamespaces()[namespace]
	if !exists {
		return commonobject.Borrowed[*commonproto.ShardMetadata]{}, false, nil
	}
	shardStatus, exists := namespaceStatus.GetShards()[shard]
	if !exists {
		return commonobject.Borrowed[*commonproto.ShardMetadata]{}, false, nil
	}
	return commonobject.Borrow(shardStatus), true, nil
}

func (m *defaultMetadata) UpdateShardStatus(namespace string, shard int64, shardMetadata *commonproto.ShardMetadata) error {
	if shardMetadata != nil {
		shardMetadata = gproto.Clone(shardMetadata).(*commonproto.ShardMetadata) //nolint:revive
	}
	shardExists := true
	if err := m.computeStatus(func(clusterStatus *commonproto.ClusterStatus, _ metadatacommon.Version) (*commonproto.ClusterStatus, bool, error) {
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
	}); err != nil {
		return err
	}
	if !shardExists {
		return fmt.Errorf("%w: shard %d of namespace %q", metadatacommon.ErrNotFound, shard, namespace)
	}
	return nil
}

func (m *defaultMetadata) DeleteShardStatus(namespace string, shard int64) error {
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
}

func (m *defaultMetadata) GetConfig() (commonobject.Borrowed[*commonproto.ClusterConfiguration], error) {
	config, err := m.configProvider.Load()
	if err != nil {
		return commonobject.Borrowed[*commonproto.ClusterConfiguration]{}, err
	}
	return commonobject.Borrow(config.Value), nil
}

func (m *defaultMetadata) SubscribeConfig() *cache.Subscription[provider.Versioned[*commonproto.ClusterConfiguration]] {
	return m.configProvider.Subscribe()
}

func (m *defaultMetadata) GetLoadBalancerConfig() (commonobject.Borrowed[*commonproto.LoadBalancer], error) {
	config, err := m.configProvider.Load()
	if err != nil {
		return commonobject.Borrowed[*commonproto.LoadBalancer]{}, err
	}
	return commonobject.Borrow(config.Value.GetLoadBalancerWithDefaults()), nil
}

func (m *defaultMetadata) CreateNamespace(namespace *commonproto.Namespace) error {
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

func (m *defaultMetadata) PatchNamespace(desiredNamespace *commonproto.Namespace) (*commonproto.Namespace, error) {
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

func (m *defaultMetadata) DeleteNamespace(name string) (*commonproto.Namespace, error) {
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

func (m *defaultMetadata) CreateDataServer(dataServer *commonproto.DataServer) error {
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

func (m *defaultMetadata) PatchDataServer(desireDataServer *commonproto.DataServer) (*commonproto.DataServer, error) {
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

func (m *defaultMetadata) DeleteDataServer(name string) (*commonproto.DataServer, error) {
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

func (m *defaultMetadata) ListDataServer() (map[string]commonobject.Borrowed[*commonproto.DataServer], error) {
	loaded, err := m.configProvider.Load()
	if err != nil {
		return nil, err
	}
	config := loaded.Value
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
	return dataServers, nil
}

func (m *defaultMetadata) GetNamespace(name string) (commonobject.Borrowed[*commonproto.Namespace], bool, error) {
	config, err := m.configProvider.Load()
	if err != nil {
		return commonobject.Borrowed[*commonproto.Namespace]{}, false, err
	}
	for _, ns := range config.Value.GetNamespaces() {
		if ns.GetName() == name {
			return commonobject.Borrow(ns), true, nil
		}
	}
	return commonobject.Borrowed[*commonproto.Namespace]{}, false, nil
}

func (m *defaultMetadata) GetNamespaceTermOptions(name string) (*commonproto.NewTermOptions, error) {
	termOptions := &commonproto.NewTermOptions{
		EnableNotifications: true,
		KeySorting:          commonproto.KeySortingType_UNKNOWN,
	}
	borrowedNamespace, exists, err := m.GetNamespace(name)
	if err != nil {
		return nil, err
	}
	if exists {
		ns := borrowedNamespace.UnsafeBorrow()
		termOptions.EnableNotifications = ns.NotificationsEnabledOrDefault()
		termOptions.KeySorting, _ = ns.GetKeySortingType()
	}
	return termOptions, nil
}

func (m *defaultMetadata) ListNamespace() (map[string]commonobject.Borrowed[*commonproto.Namespace], error) {
	config, err := m.configProvider.Load()
	if err != nil {
		return nil, err
	}
	configNamespaces := config.Value.GetNamespaces()
	namespaces := make(map[string]commonobject.Borrowed[*commonproto.Namespace], len(configNamespaces))
	for _, namespace := range configNamespaces {
		namespaces[namespace.GetName()] = commonobject.Borrow(namespace)
	}
	return namespaces, nil
}

func (m *defaultMetadata) GetDataServer(name string) (commonobject.Borrowed[*commonproto.DataServer], bool, error) {
	config, err := m.configProvider.Load()
	if err != nil {
		return commonobject.Borrowed[*commonproto.DataServer]{}, false, err
	}
	value, ok := config.Value.GetDataServer(name)
	if !ok {
		return commonobject.Borrowed[*commonproto.DataServer]{}, false, nil
	}
	return commonobject.Borrow(value), true, nil
}
