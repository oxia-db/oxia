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
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"

	"github.com/cenkalti/backoff/v4"
	"github.com/emirpasic/gods/v2/trees/redblacktree"
	"github.com/google/uuid"
	gproto "google.golang.org/protobuf/proto"

	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/process"
	commonproto "github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/rpc"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
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
	ConfigWatch() *commonwatch.Watch[*commonproto.ClusterConfiguration]
	GetLoadBalancer() commonobject.Borrowed[*commonproto.LoadBalancer]

	ListDataServer() map[string]commonobject.Borrowed[*commonproto.DataServer]
	GetDataServer(name string) (commonobject.Borrowed[*commonproto.DataServer], bool)
}

type EnsembleSupplier func(
	namespaceConfig *commonproto.Namespace,
	status *commonproto.ClusterStatus,
) ([]*commonproto.DataServerIdentity, error)

type coordinatorMetadata struct {
	logger    *slog.Logger
	ctx       context.Context
	ctxCancel context.CancelFunc
	wg        sync.WaitGroup

	statusProvider provider.Provider[*commonproto.ClusterStatus]
	configProvider provider.Provider[*commonproto.ClusterConfiguration]

	statusLock       sync.RWMutex
	currentStatus    *commonproto.ClusterStatus
	currentVersionID metadatacommon.Version
	changeCh         chan struct{}

	clusterConfigLock     sync.RWMutex
	currentClusterConfig  *commonproto.ClusterConfiguration
	clusterConfigWatch    *commonwatch.Watch[*commonproto.ClusterConfiguration]
	namespaceConfigsIndex *redblacktree.Tree[string, *commonproto.Namespace]
}

func newMetadata(ctx context.Context, statusProvider provider.Provider[*commonproto.ClusterStatus], configProvider provider.Provider[*commonproto.ClusterConfiguration]) Metadata {
	metadataCtx, cancel := context.WithCancel(ctx)
	m := &coordinatorMetadata{
		logger:             slog.With(slog.String("component", "coordinator-metadata")),
		ctx:                metadataCtx,
		ctxCancel:          cancel,
		wg:                 sync.WaitGroup{},
		statusProvider:     statusProvider,
		configProvider:     configProvider,
		currentVersionID:   metadatacommon.NotExists,
		changeCh:           make(chan struct{}),
		clusterConfigWatch: commonwatch.New(&commonproto.ClusterConfiguration{}),
	}

	m.doStatusRecovery()

	if configWatch, err := configProvider.Watch(); err != nil && !errors.Is(err, metadatacommon.ErrWatchUnsupported) {
		m.logger.Warn("failed to watch cluster config provider", slog.Any("error", err))
	} else if configWatch != nil {
		m.wg.Go(func() {
			process.DoWithLabels(metadataCtx, map[string]string{
				"component": "coordinator-metadata-config-watcher",
			}, func() {
				m.waitForConfigUpdates(configWatch)
			})
		})
	}

	return m
}

func (m *coordinatorMetadata) Close() error {
	m.ctxCancel()
	m.wg.Wait()
	return nil
}

func (m *coordinatorMetadata) notifyStatusChange() {
	close(m.changeCh)
	m.changeCh = make(chan struct{})
}

func (m *coordinatorMetadata) doStatusRecovery() {
	m.statusLock.Lock()
	defer m.statusLock.Unlock()

	_ = backoff.RetryNotify(func() error {
		clusterStatus, version, err := m.statusProvider.Get()
		if err != nil {
			return err
		}
		if clusterStatus == nil {
			m.currentStatus = nil
			m.currentVersionID = version
			return nil
		}
		m.currentStatus = clusterStatus
		m.currentVersionID = version
		return nil
	}, backoff.NewExponentialBackOff(), func(err error, duration time.Duration) {
		m.logger.Warn(
			"failed to load status, retrying later",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})

	if m.currentStatus == nil {
		m.currentStatus = commonproto.NewClusterStatus()
	}
	if m.currentStatus.InstanceId == "" {
		clonedStatus := gproto.Clone(m.currentStatus).(*commonproto.ClusterStatus) //nolint:revive
		clonedStatus.InstanceId = uuid.NewString()
		m.persistStatusLocked(clonedStatus, "failed to initialize instance id")
		m.notifyStatusChange()
	}
}

func (m *coordinatorMetadata) persistStatusLocked(newStatus *commonproto.ClusterStatus, warnMessage string) {
	_ = backoff.RetryNotify(func() error {
		versionID, err := m.statusProvider.Store(newStatus, m.currentVersionID)
		if err != nil {
			return err
		}
		m.currentStatus = newStatus
		m.currentVersionID = versionID
		return nil
	}, backoff.NewExponentialBackOff(), func(err error, duration time.Duration) {
		m.logger.Warn(
			warnMessage,
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
}

func (m *coordinatorMetadata) GetStatus() commonobject.Borrowed[*commonproto.ClusterStatus] {
	m.statusLock.RLock()
	defer m.statusLock.RUnlock()
	return commonobject.Borrow(m.currentStatus)
}

func (m *coordinatorMetadata) UpdateStatus(newStatus *commonproto.ClusterStatus) {
	m.statusLock.Lock()
	defer m.statusLock.Unlock()

	m.persistStatusLocked(newStatus, "failed to update status")
	m.notifyStatusChange()
}

func (m *coordinatorMetadata) ReserveShardIDs(count uint32) int64 {
	m.statusLock.Lock()
	defer m.statusLock.Unlock()

	clonedStatus := gproto.Clone(m.currentStatus).(*commonproto.ClusterStatus) //nolint:revive
	base := clonedStatus.ShardIdGenerator
	clonedStatus.ShardIdGenerator += int64(count)
	m.persistStatusLocked(clonedStatus, "failed to reserve shard ids")
	m.notifyStatusChange()
	return base
}

func (m *coordinatorMetadata) CreateNamespaceStatus(name string, status *commonproto.NamespaceStatus) bool {
	m.loadClusterConfigWithInitSlow()
	serverCount := len(m.GetConfig().UnsafeBorrow().GetServers())

	m.statusLock.Lock()
	defer m.statusLock.Unlock()

	clonedStatus := gproto.Clone(m.currentStatus).(*commonproto.ClusterStatus) //nolint:revive
	if clonedStatus.Namespaces == nil {
		clonedStatus.Namespaces = map[string]*commonproto.NamespaceStatus{}
	}

	if _, exists := clonedStatus.Namespaces[name]; exists {
		return false
	}

	namespaceStatus := gproto.Clone(status).(*commonproto.NamespaceStatus) //nolint:revive
	clonedStatus.Namespaces[name] = namespaceStatus
	clonedStatus.ServerIdx = (clonedStatus.ServerIdx + uint32(len(namespaceStatus.Shards))*namespaceStatus.GetReplicationFactor()) % uint32(serverCount)

	m.persistStatusLocked(clonedStatus, "failed to create namespace status")
	m.notifyStatusChange()
	return true
}

func (m *coordinatorMetadata) ListNamespaceStatus() map[string]commonobject.Borrowed[*commonproto.NamespaceStatus] {
	m.statusLock.RLock()
	defer m.statusLock.RUnlock()

	namespaces := make(map[string]commonobject.Borrowed[*commonproto.NamespaceStatus], len(m.currentStatus.Namespaces))
	for name, status := range m.currentStatus.Namespaces {
		namespaces[name] = commonobject.Borrow(status)
	}
	return namespaces
}

func (m *coordinatorMetadata) GetNamespaceStatus(namespace string) (commonobject.Borrowed[*commonproto.NamespaceStatus], bool) {
	m.statusLock.RLock()
	defer m.statusLock.RUnlock()

	namespaceStatus, exists := m.currentStatus.Namespaces[namespace]
	if !exists {
		return commonobject.Borrowed[*commonproto.NamespaceStatus]{}, false
	}
	return commonobject.Borrow(namespaceStatus), true
}

func (m *coordinatorMetadata) DeleteNamespaceStatus(name string) commonobject.Borrowed[*commonproto.NamespaceStatus] {
	m.statusLock.Lock()
	defer m.statusLock.Unlock()

	clonedStatus := gproto.Clone(m.currentStatus).(*commonproto.ClusterStatus) //nolint:revive
	namespaceStatus, exists := clonedStatus.Namespaces[name]
	if !exists {
		return commonobject.Borrowed[*commonproto.NamespaceStatus]{}
	}

	changed := false
	for shardID, shardMetadata := range namespaceStatus.Shards {
		if shardMetadata.Status != commonproto.ShardStatusDeleting {
			shardMetadata.Status = commonproto.ShardStatusDeleting
			namespaceStatus.Shards[shardID] = shardMetadata
			changed = true
		}
	}
	if changed {
		m.persistStatusLocked(clonedStatus, "failed to mark namespace deleting")
		m.notifyStatusChange()
	}
	return commonobject.Borrow(namespaceStatus)
}

func (m *coordinatorMetadata) UpdateShardStatus(namespace string, shard int64, shardMetadata *commonproto.ShardMetadata) {
	m.statusLock.Lock()
	defer m.statusLock.Unlock()

	clonedStatus := gproto.Clone(m.currentStatus).(*commonproto.ClusterStatus) //nolint:revive
	ns, exist := clonedStatus.Namespaces[namespace]
	if !exist {
		return
	}
	ns.Shards[shard] = gproto.Clone(shardMetadata).(*commonproto.ShardMetadata) //nolint:revive
	m.persistStatusLocked(clonedStatus, "failed to update shard metadata")
	m.notifyStatusChange()
}

func (m *coordinatorMetadata) DeleteShardStatus(namespace string, shard int64) {
	m.statusLock.Lock()
	defer m.statusLock.Unlock()

	clonedStatus := gproto.Clone(m.currentStatus).(*commonproto.ClusterStatus) //nolint:revive
	ns, exist := clonedStatus.Namespaces[namespace]
	if !exist {
		return
	}
	delete(ns.Shards, shard)
	if len(ns.Shards) == 0 {
		delete(clonedStatus.Namespaces, namespace)
	}
	m.persistStatusLocked(clonedStatus, "failed to delete shard metadata")
	m.notifyStatusChange()
}

func (m *coordinatorMetadata) statusChangeNotify() <-chan struct{} {
	m.statusLock.RLock()
	defer m.statusLock.RUnlock()
	return m.changeCh
}

func (m *coordinatorMetadata) loadClusterConfigWithInitSlow() {
	m.clusterConfigLock.Lock()
	defer m.clusterConfigLock.Unlock()
	if m.currentClusterConfig != nil {
		return
	}

	_ = backoff.RetryNotify(func() error {
		newConfig, err := loadClusterConfigFromProvider(m.configProvider)
		if err != nil {
			return err
		}
		m.currentClusterConfig = gproto.Clone(newConfig).(*commonproto.ClusterConfiguration) //nolint:revive
		m.rebuildConfigIndexesLocked()
		m.clusterConfigWatch.Publish(m.currentClusterConfig)
		return nil
	}, backoff.NewExponentialBackOff(), func(err error, duration time.Duration) {
		m.logger.Warn(
			"failed to load cluster configuration, retrying later",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
}

func loadClusterConfigFromProvider(configProvider provider.Provider[*commonproto.ClusterConfiguration]) (*commonproto.ClusterConfiguration, error) {
	config, _, err := configProvider.Get()
	if err != nil {
		return nil, err
	}
	if config == nil {
		return nil, metadatacommon.ErrNotInitialized
	}
	if err := validateClusterConfig(config); err != nil {
		return nil, err
	}
	return config, nil
}

func validateClusterConfig(config *commonproto.ClusterConfiguration) error {
	if config == nil {
		return metadatacommon.ErrNotInitialized
	}
	if err := config.Validate(); err != nil {
		return err
	}
	for _, authority := range config.GetAllowExtraAuthorities() {
		if err := rpc.ValidateAuthorityAddress(authority); err != nil {
			return fmt.Errorf("cluster configuration: invalid allowExtraAuthorities entry %q: %w", authority, err)
		}
	}
	return nil
}

func (m *coordinatorMetadata) rebuildConfigIndexesLocked() {
	namespaceConfigs := redblacktree.New[string, *commonproto.Namespace]()
	for _, ns := range m.currentClusterConfig.GetNamespaces() {
		namespaceConfigs.Put(ns.GetName(), ns)
	}
	m.namespaceConfigsIndex = namespaceConfigs
}

func (m *coordinatorMetadata) waitForConfigUpdates(configWatch *commonwatch.Receiver[*commonproto.ClusterConfiguration]) {
	m.applyConfigWatchValue(configWatch)
	for {
		select {
		case <-m.ctx.Done():
			return

		case <-configWatch.Changed():
			m.logger.Info("Received cluster config change event")
			m.applyConfigWatchValue(configWatch)
		}
	}
}

func (m *coordinatorMetadata) applyConfigWatchValue(configWatch *commonwatch.Receiver[*commonproto.ClusterConfiguration]) {
	config := configWatch.Load()
	if err := validateClusterConfig(config); err != nil {
		m.logger.Warn("received invalid cluster config watch value", slog.Any("error", err))
		return
	}
	clonedConfig := gproto.Clone(config).(*commonproto.ClusterConfiguration) //nolint:revive

	m.clusterConfigLock.Lock()
	oldClusterConfig := m.currentClusterConfig
	m.currentClusterConfig = clonedConfig
	m.rebuildConfigIndexesLocked()
	m.clusterConfigLock.Unlock()

	if gproto.Equal(oldClusterConfig, clonedConfig) {
		m.logger.Info("No cluster config changes detected")
		return
	}
	m.clusterConfigWatch.Publish(clonedConfig)
}

func (m *coordinatorMetadata) GetConfig() commonobject.Borrowed[*commonproto.ClusterConfiguration] {
	m.clusterConfigLock.RLock()
	defer m.clusterConfigLock.RUnlock()
	if m.currentClusterConfig == nil {
		m.clusterConfigLock.RUnlock()
		m.loadClusterConfigWithInitSlow()
		m.clusterConfigLock.RLock()
	}
	return commonobject.Borrow(m.currentClusterConfig)
}

func (m *coordinatorMetadata) ConfigWatch() *commonwatch.Watch[*commonproto.ClusterConfiguration] {
	return m.clusterConfigWatch
}

func (m *coordinatorMetadata) GetLoadBalancer() commonobject.Borrowed[*commonproto.LoadBalancer] {
	return commonobject.Borrow(m.GetConfig().UnsafeBorrow().GetLoadBalancerWithDefaults())
}

func (m *coordinatorMetadata) ListDataServer() map[string]commonobject.Borrowed[*commonproto.DataServer] {
	m.clusterConfigLock.RLock()
	defer m.clusterConfigLock.RUnlock()
	if m.currentClusterConfig == nil {
		m.clusterConfigLock.RUnlock()
		m.loadClusterConfigWithInitSlow()
		m.clusterConfigLock.RLock()
	}

	dataServers := make(map[string]commonobject.Borrowed[*commonproto.DataServer], len(m.currentClusterConfig.GetServers()))
	for _, server := range m.currentClusterConfig.GetServers() {
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
		if value, found := m.currentClusterConfig.GetServerMetadata()[name]; found {
			dataServer.Metadata = value
		}
		dataServers[name] = commonobject.Borrow(dataServer)
	}
	return dataServers
}

func (m *coordinatorMetadata) GetNamespace(namespace string) (commonobject.Borrowed[*commonproto.Namespace], bool) {
	m.clusterConfigLock.RLock()
	defer m.clusterConfigLock.RUnlock()
	if m.currentClusterConfig == nil {
		m.clusterConfigLock.RUnlock()
		m.loadClusterConfigWithInitSlow()
		m.clusterConfigLock.RLock()
	}
	value, ok := m.namespaceConfigsIndex.Get(namespace)
	if !ok {
		return commonobject.Borrowed[*commonproto.Namespace]{}, false
	}
	return commonobject.Borrow(value), true
}

func (m *coordinatorMetadata) GetDataServer(name string) (commonobject.Borrowed[*commonproto.DataServer], bool) {
	m.clusterConfigLock.RLock()
	defer m.clusterConfigLock.RUnlock()
	if m.currentClusterConfig == nil {
		m.clusterConfigLock.RUnlock()
		m.loadClusterConfigWithInitSlow()
		m.clusterConfigLock.RLock()
	}

	value, ok := m.currentClusterConfig.GetDataServer(name)
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
