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
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/emirpasic/gods/v2/sets/linkedhashset"
	"github.com/emirpasic/gods/v2/trees/redblacktree"
	"github.com/google/uuid"
	gproto "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/process"
	commonproto "github.com/oxia-db/oxia/common/proto"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/util"
)

type Metadata interface {
	io.Closer

	LoadStatus() *model.ClusterStatus
	ApplyStatusChanges(config *commonproto.ClusterConfiguration, ensembleSupplier EnsembleSupplier) (*model.ClusterStatus, map[int64]string, []int64)
	UpdateStatus(newStatus *model.ClusterStatus)
	UpdateShardMetadata(namespace string, shard int64, shardMetadata model.ShardMetadata)
	DeleteShardMetadata(namespace string, shard int64)
	IsReady(clusterConfig *commonproto.ClusterConfiguration) bool
	StatusChangeNotify() <-chan struct{}

	LoadConfig() *commonproto.ClusterConfiguration
	ConfigWatch() *commonoption.Watch[*commonproto.ClusterConfiguration]
	LoadLoadBalancer() *commonproto.LoadBalancer
	Nodes() *linkedhashset.Set[string]
	NodesWithMetadata() (*linkedhashset.Set[string], map[string]*commonproto.DataServerMetadata)
	Namespace(namespace string) (*commonproto.Namespace, bool)
	Node(id string) (*commonproto.DataServer, bool)
	GetDataServerInfo(name string) (*commonproto.DataServerInfo, bool)
}

type EnsembleSupplier func(namespaceConfig *commonproto.Namespace, status *model.ClusterStatus) ([]model.Server, error)

type coordinatorMetadata struct {
	*slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     *sync.WaitGroup

	metadataProvider provider.Provider

	statusLock       sync.RWMutex
	currentStatus    *model.ClusterStatus
	currentVersionID provider.Version
	changeCh         chan struct{}

	clusterConfigProvider        func() (*commonproto.ClusterConfiguration, error)
	clusterConfigNotificationsCh chan any

	clusterConfigLock     sync.RWMutex
	currentClusterConfig  *commonproto.ClusterConfiguration
	clusterConfigWatch    *commonoption.Watch[*commonproto.ClusterConfiguration]
	nodesIndex            *redblacktree.Tree[string, *commonproto.DataServer]
	namespaceConfigsIndex *redblacktree.Tree[string, *commonproto.Namespace]
}

func New(
	ctx context.Context,
	metadataProvider provider.Provider,
	clusterConfigProvider func() (*commonproto.ClusterConfiguration, error),
	clusterConfigNotificationsCh chan any,
) Metadata {
	metadataCtx, cancel := context.WithCancel(ctx)
	m := &coordinatorMetadata{
		Logger:                       slog.With(slog.String("component", "coordinator-metadata")),
		ctx:                          metadataCtx,
		cancel:                       cancel,
		wg:                           &sync.WaitGroup{},
		metadataProvider:             metadataProvider,
		currentVersionID:             provider.NotExists,
		changeCh:                     make(chan struct{}),
		clusterConfigProvider:        clusterConfigProvider,
		clusterConfigNotificationsCh: clusterConfigNotificationsCh,
		clusterConfigWatch:           commonoption.NewWatch[*commonproto.ClusterConfiguration](nil),
	}

	m.doStatusRecovery()

	if clusterConfigNotificationsCh != nil {
		m.wg.Go(func() {
			process.DoWithLabels(metadataCtx, map[string]string{
				"component": "coordinator-metadata-config-watcher",
			}, m.waitForConfigUpdates)
		})
	}

	return m
}

func (m *coordinatorMetadata) Close() error {
	m.cancel()
	m.wg.Wait()
	return m.metadataProvider.Close()
}

func (m *coordinatorMetadata) notifyStatusChange() {
	close(m.changeCh)
	m.changeCh = make(chan struct{})
}

func (m *coordinatorMetadata) doStatusRecovery() {
	m.statusLock.Lock()
	defer m.statusLock.Unlock()

	_ = backoff.RetryNotify(func() error {
		clusterStatus, version, err := m.metadataProvider.Get()
		if err != nil {
			return err
		}
		m.currentStatus = clusterStatus
		m.currentVersionID = version
		return nil
	}, backoff.NewExponentialBackOff(), func(err error, duration time.Duration) {
		m.Warn(
			"failed to load status, retrying later",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})

	if m.currentStatus == nil {
		m.currentStatus = model.NewClusterStatus()
	}
	if m.currentStatus.InstanceId == "" {
		clonedStatus := m.currentStatus.Clone()
		clonedStatus.InstanceId = uuid.NewString()
		m.persistStatusLocked(clonedStatus, "failed to initialize instance id")
		m.notifyStatusChange()
	}
}

func (m *coordinatorMetadata) persistStatusLocked(newStatus *model.ClusterStatus, warnMessage string) {
	_ = backoff.RetryNotify(func() error {
		versionID, err := m.metadataProvider.Store(newStatus, m.currentVersionID)
		if err != nil {
			return err
		}
		m.currentStatus = newStatus
		m.currentVersionID = versionID
		return nil
	}, backoff.NewExponentialBackOff(), func(err error, duration time.Duration) {
		m.Warn(
			warnMessage,
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
}

func (m *coordinatorMetadata) LoadStatus() *model.ClusterStatus {
	m.statusLock.RLock()
	defer m.statusLock.RUnlock()
	return m.currentStatus
}

func (m *coordinatorMetadata) ApplyStatusChanges(config *commonproto.ClusterConfiguration, ensembleSupplier EnsembleSupplier) (*model.ClusterStatus, map[int64]string, []int64) {
	m.statusLock.Lock()
	defer m.statusLock.Unlock()

	newStatus := m.currentStatus.Clone()
	shardsToAdd, shardsToDelete := util.ApplyClusterChanges(config, newStatus, ensembleSupplier)
	if len(shardsToAdd) == 0 && len(shardsToDelete) == 0 {
		return newStatus, shardsToAdd, shardsToDelete
	}

	m.persistStatusLocked(newStatus, "failed to apply cluster changes")
	m.notifyStatusChange()
	return newStatus, shardsToAdd, shardsToDelete
}

func (m *coordinatorMetadata) UpdateStatus(newStatus *model.ClusterStatus) {
	m.statusLock.Lock()
	defer m.statusLock.Unlock()

	m.persistStatusLocked(newStatus, "failed to update status")
	m.notifyStatusChange()
}

func (m *coordinatorMetadata) UpdateShardMetadata(namespace string, shard int64, shardMetadata model.ShardMetadata) {
	m.statusLock.Lock()
	defer m.statusLock.Unlock()

	clonedStatus := m.currentStatus.Clone()
	ns, exist := clonedStatus.Namespaces[namespace]
	if !exist {
		return
	}
	ns.Shards[shard] = shardMetadata.Clone()
	m.persistStatusLocked(clonedStatus, "failed to update shard metadata")
	m.notifyStatusChange()
}

func (m *coordinatorMetadata) DeleteShardMetadata(namespace string, shard int64) {
	m.statusLock.Lock()
	defer m.statusLock.Unlock()

	clonedStatus := m.currentStatus.Clone()
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

func (m *coordinatorMetadata) IsReady(clusterConfig *commonproto.ClusterConfiguration) bool {
	currentStatus := m.LoadStatus()
	for _, namespace := range clusterConfig.Namespaces {
		namespaceStatus, ok := currentStatus.Namespaces[namespace.Name]
		if !ok {
			return false
		}
		if len(namespaceStatus.Shards) != int(namespace.InitialShardCount) {
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

func (m *coordinatorMetadata) StatusChangeNotify() <-chan struct{} {
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
		newConfig, err := m.clusterConfigProvider()
		if err != nil {
			return err
		}
		m.currentClusterConfig = gproto.Clone(newConfig).(*commonproto.ClusterConfiguration)
		m.rebuildConfigIndexesLocked()
		watchedConfig, _ := m.clusterConfigWatch.Load()
		if watchedConfig == nil {
			m.clusterConfigWatch.Notify(m.currentClusterConfig)
		}
		return nil
	}, backoff.NewExponentialBackOff(), func(err error, duration time.Duration) {
		m.Warn(
			"failed to load cluster configuration, retrying later",
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
}

func (m *coordinatorMetadata) rebuildConfigIndexesLocked() {
	nodes := redblacktree.New[string, *commonproto.DataServer]()
	for _, server := range m.currentClusterConfig.GetServers() {
		nodes.Put(server.GetNameOrDefault(), server)
	}
	m.nodesIndex = nodes

	namespaceConfigs := redblacktree.New[string, *commonproto.Namespace]()
	for _, ns := range m.currentClusterConfig.GetNamespaces() {
		namespaceConfigs.Put(ns.GetName(), ns)
	}
	m.namespaceConfigsIndex = namespaceConfigs
}

func (m *coordinatorMetadata) waitForConfigUpdates() {
	for {
		select {
		case <-m.ctx.Done():
			return

		case <-m.clusterConfigNotificationsCh:
			m.Info("Received cluster config change event")

			m.clusterConfigLock.Lock()
			oldClusterConfig := m.currentClusterConfig
			m.currentClusterConfig = nil
			m.nodesIndex = nil
			m.namespaceConfigsIndex = nil
			m.clusterConfigLock.Unlock()

			m.loadClusterConfigWithInitSlow()

			m.clusterConfigLock.RLock()
			currentClusterConfig := m.currentClusterConfig
			m.clusterConfigLock.RUnlock()

			if gproto.Equal(oldClusterConfig, currentClusterConfig) {
				m.Info("No cluster config changes detected")
				continue
			}
			m.clusterConfigWatch.Notify(currentClusterConfig)
		}
	}
}

func (m *coordinatorMetadata) LoadConfig() *commonproto.ClusterConfiguration {
	m.clusterConfigLock.RLock()
	defer m.clusterConfigLock.RUnlock()
	if m.currentClusterConfig == nil {
		m.clusterConfigLock.RUnlock()
		m.loadClusterConfigWithInitSlow()
		m.clusterConfigLock.RLock()
	}
	return m.currentClusterConfig
}

func (m *coordinatorMetadata) ConfigWatch() *commonoption.Watch[*commonproto.ClusterConfiguration] {
	return m.clusterConfigWatch
}

func (m *coordinatorMetadata) LoadLoadBalancer() *commonproto.LoadBalancer {
	return m.LoadConfig().GetLoadBalancerWithDefaults()
}

func (m *coordinatorMetadata) Nodes() *linkedhashset.Set[string] {
	m.clusterConfigLock.RLock()
	defer m.clusterConfigLock.RUnlock()
	if m.currentClusterConfig == nil {
		m.clusterConfigLock.RUnlock()
		m.loadClusterConfigWithInitSlow()
		m.clusterConfigLock.RLock()
	}

	nodes := linkedhashset.New[string]()
	for _, server := range m.currentClusterConfig.GetServers() {
		nodes.Add(server.GetNameOrDefault())
	}
	return nodes
}

func (m *coordinatorMetadata) NodesWithMetadata() (*linkedhashset.Set[string], map[string]*commonproto.DataServerMetadata) {
	m.clusterConfigLock.RLock()
	defer m.clusterConfigLock.RUnlock()
	if m.currentClusterConfig == nil {
		m.clusterConfigLock.RUnlock()
		m.loadClusterConfigWithInitSlow()
		m.clusterConfigLock.RLock()
	}

	nodes := linkedhashset.New[string]()
	for _, server := range m.currentClusterConfig.GetServers() {
		nodes.Add(server.GetNameOrDefault())
	}

	metadata := make(map[string]*commonproto.DataServerMetadata, len(m.currentClusterConfig.GetServerMetadata()))
	for id, value := range m.currentClusterConfig.GetServerMetadata() {
		metadata[id] = value
	}
	return nodes, metadata
}

func (m *coordinatorMetadata) Namespace(namespace string) (*commonproto.Namespace, bool) {
	m.clusterConfigLock.RLock()
	defer m.clusterConfigLock.RUnlock()
	if m.currentClusterConfig == nil {
		m.clusterConfigLock.RUnlock()
		m.loadClusterConfigWithInitSlow()
		m.clusterConfigLock.RLock()
	}
	return m.namespaceConfigsIndex.Get(namespace)
}

func (m *coordinatorMetadata) Node(id string) (*commonproto.DataServer, bool) {
	m.clusterConfigLock.RLock()
	defer m.clusterConfigLock.RUnlock()
	if m.currentClusterConfig == nil {
		m.clusterConfigLock.RUnlock()
		m.loadClusterConfigWithInitSlow()
		m.clusterConfigLock.RLock()
	}
	return m.nodesIndex.Get(id)
}

func (m *coordinatorMetadata) GetDataServerInfo(id string) (*commonproto.DataServerInfo, bool) {
	m.clusterConfigLock.RLock()
	defer m.clusterConfigLock.RUnlock()
	if m.currentClusterConfig == nil {
		m.clusterConfigLock.RUnlock()
		m.loadClusterConfigWithInitSlow()
		m.clusterConfigLock.RLock()
	}

	return m.currentClusterConfig.GetDataServerInfo(id)
}

func WaitForCondition(ctx context.Context, metadata Metadata, triggerFn func(), condition func(*model.ClusterStatus) bool) error {
	for {
		ch := metadata.StatusChangeNotify()
		if condition(metadata.LoadStatus()) {
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
