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
	"reflect"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/emirpasic/gods/v2/sets/linkedhashset"
	"github.com/emirpasic/gods/v2/trees/redblacktree"
	"github.com/google/uuid"

	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/resource"
	"github.com/oxia-db/oxia/oxiad/coordinator/util"
)

const (
	defaultLoadBalancerScheduleInterval = 30 * time.Second
	defaultQuarantineTime               = 5 * time.Minute
)

type Metadata interface {
	io.Closer

	LoadStatus() *model.ClusterStatus
	ApplyStatusChanges(config *model.ClusterConfig, ensembleSupplier resource.EnsembleSupplier) (*model.ClusterStatus, map[int64]string, []int64)
	UpdateStatus(newStatus *model.ClusterStatus)
	UpdateShardMetadata(namespace string, shard int64, shardMetadata model.ShardMetadata)
	DeleteShardMetadata(namespace string, shard int64)
	IsReady(clusterConfig *model.ClusterConfig) bool
	StatusChangeNotify() <-chan struct{}

	LoadConfig() *model.ClusterConfig
	LoadLoadBalancer() *model.LoadBalancer
	Nodes() *linkedhashset.Set[string]
	NodesWithMetadata() (*linkedhashset.Set[string], map[string]model.ServerMetadata)
	NamespaceConfig(namespace string) (*model.NamespaceConfig, bool)
	Node(id string) (*model.Server, bool)
	GetDataServerInfo(name string) (*model.DataServerInfo, bool)
}

type coordinatorMetadata struct {
	*slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     *sync.WaitGroup

	statusProvider provider.Provider

	statusLock       sync.RWMutex
	currentStatus    *model.ClusterStatus
	currentVersionID provider.Version
	changeCh         chan struct{}

	clusterConfigProvider        func() (model.ClusterConfig, error)
	clusterConfigNotificationsCh chan any
	clusterConfigEventListener   resource.ClusterConfigEventListener

	clusterConfigLock     sync.RWMutex
	currentClusterConfig  *model.ClusterConfig
	nodesIndex            *redblacktree.Tree[string, *model.Server]
	namespaceConfigsIndex *redblacktree.Tree[string, *model.NamespaceConfig]
}

func New(
	ctx context.Context,
	statusProvider provider.Provider,
	clusterConfigProvider func() (model.ClusterConfig, error),
	clusterConfigNotificationsCh chan any,
	clusterConfigEventListener resource.ClusterConfigEventListener,
) Metadata {
	metadataCtx, cancel := context.WithCancel(ctx)
	m := &coordinatorMetadata{
		Logger:                       slog.With(slog.String("component", "coordinator-metadata")),
		ctx:                          metadataCtx,
		cancel:                       cancel,
		wg:                           &sync.WaitGroup{},
		statusProvider:               statusProvider,
		currentVersionID:             provider.NotExists,
		changeCh:                     make(chan struct{}),
		clusterConfigProvider:        clusterConfigProvider,
		clusterConfigNotificationsCh: clusterConfigNotificationsCh,
		clusterConfigEventListener:   clusterConfigEventListener,
	}

	m.doStatusRecovery()

	if clusterConfigNotificationsCh != nil {
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			process.DoWithLabels(metadataCtx, map[string]string{
				"component": "coordinator-metadata-config-watcher",
			}, m.waitForConfigUpdates)
		}()
	}

	return m
}

func (m *coordinatorMetadata) Close() error {
	m.cancel()
	m.wg.Wait()
	return m.statusProvider.Close()
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
		versionID, err := m.statusProvider.Store(newStatus, m.currentVersionID)
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

func (m *coordinatorMetadata) ApplyStatusChanges(config *model.ClusterConfig, ensembleSupplier resource.EnsembleSupplier) (*model.ClusterStatus, map[int64]string, []int64) {
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

func (m *coordinatorMetadata) IsReady(clusterConfig *model.ClusterConfig) bool {
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
		m.currentClusterConfig = &newConfig
		m.rebuildConfigIndexesLocked()
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
	nodes := redblacktree.New[string, *model.Server]()
	for idx := range m.currentClusterConfig.Servers {
		server := &m.currentClusterConfig.Servers[idx]
		nodes.Put(server.GetIdentifier(), server)
	}
	m.nodesIndex = nodes

	namespaceConfigs := redblacktree.New[string, *model.NamespaceConfig]()
	for idx := range m.currentClusterConfig.Namespaces {
		ns := &m.currentClusterConfig.Namespaces[idx]
		namespaceConfigs.Put(ns.Name, ns)
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

			if reflect.DeepEqual(oldClusterConfig, currentClusterConfig) {
				m.Info("No cluster config changes detected")
				continue
			}
			if m.clusterConfigEventListener != nil {
				m.clusterConfigEventListener.ConfigChanged(currentClusterConfig)
			}
		}
	}
}

func (m *coordinatorMetadata) LoadConfig() *model.ClusterConfig {
	m.clusterConfigLock.RLock()
	defer m.clusterConfigLock.RUnlock()
	if m.currentClusterConfig == nil {
		m.clusterConfigLock.RUnlock()
		m.loadClusterConfigWithInitSlow()
		m.clusterConfigLock.RLock()
	}
	return m.currentClusterConfig
}

func (m *coordinatorMetadata) LoadLoadBalancer() *model.LoadBalancer {
	config := m.LoadConfig()
	if config.LoadBalancer == nil {
		return &model.LoadBalancer{
			ScheduleInterval: defaultLoadBalancerScheduleInterval,
			QuarantineTime:   defaultQuarantineTime,
		}
	}

	loadBalancer := *config.LoadBalancer
	if loadBalancer.ScheduleInterval == 0 {
		loadBalancer.ScheduleInterval = defaultLoadBalancerScheduleInterval
	}
	if loadBalancer.QuarantineTime == 0 {
		loadBalancer.QuarantineTime = defaultQuarantineTime
	}
	return &loadBalancer
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
	for idx := range m.currentClusterConfig.Servers {
		nodes.Add(m.currentClusterConfig.Servers[idx].GetIdentifier())
	}
	return nodes
}

func (m *coordinatorMetadata) NodesWithMetadata() (*linkedhashset.Set[string], map[string]model.ServerMetadata) {
	m.clusterConfigLock.RLock()
	defer m.clusterConfigLock.RUnlock()
	if m.currentClusterConfig == nil {
		m.clusterConfigLock.RUnlock()
		m.loadClusterConfigWithInitSlow()
		m.clusterConfigLock.RLock()
	}

	nodes := linkedhashset.New[string]()
	for idx := range m.currentClusterConfig.Servers {
		nodes.Add(m.currentClusterConfig.Servers[idx].GetIdentifier())
	}
	return nodes, m.currentClusterConfig.ServerMetadata
}

func (m *coordinatorMetadata) NamespaceConfig(namespace string) (*model.NamespaceConfig, bool) {
	m.clusterConfigLock.RLock()
	defer m.clusterConfigLock.RUnlock()
	if m.currentClusterConfig == nil {
		m.clusterConfigLock.RUnlock()
		m.loadClusterConfigWithInitSlow()
		m.clusterConfigLock.RLock()
	}
	return m.namespaceConfigsIndex.Get(namespace)
}

func (m *coordinatorMetadata) Node(id string) (*model.Server, bool) {
	m.clusterConfigLock.RLock()
	defer m.clusterConfigLock.RUnlock()
	if m.currentClusterConfig == nil {
		m.clusterConfigLock.RUnlock()
		m.loadClusterConfigWithInitSlow()
		m.clusterConfigLock.RLock()
	}
	return m.nodesIndex.Get(id)
}

func (m *coordinatorMetadata) GetDataServerInfo(id string) (*model.DataServerInfo, bool) {
	m.clusterConfigLock.RLock()
	defer m.clusterConfigLock.RUnlock()
	if m.currentClusterConfig == nil {
		m.clusterConfigLock.RUnlock()
		m.loadClusterConfigWithInitSlow()
		m.clusterConfigLock.RLock()
	}

	node, found := m.nodesIndex.Get(id)
	if !found {
		return nil, false
	}

	info := &model.DataServerInfo{
		Server:   node,
		Metadata: model.ServerMetadata{},
	}
	if metadata, found := m.currentClusterConfig.ServerMetadata[id]; found {
		info.Metadata = metadata
	}
	return info, true
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
