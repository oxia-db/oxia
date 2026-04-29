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

	"github.com/cenkalti/backoff/v4"
	"github.com/emirpasic/gods/v2/sets/linkedhashset"
	"github.com/emirpasic/gods/v2/trees/redblacktree"
	"github.com/google/uuid"
	gproto "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/process"
	commonproto "github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/rpc"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/changes"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
)

type Metadata interface {
	io.Closer

	GetStatus() *commonproto.ClusterStatus
	PutStatus(newStatus *commonproto.ClusterStatus)
	ApplyStatusChanges(config *commonproto.ClusterConfiguration, ensembleSupplier EnsembleSupplier) (*commonproto.ClusterStatus, map[int64]string, []int64)

	PutShard(namespace string, shard int64, shardMetadata *commonproto.ShardMetadata)
	DeleteShard(namespace string, shard int64)

	GetConfig() *commonproto.ClusterConfiguration
	ConfigWatch() *commonwatch.Watch[*commonproto.ClusterConfiguration]
	GetLoadBalancer() *commonproto.LoadBalancer

	ListDataServers() *linkedhashset.Set[string]
	ListDataServersWithMetadata() (*linkedhashset.Set[string], map[string]*commonproto.DataServerMetadata)
	GetDataServerIdentity(id string) (*commonproto.DataServerIdentity, bool)
	GetDataServer(name string) (*commonproto.DataServer, bool)
	GetNamespace(namespace string) (*commonproto.Namespace, bool)
}

type EnsembleSupplier func(namespaceConfig *commonproto.Namespace, status *commonproto.ClusterStatus) ([]*commonproto.DataServerIdentity, error)

type coordinatorMetadata struct {
	logger    *slog.Logger
	ctx       context.Context
	ctxCancel context.CancelFunc
	wg        sync.WaitGroup

	statusProvider provider.Provider[*commonproto.ClusterStatus]
	configProvider provider.Provider[*commonproto.ClusterConfiguration]

	statusLock       sync.RWMutex
	currentStatus    *commonproto.ClusterStatus
	currentVersionID provider.Version
	changeCh         chan struct{}

	clusterConfigLock     sync.RWMutex
	currentClusterConfig  *commonproto.ClusterConfiguration
	clusterConfigWatch    *commonwatch.Watch[*commonproto.ClusterConfiguration]
	nodesIndex            *redblacktree.Tree[string, *commonproto.DataServerIdentity]
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
		currentVersionID:   provider.NotExists,
		changeCh:           make(chan struct{}),
		clusterConfigWatch: commonwatch.New(&commonproto.ClusterConfiguration{}),
	}

	m.doStatusRecovery()

	if configWatch, err := configProvider.Watch(); err != nil && !errors.Is(err, provider.ErrWatchUnsupported) {
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

type callbackConfigProvider struct {
	ctx           context.Context
	ctxCancel     context.CancelFunc
	wg            sync.WaitGroup
	load          func() (*commonproto.ClusterConfiguration, error)
	notifications <-chan any
	watcher       *commonwatch.Watch[*commonproto.ClusterConfiguration]
}

func newCallbackConfigProvider(
	ctx context.Context,
	load func() (*commonproto.ClusterConfiguration, error),
	notifications <-chan any,
) provider.Provider[*commonproto.ClusterConfiguration] {
	watchCtx, cancel := context.WithCancel(ctx)
	p := &callbackConfigProvider{
		ctx:           watchCtx,
		ctxCancel:     cancel,
		load:          load,
		notifications: notifications,
	}
	if notifications != nil {
		initialValue, _, err := p.Get()
		if err != nil {
			slog.Warn("Failed to load initial watched callback config provider metadata", slog.Any("error", err))
		}
		p.watcher = commonwatch.New(initialValue)
		p.wg.Go(func() {
			process.DoWithLabels(p.ctx, map[string]string{
				"component": "callback-config-provider-watch",
			}, p.watchLoop)
		})
	}
	return p
}

func (p *callbackConfigProvider) Get() (*commonproto.ClusterConfiguration, provider.Version, error) {
	config, err := p.LoadConfig()
	return config, provider.NotExists, err
}

func (p *callbackConfigProvider) LoadConfig() (*commonproto.ClusterConfiguration, error) {
	if p.load == nil {
		return nil, provider.ErrNotInitialized
	}
	return p.load()
}

func (*callbackConfigProvider) Store(*commonproto.ClusterConfiguration, provider.Version) (provider.Version, error) {
	return provider.NotExists, errors.New("callback config provider is read-only")
}

func (*callbackConfigProvider) WaitToBecomeLeader() error {
	return nil
}

func (p *callbackConfigProvider) Watch() (*commonwatch.Receiver[*commonproto.ClusterConfiguration], error) {
	if p.watcher == nil {
		return nil, provider.ErrWatchUnsupported
	}
	return p.watcher.Subscribe(), nil
}

func (p *callbackConfigProvider) watchLoop() {
	for {
		select {
		case <-p.ctx.Done():
			return
		case _, ok := <-p.notifications:
			if !ok {
				return
			}
			p.publishCurrentValue()
		}
	}
}

func (p *callbackConfigProvider) publishCurrentValue() {
	value, _, err := p.Get()
	if err != nil {
		slog.Warn("Failed to load watched callback config provider metadata", slog.Any("error", err))
		return
	}
	p.watcher.Publish(value)
}

func (p *callbackConfigProvider) Close() error {
	p.ctxCancel()
	p.wg.Wait()
	return nil
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

func (m *coordinatorMetadata) GetStatus() *commonproto.ClusterStatus {
	m.statusLock.RLock()
	defer m.statusLock.RUnlock()
	return m.currentStatus
}

func (m *coordinatorMetadata) ApplyStatusChanges(config *commonproto.ClusterConfiguration, ensembleSupplier EnsembleSupplier) (*commonproto.ClusterStatus, map[int64]string, []int64) {
	m.statusLock.Lock()
	defer m.statusLock.Unlock()

	newStatus := gproto.Clone(m.currentStatus).(*commonproto.ClusterStatus) //nolint:revive
	shardsToAdd, shardsToDelete := changes.ApplyClusterChanges(config, newStatus, ensembleSupplier)
	if len(shardsToAdd) == 0 && len(shardsToDelete) == 0 {
		return newStatus, shardsToAdd, shardsToDelete
	}

	m.persistStatusLocked(newStatus, "failed to apply cluster changes")
	m.notifyStatusChange()
	return newStatus, shardsToAdd, shardsToDelete
}

func (m *coordinatorMetadata) PutStatus(newStatus *commonproto.ClusterStatus) {
	m.statusLock.Lock()
	defer m.statusLock.Unlock()

	m.persistStatusLocked(newStatus, "failed to update status")
	m.notifyStatusChange()
}

func (m *coordinatorMetadata) PutShard(namespace string, shard int64, shardMetadata *commonproto.ShardMetadata) {
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

func (m *coordinatorMetadata) DeleteShard(namespace string, shard int64) {
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
	if loader, ok := configProvider.(interface {
		LoadConfig() (*commonproto.ClusterConfiguration, error)
	}); ok {
		return loader.LoadConfig()
	}

	config, _, err := configProvider.Get()
	if err != nil {
		return nil, err
	}
	if config == nil {
		return nil, provider.ErrNotInitialized
	}
	if err := validateClusterConfig(config); err != nil {
		return nil, err
	}
	return config, nil
}

func validateClusterConfig(config *commonproto.ClusterConfiguration) error {
	if config == nil {
		return provider.ErrNotInitialized
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
	nodes := redblacktree.New[string, *commonproto.DataServerIdentity]()
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
	if !m.usesCallbackConfigProvider() {
		if err := validateClusterConfig(config); err != nil {
			m.logger.Warn("received invalid cluster config watch value", slog.Any("error", err))
			return
		}
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

func (m *coordinatorMetadata) usesCallbackConfigProvider() bool {
	_, ok := m.configProvider.(*callbackConfigProvider)
	return ok
}

func (m *coordinatorMetadata) GetConfig() *commonproto.ClusterConfiguration {
	m.clusterConfigLock.RLock()
	defer m.clusterConfigLock.RUnlock()
	if m.currentClusterConfig == nil {
		m.clusterConfigLock.RUnlock()
		m.loadClusterConfigWithInitSlow()
		m.clusterConfigLock.RLock()
	}
	return m.currentClusterConfig
}

func (m *coordinatorMetadata) ConfigWatch() *commonwatch.Watch[*commonproto.ClusterConfiguration] {
	return m.clusterConfigWatch
}

func (m *coordinatorMetadata) GetLoadBalancer() *commonproto.LoadBalancer {
	return m.GetConfig().GetLoadBalancerWithDefaults()
}

func (m *coordinatorMetadata) ListDataServers() *linkedhashset.Set[string] {
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

func (m *coordinatorMetadata) ListDataServersWithMetadata() (*linkedhashset.Set[string], map[string]*commonproto.DataServerMetadata) {
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

func (m *coordinatorMetadata) GetNamespace(namespace string) (*commonproto.Namespace, bool) {
	m.clusterConfigLock.RLock()
	defer m.clusterConfigLock.RUnlock()
	if m.currentClusterConfig == nil {
		m.clusterConfigLock.RUnlock()
		m.loadClusterConfigWithInitSlow()
		m.clusterConfigLock.RLock()
	}
	return m.namespaceConfigsIndex.Get(namespace)
}

func (m *coordinatorMetadata) GetDataServerIdentity(id string) (*commonproto.DataServerIdentity, bool) {
	m.clusterConfigLock.RLock()
	defer m.clusterConfigLock.RUnlock()
	if m.currentClusterConfig == nil {
		m.clusterConfigLock.RUnlock()
		m.loadClusterConfigWithInitSlow()
		m.clusterConfigLock.RLock()
	}
	return m.nodesIndex.Get(id)
}

func (m *coordinatorMetadata) GetDataServer(id string) (*commonproto.DataServer, bool) {
	m.clusterConfigLock.RLock()
	defer m.clusterConfigLock.RUnlock()
	if m.currentClusterConfig == nil {
		m.clusterConfigLock.RUnlock()
		m.loadClusterConfigWithInitSlow()
		m.clusterConfigLock.RLock()
	}

	return m.currentClusterConfig.GetDataServer(id)
}

func WaitForCondition(ctx context.Context, metadata Metadata, triggerFn func(), condition func(*commonproto.ClusterStatus) bool) error {
	notifier, ok := metadata.(interface{ statusChangeNotify() <-chan struct{} })
	if !ok {
		return errors.New("metadata does not support status change notifications")
	}
	for {
		ch := notifier.statusChangeNotify()
		if condition(metadata.GetStatus()) {
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
