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

	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/common/process"
	commonproto "github.com/oxia-db/oxia/common/proto"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
	"github.com/oxia-db/oxia/oxiad/common/rpc"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/file"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/kubernetes"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/raft"
	"github.com/oxia-db/oxia/oxiad/coordinator/option"
	"github.com/oxia-db/oxia/oxiad/coordinator/util"
)

type Metadata interface {
	io.Closer

	LoadStatus() *commonproto.ClusterStatus
	ApplyStatusChanges(config *commonproto.ClusterConfiguration, ensembleSupplier EnsembleSupplier) (*commonproto.ClusterStatus, map[int64]string, []int64)
	UpdateStatus(newStatus *commonproto.ClusterStatus)
	UpdateShardMetadata(namespace string, shard int64, shardMetadata *commonproto.ShardMetadata)
	DeleteShardMetadata(namespace string, shard int64)
	IsReady(clusterConfig *commonproto.ClusterConfiguration) bool
	StatusChangeNotify() <-chan struct{}

	LoadConfig() *commonproto.ClusterConfiguration
	ConfigWatch() *commonoption.Watch[*commonproto.ClusterConfiguration]
	LoadLoadBalancer() *commonproto.LoadBalancer
	Nodes() *linkedhashset.Set[string]
	NodesWithMetadata() (*linkedhashset.Set[string], map[string]*commonproto.DataServerMetadata)
	Namespace(namespace string) (*commonproto.Namespace, bool)
	Node(id string) (*commonproto.DataServerIdentity, bool)
	GetDataServer(name string) (*commonproto.DataServer, bool)
}

type EnsembleSupplier func(namespaceConfig *commonproto.Namespace, status *commonproto.ClusterStatus) ([]*commonproto.DataServerIdentity, error)

type coordinatorMetadata struct {
	*slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     *sync.WaitGroup

	statusProvider provider.Provider
	configProvider provider.Provider

	statusLock       sync.RWMutex
	currentStatus    *commonproto.ClusterStatus
	currentVersionID provider.Version
	changeCh         chan struct{}

	clusterConfigLock     sync.RWMutex
	currentClusterConfig  *commonproto.ClusterConfiguration
	clusterConfigWatch    *commonoption.Watch[*commonproto.ClusterConfiguration]
	nodesIndex            *redblacktree.Tree[string, *commonproto.DataServerIdentity]
	namespaceConfigsIndex *redblacktree.Tree[string, *commonproto.Namespace]
}

func New(
	ctx context.Context,
	metadataProvider provider.Provider,
	clusterConfigProvider func() (*commonproto.ClusterConfiguration, error),
	clusterConfigNotificationsCh chan any,
) Metadata {
	return newCoordinatorMetadata(ctx, metadataProvider, newCallbackConfigProvider(ctx, clusterConfigProvider, clusterConfigNotificationsCh))
}

func NewWithProviders(ctx context.Context, statusProvider provider.Provider, configProvider provider.Provider) Metadata {
	return newCoordinatorMetadata(ctx, statusProvider, configProvider)
}

func NewFromOptions(ctx context.Context, meta option.MetadataOptions, legacyClusterConfigPath string) (Metadata, error) {
	if err := meta.ApplyLegacyClusterConfigPath(legacyClusterConfigPath); err != nil {
		return nil, err
	}

	statusProvider, configProvider, err := newProviders(meta)
	if err != nil {
		return nil, err
	}

	slog.Info("Waiting to become leader", slog.String("component", "coordinator"))
	if err := statusProvider.WaitToBecomeLeader(); err != nil {
		_ = multierr.Combine(statusProvider.Close(), configProvider.Close())
		return nil, fmt.Errorf("failed to wait in becoming leader: %w", err)
	}
	slog.Info("This coordinator is now leader", slog.String("component", "coordinator"))

	return NewWithProviders(ctx, statusProvider, configProvider), nil
}

func newProviders(meta option.MetadataOptions) (statusProvider provider.Provider, configProvider provider.Provider, err error) {
	switch meta.ProviderName {
	case provider.NameMemory:
		return memory.NewProvider(provider.ResourceStatus), memory.NewProvider(provider.ResourceConfig), nil
	case provider.NameFile:
		return file.NewProvider(meta.File.StatusPath(), provider.ResourceStatus, false),
			file.NewProvider(meta.File.ConfigPath(), provider.ResourceConfig, true), nil
	case provider.NameConfigMap:
		k8sConfig := kubernetes.NewK8SClientConfig()
		client := kubernetes.NewK8SClientset(k8sConfig)
		statusProvider := kubernetes.NewConfigMapProvider(client, meta.Kubernetes.Namespace, meta.Kubernetes.StatusNameOrDefault(), provider.ResourceStatus, false)
		if meta.File.Dir != "" || meta.File.ConfigName != "" {
			return statusProvider, file.NewProvider(meta.File.ConfigPath(), provider.ResourceConfig, true), nil
		}
		configProvider := kubernetes.NewConfigMapProvider(client, meta.Kubernetes.Namespace, meta.Kubernetes.ConfigNameOrDefault(), provider.ResourceConfig, true)
		return statusProvider, configProvider, nil
	case provider.NameRaft:
		statusProvider, configProvider, err := raft.NewProviders(meta.Raft.Address, meta.Raft.BootstrapNodes, meta.Raft.DataDir)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create raft metadata provider: %w", err)
		}
		return statusProvider, configProvider, nil
	default:
		return nil, nil, errors.New(`must be one of "memory", "configmap", "raft" or "file"`)
	}
}

func newCoordinatorMetadata(ctx context.Context, statusProvider provider.Provider, configProvider provider.Provider) Metadata {
	metadataCtx, cancel := context.WithCancel(ctx)
	m := &coordinatorMetadata{
		Logger:             slog.With(slog.String("component", "coordinator-metadata")),
		ctx:                metadataCtx,
		cancel:             cancel,
		wg:                 &sync.WaitGroup{},
		statusProvider:     statusProvider,
		configProvider:     configProvider,
		currentVersionID:   provider.NotExists,
		changeCh:           make(chan struct{}),
		clusterConfigWatch: commonoption.NewWatch[*commonproto.ClusterConfiguration](nil),
	}

	m.doStatusRecovery()

	if configWatch, err := configProvider.Watch(); err != nil && !errors.Is(err, provider.ErrWatchUnsupported) {
		m.Warn("failed to watch cluster config provider", slog.Any("error", err))
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
	cancel        context.CancelFunc
	load          func() (*commonproto.ClusterConfiguration, error)
	notifications <-chan any
}

func newCallbackConfigProvider(
	ctx context.Context,
	load func() (*commonproto.ClusterConfiguration, error),
	notifications <-chan any,
) provider.Provider {
	watchCtx, cancel := context.WithCancel(ctx)
	return &callbackConfigProvider{
		ctx:           watchCtx,
		cancel:        cancel,
		load:          load,
		notifications: notifications,
	}
}

func (p *callbackConfigProvider) Get() (gproto.Message, provider.Version, error) {
	config, err := p.LoadConfig()
	return config, provider.NotExists, err
}

func (p *callbackConfigProvider) LoadConfig() (*commonproto.ClusterConfiguration, error) {
	if p.load == nil {
		return nil, provider.ErrNotInitialized
	}
	return p.load()
}

func (*callbackConfigProvider) Store(gproto.Message, provider.Version) (provider.Version, error) {
	return provider.NotExists, errors.New("callback config provider is read-only")
}

func (*callbackConfigProvider) WaitToBecomeLeader() error {
	return nil
}

func (p *callbackConfigProvider) Watch() (<-chan struct{}, error) {
	if p.notifications == nil {
		return nil, provider.ErrWatchUnsupported
	}

	ch := make(chan struct{}, 1)
	go process.DoWithLabels(p.ctx, map[string]string{
		"component": "callback-config-provider-watch",
	}, func() {
		defer close(ch)
		for {
			select {
			case <-p.ctx.Done():
				return
			case _, ok := <-p.notifications:
				if !ok {
					return
				}
				select {
				case ch <- struct{}{}:
				default:
				}
			}
		}
	})
	return ch, nil
}

func (p *callbackConfigProvider) Close() error {
	p.cancel()
	return nil
}

func (m *coordinatorMetadata) Close() error {
	m.cancel()
	m.wg.Wait()
	return multierr.Combine(m.statusProvider.Close(), m.configProvider.Close())
}

func (m *coordinatorMetadata) notifyStatusChange() {
	close(m.changeCh)
	m.changeCh = make(chan struct{})
}

func (m *coordinatorMetadata) doStatusRecovery() {
	m.statusLock.Lock()
	defer m.statusLock.Unlock()

	_ = backoff.RetryNotify(func() error {
		value, version, err := m.statusProvider.Get()
		if err != nil {
			return err
		}
		if value == nil {
			m.currentStatus = nil
			m.currentVersionID = version
			return nil
		}
		clusterStatus, ok := value.(*commonproto.ClusterStatus)
		if !ok {
			return fmt.Errorf("expected *ClusterStatus from status provider, got %T", value)
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
		m.Warn(
			warnMessage,
			slog.Any("error", err),
			slog.Duration("retry-after", duration),
		)
	})
}

func (m *coordinatorMetadata) LoadStatus() *commonproto.ClusterStatus {
	m.statusLock.RLock()
	defer m.statusLock.RUnlock()
	return m.currentStatus
}

func (m *coordinatorMetadata) ApplyStatusChanges(config *commonproto.ClusterConfiguration, ensembleSupplier EnsembleSupplier) (*commonproto.ClusterStatus, map[int64]string, []int64) {
	m.statusLock.Lock()
	defer m.statusLock.Unlock()

	newStatus := gproto.Clone(m.currentStatus).(*commonproto.ClusterStatus) //nolint:revive
	shardsToAdd, shardsToDelete := util.ApplyClusterChanges(config, newStatus, ensembleSupplier)
	if len(shardsToAdd) == 0 && len(shardsToDelete) == 0 {
		return newStatus, shardsToAdd, shardsToDelete
	}

	m.persistStatusLocked(newStatus, "failed to apply cluster changes")
	m.notifyStatusChange()
	return newStatus, shardsToAdd, shardsToDelete
}

func (m *coordinatorMetadata) UpdateStatus(newStatus *commonproto.ClusterStatus) {
	m.statusLock.Lock()
	defer m.statusLock.Unlock()

	m.persistStatusLocked(newStatus, "failed to update status")
	m.notifyStatusChange()
}

func (m *coordinatorMetadata) UpdateShardMetadata(namespace string, shard int64, shardMetadata *commonproto.ShardMetadata) {
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

func (m *coordinatorMetadata) DeleteShardMetadata(namespace string, shard int64) {
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
			if shard.GetStatusOrDefault() != commonproto.ShardStatusSteadyState {
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
		newConfig, err := m.loadConfigFromProvider()
		if err != nil {
			return err
		}
		m.currentClusterConfig = gproto.Clone(newConfig).(*commonproto.ClusterConfiguration) //nolint:revive
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

func (m *coordinatorMetadata) loadConfigFromProvider() (*commonproto.ClusterConfiguration, error) {
	if loader, ok := m.configProvider.(interface {
		LoadConfig() (*commonproto.ClusterConfiguration, error)
	}); ok {
		return loader.LoadConfig()
	}

	value, _, err := m.configProvider.Get()
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, provider.ErrNotInitialized
	}
	config, ok := value.(*commonproto.ClusterConfiguration)
	if !ok {
		return nil, fmt.Errorf("expected *ClusterConfiguration from config provider, got %T", value)
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	for _, authority := range config.GetAllowExtraAuthorities() {
		if err := rpc.ValidateAuthorityAddress(authority); err != nil {
			return nil, fmt.Errorf("cluster configuration: invalid allowExtraAuthorities entry %q: %w", authority, err)
		}
	}
	return config, nil
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

func (m *coordinatorMetadata) waitForConfigUpdates(configNotifications <-chan struct{}) {
	for {
		select {
		case <-m.ctx.Done():
			return

		case _, ok := <-configNotifications:
			if !ok {
				return
			}
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

func (m *coordinatorMetadata) Node(id string) (*commonproto.DataServerIdentity, bool) {
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
