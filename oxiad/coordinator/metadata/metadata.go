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
	"github.com/pkg/errors"
	"go.uber.org/multierr"
	gproto "google.golang.org/protobuf/proto"
	k8s "k8s.io/client-go/kubernetes"

	"github.com/oxia-db/oxia/common/process"
	commonproto "github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	filemetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/file"
	k8smetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/kubernetes"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/raft"
	"github.com/oxia-db/oxia/oxiad/coordinator/option"
	"github.com/oxia-db/oxia/oxiad/coordinator/util"
)

type Metadata interface {
	io.Closer

	Load() (*commonproto.ClusterConfiguration, error)
	Store(*commonproto.ClusterConfiguration) error
	DeclarativeConfigEnabled() bool
	Watch() <-chan struct{}
	LoadStatus() *commonproto.ClusterStatus
	ApplyStatusChanges(config *commonproto.ClusterConfiguration, ensembleSupplier EnsembleSupplier) (*commonproto.ClusterStatus, map[int64]string, []int64)
	UpdateStatus(newStatus *commonproto.ClusterStatus)
	UpdateShardMetadata(namespace string, shard int64, shardMetadata *commonproto.ShardMetadata)
	DeleteShardMetadata(namespace string, shard int64)
	IsReady(clusterConfig *commonproto.ClusterConfiguration) bool
	StatusChangeNotify() <-chan struct{}

	LoadConfig() *commonproto.ClusterConfiguration
	UpdateConfig(*commonproto.ClusterConfiguration) bool
	LoadLoadBalancer() *commonproto.LoadBalancer
	Nodes() *linkedhashset.Set[string]
	NodesWithMetadata() (*linkedhashset.Set[string], map[string]*commonproto.DataServerMetadata)
	Namespace(namespace string) (*commonproto.Namespace, bool)
	Node(id string) (*commonproto.DataServerIdentity, bool)
	GetDataServer(name string) (*commonproto.DataServer, bool)
}

type EnsembleSupplier func(namespaceConfig *commonproto.Namespace, status *commonproto.ClusterStatus) ([]*commonproto.DataServerIdentity, error)

var newK8SClient = func() k8s.Interface {
	k8sConfig := k8smetadata.NewK8SClientConfig()
	return k8smetadata.NewK8SClientset(k8sConfig)
}

type loaderClusterConfigProvider struct {
	ctx             context.Context
	load            func() (*commonproto.ClusterConfiguration, error)
	notificationsCh chan any
	watchM          sync.Mutex
	watch           chan struct{}
}

func NewClusterConfigProviderFromLoader(
	ctx context.Context,
	load func() (*commonproto.ClusterConfiguration, error),
	notificationsCh chan any,
) provider.Provider {
	return &loaderClusterConfigProvider{
		ctx:             ctx,
		load:            load,
		notificationsCh: notificationsCh,
	}
}

func (p *loaderClusterConfigProvider) Load(document provider.Document) (data []byte, version provider.Version, err error) {
	if document != provider.DocumentClusterConfiguration {
		return nil, provider.NotExists, errors.Errorf("unsupported metadata document %q", document)
	}
	config, err := p.load()
	if err != nil {
		return nil, provider.NotExists, err
	}
	data, err = commonproto.MarshalClusterConfigurationYAML(config)
	return data, provider.NotExists, err
}

func (*loaderClusterConfigProvider) Store(provider.Document, []byte, provider.Version) (provider.Version, error) {
	return provider.NotExists, provider.ErrUnsupported
}

func (p *loaderClusterConfigProvider) SupportsWatch() bool {
	return p.notificationsCh != nil
}

func (p *loaderClusterConfigProvider) Watch() <-chan struct{} {
	if !p.SupportsWatch() {
		return nil
	}

	p.watchM.Lock()
	defer p.watchM.Unlock()
	if p.watch != nil {
		return p.watch
	}

	p.watch = make(chan struct{}, 1)
	go process.DoWithLabels(p.ctx, map[string]string{
		"component": "cluster-config-provider",
	}, p.forwardNotifications)
	return p.watch
}

func (p *loaderClusterConfigProvider) forwardNotifications() {
	for {
		select {
		case <-p.ctx.Done():
			return
		case _, ok := <-p.notificationsCh:
			if !ok {
				return
			}
			p.notify()
		}
	}
}

func (p *loaderClusterConfigProvider) notify() {
	p.watchM.Lock()
	ch := p.watch
	p.watchM.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (*loaderClusterConfigProvider) WaitToBecomeLeader() error {
	return nil
}

func (*loaderClusterConfigProvider) Close() error {
	return nil
}

func newProvider(providerOptions option.ProviderOptions) (provider.Provider, error) {
	switch providerOptions.ProviderName {
	case provider.NameMemory:
		return memory.NewProvider(), nil
	case provider.NameFile:
		return filemetadata.NewProviderWithPaths(providerOptions.File.StatusPath(), providerOptions.File.ConfigPath()), nil
	case provider.NameConfigMap:
		return k8smetadata.NewProvider(
			newK8SClient(),
			providerOptions.Kubernetes.Namespace,
			providerOptions.Kubernetes.StatusNameOrDefault(),
			providerOptions.Kubernetes.ConfigNameOrDefault(),
		), nil
	case provider.NameRaft:
		metadataProvider, err := raft.NewProvider(
			providerOptions.Raft.Address, providerOptions.Raft.BootstrapNodes, providerOptions.Raft.DataDir)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create raft metadata provider")
		}
		return metadataProvider, nil
	default:
		return nil, errors.New(`must be one of "memory", "configmap", "raft" or "file"`)
	}
}

func NewMetadataProvider(metadataOptions *option.MetadataOptions) (provider.Provider, error) {
	return newProvider(metadataOptions.GetStatusProviderOptions())
}

func NewConfigProvider(metadataProvider provider.Provider) (provider.Provider, bool, error) {
	return metadataProvider, metadataProvider.SupportsWatch(), nil
}

func NewFromOptions(
	ctx context.Context,
	clusterOptions *option.ClusterOptions,
	metadataOptions *option.MetadataOptions,
) (Metadata, error) {
	if err := metadataOptions.ApplyLegacyClusterConfigPath(clusterOptions.ConfigPath); err != nil {
		return nil, err
	}

	metadataProvider, err := NewMetadataProvider(metadataOptions)
	if err != nil {
		return nil, err
	}

	clusterConfigProvider, declarativeConfig, err := NewConfigProvider(metadataProvider)
	if err != nil {
		return nil, multierr.Append(err, metadataProvider.Close())
	}

	succeeded := false
	defer func() {
		if !succeeded {
			_ = metadataProvider.Close()
			if clusterConfigProvider != metadataProvider {
				_ = clusterConfigProvider.Close()
			}
		}
	}()

	if err := metadataProvider.WaitToBecomeLeader(); err != nil {
		return nil, errors.Wrap(err, "failed to wait in becoming leader")
	}

	metadata := newMetadata(ctx, metadataProvider, clusterConfigProvider, declarativeConfig)
	succeeded = true
	return metadata, nil
}

type coordinatorMetadata struct {
	*slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     *sync.WaitGroup

	metadataProvider      provider.Provider
	clusterConfigProvider provider.Provider
	declarativeConfig     bool

	statusLock       sync.RWMutex
	currentStatus    *commonproto.ClusterStatus
	currentVersionID provider.Version
	changeCh         chan struct{}

	clusterConfigLock     sync.RWMutex
	currentClusterConfig  *commonproto.ClusterConfiguration
	nodesIndex            *redblacktree.Tree[string, *commonproto.DataServerIdentity]
	namespaceConfigsIndex *redblacktree.Tree[string, *commonproto.Namespace]
}

func New(
	ctx context.Context,
	metadataProvider provider.Provider,
	clusterConfigProvider provider.Provider,
) Metadata {
	return newMetadata(ctx, metadataProvider, clusterConfigProvider, clusterConfigProvider.SupportsWatch())
}

func newMetadata(
	ctx context.Context,
	metadataProvider provider.Provider,
	clusterConfigProvider provider.Provider,
	declarativeConfig bool,
) Metadata {
	metadataCtx, cancel := context.WithCancel(ctx)
	m := &coordinatorMetadata{
		Logger:                slog.With(slog.String("component", "coordinator-metadata")),
		ctx:                   metadataCtx,
		cancel:                cancel,
		wg:                    &sync.WaitGroup{},
		metadataProvider:      metadataProvider,
		clusterConfigProvider: clusterConfigProvider,
		declarativeConfig:     declarativeConfig,
		currentVersionID:      provider.NotExists,
		changeCh:              make(chan struct{}),
	}

	m.doStatusRecovery()

	return m
}

func (m *coordinatorMetadata) Load() (*commonproto.ClusterConfiguration, error) {
	return m.loadClusterConfigDocument()
}

func (m *coordinatorMetadata) Store(config *commonproto.ClusterConfiguration) error {
	data, err := commonproto.MarshalClusterConfigurationYAML(config)
	if err != nil {
		return err
	}
	_, version, err := m.clusterConfigProvider.Load(provider.DocumentClusterConfiguration)
	if err != nil {
		return err
	}
	_, err = m.clusterConfigProvider.Store(provider.DocumentClusterConfiguration, data, version)
	return err
}

func (m *coordinatorMetadata) DeclarativeConfigEnabled() bool {
	return m.declarativeConfig
}

func (m *coordinatorMetadata) Watch() <-chan struct{} {
	return m.clusterConfigProvider.Watch()
}

func (m *coordinatorMetadata) Close() error {
	m.cancel()
	m.wg.Wait()
	err := m.metadataProvider.Close()
	if m.clusterConfigProvider != nil && m.clusterConfigProvider != m.metadataProvider {
		err = multierr.Append(err, m.clusterConfigProvider.Close())
	}
	return err
}

func (m *coordinatorMetadata) notifyStatusChange() {
	close(m.changeCh)
	m.changeCh = make(chan struct{})
}

func (m *coordinatorMetadata) doStatusRecovery() {
	m.statusLock.Lock()
	defer m.statusLock.Unlock()

	_ = backoff.RetryNotify(func() error {
		data, version, err := m.metadataProvider.Load(provider.DocumentClusterStatus)
		if err != nil {
			return err
		}
		var clusterStatus *commonproto.ClusterStatus
		if len(data) > 0 {
			clusterStatus, err = commonproto.UnmarshalClusterStatusYAML(data)
			if err != nil {
				return err
			}
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
		data, err := commonproto.MarshalClusterStatusJSON(newStatus)
		if err != nil {
			return err
		}
		versionID, err := m.metadataProvider.Store(provider.DocumentClusterStatus, data, m.currentVersionID)
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
		newConfig, err := m.loadClusterConfigDocument()
		if err != nil {
			return err
		}
		m.currentClusterConfig = gproto.Clone(newConfig).(*commonproto.ClusterConfiguration) //nolint:revive
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

func (m *coordinatorMetadata) loadClusterConfigDocument() (*commonproto.ClusterConfiguration, error) {
	data, _, err := m.clusterConfigProvider.Load(provider.DocumentClusterConfiguration)
	if err != nil {
		return nil, err
	}
	return provider.ParseClusterConfig(data)
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

func (m *coordinatorMetadata) UpdateConfig(newConfig *commonproto.ClusterConfiguration) bool {
	m.clusterConfigLock.Lock()
	defer m.clusterConfigLock.Unlock()

	if gproto.Equal(m.currentClusterConfig, newConfig) {
		return false
	}

	m.currentClusterConfig = gproto.Clone(newConfig).(*commonproto.ClusterConfiguration) //nolint:revive
	m.rebuildConfigIndexesLocked()
	return true
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
