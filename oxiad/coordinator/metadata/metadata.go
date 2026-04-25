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
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/emirpasic/gods/v2/sets/linkedhashset"
	"github.com/emirpasic/gods/v2/trees/redblacktree"
	"github.com/fsnotify/fsnotify"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	gproto "google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	k8s "k8s.io/client-go/kubernetes"

	"github.com/oxia-db/oxia/common/process"
	commonproto "github.com/oxia-db/oxia/common/proto"
	oxiatime "github.com/oxia-db/oxia/common/time"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
	rpc2 "github.com/oxia-db/oxia/oxiad/common/rpc"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	k8smetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/kubernetes"
	"github.com/oxia-db/oxia/oxiad/coordinator/option"
	"github.com/oxia-db/oxia/oxiad/coordinator/util"
)

type Metadata interface {
	io.Closer
	ClusterConfigStore

	LoadStatus() *commonproto.ClusterStatus
	ApplyStatusChanges(config *commonproto.ClusterConfiguration, ensembleSupplier EnsembleSupplier) (*commonproto.ClusterStatus, map[int64]string, []int64)
	UpdateStatus(newStatus *commonproto.ClusterStatus)
	UpdateShardMetadata(namespace string, shard int64, shardMetadata *commonproto.ShardMetadata)
	DeleteShardMetadata(namespace string, shard int64)
	IsReady(clusterConfig *commonproto.ClusterConfiguration) bool
	StatusChangeNotify() <-chan struct{}

	LoadConfig() *commonproto.ClusterConfiguration
	UpdateConfig(*commonproto.ClusterConfiguration) bool
	ConfigWatch() *commonoption.Watch[*commonproto.ClusterConfiguration]
	LoadLoadBalancer() *commonproto.LoadBalancer
	Nodes() *linkedhashset.Set[string]
	NodesWithMetadata() (*linkedhashset.Set[string], map[string]*commonproto.DataServerMetadata)
	Namespace(namespace string) (*commonproto.Namespace, bool)
	Node(id string) (*commonproto.DataServerIdentity, bool)
	GetDataServer(name string) (*commonproto.DataServer, bool)
}

type EnsembleSupplier func(namespaceConfig *commonproto.Namespace, status *commonproto.ClusterStatus) ([]*commonproto.DataServerIdentity, error)

const (
	configMapClusterConfigPrefix = "configmap:"
	clusterConfigMapKey          = "config.yaml"
	k8sRequestTimeout            = 30 * time.Second
)

var newK8SClient = func() k8s.Interface {
	k8sConfig := k8smetadata.NewK8SClientConfig()
	return k8smetadata.NewK8SClientset(k8sConfig)
}

type ClusterConfigStore interface {
	Load() (*commonproto.ClusterConfiguration, error)
	Watch() *commonoption.Watch[*commonproto.ClusterConfiguration]
}

type providerConfigStore struct {
	load  func() (*commonproto.ClusterConfiguration, error)
	watch *commonoption.Watch[*commonproto.ClusterConfiguration]
}

func NewProviderClusterConfigStore(
	ctx context.Context,
	load func() (*commonproto.ClusterConfiguration, error),
	notificationsCh chan any,
) ClusterConfigStore {
	store := &providerConfigStore{
		load:  load,
		watch: commonoption.NewWatch[*commonproto.ClusterConfiguration](nil),
	}
	if notificationsCh != nil {
		go process.DoWithLabels(ctx, map[string]string{
			"component": "cluster-config-store",
		}, func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-notificationsCh:
					_ = backoff.RetryNotify(func() error {
						_, err := store.Load()
						return err
					}, oxiatime.NewBackOffWithInitialInterval(ctx, time.Second), func(err error, duration time.Duration) {
						slog.Warn("failed to reload cluster configuration, retrying later",
							slog.Any("error", err),
							slog.Duration("retry-after", duration),
						)
					})
				}
			}
		})
	}
	return store
}

func (s *providerConfigStore) Load() (*commonproto.ClusterConfiguration, error) {
	config, err := s.load()
	if err != nil {
		return nil, err
	}
	s.notify(config)
	return gproto.Clone(config).(*commonproto.ClusterConfiguration), nil //nolint:revive
}

func (s *providerConfigStore) Watch() *commonoption.Watch[*commonproto.ClusterConfiguration] {
	return s.watch
}

func (s *providerConfigStore) notify(config *commonproto.ClusterConfiguration) {
	current, _ := s.watch.Load()
	if gproto.Equal(current, config) {
		return
	}
	s.watch.Notify(gproto.Clone(config).(*commonproto.ClusterConfiguration)) //nolint:revive
}

func NewClusterConfigStore(ctx context.Context, cluster *option.ClusterOptions) (ClusterConfigStore, error) {
	if strings.HasPrefix(cluster.ConfigPath, configMapClusterConfigPrefix) {
		return newConfigMapConfigStore(ctx, cluster.ConfigPath)
	}
	return newFileConfigStore(cluster.ConfigPath)
}

type fileConfigStore struct {
	v     *viper.Viper
	watch *commonoption.Watch[*commonproto.ClusterConfiguration]
}

func newFileConfigStore(configPath string) (*fileConfigStore, error) {
	v := viper.New()
	v.SetConfigType("yaml")
	if configPath == "" {
		v.AddConfigPath("/oxia/conf")
		v.AddConfigPath(".")
	} else {
		v.SetConfigFile(configPath)
	}
	store := &fileConfigStore{
		v:     v,
		watch: commonoption.NewWatch[*commonproto.ClusterConfiguration](nil),
	}
	if _, err := store.Load(); err != nil {
		return nil, err
	}
	v.OnConfigChange(func(_ fsnotify.Event) {
		if _, err := store.Load(); err != nil {
			slog.Warn("failed to reload file cluster configuration", slog.Any("error", err))
		}
	})
	v.WatchConfig()
	return store, nil
}

func (s *fileConfigStore) Load() (*commonproto.ClusterConfiguration, error) {
	if err := s.v.ReadInConfig(); err != nil {
		return nil, err
	}

	configFile := s.v.ConfigFileUsed()
	if configFile == "" {
		return nil, errors.New("cluster configuration: no config file was loaded")
	}
	data, err := os.ReadFile(configFile)
	if err != nil {
		return nil, err
	}
	config, err := parseClusterConfig(data)
	if err != nil {
		return nil, err
	}
	current, _ := s.watch.Load()
	if !gproto.Equal(current, config) {
		s.watch.Notify(gproto.Clone(config).(*commonproto.ClusterConfiguration)) //nolint:revive
	}
	return config, nil
}

func (s *fileConfigStore) Watch() *commonoption.Watch[*commonproto.ClusterConfiguration] {
	return s.watch
}

type configMapConfigStore struct {
	client          k8s.Interface
	namespace, name string
	watch           *commonoption.Watch[*commonproto.ClusterConfiguration]
}

func newConfigMapConfigStore(ctx context.Context, configPath string) (*configMapConfigStore, error) {
	path := strings.TrimPrefix(configPath, configMapClusterConfigPrefix)
	namespace, name, ok := strings.Cut(path, "/")
	if !ok || namespace == "" || name == "" {
		return nil, errors.Errorf("invalid configmap cluster configuration path %q, expected configmap:<namespace>/<name>", configPath)
	}
	store := &configMapConfigStore{
		client:    newK8SClient(),
		namespace: namespace,
		name:      name,
		watch:     commonoption.NewWatch[*commonproto.ClusterConfiguration](nil),
	}
	if _, err := store.Load(); err != nil {
		return nil, err
	}
	go process.DoWithLabels(ctx, map[string]string{
		"component": "k8s-configmap-watch",
	}, func() {
		bo := oxiatime.NewBackOffWithInitialInterval(ctx, time.Second)
		_ = backoff.RetryNotify(func() error {
			err := store.watchOnce(ctx)
			if err == nil {
				return errors.New("K8S config map watch closed")
			}
			return err
		}, bo, func(err error, duration time.Duration) {
			slog.Warn("K8S config map watch failed, reconnecting",
				slog.String("k8s-namespace", store.namespace),
				slog.String("k8s-config-map", store.name),
				slog.Any("error", err),
				slog.Duration("retry-after", duration),
			)
		})
	})
	return store, nil
}

func (s *configMapConfigStore) Load() (*commonproto.ClusterConfiguration, error) {
	ctx, cancel := context.WithTimeout(context.Background(), k8sRequestTimeout)
	defer cancel()

	configMap, err := s.client.CoreV1().ConfigMaps(s.namespace).Get(ctx, s.name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	config, err := parseConfigMapData(configMap, s.namespace, s.name)
	if err != nil {
		return nil, err
	}
	s.notify(config)
	return config, nil
}

func (s *configMapConfigStore) Watch() *commonoption.Watch[*commonproto.ClusterConfiguration] {
	return s.watch
}

func (s *configMapConfigStore) watchOnce(ctx context.Context) error {
	w, err := s.client.CoreV1().ConfigMaps(s.namespace).Watch(
		ctx,
		metav1.SingleObject(metav1.ObjectMeta{Name: s.name, Namespace: s.namespace}),
	)
	if err != nil {
		return errors.Wrap(err, "failed to setup watch on config map")
	}
	defer w.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case res, ok := <-w.ResultChan():
			if !ok {
				return errors.New("K8S config map watch closed")
			}
			if res.Type == watch.Error {
				return errors.Errorf("watch error: %v", res.Object)
			}
			configMap, ok := res.Object.(*corev1.ConfigMap)
			if !ok {
				slog.Warn("Got wrong type of object notification",
					slog.String("k8s-namespace", s.namespace),
					slog.String("k8s-config-map", s.name),
					slog.Any("object", res),
				)
				continue
			}

			slog.Info("Got watch event from K8S",
				slog.String("k8s-namespace", s.namespace),
				slog.String("k8s-config-map", s.name),
				slog.Any("event-type", res.Type),
			)

			switch res.Type {
			case watch.Added, watch.Modified:
				config, err := parseConfigMapData(configMap, s.namespace, s.name)
				if err != nil {
					return err
				}
				s.notify(config)
			default:
				return errors.Errorf("unexpected event on config map: %v", res.Type)
			}
		}
	}
}

func (s *configMapConfigStore) notify(config *commonproto.ClusterConfiguration) {
	current, _ := s.watch.Load()
	if gproto.Equal(current, config) {
		return
	}
	s.watch.Notify(gproto.Clone(config).(*commonproto.ClusterConfiguration)) //nolint:revive
}

func parseConfigMapData(configMap *corev1.ConfigMap, namespace, name string) (*commonproto.ClusterConfiguration, error) {
	data, found := configMap.Data[clusterConfigMapKey]
	if !found {
		return nil, errors.Errorf("path %q not found in config map: configmap:%s/%s", clusterConfigMapKey, namespace, name)
	}
	return parseClusterConfig([]byte(data))
}

func parseClusterConfig(data []byte) (*commonproto.ClusterConfiguration, error) {
	config, err := commonproto.UnmarshalClusterConfigurationYAML(data)
	if err != nil {
		return nil, err
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	for _, authority := range config.GetAllowExtraAuthorities() {
		if err := rpc2.ValidateAuthorityAddress(authority); err != nil {
			return nil, errors.Wrapf(err, "cluster configuration: invalid allowExtraAuthorities entry %q", authority)
		}
	}
	return config, nil
}

type coordinatorMetadata struct {
	*slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     *sync.WaitGroup

	metadataProvider provider.Provider

	statusLock       sync.RWMutex
	currentStatus    *commonproto.ClusterStatus
	currentVersionID provider.Version
	changeCh         chan struct{}

	clusterConfigStore ClusterConfigStore

	clusterConfigLock     sync.RWMutex
	currentClusterConfig  *commonproto.ClusterConfiguration
	nodesIndex            *redblacktree.Tree[string, *commonproto.DataServerIdentity]
	namespaceConfigsIndex *redblacktree.Tree[string, *commonproto.Namespace]
}

func New(
	ctx context.Context,
	metadataProvider provider.Provider,
	clusterConfigStore ClusterConfigStore,
) Metadata {
	metadataCtx, cancel := context.WithCancel(ctx)
	m := &coordinatorMetadata{
		Logger:             slog.With(slog.String("component", "coordinator-metadata")),
		ctx:                metadataCtx,
		cancel:             cancel,
		wg:                 &sync.WaitGroup{},
		metadataProvider:   metadataProvider,
		currentVersionID:   provider.NotExists,
		changeCh:           make(chan struct{}),
		clusterConfigStore: clusterConfigStore,
	}

	m.doStatusRecovery()

	return m
}

func (m *coordinatorMetadata) Load() (*commonproto.ClusterConfiguration, error) {
	return m.clusterConfigStore.Load()
}

func (m *coordinatorMetadata) Watch() *commonoption.Watch[*commonproto.ClusterConfiguration] {
	return m.clusterConfigStore.Watch()
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
		newConfig, err := m.clusterConfigStore.Load()
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

func (m *coordinatorMetadata) ConfigWatch() *commonoption.Watch[*commonproto.ClusterConfiguration] {
	return m.clusterConfigStore.Watch()
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
