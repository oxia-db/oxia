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

package kubernetes

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	gproto "google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"

	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/process"
	commonproto "github.com/oxia-db/oxia/common/proto"
	oxiatime "github.com/oxia-db/oxia/common/time"
)

var _ provider.Provider = &Provider{}

const (
	leaseDuration = 15 * time.Second
	renewDeadline = 10 * time.Second
	retryPeriod   = 2 * time.Second
)

type Provider struct {
	sync.Mutex
	kubernetes      kubernetes.Interface
	namespace, name string

	metadataSize      atomic.Int64
	getLatencyHisto   metric.LatencyHistogram
	storeLatencyHisto metric.LatencyHistogram
	metadataSizeGauge metric.Gauge

	ctx     context.Context
	cancel  context.CancelFunc
	closeCh chan any

	log *slog.Logger
}

func NewConfigMapProvider(kc kubernetes.Interface, namespace, name string) provider.Provider {
	m := &Provider{
		kubernetes: kc,
		namespace:  namespace,
		name:       name,
		log:        slog.With("component", "metadata-config-map"),

		getLatencyHisto: metric.NewLatencyHistogram("oxia_coordinator_metadata_get_latency",
			"Latency for reading coordinator metadata", nil),
		storeLatencyHisto: metric.NewLatencyHistogram("oxia_coordinator_metadata_store_latency",
			"Latency for storing coordinator metadata", nil),
	}

	m.ctx, m.cancel = context.WithCancel(context.Background())

	m.metadataSizeGauge = metric.NewGauge("oxia_coordinator_metadata_size",
		"The size of the coordinator metadata", metric.Bytes, nil, func() int64 {
			return m.metadataSize.Load()
		})

	logger := logr.FromSlogHandler(m.log.With(slog.String("sub-component", "k8s-client")).Handler())
	klog.SetLogger(logger)
	return m
}

func (m *Provider) Get() (status *commonproto.ClusterStatus, version provider.Version, err error) {
	timer := m.getLatencyHisto.Timer()
	defer timer.Done()

	m.Lock()
	defer m.Unlock()
	return m.getWithoutLock()
}

func (m *Provider) getWithoutLock() (*commonproto.ClusterStatus, provider.Version, error) {
	cm, err := K8SConfigMaps(m.kubernetes).Get(m.namespace, m.name)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil, provider.NotExists, nil
		}
		return nil, "", err
	}

	data := []byte(cm.Data["status"])
	status, err := commonproto.UnmarshalClusterStatusYAML(data)
	if err != nil {
		return nil, "", err
	}

	version := provider.Version(cm.ResourceVersion)
	slog.Debug("Get metadata successful",
		slog.String("version", cm.ResourceVersion))
	m.metadataSize.Store(int64(len(data)))
	return status, version, nil
}

func (m *Provider) Store(status *commonproto.ClusterStatus, expectedVersion provider.Version) (provider.Version, error) {
	timer := m.storeLatencyHisto.Timer()
	defer timer.Done()

	m.Lock()
	defer m.Unlock()

	_, version, err := m.getWithoutLock()
	if err != nil {
		return version, err
	}

	if version != expectedVersion {
		slog.Error("Store metadata failed for version mismatch",
			slog.Any("local-version", version),
			slog.Any("expected-version", expectedVersion))
		panic(provider.ErrBadVersion)
	}

	data := configMap(m.name, status, expectedVersion)
	cm, err := K8SConfigMaps(m.kubernetes).Upsert(m.namespace, m.name, data)
	if err != nil {
		if k8serrors.IsConflict(err) {
			panic(provider.ErrBadVersion)
		}
		return version, err
	}
	version = provider.Version(cm.ResourceVersion)
	m.metadataSize.Store(int64(len(data.Data["status"])))
	return version, nil
}

func (m *Provider) WaitToBecomeLeader() error {
	m.Lock()
	defer m.Unlock()

	myIdentity, _ := os.Hostname()

	// Create a lease lock
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      m.name,
			Namespace: m.namespace,
		},
		Client: m.kubernetes.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: myIdentity,
		},
	}

	log := m.log.With(slog.String("identity", myIdentity))
	wg := concurrent.NewWaitGroup(1)

	// Configure leader election
	leaderElectionConfig := leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   leaseDuration,
		RenewDeadline:   renewDeadline,
		RetryPeriod:     retryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(_ context.Context) {
				log.Info("Started leading - lease acquired")
				wg.Done()
			},
			OnStoppedLeading: func() {
				log.Warn("Stopped leading - lease lost!")
			},
			OnNewLeader: func(newLeader string) {
				if newLeader == myIdentity {
					return
				}

				log.Info("New leader elected", slog.String("leader", newLeader))
			},
		},
	}

	// Start leader election
	leaderElector, err := leaderelection.NewLeaderElector(leaderElectionConfig)
	if err != nil {
		panic(err)
	}

	m.closeCh = make(chan any)

	go process.DoWithLabels(m.ctx, map[string]string{
		"component":     "metadata-provider",
		"sub-component": "k8s-leader-elector",
	}, func() {
		leaderElector.Run(m.ctx)
		close(m.closeCh)
	})

	return wg.Wait(m.ctx)
}

func (m *Provider) Close() error {
	m.Lock()
	defer m.Unlock()

	m.cancel()
	if m.closeCh != nil {
		<-m.closeCh
	}
	m.log.Info("Closed metadata provider")
	return nil
}

func configMap(name string, status *commonproto.ClusterStatus, version provider.Version) *corev1.ConfigMap {
	bytes, err := commonproto.MarshalClusterStatusYAML(status)
	if err != nil {
		slog.Error(
			"unable to marshal cluster status",
			slog.Any("error", err),
		)
		panic(err)
	}

	cm := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Data: map[string]string{
			"status": string(bytes),
		},
	}

	if version != provider.NotExists {
		cm.ResourceVersion = string(version)
	}

	return cm
}

const clusterConfigMapKey = "config.yaml"

type ClusterConfigStore struct {
	kubernetes      kubernetes.Interface
	namespace, name string
	watch           *commonoption.Watch[*commonproto.ClusterConfiguration]
}

func NewClusterConfigStore(
	ctx context.Context,
	kc kubernetes.Interface,
	namespace string,
	name string,
) (*ClusterConfigStore, error) {
	store := &ClusterConfigStore{
		kubernetes: kc,
		namespace:  namespace,
		name:       name,
		watch:      commonoption.NewWatch[*commonproto.ClusterConfiguration](nil),
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

func (s *ClusterConfigStore) Load() (*commonproto.ClusterConfiguration, error) {
	configMap, err := K8SConfigMaps(s.kubernetes).Get(s.namespace, s.name)
	if err != nil {
		return nil, err
	}
	config, err := s.parseConfigMap(configMap)
	if err != nil {
		return nil, err
	}
	s.notify(config)
	return config, nil
}

func (s *ClusterConfigStore) Watch() *commonoption.Watch[*commonproto.ClusterConfiguration] {
	return s.watch
}

func (s *ClusterConfigStore) watchOnce(ctx context.Context) error {
	w, err := s.kubernetes.CoreV1().ConfigMaps(s.namespace).Watch(
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
				config, err := s.parseConfigMap(configMap)
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

func (s *ClusterConfigStore) parseConfigMap(configMap *corev1.ConfigMap) (*commonproto.ClusterConfiguration, error) {
	data, found := configMap.Data[clusterConfigMapKey]
	if !found {
		return nil, errors.Errorf("path %q not found in config map: configmap:%s/%s", clusterConfigMapKey, s.namespace, s.name)
	}
	return provider.ParseClusterConfig([]byte(data))
}

func (s *ClusterConfigStore) notify(config *commonproto.ClusterConfiguration) {
	current, _ := s.watch.Load()
	if gproto.Equal(current, config) {
		return
	}
	s.watch.Notify(gproto.Clone(config).(*commonproto.ClusterConfiguration)) //nolint:revive
}
