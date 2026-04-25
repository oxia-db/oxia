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
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/process"
	oxiatime "github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
)

var _ provider.Provider = &Provider{}

const (
	leaseDuration = 15 * time.Second
	renewDeadline = 10 * time.Second
	retryPeriod   = 2 * time.Second
)

const (
	clusterStatusConfigMapKey = "status"
	clusterConfigMapKey       = "config.yaml"
)

type Provider struct {
	sync.Mutex
	kubernetes             kubernetes.Interface
	namespace              string
	statusName, configName string

	metadataSize      atomic.Int64
	getLatencyHisto   metric.LatencyHistogram
	storeLatencyHisto metric.LatencyHistogram
	metadataSizeGauge metric.Gauge

	ctx     context.Context
	cancel  context.CancelFunc
	closeCh chan any
	watch   chan struct{}

	log *slog.Logger
}

func NewConfigMapProvider(kc kubernetes.Interface, namespace, name string) provider.Provider {
	return NewProvider(kc, namespace, name, name)
}

func NewProvider(kc kubernetes.Interface, namespace, statusName, configName string) provider.Provider {
	m := &Provider{
		kubernetes: kc,
		namespace:  namespace,
		statusName: statusName,
		configName: configName,
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

func (m *Provider) Load(document provider.Document) (data []byte, version provider.Version, err error) {
	timer := m.getLatencyHisto.Timer()
	defer timer.Done()

	m.Lock()
	defer m.Unlock()
	return m.loadWithoutLock(document)
}

func (m *Provider) loadWithoutLock(document provider.Document) ([]byte, provider.Version, error) {
	key, err := documentKey(document)
	if err != nil {
		return nil, provider.NotExists, err
	}

	name, err := m.configMapName(document)
	if err != nil {
		return nil, provider.NotExists, err
	}

	cm, err := K8SConfigMaps(m.kubernetes).Get(m.namespace, name)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil, provider.NotExists, nil
		}
		return nil, "", err
	}

	version := provider.Version(cm.ResourceVersion)
	data, found := cm.Data[key]
	if !found {
		return nil, version, nil
	}

	slog.Debug("Get metadata successful",
		slog.String("version", cm.ResourceVersion))
	m.metadataSize.Store(int64(len(data)))
	return []byte(data), version, nil
}

func (m *Provider) Store(document provider.Document, data []byte, expectedVersion provider.Version) (provider.Version, error) {
	timer := m.storeLatencyHisto.Timer()
	defer timer.Done()

	m.Lock()
	defer m.Unlock()

	key, err := documentKey(document)
	if err != nil {
		return provider.NotExists, err
	}
	name, err := m.configMapName(document)
	if err != nil {
		return provider.NotExists, err
	}

	existing, version, err := m.getConfigMapDataWithoutLock(name)
	if err != nil {
		return version, err
	}

	if version != expectedVersion {
		slog.Error("Store metadata failed for version mismatch",
			slog.Any("local-version", version),
			slog.Any("expected-version", expectedVersion))
		panic(provider.ErrBadVersion)
	}

	existing[key] = string(data)
	cm, err := K8SConfigMaps(m.kubernetes).Upsert(m.namespace, name, configMap(name, existing, expectedVersion))
	if err != nil {
		if k8serrors.IsConflict(err) {
			panic(provider.ErrBadVersion)
		}
		return version, err
	}
	version = provider.Version(cm.ResourceVersion)
	m.metadataSize.Store(int64(len(data)))
	if document == provider.DocumentClusterConfiguration {
		m.notifyWatchLocked()
	}
	return version, nil
}

func (m *Provider) getConfigMapDataWithoutLock(name string) (map[string]string, provider.Version, error) {
	cm, err := K8SConfigMaps(m.kubernetes).Get(m.namespace, name)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return map[string]string{}, provider.NotExists, nil
		}
		return nil, provider.NotExists, err
	}

	data := make(map[string]string, len(cm.Data)+1)
	for k, v := range cm.Data {
		data[k] = v
	}
	return data, provider.Version(cm.ResourceVersion), nil
}

func (m *Provider) WaitToBecomeLeader() error {
	m.Lock()
	defer m.Unlock()

	myIdentity, _ := os.Hostname()

	// Create a lease lock
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      m.statusName,
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

func (m *Provider) configMapName(document provider.Document) (string, error) {
	switch document {
	case provider.DocumentClusterStatus:
		if m.statusName == "" {
			return "", errors.New("cluster status config map name is not configured")
		}
		return m.statusName, nil
	case provider.DocumentClusterConfiguration:
		if m.configName == "" {
			return "", errors.New("cluster configuration config map name is not configured")
		}
		return m.configName, nil
	default:
		return "", errors.Errorf("unsupported metadata document %q", document)
	}
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

func configMap(name string, data map[string]string, version provider.Version) *corev1.ConfigMap {
	cm := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Data:       data,
	}

	if version != provider.NotExists {
		cm.ResourceVersion = string(version)
	}

	return cm
}

func documentKey(document provider.Document) (string, error) {
	switch document {
	case provider.DocumentClusterStatus:
		return clusterStatusConfigMapKey, nil
	case provider.DocumentClusterConfiguration:
		return clusterConfigMapKey, nil
	default:
		return "", errors.Errorf("unsupported metadata document %q", document)
	}
}

func (*Provider) SupportsWatch() bool {
	return true
}

func (m *Provider) Watch() <-chan struct{} {
	m.Lock()
	defer m.Unlock()
	if m.watch != nil {
		return m.watch
	}
	m.watch = make(chan struct{}, 1)
	m.watchConfigMap()
	return m.watch
}

func (m *Provider) watchConfigMap() {
	go process.DoWithLabels(m.ctx, map[string]string{
		"component": "k8s-configmap-watch",
	}, func() {
		bo := oxiatime.NewBackOffWithInitialInterval(m.ctx, time.Second)
		_ = backoff.RetryNotify(func() error {
			err := m.watchOnce(m.ctx)
			if err == nil {
				return errors.New("K8S config map watch closed")
			}
			return err
		}, bo, func(err error, duration time.Duration) {
			slog.Warn("K8S config map watch failed, reconnecting",
				slog.String("k8s-namespace", m.namespace),
				slog.String("k8s-config-map", m.configName),
				slog.Any("error", err),
				slog.Duration("retry-after", duration),
			)
		})
	})
}

func (m *Provider) watchOnce(ctx context.Context) error {
	w, err := m.kubernetes.CoreV1().ConfigMaps(m.namespace).Watch(
		ctx,
		metav1.SingleObject(metav1.ObjectMeta{Name: m.configName, Namespace: m.namespace}),
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
					slog.String("k8s-namespace", m.namespace),
					slog.String("k8s-config-map", m.configName),
					slog.Any("object", res),
				)
				continue
			}

			slog.Info("Got watch event from K8S",
				slog.String("k8s-namespace", m.namespace),
				slog.String("k8s-config-map", m.configName),
				slog.Any("event-type", res.Type),
			)

			switch res.Type {
			case watch.Added, watch.Modified:
				if _, found := configMap.Data[clusterConfigMapKey]; found {
					m.notifyWatch()
				}
			default:
				return errors.Errorf("unexpected event on config map: %v", res.Type)
			}
		}
	}
}

func (m *Provider) notifyWatch() {
	m.Lock()
	defer m.Unlock()
	m.notifyWatchLocked()
}

func (m *Provider) notifyWatchLocked() {
	ch := m.watch
	if ch == nil {
		return
	}
	select {
	case ch <- struct{}{}:
	default:
	}
}
