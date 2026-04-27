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
	"encoding/json"
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
	"k8s.io/apimachinery/pkg/types"
	k8swatch "k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/process"
	commonproto "github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	metadatawatch "github.com/oxia-db/oxia/oxiad/coordinator/metadata/watch"
)

var _ provider.Provider[*commonproto.ClusterStatus] = (*Provider[*commonproto.ClusterStatus])(nil)
var _ provider.Provider[*commonproto.ClusterConfiguration] = (*Provider[*commonproto.ClusterConfiguration])(nil)

const (
	leaseDuration = 15 * time.Second
	renewDeadline = 10 * time.Second
	retryPeriod   = 2 * time.Second

	fieldManager      = "oxia-coordinator"
	k8sRequestTimeout = 30 * time.Second
)

type Provider[T gproto.Message] struct {
	sync.Mutex
	kubernetes      kubernetes.Interface
	namespace, name string
	codec           provider.Codec[T]
	watchEnabled    provider.WatchMode

	metadataSize      atomic.Int64
	getLatencyHisto   metric.LatencyHistogram
	storeLatencyHisto metric.LatencyHistogram
	metadataSizeGauge metric.Gauge

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	watcher *metadatawatch.Watch[T]

	log *slog.Logger
}

func NewConfigMapProvider[T gproto.Message](
	ctx context.Context,
	kc kubernetes.Interface,
	namespace, name string,
	codec provider.Codec[T],
	watchEnabled provider.WatchMode,
) provider.Provider[T] {
	m := &Provider[T]{
		kubernetes:   kc,
		namespace:    namespace,
		name:         name,
		codec:        codec,
		watchEnabled: watchEnabled,
		log:          slog.With("component", "metadata-config-map"),

		getLatencyHisto: metric.NewLatencyHistogram("oxia_coordinator_metadata_get_latency",
			"Latency for reading coordinator metadata", nil),
		storeLatencyHisto: metric.NewLatencyHistogram("oxia_coordinator_metadata_store_latency",
			"Latency for storing coordinator metadata", nil),
	}

	m.ctx, m.cancel = context.WithCancel(ctx)
	if watchEnabled.Enabled() {
		m.watcher = metadatawatch.New[T]()
		m.wg.Go(func() {
			process.DoWithLabels(m.ctx, map[string]string{
				"component":     "metadata-provider",
				"sub-component": "k8s-configmap-watch",
			}, m.watchLoop)
		})
	}

	m.metadataSizeGauge = metric.NewGauge("oxia_coordinator_metadata_size",
		"The size of the coordinator metadata", metric.Bytes, nil, func() int64 {
			return m.metadataSize.Load()
		})

	logger := logr.FromSlogHandler(m.log.With(slog.String("sub-component", "k8s-client")).Handler())
	klog.SetLogger(logger)
	return m
}

func (m *Provider[T]) Get() (value T, version provider.Version, err error) {
	timer := m.getLatencyHisto.Timer()
	defer timer.Done()

	m.Lock()
	defer m.Unlock()
	return m.getWithoutLock()
}

func (m *Provider[T]) getWithoutLock() (value T, version provider.Version, err error) {
	ctx, cancel := context.WithTimeout(m.ctx, k8sRequestTimeout)
	defer cancel()

	cm, err := m.kubernetes.CoreV1().ConfigMaps(m.namespace).Get(ctx, m.name, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return value, provider.NotExists, nil
		}
		return value, "", err
	}

	data, ok := cm.Data[m.codec.ConfigMapDataKey()]
	if !ok {
		return value, provider.NotExists, nil
	}

	version = provider.Version(cm.ResourceVersion)
	slog.Debug("Get metadata successful",
		slog.String("version", cm.ResourceVersion))
	m.metadataSize.Store(int64(len(data)))
	value, err = m.codec.Unmarshal([]byte(data))
	return value, version, err
}

func (m *Provider[T]) Store(value T, expectedVersion provider.Version) (provider.Version, error) {
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

	data, err := m.codec.MarshalYAML(value)
	if err != nil {
		return provider.NotExists, err
	}
	cmData := makeDesiredConfigMap(m.name, m.codec.ConfigMapDataKey(), data, expectedVersion)
	desiredBytes, err := json.Marshal(cmData)
	if err != nil {
		return provider.NotExists, err
	}

	ctx, cancel := context.WithTimeout(m.ctx, k8sRequestTimeout)
	defer cancel()

	cm, err := m.kubernetes.CoreV1().ConfigMaps(m.namespace).Patch(ctx, m.name, types.ApplyPatchType, desiredBytes, metav1.PatchOptions{
		FieldManager: fieldManager,
		Force:        gproto.Bool(true),
	})
	if k8serrors.IsNotFound(err) {
		cm, err = m.kubernetes.CoreV1().ConfigMaps(m.namespace).Create(ctx, cmData, metav1.CreateOptions{})
	}
	if err != nil {
		if k8serrors.IsConflict(err) {
			panic(provider.ErrBadVersion)
		}
		return version, err
	}
	version = provider.Version(cm.ResourceVersion)
	m.metadataSize.Store(int64(len(cmData.Data[m.codec.ConfigMapDataKey()])))
	return version, nil
}

func (m *Provider[T]) WaitToBecomeLeader() error {
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

	m.wg.Go(func() {
		process.DoWithLabels(m.ctx, map[string]string{
			"component":     "metadata-provider",
			"sub-component": "k8s-leader-elector",
		}, func() {
			leaderElector.Run(m.ctx)
		})
	})

	return wg.Wait(m.ctx)
}

func (m *Provider[T]) Close() error {
	m.cancel()
	m.wg.Wait()
	if m.watcher != nil {
		m.watcher.Close()
	}
	m.log.Info("Closed metadata provider")
	return nil
}

func (m *Provider[T]) Watch() (*metadatawatch.Receiver[T], error) {
	if !m.watchEnabled.Enabled() || m.watcher == nil {
		return nil, provider.ErrWatchUnsupported
	}
	return m.watcher.Subscribe()
}

func (m *Provider[T]) watchLoop() {
	retry := backoff.NewExponentialBackOff()
	retry.InitialInterval = time.Second
	_ = backoff.RetryNotify(func() error {
		value, _, err := m.Get()
		if err != nil {
			return err
		}
		m.watcher.Publish(value)
		return m.watch()
	}, backoff.WithContext(retry, m.ctx), func(err error, duration time.Duration) {
		m.log.Warn("K8S config map watch failed, reconnecting",
			slog.String("k8s-namespace", m.namespace),
			slog.String("k8s-config-map", m.name),
			slog.Any("error", err),
			slog.Duration("retry-after", duration))
	})
}

func (m *Provider[T]) watch() error {
	w, err := m.kubernetes.CoreV1().ConfigMaps(m.namespace).Watch(
		m.ctx,
		metav1.SingleObject(metav1.ObjectMeta{Name: m.name, Namespace: m.namespace}),
	)
	if err != nil {
		return err
	}
	defer w.Stop()

	for res := range w.ResultChan() {
		if res.Type == k8swatch.Error {
			return errors.Errorf("watch error: %v", res.Object)
		}
		if res.Type != k8swatch.Added && res.Type != k8swatch.Modified {
			continue
		}
		value, _, err := m.Get()
		if err != nil {
			return err
		}
		m.watcher.Publish(value)
	}

	if m.ctx.Err() != nil {
		return nil
	}
	return errors.New("K8S config map watch channel closed")
}

func makeDesiredConfigMap(name, dataKey string, data []byte, version provider.Version) *corev1.ConfigMap {
	cm := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Data: map[string]string{
			dataKey: string(data),
		},
	}

	if version != provider.NotExists {
		cm.ResourceVersion = string(version)
	}

	return cm
}
