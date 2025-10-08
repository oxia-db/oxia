// Copyright 2023 StreamNative, Inc.
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
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/coordinator/model"
	"golang.org/x/net/context"
	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8s "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"
)

var _ Provider = &metadataProviderConfigMap{}

type metadataProviderConfigMap struct {
	sync.Mutex
	kubernetes      k8s.Interface
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

func NewMetadataProviderConfigMap(kc k8s.Interface, namespace, name string) Provider {
	m := &metadataProviderConfigMap{
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

func (m *metadataProviderConfigMap) Get() (status *model.ClusterStatus, version Version, err error) {
	timer := m.getLatencyHisto.Timer()
	defer timer.Done()

	m.Lock()
	defer m.Unlock()
	return m.getWithoutLock()
}

func (m *metadataProviderConfigMap) getWithoutLock() (*model.ClusterStatus, Version, error) {
	cm, err := K8SConfigMaps(m.kubernetes).Get(m.namespace, m.name)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil, NotExists, nil
		}
		return nil, "", err
	}

	status := &model.ClusterStatus{}
	data := []byte(cm.Data["status"])
	if err = yaml.Unmarshal(data, status); err != nil {
		return nil, "", err
	}

	version := Version(cm.ResourceVersion)
	slog.Debug("Get metadata successful",
		slog.String("version", cm.ResourceVersion))
	m.metadataSize.Store(int64(len(data)))
	return status, version, nil
}

func (m *metadataProviderConfigMap) Store(status *model.ClusterStatus, expectedVersion Version) (Version, error) {
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
		panic(ErrMetadataBadVersion)
	}

	data := configMap(m.name, status, expectedVersion)
	cm, err := K8SConfigMaps(m.kubernetes).Upsert(m.namespace, m.name, data)
	if k8serrors.IsConflict(err) {
		panic(err)
	}
	version = Version(cm.ResourceVersion)
	m.metadataSize.Store(int64(len(data.Data["status"])))
	return version, nil
}

func (m *metadataProviderConfigMap) WaitToBecomeLeader() error {
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
		LeaseDuration:   15 * time.Second,
		RenewDeadline:   10 * time.Second,
		RetryPeriod:     2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				log.Info("Started leading - lease acquired")
				wg.Done()
			},
			OnStoppedLeading: func() {
				log.Warn("Stopped leading - lease lost!")
			},
			OnNewLeader: func(identity string) {
				if identity == identity {
					return
				}

				log.Info("New leader elected", slog.String("leader", identity))
			},
		},
	}

	// Start leader election
	leaderElector, err := leaderelection.NewLeaderElector(leaderElectionConfig)
	if err != nil {
		panic(err)
	}

	go process.DoWithLabels(m.ctx, map[string]string{
		"component":     "metadata-provider",
		"sub-component": "k8s-leader-elector",
	}, func() {
		leaderElector.Run(m.ctx)
		close(m.closeCh)
	})

	return wg.Wait(m.ctx)
}

func (m *metadataProviderConfigMap) Close() error {
	m.cancel()
	<-m.closeCh
	m.log.Info("Closed metadata provider")
	return nil
}

func configMap(name string, status *model.ClusterStatus, version Version) *corev1.ConfigMap {
	bytes, err := yaml.Marshal(status)
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

	if version != NotExists {
		cm.ResourceVersion = string(version)
	}

	return cm
}
