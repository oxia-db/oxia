// Copyright 2023-2026 The Oxia Authors
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
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oxia-db/oxia/oxiad/common/errs"

	"github.com/pkg/errors"
	gproto "google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/failpoint"
	"github.com/oxia-db/oxia/common/metric"
	"github.com/oxia-db/oxia/common/process"
	commonproto "github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/cache"
	"github.com/oxia-db/oxia/oxiad/common/k8s"
	metadatacommon "github.com/oxia-db/oxia/oxiad/common/metadata"
	metadatacodec "github.com/oxia-db/oxia/oxiad/common/metadata/codec"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
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
	mu            sync.Mutex
	kubernetes    kubernetes.Interface
	namespace     string
	configMapName string
	codec         metadatacodec.Codec[T]
	watchEnabled  metadatacommon.WatchMode
	name          string
	leaderElector atomic.Pointer[leaderelection.LeaderElector]
	// hasDocument is set once this provider read the document from the config
	// map, or wrote it, and is never reset.
	hasDocument atomic.Bool

	metadataSize      atomic.Int64
	getLatencyHisto   metric.LatencyHistogram
	storeLatencyHisto metric.LatencyHistogram
	metadataSizeGauge metric.Gauge

	ctx       context.Context
	ctxCancel context.CancelFunc
	wg        sync.WaitGroup

	cache *cache.Cache[provider.Versioned[T]]

	logger     *slog.Logger
	failpoints *failpoint.Failpoints
}

func NewProvider[T gproto.Message](
	ctx context.Context,
	kc kubernetes.Interface,
	namespace, configMapName string,
	codec metadatacodec.Codec[T],
	watchEnabled metadatacommon.WatchMode,
	name string,
) provider.Provider[T] {
	return newProvider(ctx, kc, namespace, configMapName, codec, watchEnabled, name, nil)
}

func newProvider[T gproto.Message](
	ctx context.Context,
	kc kubernetes.Interface,
	namespace, configMapName string,
	codec metadatacodec.Codec[T],
	watchEnabled metadatacommon.WatchMode,
	name string,
	failpoints *failpoint.Failpoints,
) *Provider[T] {
	m := &Provider[T]{
		failpoints:    failpoints,
		kubernetes:    kc,
		namespace:     namespace,
		configMapName: configMapName,
		codec:         codec,
		watchEnabled:  watchEnabled,
		name:          strings.TrimSpace(name),
		logger:        slog.With("component", "metadata-config-map"),

		getLatencyHisto: metric.NewLatencyHistogram("oxia_coordinator_metadata_get_latency",
			"Latency for reading coordinator metadata", nil),
		storeLatencyHisto: metric.NewLatencyHistogram("oxia_coordinator_metadata_store_latency",
			"Latency for storing coordinator metadata", nil),
	}

	m.ctx, m.ctxCancel = context.WithCancel(ctx)
	var watch cache.WatchFunc
	if watchEnabled.Enabled() {
		watch = func(ctx context.Context) (<-chan struct{}, error) {
			return k8s.WatchConfigMap(ctx, m.kubernetes, m.namespace, m.configMapName)
		}
	}
	m.cache = cache.New(m.ctx, m.load, watch)

	m.metadataSizeGauge = metric.NewGauge("oxia_coordinator_metadata_size",
		"The size of the coordinator metadata", metric.Bytes, nil, func() int64 {
			return m.metadataSize.Load()
		})
	return m
}

func NewDefaultClientset() (kubernetes.Interface, error) {
	kubeconfigGetter := clientcmd.NewDefaultClientConfigLoadingRules().Load
	config, err := clientcmd.BuildConfigFromKubeconfigGetter("", kubeconfigGetter)
	if err != nil {
		return nil, err
	}
	config.QPS = -1
	return kubernetes.NewForConfig(config)
}

// load reads the config map.
func (m *Provider[T]) load(ctx context.Context) (*provider.Versioned[T], error) {
	timer := m.getLatencyHisto.Timer()
	defer timer.Done()

	cm, err := m.kubernetes.CoreV1().ConfigMaps(m.namespace).Get(ctx, m.configMapName, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return m.loadMissingDocument()
		}
		return nil, err
	}

	data, ok := cm.Data[m.codec.GetKey()]
	if !ok {
		return m.loadMissingDocument()
	}

	slog.Debug("Get metadata successful",
		slog.String("version", cm.ResourceVersion))
	m.metadataSize.Store(int64(len(data)))
	value, err := m.codec.UnmarshalYAML([]byte(data))
	if err != nil {
		panic(err)
	}
	m.hasDocument.Store(true)
	return &provider.Versioned[T]{
		Value:   value,
		Version: metadatacommon.Version(cm.ResourceVersion),
	}, nil
}

// loadMissingDocument loads a config map that doesn't exist, or doesn't hold
// the document. There is no document until the first write creates it. Once
// this provider read or wrote the document, though, it was deleted: kubectl
// replace --force, and kubectl delete followed by kubectl apply, delete the
// config map before they create it again, and an edit can remove its key.
// Loading an empty document would drain every data server and delete every
// namespace, so the load fails, and a reload keeps the cached document until
// the config map holds it again.
func (m *Provider[T]) loadMissingDocument() (*provider.Versioned[T], error) {
	if m.hasDocument.Load() {
		return nil, fmt.Errorf("config map %s/%s no longer holds %s, waiting for it to be restored",
			m.namespace, m.configMapName, m.codec.GetKey())
	}
	return &provider.Versioned[T]{
		Value:   m.codec.NewZero(),
		Version: metadatacommon.NotExists,
	}, nil
}

func (m *Provider[T]) write(snapshot provider.Versioned[T]) (*provider.Versioned[T], error) {
	timer := m.storeLatencyHisto.Timer()
	defer timer.Done()

	m.mu.Lock()
	defer m.mu.Unlock()

	data, err := m.codec.MarshalYAML(snapshot.Value)
	if err != nil {
		return nil, err
	}
	cmData := makeDesiredConfigMap(m.configMapName, m.codec.GetKey(), data, snapshot.Version)
	desiredBytes, err := json.Marshal(cmData)
	if err = m.failpoints.Fail("store.marshal", err); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(m.ctx, k8sRequestTimeout)
	defer cancel()

	var cm *corev1.ConfigMap
	if snapshot.Version == metadatacommon.NotExists {
		cm, err = m.kubernetes.CoreV1().ConfigMaps(m.namespace).Create(ctx, cmData, metav1.CreateOptions{})
		if k8serrors.IsAlreadyExists(err) {
			return nil, errs.ErrBadVersion
		}
	} else {
		if snapshot.Version == "" {
			return nil, errs.ErrBadVersion
		}
		cm, err = m.kubernetes.CoreV1().ConfigMaps(m.namespace).Patch(ctx, m.configMapName, types.ApplyPatchType, desiredBytes, metav1.PatchOptions{
			FieldManager: fieldManager,
			Force:        gproto.Bool(true),
		})
		if k8serrors.IsNotFound(err) || k8serrors.IsConflict(err) {
			return nil, errs.ErrBadVersion
		}
	}
	if err != nil {
		return nil, err
	}
	version := metadatacommon.Version(cm.ResourceVersion)
	m.metadataSize.Store(int64(len(cmData.Data[m.codec.GetKey()])))
	m.hasDocument.Store(true)
	return &provider.Versioned[T]{
		Value:   m.codec.Clone(snapshot.Value),
		Version: version,
	}, nil
}

func (m *Provider[T]) WaitToBecomeLeader() (<-chan struct{}, error) {
	myIdentity := m.name

	// Create a lease lock
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      m.configMapName,
			Namespace: m.namespace,
		},
		Client: m.kubernetes.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: myIdentity,
		},
	}

	logger := m.logger.With(
		slog.String("name", m.name))
	wg := concurrent.NewWaitGroup(1)
	lost := make(chan struct{})

	// Configure leader election
	leaderElectionConfig := leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   leaseDuration,
		RenewDeadline:   renewDeadline,
		RetryPeriod:     retryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(_ context.Context) {
				logger.Info("Started leading - lease acquired")
				wg.Done()
			},
			OnStoppedLeading: func() {
				// The elector also stops when the provider shuts down: only
				// signal the loss when the lease actually got lost
				select {
				case <-m.ctx.Done():
				default:
					logger.Warn("Stopped leading - lease lost!")
					close(lost)
				}
			},
			OnNewLeader: func(newLeader string) {
				if newLeader == myIdentity {
					return
				}

				logger.Info("New leader elected", slog.String("leader", newLeader))
			},
		},
	}

	// Start leader election
	// The elector rejects an empty coordinator name
	leaderElector, err := leaderelection.NewLeaderElector(leaderElectionConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create the leader elector")
	}
	m.leaderElector.Store(leaderElector)

	m.wg.Go(func() {
		process.DoWithLabels(m.ctx, map[string]string{
			"component":     "metadata-provider",
			"sub-component": "k8s-leader-elector",
		}, func() {
			leaderElector.Run(m.ctx)
		})
	})

	return lost, wg.Wait(m.ctx)
}

func (m *Provider[T]) GetLeaderName() (string, error) {
	if leaderElector := m.leaderElector.Load(); leaderElector != nil {
		leader := leaderElector.GetLeader()
		if leader != "" {
			return leader, nil
		}
	}
	return "", errors.Wrap(errs.ErrUnavailable, "coordinator leader")
}

func (m *Provider[T]) Close() error {
	m.ctxCancel()
	m.wg.Wait()
	_ = m.cache.Close()
	m.logger.Info("Closed metadata provider")
	return nil
}

func makeDesiredConfigMap(name, dataKey string, data []byte, version metadatacommon.Version) *corev1.ConfigMap {
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

	if version != metadatacommon.NotExists {
		cm.ResourceVersion = string(version)
	}

	return cm
}

func (m *Provider[T]) Load() (*provider.Versioned[T], error) {
	return m.cache.Get()
}

func (m *Provider[T]) Subscribe() *cache.Subscription[provider.Versioned[T]] {
	return m.cache.Subscribe()
}

// Store writes the snapshot through the cache, so that no load of the cache
// stores a snapshot read before the write. When the write fails, which it may
// do after being applied, the next Load or Store reads the stored snapshot.
func (m *Provider[T]) Store(snapshot provider.Versioned[T]) (metadatacommon.Version, error) {
	stored, err := m.cache.Compute(func(*provider.Versioned[T]) (*provider.Versioned[T], error) {
		return m.write(snapshot)
	})
	if err != nil {
		return metadatacommon.NotExists, err
	}
	return stored.Version, nil
}

func (m *Provider[T]) Reload() error {
	return m.cache.Reload()
}
