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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/oxia-db/oxia/common/failpoint"
	"github.com/oxia-db/oxia/common/metric"
	commonproto "github.com/oxia-db/oxia/common/proto"
	metadatacommon "github.com/oxia-db/oxia/oxiad/common/metadata"
	metadatacodec "github.com/oxia-db/oxia/oxiad/common/metadata/codec"
	metadataprovider "github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
)

func TestProviderUsesConfiguredCoordinatorName(t *testing.T) {
	p := NewProvider(
		t.Context(),
		fake.NewSimpleClientset(),
		"ns",
		"status",
		metadatacodec.ClusterStatusCodec,
		metadatacommon.WatchDisabled,
		"coordinator-0",
	)
	t.Cleanup(func() {
		require.NoError(t, p.Close())
	})

	provider := p.(*Provider[*commonproto.ClusterStatus])
	require.Equal(t, "coordinator-0", provider.name)
}

func TestProviderWaitToBecomeLeaderRejectsEmptyCoordinatorName(t *testing.T) {
	p := NewProvider(
		t.Context(),
		fake.NewSimpleClientset(),
		"ns",
		"status",
		metadatacodec.ClusterStatusCodec,
		metadatacommon.WatchDisabled,
		" ",
	)
	t.Cleanup(func() {
		require.NoError(t, p.Close())
	})

	_, err := p.WaitToBecomeLeader()
	require.ErrorContains(t, err, "failed to create the leader elector")
}

func TestProviderGetLeaderNameUsesLeaderElector(t *testing.T) {
	kc := fake.NewSimpleClientset()

	p := NewProvider(
		t.Context(),
		kc,
		"ns",
		"status",
		metadatacodec.ClusterStatusCodec,
		metadatacommon.WatchDisabled,
		"coordinator-0",
	)
	t.Cleanup(func() {
		require.NoError(t, p.Close())
	})

	_, err := p.WaitToBecomeLeader()
	require.NoError(t, err)

	leader, err := p.GetLeaderName()
	require.NoError(t, err)
	require.Equal(t, "coordinator-0", leader)
}

func TestProviderGetLeaderNameUnavailable(t *testing.T) {
	p := NewProvider(
		t.Context(),
		fake.NewSimpleClientset(),
		"ns",
		"status",
		metadatacodec.ClusterStatusCodec,
		metadatacommon.WatchDisabled,
		"coordinator-0",
	)
	t.Cleanup(func() {
		require.NoError(t, p.Close())
	})

	_, err := p.GetLeaderName()
	require.True(t, errors.Is(err, metadataprovider.ErrCoordinatorLeaderUnavailable))
}

// The operators manage the config map of the configuration, e.g. with Helm.
// kubectl replace --force, and kubectl delete followed by kubectl apply, delete
// it before they create it again, and an edit can remove its key. A reload in
// between waits for the document to be restored: an empty configuration would
// drain every data server and delete every namespace.
func TestProviderReloadKeepsLoadedDocumentUntilConfigMapIsRestored(t *testing.T) {
	kc := fake.NewSimpleClientset()
	configMaps := kc.CoreV1().ConfigMaps("ns")
	_, err := configMaps.Create(t.Context(), newConfigMap(t, 1), metav1.CreateOptions{})
	require.NoError(t, err)
	p := newConfigProvider(t, kc)
	requireServers(t, p, 1)

	require.NoError(t, configMaps.Delete(t.Context(), "config", metav1.DeleteOptions{}))
	recreated := newConfigMap(t, 2)
	restoreLater(t, func() error {
		_, err := configMaps.Create(t.Context(), recreated, metav1.CreateOptions{})
		return err
	})
	require.NoError(t, p.Reload())
	requireServers(t, p, 2)

	_, err = configMaps.Update(t.Context(), &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "config"}},
		metav1.UpdateOptions{})
	require.NoError(t, err)
	restored := newConfigMap(t, 3)
	restoreLater(t, func() error {
		_, err := configMaps.Update(t.Context(), restored, metav1.UpdateOptions{})
		return err
	})
	require.NoError(t, p.Reload())
	requireServers(t, p, 3)
}

// A config map that the coordinator created, e.g. from its initial cluster
// configuration, is kept in the same way.
func TestProviderReloadKeepsStoredDocumentUntilConfigMapIsRestored(t *testing.T) {
	kc := fake.NewSimpleClientset()
	p := newConfigProvider(t, kc)
	_, err := p.Store(metadataprovider.Versioned[*commonproto.ClusterConfiguration]{
		Value:   newConfig(1),
		Version: metadatacommon.NotExists,
	})
	require.NoError(t, err)

	configMaps := kc.CoreV1().ConfigMaps("ns")
	require.NoError(t, configMaps.Delete(t.Context(), "config", metav1.DeleteOptions{}))
	recreated := newConfigMap(t, 2)
	restoreLater(t, func() error {
		_, err := configMaps.Create(t.Context(), recreated, metav1.CreateOptions{})
		return err
	})
	require.NoError(t, p.Reload())
	requireServers(t, p, 2)
}

func TestNewDefaultClientsetUsesKubeconfig(t *testing.T) {
	kubeconfig := filepath.Join(t.TempDir(), "kubeconfig")
	require.NoError(t, os.WriteFile(kubeconfig, []byte(`apiVersion: v1
kind: Config
clusters:
- name: test
  cluster:
    server: https://127.0.0.1:1
contexts:
- name: test
  context:
    cluster: test
current-context: test
`), 0o600))
	t.Setenv("KUBECONFIG", kubeconfig)

	kc, err := NewDefaultClientset()
	require.NoError(t, err)
	assert.NotNil(t, kc)
}

func TestNewDefaultClientsetFailsWithoutCluster(t *testing.T) {
	kubeconfig := filepath.Join(t.TempDir(), "kubeconfig")
	require.NoError(t, os.WriteFile(kubeconfig, []byte("apiVersion: v1\nkind: Config\n"), 0o600))
	t.Setenv("KUBECONFIG", kubeconfig)
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")

	_, err := NewDefaultClientset()
	require.Error(t, err)
}

func TestProviderLoadReturnsGetError(t *testing.T) {
	kc := fake.NewSimpleClientset()
	errGet := errors.New("get failure")
	kc.PrependReactor("get", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errGet
	})
	p := newConfigProvider(t, kc)

	_, err := p.Load()
	require.ErrorIs(t, err, errGet)
}

// A document that can't be decoded must stop the coordinator, rather than be
// served or overwritten.
func TestProviderLoadPanicsOnUndecodableDocument(t *testing.T) {
	kc := fake.NewSimpleClientset()
	cm := makeDesiredConfigMap("config", metadatacodec.ClusterConfigCodec.GetKey(), []byte("servers: [unterminated"),
		metadatacommon.NotExists)
	_, err := kc.CoreV1().ConfigMaps("ns").Create(t.Context(), cm, metav1.CreateOptions{})
	require.NoError(t, err)
	p := newConfigProvider(t, kc)

	assert.Panics(t, func() { _, _ = p.Load() })
}

func TestProviderReportsMetadataSize(t *testing.T) {
	previous := metric.GetMeter()
	reader := sdkmetric.NewManualReader()
	metric.SetMeter(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"))
	defer metric.SetMeter(previous)
	data, err := metadatacodec.ClusterConfigCodec.MarshalYAML(newConfig(2))
	require.NoError(t, err)
	kc := fake.NewSimpleClientset()
	_, err = kc.CoreV1().ConfigMaps("ns").Create(t.Context(), newConfigMap(t, 2), metav1.CreateOptions{})
	require.NoError(t, err)
	p := newConfigProvider(t, kc)

	requireServers(t, p, 2)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	var sizes []int64
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "oxia_coordinator_metadata_size" {
				continue
			}
			gauge, ok := m.Data.(metricdata.Gauge[int64])
			require.True(t, ok, "unexpected data: %#v", m.Data)
			for _, dp := range gauge.DataPoints {
				sizes = append(sizes, dp.Value)
			}
		}
	}
	assert.Equal(t, []int64{int64(len(data))}, sizes)
}

func TestProviderSubscribeNotifiesStores(t *testing.T) {
	p := newConfigProvider(t, fake.NewSimpleClientset())
	subscription := p.Subscribe()
	defer subscription.Close()

	_, err := p.Store(metadataprovider.Versioned[*commonproto.ClusterConfiguration]{
		Value:   newConfig(1),
		Version: metadatacommon.NotExists,
	})
	require.NoError(t, err)

	select {
	case <-subscription.Changed():
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the subscription was not notified of the store")
	}
	stored, err := subscription.Get()
	require.NoError(t, err)
	require.Len(t, stored.Value.GetServers(), 1)
}

func TestProviderStoreFailsWhenDocumentCannotBeEncoded(t *testing.T) {
	kc := fake.NewSimpleClientset()
	encodeErr := errors.New("encode failed")
	p := NewProvider(t.Context(), kc, "ns", "config", &fakeCodec{
		Codec:       metadatacodec.ClusterConfigCodec,
		marshalYAML: func(*commonproto.ClusterConfiguration) ([]byte, error) { return nil, encodeErr },
	}, metadatacommon.WatchDisabled, "coordinator-0")
	t.Cleanup(func() {
		require.NoError(t, p.Close())
	})

	_, err := p.Store(metadataprovider.Versioned[*commonproto.ClusterConfiguration]{
		Value:   newConfig(1),
		Version: metadatacommon.NotExists,
	})
	require.ErrorIs(t, err, encodeErr)
	configMaps, err := kc.CoreV1().ConfigMaps("ns").List(t.Context(), metav1.ListOptions{})
	require.NoError(t, err)
	assert.Empty(t, configMaps.Items)
}

func TestProviderStoreFailsToSerializeConfigMap(t *testing.T) {
	if !failpoint.Enabled {
		t.Skip("needs failpoints: disabled by the disable_trap tag")
	}
	kc := fake.NewSimpleClientset()
	failpointErr := errors.New("failpoint")
	p := newProvider(t.Context(), kc, "ns", "config", metadatacodec.ClusterConfigCodec, metadatacommon.WatchDisabled,
		"coordinator-0", failpoint.New(map[string]func() error{
			"store.marshal": func() error { return failpointErr },
		}))
	t.Cleanup(func() {
		require.NoError(t, p.Close())
	})

	_, err := p.Store(metadataprovider.Versioned[*commonproto.ClusterConfiguration]{
		Value:   newConfig(1),
		Version: metadatacommon.NotExists,
	})
	require.ErrorIs(t, err, failpointErr)
	configMaps, err := kc.CoreV1().ConfigMaps("ns").List(t.Context(), metav1.ListOptions{})
	require.NoError(t, err)
	assert.Empty(t, configMaps.Items)
}

// A write that creates the config map fails with ErrBadVersion when another
// coordinator created it first.
func TestProviderStoreRejectsCreatingExistingConfigMap(t *testing.T) {
	kc := fake.NewSimpleClientset()
	p := newConfigProvider(t, kc)
	_, err := kc.CoreV1().ConfigMaps("ns").Create(t.Context(), newConfigMap(t, 1), metav1.CreateOptions{})
	require.NoError(t, err)

	_, err = p.Store(metadataprovider.Versioned[*commonproto.ClusterConfiguration]{
		Value:   newConfig(2),
		Version: metadatacommon.NotExists,
	})
	require.ErrorIs(t, err, metadatacommon.ErrBadVersion)
}

func TestProviderWaitToBecomeLeaderSignalsLeaseLoss(t *testing.T) {
	kc := fake.NewSimpleClientset()
	var failUpdates atomic.Bool
	kc.PrependReactor("update", "leases", func(k8stesting.Action) (bool, runtime.Object, error) {
		if !failUpdates.Load() {
			return false, nil, nil
		}
		return true, nil, errors.New("lease update failure")
	})
	p := NewProvider(t.Context(), kc, "ns", "status", metadatacodec.ClusterStatusCodec,
		metadatacommon.WatchDisabled, "coordinator-0")
	t.Cleanup(func() {
		require.NoError(t, p.Close())
	})
	lost, err := p.WaitToBecomeLeader()
	require.NoError(t, err)

	failUpdates.Store(true)

	select {
	case <-lost:
	case <-time.After(renewDeadline + 10*time.Second):
		require.FailNow(t, "the lease loss was not signaled")
	}
}

func TestProviderWaitToBecomeLeaderObservesOtherLeader(t *testing.T) {
	now := metav1.NewMicroTime(time.Now())
	holder := "coordinator-1"
	leaseSeconds := int32(leaseDuration / time.Second)
	kc := fake.NewSimpleClientset(&coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: "config", Namespace: "ns"},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &holder,
			LeaseDurationSeconds: &leaseSeconds,
			AcquireTime:          &now,
			RenewTime:            &now,
		},
	})
	p := NewProvider(t.Context(), kc, "ns", "config", metadatacodec.ClusterConfigCodec,
		metadatacommon.WatchDisabled, "coordinator-0")

	waited := make(chan error, 1)
	go func() {
		_, err := p.WaitToBecomeLeader()
		waited <- err
	}()

	require.Eventually(t, func() bool {
		leader, err := p.GetLeaderName()
		return err == nil && leader == holder
	}, 10*time.Second, 10*time.Millisecond)

	// The lease is held by the other coordinator: closing ends the wait
	require.NoError(t, p.Close())
	select {
	case err := <-waited:
		require.Error(t, err)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "closing did not end the wait for the leadership")
	}
}

func newConfigProvider(t *testing.T, kc *fake.Clientset) metadataprovider.Provider[*commonproto.ClusterConfiguration] {
	t.Helper()

	p := NewProvider(
		t.Context(),
		kc,
		"ns",
		"config",
		metadatacodec.ClusterConfigCodec,
		metadatacommon.WatchDisabled,
		"coordinator-0",
	)
	t.Cleanup(func() {
		require.NoError(t, p.Close())
	})
	return p
}

// restoreLater calls restore from another goroutine, after a delay: a reload
// that the caller starts right away first finds the config map as the caller
// left it, deleted or without its key.
func restoreLater(t *testing.T, restore func() error) {
	t.Helper()

	restored := make(chan error, 1)
	go func() {
		time.Sleep(100 * time.Millisecond)
		restored <- restore()
	}()
	t.Cleanup(func() {
		assert.NoError(t, <-restored)
	})
}

// newConfigMap returns the config map of a configuration with the given number
// of servers.
func newConfigMap(t *testing.T, servers int) *corev1.ConfigMap {
	t.Helper()

	data, err := metadatacodec.ClusterConfigCodec.MarshalYAML(newConfig(servers))
	require.NoError(t, err)
	return makeDesiredConfigMap("config", metadatacodec.ClusterConfigCodec.GetKey(), data, metadatacommon.NotExists)
}

func newConfig(servers int) *commonproto.ClusterConfiguration {
	config := &commonproto.ClusterConfiguration{}
	for i := range servers {
		config.Servers = append(config.Servers, &commonproto.DataServerIdentity{
			Public:   fmt.Sprintf("server-%d:6648", i),
			Internal: fmt.Sprintf("server-%d:6649", i),
		})
	}
	return config
}

func requireServers(t *testing.T, p metadataprovider.Provider[*commonproto.ClusterConfiguration], servers int) {
	t.Helper()

	loaded, err := p.Load()
	require.NoError(t, err)
	require.Len(t, loaded.Value.GetServers(), servers)
}

type fakeCodec struct {
	metadatacodec.Codec[*commonproto.ClusterConfiguration]
	marshalYAML func(value *commonproto.ClusterConfiguration) ([]byte, error)
}

func (c *fakeCodec) MarshalYAML(value *commonproto.ClusterConfiguration) ([]byte, error) {
	return c.marshalYAML(value)
}
