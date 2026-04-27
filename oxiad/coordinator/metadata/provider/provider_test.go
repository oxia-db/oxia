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

package provider_test

import (
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gproto "google.golang.org/protobuf/proto"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/file"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/kubernetes"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/memory"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/raft"

	"github.com/oxia-db/oxia/common/proto"
)

var (
	newFake = func() *fake.Clientset {
		f := fake.NewSimpleClientset()
		f.PrependReactor("*", "*", k8sResourceVersionSupport(f.Tracker()))
		return f
	}
	providers = map[string]func(t *testing.T) provider.Provider[*proto.ClusterStatus]{
		"memory": func(t *testing.T) provider.Provider[*proto.ClusterStatus] {
			t.Helper()

			return memory.NewProvider(provider.ClusterStatusCodec)
		},
		"file": func(t *testing.T) provider.Provider[*proto.ClusterStatus] {
			t.Helper()

			p, err := file.NewProvider(t.Context(), filepath.Join(t.TempDir(), "metadata"), provider.ClusterStatusCodec, provider.WatchDisabled)
			assert.NoError(t, err)
			return p
		},
		"configmap": func(t *testing.T) provider.Provider[*proto.ClusterStatus] {
			t.Helper()

			p, err := kubernetes.NewConfigMapProvider(t.Context(), newFake(), "ns", "n", provider.ClusterStatusCodec, provider.WatchDisabled)
			assert.NoError(t, err)
			return p
		},
		"raft": func(t *testing.T) provider.Provider[*proto.ClusterStatus] {
			t.Helper()

			addr := freeAddress(t)
			p, err := raft.NewProvider(addr, []string{addr}, filepath.Join(t.TempDir(), "raft"))
			assert.NoError(t, err)
			assert.NoError(t, p.WaitToBecomeLeader())
			return p
		},
	}
)

func TestProvider(t *testing.T) {
	for name, newProvider := range providers {
		t.Run(name, func(t *testing.T) {
			m := newProvider(t)

			res, version, err := m.Get()
			assert.NoError(t, err)
			assert.Equal(t, provider.NotExists, version)
			assert.Nil(t, res)

			status := &proto.ClusterStatus{
				Namespaces: map[string]*proto.NamespaceStatus{},
			}

			assert.PanicsWithError(t, provider.ErrBadVersion.Error(), func() {
				_, err := m.Store(status, "")
				assert.NoError(t, err)
			})

			newVersion, err := m.Store(status, provider.NotExists)
			assert.NoError(t, err)
			assert.EqualValues(t, provider.Version("0"), newVersion)

			res, version, err = m.Get()
			assert.NoError(t, err)
			assert.EqualValues(t, provider.Version("0"), version)
			assert.True(t, gproto.Equal(&proto.ClusterStatus{
				Namespaces: map[string]*proto.NamespaceStatus{},
			}, res))

			assert.NoError(t, m.Close())
		})
	}
}

func TestResourceTypeUnmarshalLegacyClusterStatus(t *testing.T) {
	status := &proto.ClusterStatus{
		Namespaces: map[string]*proto.NamespaceStatus{},
		InstanceId: "legacy-instance",
	}
	data, err := proto.MarshalClusterStatusJSON(status)
	require.NoError(t, err)

	value, err := provider.ResourceStatus.Unmarshal([]byte(fmt.Sprintf(`{"clusterStatus":%s,"version":"2"}`, data)))
	require.NoError(t, err)
	typedStatus, ok := value.(*proto.ClusterStatus)
	require.True(t, ok)
	assert.Equal(t, "legacy-instance", typedStatus.GetInstanceId())
	assert.True(t, gproto.Equal(status, typedStatus))
}

func TestProviderConfigResource(t *testing.T) {
	providers := map[string]func(t *testing.T) provider.Provider[*proto.ClusterConfiguration]{
		"memory": func(t *testing.T) provider.Provider[*proto.ClusterConfiguration] {
			t.Helper()

			return memory.NewProvider(provider.ClusterConfigCodec)
		},
		"file": func(t *testing.T) provider.Provider[*proto.ClusterConfiguration] {
			t.Helper()

			p, err := file.NewProvider(t.Context(), filepath.Join(t.TempDir(), "cluster.yaml"), provider.ClusterConfigCodec, provider.WatchDisabled)
			assert.NoError(t, err)
			return p
		},
		"configmap": func(t *testing.T) provider.Provider[*proto.ClusterConfiguration] {
			t.Helper()

			p, err := kubernetes.NewConfigMapProvider(t.Context(), newFake(), "ns", "config", provider.ClusterConfigCodec, provider.WatchDisabled)
			assert.NoError(t, err)
			return p
		},
		"raft": func(t *testing.T) provider.Provider[*proto.ClusterConfiguration] {
			t.Helper()

			addr := freeAddress(t)
			statusProvider, configProvider, err := raft.NewProviders(addr, []string{addr}, filepath.Join(t.TempDir(), "raft"))
			assert.NoError(t, err)
			assert.NoError(t, statusProvider.WaitToBecomeLeader())
			return configProvider
		},
	}

	for name, newProvider := range providers {
		t.Run(name, func(t *testing.T) {
			m := newProvider(t)
			config := &proto.ClusterConfiguration{
				Namespaces: []*proto.Namespace{{
					Name:              "default",
					ReplicationFactor: 1,
					InitialShardCount: 1,
				}},
				Servers: []*proto.DataServerIdentity{{
					Public:   "s1:9091",
					Internal: "s1:8191",
				}},
			}

			newVersion, err := m.Store(config, provider.NotExists)
			assert.NoError(t, err)
			assert.EqualValues(t, provider.Version("0"), newVersion)

			res, version, err := m.Get()
			assert.NoError(t, err)
			assert.EqualValues(t, provider.Version("0"), version)
			assert.True(t, gproto.Equal(config, res))

			assert.NoError(t, m.Close())
		})
	}
}

func freeAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	addr := listener.Addr().String()
	assert.NoError(t, listener.Close())
	return addr
}

func k8sResourceVersionSupport(tracker k8stesting.ObjectTracker) k8stesting.ReactionFunc {
	return func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		namespace := action.GetNamespace()
		gvr := action.GetResource()

		switch action := action.(type) {
		case k8stesting.CreateActionImpl:
			objMeta := accessor(action.GetObject())
			objMeta.SetResourceVersion("0")
			return false, action.GetObject(), nil
		case k8stesting.UpdateActionImpl:
			objMeta := accessor(action.GetObject())
			existing, err := tracker.Get(gvr, namespace, objMeta.GetName())
			if err != nil {
				// Match the in-package fake client helper behavior for update misses.
				//nolint:nilerr
				return false, action.GetObject(), nil
			}

			existingObjMeta := accessor(existing)
			if objMeta.GetResourceVersion() != existingObjMeta.GetResourceVersion() {
				return true, action.GetObject(), k8serrors.NewConflict(gvr.GroupResource(), objMeta.GetName(), errors.New("conflict"))
			}

			incrementVersion(objMeta)
			return false, action.GetObject(), nil
		}

		return false, nil, nil
	}
}

func accessor(obj runtime.Object) metav1.Object {
	objMeta, err := meta.Accessor(obj)
	if err != nil {
		panic(err)
	}

	return objMeta
}

func incrementVersion(metaObj metav1.Object) {
	i, err := strconv.ParseUint(metaObj.GetResourceVersion(), 10, 64)
	if err != nil {
		panic(err)
	}

	metaObj.SetResourceVersion(strconv.FormatUint(i+1, 10))
}
