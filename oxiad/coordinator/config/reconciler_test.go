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

package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8s "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	commonproto "github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/option"
)

const clusterConfigWithCamelCaseFields = `
namespaces:
  - name: default
    initialShardCount: 1
    replicationFactor: 1
servers:
  - public: oxia-0.oxia-svc.oxia.svc.cluster.local:6648
    internal: oxia-0.oxia-svc:6649
allowExtraAuthorities:
  - oxia.oxia.svc.cluster.local:6648
`

const updatedClusterConfig = `
namespaces:
  - name: default
    initialShardCount: 2
    replicationFactor: 1
servers:
  - public: oxia-0.oxia-svc.oxia.svc.cluster.local:6648
    internal: oxia-0.oxia-svc:6649
allowExtraAuthorities:
  - oxia.oxia.svc.cluster.local:6648
`

type recordingHandler struct {
	configs []*commonproto.ClusterConfiguration
}

func (h *recordingHandler) ReconcileClusterConfig(config *commonproto.ClusterConfiguration) error {
	h.configs = append(h.configs, config)
	return nil
}

func TestReconcilerHandlesFileSourceEventBeforeCoordinator(t *testing.T) {
	configFile := filepath.Join(t.TempDir(), "cluster.yaml")
	require.NoError(t, os.WriteFile(configFile, []byte(clusterConfigWithCamelCaseFields), 0o600))

	reconciler, err := NewReconciler(t.Context(), &option.ClusterOptions{
		ConfigPath: configFile,
	})
	require.NoError(t, err)

	handler := &recordingHandler{}
	require.NoError(t, reconciler.reconcile(t.Context(), handler))
	require.Empty(t, handler.configs)

	require.NoError(t, os.WriteFile(configFile, []byte(updatedClusterConfig), 0o600))
	require.NoError(t, reconciler.reconcile(t.Context(), handler))
	require.Len(t, handler.configs, 1)
	require.Equal(t, uint32(2), handler.configs[0].GetNamespaces()[0].GetInitialShardCount())

	config, err := reconciler.Load()
	require.NoError(t, err)
	require.Equal(t, uint32(2), config.GetNamespaces()[0].GetInitialShardCount())
}

func TestLoadClusterConfigurationFromConfigMapPreservesCamelCaseFields(t *testing.T) {
	k8sClient := fake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "oxia",
			Name:      "oxia-coordinator",
		},
		Data: map[string]string{
			clusterConfigMapKey: clusterConfigWithCamelCaseFields,
		},
	})
	previousK8SClient := newK8SClient
	newK8SClient = func() k8s.Interface {
		return k8sClient
	}
	t.Cleanup(func() {
		newK8SClient = previousK8SClient
	})

	reconciler, err := NewReconciler(t.Context(), &option.ClusterOptions{
		ConfigPath: "configmap:oxia/oxia-coordinator",
	})
	require.NoError(t, err)

	config, err := reconciler.Load()
	require.NoError(t, err)
	require.Equal(t, []string{"oxia.oxia.svc.cluster.local:6648"}, config.GetAllowExtraAuthorities())
}

func TestLoadClusterConfigurationFromFilePreservesCamelCaseFields(t *testing.T) {
	configFile := filepath.Join(t.TempDir(), "cluster.yaml")
	require.NoError(t, os.WriteFile(configFile, []byte(clusterConfigWithCamelCaseFields), 0o600))

	reconciler, err := NewReconciler(t.Context(), &option.ClusterOptions{
		ConfigPath: configFile,
	})
	require.NoError(t, err)

	config, err := reconciler.Load()
	require.NoError(t, err)
	require.Equal(t, []string{"oxia.oxia.svc.cluster.local:6648"}, config.GetAllowExtraAuthorities())
}
