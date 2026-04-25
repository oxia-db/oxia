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

package metadata

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8swatch "k8s.io/apimachinery/pkg/watch"
	k8s "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/oxia-db/oxia/oxiad/coordinator/option"
)

const clusterConfigWithCamelCaseFields = `
namespaces:
  - name: default
    initialShardCount: 1
    replicationFactor: 1
servers:
  - public: ds-0.oxia.svc.cluster.local:6648
    internal: ds-0.oxia.svc.cluster.local:6649
allowExtraAuthorities:
  - oxia.oxia.svc.cluster.local:6648
loadBalancer:
  scheduleInterval: 3s
  quarantineTime: 2m
`

func TestMetadataClusterConfig_LoadFromConfigMap(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "oxia",
			Name:      "oxia-coordinator",
		},
		Data: map[string]string{
			clusterConfigMapKey: clusterConfigWithCamelCaseFields,
		},
	})
	client.PrependWatchReactor("configmaps", func(action k8stesting.Action) (bool, k8swatch.Interface, error) {
		return true, k8swatch.NewFake(), nil
	})

	previousK8SClient := newK8SClient
	newK8SClient = func() k8s.Interface {
		return client
	}
	t.Cleanup(func() {
		newK8SClient = previousK8SClient
	})

	store, err := NewClusterConfigStore(t.Context(), &option.ClusterOptions{
		ConfigPath: "configmap:oxia/oxia-coordinator",
	})
	require.NoError(t, err)

	config, err := store.Load()
	require.NoError(t, err)
	require.Equal(t, []string{"oxia.oxia.svc.cluster.local:6648"}, config.GetAllowExtraAuthorities())
}

func TestMetadataClusterConfig_LoadFromFile(t *testing.T) {
	configFile := filepath.Join(t.TempDir(), "cluster.yaml")
	require.NoError(t, os.WriteFile(configFile, []byte(clusterConfigWithCamelCaseFields), 0o600))

	store, err := NewClusterConfigStore(t.Context(), &option.ClusterOptions{
		ConfigPath: configFile,
	})
	require.NoError(t, err)

	config, err := store.Load()
	require.NoError(t, err)
	require.Equal(t, []string{"oxia.oxia.svc.cluster.local:6648"}, config.GetAllowExtraAuthorities())
}
