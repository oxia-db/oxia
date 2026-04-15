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

package coordinator

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/option"
)

func TestClusterConfigStoreUpdatesFileBackedConfig(t *testing.T) {
	name := "server-1"
	configPath := filepath.Join(t.TempDir(), "cluster.yaml")
	initialConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              "default",
			InitialShardCount: 1,
			ReplicationFactor: 1,
		}},
		Servers: []model.Server{
			{Name: &name, Public: "public-1", Internal: "internal-1"},
		},
		ServerMetadata: map[string]model.ServerMetadata{
			name: {Labels: map[string]string{"rack": "r1"}},
		},
	}

	initialData, err := yaml.Marshal(initialConfig)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(configPath, initialData, 0o644))

	clusterOptions := &option.ClusterOptions{ConfigPath: configPath}
	v := viper.New()
	v.SetConfigFile(configPath)
	load := func() (model.ClusterConfig, error) {
		return loadClusterConfig(clusterOptions, v)
	}

	_, err = load()
	require.NoError(t, err)

	store, err := newClusterConfigStoreWithKubernetes(clusterOptions, v, nil, load)
	require.NoError(t, err)

	updatedConfig, err := store.Update(func(config *model.ClusterConfig) error {
		newPublicAddress := "public-2"
		_, err := config.PatchDataServer(name, model.DataServerPatch{
			PublicAddress: &newPublicAddress,
			Metadata: &model.ServerMetadata{
				Labels: map[string]string{"rack": "r2"},
			},
		})
		return err
	})
	require.NoError(t, err)
	assert.Equal(t, "public-2", updatedConfig.Servers[0].Public)

	updatedFile, err := os.ReadFile(configPath)
	require.NoError(t, err)

	var persisted model.ClusterConfig
	require.NoError(t, yaml.Unmarshal(updatedFile, &persisted))
	assert.Equal(t, "public-2", persisted.Servers[0].Public)
	assert.Equal(t, map[string]string{"rack": "r2"}, persisted.ServerMetadata[name].Labels)
}

func TestClusterConfigStoreUpdatesConfigMapBackedConfig(t *testing.T) {
	name := "server-1"
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:              "default",
			InitialShardCount: 1,
			ReplicationFactor: 1,
		}},
		Servers: []model.Server{
			{Name: &name, Public: "public-1", Internal: "internal-1"},
		},
	}

	initialData, err := yaml.Marshal(clusterConfig)
	require.NoError(t, err)

	clientset := fake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-config",
			Namespace: "oxia",
		},
		Data: map[string]string{
			clusterConfigConfigMapFilePath: string(initialData),
		},
	})

	store, err := newClusterConfigStoreWithKubernetes(
		&option.ClusterOptions{ConfigPath: "configmap:oxia/cluster-config"},
		viper.New(),
		clientset,
		func() (model.ClusterConfig, error) {
			return clusterConfig, nil
		},
	)
	require.NoError(t, err)

	_, err = store.Update(func(config *model.ClusterConfig) error {
		newInternalAddress := "internal-2"
		_, err := config.PatchDataServer(name, model.DataServerPatch{
			InternalAddress: &newInternalAddress,
		})
		return err
	})
	require.NoError(t, err)

	configMap, err := clientset.CoreV1().ConfigMaps("oxia").Get(t.Context(), "cluster-config", metav1.GetOptions{})
	require.NoError(t, err)

	var persisted model.ClusterConfig
	require.NoError(t, yaml.Unmarshal([]byte(configMap.Data[clusterConfigConfigMapFilePath]), &persisted))
	assert.Equal(t, "internal-2", persisted.Servers[0].Internal)
}
