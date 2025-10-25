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
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"

	"github.com/oxia-db/oxia/coordinator/metadata"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/entity"

	"github.com/oxia-db/oxia/coordinator"
	"github.com/oxia-db/oxia/coordinator/model"
)

func TestCmd(t *testing.T) {
	clusterConfig := model.ClusterConfig{
		Namespaces: []model.NamespaceConfig{{
			Name:                 constant.DefaultNamespace,
			ReplicationFactor:    1,
			InitialShardCount:    2,
			NotificationsEnabled: entity.Bool(false),
		}},
		Servers: []model.Server{{
			Public:   "public:1234",
			Internal: "internal:5678",
		}},
	}

	bytes, err := yaml.Marshal(&clusterConfig)
	assert.NoError(t, err)

	name := "config.yaml"
	err = os.WriteFile(name, bytes, os.ModePerm)
	assert.NoError(t, err)

	defer func() {
		err = os.Remove(name)
		assert.NoError(t, err)
	}()

	for _, test := range []struct {
		args                []string
		expectedConf        coordinator.Config
		expectedClusterConf model.ClusterConfig
		isErr               bool
	}{
		{[]string{}, coordinator.Config{
			InternalServiceAddr:  "localhost:6649",
			MetricsServiceAddr:   "localhost:8080",
			AdminServiceAddr:     "localhost:6651",
			MetadataProviderName: metadata.ProviderNameFile,
		}, model.ClusterConfig{
			Namespaces: []model.NamespaceConfig{{
				Name:                 constant.DefaultNamespace,
				ReplicationFactor:    1,
				InitialShardCount:    2,
				NotificationsEnabled: entity.Bool(false),
			}},
			Servers: []model.Server{{
				Public:   "public:1234",
				Internal: "internal:5678",
			},
			},
		}, false},
		{[]string{"-i=localhost:1234"}, coordinator.Config{
			InternalServiceAddr:  "localhost:1234",
			MetricsServiceAddr:   "localhost:8080",
			AdminServiceAddr:     "localhost:6651",
			MetadataProviderName: metadata.ProviderNameFile,
		}, model.ClusterConfig{
			Namespaces: []model.NamespaceConfig{{
				Name:                 constant.DefaultNamespace,
				ReplicationFactor:    1,
				InitialShardCount:    2,
				NotificationsEnabled: entity.Bool(false),
			}},
			Servers: []model.Server{{
				Public:   "public:1234",
				Internal: "internal:5678",
			},
			},
		}, false},
		{[]string{"-i=0.0.0.0:1234"}, coordinator.Config{
			InternalServiceAddr:  "0.0.0.0:1234",
			MetricsServiceAddr:   "localhost:8080",
			AdminServiceAddr:     "localhost:6651",
			MetadataProviderName: metadata.ProviderNameFile,
		}, model.ClusterConfig{
			Namespaces: []model.NamespaceConfig{{
				Name:                 constant.DefaultNamespace,
				ReplicationFactor:    1,
				InitialShardCount:    2,
				NotificationsEnabled: entity.Bool(false),
			}},
			Servers: []model.Server{{
				Public:   "public:1234",
				Internal: "internal:5678",
			},
			},
		}, false},
		{[]string{"-m=localhost:1234"}, coordinator.Config{
			InternalServiceAddr:  "localhost:6649",
			MetricsServiceAddr:   "localhost:1234",
			AdminServiceAddr:     "localhost:6651",
			MetadataProviderName: metadata.ProviderNameFile,
		}, model.ClusterConfig{
			Namespaces: []model.NamespaceConfig{{
				Name:                 constant.DefaultNamespace,
				ReplicationFactor:    1,
				InitialShardCount:    2,
				NotificationsEnabled: entity.Bool(false),
			}},
			Servers: []model.Server{{
				Public:   "public:1234",
				Internal: "internal:5678",
			},
			},
		}, false},
		{[]string{"-f=" + name}, coordinator.Config{
			InternalServiceAddr:  "localhost:6649",
			MetricsServiceAddr:   "localhost:8080",
			AdminServiceAddr:     "localhost:6651",
			MetadataProviderName: metadata.ProviderNameFile,
		}, model.ClusterConfig{
			Namespaces: []model.NamespaceConfig{{
				Name:                 constant.DefaultNamespace,
				ReplicationFactor:    1,
				InitialShardCount:    2,
				NotificationsEnabled: entity.Bool(false),
			}},
			Servers: []model.Server{{
				Public:   "public:1234",
				Internal: "internal:5678",
			},
			},
		}, false},
		{[]string{"-f=invalid.yaml"}, coordinator.Config{
			InternalServiceAddr:  "localhost:6649",
			MetricsServiceAddr:   "localhost:8080",
			AdminServiceAddr:     "localhost:6651",
			MetadataProviderName: metadata.ProviderNameFile,
		}, model.ClusterConfig{}, true},
		{[]string{"-a=localhost:9090"}, coordinator.Config{
			InternalServiceAddr:  "localhost:6649",
			MetricsServiceAddr:   "localhost:8080",
			AdminServiceAddr:     "localhost:9090",
			MetadataProviderName: metadata.ProviderNameFile,
		}, model.ClusterConfig{
			Namespaces: []model.NamespaceConfig{{
				Name:                 constant.DefaultNamespace,
				ReplicationFactor:    1,
				InitialShardCount:    2,
				NotificationsEnabled: entity.Bool(false),
			}},
			Servers: []model.Server{{
				Public:   "public:1234",
				Internal: "internal:5678",
			},
			},
		}, false},
	} {
		t.Run(strings.Join(test.args, "_"), func(t *testing.T) {
			conf = coordinator.NewConfig()
			configFile = ""
			Cmd.SetArgs(test.args)
			Cmd.RunE = func(cmd *cobra.Command, args []string) error {
				v := viper.New()
				assert.NoError(t, setConfigPath(v))

				assert.Equal(t, test.expectedConf, conf)
				conf.ClusterConfigProvider = func() (model.ClusterConfig, error) {
					return loadClusterConfig(v)
				}
				clusterConf, err := conf.ClusterConfigProvider()
				assert.Equal(t, test.isErr, err != nil)
				assert.Equal(t, test.expectedClusterConf, clusterConf)
				return err
			}
			err = Cmd.Execute()
			assert.Equal(t, test.isErr, err != nil)
		})
	}

	for _, test := range []struct {
		args  []string
		isErr bool
	}{
		{[]string{}, false},
		{[]string{"--metadata=memory"}, false},
		{[]string{"--metadata=configmap"}, true},
		{[]string{"--metadata=configmap", "--k8s-namespace=foo", "--k8s-configmap-name=bar"}, false},
		{[]string{"--metadata=configmap", "--k8s-namespace=foo}"}, true},
		{[]string{"--metadata=configmap", "--k8s-configmap-name=bar"}, true},
		{[]string{"--metadata=invalid"}, true},
	} {
		t.Run(strings.Join(test.args, "_"), func(t *testing.T) {
			conf = coordinator.NewConfig()
			configFile = ""
			viper.Reset()
			Cmd.SetArgs(test.args)
			Cmd.RunE = func(cmd *cobra.Command, args []string) error { return nil }
			err = Cmd.Execute()
			assert.Equal(t, test.isErr, err != nil)
		})
	}
}
