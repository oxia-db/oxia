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
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/option"
)

const clusterConfigConfigMapFilePath = "config.yaml"

type ClusterConfigStore interface {
	Update(mutator func(*model.ClusterConfig) error) (model.ClusterConfig, error)
}

type persistentClusterConfigStore struct {
	lock  sync.Mutex
	load  func() (model.ClusterConfig, error)
	write func(model.ClusterConfig) error
}

func (c *persistentClusterConfigStore) Update(mutator func(*model.ClusterConfig) error) (model.ClusterConfig, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	config, err := c.load()
	if err != nil {
		return model.ClusterConfig{}, err
	}

	if err := mutator(&config); err != nil {
		return model.ClusterConfig{}, err
	}
	if err := config.Validate(); err != nil {
		return model.ClusterConfig{}, err
	}
	if err := c.write(config); err != nil {
		return model.ClusterConfig{}, err
	}
	return config, nil
}

func newClusterConfigStore(
	cluster *option.ClusterOptions,
	v *viper.Viper,
	load func() (model.ClusterConfig, error),
) (ClusterConfigStore, error) {
	var clientset kubernetes.Interface
	if strings.HasPrefix(cluster.ConfigPath, "configmap:") {
		clientset = metadata.NewK8SClientset(metadata.NewK8SClientConfig())
	}

	return newClusterConfigStoreWithKubernetes(cluster, v, clientset, load)
}

func newClusterConfigStoreWithKubernetes(
	cluster *option.ClusterOptions,
	v *viper.Viper,
	clientset kubernetes.Interface,
	load func() (model.ClusterConfig, error),
) (ClusterConfigStore, error) {
	if strings.HasPrefix(cluster.ConfigPath, "configmap:") {
		namespace, configMapName, err := parseClusterConfigConfigMapPath(cluster.ConfigPath)
		if err != nil {
			return nil, err
		}
		if clientset == nil {
			return nil, errors.New("kubernetes client is required for configmap-backed cluster config")
		}

		return &persistentClusterConfigStore{
			load: load,
			write: func(config model.ClusterConfig) error {
				return writeClusterConfigConfigMap(clientset, namespace, configMapName, config)
			},
		}, nil
	}

	configFile := v.ConfigFileUsed()
	if configFile == "" {
		return nil, errors.New("cluster config file path is not available")
	}

	return &persistentClusterConfigStore{
		load: load,
		write: func(config model.ClusterConfig) error {
			return writeClusterConfigFile(configFile, config)
		},
	}, nil
}

func parseClusterConfigConfigMapPath(path string) (namespace, configMapName string, err error) {
	parts := strings.Split(strings.TrimPrefix(path, "configmap:"), "/")
	if len(parts) != 2 {
		return "", "", errors.New("invalid configmap configuration")
	}

	return parts[0], parts[1], nil
}

func writeClusterConfigFile(path string, config model.ClusterConfig) error {
	data, err := yaml.Marshal(config)
	if err != nil {
		return err
	}

	permissions := os.FileMode(0o644)
	info, err := os.Stat(path)
	switch {
	case err == nil:
		permissions = info.Mode().Perm()
	case !errors.Is(err, os.ErrNotExist):
		return err
	}

	return writeFileAtomically(path, data, permissions)
}

func writeFileAtomically(path string, data []byte, permissions os.FileMode) error {
	dir := filepath.Dir(path)
	tempFile, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}

	tempName := tempFile.Name()
	cleanup := func() {
		_ = os.Remove(tempName)
	}

	if _, err := tempFile.Write(data); err != nil {
		_ = tempFile.Close()
		cleanup()
		return err
	}
	if err := tempFile.Chmod(permissions); err != nil {
		_ = tempFile.Close()
		cleanup()
		return err
	}
	if err := tempFile.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tempName, path); err != nil {
		cleanup()
		return err
	}

	return nil
}

func writeClusterConfigConfigMap(clientset kubernetes.Interface, namespace, name string, config model.ClusterConfig) error {
	data, err := yaml.Marshal(config)
	if err != nil {
		return err
	}

	cm, err := metadata.K8SConfigMaps(clientset).Get(namespace, name)
	switch {
	case err == nil:
	case k8serrors.IsNotFound(err):
		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
		}
	default:
		return err
	}

	if cm.Data == nil {
		cm.Data = make(map[string]string)
	}
	cm.Data[clusterConfigConfigMapFilePath] = string(data)

	_, err = metadata.K8SConfigMaps(clientset).Upsert(namespace, name, cm)
	return err
}
