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

	"github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/option"
)

type ClusterConfigManager interface {
	Load() (model.ClusterConfig, error)
	Update(mutator func(*model.ClusterConfig) error) (model.ClusterConfig, error)
}

const clusterConfigConfigMapKey = "config.yaml"

type clusterConfigManager struct {
	sync.Mutex

	clusterConfigChangeNotifications chan any
	clusterOptions                   *option.ClusterOptions
	viper                            *viper.Viper
}

func newClusterConfigManager(
	clusterOptions *option.ClusterOptions,
	v *viper.Viper,
	clusterConfigChangeNotifications chan any,
) ClusterConfigManager {
	return &clusterConfigManager{
		clusterConfigChangeNotifications: clusterConfigChangeNotifications,
		clusterOptions:                   clusterOptions,
		viper:                            v,
	}
}

func (c *clusterConfigManager) Load() (model.ClusterConfig, error) {
	return loadClusterConfig(c.clusterOptions, c.viper)
}

func (c *clusterConfigManager) Update(mutator func(*model.ClusterConfig) error) (model.ClusterConfig, error) {
	c.Lock()
	defer c.Unlock()

	clusterConfig, err := c.Load()
	if err != nil {
		return model.ClusterConfig{}, err
	}

	if err := mutator(&clusterConfig); err != nil {
		return model.ClusterConfig{}, err
	}

	if err := clusterConfig.Validate(); err != nil {
		return model.ClusterConfig{}, err
	}

	if err := c.store(clusterConfig); err != nil {
		return model.ClusterConfig{}, err
	}

	if c.clusterConfigChangeNotifications != nil {
		c.clusterConfigChangeNotifications <- struct{}{}
	}

	return clusterConfig, nil
}

func (c *clusterConfigManager) store(clusterConfig model.ClusterConfig) error {
	if strings.HasPrefix(c.clusterOptions.ConfigPath, "configmap:") {
		return c.storeConfigMap(clusterConfig)
	}

	return c.storeFile(clusterConfig)
}

func (c *clusterConfigManager) storeFile(clusterConfig model.ClusterConfig) error {
	path := c.clusterOptions.ConfigPath
	if path == "" {
		path = c.viper.ConfigFileUsed()
	}
	if path == "" {
		return errors.New("cluster config file path is not configured")
	}

	data, err := yaml.Marshal(clusterConfig)
	if err != nil {
		return errors.Wrap(err, "failed to marshal cluster config")
	}

	mode := os.FileMode(0o644)
	if info, err := os.Stat(path); err == nil {
		mode = info.Mode().Perm()
	} else if !os.IsNotExist(err) {
		return errors.Wrap(err, "failed to inspect cluster config file")
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return errors.Wrap(err, "failed to create cluster config directory")
	}

	tmpFile, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return errors.Wrap(err, "failed to create temporary cluster config file")
	}

	tmpPath := tmpFile.Name()
	defer func() {
		_ = os.Remove(tmpPath)
	}()

	if err := tmpFile.Chmod(mode); err != nil {
		_ = tmpFile.Close()
		return errors.Wrap(err, "failed to set cluster config permissions")
	}

	if _, err := tmpFile.Write(data); err != nil {
		_ = tmpFile.Close()
		return errors.Wrap(err, "failed to write cluster config")
	}

	if err := tmpFile.Close(); err != nil {
		return errors.Wrap(err, "failed to close temporary cluster config file")
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return errors.Wrap(err, "failed to atomically replace cluster config file")
	}

	return nil
}

func (c *clusterConfigManager) storeConfigMap(clusterConfig model.ClusterConfig) error {
	namespace, configMapName, err := parseConfigMapPath(c.clusterOptions.ConfigPath)
	if err != nil {
		return err
	}

	data, err := yaml.Marshal(clusterConfig)
	if err != nil {
		return errors.Wrap(err, "failed to marshal cluster config")
	}

	kubernetes := metadata.NewK8SClientset(metadata.NewK8SClientConfig())
	configMaps := metadata.K8SConfigMaps(kubernetes)

	cm, err := configMaps.Get(namespace, configMapName)
	switch {
	case err == nil:
	case k8serrors.IsNotFound(err):
		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      configMapName,
				Namespace: namespace,
			},
		}
	default:
		return errors.Wrap(err, "failed to load cluster config configmap")
	}

	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	cm.Data[clusterConfigConfigMapKey] = string(data)

	if _, err := configMaps.Upsert(namespace, configMapName, cm); err != nil {
		return errors.Wrap(err, "failed to store cluster config configmap")
	}

	return nil
}

func parseConfigMapPath(path string) (namespace, configMapName string, err error) {
	parts := strings.Split(strings.TrimPrefix(path, "configmap:"), "/")
	if len(parts) != 2 {
		return "", "", errors.New("invalid configmap configuration")
	}

	return parts[0], parts[1], nil
}
