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

package option

import (
	"encoding/json"
	"errors"

	"gopkg.in/yaml.v3"
)

const (
	DefaultK8sStatusName = "oxia-cluster-status"
	DefaultK8sConfigName = "oxia-cluster-config"
)

type K8sMetadata struct {
	Namespace  string `yaml:"namespace" json:"namespace" jsonschema:"description=Kubernetes namespace for ConfigMap storage,example=default"`
	StatusName string `yaml:"statusName,omitempty" json:"statusName,omitempty" jsonschema:"description=Name of the ConfigMap for cluster status,example=oxia-cluster-status"`
	ConfigName string `yaml:"configName,omitempty" json:"configName,omitempty" jsonschema:"description=Name of the ConfigMap for cluster configuration,example=oxia-cluster-config"`
}

type k8sMetadataCompat struct {
	Namespace     string `yaml:"namespace" json:"namespace"`
	StatusName    string `yaml:"statusName" json:"statusName"`
	ConfigName    string `yaml:"configName" json:"configName"`
	ConfigMapName string `yaml:"configMapName" json:"configMapName"`
}

func (km *K8sMetadata) UnmarshalJSON(data []byte) error {
	decoded := k8sMetadataCompat{}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	return km.applyCompat(decoded)
}

func (km *K8sMetadata) UnmarshalYAML(value *yaml.Node) error {
	decoded := k8sMetadataCompat{}
	if err := value.Decode(&decoded); err != nil {
		return err
	}
	return km.applyCompat(decoded)
}

func (km *K8sMetadata) applyCompat(decoded k8sMetadataCompat) error {
	if decoded.StatusName != "" && decoded.ConfigMapName != "" && decoded.StatusName != decoded.ConfigMapName {
		return errors.New("kubernetes.statusName and deprecated kubernetes.configMapName are both set with different values")
	}

	km.Namespace = decoded.Namespace
	km.StatusName = decoded.StatusName
	if km.StatusName == "" {
		km.StatusName = decoded.ConfigMapName
	}
	km.ConfigName = decoded.ConfigName
	return nil
}

func (km K8sMetadata) StatusNameOrDefault() string {
	if km.StatusName != "" {
		return km.StatusName
	}
	return DefaultK8sStatusName
}

func (km K8sMetadata) ConfigNameOrDefault() string {
	if km.ConfigName != "" {
		return km.ConfigName
	}
	return DefaultK8sConfigName
}
