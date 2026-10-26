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
	"errors"
	"fmt"
	"os"
	"strings"

	metadataconstant "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
)

// Valid MetadataOptions.ProviderName values.
const (
	ProviderMemory    = metadataconstant.NameMemory
	ProviderFile      = metadataconstant.NameFile
	ProviderConfigMap = metadataconstant.NameConfigMap
	ProviderRaft      = metadataconstant.NameRaft
)

type ProviderOptions struct {
	ProviderName string       `yaml:"providerName" json:"providerName" jsonschema:"description=Metadata provider type. Valid values: memory/configmap/file/raft,example=configmap"`
	Kubernetes   K8sMetadata  `yaml:"kubernetes,omitempty" json:"kubernetes,omitempty" jsonschema:"description=Kubernetes ConfigMap metadata configuration"`
	File         FileMetadata `yaml:"file,omitempty" json:"file,omitempty" jsonschema:"description=File-based metadata configuration"`
	Raft         RaftMetadata `yaml:"raft,omitempty" json:"raft,omitempty" jsonschema:"description=Raft-based metadata configuration"`
}

type MetadataOptions struct {
	Name            string `yaml:"name,omitempty" json:"name,omitempty" jsonschema:"description=Stable coordinator name used by metadata leader election,example=coordinator-0"`
	ProviderOptions `yaml:",inline"`
}

func (mo *MetadataOptions) WithDefault() {
	if mo.Name == "" {
		if hostname, err := os.Hostname(); err == nil {
			mo.Name = hostname
		}
	}
}

func (mo *MetadataOptions) Validate() error {
	if strings.TrimSpace(mo.Name) == "" {
		return errors.New("metadata.name must be set")
	}
	return mo.ProviderOptions.Validate()
}

func (mo *MetadataOptions) GetStatusProviderOptions() ProviderOptions {
	return mo.ProviderOptions
}

const legacyConfigMapClusterConfigPrefix = "configmap:"

func (mo *MetadataOptions) ApplyLegacyClusterConfigPath(configPath string) error {
	if configPath == "" {
		return nil
	}

	switch mo.ProviderName {
	case metadataconstant.NameConfigMap:
		if !strings.HasPrefix(configPath, legacyConfigMapClusterConfigPrefix) {
			return mo.File.applyLegacyConfigPath(configPath)
		}
		path := strings.TrimPrefix(configPath, legacyConfigMapClusterConfigPrefix)
		namespace, name, ok := strings.Cut(path, "/")
		if !ok || namespace == "" || name == "" {
			return fmt.Errorf("invalid cluster.configPath %q, expected configmap:<namespace>/<name>", configPath)
		}
		if mo.Kubernetes.Namespace != "" && mo.Kubernetes.Namespace != namespace {
			return errors.New("metadata.kubernetes.namespace and deprecated cluster.configPath namespace are both set with different values")
		}
		if mo.Kubernetes.ConfigName != "" && mo.Kubernetes.ConfigName != name {
			return errors.New("metadata.kubernetes.configName and deprecated cluster.configPath ConfigMap name are both set with different values")
		}
		if mo.Kubernetes.Namespace == "" {
			mo.Kubernetes.Namespace = namespace
		}
		if mo.Kubernetes.ConfigName == "" {
			mo.Kubernetes.ConfigName = name
		}
	case metadataconstant.NameFile:
		if strings.HasPrefix(configPath, legacyConfigMapClusterConfigPrefix) {
			return errors.New("deprecated cluster.configPath must be a file path when metadata provider is file")
		}
		return mo.File.applyLegacyConfigPath(configPath)
	default:
		if strings.HasPrefix(configPath, legacyConfigMapClusterConfigPrefix) {
			return nil
		}
		return mo.File.applyLegacyConfigPath(configPath)
	}
	return nil
}

func (mpo ProviderOptions) Validate() error {
	switch mpo.ProviderName {
	case metadataconstant.NameConfigMap:
		if mpo.Kubernetes.Namespace == "" {
			return errors.New("k8s-namespace must be set with metadata=configmap")
		}
	case metadataconstant.NameRaft:
		if mpo.Raft.Address == "" {
			return errors.New("raft-address must be set with metadata=raft")
		}
	case metadataconstant.NameFile, metadataconstant.NameMemory:
	default:
		return errors.New(`must be one of "memory", "configmap", "raft" or "file"`)
	}
	return nil
}
