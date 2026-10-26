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

	metadataconstant "github.com/oxia-db/oxia/oxiad/common/metadata"
)

// The valid values of ProviderOptions.ProviderName.
const (
	ProviderMemory    = metadataconstant.NameMemory
	ProviderFile      = metadataconstant.NameFile
	ProviderConfigMap = metadataconstant.NameConfigMap
	ProviderRaft      = metadataconstant.NameRaft
)

// ProviderOptions selects where the coordinator keeps the cluster
// configuration and status, and configures that provider. Only the options of
// the selected provider are used.
type ProviderOptions struct {
	// ProviderName selects the provider. It must be set:
	//   - file keeps them in local files (see FileMetadata);
	//   - configmap keeps them in Kubernetes ConfigMaps (see K8sMetadata);
	//   - raft replicates them between the coordinators (see RaftMetadata);
	//   - memory keeps them in memory, and loses them on a restart: use it
	//     only for tests.
	ProviderName string `yaml:"providerName" json:"providerName" jsonschema:"description=Provider of the cluster configuration and status,enum=file,enum=configmap,enum=raft,enum=memory,example=configmap"`
	// Kubernetes configures the configmap provider.
	Kubernetes K8sMetadata `yaml:"kubernetes,omitempty" json:"kubernetes,omitempty" jsonschema:"description=Options of the configmap provider"`
	// File configures the file provider.
	File FileMetadata `yaml:"file,omitempty" json:"file,omitempty" jsonschema:"description=Options of the file provider"`
	// Raft configures the raft provider.
	Raft RaftMetadata `yaml:"raft,omitempty" json:"raft,omitempty" jsonschema:"description=Options of the raft provider"`
}

// MetadataOptions configures the metadata of the coordinator: the provider of
// the cluster configuration and status, and the name of the coordinator.
// Several coordinators can share the metadata, e.g. for high availability:
// they elect a leader, which is the only one to manage the cluster.
type MetadataOptions struct {
	// Name identifies the coordinator in the leader election between the
	// coordinators that share the metadata. It must be unique among them, and
	// stable across restarts, e.g. the name of its pod. It defaults to the
	// hostname.
	Name string `yaml:"name,omitempty" json:"name,omitempty" jsonschema:"description=Name of the coordinator in the leader election between the coordinators that share the metadata. Must be unique and stable across restarts. Defaults to the hostname,example=coordinator-0"`
	// ProviderOptions selects and configures the provider. Its keys are at
	// the level of name.
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

// ApplyLegacyClusterConfigPath moves the deprecated cluster.configPath into the
// options of the provider: a path prefixed with configmap: names the ConfigMap
// of the configuration, and any other path its file. It fails if the options
// of the provider set another location, or on a ConfigMap path with the file
// provider; the other providers ignore a ConfigMap path.
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
