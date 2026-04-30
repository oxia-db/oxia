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
	"fmt"
	"path/filepath"
	"strings"

	"go.uber.org/multierr"
	"gopkg.in/yaml.v3"

	metadataconstant "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/security"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
	"github.com/oxia-db/oxia/oxiad/common/rpc/auth"
)

type Options struct {
	Cluster       ClusterOptions                    `yaml:"cluster,omitempty" json:"cluster,omitempty" jsonschema:"description=Deprecated cluster configuration settings"`
	Server        ServerOptions                     `yaml:"server" json:"server" jsonschema:"description=Server configuration for admin and internal endpoints"`
	Controller    ControllerOptions                 `yaml:"controller,omitempty" json:"controller,omitempty" jsonschema:"description=Controller configuration for cluster management"`
	Metadata      MetadataOptions                   `yaml:"metadata" json:"metadata" jsonschema:"description=Metadata provider configuration"`
	Observability commonoption.ObservabilityOptions `yaml:"observability" json:"observability" jsonschema:"description=Observability configuration for metrics and tracing"`
}

func (op *Options) WithDefault() {
	op.Cluster.WithDefault()
	op.Server.WithDefault()
	op.Controller.WithDefault()
	op.Metadata.WithDefault()
	_ = op.Metadata.ApplyLegacyClusterConfigPath(op.Cluster.ConfigPath)
	op.Observability.WithDefault()
}

func (op *Options) Validate() error {
	return multierr.Combine(
		op.Cluster.Validate(),
		op.Server.Validate(),
		op.Controller.Validate(),
		op.Metadata.ApplyLegacyClusterConfigPath(op.Cluster.ConfigPath),
		op.Metadata.Validate(),
		op.Observability.Validate())
}

type ClusterOptions struct {
	ConfigPath string `yaml:"configPath,omitempty" json:"configPath,omitempty" jsonschema:"description=Deprecated path to the cluster configuration file,example=./configs/cluster.yaml"`
}

func (*ClusterOptions) WithDefault() {
}

func (*ClusterOptions) Validate() error {
	return nil
}

type ServerOptions struct {
	Admin    AdminServerOptions    `yaml:"admin" json:"admin" jsonschema:"description=Admin server configuration for management API endpoints"`
	Internal InternalServerOptions `yaml:"internal" json:"internal" jsonschema:"description=Internal server configuration for cluster communication"`
}

func (so *ServerOptions) WithDefault() {
	so.Admin.WithDefault()
	so.Internal.WithDefault()
}

func (so *ServerOptions) Validate() error {
	return multierr.Combine(so.Admin.Validate(), so.Internal.Validate())
}

type InternalServerOptions struct {
	BindAddress string              `yaml:"bindAddress" json:"bindAddress" jsonschema:"description=Bind address for internal server-to-server communication,example=0.0.0.0:6649,format=hostname"`
	Auth        auth.Options        `yaml:"auth,omitempty" json:"auth,omitempty" jsonschema:"description=Authentication configuration for internal server-to-server communication"`
	TLS         security.TLSOptions `yaml:"tls,omitempty" json:"tls,omitempty" jsonschema:"description=TLS configuration for securing internal cluster communication"`
}

func (iso *InternalServerOptions) WithDefault() {
	if iso.BindAddress == "" {
		iso.BindAddress = fmt.Sprintf("0.0.0.0:%d", constant.DefaultInternalPort)
	}
	iso.Auth.WithDefault()
	iso.TLS.WithDefault()
}

func (iso *InternalServerOptions) Validate() error {
	return multierr.Combine(
		iso.Auth.Validate(),
		iso.TLS.Validate(),
	)
}

type AdminServerOptions struct {
	BindAddress string              `yaml:"bindAddress" json:"bindAddress" jsonschema:"description=Bind address for the admin API server,example=0.0.0.0:6650,format=hostname"`
	Auth        auth.Options        `yaml:"auth,omitempty" json:"auth,omitempty" jsonschema:"description=Authentication configuration for the admin API"`
	TLS         security.TLSOptions `yaml:"tls,omitempty" json:"tls,omitempty" jsonschema:"description=TLS configuration for securing admin API connections"`
}

func (aso *AdminServerOptions) WithDefault() {
	if aso.BindAddress == "" {
		aso.BindAddress = fmt.Sprintf("0.0.0.0:%d", constant.DefaultAdminPort)
	}
	aso.Auth.WithDefault()
	aso.TLS.WithDefault()
}

func (aso *AdminServerOptions) Validate() error {
	return multierr.Combine(
		aso.Auth.Validate(),
		aso.TLS.Validate(),
	)
}

type ControllerOptions struct {
	TLS security.TLSOptions `yaml:"tls,omitempty" json:"tls,omitempty" jsonschema:"description=TLS configuration for securing controller communication"`
}

func (co *ControllerOptions) WithDefault() {
	co.TLS.WithDefault()
}

func (co *ControllerOptions) Validate() error {
	return co.TLS.Validate()
}

type ProviderOptions struct {
	ProviderName string       `yaml:"providerName" json:"providerName" jsonschema:"description=Metadata provider type. Valid values: memory/configmap/file/raft,example=configmap"`
	Kubernetes   K8sMetadata  `yaml:"kubernetes,omitempty" json:"kubernetes,omitempty" jsonschema:"description=Kubernetes ConfigMap metadata configuration"`
	File         FileMetadata `yaml:"file,omitempty" json:"file,omitempty" jsonschema:"description=File-based metadata configuration"`
	Raft         RaftMetadata `yaml:"raft,omitempty" json:"raft,omitempty" jsonschema:"description=Raft-based metadata configuration"`
}

type MetadataOptions struct {
	ProviderOptions `yaml:",inline"`
}

func (*MetadataOptions) WithDefault() {
}

func (mo *MetadataOptions) Validate() error {
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
	case metadataconstant.NameFile:
		if mpo.File.StatusPath() == "" {
			return errors.New("metadata.file status path must be set with metadata=file")
		}
	case metadataconstant.NameMemory:
	default:
		return errors.New(`must be one of "memory", "configmap", "raft" or "file"`)
	}
	return nil
}

const (
	DefaultK8sStatusName  = "oxia-cluster-status"
	DefaultK8sConfigName  = "oxia-cluster-config"
	DefaultFileStatusName = "cluster-status.json"
	DefaultFileConfigName = "cluster.yaml"
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

type FileMetadata struct {
	Dir        string `yaml:"dir,omitempty" json:"dir,omitempty" jsonschema:"description=Directory for file-based metadata storage,example=./metadata"`
	StatusName string `yaml:"statusName,omitempty" json:"statusName,omitempty" jsonschema:"description=File name for cluster status, or full path when dir is unset,example=cluster-status.json"`
	ConfigName string `yaml:"configName,omitempty" json:"configName,omitempty" jsonschema:"description=File name for cluster configuration, or full path when dir is unset,example=cluster.yaml"`
}

type fileMetadataCompat struct {
	Dir        string `yaml:"dir" json:"dir"`
	StatusName string `yaml:"statusName" json:"statusName"`
	ConfigName string `yaml:"configName" json:"configName"`
	Path       string `yaml:"path" json:"path"`
}

func (fm *FileMetadata) UnmarshalJSON(data []byte) error {
	decoded := fileMetadataCompat{}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	return fm.applyCompat(decoded)
}

func (fm *FileMetadata) UnmarshalYAML(value *yaml.Node) error {
	decoded := fileMetadataCompat{}
	if err := value.Decode(&decoded); err != nil {
		return err
	}
	return fm.applyCompat(decoded)
}

func (fm *FileMetadata) applyCompat(decoded fileMetadataCompat) error {
	if decoded.Path != "" {
		if decoded.Dir != "" {
			statusDir := filepath.Dir(decoded.Path)
			statusName := filepath.Base(decoded.Path)
			if filepath.Clean(decoded.Dir) != filepath.Clean(statusDir) {
				return errors.New("file.dir and deprecated file.path are both set with different directories")
			}
			if decoded.StatusName != "" && decoded.StatusName != statusName {
				return errors.New("file.statusName and deprecated file.path are both set with different file names")
			}
			if decoded.StatusName == "" {
				decoded.StatusName = statusName
			}
		} else {
			if decoded.StatusName != "" && filepath.Clean(decoded.StatusName) != filepath.Clean(decoded.Path) {
				return errors.New("file.statusName and deprecated file.path are both set with different paths")
			}
			if decoded.StatusName == "" {
				decoded.StatusName = decoded.Path
			}
		}
	}

	fm.Dir = decoded.Dir
	fm.StatusName = decoded.StatusName
	fm.ConfigName = decoded.ConfigName
	return nil
}

func (fm *FileMetadata) applyLegacyConfigPath(configPath string) error {
	if fm.Dir != "" {
		configDir := filepath.Dir(configPath)
		configName := filepath.Base(configPath)
		if filepath.Clean(fm.Dir) != filepath.Clean(configDir) {
			return errors.New("metadata.file.dir and deprecated cluster.configPath are both set with different directories")
		}
		if fm.ConfigName != "" && fm.ConfigName != configName {
			return errors.New("metadata.file.configName and deprecated cluster.configPath are both set with different file names")
		}
		if fm.ConfigName == "" {
			fm.ConfigName = configName
		}
		return nil
	}

	if fm.ConfigName != "" && filepath.Clean(fm.ConfigName) != filepath.Clean(configPath) {
		return errors.New("metadata.file.configName and deprecated cluster.configPath are both set with different paths")
	}
	if fm.ConfigName == "" {
		fm.ConfigName = configPath
	}
	return nil
}

func (fm FileMetadata) StatusPath() string {
	return filePath(fm.Dir, fm.StatusName, DefaultFileStatusName)
}

func (fm FileMetadata) ConfigPath() string {
	return filePath(fm.Dir, fm.ConfigName, DefaultFileConfigName)
}

func filePath(dir, name, defaultName string) string {
	if name == "" {
		name = defaultName
	}
	if name == "" {
		return ""
	}
	if dir == "" {
		return name
	}
	return filepath.Join(dir, name)
}

type RaftMetadata struct {
	BootstrapNodes []string `yaml:"bootstrapNodes" json:"bootstrapNodes" jsonschema:"description=List of bootstrap nodes for Raft cluster initialization"`
	Address        string   `yaml:"address" json:"address" jsonschema:"description=Address of this node in the Raft cluster,example=localhost:8080"`
	DataDir        string   `yaml:"dataDir" json:"dataDir" jsonschema:"description=Directory for Raft metadata storage,example=./data/raft"`
}

func NewDefaultOptions() *Options {
	options := Options{}
	options.WithDefault()
	return &options
}
