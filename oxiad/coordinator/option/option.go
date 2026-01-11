package option

import (
	"errors"

	"github.com/oxia-db/oxia/common/security"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"go.uber.org/multierr"
)

type Options struct {
	Cluster       ClusterOptions                    `yaml:"cluster" json:"cluster"`
	Server        ServerOptions                     `yaml:"server" json:"server"`
	Controller    ControllerOptions                 `yaml:"controller" json:"controller"`
	Metadata      MetadataOptions                   `yaml:"metadata" json:"metadata"`
	Observability commonoption.ObservabilityOptions `yaml:"observability" json:"observability"`
}

func (op *Options) WithDefault() {
	op.Metadata.WithDefault()
}

func (op *Options) Validate() error {
	return multierr.Combine(op.Metadata.Validate())
}

type ClusterOptions struct {
	ConfigPath string `yaml:"configPath" json:"configPath"`
}

type ServerOptions struct {
	Admin    AdminServerOptions    `yaml:"admin" json:"admin"`
	Internal InternalServerOptions `yaml:"internal" json:"internal"`
}

type InternalServerOptions struct {
	BindAddress string              `yaml:"bindAddress" json:"bindAddress"`
	TLS         security.TLSOptions `yaml:"tls" json:"tls"`
}

type AdminServerOptions struct {
	BindAddress string              `yaml:"bindAddress" json:"bindAddress"`
	TLS         security.TLSOptions `yaml:"tls,omitempty" json:"tls,omitempty"`
}

type ControllerOptions struct {
	TLS security.TLSOptions `yaml:"tls" json:"tls"`
}

type MetadataOptions struct {
	ProviderName string       `yaml:"providerName" json:"providerName"`
	Kubernetes   K8sMetadata  `yaml:"kubernetes,omitempty" json:"kubernetes,omitempty"`
	File         FileMetadata `yaml:"file,omitempty" json:"file,omitempty"`
	Raft         RaftMetadata `yaml:"raft,omitempty" json:"raft,omitempty"`
}

func (mo *MetadataOptions) WithDefault() {

}

func (mo *MetadataOptions) Validate() error {
	switch mo.ProviderName {
	case metadata.ProviderNameConfigmap:
		if mo.Kubernetes.Namespace == "" {
			return errors.New("k8s-namespace must be set with metadata=configmap")
		}
		if mo.Kubernetes.ConfigMapName == "" {
			return errors.New("k8s-configmap-name must be set with metadata=configmap")
		}
	case metadata.ProviderNameRaft:
		if mo.Raft.Address == "" {
			return errors.New("raft-address must be set with metadata=raft")
		}
	case metadata.ProviderNameFile:
	case metadata.ProviderNameMemory:
	default:
		return errors.New(`must be one of "memory", "configmap", "raft" or "file"`)
	}
	return nil
}

type K8sMetadata struct {
	Namespace     string `yaml:"namespace" json:"namespace"`
	ConfigMapName string `yaml:"configMapName" json:"configMapName"`
}

type FileMetadata struct {
	Path string `yaml:"path" json:"path"`
}

type RaftMetadata struct {
	BootstrapNodes []string `yaml:"bootstrapNodes" json:"bootstrapNodes"`
	Address        string   `yaml:"address" json:"address"`
	DataDir        string   `yaml:"dataDir" json:"dataDir"`
}
