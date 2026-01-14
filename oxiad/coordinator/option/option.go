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

	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/security"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata"
)

type Options struct {
	Cluster       ClusterOptions                    `yaml:"cluster" json:"cluster"`
	Server        ServerOptions                     `yaml:"server" json:"server"`
	Controller    ControllerOptions                 `yaml:"controller" json:"controller"`
	Metadata      MetadataOptions                   `yaml:"metadata" json:"metadata"`
	Observability commonoption.ObservabilityOptions `yaml:"observability" json:"observability"`
}

func (op *Options) WithDefault() {
	op.Cluster.WithDefault()
	op.Server.WithDefault()
	op.Controller.WithDefault()
	op.Metadata.WithDefault()
	op.Observability.WithDefault()
}

func (op *Options) Validate() error {
	return multierr.Combine(
		op.Cluster.Validate(),
		op.Server.Validate(),
		op.Controller.Validate(),
		op.Metadata.Validate(),
		op.Observability.Validate())
}

type ClusterOptions struct {
	ConfigPath string `yaml:"configPath" json:"configPath"`
}

func (co *ClusterOptions) WithDefault() {
	if co.ConfigPath == "" {
		co.ConfigPath = "./configs/cluster.yaml"
	}
}

func (*ClusterOptions) Validate() error {
	return nil
}

type ServerOptions struct {
	Admin    AdminServerOptions    `yaml:"admin" json:"admin"`
	Internal InternalServerOptions `yaml:"internal" json:"internal"`
}

func (so *ServerOptions) WithDefault() {
	so.Admin.WithDefault()
	so.Internal.WithDefault()
}

func (so *ServerOptions) Validate() error {
	return multierr.Combine(so.Admin.Validate(), so.Internal.Validate())
}

type InternalServerOptions struct {
	BindAddress string              `yaml:"bindAddress" json:"bindAddress"`
	TLS         security.TLSOptions `yaml:"tls" json:"tls"`
}

func (iso *InternalServerOptions) WithDefault() {
	if iso.BindAddress == "" {
		iso.BindAddress = fmt.Sprintf("0.0.0.0:%d", constant.DefaultInternalPort)
	}
	iso.TLS.WithDefault()
}

func (iso *InternalServerOptions) Validate() error {
	return iso.TLS.Validate()
}

type AdminServerOptions struct {
	BindAddress string              `yaml:"bindAddress" json:"bindAddress"`
	TLS         security.TLSOptions `yaml:"tls,omitempty" json:"tls,omitempty"`
}

func (aso *AdminServerOptions) WithDefault() {
	if aso.BindAddress == "" {
		aso.BindAddress = fmt.Sprintf("0.0.0.0:%d", constant.DefaultAdminPort)
	}
	aso.TLS.WithDefault()
}

func (aso *AdminServerOptions) Validate() error {
	return aso.TLS.Validate()
}

type ControllerOptions struct {
	TLS security.TLSOptions `yaml:"tls" json:"tls"`
}

func (co *ControllerOptions) WithDefault() {
	co.TLS.WithDefault()
}

func (co *ControllerOptions) Validate() error {
	return co.TLS.Validate()
}

type MetadataOptions struct {
	ProviderName string       `yaml:"providerName" json:"providerName"`
	Kubernetes   K8sMetadata  `yaml:"kubernetes,omitempty" json:"kubernetes,omitempty"`
	File         FileMetadata `yaml:"file,omitempty" json:"file,omitempty"`
	Raft         RaftMetadata `yaml:"raft,omitempty" json:"raft,omitempty"`
}

func (*MetadataOptions) WithDefault() {
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
	case metadata.ProviderNameFile, metadata.ProviderNameMemory:
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

func NewDefaultOptions() *Options {
	options := Options{}
	options.WithDefault()
	return &options
}
