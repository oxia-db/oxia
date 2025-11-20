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
	"io"
	"log/slog"
	"strings"

	"github.com/fsnotify/fsnotify"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/oxia-db/oxia/coordinator/metadata"

	"github.com/oxia-db/oxia/common/entity"
	"github.com/oxia-db/oxia/common/process"

	"github.com/oxia-db/oxia/cmd/flag"
	"github.com/oxia-db/oxia/common/security"
	"github.com/oxia-db/oxia/coordinator"
	"github.com/oxia-db/oxia/coordinator/model"
)

var (
	conf       = coordinator.NewConfig()
	configFile string
	peerTLS    security.TLSOption
	serverTLS  security.TLSOption

	Cmd = &cobra.Command{
		Use:     "coordinator",
		Short:   "Start a coordinator",
		Long:    `Start a coordinator`,
		PreRunE: validate,
		RunE:    exec,
	}
)

func init() {
	flag.InternalAddr(Cmd, &conf.InternalServiceAddr)
	flag.MetricsAddr(Cmd, &conf.MetricsServiceAddr)
	flag.AdminAddr(Cmd, &conf.AdminServiceAddr)
	Cmd.Flags().StringVar(&conf.MetadataProviderName, "metadata", "file", "Metadata provider implementation: file, configmap, raft or memory")

	Cmd.Flags().StringVar(&conf.K8SMetadataNamespace, "k8s-namespace", conf.K8SMetadataNamespace, "Kubernetes namespace for oxia config maps")
	Cmd.Flags().StringVar(&conf.K8SMetadataConfigMapName, "k8s-configmap-name", conf.K8SMetadataConfigMapName, "ConfigMap name for cluster status configmap")

	Cmd.Flags().StringVar(&conf.FileMetadataPath, "file-clusters-status-path", "data/cluster-status.json", "The path where the cluster status is stored when using 'file' provider")

	Cmd.Flags().StringSliceVar(&conf.RaftBootstrapNodes, "raft-bootstrap-nodes", conf.RaftBootstrapNodes, "Raft bootstrap nodes")
	Cmd.Flags().StringVar(&conf.RaftAddress, "raft-address", "", "Raft address")
	Cmd.Flags().StringVar(&conf.RaftDataDir, "raft-data-dir", "data/raft", "Raft address")

	Cmd.Flags().StringVarP(&configFile, "conf", "f", "", "Cluster config file")

	// dataserver TLS section
	Cmd.Flags().StringVar(&serverTLS.CertFile, "tls-cert-file", "", "Tls certificate file")
	Cmd.Flags().StringVar(&serverTLS.KeyFile, "tls-key-file", "", "Tls key file")
	Cmd.Flags().Uint16Var(&serverTLS.MinVersion, "tls-min-version", 0, "Tls minimum version")
	Cmd.Flags().Uint16Var(&serverTLS.MaxVersion, "tls-max-version", 0, "Tls maximum version")
	Cmd.Flags().StringVar(&serverTLS.TrustedCaFile, "tls-trusted-ca-file", "", "Tls trusted ca file")
	Cmd.Flags().BoolVar(&serverTLS.InsecureSkipVerify, "tls-insecure-skip-verify", false, "Tls insecure skip verify")
	Cmd.Flags().BoolVar(&serverTLS.ClientAuth, "tls-client-auth", false, "Tls client auth")

	// peer client TLS section
	Cmd.Flags().StringVar(&peerTLS.CertFile, "peer-tls-cert-file", "", "Peer tls certificate file")
	Cmd.Flags().StringVar(&peerTLS.KeyFile, "peer-tls-key-file", "", "Peer tls key file")
	Cmd.Flags().Uint16Var(&peerTLS.MinVersion, "peer-tls-min-version", 0, "Peer tls minimum version")
	Cmd.Flags().Uint16Var(&peerTLS.MaxVersion, "peer-tls-max-version", 0, "Peer tls maximum version")
	Cmd.Flags().StringVar(&peerTLS.TrustedCaFile, "peer-tls-trusted-ca-file", "", "Peer tls trusted ca file")
	Cmd.Flags().BoolVar(&peerTLS.InsecureSkipVerify, "peer-tls-insecure-skip-verify", false, "Peer tls insecure skip verify")
	Cmd.Flags().StringVar(&peerTLS.ServerName, "peer-tls-dataserver-name", "", "Peer tls dataserver name")
}

func validate(*cobra.Command, []string) error {
	switch conf.MetadataProviderName {
	case metadata.ProviderNameConfigmap:
		if conf.K8SMetadataNamespace == "" {
			return errors.New("k8s-namespace must be set with metadata=configmap")
		}
		if conf.K8SMetadataConfigMapName == "" {
			return errors.New("k8s-configmap-name must be set with metadata=configmap")
		}
	case metadata.ProviderNameRaft:
		if conf.RaftAddress == "" {
			return errors.New("raft-address must be set with metadata=raft")
		}
	default:
		// no-op
	}

	switch conf.MetadataProviderName {
	case "memory", "configmap", "raft", "file":
		return nil
	default:
		return errors.New(`must be one of "memory", "configmap", "raft" or "file"`)
	}
}

func configIsRemote() bool {
	return strings.HasPrefix(configFile, "configmap:")
}

func setConfigPath(v *viper.Viper) error {
	v.SetConfigType("yaml")

	if configIsRemote() {
		err := v.AddRemoteProvider("configmap", "endpoint", configFile)
		if err != nil {
			slog.Error("Failed to add remote provider", slog.Any("error", err))
			return err
		}

		return v.WatchRemoteConfigOnChannel()
	}

	if configFile == "" {
		v.AddConfigPath("/oxia/conf")
		v.AddConfigPath(".")
	}

	v.SetConfigFile(configFile)
	v.WatchConfig()
	return nil
}

func loadClusterConfig(v *viper.Viper) (model.ClusterConfig, error) {
	cc := model.ClusterConfig{}

	var err error

	if configIsRemote() {
		err = v.ReadRemoteConfig()
	} else {
		err = v.ReadInConfig()
	}

	if err != nil {
		return cc, err
	}

	if err := v.Unmarshal(&cc, viper.DecodeHook(mapstructure.ComposeDecodeHookFunc(
		entity.OptBooleanViperHook(),
		mapstructure.StringToTimeDurationHookFunc(), // default hook
		mapstructure.StringToSliceHookFunc(","),     // default hook
	))); err != nil {
		return cc, errors.Wrap(err, "failed to load cluster config")
	}

	return cc, nil
}

func exec(*cobra.Command, []string) error {
	v := viper.New()

	conf.ClusterConfigChangeNotifications = make(chan any)
	conf.ClusterConfigProvider = func() (model.ClusterConfig, error) {
		return loadClusterConfig(v)
	}

	v.OnConfigChange(func(_ fsnotify.Event) {
		conf.ClusterConfigChangeNotifications <- nil
	})

	if err := setConfigPath(v); err != nil {
		return err
	}

	if _, err := loadClusterConfig(v); err != nil {
		return err
	}

	process.RunProcess(func() (io.Closer, error) {
		var err error
		if serverTLS.IsConfigured() {
			if conf.ServerTLS, err = serverTLS.MakeServerTLSConf(); err != nil {
				return nil, err
			}
		}
		if peerTLS.IsConfigured() {
			if conf.PeerTLS, err = peerTLS.MakeClientTLSConf(); err != nil {
				return nil, err
			}
		}
		return coordinator.NewGrpcServer(conf)
	})
	return nil
}
