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
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/oxia-db/oxia/common/codec"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/process"
	oxiadcommonoption "github.com/oxia-db/oxia/oxiad/common/option"
	"github.com/oxia-db/oxia/oxiad/coordinator"
	"github.com/oxia-db/oxia/oxiad/coordinator/option"
)

var (
	confFile           string
	coordinatorOptions = option.NewDefaultOptions()

	Cmd = &cobra.Command{
		Use:   "coordinator",
		Short: "Start a coordinator",
		Long:  `Start a coordinator`,
		Run:   exec,
	}
)

func init() {
	Cmd.Flags().StringVar(&confFile, "sconfig", "", "server config file path")

	Cmd.Flags().StringVarP(&coordinatorOptions.Server.Internal.BindAddress, "internal-addr", "i", fmt.Sprintf("0.0.0.0:%d", constant.DefaultInternalPort), "Internal service bind address")
	Cmd.Flags().StringVarP(&coordinatorOptions.Server.Admin.BindAddress, "admin-addr", "a", fmt.Sprintf("0.0.0.0:%d", constant.DefaultAdminPort), "Admin service bind address")

	observability := &coordinatorOptions.Observability
	Cmd.Flags().StringVarP(&observability.Metric.BindAddress, "metrics-addr", "m", fmt.Sprintf("0.0.0.0:%d", oxiadcommonoption.DefaultMetricsPort), "Metrics service bind address")

	meta := &coordinatorOptions.Metadata
	Cmd.Flags().StringVar(&meta.ProviderName, "metadata", "file", "Metadata provider implementation: file, configmap, raft or memory")

	Cmd.Flags().StringVar(&meta.Kubernetes.Namespace, "k8s-namespace", meta.Kubernetes.Namespace, "Kubernetes namespace for oxia config maps")
	Cmd.Flags().StringVar(&meta.Kubernetes.ConfigMapName, "k8s-configmap-name", meta.Kubernetes.ConfigMapName, "ConfigMap name for cluster status configmap")

	Cmd.Flags().StringVar(&meta.File.Path, "file-clusters-status-path", "data/cluster-status.json", "The path where the cluster status is stored when using 'file' provider")

	Cmd.Flags().StringSliceVar(&meta.Raft.BootstrapNodes, "raft-bootstrap-nodes", meta.Raft.BootstrapNodes, "Raft bootstrap nodes")
	Cmd.Flags().StringVar(&meta.Raft.Address, "raft-address", "", "Raft address")
	Cmd.Flags().StringVar(&meta.Raft.DataDir, "raft-data-dir", "data/raft", "Raft address")

	cluster := &coordinatorOptions.Cluster
	Cmd.Flags().StringVarP(&cluster.ConfigPath, "conf", "f", "", "Cluster config file")
	_ = Cmd.Flags().MarkDeprecated("conf", "--conf and its short form -f are deprecated; please use --cconfig instead (no short form)")
	Cmd.Flags().StringVar(&cluster.ConfigPath, "cconfig", "", "Cluster config file")

	internalServer := &coordinatorOptions.Server.Internal
	Cmd.Flags().StringVar(&internalServer.TLS.CertFile, "tls-cert-file", "", "Tls certificate file")
	Cmd.Flags().StringVar(&internalServer.TLS.KeyFile, "tls-key-file", "", "Tls key file")
	Cmd.Flags().Uint16Var(&internalServer.TLS.MinVersion, "tls-min-version", 0, "Tls minimum version")
	Cmd.Flags().Uint16Var(&internalServer.TLS.MaxVersion, "tls-max-version", 0, "Tls maximum version")
	Cmd.Flags().StringVar(&internalServer.TLS.TrustedCaFile, "tls-trusted-ca-file", "", "Tls trusted ca file")
	Cmd.Flags().BoolVar(&internalServer.TLS.InsecureSkipVerify, "tls-insecure-skip-verify", false, "Tls insecure skip verify")
	Cmd.Flags().BoolVar(&internalServer.TLS.ClientAuth, "tls-client-auth", false, "Tls client auth")

	controller := &coordinatorOptions.Controller
	Cmd.Flags().StringVar(&controller.TLS.CertFile, "peer-tls-cert-file", "", "Peer tls certificate file")
	Cmd.Flags().StringVar(&controller.TLS.KeyFile, "peer-tls-key-file", "", "Peer tls key file")
	Cmd.Flags().Uint16Var(&controller.TLS.MinVersion, "peer-tls-min-version", 0, "Peer tls minimum version")
	Cmd.Flags().Uint16Var(&controller.TLS.MaxVersion, "peer-tls-max-version", 0, "Peer tls maximum version")
	Cmd.Flags().StringVar(&controller.TLS.TrustedCaFile, "peer-tls-trusted-ca-file", "", "Peer tls trusted ca file")
	Cmd.Flags().BoolVar(&controller.TLS.InsecureSkipVerify, "peer-tls-insecure-skip-verify", false, "Peer tls insecure skip verify")
	Cmd.Flags().StringVar(&controller.TLS.ServerName, "peer-tls-server-name", "", "Peer tls server name")
}

func exec(cmd *cobra.Command, _ []string) {
	process.RunProcess(func() (io.Closer, error) {
		watchableOptions := oxiadcommonoption.NewWatch(coordinatorOptions)
		// configure the options
		if cmd.Flags().Changed("conf") {
			// init options
			if err := codec.TryReadAndInitConf(confFile, coordinatorOptions); err != nil {
				return nil, err
			}
			// start listener
			v := viper.New()
			v.SetConfigFile(confFile)
			v.OnConfigChange(func(fsnotify.Event) {
				temporaryOptions := option.NewDefaultOptions()
				if err := codec.TryReadAndInitConf(confFile, temporaryOptions); err != nil {
					slog.Warn("parse updated configuration file failed", slog.Any("err", err))
					return
				}
				previous, _ := watchableOptions.Load()
				slog.Info("configuration file has changed.",
					slog.Any("previous", previous),
					slog.Any("current", temporaryOptions))
				watchableOptions.Notify(temporaryOptions)
			})
			v.WatchConfig()
		} else {
			coordinatorOptions.WithDefault()
			if err := coordinatorOptions.Validate(); err != nil {
				return nil, err
			}
		}
		return coordinator.NewGrpcServer(context.Background(), watchableOptions)
	})
}
