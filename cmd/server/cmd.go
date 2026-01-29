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

package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/oxia-db/oxia/common/codec"

	"github.com/oxia-db/oxia/common/constant"
	oxiadcommonoption "github.com/oxia-db/oxia/oxiad/common/option"
	"github.com/oxia-db/oxia/oxiad/dataserver/option"

	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"

	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/oxiad/dataserver"
)

var (
	sconfFile         string
	dataServerOptions = option.NewDefaultOptions()
	Cmd               = &cobra.Command{
		Use:   "server",
		Short: "Start a server",
		Long:  `Long description`,
		Run:   exec,
	}
)

func init() {
	Cmd.Flags().SortFlags = false
	Cmd.Flags().StringVar(&sconfFile, "sconfig", "", "server config file path")

	Cmd.Flags().StringVarP(&dataServerOptions.Server.Public.BindAddress, "public-addr", "p", fmt.Sprintf("0.0.0.0:%d", constant.DefaultPublicPort), "Public service bind address")
	Cmd.Flags().StringVarP(&dataServerOptions.Server.Internal.BindAddress, "internal-addr", "i", fmt.Sprintf("0.0.0.0:%d", constant.DefaultInternalPort), "Internal service bind address")
	observability := &dataServerOptions.Observability
	Cmd.Flags().StringVarP(&observability.Metric.BindAddress, "metrics-addr", "m", fmt.Sprintf("0.0.0.0:%d", oxiadcommonoption.DefaultMetricsPort), "Metrics service bind address")

	storageWal := &dataServerOptions.Storage.WAL
	storageWal.Sync = &constant.FlagTrue

	var walRetention = 1 * time.Hour
	var notificationRetention = 1 * time.Hour

	Cmd.Flags().StringVar(&storageWal.Dir, "wal-dir", "./data/wal", "Directory for write-ahead-logs")
	Cmd.Flags().BoolVar(storageWal.Sync, "wal-sync-data", true, "Whether to sync data in write-ahead-log")
	Cmd.Flags().DurationVar(&walRetention, "wal-retention-time", 1*time.Hour, "Retention time for the entries in the write-ahead-log")

	Cmd.Flags().DurationVar(&notificationRetention, "notifications-retention-time", 1*time.Hour, "Retention time for the db notifications to clients")

	storageDatabase := &dataServerOptions.Storage.Database
	Cmd.Flags().StringVar(&storageDatabase.Dir, "data-dir", "./data/db", "Directory where to store data")
	Cmd.Flags().Int64Var(&storageDatabase.ReadCacheSizeMB, "db-cache-size-mb", kvstore.DefaultFactoryOptions.CacheSizeMB,
		"Max size of the shared DB cache")

	publicAuth := &dataServerOptions.Server.Public.Auth
	Cmd.Flags().StringVar(&publicAuth.Provider, "auth-provider-name", "", "Authentication provider name. supported: oidc")
	Cmd.Flags().StringVar(&publicAuth.ProviderParams, "auth-provider-params", "", "Authentication provider params. \n oidc: "+"{\"allowedIssueURLs\":\"required1,required2\",\"allowedAudiences\":\"required1,required2\",\"userNameClaim\":\"optional(default:sub)\",\"staticKeyFiles\":{\"issuer1\":\"path/to/key1.pem\",\"issuer2\":\"path/to/key2.pem\"}}")

	publicTLS := &dataServerOptions.Server.Public.TLS
	Cmd.Flags().StringVar(&publicTLS.CertFile, "tls-cert-file", "", "Tls certificate file")
	Cmd.Flags().StringVar(&publicTLS.KeyFile, "tls-key-file", "", "Tls key file")
	Cmd.Flags().Uint16Var(&publicTLS.MinVersion, "tls-min-version", 0, "Tls minimum version")
	Cmd.Flags().Uint16Var(&publicTLS.MaxVersion, "tls-max-version", 0, "Tls maximum version")
	Cmd.Flags().StringVar(&publicTLS.TrustedCaFile, "tls-trusted-ca-file", "", "Tls trusted ca file")
	Cmd.Flags().BoolVar(&publicTLS.InsecureSkipVerify, "tls-insecure-skip-verify", false, "Tls insecure skip verify")
	Cmd.Flags().BoolVar(&publicTLS.ClientAuth, "tls-client-auth", false, "Tls client auth")

	internalTLS := &dataServerOptions.Server.Internal.TLS
	Cmd.Flags().StringVar(&internalTLS.CertFile, "internal-tls-cert-file", "", "Internal server tls certificate file")
	Cmd.Flags().StringVar(&internalTLS.KeyFile, "internal-tls-key-file", "", "Internal server tls key file")
	Cmd.Flags().Uint16Var(&internalTLS.MinVersion, "internal-tls-min-version", 0, "Internal server tls minimum version")
	Cmd.Flags().Uint16Var(&internalTLS.MaxVersion, "internal-tls-max-version", 0, "Internal server tls maximum version")
	Cmd.Flags().StringVar(&internalTLS.TrustedCaFile, "internal-tls-trusted-ca-file", "", "Internal server tls trusted ca file")
	Cmd.Flags().BoolVar(&internalTLS.InsecureSkipVerify, "internal-tls-insecure-skip-verify", false, "Internal server tls insecure skip verify")
	Cmd.Flags().BoolVar(&internalTLS.ClientAuth, "internal-tls-client-auth", false, "Internal server tls client auth")

	replicationTLS := &dataServerOptions.Replication.TLS
	Cmd.Flags().StringVar(&replicationTLS.CertFile, "peer-tls-cert-file", "", "Peer tls certificate file")
	Cmd.Flags().StringVar(&replicationTLS.KeyFile, "peer-tls-key-file", "", "Peer tls key file")
	Cmd.Flags().Uint16Var(&replicationTLS.MinVersion, "peer-tls-min-version", 0, "Peer tls minimum version")
	Cmd.Flags().Uint16Var(&replicationTLS.MaxVersion, "peer-tls-max-version", 0, "Peer tls maximum version")
	Cmd.Flags().StringVar(&replicationTLS.TrustedCaFile, "peer-tls-trusted-ca-file", "", "Peer tls trusted ca file")
	Cmd.Flags().BoolVar(&replicationTLS.InsecureSkipVerify, "peer-tls-insecure-skip-verify", false, "Peer tls insecure skip verify")
	Cmd.Flags().StringVar(&replicationTLS.ServerName, "peer-tls-server-name", "", "Peer tls server name")

	// Convert time.Duration to option.Duration after flag parsing
	Cmd.PreRun = func(*cobra.Command, []string) {
		storageWal.Retention = oxiadcommonoption.Duration(walRetention)
		dataServerOptions.Storage.Notification.Retention = oxiadcommonoption.Duration(notificationRetention)
	}
}

func exec(cmd *cobra.Command, _ []string) {
	process.RunProcess(func() (io.Closer, error) {
		watchableOptions := oxiadcommonoption.NewWatch(dataServerOptions)
		switch {
		case cmd.Flags().Changed("sconfig"):
			// init options
			if err := codec.TryReadAndInitConf(sconfFile, dataServerOptions); err != nil {
				return nil, err
			}
			// start listener
			v := viper.New()
			v.SetConfigFile(sconfFile)
			v.OnConfigChange(func(fsnotify.Event) {
				temporaryOptions := option.NewDefaultOptions()
				if err := codec.TryReadAndInitConf(sconfFile, temporaryOptions); err != nil {
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
		default:
			dataServerOptions.WithDefault()
			if err := dataServerOptions.Validate(); err != nil {
				return nil, err
			}
		}

		return dataserver.New(context.Background(), watchableOptions)
	})
}
