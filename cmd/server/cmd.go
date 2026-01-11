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
	"fmt"
	"io"
	"time"

	"github.com/oxia-db/oxia/common/constant"
	oxiadcommon_option "github.com/oxia-db/oxia/oxiad/common/option"
	"github.com/oxia-db/oxia/oxiad/dataserver/option"
	"github.com/spf13/cobra"

	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"

	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/oxiad/dataserver"
)

var (
	dataServerOptions = &option.Options{}
	Cmd               = &cobra.Command{
		Use:   "server",
		Short: "Start a server",
		Long:  `Long description`,
		Run:   exec,
	}
)

func init() {
	Cmd.Flags().SortFlags = false

	Cmd.Flags().StringVarP(&dataServerOptions.Server.Public.BindAddress, "public-addr", "p", fmt.Sprintf("0.0.0.0:%d", constant.DefaultPublicPort), "Public service bind address")
	Cmd.Flags().StringVarP(&dataServerOptions.Server.Internal.BindAddress, "internal-addr", "i", fmt.Sprintf("0.0.0.0:%d", constant.DefaultInternalPort), "Internal service bind address")
	observability := &dataServerOptions.Observability
	Cmd.Flags().StringVarP(&observability.Metric.BindAddress, "metrics-addr", "m", fmt.Sprintf("0.0.0.0:%d", oxiadcommon_option.DefaultMetricsPort), "Metrics service bind address")

	storageWal := &dataServerOptions.Storage.WAL
	Cmd.Flags().StringVar(&storageWal.Dir, "wal-dir", "./data/wal", "Directory for write-ahead-logs")
	Cmd.Flags().BoolVar(&storageWal.Sync, "wal-sync-data", true, "Whether to sync data in write-ahead-log")
	Cmd.Flags().DurationVar(&storageWal.Retention, "wal-retention-time", 1*time.Hour, "Retention time for the entries in the write-ahead-log")

	notification := &dataServerOptions.Storage.Notification
	Cmd.Flags().DurationVar(&notification.Retention, "notifications-retention-time", 1*time.Hour, "Retention time for the db notifications to clients")

	storageDatabase := &dataServerOptions.Storage.Database
	Cmd.Flags().StringVar(&storageDatabase.Dir, "data-dir", "./data/db", "Directory where to store data")
	Cmd.Flags().Int64Var(&storageDatabase.ReadCacheSizeMB, "db-cache-size-mb", kvstore.DefaultFactoryOptions.CacheSizeMB,
		"Max size of the shared DB cache")

	publicAuth := &dataServerOptions.Server.Public.Auth
	Cmd.Flags().StringVar(&publicAuth.Provider, "auth-provider-name", "", "Authentication provider name. supported: oidc")
	Cmd.Flags().StringVar(&publicAuth.ProviderParams, "auth-provider-params", "", "Authentication provider params. \n oidc: "+"{\"allowedIssueURLs\":\"required1,required2\",\"allowedAudiences\":\"required1,required2\",\"userNameClaim\":\"optional(default:sub)\"}")

	publicTls := &dataServerOptions.Server.Public.TLS
	Cmd.Flags().StringVar(&publicTls.CertFile, "tls-cert-file", "", "Tls certificate file")
	Cmd.Flags().StringVar(&publicTls.KeyFile, "tls-key-file", "", "Tls key file")
	Cmd.Flags().Uint16Var(&publicTls.MinVersion, "tls-min-version", 0, "Tls minimum version")
	Cmd.Flags().Uint16Var(&publicTls.MaxVersion, "tls-max-version", 0, "Tls maximum version")
	Cmd.Flags().StringVar(&publicTls.TrustedCaFile, "tls-trusted-ca-file", "", "Tls trusted ca file")
	Cmd.Flags().BoolVar(&publicTls.InsecureSkipVerify, "tls-insecure-skip-verify", false, "Tls insecure skip verify")
	Cmd.Flags().BoolVar(&publicTls.ClientAuth, "tls-client-auth", false, "Tls client auth")

	internalTls := &dataServerOptions.Server.Public.TLS
	Cmd.Flags().StringVar(&internalTls.CertFile, "internal-tls-cert-file", "", "Internal server tls certificate file")
	Cmd.Flags().StringVar(&internalTls.KeyFile, "internal-tls-key-file", "", "Internal server tls key file")
	Cmd.Flags().Uint16Var(&internalTls.MinVersion, "internal-tls-min-version", 0, "Internal server tls minimum version")
	Cmd.Flags().Uint16Var(&internalTls.MaxVersion, "internal-tls-max-version", 0, "Internal server tls maximum version")
	Cmd.Flags().StringVar(&internalTls.TrustedCaFile, "internal-tls-trusted-ca-file", "", "Internal server tls trusted ca file")
	Cmd.Flags().BoolVar(&internalTls.InsecureSkipVerify, "internal-tls-insecure-skip-verify", false, "Internal server tls insecure skip verify")
	Cmd.Flags().BoolVar(&internalTls.ClientAuth, "internal-tls-client-auth", false, "Internal server tls client auth")

	replicationTls := &dataServerOptions.Replication.TLS
	Cmd.Flags().StringVar(&replicationTls.CertFile, "peer-tls-cert-file", "", "Peer tls certificate file")
	Cmd.Flags().StringVar(&replicationTls.KeyFile, "peer-tls-key-file", "", "Peer tls key file")
	Cmd.Flags().Uint16Var(&replicationTls.MinVersion, "peer-tls-min-version", 0, "Peer tls minimum version")
	Cmd.Flags().Uint16Var(&replicationTls.MaxVersion, "peer-tls-max-version", 0, "Peer tls maximum version")
	Cmd.Flags().StringVar(&replicationTls.TrustedCaFile, "peer-tls-trusted-ca-file", "", "Peer tls trusted ca file")
	Cmd.Flags().BoolVar(&replicationTls.InsecureSkipVerify, "peer-tls-insecure-skip-verify", false, "Peer tls insecure skip verify")
	Cmd.Flags().StringVar(&replicationTls.ServerName, "peer-tls-server-name", "", "Peer tls server name")
}

func exec(*cobra.Command, []string) {
	process.RunProcess(func() (io.Closer, error) {
		dataServerOptions.WithDefault()
		if err := dataServerOptions.Validate(); err != nil {
			return nil, err
		}
		return dataserver.New(dataServerOptions)
	})
}
