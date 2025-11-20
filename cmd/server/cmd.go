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
	"io"
	"time"

	"github.com/spf13/cobra"

	"github.com/oxia-db/oxia/common/process"

	"github.com/oxia-db/oxia/cmd/flag"
	"github.com/oxia-db/oxia/common/security"
	"github.com/oxia-db/oxia/server"
	"github.com/oxia-db/oxia/server/kv"
)

var (
	conf = server.Config{}

	peerTLS           = security.TLSOption{}
	serverTLS         = security.TLSOption{}
	internalServerTLS = security.TLSOption{}

	Cmd = &cobra.Command{
		Use:   "server",
		Short: "Start a server",
		Long:  `Long description`,
		Run:   exec,
	}
)

func init() {
	Cmd.Flags().SortFlags = false

	flag.PublicAddr(Cmd, &conf.PublicServiceAddr)
	flag.InternalAddr(Cmd, &conf.InternalServiceAddr)
	flag.MetricsAddr(Cmd, &conf.MetricsServiceAddr)
	Cmd.Flags().StringVar(&conf.DataDir, "data-dir", "./data/db", "Directory where to store data")
	Cmd.Flags().StringVar(&conf.WalDir, "wal-dir", "./data/wal", "Directory for write-ahead-logs")
	Cmd.Flags().DurationVar(&conf.WalRetentionTime, "wal-retention-time", 1*time.Hour, "Retention time for the entries in the write-ahead-log")
	Cmd.Flags().DurationVar(&conf.NotificationsRetentionTime, "notifications-retention-time", 1*time.Hour, "Retention time for the db notifications to clients")

	Cmd.Flags().BoolVar(&conf.WalSyncData, "wal-sync-data", true, "Whether to sync data in write-ahead-log")
	Cmd.Flags().Int64Var(&conf.DbBlockCacheMB, "db-cache-size-mb", kv.DefaultFactoryOptions.CacheSizeMB,
		"Max size of the shared DB cache")
	Cmd.Flags().StringVar(&conf.AuthOptions.ProviderName, "auth-provider-name", "", "Authentication provider name. supported: oidc")
	Cmd.Flags().StringVar(&conf.AuthOptions.ProviderParams, "auth-provider-params", "", "Authentication provider params. \n oidc: "+"{\"allowedIssueURLs\":\"required1,required2\",\"allowedAudiences\":\"required1,required2\",\"userNameClaim\":\"optional(default:sub)\"}")

	// server TLS section
	Cmd.Flags().StringVar(&serverTLS.CertFile, "tls-cert-file", "", "Tls certificate file")
	Cmd.Flags().StringVar(&serverTLS.KeyFile, "tls-key-file", "", "Tls key file")
	Cmd.Flags().Uint16Var(&serverTLS.MinVersion, "tls-min-version", 0, "Tls minimum version")
	Cmd.Flags().Uint16Var(&serverTLS.MaxVersion, "tls-max-version", 0, "Tls maximum version")
	Cmd.Flags().StringVar(&serverTLS.TrustedCaFile, "tls-trusted-ca-file", "", "Tls trusted ca file")
	Cmd.Flags().BoolVar(&serverTLS.InsecureSkipVerify, "tls-insecure-skip-verify", false, "Tls insecure skip verify")
	Cmd.Flags().BoolVar(&serverTLS.ClientAuth, "tls-client-auth", false, "Tls client auth")

	// internal server TLS section
	Cmd.Flags().StringVar(&internalServerTLS.CertFile, "internal-tls-cert-file", "", "Internal server tls certificate file")
	Cmd.Flags().StringVar(&internalServerTLS.KeyFile, "internal-tls-key-file", "", "Internal server tls key file")
	Cmd.Flags().Uint16Var(&internalServerTLS.MinVersion, "internal-tls-min-version", 0, "Internal server tls minimum version")
	Cmd.Flags().Uint16Var(&internalServerTLS.MaxVersion, "internal-tls-max-version", 0, "Internal server tls maximum version")
	Cmd.Flags().StringVar(&internalServerTLS.TrustedCaFile, "internal-tls-trusted-ca-file", "", "Internal server tls trusted ca file")
	Cmd.Flags().BoolVar(&internalServerTLS.InsecureSkipVerify, "internal-tls-insecure-skip-verify", false, "Internal server tls insecure skip verify")
	Cmd.Flags().BoolVar(&internalServerTLS.ClientAuth, "internal-tls-client-auth", false, "Internal server tls client auth")

	// peer client TLS section
	Cmd.Flags().StringVar(&peerTLS.CertFile, "peer-tls-cert-file", "", "Peer tls certificate file")
	Cmd.Flags().StringVar(&peerTLS.KeyFile, "peer-tls-key-file", "", "Peer tls key file")
	Cmd.Flags().Uint16Var(&peerTLS.MinVersion, "peer-tls-min-version", 0, "Peer tls minimum version")
	Cmd.Flags().Uint16Var(&peerTLS.MaxVersion, "peer-tls-max-version", 0, "Peer tls maximum version")
	Cmd.Flags().StringVar(&peerTLS.TrustedCaFile, "peer-tls-trusted-ca-file", "", "Peer tls trusted ca file")
	Cmd.Flags().BoolVar(&peerTLS.InsecureSkipVerify, "peer-tls-insecure-skip-verify", false, "Peer tls insecure skip verify")
	Cmd.Flags().StringVar(&peerTLS.ServerName, "peer-tls-server-name", "", "Peer tls server name")
}

func exec(*cobra.Command, []string) {
	process.RunProcess(func() (io.Closer, error) {
		if err := configureTLS(); err != nil {
			return nil, err
		}
		return server.New(conf)
	})
}

func configureTLS() error {
	var err error
	if serverTLS.IsConfigured() {
		if conf.ServerTLS, err = serverTLS.MakeServerTLSConf(); err != nil {
			return err
		}
	}
	if peerTLS.IsConfigured() {
		if conf.PeerTLS, err = peerTLS.MakeClientTLSConf(); err != nil {
			return err
		}
	}
	if internalServerTLS.IsConfigured() {
		if conf.InternalServerTLS, err = internalServerTLS.MakeServerTLSConf(); err != nil {
			return err
		}
	}
	return nil
}
