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

package standalone

import (
	"fmt"
	"io"
	"time"

	"github.com/oxia-db/oxia/common/constant"
	oxiadcommonoption "github.com/oxia-db/oxia/oxiad/common/option"
	"github.com/spf13/cobra"

	"github.com/oxia-db/oxia/oxiad/dataserver/database/kvstore"

	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/oxiad/dataserver"
)

var (
	conf = dataserver.StandaloneConfig{}

	Cmd = &cobra.Command{
		Use:   "standalone",
		Short: "Start a standalone service",
		Long:  `Long description`,
		Run:   exec,
	}
)

func init() {
	dataServerOptions := conf.DataServerOptions
	Cmd.Flags().StringVarP(&dataServerOptions.Server.Public.BindAddress, "public-addr", "p", fmt.Sprintf("0.0.0.0:%d", constant.DefaultPublicPort), "Public service bind address")
	observability := &dataServerOptions.Observability
	Cmd.Flags().StringVarP(&observability.Metric.BindAddress, "metrics-addr", "m", fmt.Sprintf("0.0.0.0:%d", oxiadcommonoption.DefaultMetricsPort), "Metrics service bind address")

	Cmd.Flags().BoolVar(&conf.NotificationsEnabled, "notifications-enabled", true, "Whether notifications are enabled")
	Cmd.Flags().Var(&conf.KeySorting, "key-sorting", `Key sorting. allowed: "hierarchical", "natural". Default: "hierarchical"`)
	_ = Cmd.RegisterFlagCompletionFunc("key-sorting", keySortingCompletion)

	Cmd.Flags().Uint32VarP(&conf.NumShards, "shards", "s", 1, "Number of shards")

	storageOptions := &dataServerOptions.Storage
	Cmd.Flags().StringVar(&storageOptions.WAL.Dir, "wal-dir", "./data/wal", "Directory for write-ahead-logs")
	Cmd.Flags().DurationVar(&storageOptions.WAL.Retention, "wal-retention-time", 1*time.Hour, "Retention time for the entries in the write-ahead-log")
	Cmd.Flags().BoolVar(&storageOptions.WAL.Sync, "wal-sync-data", true, "Whether to sync data in write-ahead-log")

	Cmd.Flags().DurationVar(&storageOptions.Notification.Retention, "notifications-retention-time", 1*time.Hour, "Retention time for the db notifications to clients")

	Cmd.Flags().StringVar(&storageOptions.Database.Dir, "data-dir", "./data/db", "Directory where to store data")
	Cmd.Flags().Int64Var(&storageOptions.Database.ReadCacheSizeMB, "db-cache-size-mb", kvstore.DefaultFactoryOptions.CacheSizeMB,
		"Max size of the shared DB cache")
}

func exec(*cobra.Command, []string) {
	process.RunProcess(func() (io.Closer, error) {
		return dataserver.NewStandalone(conf)
	})
}

func keySortingCompletion(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
	return []string{
		"hierarchical\tUse file-system like hierarchical sorting based on `/`",
		"natural\tUse natural, byte-wise sorting",
	}, cobra.ShellCompDirectiveDefault
}
