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

package backup

import (
	"github.com/spf13/cobra"

	"github.com/oxia-db/oxia/cmd/admin/backup/list"
	"github.com/oxia-db/oxia/cmd/admin/commons"
	cc "github.com/oxia-db/oxia/cmd/client/common"
	"github.com/oxia-db/oxia/oxia"
)

var (
	namespace string
	shardId   int64
)

func init() {
	Cmd.Flags().StringVar(&namespace, "namespace", "", "Namespace to back up (all of them when omitted)")
	Cmd.Flags().Int64Var(&shardId, "shard", -1, "Shard to back up (every shard of the namespace when omitted)")
	Cmd.AddCommand(list.Cmd)
}

var Cmd = &cobra.Command{
	Use:          "backup",
	Short:        "Back up the shards now",
	Long:         `Make the backups of the selected shards due now, e.g. before an upgrade. The backups run in the background.`,
	Args:         cobra.ExactArgs(0),
	RunE:         exec,
	SilenceUsage: true,
}

// Result is the output of the command.
type Result struct {
	Shards uint32 `json:"shards" yaml:"shards"`
}

func exec(cmd *cobra.Command, _ []string) error {
	client, err := commons.AdminConfig.NewAdminClient()
	if err != nil {
		return err
	}
	defer func(client oxia.AdminClient) {
		_ = client.Close()
	}(client)

	var shard *int64
	if cmd.Flags().Changed("shard") {
		shard = &shardId
	}
	shards, err := client.RequestBackup(cmd.Context(), namespace, shard)
	if err != nil {
		return err
	}
	cc.WriteOutput(cmd.OutOrStdout(), Result{Shards: shards})
	return nil
}
