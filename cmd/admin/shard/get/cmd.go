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

package get

import (
	"strconv"

	"github.com/spf13/cobra"

	"github.com/oxia-db/oxia/cmd/admin/commons"
	shardcli "github.com/oxia-db/oxia/cmd/admin/shard/cli"
	"github.com/oxia-db/oxia/oxia"
)

var namespace string

func init() {
	Cmd.Flags().StringVar(&namespace, "namespace", "default", "Namespace of the shard")
}

var Cmd = &cobra.Command{
	Use:          "get [shard]",
	Short:        "Get a shard or list shards",
	Long:         `Get a shard or list shards`,
	Args:         cobra.MaximumNArgs(1),
	RunE:         exec,
	SilenceUsage: true,
}

func exec(cmd *cobra.Command, args []string) error {
	outputFormat, err := cmd.Flags().GetString("output")
	if err != nil {
		return err
	}
	if err := commons.ValidateOutputFormat(outputFormat); err != nil {
		return err
	}

	client, err := commons.AdminConfig.NewAdminClient()
	if err != nil {
		return err
	}
	defer func(client oxia.AdminClient) {
		_ = client.Close()
	}(client)

	if len(args) == 0 {
		shards, err := client.ListShards(cmd.Context(), namespace)
		if err != nil {
			return err
		}
		return shardcli.WriteShards(cmd.OutOrStdout(), outputFormat, shards)
	}

	shardID, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		return err
	}
	shard, err := client.GetShard(cmd.Context(), namespace, shardID)
	if err != nil {
		return err
	}
	return shardcli.WriteShard(cmd.OutOrStdout(), outputFormat, shard)
}
