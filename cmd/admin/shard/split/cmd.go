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

package split

import (
	"github.com/spf13/cobra"

	"github.com/oxia-db/oxia/cmd/admin/commons"
	cc "github.com/oxia-db/oxia/cmd/client/common"
	"github.com/oxia-db/oxia/oxia"
)

var (
	namespace  string
	shardId    int64
	splitPoint uint32
)

func init() {
	Cmd.Flags().StringVar(&namespace, "namespace", "default", "Namespace of the shard to split")
	Cmd.Flags().Int64Var(&shardId, "shard", -1, "Shard ID to split")
	Cmd.Flags().Uint32Var(&splitPoint, "split-point", 0, "Explicit hash split point (optional, defaults to midpoint)")

	_ = Cmd.MarkFlagRequired("shard")
}

var Cmd = &cobra.Command{
	Use:          "split",
	Short:        "Split a shard into two children",
	Long:         `Split a shard into two children at the specified split point (or midpoint if not specified)`,
	Args:         cobra.ExactArgs(0),
	RunE:         exec,
	SilenceUsage: true,
}

func exec(cmd *cobra.Command, _ []string) error {
	client, err := commons.AdminConfig.NewAdminClient()
	if err != nil {
		return err
	}
	defer func(client oxia.AdminClient) {
		_ = client.Close()
	}(client)

	var sp *uint32
	if cmd.Flags().Changed("split-point") {
		sp = &splitPoint
	}

	result := client.SplitShard(namespace, shardId, sp)
	if result.Error != nil {
		return result.Error
	}

	cc.WriteOutput(cmd.OutOrStdout(), result)
	return nil
}
