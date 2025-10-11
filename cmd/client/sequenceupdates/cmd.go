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

package sequenceupdates

import (
	"context"
	"log/slog"

	"github.com/spf13/cobra"

	"github.com/oxia-db/oxia/cmd/client/common"
	"github.com/oxia-db/oxia/oxia"
)

var Config = flags{}

type flags struct {
	key          string
	partitionKey string
}

func (flags *flags) Reset() {
	flags.key = ""
	flags.partitionKey = ""
}

func init() {
	Cmd.Flags().StringVarP(&Config.partitionKey, "partition-key", "p", "", "Partition Key to be used in override the shard routing")
	_ = Cmd.MarkFlagRequired("partition-key")
}

var Cmd = &cobra.Command{
	Use:   "sequence-updates [flags] KEY --partition-key PARTITION_KEY",
	Short: "Get key sequences updates",
	Long:  `Follow the updates on a sequence of keys`,
	Args:  cobra.ExactArgs(1),
	RunE:  exec,
}

func exec(_ *cobra.Command, args []string) error {
	client, err := common.Config.NewClient()
	if err != nil {
		return err
	}

	defer client.Close()

	Config.key = args[0]
	updates, err := client.GetSequenceUpdates(context.Background(), Config.key, oxia.PartitionKey(Config.partitionKey))
	if err != nil {
		return err
	}

	for key := range updates {
		slog.Info("", slog.String("key", key))
	}

	return nil
}
