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

package del

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/oxia-db/oxia/cmd/client/common"
	"github.com/oxia-db/oxia/oxia"
)

var (
	Config = flags{}
)

type flags struct {
	expectedVersion int64
	partitionKey    string
}

func (flags *flags) Reset() {
	flags.expectedVersion = -1
}

func init() {
	Cmd.Flags().Int64VarP(&Config.expectedVersion, "expected-version", "e", -1, "Version of entry expected to be on the server")
	Cmd.Flags().StringVarP(&Config.partitionKey, "partition-key", "p", "", "Partition Key to be used in override the shard routing")
}

var Cmd = &cobra.Command{
	Use:          "delete [flags] KEY",
	Short:        "Delete one record",
	Long:         `Delete the record with the given key, if they exists. If an expected version is provided, the delete will only take place if it matches the version of the current record on the server`,
	Args:         cobra.ExactArgs(1),
	RunE:         exec,
	SilenceUsage: true,
}

func exec(_ *cobra.Command, args []string) error {
	client, err := common.Config.NewClient()
	if err != nil {
		return err
	}

	key := args[0]

	var options []oxia.DeleteOption
	if Config.expectedVersion >= 0 {
		options = append(options, oxia.ExpectedVersionId(Config.expectedVersion))
	}
	if Config.partitionKey != "" {
		options = append(options, oxia.PartitionKey(Config.partitionKey))
	}

	return client.Delete(context.Background(), key, options...)
}
