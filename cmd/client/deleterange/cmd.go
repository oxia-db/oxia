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

package deleterange

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
	keyMin       string
	keyMax       string
	partitionKey string
}

func (flags *flags) Reset() {
	flags.keyMin = ""
	flags.keyMax = ""
	flags.partitionKey = ""
}

func init() {
	Cmd.Flags().StringVarP(&Config.keyMin, "key-min", "s", "", "Key range minimum (inclusive)")
	Cmd.Flags().StringVarP(&Config.keyMax, "key-max", "e", "", "Key range maximum (exclusive)")
	Cmd.Flags().StringVarP(&Config.partitionKey, "partition-key", "p", "", "Partition Key to be used in override the shard routing")
	err := Cmd.MarkFlagRequired("key-min")
	if err != nil {
		panic(err)
	}
	err = Cmd.MarkFlagRequired("key-max")
	if err != nil {
		panic(err)
	}
}

var Cmd = &cobra.Command{
	Use:   "delete-range",
	Short: "Delete a range of records",
	Long:  `Delete a set of records based on a range of keys`,
	Args:  cobra.NoArgs,
	RunE:  exec,
}

func exec(_ *cobra.Command, _ []string) error {
	client, err := common.Config.NewClient()
	if err != nil {
		return err
	}

	var options []oxia.DeleteRangeOption
	if Config.partitionKey != "" {
		options = append(options, oxia.PartitionKey(Config.partitionKey))
	}

	return client.DeleteRange(context.Background(), Config.keyMin, Config.keyMax, options...)
}
