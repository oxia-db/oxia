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

package rangescan

import (
	"context"
	"time"

	"github.com/spf13/cobra"

	"github.com/oxia-db/oxia/cmd/client/common"
	"github.com/oxia-db/oxia/oxia"
)

var (
	Config = flags{}
)

type flags struct {
	keyMin         string
	keyMax         string
	hexDump        bool
	includeVersion bool
	partitionKey   string
	secondaryIndex string
}

func (flags *flags) Reset() {
	flags.keyMin = ""
	flags.keyMax = ""
	flags.hexDump = false
	flags.includeVersion = false
	flags.partitionKey = ""
	flags.secondaryIndex = ""
}

func init() {
	Cmd.Flags().StringVarP(&Config.keyMin, "key-min", "s", "", "Key range minimum (inclusive)")
	Cmd.Flags().StringVarP(&Config.keyMax, "key-max", "e", "", "Key range maximum (exclusive)")
	Cmd.Flags().BoolVarP(&Config.includeVersion, "include-version", "v", false, "Include the record version object")
	Cmd.Flags().BoolVar(&Config.hexDump, "hex", false, "Print the value in HexDump format")
	Cmd.Flags().StringVarP(&Config.partitionKey, "partition-key", "p", "", "Partition Key to be used in override the shard routing")
	Cmd.Flags().StringVar(&Config.secondaryIndex, "index", "", "Secondary Index")
}

var Cmd = &cobra.Command{
	Use:   "range-scan",
	Short: "Scan records",
	Long:  `Scan all the records whose keys are in the specified range.`,
	Args:  cobra.NoArgs,
	RunE:  exec,
}

const lineSeparator = "-------------------------------------------------------------------------------\n"

func exec(cmd *cobra.Command, _ []string) error {
	client, err := common.Config.NewClient()
	if err != nil {
		return err
	}

	var options []oxia.RangeScanOption
	if Config.partitionKey != "" {
		options = append(options, oxia.PartitionKey(Config.partitionKey))
	}
	if Config.secondaryIndex != "" {
		options = append(options, oxia.UseIndex(Config.secondaryIndex))
	}

	if Config.keyMax == "" {
		// By default, do not list internal keys
		Config.keyMax = "__oxia/"
	}

	ch := client.RangeScan(context.Background(), Config.keyMin, Config.keyMax, options...)

	isFirst := true
	for result := range ch {
		if result.Err != nil {
			return result.Err
		}

		if !isFirst {
			_, _ = cmd.OutOrStdout().Write([]byte(lineSeparator))
		}

		isFirst = false
		if Config.hexDump {
			common.WriteHexDump(cmd.OutOrStdout(), result.Value)
		} else {
			common.WriteOutput(cmd.OutOrStdout(), result.Value)
		}

		if Config.includeVersion {
			_, _ = cmd.OutOrStdout().Write([]byte("---\n"))

			version := result.Version
			common.WriteOutput(cmd.OutOrStdout(), common.OutputVersion{
				Key:                result.Key,
				VersionId:          version.VersionId,
				CreatedTimestamp:   time.UnixMilli(int64(version.CreatedTimestamp)),
				ModifiedTimestamp:  time.UnixMilli(int64(version.ModifiedTimestamp)),
				ModificationsCount: version.ModificationsCount,
				Ephemeral:          version.Ephemeral,
				SessionId:          version.SessionId,
				ClientIdentity:     version.ClientIdentity,
			})
		}
	}

	return nil
}
