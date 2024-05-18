// Copyright 2023 StreamNative, Inc.
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
	"context"
	"time"

	"github.com/spf13/cobra"

	"github.com/streamnative/oxia/cmd/client/common"
)

var (
	Config = flags{}
)

type flags struct {
	key            string
	hexDump        bool
	includeVersion bool
}

func (flags *flags) Reset() {
	flags.key = ""
	flags.hexDump = false
	flags.includeVersion = false
}

func init() {
	Cmd.Flags().BoolVarP(&Config.includeVersion, "include-version", "v", false, "Include the record version object")
	Cmd.Flags().BoolVar(&Config.hexDump, "hex", false, "Print the value in HexDump format")
}

var Cmd = &cobra.Command{
	Use:   "get [flags] KEY",
	Short: "Get one record",
	Long:  `Get the values of the recover associated with the given key.`,
	Args:  cobra.ExactArgs(1),
	RunE:  exec,
}

func exec(cmd *cobra.Command, args []string) error {
	client, err := common.Config.NewClient()
	if err != nil {
		return err
	}

	queryKey := args[0]
	key, value, version, err := client.Get(context.Background(), queryKey)
	if err != nil {
		return err
	}

	if Config.hexDump {
		common.WriteHexDump(cmd.OutOrStdout(), value)
	} else {
		common.WriteOutput(cmd.OutOrStdout(), value)
	}

	if Config.includeVersion {
		_, _ = cmd.OutOrStdout().Write([]byte("---\n"))
		common.WriteOutput(cmd.OutOrStdout(), common.OutputVersion{
			Key:                key,
			VersionId:          version.VersionId,
			CreatedTimestamp:   time.UnixMilli(int64(version.CreatedTimestamp)),
			ModifiedTimestamp:  time.UnixMilli(int64(version.ModifiedTimestamp)),
			ModificationsCount: version.ModificationsCount,
			Ephemeral:          version.Ephemeral,
			ClientIdentity:     version.ClientIdentity,
		})
	}
	return nil
}
