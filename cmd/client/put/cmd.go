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

package put

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/spf13/cobra"

	"github.com/streamnative/oxia/oxia"

	"github.com/streamnative/oxia/cmd/client/common"
)

var (
	Config = flags{}
)

type flags struct {
	expectedVersion    int64
	readValueFromStdIn bool
}

func (flags *flags) Reset() {
	flags.expectedVersion = -1
	flags.readValueFromStdIn = false
}

func init() {
	Cmd.Flags().Int64VarP(&Config.expectedVersion, "expected-version", "e", -1, "Version of entry expected to be on the server")
	Cmd.Flags().BoolVarP(&Config.readValueFromStdIn, "std-in", "c", false, "Read value from stdin")
}

var Cmd = &cobra.Command{
	Use:          "put [flags] KEY [VALUE]",
	Short:        "Put value",
	Long:         `Put a value and associated it with the given key, either inserting a new entry or updating the existing one. If an expected version is provided, the put will only take place if it matches the version of the current record on the server`,
	Args:         cobra.RangeArgs(1, 2),
	RunE:         exec,
	SilenceUsage: true,
}

func exec(cmd *cobra.Command, args []string) error {
	client, err := common.Config.NewClient()
	if err != nil {
		return err
	}

	key := args[0]
	var value []byte
	if len(args) == 2 { //nolint:gocritic
		// We have a value specified as argument
		if Config.readValueFromStdIn {
			return errors.New("the value can either be provided as argument or read from std-in")
		}
		value = []byte(args[1])
	} else if len(args) == 1 && !Config.readValueFromStdIn {
		return errors.New("no value provided for the record")
	} else {
		if value, err = io.ReadAll(cmd.InOrStdin()); err != nil {
			return err
		}
	}

	var options []oxia.PutOption
	if Config.expectedVersion != -1 {
		options = append(options, oxia.ExpectedVersionId(Config.expectedVersion))
	}

	key, version, err := client.Put(context.Background(), key, value, options...)
	if err != nil {
		return err
	}

	common.WriteOutput(cmd.OutOrStdout(), common.OutputVersion{
		Key:                key,
		VersionId:          version.VersionId,
		CreatedTimestamp:   time.UnixMilli(int64(version.CreatedTimestamp)),
		ModifiedTimestamp:  time.UnixMilli(int64(version.ModifiedTimestamp)),
		ModificationsCount: version.ModificationsCount,
		Ephemeral:          version.Ephemeral,
		ClientIdentity:     version.ClientIdentity,
	})
	return nil
}
