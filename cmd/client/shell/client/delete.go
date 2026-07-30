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

package client

import (
	"errors"
	"io"

	"github.com/spf13/pflag"

	"github.com/oxia-db/oxia/cmd/client/common"
	"github.com/oxia-db/oxia/oxia"
)

func (e Executor) delete(args []string) error {
	options, key, err := parseDeleteCommand(args)
	if err != nil {
		return err
	}
	syncClient, err := e.Client()
	if err != nil {
		return err
	}
	if err := syncClient.Delete(e.Ctx, key, options...); err != nil {
		return err
	}
	common.WriteOutput(e.Out, "OK")
	return nil
}

func parseDeleteCommand(args []string) ([]oxia.DeleteOption, string, error) {
	flags := pflag.NewFlagSet("delete", pflag.ContinueOnError)
	flags.SetOutput(io.Discard)

	var (
		expectedVersion int64
		partitionKey    string
	)
	flags.Int64VarP(&expectedVersion, "expected-version", "e", -1, "")
	flags.StringVarP(&partitionKey, "partition-key", "p", "", "")
	if err := flags.Parse(args[1:]); err != nil {
		return nil, "", errors.New("usage: delete [flags] KEY")
	}
	if flags.NArg() != 1 {
		return nil, "", errors.New("usage: delete [flags] KEY")
	}

	var options []oxia.DeleteOption
	if expectedVersion >= 0 {
		options = append(options, oxia.ExpectedVersionId(expectedVersion))
	}
	if partitionKey != "" {
		options = append(options, oxia.PartitionKey(partitionKey))
	}

	return options, flags.Arg(0), nil
}
