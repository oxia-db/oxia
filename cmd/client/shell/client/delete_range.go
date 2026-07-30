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

func (e Executor) deleteRange(args []string) error {
	options, minKey, maxKey, err := parseDeleteRangeCommand(args)
	if err != nil {
		return err
	}
	syncClient, err := e.Client()
	if err != nil {
		return err
	}
	if err := syncClient.DeleteRange(e.Ctx, minKey, maxKey, options...); err != nil {
		return err
	}
	common.WriteOutput(e.Out, "OK")
	return nil
}

func parseDeleteRangeCommand(args []string) (options []oxia.DeleteRangeOption, minKey string, maxKey string, err error) {
	const usage = "usage: delete-range [flags] (--key-min MIN --key-max MAX | MIN_KEY MAX_KEY)"
	flags := pflag.NewFlagSet("delete-range", pflag.ContinueOnError)
	flags.SetOutput(io.Discard)

	var (
		keyMinFlag   string
		keyMaxFlag   string
		partitionKey string
	)
	flags.StringVarP(&keyMinFlag, "key-min", "s", "", "")
	flags.StringVarP(&keyMaxFlag, "key-max", "e", "", "")
	flags.StringVarP(&partitionKey, "partition-key", "p", "", "")
	if err := flags.Parse(args[1:]); err != nil {
		return nil, "", "", errors.New(usage)
	}

	rangeFlagUsed := flags.Changed("key-min") || flags.Changed("key-max")
	switch {
	case rangeFlagUsed && (flags.NArg() != 0 || !flags.Changed("key-min") || !flags.Changed("key-max")):
		return nil, "", "", errors.New(usage)
	case rangeFlagUsed:
		minKey = keyMinFlag
		maxKey = keyMaxFlag
	case flags.NArg() != 2:
		return nil, "", "", errors.New(usage)
	default:
		minKey = flags.Arg(0)
		maxKey = flags.Arg(1)
	}

	if partitionKey != "" {
		options = append(options, oxia.PartitionKey(partitionKey))
	}

	return options, minKey, maxKey, nil
}
