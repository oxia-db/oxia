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

func (e Executor) list(args []string) error {
	options, minKey, maxKey, err := parseListCommand(args)
	if err != nil {
		return err
	}

	syncClient, err := e.Client()
	if err != nil {
		return err
	}
	keys, err := syncClient.List(e.Ctx, minKey, maxKey, options...)
	if err != nil {
		return err
	}
	common.WriteOutput(e.Out, keys)
	return nil
}

func parseListCommand(args []string) (options []oxia.ListOption, minKey string, maxKey string, err error) {
	flags := pflag.NewFlagSet("list", pflag.ContinueOnError)
	flags.SetOutput(io.Discard)

	var (
		keyMinFlag       string
		keyMaxFlag       string
		partitionKey     string
		index            string
		showInternalKeys bool
	)
	flags.StringVarP(&keyMinFlag, "key-min", "s", "", "")
	flags.StringVarP(&keyMaxFlag, "key-max", "e", "", "")
	flags.StringVarP(&partitionKey, "partition-key", "p", "", "")
	flags.StringVar(&index, "index", "", "")
	flags.BoolVar(&showInternalKeys, "internal-keys", false, "")
	if err := flags.Parse(args[1:]); err != nil {
		return nil, "", "", errors.New("usage: list [flags] [MIN_KEY [MAX_KEY]]")
	}

	rangeFlagUsed := flags.Changed("key-min") || flags.Changed("key-max")
	if rangeFlagUsed && len(flags.Args()) > 0 {
		return nil, "", "", errors.New("usage: list [flags] [MIN_KEY [MAX_KEY]]")
	}

	minKey = keyMinFlag
	maxKey = keyMaxFlag
	switch remaining := flags.Args(); len(remaining) {
	case 0:
	case 1:
		minKey = remaining[0]
	case 2:
		minKey = remaining[0]
		maxKey = remaining[1]
	default:
		return nil, "", "", errors.New("usage: list [flags] [MIN_KEY [MAX_KEY]]")
	}

	if partitionKey != "" {
		options = append(options, oxia.PartitionKey(partitionKey))
	}
	if index != "" {
		options = append(options, oxia.UseIndex(index))
	}
	if showInternalKeys {
		options = append(options, oxia.ShowInternalKeys(true))
	}

	return options, minKey, maxKey, nil
}
