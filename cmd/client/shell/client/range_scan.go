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
	"fmt"
	"io"

	"github.com/spf13/pflag"

	"github.com/oxia-db/oxia/cmd/client/common"
	"github.com/oxia-db/oxia/oxia"
)

type scanCommandOptions struct {
	clientOptions  []oxia.RangeScanOption
	hexDump        bool
	includeVersion bool
}

func (e Executor) rangeScan(args []string) error {
	options, minKey, maxKey, err := parseRangeScanCommand(args)
	if err != nil {
		return err
	}

	syncClient, err := e.Client()
	if err != nil {
		return err
	}
	for result := range syncClient.RangeScan(e.Ctx, minKey, maxKey, options.clientOptions...) {
		if result.Err != nil {
			return result.Err
		}
		if options.hexDump {
			if _, err := fmt.Fprintf(e.Out, "%s\n", result.Key); err != nil {
				return err
			}
			common.WriteHexDump(e.Out, result.Value)
		} else {
			if _, err := fmt.Fprintf(e.Out, "%s\t%s\n", result.Key, result.Value); err != nil {
				return err
			}
		}
		if options.includeVersion {
			if _, err := e.Out.Write([]byte("---\n")); err != nil {
				return err
			}
			common.WriteOutput(e.Out, outputVersion(result.Key, result.Version))
		}
	}
	return nil
}

func parseRangeScanCommand(args []string) (options scanCommandOptions, minKey string, maxKey string, err error) {
	flags := pflag.NewFlagSet("range-scan", pflag.ContinueOnError)
	flags.SetOutput(io.Discard)

	var (
		keyMinFlag       string
		keyMaxFlag       string
		hexDump          bool
		includeVersion   bool
		partitionKey     string
		index            string
		showInternalKeys bool
	)
	flags.StringVarP(&keyMinFlag, "key-min", "s", "", "")
	flags.StringVarP(&keyMaxFlag, "key-max", "e", "", "")
	flags.BoolVar(&hexDump, "hex", false, "")
	flags.BoolVarP(&includeVersion, "include-version", "v", false, "")
	flags.StringVarP(&partitionKey, "partition-key", "p", "", "")
	flags.StringVar(&index, "index", "", "")
	flags.BoolVar(&showInternalKeys, "internal-keys", false, "")
	if err := flags.Parse(args[1:]); err != nil {
		return scanCommandOptions{}, "", "", errors.New("usage: range-scan [flags] [MIN_KEY [MAX_KEY]]")
	}

	rangeFlagUsed := flags.Changed("key-min") || flags.Changed("key-max")
	if rangeFlagUsed && len(flags.Args()) > 0 {
		return scanCommandOptions{}, "", "", errors.New("usage: range-scan [flags] [MIN_KEY [MAX_KEY]]")
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
		return scanCommandOptions{}, "", "", errors.New("usage: range-scan [flags] [MIN_KEY [MAX_KEY]]")
	}

	var clientOptions []oxia.RangeScanOption
	if partitionKey != "" {
		clientOptions = append(clientOptions, oxia.PartitionKey(partitionKey))
	}
	if index != "" {
		clientOptions = append(clientOptions, oxia.UseIndex(index))
	}
	if showInternalKeys {
		clientOptions = append(clientOptions, oxia.ShowInternalKeys(true))
	}

	return scanCommandOptions{
		clientOptions:  clientOptions,
		hexDump:        hexDump,
		includeVersion: includeVersion,
	}, minKey, maxKey, nil
}
