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

type getCommandOptions struct {
	clientOptions  []oxia.GetOption
	hexDump        bool
	includeVersion bool
}

func (e Executor) get(args []string) error {
	options, key, err := parseGetCommand(args)
	if err != nil {
		return err
	}

	syncClient, err := e.Client()
	if err != nil {
		return err
	}
	storedKey, value, version, err := syncClient.Get(e.Ctx, key, options.clientOptions...)
	if err != nil {
		return err
	}
	if options.hexDump {
		common.WriteHexDump(e.Out, value)
	} else {
		common.WriteOutput(e.Out, value)
	}
	if options.includeVersion {
		if _, err := e.Out.Write([]byte("---\n")); err != nil {
			return err
		}
		common.WriteOutput(e.Out, outputVersion(storedKey, version))
	}
	return nil
}

func parseGetCommand(args []string) (getCommandOptions, string, error) {
	flags := pflag.NewFlagSet("get", pflag.ContinueOnError)
	flags.SetOutput(io.Discard)

	var (
		hexDump        bool
		includeVersion bool
		partitionKey   string
		index          string
		comparisonType string
	)
	flags.BoolVar(&hexDump, "hex", false, "")
	flags.BoolVarP(&includeVersion, "include-version", "v", false, "")
	flags.StringVarP(&partitionKey, "partition-key", "p", "", "")
	flags.StringVar(&index, "index", "", "")
	flags.StringVarP(&comparisonType, "comparison-type", "t", "equal", "")
	if err := flags.Parse(args[1:]); err != nil {
		return getCommandOptions{}, "", errors.New("usage: get [flags] KEY")
	}
	if flags.NArg() != 1 {
		return getCommandOptions{}, "", errors.New("usage: get [flags] KEY")
	}

	var options []oxia.GetOption
	if partitionKey != "" {
		options = append(options, oxia.PartitionKey(partitionKey))
	}
	if index != "" {
		options = append(options, oxia.UseIndex(index))
	}
	options, err := addComparisonOption(options, comparisonType)
	if err != nil {
		return getCommandOptions{}, "", err
	}

	return getCommandOptions{
		clientOptions:  options,
		hexDump:        hexDump,
		includeVersion: includeVersion,
	}, flags.Arg(0), nil
}

func addComparisonOption(options []oxia.GetOption, comparisonType string) ([]oxia.GetOption, error) {
	switch comparisonType {
	case "equal":
		return options, nil
	case "floor":
		return append(options, oxia.ComparisonFloor()), nil
	case "ceiling":
		return append(options, oxia.ComparisonCeiling()), nil
	case "lower":
		return append(options, oxia.ComparisonLower()), nil
	case "higher":
		return append(options, oxia.ComparisonHigher()), nil
	default:
		return nil, fmt.Errorf("invalid comparison type: %s", comparisonType)
	}
}
