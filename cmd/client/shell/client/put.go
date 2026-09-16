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
	"strconv"
	"strings"

	"github.com/spf13/pflag"

	"github.com/oxia-db/oxia/cmd/client/common"
	"github.com/oxia-db/oxia/oxia"
)

type putCommandFlags struct {
	expectedVersion    int64
	createOnly         bool
	ephemeral          bool
	partitionKey       string
	sequenceKeysDeltas string
	secondaryIndexes   []string
}

func (e Executor) put(args []string) error {
	options, key, value, err := parsePutCommand(args)
	if err != nil {
		return err
	}

	syncClient, err := e.Client()
	if err != nil {
		return err
	}
	key, version, err := syncClient.Put(e.Ctx, key, []byte(value), options...)
	if err != nil {
		return err
	}
	common.WriteOutput(e.Out, outputVersion(key, version))
	return nil
}

func parsePutCommand(args []string) (options []oxia.PutOption, key string, value string, err error) {
	flags := pflag.NewFlagSet("put", pflag.ContinueOnError)
	flags.SetOutput(io.Discard)

	var values putCommandFlags
	flags.Int64VarP(&values.expectedVersion, "expected-version", "e", -1, "")
	flags.BoolVar(&values.createOnly, "create-only", false, "")
	flags.BoolVar(&values.ephemeral, "ephemeral", false, "")
	flags.StringVarP(&values.partitionKey, "partition-key", "p", "", "")
	flags.StringVarP(&values.sequenceKeysDeltas, "sequence-keys-deltas", "d", "", "")
	flags.StringArrayVar(&values.secondaryIndexes, "index", nil, "")
	if err := flags.Parse(args[1:]); err != nil {
		return nil, "", "", errors.New("usage: put [flags] KEY VALUE")
	}
	if flags.NArg() < 2 {
		return nil, "", "", errors.New("usage: put [flags] KEY VALUE")
	}
	if values.expectedVersion >= 0 && values.createOnly {
		return nil, "", "", errors.New("expected-version and create-only cannot both be set")
	}

	options, err = values.options()
	if err != nil {
		return nil, "", "", err
	}
	return options, flags.Arg(0), strings.Join(flags.Args()[1:], " "), nil
}

func (f putCommandFlags) options() ([]oxia.PutOption, error) {
	if !f.hasOptions() {
		return nil, nil
	}

	options := make([]oxia.PutOption, 0, 5+len(f.secondaryIndexes))
	if f.expectedVersion >= 0 {
		options = append(options, oxia.ExpectedVersionId(f.expectedVersion))
	}
	if f.createOnly {
		options = append(options, oxia.ExpectedRecordNotExists())
	}
	if f.ephemeral {
		options = append(options, oxia.Ephemeral())
	}
	if f.partitionKey != "" {
		options = append(options, oxia.PartitionKey(f.partitionKey))
	}
	if f.sequenceKeysDeltas != "" {
		deltas, err := parseUint64CSV(f.sequenceKeysDeltas)
		if err != nil {
			return nil, err
		}
		options = append(options, oxia.SequenceKeysDeltas(deltas...))
	}
	for _, secondaryIndex := range f.secondaryIndexes {
		name, secondaryKey, ok := strings.Cut(secondaryIndex, "=")
		if !ok || name == "" {
			return nil, errors.New("secondary index must be in NAME=KEY format")
		}
		options = append(options, oxia.SecondaryIndex(name, secondaryKey))
	}

	return options, nil
}

func (f putCommandFlags) hasOptions() bool {
	return f.expectedVersion >= 0 ||
		f.createOnly ||
		f.ephemeral ||
		f.partitionKey != "" ||
		f.sequenceKeysDeltas != "" ||
		len(f.secondaryIndexes) > 0
}

func parseUint64CSV(value string) ([]uint64, error) {
	parts := strings.Split(value, ",")
	deltas := make([]uint64, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			return nil, fmt.Errorf("invalid sequence key delta %q", part)
		}
		delta, err := strconv.ParseUint(part, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid sequence key delta %q", part)
		}
		deltas = append(deltas, delta)
	}
	return deltas, nil
}
