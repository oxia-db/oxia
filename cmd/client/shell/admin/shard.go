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

package admin

import (
	"errors"
	"fmt"
	"io"

	"github.com/spf13/pflag"

	"github.com/oxia-db/oxia/cmd/client/common"
	"github.com/oxia-db/oxia/common/validation"
	"github.com/oxia-db/oxia/oxia"
)

func (e Executor) shard(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: admin shard split --shard ID [--namespace NAME] [--split-point HASH]")
	}

	switch args[0] {
	case "split":
		namespace, shardID, splitPoint, err := parseShardSplitCommand(args)
		if err != nil {
			return err
		}
		admin, err := e.Client()
		if err != nil {
			return err
		}
		result := admin.SplitShard(e.Ctx, namespace, shardID, splitPoint)
		if result.Error != nil {
			return result.Error
		}
		common.WriteOutput(e.Out, result)
		return nil
	default:
		return fmt.Errorf("unknown admin shard command %q", args[0])
	}
}

func parseShardSplitCommand(args []string) (string, int64, *uint32, error) {
	const usage = "admin shard split --shard ID [--namespace NAME] [--split-point HASH]"
	flags := pflag.NewFlagSet("shard-split", pflag.ContinueOnError)
	flags.SetOutput(io.Discard)

	var (
		namespace  string
		shardID    int64
		splitPoint uint32
	)
	flags.StringVar(&namespace, "namespace", oxia.DefaultNamespace, "")
	flags.Int64Var(&shardID, "shard", -1, "")
	flags.Uint32Var(&splitPoint, "split-point", 0, "")
	if err := flags.Parse(args[1:]); err != nil {
		return "", 0, nil, fmt.Errorf("usage: %s", usage)
	}
	if flags.NArg() != 0 || !flags.Changed("shard") {
		return "", 0, nil, fmt.Errorf("usage: %s", usage)
	}
	if err := validation.ValidateNamespace(namespace); err != nil {
		return "", 0, nil, err
	}

	var splitPointPtr *uint32
	if flags.Changed("split-point") {
		splitPointPtr = &splitPoint
	}
	return namespace, shardID, splitPointPtr, nil
}
