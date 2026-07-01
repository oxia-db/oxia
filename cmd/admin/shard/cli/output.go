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

package cli

import (
	"fmt"
	"io"
	"strings"

	"github.com/pkg/errors"

	"github.com/oxia-db/oxia/cmd/admin/commons"
	"github.com/oxia-db/oxia/common/proto"
)

func WriteShard(out io.Writer, format string, shard *proto.ShardView) error {
	if shard == nil {
		return errors.New("shard must not be nil")
	}

	if err := commons.ValidateOutputFormat(format); err != nil {
		return err
	}

	format = commons.NormalizeOutputFormat(format)
	switch format {
	case commons.OutputJSON, commons.OutputYAML:
		return commons.WriteStructuredOutput(out, format, shard)
	case commons.OutputTable:
		return writeShardTable(out, []*proto.ShardView{shard})
	default:
		return errors.Errorf("unsupported output format %q", format)
	}
}

func WriteShards(out io.Writer, format string, shards []*proto.ShardView) error {
	if err := commons.ValidateOutputFormat(format); err != nil {
		return err
	}
	for _, shard := range shards {
		if shard == nil {
			return errors.New("shard must not be nil")
		}
	}

	format = commons.NormalizeOutputFormat(format)
	switch format {
	case commons.OutputJSON, commons.OutputYAML:
		return commons.WriteStructuredOutput(out, format, shards)
	case commons.OutputTable:
		return writeShardTable(out, shards)
	default:
		return errors.Errorf("unsupported output format %q", format)
	}
}

func writeShardTable(out io.Writer, shards []*proto.ShardView) error {
	tw := commons.NewTableWriter(out)
	if _, err := fmt.Fprintln(tw, "NAMESPACE\tSHARD\tSTATUS\tTERM\tLEADER\tENSEMBLE\tRANGE"); err != nil {
		return err
	}
	for _, shard := range shards {
		if shard == nil {
			continue
		}
		status := shard.GetShardStatus()
		if _, err := fmt.Fprintf(tw, "%s\t%d\t%s\t%d\t%s\t%s\t%s\n",
			shard.GetNamespace(),
			shard.GetShard(),
			status.GetStatus().String(),
			status.GetTerm(),
			formatIdentity(status.GetLeader()),
			formatEnsemble(status.GetEnsemble()),
			formatHashRange(status.GetInt32HashRange()),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func formatIdentity(identity *proto.DataServerIdentity) string {
	if identity == nil {
		return ""
	}
	return identity.GetNameOrDefault()
}

func formatEnsemble(ensemble []*proto.DataServerIdentity) string {
	if len(ensemble) == 0 {
		return ""
	}
	names := make([]string, 0, len(ensemble))
	for _, identity := range ensemble {
		if identity == nil {
			continue
		}
		names = append(names, identity.GetNameOrDefault())
	}
	return strings.Join(names, ",")
}

func formatHashRange(hashRange *proto.HashRange) string {
	if hashRange == nil {
		return ""
	}
	return fmt.Sprintf("%d-%d", hashRange.GetMin(), hashRange.GetMax())
}
