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

package restore

import (
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/spf13/cobra"

	"github.com/oxia-db/oxia/cmd/admin/commons"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia"
)

var (
	namespace   string
	asOf        string
	toNamespace string
	destination string
)

func init() {
	Cmd.Flags().StringVar(&namespace, "namespace", "", "Namespace to restore")
	Cmd.Flags().StringVar(&asOf, "as-of", "", "Time to restore as of, in RFC 3339, e.g. 2026-10-03T12:00:00Z")
	Cmd.Flags().StringVar(&toNamespace, "to-namespace", "", "New namespace to restore into")
	Cmd.Flags().StringVar(&destination, "from", "", "Destination to restore from, instead of the one of the backup policy")
	_ = Cmd.MarkFlagRequired("namespace")
	_ = Cmd.MarkFlagRequired("as-of")
	_ = Cmd.MarkFlagRequired("to-namespace")
}

var Cmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore a namespace from its backups into a new namespace",
	Long: `Restore a namespace as of a point in time into a new namespace, from the newest backup of each part of its
hash range at that time. The new namespace serves once its shards have installed their backups and elected their
leaders. The source namespace is left untouched.`,
	Args:         cobra.ExactArgs(0),
	RunE:         exec,
	SilenceUsage: true,
}

// Shard is a shard of the restored namespace in the output of the command.
type Shard struct {
	Shard        int64  `json:"shard" yaml:"shard"`
	HashRangeMin uint32 `json:"hashRangeMin" yaml:"hashRangeMin"`
	HashRangeMax uint32 `json:"hashRangeMax" yaml:"hashRangeMax"`
	SourceShard  int64  `json:"sourceShard" yaml:"sourceShard"`
	BackupID     string `json:"backupId" yaml:"backupId"`
}

// Result is the output of the command.
type Result struct {
	Namespace string  `json:"namespace" yaml:"namespace"`
	Shards    []Shard `json:"shards" yaml:"shards"`
}

func exec(cmd *cobra.Command, _ []string) error {
	at, err := time.Parse(time.RFC3339, asOf)
	if err != nil {
		return fmt.Errorf("invalid --as-of time: %w", err)
	}
	outputFormat, err := cmd.Flags().GetString("output")
	if err != nil {
		return err
	}
	if err := commons.ValidateOutputFormat(outputFormat); err != nil {
		return err
	}
	client, err := commons.AdminConfig.NewAdminClient()
	if err != nil {
		return err
	}
	defer func(client oxia.AdminClient) {
		_ = client.Close()
	}(client)

	shards, err := client.RestoreNamespace(cmd.Context(), namespace, at, toNamespace, destination)
	if err != nil {
		return err
	}
	return writeResult(cmd.OutOrStdout(), outputFormat, toResult(toNamespace, shards))
}

// writeResult writes the shards of the restored namespace in the output
// format: a table, or the result as JSON or YAML.
func writeResult(out io.Writer, format string, result Result) error {
	if commons.NormalizeOutputFormat(format) != commons.OutputTable {
		return commons.WriteStructuredOutput(out, format, result)
	}
	rows := make([][]string, 0, len(result.Shards))
	for _, s := range result.Shards {
		rows = append(rows, []string{result.Namespace, strconv.FormatInt(s.Shard, 10),
			fmt.Sprintf("%d-%d", s.HashRangeMin, s.HashRangeMax), strconv.FormatInt(s.SourceShard, 10), s.BackupID})
	}
	return commons.WriteTable(out, []string{"NAMESPACE", "SHARD", "HASH RANGE", "SOURCE SHARD", "BACKUP ID"}, rows)
}

func toResult(namespace string, shards []*proto.RestoredShard) Result {
	result := Result{Namespace: namespace, Shards: make([]Shard, 0, len(shards))}
	for _, s := range shards {
		result.Shards = append(result.Shards, Shard{
			Shard:        s.GetShard(),
			HashRangeMin: s.GetHashRange().GetMin(),
			HashRangeMax: s.GetHashRange().GetMax(),
			SourceShard:  s.GetSourceShard(),
			BackupID:     s.GetBackupId(),
		})
	}
	return result
}
