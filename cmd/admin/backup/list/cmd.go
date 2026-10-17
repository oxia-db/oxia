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

package list

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/oxia-db/oxia/cmd/admin/commons"
	cc "github.com/oxia-db/oxia/cmd/client/common"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia"
)

var (
	namespace   string
	shardId     int64
	destination string
)

func init() {
	Cmd.Flags().StringVar(&namespace, "namespace", "default", "Namespace to list the backups of")
	Cmd.Flags().Int64Var(&shardId, "shard", -1, "Shard to list the backups of (every shard when omitted)")
	Cmd.Flags().StringVar(&destination, "destination", "", "Destination to list, instead of the one of the backup policy")
}

var Cmd = &cobra.Command{
	Use:          "list",
	Short:        "List the backups of a namespace",
	Long:         `List the backups of a namespace, or of one of its shards, oldest first`,
	Args:         cobra.ExactArgs(0),
	RunE:         exec,
	SilenceUsage: true,
}

// Backup is a backup in the output of the command.
type Backup struct {
	Shard        int64  `json:"shard" yaml:"shard"`
	BackupID     string `json:"backupId" yaml:"backupId"`
	Time         string `json:"time" yaml:"time"`
	CommitOffset int64  `json:"commitOffset" yaml:"commitOffset"`
	Term         int64  `json:"term" yaml:"term"`
	HashRangeMin uint32 `json:"hashRangeMin" yaml:"hashRangeMin"`
	HashRangeMax uint32 `json:"hashRangeMax" yaml:"hashRangeMax"`
	SizeBytes    uint64 `json:"sizeBytes" yaml:"sizeBytes"`
	SSTCount     uint32 `json:"sstCount" yaml:"sstCount"`
}

// Result is the output of the command.
type Result struct {
	Backups []Backup `json:"backups" yaml:"backups"`
}

func exec(cmd *cobra.Command, _ []string) error {
	client, err := commons.AdminConfig.NewAdminClient()
	if err != nil {
		return err
	}
	defer func(client oxia.AdminClient) {
		_ = client.Close()
	}(client)

	var shard *int64
	if cmd.Flags().Changed("shard") {
		shard = &shardId
	}
	backups, err := client.ListBackups(cmd.Context(), namespace, shard, destination)
	if err != nil {
		return err
	}
	cc.WriteOutput(cmd.OutOrStdout(), toResult(backups))
	return nil
}

func toResult(backups []*proto.BackupInfo) Result {
	result := Result{Backups: make([]Backup, 0, len(backups))}
	for _, b := range backups {
		result.Backups = append(result.Backups, Backup{
			Shard:        b.GetShard(),
			BackupID:     b.GetBackupId(),
			Time:         time.UnixMilli(b.GetTimestampMillis()).UTC().Format(time.RFC3339),
			CommitOffset: b.GetCommitOffset(),
			Term:         b.GetTerm(),
			HashRangeMin: b.GetHashRange().GetMin(),
			HashRangeMax: b.GetHashRange().GetMax(),
			SizeBytes:    b.GetSizeBytes(),
			SSTCount:     b.GetSstCount(),
		})
	}
	return result
}
