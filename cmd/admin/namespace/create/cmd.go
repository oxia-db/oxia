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

package create

import (
	"errors"

	"github.com/spf13/cobra"

	"github.com/oxia-db/oxia/cmd/admin/commons"
	namespaceoutput "github.com/oxia-db/oxia/cmd/admin/namespace/output"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/common/validation"
	"github.com/oxia-db/oxia/oxia"
)

const (
	initialShardsFlagName     = "initial-shards"
	replicationFactorFlagName = "replication-factor"
	notificationsFlagName     = "notifications"
	keySortingFlagName        = "key-sorting"
)

var fields namespaceFields

var Cmd = &cobra.Command{
	Use:          "create <namespace> --initial-shards <count> --replication-factor <factor>",
	Short:        "Create a namespace",
	Long:         `Create a namespace`,
	Args:         cobra.ExactArgs(1),
	RunE:         exec,
	SilenceUsage: true,
}

func init() {
	fields.addFlags(Cmd)
	_ = Cmd.MarkFlagRequired(initialShardsFlagName)
	_ = Cmd.MarkFlagRequired(replicationFactorFlagName)
}

type namespaceFields struct {
	initialShardCount uint32
	replicationFactor uint32
	notifications     bool
	keySorting        string
}

func (f *namespaceFields) addFlags(cmd *cobra.Command) {
	f.notifications = true
	f.keySorting = "hierarchical"
	cmd.Flags().Uint32Var(&f.initialShardCount, initialShardsFlagName, 0, "Initial shard count for the namespace")
	cmd.Flags().Uint32Var(&f.replicationFactor, replicationFactorFlagName, 0, "Replication factor for the namespace")
	cmd.Flags().BoolVar(&f.notifications, notificationsFlagName, true, "Whether notifications are enabled")
	cmd.Flags().StringVar(&f.keySorting, keySortingFlagName, f.keySorting, `Key sorting. allowed: "hierarchical", "natural"`)
	_ = cmd.RegisterFlagCompletionFunc(keySortingFlagName, keySortingCompletion)
}

func (f *namespaceFields) reset() {
	f.initialShardCount = 0
	f.replicationFactor = 0
	f.notifications = true
	f.keySorting = "hierarchical"
}

func exec(cmd *cobra.Command, args []string) error {
	outputFormat, err := cmd.Flags().GetString("output")
	if err != nil {
		return err
	}
	if err := commons.ValidateOutputFormat(outputFormat); err != nil {
		return err
	}

	name := args[0]
	if err := validation.ValidateNamespace(name); err != nil {
		return err
	}
	if fields.initialShardCount == 0 {
		return errors.New("namespace initial shard count must be greater than 0")
	}
	if fields.replicationFactor == 0 {
		return errors.New("namespace replication factor must be greater than 0")
	}
	keySorting, err := proto.ParseKeySortingType(fields.keySorting)
	if err != nil {
		return err
	}
	if keySorting == proto.KeySortingType_UNKNOWN {
		return errors.New(`key sorting must be one of "natural" or "hierarchical"`)
	}

	client, err := commons.AdminConfig.NewAdminClient()
	if err != nil {
		return err
	}
	defer func(client oxia.AdminClient) {
		_ = client.Close()
	}(client)

	created, err := client.CreateNamespace(&proto.Namespace{
		Name:                 name,
		InitialShardCount:    fields.initialShardCount,
		ReplicationFactor:    fields.replicationFactor,
		NotificationsEnabled: &fields.notifications,
		KeySorting:           fields.keySorting,
	})
	if err != nil {
		return err
	}

	return namespaceoutput.WriteNamespace(cmd.OutOrStdout(), outputFormat, created)
}

func keySortingCompletion(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
	return []string{
		"hierarchical\tUse file-system like hierarchical sorting based on `/`",
		"natural\tUse natural, byte-wise sorting",
	}, cobra.ShellCompDirectiveDefault
}
