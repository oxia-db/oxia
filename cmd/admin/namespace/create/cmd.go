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
	"github.com/oxia-db/oxia/cmd/admin/namespace/option"
	namespaceoutput "github.com/oxia-db/oxia/cmd/admin/namespace/output"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/common/validation"
	"github.com/oxia-db/oxia/oxia"
)

var fields option.NamespaceFields

var Cmd = &cobra.Command{
	Use:          "create <namespace> --initial-shards <count> --replication-factor <factor>",
	Short:        "Create a namespace",
	Long:         `Create a namespace`,
	Args:         cobra.ExactArgs(1),
	RunE:         exec,
	SilenceUsage: true,
}

func init() {
	fields.AddFlags(Cmd)
	_ = Cmd.MarkFlagRequired(option.InitialShardsFlagName)
	_ = Cmd.MarkFlagRequired(option.ReplicationFactorFlagName)
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
	if fields.InitialShardCount == 0 {
		return errors.New("namespace initial shard count must be greater than 0")
	}
	if fields.ReplicationFactor == 0 {
		return errors.New("namespace replication factor must be greater than 0")
	}
	keySorting, err := proto.ParseKeySortingType(fields.KeySorting)
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
		InitialShardCount:    fields.InitialShardCount,
		ReplicationFactor:    fields.ReplicationFactor,
		NotificationsEnabled: &fields.Notifications,
		KeySorting:           fields.KeySorting,
	})
	if err != nil {
		return err
	}

	return namespaceoutput.WriteNamespace(cmd.OutOrStdout(), outputFormat, created)
}
