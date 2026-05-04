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

package patch

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
	Use:          "patch <namespace>",
	Short:        "Patch a namespace",
	Long:         `Patch a namespace`,
	Args:         cobra.ExactArgs(1),
	RunE:         exec,
	SilenceUsage: true,
}

func init() {
	fields.AddFlags(Cmd)
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

	namespace, err := namespaceFromFlags(cmd, name)
	if err != nil {
		return err
	}

	client, err := commons.AdminConfig.NewAdminClient()
	if err != nil {
		return err
	}
	defer func(client oxia.AdminClient) {
		_ = client.Close()
	}(client)

	patched, err := client.PatchNamespace(namespace)
	if err != nil {
		return err
	}

	return namespaceoutput.WriteNamespace(cmd.OutOrStdout(), outputFormat, patched)
}

func namespaceFromFlags(cmd *cobra.Command, name string) (*proto.Namespace, error) {
	initialShardCountChanged := cmd.Flags().Changed(option.InitialShardsFlagName)
	replicationFactorChanged := cmd.Flags().Changed(option.ReplicationFactorFlagName)
	notificationsChanged := cmd.Flags().Changed(option.NotificationsFlagName)
	keySortingChanged := cmd.Flags().Changed(option.KeySortingFlagName)
	if !initialShardCountChanged && !replicationFactorChanged && !notificationsChanged && !keySortingChanged {
		return nil, errors.New("must specify at least one field to patch")
	}
	if initialShardCountChanged && fields.InitialShardCount == 0 {
		return nil, errors.New("namespace initial shard count must be greater than 0")
	}
	if replicationFactorChanged && fields.ReplicationFactor == 0 {
		return nil, errors.New("namespace replication factor must be greater than 0")
	}
	if keySortingChanged {
		keySorting, err := proto.ParseKeySortingType(fields.KeySorting)
		if err != nil {
			return nil, err
		}
		if keySorting == proto.KeySortingType_UNKNOWN {
			return nil, errors.New(`key sorting must be one of "natural" or "hierarchical"`)
		}
	}

	namespace := &proto.Namespace{
		Name: name,
	}
	if initialShardCountChanged {
		namespace.InitialShardCount = fields.InitialShardCount
	}
	if replicationFactorChanged {
		namespace.ReplicationFactor = fields.ReplicationFactor
	}
	if notificationsChanged {
		namespace.NotificationsEnabled = &fields.Notifications
	}
	if keySortingChanged {
		namespace.KeySorting = fields.KeySorting
	}

	return namespace, nil
}
