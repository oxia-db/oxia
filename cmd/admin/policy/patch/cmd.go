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
	policyoutput "github.com/oxia-db/oxia/cmd/admin/policy/output"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia"
)

var fields option.NamespaceFields

var Cmd = &cobra.Command{
	Use:          "patch",
	Short:        "Patch the cluster policy defaults",
	Long:         `Patch the cluster policy defaults`,
	Args:         cobra.NoArgs,
	RunE:         exec,
	SilenceUsage: true,
}

func init() {
	fields.AddPolicyFlags(Cmd)
}

func exec(cmd *cobra.Command, _ []string) error {
	outputFormat, err := cmd.Flags().GetString("output")
	if err != nil {
		return err
	}
	if err := commons.ValidateOutputFormat(outputFormat); err != nil {
		return err
	}

	policy, changed := fields.PatchPolicy(cmd)
	if !changed {
		return errors.New("must specify at least one field to patch")
	}
	if policy.InitialShardCount != nil && policy.GetInitialShardCount() == 0 {
		return errors.New("initial shard count must be greater than 0")
	}
	if policy.ReplicationFactor != nil && policy.GetReplicationFactor() == 0 {
		return errors.New("replication factor must be greater than 0")
	}
	if policy.KeySorting != nil {
		keySorting, err := proto.ParseKeySortingType(policy.GetKeySorting())
		if err != nil {
			return err
		}
		if keySorting == proto.KeySortingType_UNKNOWN {
			return errors.New(`key sorting must be one of "natural" or "hierarchical"`)
		}
	}

	client, err := commons.AdminConfig.NewAdminClient()
	if err != nil {
		return err
	}
	defer func(client oxia.AdminClient) {
		_ = client.Close()
	}(client)

	patched, err := client.PatchClusterPolicy(policy)
	if err != nil {
		return err
	}

	return policyoutput.WritePolicy(cmd.OutOrStdout(), outputFormat, patched)
}
