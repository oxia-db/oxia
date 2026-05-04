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

package get

import (
	"github.com/spf13/cobra"

	"github.com/oxia-db/oxia/cmd/admin/commons"
	policyoutput "github.com/oxia-db/oxia/cmd/admin/policy/output"
	"github.com/oxia-db/oxia/oxia"
)

var Cmd = &cobra.Command{
	Use:          "get",
	Short:        "Get the cluster policy defaults",
	Long:         `Get the cluster policy defaults`,
	Args:         cobra.NoArgs,
	RunE:         exec,
	SilenceUsage: true,
}

func exec(cmd *cobra.Command, _ []string) error {
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

	policy, err := client.GetClusterPolicy()
	if err != nil {
		return err
	}

	return policyoutput.WritePolicy(cmd.OutOrStdout(), outputFormat, policy)
}
