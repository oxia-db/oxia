// Copyright 2023-2025 The Oxia Authors
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

package listnodes

import (
	"github.com/spf13/cobra"

	"github.com/oxia-db/oxia/cmd/admin/commons"

	cc "github.com/oxia-db/oxia/cmd/client/common"
	"github.com/oxia-db/oxia/oxia"
)

func init() {
}

var Cmd = &cobra.Command{
	Use:          "list-nodes",
	Short:        "List nodes",
	Long:         `List nodes`,
	Args:         cobra.ExactArgs(0),
	RunE:         exec,
	SilenceUsage: true,
}

func exec(cmd *cobra.Command, _ []string) error {
	client, err := commons.AdminConfig.NewAdminClient()
	defer func(client oxia.AdminClient) {
		_ = client.Close()
	}(client)

	if err != nil {
		return err
	}

	result := client.ListNodes()
	if result.Error != nil {
		return result.Error
	}
	cc.WriteOutput(cmd.OutOrStdout(), result.Nodes)
	return nil
}
