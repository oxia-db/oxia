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
	cc "github.com/oxia-db/oxia/cmd/client/common"
	"github.com/oxia-db/oxia/oxia"
)

var Cmd = &cobra.Command{
	Use:          "get [data-server]",
	Short:        "Get a data server",
	Long:         `Get a data server`,
	Args:         cobra.ExactArgs(1),
	RunE:         exec,
	SilenceUsage: true,
}

func exec(cmd *cobra.Command, args []string) error {
	client, err := commons.AdminConfig.NewAdminClient()
	if err != nil {
		return err
	}
	defer func(client oxia.AdminClient) {
		_ = client.Close()
	}(client)

	dataServer, err := client.GetDataServer(args[0])
	if err != nil {
		return err
	}
	cc.WriteOutput(cmd.OutOrStdout(), dataServer)
	return nil
}
