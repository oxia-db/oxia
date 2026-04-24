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
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia"
)

var Cmd = &cobra.Command{
	Use:          "get [data-server]",
	Short:        "Get a data server or list data server identities",
	Long:         `Get a data server or list data server identities`,
	Args:         cobra.MaximumNArgs(1),
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

	if len(args) == 0 {
		dataServers, err := client.ListDataServers()
		if err != nil {
			return err
		}

		identities := make([]*proto.DataServerIdentity, 0, len(dataServers))
		for _, dataServer := range dataServers {
			if dataServer == nil || dataServer.Identity == nil {
				continue
			}
			identities = append(identities, dataServer.Identity)
		}
		cc.WriteOutput(cmd.OutOrStdout(), identities)
		return nil
	}

	dataServer, err := client.GetDataServer(args[0])
	if err != nil {
		return err
	}
	cc.WriteOutput(cmd.OutOrStdout(), dataServer)
	return nil
}
