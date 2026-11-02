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

package confirmdrained

import (
	"errors"
	"strings"

	"github.com/spf13/cobra"

	"github.com/oxia-db/oxia/cmd/admin/commons"
	"github.com/oxia-db/oxia/oxia"
)

var Cmd = &cobra.Command{
	Use:   "confirm-drained <name>",
	Short: "Confirm the drain of a deleted data server",
	Long: `Confirm the drain of a deleted data server, once it is DATA_SERVER_STATE_DRAINED
and shut down: the coordinator then forgets it, and drops it from the pending
deletes of the shards. Its copies on disk are left as they are. Get the data
server to follow it: it is not found once forgotten, and its name is free for a
new data server, which must start with an empty disk.`,
	Args:         cobra.ExactArgs(1),
	RunE:         exec,
	SilenceUsage: true,
}

func exec(cmd *cobra.Command, args []string) error {
	name := strings.TrimSpace(args[0])
	if name == "" {
		return errors.New("data server name must not be empty")
	}

	client, err := commons.AdminConfig.NewAdminClient()
	if err != nil {
		return err
	}
	defer func(client oxia.AdminClient) {
		_ = client.Close()
	}(client)

	return client.ConfirmDrainedDataServer(cmd.Context(), name)
}
