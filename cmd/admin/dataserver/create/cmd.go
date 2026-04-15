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
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/oxia-db/oxia/cmd/admin/commons"
	cc "github.com/oxia-db/oxia/cmd/client/common"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia"
)

var (
	name            string
	publicAddress   string
	internalAddress string
	metadataFlags   []string
)

var Cmd = &cobra.Command{
	Use:          "create",
	Short:        "Create a data server",
	Long:         `Create a data server`,
	Args:         cobra.ExactArgs(0),
	RunE:         exec,
	SilenceUsage: true,
}

func init() {
	Cmd.Flags().StringVar(&name, "name", "", "Unique data server name")
	Cmd.Flags().StringVar(&publicAddress, "public-address", "", "Public data server address")
	Cmd.Flags().StringVar(&internalAddress, "internal-address", "", "Internal data server address")
	Cmd.Flags().StringSliceVar(&metadataFlags, "metadata", nil, "Metadata label in key=value form")

	_ = Cmd.MarkFlagRequired("public-address")
	_ = Cmd.MarkFlagRequired("internal-address")
}

func exec(cmd *cobra.Command, _ []string) error {
	client, err := commons.AdminConfig.NewAdminClient()
	if err != nil {
		return err
	}
	defer func(client oxia.AdminClient) {
		_ = client.Close()
	}(client)

	metadata, err := parseMetadataFlags(metadataFlags)
	if err != nil {
		return err
	}

	dataServer := &proto.DataServer{
		PublicAddress:   publicAddress,
		InternalAddress: internalAddress,
	}
	if name != "" {
		dataServer.Name = &name
	}

	created, err := client.CreateDataServer(&proto.DataServerInfo{
		DataServer: dataServer,
		Metadata:   metadata,
	})
	if err != nil {
		return err
	}

	cc.WriteOutput(cmd.OutOrStdout(), created)
	return nil
}

func parseMetadataFlags(entries []string) (map[string]string, error) {
	if len(entries) == 0 {
		return nil, nil
	}

	metadata := make(map[string]string, len(entries))
	for _, entry := range entries {
		key, value, found := strings.Cut(entry, "=")
		if !found {
			return nil, fmt.Errorf("invalid metadata entry %q: expected key=value", entry)
		}
		if key == "" {
			return nil, fmt.Errorf("invalid metadata entry %q: key must not be empty", entry)
		}
		metadata[key] = value
	}
	return metadata, nil
}
