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
	"strings"

	"github.com/spf13/cobra"

	"github.com/oxia-db/oxia/cmd/admin/commons"
	dataserveroutput "github.com/oxia-db/oxia/cmd/admin/dataserver/output"
	cmdparse "github.com/oxia-db/oxia/cmd/common/parse"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia"
)

const (
	publicFlagName   = "public"
	internalFlagName = "internal"
)

var Cmd = &cobra.Command{
	Use:          "create <name> --public <address> --internal <address>",
	Short:        "Create a data server",
	Long:         `Create a data server`,
	Args:         cobra.ExactArgs(1),
	RunE:         exec,
	SilenceUsage: true,
}

func init() {
	Cmd.Flags().String(publicFlagName, "", "Public address for the data server")
	Cmd.Flags().String(internalFlagName, "", "Internal address for the data server")
	Cmd.Flags().StringArray("label", nil, "Label to attach to the data server in key=value form")
	_ = Cmd.MarkFlagRequired(publicFlagName)
	_ = Cmd.MarkFlagRequired(internalFlagName)
}

func exec(cmd *cobra.Command, args []string) error {
	var (
		outputFormat    string
		publicAddress   string
		internalAddress string
		labelValues     []string
		err             error
	)
	if outputFormat, err = cmd.Flags().GetString("output"); err != nil {
		return err
	}
	if err := commons.ValidateOutputFormat(outputFormat); err != nil {
		return err
	}
	if publicAddress, err = cmd.Flags().GetString(publicFlagName); err != nil {
		return err
	}
	if internalAddress, err = cmd.Flags().GetString(internalFlagName); err != nil {
		return err
	}
	if labelValues, err = cmd.Flags().GetStringArray("label"); err != nil {
		return err
	}

	labels, err := cmdparse.StringMap(labelValues)
	if err != nil {
		return err
	}

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

	created, err := client.CreateDataServer(&proto.DataServer{
		Identity: &proto.DataServerIdentity{
			Name:     &name,
			Public:   publicAddress,
			Internal: internalAddress,
		},
		Metadata: &proto.DataServerMetadata{
			Labels: labels,
		},
	})
	if err != nil {
		return err
	}

	return dataserveroutput.WriteDataServer(cmd.OutOrStdout(), outputFormat, created)
}
