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
	"github.com/oxia-db/oxia/cmd/admin/dataserver/option"
	dataserveroutput "github.com/oxia-db/oxia/cmd/admin/dataserver/output"
	cmdparse "github.com/oxia-db/oxia/cmd/common/parse"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia"
)

var fields option.DataServerFields

var Cmd = &cobra.Command{
	Use:          "create <name> --public <address> --internal <address>",
	Short:        "Create a data server",
	Long:         `Create a data server`,
	Args:         cobra.ExactArgs(1),
	RunE:         exec,
	SilenceUsage: true,
}

func init() {
	fields.AddFlags(Cmd.Flags())
	_ = Cmd.MarkFlagRequired(option.PublicFlagName)
	_ = Cmd.MarkFlagRequired(option.InternalFlagName)
}

func exec(cmd *cobra.Command, args []string) error {
	outputFormat, err := cmd.Flags().GetString("output")
	if err != nil {
		return err
	}
	if err := commons.ValidateOutputFormat(outputFormat); err != nil {
		return err
	}

	name := strings.TrimSpace(args[0])
	if name == "" {
		return errors.New("data server name must not be empty")
	}
	if strings.TrimSpace(fields.PublicAddress) == "" {
		return errors.New("data server public address must not be empty")
	}
	if strings.TrimSpace(fields.InternalAddress) == "" {
		return errors.New("data server internal address must not be empty")
	}

	labels, err := cmdparse.StringMap(fields.Labels)
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

	created, err := client.CreateDataServer(&proto.DataServer{
		Identity: &proto.DataServerIdentity{
			Name:     &name,
			Public:   fields.PublicAddress,
			Internal: fields.InternalAddress,
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
