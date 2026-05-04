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
	"strings"

	"github.com/spf13/cobra"

	"github.com/oxia-db/oxia/cmd/admin/commons"
	"github.com/oxia-db/oxia/cmd/admin/dataserver/option"
	dataserveroutput "github.com/oxia-db/oxia/cmd/admin/dataserver/output"
	cmdparse "github.com/oxia-db/oxia/cmd/common/parse"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia"
)

var (
	fields option.DataServerFields
)

var Cmd = &cobra.Command{
	Use:          "patch <name>",
	Short:        "Patch a data server",
	Long:         `Patch a data server`,
	Args:         cobra.ExactArgs(1),
	RunE:         exec,
	SilenceUsage: true,
}

func init() {
	fields.AddFlags(Cmd.Flags())
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

	publicAddress := strings.TrimSpace(fields.PublicAddress)
	internalAddress := strings.TrimSpace(fields.InternalAddress)
	if publicAddress == "" && internalAddress == "" && len(fields.Labels) == 0 {
		return errors.New("must specify at least one field to patch")
	}

	var metadata *proto.DataServerMetadata
	if len(fields.Labels) > 0 {
		parsedLabels, err := cmdparse.StringMap(fields.Labels)
		if err != nil {
			return err
		}
		metadata = &proto.DataServerMetadata{Labels: parsedLabels}
	}

	dataServer := &proto.DataServer{
		Identity: &proto.DataServerIdentity{
			Name: &name,
		},
		Metadata: metadata,
	}
	if publicAddress != "" {
		dataServer.Identity.Public = publicAddress
	}
	if internalAddress != "" {
		dataServer.Identity.Internal = internalAddress
	}

	client, err := commons.AdminConfig.NewAdminClient()
	if err != nil {
		return err
	}
	defer func(client oxia.AdminClient) {
		_ = client.Close()
	}(client)

	patched, err := client.PatchDataServer(dataServer)
	if err != nil {
		return err
	}

	return dataserveroutput.WriteDataServer(cmd.OutOrStdout(), outputFormat, patched)
}
