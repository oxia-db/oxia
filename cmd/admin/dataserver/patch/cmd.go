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
	Use:          "patch <name>",
	Short:        "Patch a data server",
	Long:         `Patch a data server`,
	Args:         cobra.ExactArgs(1),
	RunE:         exec,
	SilenceUsage: true,
}

func init() {
	Cmd.Flags().String(publicFlagName, "", "Public address for the data server")
	Cmd.Flags().String(internalFlagName, "", "Internal address for the data server")
	Cmd.Flags().StringArray("label", nil, "Label to attach to the data server in key=value form")
}

func exec(cmd *cobra.Command, args []string) error {
	outputFormat, err := cmd.Flags().GetString("output")
	if err != nil {
		return err
	}
	if err := commons.ValidateOutputFormat(outputFormat); err != nil {
		return err
	}

	dataServer, err := patchRequestFromFlags(cmd, args[0])
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

	patched, err := client.PatchDataServer(dataServer)
	if err != nil {
		return err
	}

	return dataserveroutput.WriteDataServer(cmd.OutOrStdout(), outputFormat, patched)
}

func patchRequestFromFlags(cmd *cobra.Command, rawName string) (*proto.DataServer, error) {
	name := strings.TrimSpace(rawName)
	if name == "" {
		return nil, errors.New("data server name must not be empty")
	}

	publicAddress, err := cmd.Flags().GetString(publicFlagName)
	if err != nil {
		return nil, err
	}
	internalAddress, err := cmd.Flags().GetString(internalFlagName)
	if err != nil {
		return nil, err
	}
	labelValues, err := cmd.Flags().GetStringArray("label")
	if err != nil {
		return nil, err
	}

	publicChanged := cmd.Flags().Changed(publicFlagName)
	internalChanged := cmd.Flags().Changed(internalFlagName)
	labelChanged := cmd.Flags().Changed("label")
	if !publicChanged && !internalChanged && !labelChanged {
		return nil, errors.New("must specify at least one field to patch")
	}

	if publicChanged && strings.TrimSpace(publicAddress) == "" {
		return nil, errors.New("data server public address must not be empty")
	}
	if internalChanged && strings.TrimSpace(internalAddress) == "" {
		return nil, errors.New("data server internal address must not be empty")
	}

	var metadata *proto.DataServerMetadata
	if labelChanged {
		labels, err := cmdparse.StringMap(labelValues)
		if err != nil {
			return nil, err
		}
		metadata = &proto.DataServerMetadata{Labels: labels}
	}

	dataServer := &proto.DataServer{
		Identity: &proto.DataServerIdentity{
			Name: &name,
		},
		Metadata: metadata,
	}
	if publicChanged {
		dataServer.Identity.Public = publicAddress
	}
	if internalChanged {
		dataServer.Identity.Internal = internalAddress
	}
	return dataServer, nil
}
