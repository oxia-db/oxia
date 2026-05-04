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
	dataserveroutput "github.com/oxia-db/oxia/cmd/admin/dataserver/output"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia"
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
	Cmd.Flags().StringP("output", "o", "", "Output format. One of: json|yaml|name|table")
	Cmd.Flags().String("public", "", "Public address for the data server")
	Cmd.Flags().String("internal", "", "Internal address for the data server")
	Cmd.Flags().StringArray("label", nil, "Label to attach to the data server in key=value form")
	_ = Cmd.MarkFlagRequired("public")
	_ = Cmd.MarkFlagRequired("internal")
}

func exec(cmd *cobra.Command, args []string) error {
	outputFormat, err := cmd.Flags().GetString("output")
	if err != nil {
		return err
	}
	if err := commons.ValidateOutputFormat(outputFormat); err != nil {
		return err
	}

	publicAddress, err := cmd.Flags().GetString("public")
	if err != nil {
		return err
	}
	internalAddress, err := cmd.Flags().GetString("internal")
	if err != nil {
		return err
	}
	labelValues, err := cmd.Flags().GetStringArray("label")
	if err != nil {
		return err
	}

	labels, err := parseLabels(labelValues)
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

	name := args[0]
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

func parseLabels(values []string) (map[string]string, error) {
	if len(values) == 0 {
		return map[string]string{}, nil
	}

	labels := make(map[string]string, len(values))
	for _, value := range values {
		key, labelValue, ok := strings.Cut(value, "=")
		if !ok || key == "" {
			return nil, fmt.Errorf("invalid label %q, expected key=value", value)
		}
		labels[key] = labelValue
	}
	return labels, nil
}
