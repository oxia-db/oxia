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
	namespaceoutput "github.com/oxia-db/oxia/cmd/admin/namespace/output"
	"github.com/oxia-db/oxia/oxia"
)

var Cmd = &cobra.Command{
	Use:          "get [namespace]",
	Short:        "Get a namespace or list namespaces",
	Long:         `Get a namespace or list namespaces`,
	Args:         cobra.MaximumNArgs(1),
	RunE:         exec,
	SilenceUsage: true,
}

func exec(cmd *cobra.Command, args []string) error {
	outputFormat, err := cmd.Flags().GetString("output")
	if err != nil {
		return err
	}
	if err := commons.ValidateOutputFormat(outputFormat); err != nil {
		return err
	}

	client, err := commons.AdminConfig.NewAdminClient()
	if err != nil {
		return err
	}
	defer func(client oxia.AdminClient) {
		_ = client.Close()
	}(client)

	if len(args) == 0 {
		namespaces, err := client.ListNamespaces()
		if err != nil {
			return err
		}
		return namespaceoutput.WriteNamespaces(cmd.OutOrStdout(), outputFormat, namespaces)
	}

	namespace, err := client.GetNamespace(args[0])
	if err != nil {
		return err
	}
	return namespaceoutput.WriteNamespace(cmd.OutOrStdout(), outputFormat, namespace)
}
