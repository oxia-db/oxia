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

package output

import (
	"fmt"
	"io"

	"github.com/pkg/errors"

	"github.com/oxia-db/oxia/cmd/admin/commons"
	"github.com/oxia-db/oxia/common/proto"
)

func WritePolicy(out io.Writer, format string, policy *proto.HierarchyPolicies) error {
	if policy == nil {
		return errors.New("policy must not be nil")
	}
	if err := commons.ValidateOutputFormat(format); err != nil {
		return err
	}

	format = commons.NormalizeOutputFormat(format)
	switch format {
	case commons.OutputJSON, commons.OutputYAML:
		return commons.WriteStructuredOutput(out, format, policy)
	case commons.OutputName:
		return commons.WriteResourceNames(out, "policy", []string{"cluster"})
	case commons.OutputTable:
		return writePolicyTable(out, policy)
	default:
		return errors.Errorf("unsupported output format %q", format)
	}
}

func writePolicyTable(out io.Writer, policy *proto.HierarchyPolicies) error {
	tw := commons.NewTableWriter(out)
	if _, err := fmt.Fprintln(tw, "INITIAL_SHARDS\tREPLICATION_FACTOR\tNOTIFICATIONS\tKEY_SORTING"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(tw, "%d\t%d\t%t\t%s\n",
		policy.GetInitialShardCount(),
		policy.GetReplicationFactor(),
		policy.GetNotificationsEnabled(),
		policy.GetKeySorting(),
	); err != nil {
		return err
	}
	return tw.Flush()
}
