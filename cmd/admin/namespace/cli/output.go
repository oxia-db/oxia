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

package cli

import (
	"fmt"
	"io"

	"github.com/pkg/errors"

	"github.com/oxia-db/oxia/cmd/admin/commons"
	"github.com/oxia-db/oxia/common/proto"
)

const (
	errNamespaceMustNotBeNil = "namespace must not be nil"
	errUnsupportedOutputFmt  = "unsupported output format %q"
)

func WriteNamespace(out io.Writer, format string, namespace *proto.Namespace) error {
	if namespace == nil {
		return errors.New(errNamespaceMustNotBeNil)
	}

	if err := commons.ValidateOutputFormat(format); err != nil {
		return err
	}

	format = commons.NormalizeOutputFormat(format)
	switch format {
	case commons.OutputJSON, commons.OutputYAML:
		return commons.WriteStructuredOutput(out, format, namespace)
	case commons.OutputTable:
		return writeNamespaceTable(out, []*proto.Namespace{namespace})
	default:
		return errors.Errorf(errUnsupportedOutputFmt, format)
	}
}

func WriteNamespaces(out io.Writer, format string, namespaces []*proto.Namespace) error {
	if err := commons.ValidateOutputFormat(format); err != nil {
		return err
	}
	for _, namespace := range namespaces {
		if namespace == nil {
			return errors.New(errNamespaceMustNotBeNil)
		}
	}

	format = commons.NormalizeOutputFormat(format)
	switch format {
	case commons.OutputJSON, commons.OutputYAML:
		return commons.WriteStructuredOutput(out, format, namespaces)
	case commons.OutputTable:
		return writeNamespaceTable(out, namespaces)
	default:
		return errors.Errorf(errUnsupportedOutputFmt, format)
	}
}

func WriteNamespaceView(out io.Writer, format string, namespace *proto.NamespaceView) error {
	if namespace == nil || namespace.GetNamespace() == nil {
		return errors.New(errNamespaceMustNotBeNil)
	}

	if err := commons.ValidateOutputFormat(format); err != nil {
		return err
	}

	format = commons.NormalizeOutputFormat(format)
	switch format {
	case commons.OutputJSON, commons.OutputYAML:
		return commons.WriteStructuredOutput(out, format, namespace)
	case commons.OutputTable:
		return writeNamespaceViewTable(out, []*proto.NamespaceView{namespace})
	default:
		return errors.Errorf(errUnsupportedOutputFmt, format)
	}
}

func WriteNamespaceViews(out io.Writer, format string, namespaces []*proto.NamespaceView) error {
	if err := commons.ValidateOutputFormat(format); err != nil {
		return err
	}
	for _, namespace := range namespaces {
		if namespace == nil || namespace.GetNamespace() == nil {
			return errors.New(errNamespaceMustNotBeNil)
		}
	}

	format = commons.NormalizeOutputFormat(format)
	switch format {
	case commons.OutputJSON, commons.OutputYAML:
		return commons.WriteStructuredOutput(out, format, namespaces)
	case commons.OutputTable:
		return writeNamespaceViewTable(out, namespaces)
	default:
		return errors.Errorf(errUnsupportedOutputFmt, format)
	}
}

func writeNamespaceTable(out io.Writer, namespaces []*proto.Namespace) error {
	tw := commons.NewTableWriter(out)
	if _, err := fmt.Fprintln(tw, "NAME\tINITIAL_SHARDS\tREPLICATION_FACTOR\tNOTIFICATIONS\tKEY_SORTING"); err != nil {
		return err
	}
	for _, namespace := range namespaces {
		if namespace == nil {
			continue
		}
		if _, err := fmt.Fprintf(tw, "%s\t%d\t%d\t%t\t%s\n",
			namespace.GetName(),
			namespace.GetInitialShardCount(),
			namespace.GetReplicationFactor(),
			namespace.NotificationsEnabledOrDefault(),
			namespace.GetKeySorting(),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeNamespaceViewTable(out io.Writer, namespaces []*proto.NamespaceView) error {
	tw := commons.NewTableWriter(out)
	if _, err := fmt.Fprintln(tw, "NAME\tINITIAL_SHARDS\tCURRENT_SHARDS\tREPLICATION_FACTOR\tNOTIFICATIONS\tKEY_SORTING"); err != nil {
		return err
	}
	for _, view := range namespaces {
		if view == nil || view.GetNamespace() == nil {
			continue
		}
		namespace := view.GetNamespace()
		if _, err := fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%t\t%s\n",
			namespace.GetName(),
			namespace.GetInitialShardCount(),
			len(view.GetNamespaceStatus().GetShards()),
			namespace.GetReplicationFactor(),
			namespace.NotificationsEnabledOrDefault(),
			namespace.GetKeySorting(),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}
