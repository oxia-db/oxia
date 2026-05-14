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
	"strings"

	"github.com/pkg/errors"

	"github.com/oxia-db/oxia/cmd/admin/commons"
	"github.com/oxia-db/oxia/common/proto"
)

func WriteDataServers(out io.Writer, format string, dataServers []*proto.DataServerView) error {
	if err := commons.ValidateOutputFormat(format); err != nil {
		return err
	}

	format = commons.NormalizeOutputFormat(format)
	switch format {
	case commons.OutputJSON, commons.OutputYAML:
		return commons.WriteStructuredOutput(out, format, dataServers)
	case commons.OutputName:
		return commons.WriteResourceNames(out, "dataserver", dataServerNames(dataServers))
	case commons.OutputTable:
		return writeDataServerTable(out, dataServers)
	default:
		return errors.Errorf("unsupported output format %q", format)
	}
}

func WriteDataServer(out io.Writer, format string, dataServer *proto.DataServer) error {
	if err := commons.ValidateOutputFormat(format); err != nil {
		return err
	}

	format = commons.NormalizeOutputFormat(format)
	switch format {
	case commons.OutputJSON, commons.OutputYAML:
		return commons.WriteStructuredOutput(out, format, dataServer)
	case commons.OutputName:
		return commons.WriteResourceNames(out, "dataserver", []string{dataServer.GetNameOrDefault()})
	case commons.OutputTable:
		return writeDataServerConfigTable(out, []*proto.DataServer{dataServer})
	default:
		return errors.Errorf("unsupported output format %q", format)
	}
}

func WriteDataServerView(out io.Writer, format string, dataServer *proto.DataServerView) error {
	if err := commons.ValidateOutputFormat(format); err != nil {
		return err
	}

	format = commons.NormalizeOutputFormat(format)
	switch format {
	case commons.OutputJSON, commons.OutputYAML:
		return commons.WriteStructuredOutput(out, format, dataServer)
	case commons.OutputName:
		return commons.WriteResourceNames(out, "dataserver", []string{dataServer.GetDataServer().GetNameOrDefault()})
	case commons.OutputTable:
		return writeDataServerTable(out, []*proto.DataServerView{dataServer})
	default:
		return errors.Errorf("unsupported output format %q", format)
	}
}

func dataServerView(dataServer *proto.DataServer, status *proto.DataServerStatus) *proto.DataServerView {
	if status == nil {
		status = &proto.DataServerStatus{}
	}
	return &proto.DataServerView{
		DataServer: dataServer,
		Status:     status,
	}
}

func writeDataServerConfigTable(out io.Writer, dataServers []*proto.DataServer) error {
	tw := commons.NewTableWriter(out)
	if _, err := fmt.Fprintln(tw, "NAME\tPUBLIC\tINTERNAL\tLABELS"); err != nil {
		return err
	}
	for _, dataServer := range dataServers {
		if dataServer == nil {
			continue
		}
		identity := dataServer.GetIdentity()
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n",
			dataServer.GetNameOrDefault(),
			identity.GetPublic(),
			identity.GetInternal(),
			commons.FormatLabels(dataServer.GetMetadata().GetLabels()),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeDataServerTable(out io.Writer, dataServers []*proto.DataServerView) error {
	tw := commons.NewTableWriter(out)
	if _, err := fmt.Fprintln(tw, "NAME\tPUBLIC\tINTERNAL\tSTATE\tSHARDS\tLEADERS\tFEATURES\tLABELS"); err != nil {
		return err
	}
	for _, view := range dataServers {
		if view == nil || view.GetDataServer() == nil {
			continue
		}
		dataServer := view.GetDataServer()
		identity := dataServer.GetIdentity()
		status := view.GetStatus()
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%d\t%s\t%s\n",
			dataServer.GetNameOrDefault(),
			identity.GetPublic(),
			identity.GetInternal(),
			status.GetState().String(),
			status.GetShardCount(),
			status.GetLeaderShardCount(),
			formatFeatures(status.GetSupportedFeatures()),
			commons.FormatLabels(dataServer.GetMetadata().GetLabels()),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func dataServerNames(dataServers []*proto.DataServerView) []string {
	names := make([]string, 0, len(dataServers))
	for _, view := range dataServers {
		if view == nil || view.GetDataServer() == nil {
			continue
		}
		names = append(names, view.GetDataServer().GetNameOrDefault())
	}
	return names
}

func formatFeatures(features []proto.Feature) string {
	if len(features) == 0 {
		return ""
	}
	values := make([]string, 0, len(features))
	for _, feature := range features {
		values = append(values, feature.String())
	}
	return strings.Join(values, ",")
}
