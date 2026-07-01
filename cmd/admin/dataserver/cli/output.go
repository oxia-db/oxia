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

type dataServerViewOutput struct {
	DataServer       *proto.DataServer       `json:"data_server,omitempty" yaml:"dataserver,omitempty"`
	DataServerStatus *dataServerStatusOutput `json:"data_server_status,omitempty" yaml:"dataserverstatus,omitempty"`
}

type dataServerStatusOutput struct {
	State             string   `json:"state,omitempty" yaml:"state,omitempty"`
	SupportedFeatures []string `json:"supported_features,omitempty" yaml:"supportedfeatures,omitempty"`
}

func ValidateOutputFormat(format string) error {
	if err := commons.ValidateOutputFormat(format); err != nil {
		return err
	}
	if format == commons.OutputName {
		return errors.Errorf("unsupported output format %q, expected one of: json, yaml, table", format)
	}
	return nil
}

func WriteDataServers(out io.Writer, format string, dataServers []*proto.DataServerView) error {
	if err := ValidateOutputFormat(format); err != nil {
		return err
	}

	format = commons.NormalizeOutputFormat(format)
	switch format {
	case commons.OutputJSON, commons.OutputYAML:
		values := make([]dataServerViewOutput, 0, len(dataServers))
		for _, view := range dataServers {
			if view == nil {
				continue
			}
			values = append(values, dataServerViewOutput{
				DataServer:       view.GetDataServer(),
				DataServerStatus: dataServerStatusOutputFor(view.GetDataServerStatus()),
			})
		}
		return commons.WriteStructuredOutput(out, format, values)
	case commons.OutputTable:
		return writeDataServerStatusTable(out, dataServers)
	default:
		return errors.Errorf("unsupported output format %q", format)
	}
}

func WriteDataServer(out io.Writer, format string, dataServer *proto.DataServer) error {
	if err := ValidateOutputFormat(format); err != nil {
		return err
	}

	format = commons.NormalizeOutputFormat(format)
	switch format {
	case commons.OutputJSON, commons.OutputYAML:
		return commons.WriteStructuredOutput(out, format, dataServer)
	case commons.OutputTable:
		return writeDataServerConfigTable(out, []*proto.DataServer{dataServer})
	default:
		return errors.Errorf("unsupported output format %q", format)
	}
}

func WriteDataServerView(out io.Writer, format string, dataServer *proto.DataServerView) error {
	if err := ValidateOutputFormat(format); err != nil {
		return err
	}

	format = commons.NormalizeOutputFormat(format)
	switch format {
	case commons.OutputJSON, commons.OutputYAML:
		value := dataServerViewOutput{}
		if dataServer != nil {
			value.DataServer = dataServer.GetDataServer()
			value.DataServerStatus = dataServerStatusOutputFor(dataServer.GetDataServerStatus())
		}
		return commons.WriteStructuredOutput(out, format, value)
	case commons.OutputTable:
		return writeDataServerStatusTable(out, []*proto.DataServerView{dataServer})
	default:
		return errors.Errorf("unsupported output format %q", format)
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

func dataServerStatusOutputFor(status *proto.DataServerStatus) *dataServerStatusOutput {
	if status == nil {
		return nil
	}
	features := status.GetSupportedFeatures()
	featureNames := make([]string, 0, len(features))
	for _, feature := range features {
		featureNames = append(featureNames, feature.String())
	}
	return &dataServerStatusOutput{
		State:             status.GetState().String(),
		SupportedFeatures: featureNames,
	}
}

func writeDataServerStatusTable(out io.Writer, dataServers []*proto.DataServerView) error {
	tw := commons.NewTableWriter(out)
	if _, err := fmt.Fprintln(tw, "NAME\tPUBLIC\tINTERNAL\tSTATE"); err != nil {
		return err
	}
	for _, view := range dataServers {
		if view == nil || view.GetDataServer() == nil {
			continue
		}
		dataServer := view.GetDataServer()
		identity := dataServer.GetIdentity()
		status := view.GetDataServerStatus()
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n",
			dataServer.GetNameOrDefault(),
			identity.GetPublic(),
			identity.GetInternal(),
			status.GetState().String(),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}
