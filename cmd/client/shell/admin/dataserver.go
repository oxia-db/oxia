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

package admin

import (
	"errors"
	"fmt"
	"strings"

	dataservercli "github.com/oxia-db/oxia/cmd/admin/dataserver/cli"
	cmdparse "github.com/oxia-db/oxia/cmd/common/parse"
	"github.com/oxia-db/oxia/common/proto"
)

func (e Executor) dataServer(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: admin dataserver list|get|create|patch|delete")
	}

	switch args[0] {
	case "list":
		return e.dataServerList(args)
	case "get":
		return e.dataServerGet(args)
	case "create":
		return e.dataServerCreate(args)
	case "patch":
		return e.dataServerPatch(args)
	case "delete", "del":
		return e.dataServerDelete(args)
	default:
		return fmt.Errorf("unknown admin dataserver command %q", args[0])
	}
}

func (e Executor) dataServerList(args []string) error {
	outputFormat, err := e.parseOutputCommand(args, "admin dataserver list [--output FORMAT]")
	if err != nil {
		return err
	}
	admin, err := e.Client()
	if err != nil {
		return err
	}
	dataServers, err := admin.ListDataServers(e.Ctx)
	if err != nil {
		return err
	}
	return dataservercli.WriteDataServers(e.Out, outputFormat, dataServers)
}

func (e Executor) dataServerGet(args []string) error {
	outputFormat, name, err := e.parseGetCommand(args, "admin dataserver get [--output FORMAT] NAME")
	if err != nil {
		return err
	}
	admin, err := e.Client()
	if err != nil {
		return err
	}
	dataServer, err := admin.GetDataServer(e.Ctx, name)
	if err != nil {
		return err
	}
	return dataservercli.WriteDataServerView(e.Out, outputFormat, dataServer)
}

func (e Executor) dataServerCreate(args []string) error {
	outputFormat, dataServer, err := e.parseDataServerCreateCommand(args)
	if err != nil {
		return err
	}
	admin, err := e.Client()
	if err != nil {
		return err
	}
	created, err := admin.CreateDataServer(e.Ctx, dataServer)
	if err != nil {
		return err
	}
	return dataservercli.WriteDataServer(e.Out, outputFormat, created)
}

func (e Executor) parseDataServerCreateCommand(args []string) (string, *proto.DataServer, error) {
	const usage = "admin dataserver create [--output FORMAT] --public ADDRESS --internal ADDRESS [--label KEY=VALUE] NAME"
	flags := e.newFlagSet("dataserver-create")

	var (
		publicAddress   string
		internalAddress string
		labels          []string
	)
	flags.StringVar(&publicAddress, dataservercli.PublicFlagName, "", "")
	flags.StringVar(&internalAddress, dataservercli.InternalFlagName, "", "")
	flags.StringArrayVar(&labels, dataservercli.LabelFlagName, nil, "")
	if err := flags.Parse(args[1:]); err != nil {
		return "", nil, fmt.Errorf("usage: %s", usage)
	}
	if flags.NArg() != 1 {
		return "", nil, fmt.Errorf("usage: %s", usage)
	}

	name := strings.TrimSpace(flags.Arg(0))
	if name == "" {
		return "", nil, errors.New("data server name must not be empty")
	}
	publicAddress = strings.TrimSpace(publicAddress)
	internalAddress = strings.TrimSpace(internalAddress)
	if publicAddress == "" {
		return "", nil, errors.New("data server public address must not be empty")
	}
	if internalAddress == "" {
		return "", nil, errors.New("data server internal address must not be empty")
	}
	parsedLabels, err := cmdparse.StringMap(labels)
	if err != nil {
		return "", nil, err
	}
	outputFormat, err := adminOutputFormat(flags)
	if err != nil {
		return "", nil, err
	}

	return outputFormat, &proto.DataServer{
		Identity: &proto.DataServerIdentity{
			Name:     &name,
			Public:   publicAddress,
			Internal: internalAddress,
		},
		Metadata: &proto.DataServerMetadata{
			Labels: parsedLabels,
		},
	}, nil
}

func (e Executor) dataServerPatch(args []string) error {
	outputFormat, dataServer, err := e.parseDataServerPatchCommand(args)
	if err != nil {
		return err
	}
	admin, err := e.Client()
	if err != nil {
		return err
	}
	patched, err := admin.PatchDataServer(e.Ctx, dataServer)
	if err != nil {
		return err
	}
	return dataservercli.WriteDataServer(e.Out, outputFormat, patched)
}

func (e Executor) parseDataServerPatchCommand(args []string) (string, *proto.DataServer, error) {
	const usage = "admin dataserver patch [--output FORMAT] [--public ADDRESS] [--internal ADDRESS] [--label KEY=VALUE] NAME"
	flags := e.newFlagSet("dataserver-patch")

	var (
		publicAddress   string
		internalAddress string
		labels          []string
	)
	flags.StringVar(&publicAddress, dataservercli.PublicFlagName, "", "")
	flags.StringVar(&internalAddress, dataservercli.InternalFlagName, "", "")
	flags.StringArrayVar(&labels, dataservercli.LabelFlagName, nil, "")
	if err := flags.Parse(args[1:]); err != nil {
		return "", nil, fmt.Errorf("usage: %s", usage)
	}
	if flags.NArg() != 1 {
		return "", nil, fmt.Errorf("usage: %s", usage)
	}

	name := strings.TrimSpace(flags.Arg(0))
	if name == "" {
		return "", nil, errors.New("data server name must not be empty")
	}
	publicAddress = strings.TrimSpace(publicAddress)
	internalAddress = strings.TrimSpace(internalAddress)
	publicChanged := flags.Changed(dataservercli.PublicFlagName)
	internalChanged := flags.Changed(dataservercli.InternalFlagName)
	labelChanged := flags.Changed(dataservercli.LabelFlagName)
	if !publicChanged && !internalChanged && !labelChanged {
		return "", nil, errors.New("must specify at least one field to patch")
	}
	if publicChanged && publicAddress == "" {
		return "", nil, errors.New("data server public address must not be empty")
	}
	if internalChanged && internalAddress == "" {
		return "", nil, errors.New("data server internal address must not be empty")
	}

	var metadata *proto.DataServerMetadata
	if labelChanged {
		parsedLabels, err := cmdparse.StringMap(labels)
		if err != nil {
			return "", nil, err
		}
		metadata = &proto.DataServerMetadata{Labels: parsedLabels}
	}
	outputFormat, err := adminOutputFormat(flags)
	if err != nil {
		return "", nil, err
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
	return outputFormat, dataServer, nil
}

func (e Executor) dataServerDelete(args []string) error {
	outputFormat, name, err := e.parseGetCommand(args, "admin dataserver delete [--output FORMAT] NAME")
	if err != nil {
		return err
	}
	admin, err := e.Client()
	if err != nil {
		return err
	}
	deleted, err := admin.DeleteDataServer(e.Ctx, name)
	if err != nil {
		return err
	}
	return dataservercli.WriteDataServer(e.Out, outputFormat, deleted)
}
