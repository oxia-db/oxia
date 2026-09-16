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

	namespacecli "github.com/oxia-db/oxia/cmd/admin/namespace/cli"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/common/validation"
)

func (e Executor) namespace(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: admin namespace list|get|create|patch|delete")
	}

	switch args[0] {
	case "list":
		return e.namespaceList(args)
	case "get":
		return e.namespaceGet(args)
	case "create":
		return e.namespaceCreate(args)
	case "patch":
		return e.namespacePatch(args)
	case "delete", "del":
		return e.namespaceDelete(args)
	default:
		return fmt.Errorf("unknown admin namespace command %q", args[0])
	}
}

func (e Executor) namespaceList(args []string) error {
	outputFormat, err := e.parseOutputCommand(args, "admin namespace list [--output FORMAT]")
	if err != nil {
		return err
	}
	admin, err := e.Client()
	if err != nil {
		return err
	}
	namespaces, err := admin.ListNamespaces(e.Ctx)
	if err != nil {
		return err
	}
	return namespacecli.WriteNamespaceViews(e.Out, outputFormat, namespaces)
}

func (e Executor) namespaceGet(args []string) error {
	outputFormat, namespace, err := e.parseGetCommand(args, "admin namespace get [--output FORMAT] NAMESPACE")
	if err != nil {
		return err
	}
	admin, err := e.Client()
	if err != nil {
		return err
	}
	view, err := admin.GetNamespace(e.Ctx, namespace)
	if err != nil {
		return err
	}
	return namespacecli.WriteNamespaceView(e.Out, outputFormat, view)
}

func (e Executor) namespaceCreate(args []string) error {
	outputFormat, namespace, err := e.parseNamespaceCreateCommand(args)
	if err != nil {
		return err
	}
	admin, err := e.Client()
	if err != nil {
		return err
	}
	created, err := admin.CreateNamespace(e.Ctx, namespace)
	if err != nil {
		return err
	}
	return namespacecli.WriteNamespace(e.Out, outputFormat, created)
}

func (e Executor) parseNamespaceCreateCommand(args []string) (string, *proto.Namespace, error) {
	const usage = "admin namespace create [--output FORMAT] --initial-shards COUNT --replication-factor FACTOR [--notifications BOOL] [--key-sorting TYPE] NAMESPACE"
	flags := e.newFlagSet("namespace-create")

	var (
		initialShardCount uint32
		replicationFactor uint32
		notifications     bool
		keySorting        string
	)
	flags.Uint32Var(&initialShardCount, namespacecli.InitialShardsFlagName, 0, "")
	flags.Uint32Var(&replicationFactor, namespacecli.ReplicationFactorFlagName, 0, "")
	flags.BoolVar(&notifications, namespacecli.NotificationsFlagName, true, "")
	flags.StringVar(&keySorting, namespacecli.KeySortingFlagName, "hierarchical", "")
	if err := flags.Parse(args[1:]); err != nil {
		return "", nil, fmt.Errorf("usage: %s", usage)
	}
	if flags.NArg() != 1 {
		return "", nil, fmt.Errorf("usage: %s", usage)
	}
	if !flags.Changed(namespacecli.InitialShardsFlagName) {
		return "", nil, errors.New("namespace initial shard count is required")
	}
	if !flags.Changed(namespacecli.ReplicationFactorFlagName) {
		return "", nil, errors.New("namespace replication factor is required")
	}
	if initialShardCount == 0 {
		return "", nil, errors.New("namespace initial shard count must be greater than 0")
	}
	if replicationFactor == 0 {
		return "", nil, errors.New("namespace replication factor must be greater than 0")
	}
	name := strings.TrimSpace(flags.Arg(0))
	if err := validation.ValidateNamespace(name); err != nil {
		return "", nil, err
	}
	parsedKeySorting, err := proto.ParseKeySortingType(keySorting)
	if err != nil {
		return "", nil, err
	}
	if parsedKeySorting == proto.KeySortingType_UNKNOWN {
		return "", nil, errors.New(`key sorting must be one of "natural" or "hierarchical"`)
	}
	outputFormat, err := adminOutputFormat(flags)
	if err != nil {
		return "", nil, err
	}

	return outputFormat, &proto.Namespace{
		Name:                 name,
		InitialShardCount:    initialShardCount,
		ReplicationFactor:    replicationFactor,
		NotificationsEnabled: &notifications,
		KeySorting:           keySorting,
	}, nil
}

func (e Executor) namespacePatch(args []string) error {
	outputFormat, namespace, err := e.parseNamespacePatchCommand(args)
	if err != nil {
		return err
	}
	admin, err := e.Client()
	if err != nil {
		return err
	}
	patched, err := admin.PatchNamespace(e.Ctx, namespace)
	if err != nil {
		return err
	}
	return namespacecli.WriteNamespace(e.Out, outputFormat, patched)
}

func (e Executor) parseNamespacePatchCommand(args []string) (string, *proto.Namespace, error) {
	const usage = "admin namespace patch [--output FORMAT] [--replication-factor FACTOR] [--notifications BOOL] NAMESPACE"
	flags := e.newFlagSet("namespace-patch")

	var (
		replicationFactor uint32
		notifications     bool
	)
	flags.Uint32Var(&replicationFactor, namespacecli.ReplicationFactorFlagName, 0, "")
	flags.BoolVar(&notifications, namespacecli.NotificationsFlagName, true, "")
	if err := flags.Parse(args[1:]); err != nil {
		return "", nil, fmt.Errorf("usage: %s", usage)
	}
	if flags.NArg() != 1 {
		return "", nil, fmt.Errorf("usage: %s", usage)
	}

	replicationFactorChanged := flags.Changed(namespacecli.ReplicationFactorFlagName)
	notificationsChanged := flags.Changed(namespacecli.NotificationsFlagName)
	if !replicationFactorChanged && !notificationsChanged {
		return "", nil, errors.New("must specify at least one field to patch")
	}
	if replicationFactorChanged && replicationFactor == 0 {
		return "", nil, errors.New("namespace replication factor must be greater than 0")
	}
	name := strings.TrimSpace(flags.Arg(0))
	if err := validation.ValidateNamespace(name); err != nil {
		return "", nil, err
	}
	outputFormat, err := adminOutputFormat(flags)
	if err != nil {
		return "", nil, err
	}

	namespace := &proto.Namespace{Name: name}
	if replicationFactorChanged {
		namespace.ReplicationFactor = replicationFactor
	}
	if notificationsChanged {
		namespace.NotificationsEnabled = &notifications
	}
	return outputFormat, namespace, nil
}

func (e Executor) namespaceDelete(args []string) error {
	outputFormat, namespace, err := e.parseGetCommand(args, "admin namespace delete [--output FORMAT] NAMESPACE")
	if err != nil {
		return err
	}
	admin, err := e.Client()
	if err != nil {
		return err
	}
	deleted, err := admin.DeleteNamespace(e.Ctx, namespace)
	if err != nil {
		return err
	}
	return namespacecli.WriteNamespace(e.Out, outputFormat, deleted)
}
