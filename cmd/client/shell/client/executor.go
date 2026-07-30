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

package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/oxia-db/oxia/cmd/client/common"
	"github.com/oxia-db/oxia/oxia"
)

type Executor struct {
	Ctx    context.Context
	Client func() (oxia.SyncClient, error)
	Out    io.Writer
}

func (e Executor) Execute(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: get|put|delete|delete-range|list|range-scan")
	}

	switch args[0] {
	case "get":
		return e.get(args)
	case "put":
		return e.put(args)
	case "delete", "del":
		return e.delete(args)
	case "delete-range":
		return e.deleteRange(args)
	case "list", "ls":
		return e.list(args)
	case "range-scan", "scan":
		return e.rangeScan(args)
	default:
		return fmt.Errorf("unknown client command %q", args[0])
	}
}

func outputVersion(key string, version oxia.Version) common.OutputVersion {
	return common.OutputVersion{
		Key:                key,
		VersionId:          version.VersionId,
		CreatedTimestamp:   time.UnixMilli(int64(version.CreatedTimestamp)),
		ModifiedTimestamp:  time.UnixMilli(int64(version.ModifiedTimestamp)),
		ModificationsCount: version.ModificationsCount,
		Ephemeral:          version.Ephemeral,
		SessionId:          version.SessionId,
		ClientIdentity:     version.ClientIdentity,
	}
}
