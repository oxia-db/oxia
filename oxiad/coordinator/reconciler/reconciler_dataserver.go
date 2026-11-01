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

package reconciler

import (
	"context"
	"log/slog"

	"github.com/emirpasic/gods/v2/sets/hashset"
	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/dataserver"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime"
)

var _ Reconciler = (*dataServerReconciler)(nil)

type dataServerReconciler struct {
	runtime runtime.Runtime
}

func (*dataServerReconciler) Close() error { return nil }

func (r *dataServerReconciler) Reconcile(_ context.Context, snapshot *proto.ClusterConfiguration) error {
	desired := hashset.New[string]()
	statuses, err := r.runtime.Metadata().ListDataServerStatus()
	if err != nil {
		return err
	}

	for _, dataServer := range snapshot.ListDataServers() {
		serverName := dataServer.GetNameOrDefault()
		// A data server that left the cluster can't join it again under the
		// same name: it isn't reconciled, and keeps draining
		state := statuses[serverName].UnsafeBorrow().GetState()
		if live, found := r.runtime.GetDataServerStatus(serverName); found && !dataserver.IsRemoved(state) {
			state = live.GetState()
		}
		if dataserver.IsRemoved(state) {
			slog.Warn("Rejecting a data server of the configuration that was removed",
				slog.String("data-server", serverName), slog.String("state", state.String()))
			continue
		}
		desired.Add(serverName)
		r.runtime.CreateDataServer(serverName, dataServer)
		if _, stored := statuses[serverName]; stored {
			continue
		}
		err = multierr.Append(err, r.runtime.Metadata().PatchDataServerStatus(serverName,
			func(status *proto.DataServerStatus) (bool, error) {
				if status.GetState() != proto.DataServerState_DATA_SERVER_STATE_UNSPECIFIED {
					return false, nil
				}
				status.State = proto.DataServerState_DATA_SERVER_STATE_RUNNING
				return true, nil
			}))
	}

	for name := range r.runtime.ListDataServer() {
		if !desired.Contains(name) {
			r.runtime.DeleteDataServer(name)
		}
	}

	r.runtime.RefreshShardControllers()
	return err
}
