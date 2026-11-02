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
	"errors"
	"log/slog"

	"github.com/emirpasic/gods/v2/sets/hashset"
	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/dataserver"
	"github.com/oxia-db/oxia/oxiad/common/errs"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime"
)

var _ Reconciler = (*dataServerReconciler)(nil)

type dataServerReconciler struct {
	runtime runtime.Runtime
}

func (*dataServerReconciler) Close() error { return nil }

func (r *dataServerReconciler) Reconcile(_ context.Context, snapshot *proto.ClusterConfiguration) error {
	desired := hashset.New[string]()
	// The controllers are reconciled also when the statuses can't be read or
	// stored: the runtime knows the data servers that drain, and the error
	// fails the pass, which is retried
	statuses, err := r.runtime.Metadata().ListDataServerStatus()
	// stale holds the data servers whose stored status lacks their state, or
	// their metadata as the configuration has it
	var stale []*proto.DataServer

	for _, dataServer := range snapshot.ListDataServers() {
		serverName := dataServer.GetNameOrDefault()
		// A data server that left the cluster can't join it again under the
		// same name until its drain is confirmed: it isn't reconciled, and
		// keeps draining
		state := statuses[serverName].UnsafeBorrow().GetState()
		live, found := r.runtime.GetLiveDataServerStatus(serverName)
		if found && !dataserver.IsRemoved(state) {
			state = live.GetState()
		}
		if dataserver.IsRemoved(state) {
			slog.Warn("Rejecting a data server of the configuration that was removed",
				slog.String("data-server", serverName), slog.String("state", state.String()))
			continue
		}
		desired.Add(serverName)
		// Without its stored status, a data server is left to the retry: it
		// could have drained
		if !found && err != nil {
			continue
		}
		r.runtime.CreateDataServer(serverName, dataServer)
		stored, found := statuses[serverName]
		if err == nil && (!found || !stored.UnsafeBorrow().GetMetadata().EqualVT(dataServer.GetMetadata())) {
			stale = append(stale, dataServer)
		}
	}

	for name := range r.runtime.ListLiveDataServer() {
		if !desired.Contains(name) {
			r.runtime.DeleteDataServer(name)
		}
	}

	r.runtime.RefreshShardControllers()

	err = multierr.Append(err, r.deleteConfirmedDrained(snapshot))
	for _, dataServer := range stale {
		err = multierr.Append(err, r.runtime.Metadata().PatchDataServerStatus(dataServer.GetNameOrDefault(),
			func(status *proto.DataServerStatus) (bool, error) {
				changed := false
				if status.GetState() == proto.DataServerState_DATA_SERVER_STATE_UNSPECIFIED {
					status.State = proto.DataServerState_DATA_SERVER_STATE_RUNNING
					changed = true
				}
				// The status keeps the metadata once the data server leaves the
				// configuration, for the replicas that it still has to drain
				if !status.GetMetadata().EqualVT(dataServer.GetMetadata()) {
					status.Metadata = dataServer.GetMetadata().CloneVT()
					changed = true
				}
				return changed, nil
			}))
	}
	return err
}

// deleteConfirmedDrained forgets the data servers whose drain the
// configuration confirms. A data server still in the configuration is
// ignored, one not DRAINED yet is left to a later pass, and one already
// forgotten is skipped.
func (r *dataServerReconciler) deleteConfirmedDrained(snapshot *proto.ClusterConfiguration) error {
	var err error
	for _, name := range snapshot.GetConfirmedDrainedDataServers() {
		if _, configured := snapshot.GetDataServer(name); configured {
			slog.Warn("Ignoring the confirmed drain of a data server of the configuration",
				slog.String("data-server", name))
			continue
		}
		forgetErr := r.runtime.ForgetDrainedDataServer(name)
		if forgetErr == nil {
			slog.Info("Forgot a confirmed drained data server", slog.String("data-server", name))
			continue
		}
		if !errors.Is(forgetErr, errs.ErrNotFound) && !errors.Is(forgetErr, errs.ErrFailedPrecondition) {
			err = multierr.Append(err, forgetErr)
		}
	}
	return err
}
