// Copyright 2023-2025 The Oxia Authors
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

	"github.com/emirpasic/gods/v2/sets/hashset"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime"
)

var _ Reconciler = (*dataServerReconciler)(nil)

type dataServerReconciler struct {
	runtime runtime.Runtime
}

func (*dataServerReconciler) Close() error { return nil }

func (r *dataServerReconciler) Reconcile(_ context.Context, snapshot *proto.ClusterConfiguration) error {
	desired := hashset.New[string]()

	for _, server := range snapshot.GetServers() {
		serverName := server.GetNameOrDefault()
		desired.Add(serverName)
		configuredServer, exist := snapshot.GetDataServer(serverName)
		if !exist {
			continue
		}
		r.runtime.PutDataServerIfAbsent(configuredServer)
	}

	for serverID := range r.runtime.NodeControllers() {
		if !desired.Contains(serverID) {
			r.runtime.DeleteDataServer(serverID)
		}
	}

	r.runtime.SyncShardControllerServerAddresses()
	return nil
}
