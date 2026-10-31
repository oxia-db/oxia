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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
)

// The data servers of the configuration are stored RUNNING: the ones without
// a stored status, and the drained ones added again. A draining one added
// again keeps draining.
func TestDataServerReconcilerReconcileStoresRunningDataServers(t *testing.T) {
	runtime := namespaceRuntimeForErrors()
	runtime.metadata.status.DataServers = map[string]*proto.DataServerStatus{
		"draining": {State: proto.DataServerState_DATA_SERVER_STATE_DRAINING},
		"drained":  {State: proto.DataServerState_DATA_SERVER_STATE_DRAINED},
		"running":  {State: proto.DataServerState_DATA_SERVER_STATE_RUNNING},
	}
	reconciler := &dataServerReconciler{runtime: runtime}

	require.NoError(t, reconciler.Reconcile(t.Context(), dataServersConfig("new", "draining", "drained", "running")))

	states := map[string]proto.DataServerState{}
	for name, status := range runtime.metadata.status.GetDataServers() {
		states[name] = status.GetState()
	}
	assert.Equal(t, map[string]proto.DataServerState{
		"new":      proto.DataServerState_DATA_SERVER_STATE_RUNNING,
		"draining": proto.DataServerState_DATA_SERVER_STATE_DRAINING,
		"drained":  proto.DataServerState_DATA_SERVER_STATE_RUNNING,
		"running":  proto.DataServerState_DATA_SERVER_STATE_RUNNING,
	}, states)
}

// A data server stored RUNNING since its status was read isn't written again.
func TestDataServerReconcilerReconcileKeepsRunningStoredSinceRead(t *testing.T) {
	runtime := namespaceRuntimeForErrors()
	runtime.metadata.status.DataServers = map[string]*proto.DataServerStatus{
		"s1": {State: proto.DataServerState_DATA_SERVER_STATE_RUNNING},
	}
	runtime.metadata.staleDataServerStatus = map[string]*proto.DataServerStatus{}
	reconciler := &dataServerReconciler{runtime: runtime}

	require.NoError(t, reconciler.Reconcile(t.Context(), dataServersConfig("s1")))
	assert.Equal(t, proto.DataServerState_DATA_SERVER_STATE_RUNNING,
		runtime.metadata.status.GetDataServers()["s1"].GetState())
}

// A status that can't be read or stored fails the pass, which the cluster
// reconciler retries.
func TestDataServerReconcilerReconcileReturnsStatusErrors(t *testing.T) {
	runtime := namespaceRuntimeForErrors()
	reconciler := &dataServerReconciler{runtime: runtime}
	readErr := errors.New("status unavailable")
	runtime.metadata.listDataServerStatusErr = readErr
	require.ErrorIs(t, reconciler.Reconcile(t.Context(), dataServersConfig("s1")), readErr)

	runtime.metadata.listDataServerStatusErr = nil
	writeErr := errors.New("store unavailable")
	runtime.metadata.patchDataServerStatusErr = writeErr
	require.ErrorIs(t, reconciler.Reconcile(t.Context(), dataServersConfig("s1")), writeErr)
	assert.Empty(t, runtime.metadata.status.GetDataServers())
}

func dataServersConfig(names ...string) *proto.ClusterConfiguration {
	config := &proto.ClusterConfiguration{}
	for _, name := range names {
		config.Servers = append(config.Servers, &proto.DataServerIdentity{
			Name: &name, Public: name + ":9091", Internal: name + ":8191",
		})
	}
	return config
}
