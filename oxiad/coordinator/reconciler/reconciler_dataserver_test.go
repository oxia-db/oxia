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

// The data servers of the configuration are created, and the ones without a
// stored status are stored RUNNING.
func TestDataServerReconcilerReconcileStoresRunningDataServers(t *testing.T) {
	runtime := namespaceRuntimeForErrors()
	runtime.metadata.status.DataServers = map[string]*proto.DataServerStatus{
		"running": {State: proto.DataServerState_DATA_SERVER_STATE_RUNNING},
	}
	reconciler := &dataServerReconciler{runtime: runtime}

	require.NoError(t, reconciler.Reconcile(t.Context(), dataServersConfig("new", "running")))

	assert.Equal(t, []string{"new", "running"}, runtime.createdDataServers)
	assert.Equal(t, map[string]proto.DataServerState{
		"new":     proto.DataServerState_DATA_SERVER_STATE_RUNNING,
		"running": proto.DataServerState_DATA_SERVER_STATE_RUNNING,
	}, storedDataServerStates(runtime))
}

// A data server that left the cluster, stored DRAINING or DRAINED, or only
// draining in the runtime yet, isn't reconciled when the configuration has it
// again.
func TestDataServerReconcilerReconcileRejectsRemovedDataServers(t *testing.T) {
	runtime := namespaceRuntimeForErrors()
	runtime.metadata.status.DataServers = map[string]*proto.DataServerStatus{
		"draining": {State: proto.DataServerState_DATA_SERVER_STATE_DRAINING},
		"drained":  {State: proto.DataServerState_DATA_SERVER_STATE_DRAINED},
	}
	runtime.dataServerStatus = map[string]*proto.DataServerStatus{
		"draining": {State: proto.DataServerState_DATA_SERVER_STATE_RUNNING},
		"leaving":  {State: proto.DataServerState_DATA_SERVER_STATE_DRAINING},
	}
	reconciler := &dataServerReconciler{runtime: runtime}

	require.NoError(t, reconciler.Reconcile(t.Context(), dataServersConfig("new", "draining", "drained", "leaving")))

	assert.Equal(t, []string{"new"}, runtime.createdDataServers)
	assert.Equal(t, []string{"draining"}, runtime.drainedDataServers)
	assert.Equal(t, map[string]proto.DataServerState{
		"new":      proto.DataServerState_DATA_SERVER_STATE_RUNNING,
		"draining": proto.DataServerState_DATA_SERVER_STATE_DRAINING,
		"drained":  proto.DataServerState_DATA_SERVER_STATE_DRAINED,
	}, storedDataServerStates(runtime))
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
// reconciler retries, but the controllers are still reconciled.
func TestDataServerReconcilerReconcileReturnsStatusErrors(t *testing.T) {
	runtime := namespaceRuntimeForErrors()
	reconciler := &dataServerReconciler{runtime: runtime}
	readErr := errors.New("status unavailable")
	runtime.metadata.listDataServerStatusErr = readErr
	runtime.dataServerStatus = map[string]*proto.DataServerStatus{
		"s2": {State: proto.DataServerState_DATA_SERVER_STATE_RUNNING},
	}
	// s1 could have drained: it is left to the retry, and s2 is kept
	require.ErrorIs(t, reconciler.Reconcile(t.Context(), dataServersConfig("s1", "s2")), readErr)
	assert.Empty(t, runtime.createdDataServers)
	assert.Empty(t, runtime.drainedDataServers)
	assert.Empty(t, runtime.metadata.status.GetDataServers())

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

func storedDataServerStates(runtime *mockNamespaceRuntime) map[string]proto.DataServerState {
	states := map[string]proto.DataServerState{}
	for name, status := range runtime.metadata.status.GetDataServers() {
		states[name] = status.GetState()
	}
	return states
}
