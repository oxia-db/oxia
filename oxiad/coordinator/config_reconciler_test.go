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

package coordinator

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/rpc"
)

func TestConfigReconcilerReconcileInitialAndUpdatedConfig(t *testing.T) {
	server1 := &proto.DataServerIdentity{Internal: "s1"}
	server2 := &proto.DataServerIdentity{Internal: "s2"}
	initial := &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{
			Name:              "default",
			InitialShardCount: 1,
			ReplicationFactor: 1,
		}},
		Servers: []*proto.DataServerIdentity{server1},
	}
	updated := &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{
			Name:              "default",
			InitialShardCount: 1,
			ReplicationFactor: 1,
		}},
		Servers: []*proto.DataServerIdentity{server2},
	}

	metadata := newTestMetadata(t, initial)
	coord, err := newCoordinator(metadata, rpc.NewRpcProviderFactory(nil))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, coord.Close())
	})

	c := coord.(*coordinator)
	reconciler := NewConfigReconciler(slog.Default(), c)
	reconciler.Reconcile(initial)

	_, ok := c.nodeControllers[server1.GetNameOrDefault()]
	assert.True(t, ok)

	reconciler.Reconcile(updated)
	_, ok = c.nodeControllers[server2.GetNameOrDefault()]
	assert.True(t, ok)
}

func TestConfigReconcilerRunSkipsNilConfig(t *testing.T) {
	metadata := newTestMetadata(t, nil)
	coord, err := newCoordinator(metadata, rpc.NewRpcProviderFactory(nil))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, coord.Close())
	})

	c := coord.(*coordinator)
	reconciler := NewConfigReconciler(slog.Default(), c)

	ctx, cancel := context.WithCancel(t.Context())
	go reconciler.Run(ctx)
	time.Sleep(50 * time.Millisecond)
	cancel()
}
