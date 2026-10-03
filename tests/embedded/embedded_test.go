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

// Package embedded verifies that a whole Oxia cluster can be embedded in a
// single Go process through the public dataserver and coordinator APIs.
package embedded

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia"
	"github.com/oxia-db/oxia/oxiad/coordinator"
	coordinatoroption "github.com/oxia-db/oxia/oxiad/coordinator/option"
	"github.com/oxia-db/oxia/oxiad/dataserver"
	dataserveroption "github.com/oxia-db/oxia/oxiad/dataserver/option"
)

func newDataServerOptions(t *testing.T) *dataserveroption.Options {
	t.Helper()

	options := dataserveroption.NewDefaultOptions()
	options.Server.Public.BindAddress = "localhost:0"
	options.Server.Internal.BindAddress = "localhost:0"
	options.Observability.Metric.Enabled = &constant.FlagFalse
	options.Storage.Database.Dir = t.TempDir()
	options.Storage.WAL.Dir = t.TempDir()
	return options
}

func newDataServer(t *testing.T) (*dataserver.Server, *proto.DataServerIdentity) {
	t.Helper()

	server, err := dataserver.New(t.Context(), newDataServerOptions(t))
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, server.Close())
	})

	return server, &proto.DataServerIdentity{
		Public:   fmt.Sprintf("localhost:%d", server.PublicPort()),
		Internal: fmt.Sprintf("localhost:%d", server.InternalPort()),
	}
}

func TestEmbeddedCluster(t *testing.T) {
	_, id1 := newDataServer(t)
	_, id2 := newDataServer(t)
	_, id3 := newDataServer(t)

	options := coordinatoroption.NewDefaultOptions()
	options.Server.Public.BindAddress = "localhost:0"
	options.Server.Internal.BindAddress = "localhost:0"
	options.Observability.Metric.Enabled = &constant.FlagFalse
	options.Metadata.Name = "embedded-coordinator"
	options.Metadata.ProviderName = coordinatoroption.ProviderMemory

	coord, err := coordinator.New(t.Context(), options,
		coordinator.WithInitialClusterConfiguration(&proto.ClusterConfiguration{
			Namespaces: []*proto.Namespace{{
				Name:              constant.DefaultNamespace,
				ReplicationFactor: 3,
				InitialShardCount: 2,
			}},
			Servers: []*proto.DataServerIdentity{id1, id2, id3},
		}))
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, coord.Close())
	})
	assert.NotZero(t, coord.PublicPort())
	assert.NotZero(t, coord.InternalPort())

	client, err := oxia.NewSyncClient(id1.Public)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, client.Close())
	})

	ctx := t.Context()
	for i := range 10 {
		key := fmt.Sprintf("key-%d", i)
		_, _, err = client.Put(ctx, key, fmt.Appendf(nil, "value-%d", i))
		require.NoError(t, err)
	}
	for i := range 10 {
		key := fmt.Sprintf("key-%d", i)
		_, value, _, err := client.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("value-%d", i), string(value))
	}
}

func TestEmbeddedStandalone(t *testing.T) {
	config := dataserver.StandaloneConfig{}
	config.DataServerOptions.Server.Public.BindAddress = "localhost:0"
	config.DataServerOptions.Server.Internal.BindAddress = "localhost:0"
	config.DataServerOptions.Observability.Metric.Enabled = &constant.FlagFalse
	config.DataServerOptions.Storage.Database.Dir = t.TempDir()
	config.DataServerOptions.Storage.WAL.Dir = t.TempDir()

	server, err := dataserver.NewStandalone(config)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, server.Close())
	})

	client, err := oxia.NewSyncClient(server.ServiceAddr())
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, client.Close())
	})

	ctx := t.Context()
	_, _, err = client.Put(ctx, "key", []byte("value"))
	require.NoError(t, err)

	_, value, _, err := client.Get(ctx, "key")
	require.NoError(t, err)
	assert.Equal(t, "value", string(value))
}

func TestEmbeddedDataServerUpdateOptions(t *testing.T) {
	server, _ := newDataServer(t)

	assert.Error(t, server.UpdateOptions(nil))
	assert.NoError(t, server.UpdateOptions(newDataServerOptions(t)))
}
