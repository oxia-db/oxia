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
	"path/filepath"
	"strconv"
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

// embeddedCluster is the in-process cluster of the coordinator package
// documentation: three data servers and a coordinator on the file metadata
// provider, every server with its metrics on an ephemeral port.
type embeddedCluster struct {
	dataServers []*dataserver.Server
	coordinator *coordinator.GrpcServer
	identities  []*proto.DataServerIdentity
}

// newDataServerIdentities picks the addresses of the data servers. They are
// fixed for the life of the cluster: the coordinator stores them in the
// cluster configuration, so a restarted data server must bind them again.
func newDataServerIdentities(t *testing.T) []*proto.DataServerIdentity {
	t.Helper()

	identities := make([]*proto.DataServerIdentity, 3)
	for i := range identities {
		identities[i] = &proto.DataServerIdentity{
			Public:   freeAddress(t),
			Internal: freeAddress(t),
		}
	}
	return identities
}

func startEmbeddedCluster(t *testing.T, dir string, identities []*proto.DataServerIdentity) *embeddedCluster {
	t.Helper()

	cluster := &embeddedCluster{identities: identities}
	for i, identity := range identities {
		options := dataserveroption.NewDefaultOptions()
		options.Server.Public.BindAddress = identity.Public
		options.Server.Internal.BindAddress = identity.Internal
		options.Observability.Metric.BindAddress = "localhost:0"
		options.Storage.Database.Dir = filepath.Join(dir, strconv.Itoa(i), "db")
		options.Storage.WAL.Dir = filepath.Join(dir, strconv.Itoa(i), "wal")

		server, err := dataserver.New(t.Context(), options)
		require.NoError(t, err)
		cluster.dataServers = append(cluster.dataServers, server)
	}

	options := coordinatoroption.NewDefaultOptions()
	options.Server.Public.BindAddress = "localhost:0"
	options.Server.Internal.BindAddress = "localhost:0"
	options.Observability.Metric.BindAddress = "localhost:0"
	options.Metadata.ProviderName = coordinatoroption.ProviderFile
	options.Metadata.File.Dir = filepath.Join(dir, "coordinator")

	coord, err := coordinator.New(t.Context(), options,
		coordinator.WithInitialClusterConfiguration(&proto.ClusterConfiguration{
			Namespaces: []*proto.Namespace{{
				Name:              constant.DefaultNamespace,
				ReplicationFactor: 3,
				InitialShardCount: 2,
			}},
			Servers: identities,
		}))
	require.NoError(t, err)
	cluster.coordinator = coord
	return cluster
}

func (c *embeddedCluster) Close() error {
	err := c.coordinator.Close()
	for _, server := range c.dataServers {
		if closeErr := server.Close(); err == nil {
			err = closeErr
		}
	}
	return err
}

func newClient(t *testing.T, address string) oxia.SyncClient {
	t.Helper()

	client, err := oxia.NewSyncClient(address)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, client.Close())
	})
	return client
}

func putKeys(t *testing.T, client oxia.SyncClient) {
	t.Helper()

	for i := range 10 {
		_, _, err := client.Put(t.Context(), fmt.Sprintf("key-%d", i), fmt.Appendf(nil, "value-%d", i))
		require.NoError(t, err)
	}
}

func requireKeys(t *testing.T, client oxia.SyncClient) {
	t.Helper()

	for i := range 10 {
		_, value, _, err := client.Get(t.Context(), fmt.Sprintf("key-%d", i))
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("value-%d", i), string(value))
	}
}

func TestEmbeddedCluster(t *testing.T) {
	cluster := startEmbeddedCluster(t, t.TempDir(), newDataServerIdentities(t))
	t.Cleanup(func() {
		assert.NoError(t, cluster.Close())
	})
	assert.NotZero(t, cluster.coordinator.PublicPort())
	assert.NotZero(t, cluster.coordinator.InternalPort())

	client := newClient(t, cluster.identities[0].Public)
	putKeys(t, client)
	requireKeys(t, client)
}

// With the file metadata provider, the cluster survives a restart of the
// whole embedding process: the coordinator finds the cluster status (and its
// instance id) where it left it, and the data servers their data.
func TestEmbeddedClusterSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	identities := newDataServerIdentities(t)

	cluster := startEmbeddedCluster(t, dir, identities)
	client, err := oxia.NewSyncClient(identities[0].Public)
	require.NoError(t, err)
	putKeys(t, client)
	require.NoError(t, client.Close())
	require.NoError(t, cluster.Close())

	cluster = startEmbeddedCluster(t, dir, identities)
	t.Cleanup(func() {
		assert.NoError(t, cluster.Close())
	})
	requireKeys(t, newClient(t, identities[1].Public))
}

func TestEmbeddedStandalone(t *testing.T) {
	config := dataserver.StandaloneConfig{NotificationsEnabled: true}
	config.DataServerOptions.Server.Public.BindAddress = "localhost:0"
	config.DataServerOptions.Server.Internal.BindAddress = "localhost:0"
	config.DataServerOptions.Observability.Metric.BindAddress = "localhost:0"
	config.DataServerOptions.Storage.Database.Dir = t.TempDir()
	config.DataServerOptions.Storage.WAL.Dir = t.TempDir()

	server, err := dataserver.NewStandalone(config)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, server.Close())
	})

	client := newClient(t, server.ServiceAddr())

	notifications, err := client.GetNotifications()
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, notifications.Close())
	})

	_, _, err = client.Put(t.Context(), "key", []byte("value"))
	require.NoError(t, err)

	_, value, _, err := client.Get(t.Context(), "key")
	require.NoError(t, err)
	assert.Equal(t, "value", string(value))

	notification := <-notifications.Ch()
	assert.Equal(t, "key", notification.Key)
}
