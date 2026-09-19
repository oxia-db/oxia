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

// Package coordinator implements the Oxia coordinator: it manages the
// cluster status, assigns shards to data servers and drives leader
// elections. It backs the `oxia coordinator` command and can equally be
// embedded in a Go application together with the dataserver package, running
// a whole Oxia cluster in-process:
//
//	ctx := context.Background()
//
//	var identities []*proto.DataServerIdentity
//	for i := 0; i < 3; i++ {
//		dsOptions := dsoption.NewDefaultOptions()
//		dsOptions.Server.Public.BindAddress = "localhost:0"
//		dsOptions.Server.Internal.BindAddress = "localhost:0"
//		dsOptions.Storage.Database.Dir = filepath.Join(dataDir, strconv.Itoa(i), "db")
//		dsOptions.Storage.WAL.Dir = filepath.Join(dataDir, strconv.Itoa(i), "wal")
//
//		server, err := dataserver.New(ctx, dsOptions)
//		if err != nil {
//			return err
//		}
//		defer server.Close()
//
//		identities = append(identities, &proto.DataServerIdentity{
//			Public:   fmt.Sprintf("localhost:%d", server.PublicPort()),
//			Internal: fmt.Sprintf("localhost:%d", server.InternalPort()),
//		})
//	}
//
//	options := option.NewDefaultOptions()
//	options.Server.Public.BindAddress = "localhost:0"
//	options.Server.Internal.BindAddress = "localhost:0"
//	options.Metadata.ProviderName = option.ProviderMemory
//
//	coord, err := coordinator.New(ctx, options,
//		coordinator.WithInitialClusterConfiguration(&proto.ClusterConfiguration{
//			Namespaces: []*proto.Namespace{{
//				Name:              constant.DefaultNamespace,
//				ReplicationFactor: 3,
//				InitialShardCount: 1,
//			}},
//			Servers: identities,
//		}))
//	if err != nil {
//		return err
//	}
//	defer coord.Close()
//
//	client, err := oxia.NewSyncClient(identities[0].Public)
//
// The example uses the in-memory metadata provider, which keeps the cluster
// status inside the coordinator process and is suitable when a single
// coordinator is embedded. Multi-coordinator topologies should use the file
// or raft metadata providers instead, and must decide what happens when a
// coordinator loses the metadata leadership: by default the whole process
// exits, which embedding applications usually want to override with
// [WithOnLeadershipLost].
package coordinator
