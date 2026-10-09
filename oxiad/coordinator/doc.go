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
//		dsOptions.Observability.Metric.BindAddress = "localhost:0"
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
//	options.Observability.Metric.BindAddress = "localhost:0"
//	options.Metadata.ProviderName = option.ProviderFile
//	options.Metadata.File.Dir = filepath.Join(dataDir, "coordinator")
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
// Every server serves metrics on 0.0.0.0:8080 by default: servers sharing a
// process need distinct metrics addresses, or metrics disabled.
//
// # Choosing a metadata provider
//
// The coordinator keeps the cluster status (including the cluster instance
// id, which every data server records and checks) in its metadata provider:
//
//   - file: the status survives a restart of the process. The right choice for
//     a single embedded coordinator with persistent data servers, as above.
//     Its leadership is a lock on a local file, so it does not elect a leader
//     among coordinators on different machines.
//   - raft or configmap (on Kubernetes): for several coordinators, on
//     different machines, of which one leads at a time.
//   - memory: the status is lost when the process exits, and a restarted
//     coordinator mints a new cluster instance id that the data servers
//     reject. Use it only with data servers whose storage is ephemeral too,
//     as in tests.
//
// With the file, configmap or raft provider, [New] blocks until the
// coordinator acquires the metadata leadership; cancel its context to stop
// waiting.
//
// Only the raft and configmap providers can lose the leadership. By default a
// coordinator that loses it terminates the process; an embedding application
// sets [WithOnLeadershipLost] instead, and starts a new coordinator to take
// part in the next election.
//
// # Limitations
//
// Embedded servers share process-global state, and some failures still
// terminate the host process:
//
//   - The metrics package installs the global OpenTelemetry MeterProvider when
//     it is loaded, and every metrics endpoint serves the default Prometheus
//     registry: the servers of one process share one metrics pipeline.
//   - The log level is package-global, shared by all the servers.
//   - A failure to serve gRPC or metrics, and a fatal Pebble error, exit the
//     process.
package coordinator
