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
//	// Bootstrap the cluster configuration on the first start: a restart
//	// keeps the stored one, with the changes made through the admin API.
//	configPath := options.Metadata.File.ConfigPath()
//	if _, err := os.Stat(configPath); errors.Is(err, fs.ErrNotExist) {
//		config, err := codec.ClusterConfigCodec.MarshalYAML(&proto.ClusterConfiguration{
//			Namespaces: []*proto.Namespace{{
//				Name:              constant.DefaultNamespace,
//				ReplicationFactor: 3,
//				InitialShardCount: 1,
//			}},
//			Servers: identities,
//		})
//		if err != nil {
//			return err
//		}
//		if err := os.MkdirAll(options.Metadata.File.Dir, 0o755); err != nil {
//			return err
//		}
//		if err := os.WriteFile(configPath, config, 0o600); err != nil {
//			return err
//		}
//	}
//
//	coord, err := coordinator.New(ctx, commonwatch.New(options))
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
// Only the raft and configmap providers can lose the leadership. A coordinator
// that loses it stops coordinating at once, so that it can't act alongside the
// new leader, and then closes [Coordinator.LeadershipLost]. Close it, and call
// [New] again to rejoin the election:
//
//	for ctx.Err() == nil {
//		coord, err := coordinator.New(ctx, optionsWatch)
//		if err != nil {
//			return err
//		}
//		select {
//		case <-coord.LeadershipLost():
//		case <-ctx.Done():
//		}
//		if err := coord.Close(); err != nil {
//			return err
//		}
//	}
//
// The `oxia coordinator` command exits instead, and its restart rejoins.
//
// # Limitations
//
// Embedded servers share process-global state, and a fatal error still
// terminates the host process:
//
//   - The metrics package installs the global OpenTelemetry MeterProvider when
//     it is loaded, and every metrics endpoint serves the default Prometheus
//     registry: the servers of one process share one metrics pipeline.
//   - The log level is package-global, shared by all the servers.
//   - A fatal Pebble error panics.
package coordinator
