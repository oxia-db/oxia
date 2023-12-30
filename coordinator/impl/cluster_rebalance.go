// Copyright 2023 StreamNative, Inc.
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

package impl

import (
	"log/slog"
	"sort"

	"github.com/streamnative/oxia/common"
	"github.com/streamnative/oxia/coordinator/model"
)

type SwapNodeAction struct {
	Shard int64
	From  model.ServerAddress
	To    model.ServerAddress
}

// Make sure every server is assigned a similar number of shards
// Output a list of actions to be taken to rebalance the cluster.
func rebalanceCluster(servers []model.ServerAddress, currentStatus *model.ClusterStatus) []SwapNodeAction { //nolint:revive
	res := make([]SwapNodeAction, 0)

	serversCount := len(servers)
	shardsPerServer, deletedServers := getShardsPerServer(servers, currentStatus)

outer:
	for {
		rankings := getServerRanking(shardsPerServer)
		slog.Debug("Computed rankings: ")
		for _, r := range rankings {
			slog.Debug(
				"",
				slog.String("server", r.Addr.Internal),
				slog.Int("count", r.Shards.Count()),
			)
		}
		if len(deletedServers) > 0 {
			slog.Debug("Deleted servers: ")
			for ds, shards := range deletedServers {
				slog.Debug(
					"",
					slog.String("server", ds.Internal),
					slog.Int("count", shards.Count()),
				)
			}
		}
		slog.Debug("------------------------------")

		// First try to reassign shards from the removed servers.
		// We do it one by one, by placing in the lead loaded server
		if len(deletedServers) > 0 {
			ds, shards := getFirstEntry(deletedServers)

			for j := serversCount - 1; j >= 0; j-- {
				to := rankings[j]
				eligibleShards := shards.Complement(to.Shards)

				if !eligibleShards.IsEmpty() {
					a := SwapNodeAction{
						Shard: eligibleShards.GetSorted()[0],
						From:  ds,
						To:    to.Addr,
					}

					shards.Remove(a.Shard)
					if shards.IsEmpty() {
						delete(deletedServers, ds)
					} else {
						deletedServers[ds] = shards
					}
					shardsPerServer[a.To].Add(a.Shard)

					slog.Debug(
						"Transfer from removed node",
						slog.Any("swap-action", a),
					)

					res = append(res, a)
					continue outer
				}
			}

			slog.Warn("It wasn't possible to reassign any shard from deleted servers")
			break
		}

		// Find a shard from the most loaded server that can be moved to the
		// least loaded server, with the constraint that multiple replicas of
		// the same shard should not be assigned to one server
		mostLoaded := rankings[0]
		leastLoaded := rankings[serversCount-1]
		if mostLoaded.Shards.Count() <= leastLoaded.Shards.Count()+1 {
			break
		}

		eligibleShards := mostLoaded.Shards.Complement(leastLoaded.Shards)
		if eligibleShards.IsEmpty() {
			break
		}

		a := SwapNodeAction{
			Shard: eligibleShards.GetSorted()[0],
			From:  mostLoaded.Addr,
			To:    leastLoaded.Addr,
		}

		shardsPerServer[a.From].Remove(a.Shard)
		shardsPerServer[a.To].Add(a.Shard)

		slog.Debug(
			"Swapping nodes",
			slog.Any("swap-action", a),
		)

		res = append(res, a)
	}

	return res
}

func getShardsPerServer(servers []model.ServerAddress, currentStatus *model.ClusterStatus) ( //nolint:revive
	existingServers map[model.ServerAddress]common.Set[int64],
	deletedServers map[model.ServerAddress]common.Set[int64]) {
	existingServers = map[model.ServerAddress]common.Set[int64]{}
	deletedServers = map[model.ServerAddress]common.Set[int64]{}

	for _, s := range servers {
		existingServers[s] = common.NewSet[int64]()
	}

	for _, nss := range currentStatus.Namespaces {
		for shardId, shard := range nss.Shards {
			for _, addr := range shard.Ensemble {
				if _, ok := existingServers[addr]; ok {
					existingServers[addr].Add(shardId)
					continue
				}

				// This server is getting removed
				if _, ok := deletedServers[addr]; !ok {
					deletedServers[addr] = common.NewSet[int64]()
				}

				deletedServers[addr].Add(shardId)
			}
		}
	}

	return existingServers, deletedServers
}

type ServerRank struct {
	Addr   model.ServerAddress
	Shards common.Set[int64]
}

func getServerRanking(shardsPerServer map[model.ServerAddress]common.Set[int64]) []ServerRank {
	res := make([]ServerRank, 0)

	for server, shards := range shardsPerServer {
		res = append(res, ServerRank{
			Addr:   server,
			Shards: shards,
		})
	}

	// Rank the servers from the one with most shards to the one with the least
	sort.SliceStable(res, func(i, j int) bool {
		c1 := res[i].Shards.Count()
		c2 := res[j].Shards.Count()
		if c1 != c2 {
			return c1 >= c2
		}

		// Ensure predictable sorting
		return res[i].Addr.Internal < res[j].Addr.Internal
	})
	return res
}

func getFirstEntry(m map[model.ServerAddress]common.Set[int64]) (model.ServerAddress, common.Set[int64]) {
	keys := make([]model.ServerAddress, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.SliceStable(keys, func(i, j int) bool {
		return keys[i].Internal < keys[j].Internal
	})

	return keys[0], m[keys[0]]
}
