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

	"github.com/emirpasic/gods/v2/sets/hashset"
	"github.com/pkg/errors"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/controller"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/resource"
)

// ShardSplitter is implemented by the coordinator to initiate shard splits.
type ShardSplitter interface {
	InitiateSplit(namespace string, parentShardId int64, splitPoint *uint32) (leftChild, rightChild int64, err error)
}

type DataServerInfo struct {
	Status            controller.DataServerStatus
	FeaturesSupported []proto.Feature
}

var _ proto.OxiaAdminServer = (*adminServer)(nil)

type adminServer struct {
	proto.UnimplementedOxiaAdminServer

	statusResource resource.StatusResource
	clusterConfig  func() (model.ClusterConfig, error)
	dataServers    func() map[string]DataServerInfo
	shardSplitter  ShardSplitter
}

func (admin *adminServer) ListDataServers(context.Context, *proto.ListDataServersRequest) (*proto.ListDataServersResponse, error) {
	cnf, err := admin.clusterConfig()
	if err != nil {
		return nil, err
	}

	status := admin.statusResource.Load()
	runtimeData := map[string]DataServerInfo{}
	if admin.dataServers != nil {
		runtimeData = admin.dataServers()
	}
	shardCounts := getShardCounts(status)

	dataServers := make([]*proto.DataServer, len(cnf.Servers))
	for i, server := range cnf.Servers {
		serverID := server.GetIdentifier()
		serverMetadata, found := cnf.ServerMetadata[serverID]
		if !found {
			serverMetadata = model.ServerMetadata{}
		}

		runtimeInfo, found := runtimeData[serverID]
		serverStatus := proto.DataServerStatus_DATA_SERVER_STATUS_UNKNOWN
		if found {
			serverStatus = toProtoDataServerStatus(runtimeInfo.Status)
		}

		dataServers[i] = &proto.DataServer{
			Name:              server.Name,
			PublicAddress:     server.Public,
			InternalAddress:   server.Internal,
			Metadata:          serverMetadata.Labels,
			Status:            serverStatus,
			FeaturesSupported: runtimeInfo.FeaturesSupported,
			ShardCount:        shardCounts[serverID],
		}
	}

	return &proto.ListDataServersResponse{DataServers: dataServers}, nil
}

func (admin *adminServer) ListNamespaces(context.Context, *proto.ListNamespacesRequest) (*proto.ListNamespacesResponse, error) {
	cnf, err := admin.clusterConfig()
	if err != nil {
		return nil, err
	}

	namespaceConfigs := cnf.Namespaces
	namespaceNames := hashset.New[string]()
	for _, nsConfig := range namespaceConfigs {
		namespaceNames.Add(nsConfig.Name)
	}
	return &proto.ListNamespacesResponse{
		Namespaces: namespaceNames.Values(),
	}, nil
}

func getShardCounts(status *model.ClusterStatus) map[string]uint32 {
	shardCounts := make(map[string]uint32)
	if status == nil {
		return shardCounts
	}

	for _, namespace := range status.Namespaces {
		for _, shard := range namespace.Shards {
			servers := hashset.New[string]()
			for _, member := range shard.Ensemble {
				servers.Add(member.GetIdentifier())
			}
			for _, member := range shard.RemovedNodes {
				servers.Add(member.GetIdentifier())
			}
			for _, member := range shard.PendingDeleteShardNodes {
				servers.Add(member.GetIdentifier())
			}
			for _, serverID := range servers.Values() {
				shardCounts[serverID]++
			}
		}
	}

	return shardCounts
}

func toProtoDataServerStatus(status controller.DataServerStatus) proto.DataServerStatus {
	switch status {
	case controller.Running:
		return proto.DataServerStatus_DATA_SERVER_STATUS_RUNNING
	case controller.NotRunning:
		return proto.DataServerStatus_DATA_SERVER_STATUS_NOT_RUNNING
	case controller.Draining:
		return proto.DataServerStatus_DATA_SERVER_STATUS_DRAINING
	default:
		return proto.DataServerStatus_DATA_SERVER_STATUS_UNKNOWN
	}
}

func (admin *adminServer) ListNodes(context.Context, *proto.ListNodesRequest) (*proto.ListNodesResponse, error) {
	cnf, err := admin.clusterConfig()
	if err != nil {
		return nil, err
	}

	cnfNodes := cnf.Servers
	cnfMeta := cnf.ServerMetadata
	nodes := make([]*proto.Node, len(cnfNodes))
	for i, node := range cnfNodes {
		nodeMeta, found := cnfMeta[node.GetIdentifier()]
		if !found {
			nodeMeta = model.ServerMetadata{}
		}
		nodes[i] = &proto.Node{
			Name:            node.Name,
			PublicAddress:   node.Public,
			InternalAddress: node.Internal,
			Metadata:        nodeMeta.Labels,
		}
	}
	return &proto.ListNodesResponse{Nodes: nodes}, nil
}

func (admin *adminServer) SplitShard(_ context.Context, req *proto.SplitShardRequest) (*proto.SplitShardResponse, error) {
	if admin.shardSplitter == nil {
		return nil, errors.New("split shard not supported")
	}

	slog.Info("Received SplitShard request",
		slog.String("namespace", req.Namespace),
		slog.Int64("shard", req.Shard),
		slog.Any("split-point", req.SplitPoint),
	)

	left, right, err := admin.shardSplitter.InitiateSplit(req.Namespace, req.Shard, req.SplitPoint)
	if err != nil {
		return nil, err
	}

	return &proto.SplitShardResponse{
		LeftChildShard:  left,
		RightChildShard: right,
	}, nil
}

func newAdminServer(
	statusResource resource.StatusResource,
	clusterConfig func() (model.ClusterConfig, error),
	dataServers func() map[string]DataServerInfo,
	shardSplitter ShardSplitter,
) *adminServer {
	return &adminServer{
		statusResource: statusResource,
		clusterConfig:  clusterConfig,
		dataServers:    dataServers,
		shardSplitter:  shardSplitter,
	}
}
