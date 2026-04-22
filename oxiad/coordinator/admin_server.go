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
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/proto"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
)

// ShardSplitter is implemented by the coordinator to initiate shard splits.
type ShardSplitter interface {
	InitiateSplit(namespace string, parentShardId int64, splitPoint *uint32) (leftChild, rightChild int64, err error)
}

var _ proto.OxiaAdminServer = (*adminServer)(nil)

type adminServer struct {
	proto.UnimplementedOxiaAdminServer

	metadata      coordmetadata.Metadata
	shardSplitter ShardSplitter
}

func (admin *adminServer) ListDataServers(context.Context, *proto.ListDataServersRequest) (*proto.ListDataServersResponse, error) {
	cnf := admin.metadata.LoadConfig()

	dataServers := make([]*proto.DataServer, 0, len(cnf.GetServers()))
	for _, server := range cnf.GetServers() {
		dataServer := server
		if server.GetName() == "" {
			name := server.GetIdentifier()
			dataServer = &proto.DataServer{
				Name:     &name,
				Public:   server.GetPublic(),
				Internal: server.GetInternal(),
			}
		}
		dataServers = append(dataServers, dataServer)
	}

	return &proto.ListDataServersResponse{DataServers: dataServers}, nil
}

func (admin *adminServer) GetDataServer(_ context.Context, req *proto.GetDataServerRequest) (*proto.GetDataServerResponse, error) {
	if req == nil || req.DataServer == "" {
		return nil, grpcstatus.Error(codes.InvalidArgument, "data server must not be empty")
	}

	dataServerInfo, found := admin.metadata.GetDataServerInfo(req.DataServer)
	if !found {
		return nil, grpcstatus.Errorf(codes.NotFound, "data server %q not found", req.DataServer)
	}

	return &proto.GetDataServerResponse{
		DataServerInfo: dataServerInfo,
	}, nil
}

func (admin *adminServer) ListNamespaces(context.Context, *proto.ListNamespacesRequest) (*proto.ListNamespacesResponse, error) {
	cnf := admin.metadata.LoadConfig()

	namespaceNames := hashset.New[string]()
	for _, nsConfig := range cnf.GetNamespaces() {
		namespaceNames.Add(nsConfig.GetName())
	}
	return &proto.ListNamespacesResponse{
		Namespaces: namespaceNames.Values(),
	}, nil
}

func (admin *adminServer) ListNodes(context.Context, *proto.ListNodesRequest) (*proto.ListNodesResponse, error) {
	cnf := admin.metadata.LoadConfig()

	cnfNodes := cnf.GetServers()
	cnfMeta := cnf.GetServerMetadata()
	nodes := make([]*proto.Node, len(cnfNodes))
	for i, node := range cnfNodes {
		name := node.GetIdentifier()
		nodes[i] = &proto.Node{
			Name:            &name,
			PublicAddress:   node.GetPublic(),
			InternalAddress: node.GetInternal(),
			Metadata:        map[string]string{},
		}
		if nodeMeta, found := cnfMeta[node.GetIdentifier()]; found {
			nodes[i].Metadata = nodeMeta.GetLabels()
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
	metadata coordmetadata.Metadata,
	shardSplitter ShardSplitter,
) *adminServer {
	return &adminServer{
		metadata:      metadata,
		shardSplitter: shardSplitter,
	}
}
