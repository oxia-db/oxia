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
	"github.com/oxia-db/oxia/oxiad/coordinator/controller"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"
	"github.com/oxia-db/oxia/oxiad/coordinator/resource"
)

// ShardSplitter is implemented by the coordinator to initiate shard splits.
type ShardSplitter interface {
	InitiateSplit(namespace string, parentShardId int64, splitPoint *uint32) (leftChild, rightChild int64, err error)
}

type DataServerFeatureProvider interface {
	NodeControllers() map[string]controller.DataServerController
}

var _ proto.OxiaAdminServer = (*adminServer)(nil)

type adminServer struct {
	proto.UnimplementedOxiaAdminServer

	statusResource     resource.StatusResource
	clusterConfig      func() (model.ClusterConfig, error)
	clusterConfigStore ClusterConfigStore
	shardSplitter      ShardSplitter
	featureSource      DataServerFeatureProvider
}

func (admin *adminServer) ListDataServers(context.Context, *proto.ListDataServersRequest) (*proto.ListDataServersResponse, error) {
	cnf, err := admin.clusterConfig()
	if err != nil {
		return nil, err
	}

	dataServers := make([]*proto.DataServer, len(cnf.Servers))
	for i, server := range cnf.Servers {
		dataServers[i] = admin.toProtoDataServer(cnf, server)
	}

	return &proto.ListDataServersResponse{DataServers: dataServers}, nil
}

func (admin *adminServer) GetDataServer(_ context.Context, req *proto.GetDataServerRequest) (*proto.DataServer, error) {
	cnf, err := admin.clusterConfig()
	if err != nil {
		return nil, err
	}

	server, ok := findDataServer(cnf, req.DataServer)
	if ok {
		return admin.toProtoDataServer(cnf, server), nil
	}

	return nil, grpcstatus.Errorf(codes.NotFound, "data server %q not found", req.DataServer)
}

func (admin *adminServer) PatchDataServer(_ context.Context, req *proto.PatchDataServerRequest) (*proto.DataServer, error) {
	if admin.clusterConfigStore == nil {
		return nil, grpcstatus.Error(codes.Unimplemented, "patch data server not supported")
	}

	patch := model.DataServerPatch{}
	if req.PublicAddress != nil {
		patch.PublicAddress = req.PublicAddress
	}
	if req.InternalAddress != nil {
		patch.InternalAddress = req.InternalAddress
	}
	if req.Metadata != nil {
		patch.Metadata = &model.ServerMetadata{Labels: req.Metadata.Labels}
	}

	cnf, err := admin.clusterConfigStore.Update(func(config *model.ClusterConfig) error {
		_, err := config.PatchDataServer(req.DataServer, patch)
		return err
	})
	if err != nil {
		switch {
		case errors.Is(err, model.ErrDataServerNotFound):
			return nil, grpcstatus.Error(codes.NotFound, err.Error())
		case errors.Is(err, model.ErrInvalidDataServerPatch):
			return nil, grpcstatus.Error(codes.InvalidArgument, err.Error())
		default:
			return nil, err
		}
	}

	server, ok := findDataServer(cnf, req.DataServer)
	if !ok {
		return nil, grpcstatus.Errorf(codes.Internal, "patched data server %q not found in updated cluster config", req.DataServer)
	}

	return admin.toProtoDataServer(cnf, server), nil
}

func (admin *adminServer) toProtoDataServer(cnf model.ClusterConfig, server model.Server) *proto.DataServer {
	identifier := server.GetIdentifier()
	nodeMeta := cnf.ServerMetadata[identifier]

	return &proto.DataServer{
		Name:              server.Name,
		PublicAddress:     server.Public,
		InternalAddress:   server.Internal,
		Metadata:          nodeMeta.Labels,
		SupportedFeatures: admin.supportedFeatures(identifier),
	}
}

func (admin *adminServer) supportedFeatures(dataServer string) []proto.Feature {
	if admin.featureSource == nil {
		return nil
	}

	nodeController, ok := admin.featureSource.NodeControllers()[dataServer]
	if !ok || nodeController == nil {
		return nil
	}

	return nodeController.SupportedFeatures()
}

func findDataServer(cnf model.ClusterConfig, dataServer string) (model.Server, bool) {
	for _, server := range cnf.Servers {
		if server.GetIdentifier() == dataServer {
			return server, true
		}
	}

	return model.Server{}, false
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
	clusterConfigStore ClusterConfigStore,
	shardSplitter ShardSplitter,
	featureSource DataServerFeatureProvider,
) *adminServer {
	return &adminServer{
		statusResource:     statusResource,
		clusterConfig:      clusterConfig,
		clusterConfigStore: clusterConfigStore,
		shardSplitter:      shardSplitter,
		featureSource:      featureSource,
	}
}
