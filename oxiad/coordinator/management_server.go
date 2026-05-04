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
	"errors"
	"log/slog"

	"github.com/emirpasic/gods/v2/sets/hashset"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/proto"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/controller"
)

var _ proto.OxiaAdminServer = (*managementServer)(nil)

type managementServer struct {
	proto.UnimplementedOxiaAdminServer

	metadata      coordmetadata.Metadata
	shardSplitter controller.ShardSplitter
}

func (management *managementServer) ListDataServers(context.Context, *proto.ListDataServersRequest) (*proto.ListDataServersResponse, error) {
	cnf := management.metadata.GetConfig().UnsafeBorrow()

	dataServers := make([]*proto.DataServer, 0, len(cnf.GetServers()))
	for _, server := range cnf.GetServers() {
		id := server.GetNameOrDefault()
		identity := server
		if server.GetName() == "" {
			name := id
			identity = &proto.DataServerIdentity{
				Name:     &name,
				Public:   server.GetPublic(),
				Internal: server.GetInternal(),
			}
		}
		metadata := &proto.DataServerMetadata{}
		if value, found := cnf.GetServerMetadata()[id]; found {
			metadata = value
		}
		dataServers = append(dataServers, &proto.DataServer{
			Identity: identity,
			Metadata: metadata,
		})
	}

	return &proto.ListDataServersResponse{DataServers: dataServers}, nil
}

func (management *managementServer) GetDataServer(_ context.Context, req *proto.GetDataServerRequest) (*proto.GetDataServerResponse, error) {
	if req == nil || req.DataServer == "" {
		return nil, grpcstatus.Error(codes.InvalidArgument, "data server must not be empty")
	}

	borrowedDataServer, found := management.metadata.GetDataServer(req.DataServer)
	if !found {
		return nil, grpcstatus.Errorf(codes.NotFound, "data server %q not found", req.DataServer)
	}

	return &proto.GetDataServerResponse{
		DataServer: borrowedDataServer.UnsafeBorrow(),
	}, nil
}

func (management *managementServer) CreateDataServer(_ context.Context, req *proto.CreateDataServerRequest) (*proto.CreateDataServerResponse, error) {
	if req == nil || req.DataServer == nil {
		return nil, grpcstatus.Error(codes.InvalidArgument, "data server must not be nil")
	}
	if req.DataServer.Identity == nil {
		return nil, grpcstatus.Error(codes.InvalidArgument, "data server identity must not be nil")
	}
	if req.DataServer.Identity.GetName() == "" {
		return nil, grpcstatus.Error(codes.InvalidArgument, "data server identity name must not be empty")
	}
	if req.DataServer.Identity.GetPublic() == "" {
		return nil, grpcstatus.Error(codes.InvalidArgument, "data server public address must not be empty")
	}
	if req.DataServer.Identity.GetInternal() == "" {
		return nil, grpcstatus.Error(codes.InvalidArgument, "data server internal address must not be empty")
	}

	err := management.metadata.CreateDataServer(req.DataServer)
	if err != nil {
		if errors.Is(err, metadatacommon.ErrAlreadyExists) {
			return nil, grpcstatus.Errorf(codes.AlreadyExists, "data server %q already exists", req.DataServer.GetNameOrDefault())
		}
		if errors.Is(err, metadatacommon.ErrBadVersion) {
			return nil, grpcstatus.Errorf(codes.Aborted, "failed to create data server %q due to concurrent config update", req.DataServer.GetNameOrDefault())
		}
		return nil, grpcstatus.Errorf(codes.Internal, "failed to create data server %q: %v", req.DataServer.GetNameOrDefault(), err)
	}

	return &proto.CreateDataServerResponse{
		DataServer: req.DataServer,
	}, nil
}

func (management *managementServer) PatchDataServer(_ context.Context, req *proto.PatchDataServerRequest) (*proto.PatchDataServerResponse, error) {
	if req == nil || req.DataServer == nil {
		return nil, grpcstatus.Error(codes.InvalidArgument, "data server must not be nil")
	}
	if req.DataServer.Identity == nil {
		return nil, grpcstatus.Error(codes.InvalidArgument, "data server identity must not be nil")
	}
	if req.DataServer.Identity.GetName() == "" {
		return nil, grpcstatus.Error(codes.InvalidArgument, "data server identity name must not be empty")
	}
	if req.DataServer.Identity.GetPublic() == "" {
		return nil, grpcstatus.Error(codes.InvalidArgument, "data server public address must not be empty")
	}
	if req.DataServer.Identity.GetInternal() == "" {
		return nil, grpcstatus.Error(codes.InvalidArgument, "data server internal address must not be empty")
	}

	dataServer, err := management.metadata.PatchDataServer(req.DataServer)
	if err != nil {
		if errors.Is(err, metadatacommon.ErrNotFound) {
			return nil, grpcstatus.Errorf(codes.NotFound, "data server %q not found", req.DataServer.GetNameOrDefault())
		}
		if errors.Is(err, metadatacommon.ErrBadVersion) {
			return nil, grpcstatus.Errorf(codes.Aborted, "failed to patch data server %q due to concurrent config update", req.DataServer.GetNameOrDefault())
		}
		return nil, grpcstatus.Errorf(codes.Internal, "failed to patch data server %q: %v", req.DataServer.GetNameOrDefault(), err)
	}

	return &proto.PatchDataServerResponse{
		DataServer: dataServer,
	}, nil
}

func (management *managementServer) ListNamespaces(context.Context, *proto.ListNamespacesRequest) (*proto.ListNamespacesResponse, error) {
	cnf := management.metadata.GetConfig().UnsafeBorrow()

	namespaceNames := hashset.New[string]()
	for _, nsConfig := range cnf.GetNamespaces() {
		namespaceNames.Add(nsConfig.GetName())
	}
	return &proto.ListNamespacesResponse{
		Namespaces: namespaceNames.Values(),
	}, nil
}

func (management *managementServer) SplitShard(_ context.Context, req *proto.SplitShardRequest) (*proto.SplitShardResponse, error) {
	if management.shardSplitter == nil {
		return nil, errors.New("split shard not supported")
	}

	slog.Info("Received SplitShard request",
		slog.String("namespace", req.Namespace),
		slog.Int64("shard", req.Shard),
		slog.Any("split-point", req.SplitPoint),
	)

	left, right, err := management.shardSplitter.InitiateSplit(req.Namespace, req.Shard, req.SplitPoint)
	if err != nil {
		return nil, err
	}

	return &proto.SplitShardResponse{
		LeftChildShard:  left,
		RightChildShard: right,
	}, nil
}

func newManagementServer(
	metadata coordmetadata.Metadata,
	shardSplitter controller.ShardSplitter,
) *managementServer {
	return &managementServer{
		metadata:      metadata,
		shardSplitter: shardSplitter,
	}
}
