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

	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/common/validation"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	coordruntime "github.com/oxia-db/oxia/oxiad/coordinator/runtime"
)

var _ proto.OxiaAdminServer = (*managementServer)(nil)

type managementServer struct {
	proto.UnimplementedOxiaAdminServer

	metadata coordmetadata.Metadata
	runtime  coordruntime.Runtime
}

func (management *managementServer) ListDataServers(context.Context, *proto.ListDataServersRequest) (*proto.ListDataServersResponse, error) {
	config := management.metadata.GetConfig().UnsafeBorrow()
	statuses := management.runtime.ListDataServerStatus()

	views := make([]*proto.DataServerView, 0, len(config.GetServers()))
	for _, server := range config.GetServers() {
		name := server.GetNameOrDefault()
		dataServer, found := config.GetDataServer(name)
		if !found {
			continue
		}
		status := statuses[name]
		if status == nil {
			status = &proto.DataServerStatus{}
		}
		views = append(views, &proto.DataServerView{
			DataServer:       dataServer,
			DataServerStatus: status,
		})
	}

	return &proto.ListDataServersResponse{DataServers: views}, nil
}

func (management *managementServer) GetDataServer(_ context.Context, req *proto.GetDataServerRequest) (*proto.GetDataServerResponse, error) {
	if req == nil || req.DataServer == "" {
		return nil, grpcstatus.Error(codes.InvalidArgument, "data server must not be empty")
	}

	borrowedDataServer, found := management.metadata.GetDataServer(req.DataServer)
	if !found {
		return nil, grpcstatus.Errorf(codes.NotFound, "data server %q not found", req.DataServer)
	}

	status := &proto.DataServerStatus{}
	if runtimeStatus, found := management.runtime.GetDataServerStatus(req.DataServer); found && runtimeStatus != nil {
		status = runtimeStatus
	}

	return &proto.GetDataServerResponse{
		DataServer: &proto.DataServerView{
			DataServer:       borrowedDataServer.UnsafeBorrow(),
			DataServerStatus: status,
		},
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

func (management *managementServer) DeleteDataServer(_ context.Context, req *proto.DeleteDataServerRequest) (*proto.DeleteDataServerResponse, error) {
	if req == nil || req.DataServer == "" {
		return nil, grpcstatus.Error(codes.InvalidArgument, "data server must not be empty")
	}

	dataServer, err := management.metadata.DeleteDataServer(req.DataServer)
	if err != nil {
		if errors.Is(err, metadatacommon.ErrNotFound) {
			return nil, grpcstatus.Errorf(codes.NotFound, "data server %q not found", req.DataServer)
		}
		if errors.Is(err, metadatacommon.ErrBadVersion) {
			return nil, grpcstatus.Errorf(codes.Aborted, "failed to delete data server %q due to concurrent config update", req.DataServer)
		}
		if errors.Is(err, metadatacommon.ErrFailedPrecondition) {
			return nil, grpcstatus.Errorf(codes.FailedPrecondition, "failed to delete data server %q: %v", req.DataServer, err)
		}
		return nil, grpcstatus.Errorf(codes.Internal, "failed to delete data server %q: %v", req.DataServer, err)
	}

	return &proto.DeleteDataServerResponse{
		DataServer: dataServer,
	}, nil
}

func (management *managementServer) ListNamespaces(context.Context, *proto.ListNamespacesRequest) (*proto.ListNamespacesResponse, error) {
	namespaces := management.metadata.ListNamespace()
	responseNamespaces := make([]*proto.Namespace, 0, len(namespaces))
	for _, namespace := range namespaces {
		responseNamespaces = append(responseNamespaces, namespace.UnsafeBorrow())
	}

	return &proto.ListNamespacesResponse{
		Namespaces: responseNamespaces,
	}, nil
}

func (management *managementServer) CreateNamespace(_ context.Context, req *proto.CreateNamespaceRequest) (*proto.CreateNamespaceResponse, error) {
	if req == nil || req.Namespace == nil {
		return nil, grpcstatus.Error(codes.InvalidArgument, "namespace must not be nil")
	}
	if err := validation.ValidateNamespace(req.Namespace.GetName()); err != nil {
		return nil, grpcstatus.Error(codes.InvalidArgument, err.Error())
	}
	if req.Namespace.GetInitialShardCount() == 0 {
		return nil, grpcstatus.Error(codes.InvalidArgument, "namespace initial shard count must be greater than 0")
	}
	if req.Namespace.GetReplicationFactor() == 0 {
		return nil, grpcstatus.Error(codes.InvalidArgument, "namespace replication factor must be greater than 0")
	}
	keySorting, err := req.Namespace.GetKeySortingType()
	if err != nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, "namespace key sorting is invalid: %v", err)
	}
	if keySorting == proto.KeySortingType_UNKNOWN {
		return nil, grpcstatus.Error(codes.InvalidArgument, `namespace key sorting must be one of "natural" or "hierarchical"`)
	}
	if len(req.Namespace.GetAntiAffinities()) > 0 {
		return nil, grpcstatus.Error(codes.InvalidArgument, "namespace anti-affinities cannot be set on create")
	}

	err = management.metadata.CreateNamespace(req.Namespace)
	if err != nil {
		if errors.Is(err, metadatacommon.ErrAlreadyExists) {
			return nil, grpcstatus.Errorf(codes.AlreadyExists, "namespace %q already exists", req.Namespace.GetName())
		}
		if errors.Is(err, metadatacommon.ErrBadVersion) {
			return nil, grpcstatus.Errorf(codes.Aborted, "failed to create namespace %q due to concurrent config update", req.Namespace.GetName())
		}
		if errors.Is(err, metadatacommon.ErrFailedPrecondition) {
			return nil, grpcstatus.Errorf(codes.FailedPrecondition, "failed to create namespace %q: %v", req.Namespace.GetName(), err)
		}
		return nil, grpcstatus.Errorf(codes.Internal, "failed to create namespace %q: %v", req.Namespace.GetName(), err)
	}

	return &proto.CreateNamespaceResponse{
		Namespace: req.Namespace,
	}, nil
}

func (management *managementServer) PatchNamespace(_ context.Context, req *proto.PatchNamespaceRequest) (*proto.PatchNamespaceResponse, error) {
	if req == nil || req.Namespace == nil {
		return nil, grpcstatus.Error(codes.InvalidArgument, "namespace must not be nil")
	}
	if err := validation.ValidateNamespace(req.Namespace.GetName()); err != nil {
		return nil, grpcstatus.Error(codes.InvalidArgument, err.Error())
	}
	if req.Namespace.GetInitialShardCount() != 0 {
		return nil, grpcstatus.Error(codes.InvalidArgument, "namespace initial shard count cannot be patched")
	}
	if req.Namespace.GetKeySorting() != "" {
		return nil, grpcstatus.Error(codes.InvalidArgument, "namespace key sorting cannot be patched")
	}
	if len(req.Namespace.GetAntiAffinities()) > 0 {
		return nil, grpcstatus.Error(codes.InvalidArgument, "namespace anti-affinities cannot be patched")
	}

	namespace, err := management.metadata.PatchNamespace(req.Namespace)
	if err != nil {
		if errors.Is(err, metadatacommon.ErrNotFound) {
			return nil, grpcstatus.Errorf(codes.NotFound, "namespace %q not found", req.Namespace.GetName())
		}
		if errors.Is(err, metadatacommon.ErrBadVersion) {
			return nil, grpcstatus.Errorf(codes.Aborted, "failed to patch namespace %q due to concurrent config update", req.Namespace.GetName())
		}
		if errors.Is(err, metadatacommon.ErrFailedPrecondition) {
			return nil, grpcstatus.Errorf(codes.FailedPrecondition, "failed to patch namespace %q: %v", req.Namespace.GetName(), err)
		}
		return nil, grpcstatus.Errorf(codes.Internal, "failed to patch namespace %q: %v", req.Namespace.GetName(), err)
	}

	return &proto.PatchNamespaceResponse{
		Namespace: namespace,
	}, nil
}

func (management *managementServer) DeleteNamespace(_ context.Context, req *proto.DeleteNamespaceRequest) (*proto.DeleteNamespaceResponse, error) {
	if req == nil {
		return nil, grpcstatus.Error(codes.InvalidArgument, "namespace must not be nil")
	}
	if err := validation.ValidateNamespace(req.Namespace); err != nil {
		return nil, grpcstatus.Error(codes.InvalidArgument, err.Error())
	}

	namespace, err := management.metadata.DeleteNamespace(req.Namespace)
	if err != nil {
		if errors.Is(err, metadatacommon.ErrNotFound) {
			return nil, grpcstatus.Errorf(codes.NotFound, "namespace %q not found", req.Namespace)
		}
		if errors.Is(err, metadatacommon.ErrBadVersion) {
			return nil, grpcstatus.Errorf(codes.Aborted, "failed to delete namespace %q due to concurrent config update", req.Namespace)
		}
		return nil, grpcstatus.Errorf(codes.Internal, "failed to delete namespace %q: %v", req.Namespace, err)
	}

	return &proto.DeleteNamespaceResponse{
		Namespace: namespace,
	}, nil
}

func (management *managementServer) GetNamespace(_ context.Context, req *proto.GetNamespaceRequest) (*proto.GetNamespaceResponse, error) {
	if req == nil || req.Namespace == "" {
		return nil, grpcstatus.Error(codes.InvalidArgument, "namespace must not be empty")
	}

	namespace, found := management.metadata.GetNamespace(req.Namespace)
	if !found {
		return nil, grpcstatus.Errorf(codes.NotFound, "namespace %q not found", req.Namespace)
	}

	return &proto.GetNamespaceResponse{
		Namespace: namespace.UnsafeBorrow(),
	}, nil
}

func (management *managementServer) SplitShard(_ context.Context, req *proto.SplitShardRequest) (*proto.SplitShardResponse, error) {
	slog.Info("Received SplitShard request",
		slog.String("namespace", req.Namespace),
		slog.Int64("shard", req.Shard),
		slog.Any("split-point", req.SplitPoint),
	)

	left, right, err := management.runtime.InitiateSplit(req.Namespace, req.Shard, req.SplitPoint)
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
	runtime coordruntime.Runtime,
) *managementServer {
	if runtime == nil {
		panic("runtime must not be nil")
	}
	return &managementServer{
		metadata: metadata,
		runtime:  runtime,
	}
}
