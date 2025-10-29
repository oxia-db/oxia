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

	"github.com/emirpasic/gods/v2/sets/hashset"

	"github.com/oxia-db/oxia/coordinator/model"
	"github.com/oxia-db/oxia/coordinator/resources"
	"github.com/oxia-db/oxia/proto"
)

var _ proto.OxiaAdminServer = (*adminServer)(nil)

type adminServer struct {
	proto.UnimplementedOxiaAdminServer

	statusResource resources.StatusResource
	clusterConfig  func() (model.ClusterConfig, error)
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
	nodes := make([]*proto.Node, len(cnfNodes))
	for i, node := range cnfNodes {
		nodes[i] = &proto.Node{
			Name:            node.Name,
			PublicAddress:   node.Public,
			InternalAddress: node.Internal,
		}
	}
	return &proto.ListNodesResponse{Nodes: nodes}, nil
}

func newAdminServer(statusResource resources.StatusResource, clusterConfig func() (model.ClusterConfig, error)) *adminServer {
	return &adminServer{
		statusResource: statusResource,
		clusterConfig:  clusterConfig,
	}
}
