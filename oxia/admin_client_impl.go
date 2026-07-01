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

package oxia

import (
	"context"
	"crypto/tls"
	"errors"

	"github.com/cenkalti/backoff/v4"

	"github.com/oxia-db/oxia/common/auth"
	"github.com/oxia-db/oxia/common/constant"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/common/rpc"
)

var _ AdminClient = (*adminClientImpl)(nil)

const errUnableToConnectToAdminServer = "unable to connect to admin server"

type adminClientImpl struct {
	adminAddr string

	clientPool rpc.ClientPool
}

func (admin *adminClientImpl) ListDataServers(ctx context.Context) ([]*proto.DataServerView, error) {
	response, err := executeAdminRPCWithRedirect(ctx, admin, func(ctx context.Context, client proto.OxiaAdminClient) (*proto.ListDataServersResponse, error) {
		return client.ListDataServers(ctx, &proto.ListDataServersRequest{})
	})
	if err != nil {
		return nil, err
	}
	return response.DataServers, nil
}

func (admin *adminClientImpl) GetDataServer(ctx context.Context, dataServer string) (*proto.DataServerView, error) {
	response, err := executeAdminRPCWithRedirect(ctx, admin, func(ctx context.Context, client proto.OxiaAdminClient) (*proto.GetDataServerResponse, error) {
		return client.GetDataServer(ctx, &proto.GetDataServerRequest{DataServer: dataServer})
	})
	if err != nil {
		return nil, err
	}
	return response.DataServer, nil
}

func (admin *adminClientImpl) CreateDataServer(ctx context.Context, dataServer *proto.DataServer) (*proto.DataServer, error) {
	response, err := executeAdminRPCWithRedirect(ctx, admin, func(ctx context.Context, client proto.OxiaAdminClient) (*proto.CreateDataServerResponse, error) {
		return client.CreateDataServer(ctx, &proto.CreateDataServerRequest{DataServer: dataServer})
	})
	if err != nil {
		return nil, err
	}
	return response.DataServer, nil
}

func (admin *adminClientImpl) PatchDataServer(ctx context.Context, dataServer *proto.DataServer) (*proto.DataServer, error) {
	response, err := executeAdminRPCWithRedirect(ctx, admin, func(ctx context.Context, client proto.OxiaAdminClient) (*proto.PatchDataServerResponse, error) {
		return client.PatchDataServer(ctx, &proto.PatchDataServerRequest{
			DataServer: dataServer,
		})
	})
	if err != nil {
		return nil, err
	}
	return response.DataServer, nil
}

func (admin *adminClientImpl) DeleteDataServer(ctx context.Context, dataServer string) (*proto.DataServer, error) {
	response, err := executeAdminRPCWithRedirect(ctx, admin, func(ctx context.Context, client proto.OxiaAdminClient) (*proto.DeleteDataServerResponse, error) {
		return client.DeleteDataServer(ctx, &proto.DeleteDataServerRequest{
			DataServer: dataServer,
		})
	})
	if err != nil {
		return nil, err
	}
	return response.DataServer, nil
}

func (admin *adminClientImpl) Close() error {
	return admin.clientPool.Close()
}

func (admin *adminClientImpl) CreateNamespace(ctx context.Context, namespace *proto.Namespace) (*proto.Namespace, error) {
	response, err := executeAdminRPCWithRedirect(ctx, admin, func(ctx context.Context, client proto.OxiaAdminClient) (*proto.CreateNamespaceResponse, error) {
		return client.CreateNamespace(ctx, &proto.CreateNamespaceRequest{
			Namespace: namespace,
		})
	})
	if err != nil {
		return nil, err
	}
	return response.Namespace, nil
}

func (admin *adminClientImpl) PatchNamespace(ctx context.Context, namespace *proto.Namespace) (*proto.Namespace, error) {
	response, err := executeAdminRPCWithRedirect(ctx, admin, func(ctx context.Context, client proto.OxiaAdminClient) (*proto.PatchNamespaceResponse, error) {
		return client.PatchNamespace(ctx, &proto.PatchNamespaceRequest{
			Namespace: namespace,
		})
	})
	if err != nil {
		return nil, err
	}
	return response.Namespace, nil
}

func (admin *adminClientImpl) DeleteNamespace(ctx context.Context, namespace string) (*proto.Namespace, error) {
	response, err := executeAdminRPCWithRedirect(ctx, admin, func(ctx context.Context, client proto.OxiaAdminClient) (*proto.DeleteNamespaceResponse, error) {
		return client.DeleteNamespace(ctx, &proto.DeleteNamespaceRequest{
			Namespace: namespace,
		})
	})
	if err != nil {
		return nil, err
	}
	return response.Namespace, nil
}

func (admin *adminClientImpl) ListNamespaces(ctx context.Context) ([]*proto.Namespace, error) {
	response, err := executeAdminRPCWithRedirect(ctx, admin, func(ctx context.Context, client proto.OxiaAdminClient) (*proto.ListNamespacesResponse, error) {
		return client.ListNamespaces(ctx, &proto.ListNamespacesRequest{})
	})
	if err != nil {
		return nil, err
	}
	return response.Namespaces, nil
}

func (admin *adminClientImpl) GetNamespace(ctx context.Context, namespace string) (*proto.Namespace, error) {
	response, err := executeAdminRPCWithRedirect(ctx, admin, func(ctx context.Context, client proto.OxiaAdminClient) (*proto.GetNamespaceResponse, error) {
		return client.GetNamespace(ctx, &proto.GetNamespaceRequest{Namespace: namespace})
	})
	if err != nil {
		return nil, err
	}
	return response.Namespace, nil
}

func (admin *adminClientImpl) SplitShard(ctx context.Context, namespace string, shardId int64, splitPoint *uint32) *SplitShardResult {
	req := &proto.SplitShardRequest{
		Namespace: namespace,
		Shard:     shardId,
	}
	if splitPoint != nil {
		req.SplitPoint = splitPoint
	}

	response, err := executeAdminRPCWithRedirect(ctx, admin, func(ctx context.Context, client proto.OxiaAdminClient) (*proto.SplitShardResponse, error) {
		return client.SplitShard(ctx, req)
	})
	if err != nil {
		return &SplitShardResult{Error: err}
	}
	return &SplitShardResult{
		LeftChildShardId:  response.LeftChildShard,
		RightChildShardId: response.RightChildShard,
	}
}

func NewAdminClient(adminAddr string, tlsConf *tls.Config, authentication auth.Authentication) (AdminClient, error) {
	c := rpc.NewClientPool(tlsConf, authentication)
	return &adminClientImpl{
		adminAddr:  adminAddr,
		clientPool: c,
	}, nil
}

func executeAdminRPCWithRedirect[T any](ctx context.Context, admin *adminClientImpl, operation func(context.Context, proto.OxiaAdminClient) (T, error)) (T, error) {
	var result T
	target := admin.adminAddr

	ctx, cancel := context.WithTimeout(ctx, rpc.DefaultRpcTimeout)
	defer cancel()
	err := backoff.Retry(func() error {
		client, err := admin.clientPool.GetAminRpc(target)
		if err != nil {
			return backoff.Permanent(err)
		}
		if client == nil {
			return backoff.Permanent(errors.New(errUnableToConnectToAdminServer))
		}

		result, err = operation(ctx, client)
		if err == nil {
			return nil
		}

		oxiaErr, metadata := constant.FromGrpcError(err)
		leader, ok := metadata.GetCoordinatorLeaderHint()
		if errors.Is(oxiaErr, constant.ErrNodeIsNotLeader) && ok && leader != target {
			target = leader
			return err
		}

		return backoff.Permanent(err)
	}, backoff.WithContext(&backoff.ZeroBackOff{}, ctx))
	return result, mapAdminError(err)
}
