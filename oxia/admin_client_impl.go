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

	"github.com/oxia-db/oxia/common/auth"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/common/rpc"
)

var _ AdminClient = (*adminClientImpl)(nil)

const errUnableToConnectToAdminServer = "unable to connect to admin server"

type adminClientImpl struct {
	adminAddr string

	clientPool rpc.ClientPool
}

func (admin *adminClientImpl) ListDataServers(ctx context.Context) ([]*proto.DataServer, error) {
	client, err := admin.clientPool.GetAminRpc(admin.adminAddr)
	if err != nil {
		return nil, mapAdminError(err)
	}

	if client == nil {
		return nil, wrapAdminError(ErrUnknown, errors.New(errUnableToConnectToAdminServer))
	}

	response, err := client.ListDataServers(ctx, &proto.ListDataServersRequest{})
	if err != nil {
		return nil, mapAdminError(err)
	}
	return response.DataServers, nil
}

func (admin *adminClientImpl) GetDataServer(ctx context.Context, dataServer string) (*proto.DataServer, error) {
	client, err := admin.clientPool.GetAminRpc(admin.adminAddr)
	if err != nil {
		return nil, mapAdminError(err)
	}

	if client == nil {
		return nil, wrapAdminError(ErrUnknown, errors.New(errUnableToConnectToAdminServer))
	}

	response, err := client.GetDataServer(ctx, &proto.GetDataServerRequest{DataServer: dataServer})
	if err != nil {
		return nil, mapAdminError(err)
	}
	return response.DataServer, nil
}

func (admin *adminClientImpl) CreateDataServer(ctx context.Context, dataServer *proto.DataServer) (*proto.DataServer, error) {
	client, err := admin.clientPool.GetAminRpc(admin.adminAddr)
	if err != nil {
		return nil, mapAdminError(err)
	}

	if client == nil {
		return nil, wrapAdminError(ErrUnknown, errors.New(errUnableToConnectToAdminServer))
	}

	response, err := client.CreateDataServer(ctx, &proto.CreateDataServerRequest{DataServer: dataServer})
	if err != nil {
		return nil, mapAdminError(err)
	}
	return response.DataServer, nil
}

func (admin *adminClientImpl) PatchDataServer(ctx context.Context, dataServer *proto.DataServer) (*proto.DataServer, error) {
	client, err := admin.clientPool.GetAminRpc(admin.adminAddr)
	if err != nil {
		return nil, mapAdminError(err)
	}

	if client == nil {
		return nil, wrapAdminError(ErrUnknown, errors.New(errUnableToConnectToAdminServer))
	}

	response, err := client.PatchDataServer(ctx, &proto.PatchDataServerRequest{
		DataServer: dataServer,
	})
	if err != nil {
		return nil, mapAdminError(err)
	}
	return response.DataServer, nil
}

func (admin *adminClientImpl) DeleteDataServer(ctx context.Context, dataServer string) (*proto.DataServer, error) {
	client, err := admin.clientPool.GetAminRpc(admin.adminAddr)
	if err != nil {
		return nil, mapAdminError(err)
	}

	if client == nil {
		return nil, wrapAdminError(ErrUnknown, errors.New(errUnableToConnectToAdminServer))
	}

	response, err := client.DeleteDataServer(ctx, &proto.DeleteDataServerRequest{
		DataServer: dataServer,
	})
	if err != nil {
		return nil, mapAdminError(err)
	}
	return response.DataServer, nil
}

func (admin *adminClientImpl) Close() error {
	return admin.clientPool.Close()
}

func (admin *adminClientImpl) CreateNamespace(ctx context.Context, namespace *proto.Namespace) (*proto.Namespace, error) {
	client, err := admin.clientPool.GetAminRpc(admin.adminAddr)
	if err != nil {
		return nil, mapAdminError(err)
	}

	if client == nil {
		return nil, wrapAdminError(ErrUnknown, errors.New(errUnableToConnectToAdminServer))
	}

	response, err := client.CreateNamespace(ctx, &proto.CreateNamespaceRequest{
		Namespace: namespace,
	})
	if err != nil {
		return nil, mapAdminError(err)
	}
	return response.Namespace, nil
}

func (admin *adminClientImpl) PatchNamespace(ctx context.Context, namespace *proto.Namespace) (*proto.Namespace, error) {
	client, err := admin.clientPool.GetAminRpc(admin.adminAddr)
	if err != nil {
		return nil, mapAdminError(err)
	}

	if client == nil {
		return nil, wrapAdminError(ErrUnknown, errors.New(errUnableToConnectToAdminServer))
	}

	response, err := client.PatchNamespace(ctx, &proto.PatchNamespaceRequest{
		Namespace: namespace,
	})
	if err != nil {
		return nil, mapAdminError(err)
	}
	return response.Namespace, nil
}

func (admin *adminClientImpl) DeleteNamespace(ctx context.Context, namespace string) (*proto.Namespace, error) {
	client, err := admin.clientPool.GetAminRpc(admin.adminAddr)
	if err != nil {
		return nil, mapAdminError(err)
	}

	if client == nil {
		return nil, wrapAdminError(ErrUnknown, errors.New(errUnableToConnectToAdminServer))
	}

	response, err := client.DeleteNamespace(ctx, &proto.DeleteNamespaceRequest{
		Namespace: namespace,
	})
	if err != nil {
		return nil, mapAdminError(err)
	}
	return response.Namespace, nil
}

func (admin *adminClientImpl) ListNamespaces(ctx context.Context) ([]*proto.Namespace, error) {
	client, err := admin.clientPool.GetAminRpc(admin.adminAddr)
	if err != nil {
		return nil, mapAdminError(err)
	}

	if client == nil {
		return nil, wrapAdminError(ErrUnknown, errors.New(errUnableToConnectToAdminServer))
	}

	response, err := client.ListNamespaces(ctx, &proto.ListNamespacesRequest{})
	if err != nil {
		return nil, mapAdminError(err)
	}
	return response.Namespaces, nil
}

func (admin *adminClientImpl) GetNamespace(ctx context.Context, namespace string) (*proto.Namespace, error) {
	client, err := admin.clientPool.GetAminRpc(admin.adminAddr)
	if err != nil {
		return nil, mapAdminError(err)
	}

	if client == nil {
		return nil, wrapAdminError(ErrUnknown, errors.New(errUnableToConnectToAdminServer))
	}

	response, err := client.GetNamespace(ctx, &proto.GetNamespaceRequest{Namespace: namespace})
	if err != nil {
		return nil, mapAdminError(err)
	}
	return response.Namespace, nil
}

func (admin *adminClientImpl) SplitShard(ctx context.Context, namespace string, shardId int64, splitPoint *uint32) *SplitShardResult {
	client, err := admin.clientPool.GetAminRpc(admin.adminAddr)
	if err != nil {
		return &SplitShardResult{Error: err}
	}
	if client == nil {
		return &SplitShardResult{Error: errors.New("no coordinator admin client available")}
	}

	req := &proto.SplitShardRequest{
		Namespace: namespace,
		Shard:     shardId,
	}
	if splitPoint != nil {
		req.SplitPoint = splitPoint
	}

	response, err := client.SplitShard(ctx, req)
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
