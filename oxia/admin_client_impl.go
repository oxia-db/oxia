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

type adminClientImpl struct {
	adminAddr string

	clientPool rpc.ClientPool
}

func (admin *adminClientImpl) ListDataServers() ([]*proto.DataServer, error) {
	client, err := admin.clientPool.GetAminRpc(admin.adminAddr)
	if err != nil {
		return nil, mapAdminError(err)
	}

	if client == nil {
		return nil, wrapAdminError(ErrUnknown, errors.New("unable to connect to admin server"))
	}

	response, err := client.ListDataServers(context.Background(), &proto.ListDataServersRequest{})
	if err != nil {
		return nil, mapAdminError(err)
	}
	return response.DataServers, nil
}

func (admin *adminClientImpl) GetDataServer(dataServer string) (*proto.DataServer, error) {
	client, err := admin.clientPool.GetAminRpc(admin.adminAddr)
	if err != nil {
		return nil, mapAdminError(err)
	}

	if client == nil {
		return nil, wrapAdminError(ErrUnknown, errors.New("unable to connect to admin server"))
	}

	response, err := client.GetDataServer(context.Background(), &proto.GetDataServerRequest{DataServer: dataServer})
	if err != nil {
		return nil, mapAdminError(err)
	}
	return response.DataServer, nil
}

func (admin *adminClientImpl) CreateDataServer(dataServer *proto.DataServer) (*proto.DataServer, error) {
	client, err := admin.clientPool.GetAminRpc(admin.adminAddr)
	if err != nil {
		return nil, mapAdminError(err)
	}

	if client == nil {
		return nil, wrapAdminError(ErrUnknown, errors.New("unable to connect to admin server"))
	}

	response, err := client.CreateDataServer(context.Background(), &proto.CreateDataServerRequest{DataServer: dataServer})
	if err != nil {
		return nil, mapAdminError(err)
	}
	return response.DataServer, nil
}

func (admin *adminClientImpl) Close() error {
	return admin.clientPool.Close()
}

func (admin *adminClientImpl) ListNamespaces() *ListNamespacesResult {
	client, err := admin.clientPool.GetAminRpc(admin.adminAddr)
	if err != nil {
		return &ListNamespacesResult{
			Error: err,
		}
	}
	if client == nil {
		return &ListNamespacesResult{
			Error: errors.New("no coordinator admin client available"),
		}
	}

	namespaces, err := client.ListNamespaces(context.Background(), &proto.ListNamespacesRequest{})
	if err != nil {
		return &ListNamespacesResult{
			Error: err,
		}
	}
	return &ListNamespacesResult{
		Namespaces: namespaces.Namespaces,
	}
}

func (admin *adminClientImpl) SplitShard(namespace string, shardId int64, splitPoint *uint32) *SplitShardResult {
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

	response, err := client.SplitShard(context.Background(), req)
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
