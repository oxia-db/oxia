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

package commons

import (
	"context"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/mock"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia"
)

var _ oxia.AdminClient = (*MockAdminClient)(nil)

type MockAdminClient struct {
	mock.Mock
}

func NewMockAdminClient() *MockAdminClient {
	return &MockAdminClient{}
}

func (m *MockAdminClient) Close() error {
	args := m.MethodCalled("Close")
	return args.Error(0)
}

func (m *MockAdminClient) ListNamespaces(context.Context) ([]*proto.NamespaceView, error) {
	args := m.MethodCalled("ListNamespaces")
	if v, ok := args.Get(0).([]*proto.NamespaceView); ok {
		return v, args.Error(1)
	}
	return nil, errors.New("no namespaces available")
}

func (m *MockAdminClient) ListDataServers(context.Context) ([]*proto.DataServerView, error) {
	args := m.MethodCalled("ListDataServers")
	if v, ok := args.Get(0).([]*proto.DataServerView); ok {
		return v, args.Error(1)
	}
	return nil, errors.New("no data servers available")
}

func (m *MockAdminClient) GetDataServer(_ context.Context, dataServer string) (*proto.DataServerView, error) {
	args := m.MethodCalled("GetDataServer", dataServer)
	if v, ok := args.Get(0).(*proto.DataServerView); ok {
		return v, args.Error(1)
	}
	return nil, errors.New("data server not found")
}

func (m *MockAdminClient) CreateDataServer(_ context.Context, dataServer *proto.DataServer) (*proto.DataServer, error) {
	args := m.MethodCalled("CreateDataServer", dataServer)
	if v, ok := args.Get(0).(*proto.DataServer); ok {
		return v, args.Error(1)
	}
	return nil, errors.New("failed to create data server")
}

func (m *MockAdminClient) PatchDataServer(_ context.Context, dataServer *proto.DataServer) (*proto.DataServer, error) {
	args := m.MethodCalled("PatchDataServer", dataServer)
	if v, ok := args.Get(0).(*proto.DataServer); ok {
		return v, args.Error(1)
	}
	return nil, errors.New("failed to patch data server")
}

func (m *MockAdminClient) DeleteDataServer(_ context.Context, dataServer string) (*proto.DataServer, error) {
	args := m.MethodCalled("DeleteDataServer", dataServer)
	if v, ok := args.Get(0).(*proto.DataServer); ok {
		return v, args.Error(1)
	}
	return nil, errors.New("failed to delete data server")
}

func (m *MockAdminClient) CreateNamespace(_ context.Context, namespace *proto.Namespace) (*proto.Namespace, error) {
	args := m.MethodCalled("CreateNamespace", namespace)
	if v, ok := args.Get(0).(*proto.Namespace); ok {
		return v, args.Error(1)
	}
	return nil, errors.New("failed to create namespace")
}

func (m *MockAdminClient) PatchNamespace(_ context.Context, namespace *proto.Namespace) (*proto.Namespace, error) {
	args := m.MethodCalled("PatchNamespace", namespace)
	if v, ok := args.Get(0).(*proto.Namespace); ok {
		return v, args.Error(1)
	}
	return nil, errors.New("failed to patch namespace")
}

func (m *MockAdminClient) DeleteNamespace(_ context.Context, namespace string) (*proto.Namespace, error) {
	args := m.MethodCalled("DeleteNamespace", namespace)
	if v, ok := args.Get(0).(*proto.Namespace); ok {
		return v, args.Error(1)
	}
	return nil, errors.New("failed to delete namespace")
}

func (m *MockAdminClient) GetNamespace(_ context.Context, namespace string) (*proto.NamespaceView, error) {
	args := m.MethodCalled("GetNamespace", namespace)
	if v, ok := args.Get(0).(*proto.NamespaceView); ok {
		return v, args.Error(1)
	}
	return nil, errors.New("namespace not found")
}

func (m *MockAdminClient) SplitShard(_ context.Context, namespace string, shardId int64, splitPoint *uint32) *oxia.SplitShardResult {
	args := m.MethodCalled("SplitShard", namespace, shardId, splitPoint)
	if v, ok := args.Get(0).(*oxia.SplitShardResult); ok {
		return v
	}
	return &oxia.SplitShardResult{
		Error: errors.New("split shard failed"),
	}
}
