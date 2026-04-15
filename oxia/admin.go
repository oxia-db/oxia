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
	"io"

	"github.com/oxia-db/oxia/common/proto"
)

type AdminClient interface {
	io.Closer

	ListDataServers() ([]*proto.DataServer, error)
	GetDataServer(dataServer string) (*proto.DataServerInfo, error)
	CreateDataServer(dataServerInfo *proto.DataServerInfo) (*proto.DataServerInfo, error)

	ListNamespaces() *ListNamespacesResult

	// Deprecated: Use ListDataServers instead.
	ListNodes() *ListNodesResult

	SplitShard(namespace string, shardId int64, splitPoint *uint32) *SplitShardResult
}

type ListNamespacesResult struct {
	Namespaces []string
	Error      error
}

type Node struct {
	Name            *string
	PublicAddress   string
	InternalAddress string
	Metadata        map[string]string
}

type ListNodesResult struct {
	Nodes []*Node
	Error error
}

type SplitShardResult struct {
	LeftChildShardId  int64
	RightChildShardId int64
	Error             error
}
