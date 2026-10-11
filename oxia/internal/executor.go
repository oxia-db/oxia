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

package internal

import (
	"context"

	"github.com/oxia-db/oxia/common/proto"
)

type Executor interface {
	ExecuteWrite(ctx context.Context, request *proto.WriteRequest) (*proto.WriteResponse, error)
	// ExecuteWriteAsync sends the request, and returns a function that waits
	// for its response. The requests sent by consecutive calls for a shard
	// reach its leader in the order of the calls. A request that was not sent
	// yet is retried. Once sent, it is sent again only when the end of its
	// stream proves that the server did not process it: the stream was rejected
	// at its setup by a node that is not the leader, or the server marked the
	// requests it left unanswered as unprocessed. Otherwise, e.g. when the
	// connection breaks or the leader steps down mid-stream, the request
	// fails, since the server may have applied it.
	ExecuteWriteAsync(ctx context.Context, request *proto.WriteRequest) (wait func() (*proto.WriteResponse, error))
	ExecuteRead(ctx context.Context, request *proto.ReadRequest) (*proto.ReadResponse, error)
	ExecuteList(ctx context.Context, request *proto.ListRequest, listResponseConsumer func(*proto.ListResponse)) error
	ExecuteRangeScan(ctx context.Context, request *proto.RangeScanRequest, rangeScanResponseConsumer func(*proto.RangeScanResponse)) error
}
