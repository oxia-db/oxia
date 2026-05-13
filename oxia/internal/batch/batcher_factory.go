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

package batch

import (
	"context"
	"time"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	batch2 "github.com/oxia-db/oxia/oxia/batch"
	"github.com/oxia-db/oxia/oxia/internal"
	"github.com/oxia-db/oxia/oxia/internal/metrics"
	"github.com/oxia-db/oxia/oxia/internal/model"
)

type WriteRerouter func([]model.PutCall, []model.DeleteCall, []model.DeleteRangeCall)

type ReadRerouter func([]model.GetCall)

type BatcherFactory struct {
	batch2.BatcherFactory
	Namespace      string
	Executor       internal.Executor
	RequestTimeout time.Duration
	Metrics        *metrics.Metrics
	WriteRerouter  WriteRerouter
	ReadRerouter   ReadRerouter
}

func NewBatcherFactory(
	executor internal.Executor,
	namespace string,
	batchLinger time.Duration,
	maxRequestsPerBatch int,
	metric *metrics.Metrics,
	requestTimeout time.Duration) *BatcherFactory {
	return &BatcherFactory{
		Namespace: namespace,
		Executor:  executor,
		BatcherFactory: batch2.BatcherFactory{
			Linger:              batchLinger,
			MaxRequestsPerBatch: maxRequestsPerBatch,
		},
		Metrics:        metric,
		RequestTimeout: requestTimeout,
	}
}

func (b *BatcherFactory) NewWriteBatcher(ctx context.Context, shardId *int64, maxWriteBatchSize int) batch2.Batcher {
	return b.newBatcher(ctx, shardId, "write", writeBatchFactory{
		execute: func(ctx context.Context, request *proto.WriteRequest, hint constant.ErrorMetadata) (*proto.WriteResponse, error) {
			return b.Executor.ExecuteWrite(ctx, request, hint)
		},
		reroute:        b.WriteRerouter,
		metrics:        b.Metrics,
		requestTimeout: b.RequestTimeout,
		maxByteSize:    maxWriteBatchSize,
	}.newBatch)
}

func (b *BatcherFactory) NewReadBatcher(ctx context.Context, shardId *int64) batch2.Batcher {
	return b.newBatcher(ctx, shardId, "read", readBatchFactory{
		execute: func(ctx context.Context, request *proto.ReadRequest, hint constant.ErrorMetadata) (proto.OxiaClient_ReadClient, error) {
			return b.Executor.ExecuteRead(ctx, request, hint)
		},
		reroute:        b.ReadRerouter,
		metrics:        b.Metrics,
		requestTimeout: b.RequestTimeout,
	}.newBatch)
}

func (b *BatcherFactory) newBatcher(ctx context.Context, shardId *int64, batcherType string, batchFactory func(shardId *int64) batch2.Batch) batch2.Batcher {
	return b.NewBatcher(ctx, *shardId, batcherType, func() batch2.Batch {
		return batchFactory(shardId)
	})
}
