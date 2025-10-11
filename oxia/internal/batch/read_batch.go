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
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/cenkalti/backoff/v4"

	time2 "github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/oxia/batch"

	"github.com/oxia-db/oxia/oxia/internal/metrics"
	"github.com/oxia-db/oxia/oxia/internal/model"
	"github.com/oxia-db/oxia/proto"
)

type readBatchFactory struct {
	namespace      string
	execute        func(context.Context, *proto.ReadRequest) (proto.OxiaClient_ReadClient, error)
	metrics        *metrics.Metrics
	requestTimeout time.Duration
}

func (b readBatchFactory) newBatch(shardId *int64) batch.Batch {
	return &readBatch{
		namespace:      b.namespace,
		shardId:        shardId,
		execute:        b.execute,
		gets:           make([]model.GetCall, 0),
		start:          time.Now(),
		metrics:        b.metrics,
		callback:       b.metrics.ReadCallback(),
		requestTimeout: b.requestTimeout,
	}
}

type readBatch struct {
	namespace      string
	shardId        *int64
	execute        func(context.Context, *proto.ReadRequest) (proto.OxiaClient_ReadClient, error)
	gets           []model.GetCall
	start          time.Time
	requestTimeout time.Duration
	metrics        *metrics.Metrics
	callback       func(time.Time, *proto.ReadRequest, *proto.ReadResponse, error)
}

func (*readBatch) CanAdd(_ any) bool {
	return true
}

func (b *readBatch) Add(call any) {
	switch c := call.(type) {
	case model.GetCall:
		b.gets = append(b.gets, b.metrics.DecorateGet(c))
	default:
		panic("invalid call")
	}
}

func (b *readBatch) Size() int {
	return len(b.gets)
}

func (b *readBatch) Complete() {
	executionStart := time.Now()
	request := b.toProto()
	response, err := b.doRequestWithRetries(request)
	b.callback(executionStart, request, response, err)
	if err != nil {
		b.Fail(err)
	} else {
		b.handle(response)
	}
}

func (b *readBatch) doRequestWithRetries(request *proto.ReadRequest) (response *proto.ReadResponse, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), b.requestTimeout)
	defer cancel()

	backOff := time2.NewBackOff(ctx)

	err = backoff.RetryNotify(func() error {
		response, err = b.doRequest(ctx, request)
		if !isRetriable(err) {
			return backoff.Permanent(err)
		}
		return err
	}, backOff, func(err error, duration time.Duration) {
		slog.Debug(
			"Failed to perform request, retrying later",
			slog.Any("error", err),
			slog.String("namespace", b.namespace),
			slog.Int64("shard", *b.shardId),
			slog.Duration("retry-after", duration),
		)
	})

	return response, err
}

func (b *readBatch) doRequest(ctx context.Context, request *proto.ReadRequest) (*proto.ReadResponse, error) {
	stream, err := b.execute(ctx, request)
	if err != nil {
		return nil, err
	}

	response := &proto.ReadResponse{}

	for {
		recv, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return response, nil
		}
		if err != nil {
			return response, err
		}
		response.Gets = append(response.Gets, recv.Gets...)
	}
}

func (b *readBatch) Fail(err error) {
	for _, get := range b.gets {
		get.Callback(nil, err)
	}
}

func (b *readBatch) handle(response *proto.ReadResponse) {
	for i, get := range b.gets {
		get.Callback(response.Gets[i], nil)
	}
}

func (b *readBatch) toProto() *proto.ReadRequest {
	return &proto.ReadRequest{
		Shard: b.shardId,
		Gets:  model.Convert[model.GetCall, *proto.GetRequest](b.gets, model.GetCall.ToProto),
	}
}
