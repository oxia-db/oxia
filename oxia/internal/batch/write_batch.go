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

package batch

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"time"

	"github.com/oxia-db/oxia/oxia/batch"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia/internal/metrics"
	"github.com/oxia-db/oxia/oxia/internal/model"
)

var ErrRequestTooLarge = errors.New("put request is too large")

// errCallsDropped fails the execution of a batch whose calls were all dropped.
var errCallsDropped = errors.New("all the calls were dropped")

type writeBatchFactory struct {
	namespace      string
	execute        func(context.Context, int64, func() (*proto.WriteRequest, error)) (*proto.WriteResponse, error)
	reroute        WriteRerouter
	metrics        *metrics.Metrics
	requestTimeout time.Duration
	maxByteSize    int
}

func (b writeBatchFactory) newBatch(shardId *int64) batch.Batch {
	return &writeBatch{
		namespace:      b.namespace,
		shardId:        shardId,
		execute:        b.execute,
		reroute:        b.reroute,
		puts:           make([]model.PutCall, 0),
		deletes:        make([]model.DeleteCall, 0),
		deleteRanges:   make([]model.DeleteRangeCall, 0),
		requestTimeout: b.requestTimeout,
		metrics:        b.metrics,
		callback:       b.metrics.WriteCallback(),
		maxByteSize:    b.maxByteSize,
		byteSize:       0,
	}
}

type writeBatch struct {
	namespace      string
	shardId        *int64
	execute        func(context.Context, int64, func() (*proto.WriteRequest, error)) (*proto.WriteResponse, error)
	reroute        WriteRerouter
	puts           []model.PutCall
	deletes        []model.DeleteCall
	deleteRanges   []model.DeleteRangeCall
	metrics        *metrics.Metrics
	requestTimeout time.Duration
	callback       func(time.Time, *proto.WriteRequest, *proto.WriteResponse, error)
	maxByteSize    int
	byteSize       int
}

func (b *writeBatch) CanAdd(call any) bool {
	size := getByteSize(call)
	return b.byteSize+size <= b.maxByteSize
}

func (b *writeBatch) Add(call any) {
	opIndex := uint32(b.Size())
	switch c := call.(type) {
	case model.PutCall:
		c.OpIndex = opIndex
		b.puts = append(b.puts, b.metrics.DecoratePut(c))
	case model.DeleteCall:
		c.OpIndex = opIndex
		b.deletes = append(b.deletes, b.metrics.DecorateDelete(c))
	case model.DeleteRangeCall:
		c.OpIndex = opIndex
		b.deleteRanges = append(b.deleteRanges, b.metrics.DecorateDeleteRange(c))
	default:
		panic("invalid call")
	}
	b.byteSize += getByteSize(call)
}

func (b *writeBatch) Size() int {
	return len(b.puts) + len(b.deletes) + len(b.deleteRanges)
}

func (b *writeBatch) Complete() {
	if b.Size() == 0 {
		return
	}
	executionStart := time.Now()
	request := b.toProto()

	ctx, cancel := context.WithTimeout(context.Background(), b.requestTimeout)
	defer cancel()

	// The calls are dropped or sent when the request is first handed to gRPC
	prepared := false
	response, err := b.execute(ctx, *b.shardId, func() (*proto.WriteRequest, error) {
		if !prepared {
			prepared = true
			if b.dropCancelledCalls() {
				if b.Size() == 0 {
					return nil, errCallsDropped
				}
				request = b.toProto()
			}
		}
		return request, nil
	})
	if b.Size() == 0 {
		// Every call was dropped, and failed already
		return
	}
	if errors.Is(err, constant.ErrShardNotFound) && b.reroute != nil {
		slog.Info("Shard was split/merged, re-routing write batch operations",
			slog.Int64("shard", *b.shardId),
			slog.Int("puts", len(b.puts)),
			slog.Int("deletes", len(b.deletes)),
			slog.Int("delete-ranges", len(b.deleteRanges)),
		)
		b.reroute(*b.shardId, b.puts, b.deletes, b.deleteRanges)
		return
	}

	b.callback(executionStart, request, response, err)

	if err != nil {
		if !prepared {
			// The request was never handed to gRPC: the server did not apply
			// it, and never will
			err = model.NotSent(err)
		}
		b.Fail(err)
	} else {
		b.handle(response)
	}
}

// dropCancelledCalls fails and removes the calls whose caller gave up on them,
// right before the request is first handed to gRPC. It marks the other calls
// as sent, and it returns true if any call was dropped.
func (b *writeBatch) dropCancelledCalls() bool {
	size := b.Size()
	b.puts = slices.DeleteFunc(b.puts, func(put model.PutCall) bool {
		return dropIfCancelled(put.CallContext, func(err error) { put.Callback(nil, err) })
	})
	b.deletes = slices.DeleteFunc(b.deletes, func(_delete model.DeleteCall) bool {
		return dropIfCancelled(_delete.CallContext, func(err error) { _delete.Callback(nil, err) })
	})
	b.deleteRanges = slices.DeleteFunc(b.deleteRanges, func(deleteRange model.DeleteRangeCall) bool {
		return dropIfCancelled(deleteRange.CallContext, func(err error) { deleteRange.Callback(nil, err) })
	})
	return b.Size() < size
}

// dropIfCancelled marks a call as sent, unless its caller gave up on it: then
// it fails the call, and returns true.
func dropIfCancelled(callContext *model.CallContext, fail func(error)) bool {
	if callContext.MarkSent() {
		return false
	}
	fail(callContext.Err())
	return true
}

func (b *writeBatch) Fail(err error) {
	for _, put := range b.puts {
		put.Callback(nil, err)
	}
	for _, _delete := range b.deletes {
		_delete.Callback(nil, err)
	}
	for _, deleteRange := range b.deleteRanges {
		deleteRange.Callback(nil, err)
	}
}

func (b *writeBatch) handle(response *proto.WriteResponse) {
	for i, put := range b.puts {
		put.Callback(response.Puts[i], nil)
	}
	for i, _delete := range b.deletes {
		_delete.Callback(response.Deletes[i], nil)
	}
	for i, deleteRange := range b.deleteRanges {
		deleteRange.Callback(response.DeleteRanges[i], nil)
	}
}

func (b *writeBatch) toProto() *proto.WriteRequest {
	return &proto.WriteRequest{
		Shard:        b.shardId,
		Puts:         model.Convert[model.PutCall, *proto.PutRequest](b.puts, model.PutCall.ToProto),
		Deletes:      model.Convert[model.DeleteCall, *proto.DeleteRequest](b.deletes, model.DeleteCall.ToProto),
		DeleteRanges: model.Convert[model.DeleteRangeCall, *proto.DeleteRangeRequest](b.deleteRanges, model.DeleteRangeCall.ToProto),
	}
}

func getByteSize(call any) int {
	switch c := call.(type) {
	case model.PutCall:
		return len(c.Key) + len(c.Value)
	case model.DeleteCall:
		return len(c.Key)
	case model.DeleteRangeCall:
		return len(c.MinKeyInclusive) + len(c.MaxKeyExclusive)
	default:
		panic("invalid call")
	}
}
