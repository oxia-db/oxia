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
	"io"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc/metadata"

	"github.com/oxia-db/oxia/oxia/internal/metrics"
	"github.com/oxia-db/oxia/oxia/internal/model"
	"github.com/oxia-db/oxia/proto"
)

func TestReadBatchAdd(t *testing.T) {
	for _, item := range []struct {
		call         any
		expectPanic  bool
		expectedSize int
	}{
		{model.PutCall{}, true, 0},
		{model.DeleteCall{}, true, 0},
		{model.DeleteRangeCall{}, true, 0},
		{model.GetCall{}, false, 1},
	} {
		factory := &readBatchFactory{
			metrics: metrics.NewMetrics(noop.NewMeterProvider()),
		}
		batch := factory.newBatch(&shardId)

		panicked := add(batch, item.call)

		callType := reflect.TypeOf(item.call)
		assert.Equal(t, item.expectPanic, panicked, callType)
		assert.Equal(t, item.expectedSize, batch.Size(), callType)
	}
}

func TestReadBatchComplete(t *testing.T) {
	getResponseOk := &proto.GetResponse{
		Value:  []byte{0},
		Status: proto.Status_OK,
		Version: &proto.Version{
			VersionId:          1,
			CreatedTimestamp:   2,
			ModifiedTimestamp:  3,
			ModificationsCount: 1,
		},
	}
	for _, item := range []struct {
		response            *proto.ReadResponse
		err                 error
		expectedGetResponse *proto.GetResponse
		expectedGetErr      error
	}{
		{
			&proto.ReadResponse{
				Gets: []*proto.GetResponse{getResponseOk},
			},
			nil,
			getResponseOk,
			nil,
		},
		{
			&proto.ReadResponse{
				Gets: []*proto.GetResponse{{
					Status: proto.Status_KEY_NOT_FOUND,
				}},
			},
			nil,
			&proto.GetResponse{
				Status: proto.Status_KEY_NOT_FOUND,
			},
			nil,
		},
	} {
		execute := func(ctx context.Context, request *proto.ReadRequest) (proto.OxiaClient_ReadClient, error) {
			assert.Equal(t, &proto.ReadRequest{
				Shard: &shardId,
				Gets: []*proto.GetRequest{{
					Key:          "/a",
					IncludeValue: true,
				}},
			}, request)
			return readClient([]*proto.ReadResponse{item.response}), item.err
		}

		factory := &readBatchFactory{
			execute: execute,
			metrics: metrics.NewMetrics(noop.NewMeterProvider()),
		}
		batch := factory.newBatch(&shardId)

		var wg sync.WaitGroup
		wg.Add(3)

		var getResponse *proto.GetResponse
		var getErr error

		getCallback := func(response *proto.GetResponse, err error) {
			getResponse = response
			getErr = err
			wg.Done()
		}

		batch.Add(model.GetCall{
			Key:          "/a",
			Callback:     getCallback,
			IncludeValue: true,
		})
		assert.Equal(t, 1, batch.Size())

		batch.Complete()

		assert.Equal(t, item.expectedGetResponse, getResponse)
		assert.ErrorIs(t, getErr, item.expectedGetErr)
	}
}

type readResult struct {
	response *proto.ReadResponse
	err      error
}

type testOxiaClientReadClient struct {
	ch <-chan readResult
}

func readClient(responses []*proto.ReadResponse) proto.OxiaClient_ReadClient {
	ch := make(chan readResult, len(responses)+1)
	for _, response := range responses {
		ch <- readResult{response: response}
	}
	ch <- readResult{err: io.EOF}
	return &testOxiaClientReadClient{ch: ch}
}

func (t *testOxiaClientReadClient) Recv() (*proto.ReadResponse, error) {
	r := <-t.ch
	return r.response, r.err
}

func (t *testOxiaClientReadClient) Header() (metadata.MD, error) {
	panic("not implemented")
}

func (t *testOxiaClientReadClient) Trailer() metadata.MD {
	panic("not implemented")
}

func (t *testOxiaClientReadClient) CloseSend() error {
	panic("not implemented")
}

func (t *testOxiaClientReadClient) Context() context.Context {
	panic("not implemented")
}

func (t *testOxiaClientReadClient) SendMsg(m any) error {
	panic("not implemented")
}

func (t *testOxiaClientReadClient) RecvMsg(m any) error {
	panic("not implemented")
}
