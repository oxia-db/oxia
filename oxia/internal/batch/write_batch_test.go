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
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxia/internal/metrics"
	"github.com/oxia-db/oxia/oxia/internal/model"
)

func TestWriteBatchAdd(t *testing.T) {
	for _, item := range []struct {
		call         any
		expectPanic  bool
		expectedSize int
	}{
		{model.PutCall{}, false, 1},
		{model.DeleteCall{}, false, 1},
		{model.DeleteRangeCall{}, false, 1},
		{model.GetCall{}, true, 0},
	} {
		factory := &writeBatchFactory{
			metrics:     metrics.NewMetrics(noop.NewMeterProvider()),
			maxByteSize: 1024,
		}
		batch := factory.newBatch(&shardId)

		panicked := add(batch, item.call)

		callType := reflect.TypeOf(item.call)
		assert.Equal(t, item.expectPanic, panicked, callType)
		assert.Equal(t, item.expectedSize, batch.Size(), callType)
	}
}

func TestWriteBatchComplete(t *testing.T) {
	errFailure := errors.New("failure")
	putResponseOk := &proto.PutResponse{
		Status: proto.Status_OK,
		Version: &proto.Version{
			VersionId:          1,
			CreatedTimestamp:   2,
			ModifiedTimestamp:  3,
			ModificationsCount: 1,
		},
	}
	for _, item := range []struct {
		response                    *proto.WriteResponse
		err                         error
		expectedPutResponse         *proto.PutResponse
		expectedPutErr              error
		expectedDeleteResponse      *proto.DeleteResponse
		expectedDeleteErr           error
		expectedDeleteRangeResponse *proto.DeleteRangeResponse
		expectedDeleteRangeErr      error
	}{
		{
			&proto.WriteResponse{
				Puts: []*proto.PutResponse{putResponseOk},
				Deletes: []*proto.DeleteResponse{{
					Status: proto.Status_OK,
				}},
				DeleteRanges: []*proto.DeleteRangeResponse{{
					Status: proto.Status_OK,
				}},
			},
			nil,
			putResponseOk,
			nil,
			&proto.DeleteResponse{
				Status: proto.Status_OK,
			},
			nil,
			&proto.DeleteRangeResponse{
				Status: proto.Status_OK,
			},
			nil,
		},
		{
			&proto.WriteResponse{
				Puts: []*proto.PutResponse{{
					Status: proto.Status_UNEXPECTED_VERSION_ID,
				}},
				Deletes: []*proto.DeleteResponse{{
					Status: proto.Status_KEY_NOT_FOUND,
				}},
				DeleteRanges: []*proto.DeleteRangeResponse{{
					Status: proto.Status_OK,
				}},
			},
			nil,
			&proto.PutResponse{
				Status: proto.Status_UNEXPECTED_VERSION_ID,
			},
			nil,
			&proto.DeleteResponse{
				Status: proto.Status_KEY_NOT_FOUND,
			},
			nil,
			&proto.DeleteRangeResponse{
				Status: proto.Status_OK,
			},
			nil,
		},
		{
			nil,
			errFailure,
			nil,
			errFailure,
			nil,
			errFailure,
			nil,
			errFailure,
		},
	} {
		execute := func(ctx context.Context, request *proto.WriteRequest) (*proto.WriteResponse, error) {
			assert.Equal(t, &proto.WriteRequest{
				Shard: &shardId,
				Puts: []*proto.PutRequest{{
					Key:               "/a",
					Value:             []byte{0},
					ExpectedVersionId: &one,
				}},
				Deletes: []*proto.DeleteRequest{{
					Key:               "/b",
					ExpectedVersionId: &two,
				}},
				DeleteRanges: []*proto.DeleteRangeRequest{{
					StartInclusive: "/callC",
					EndExclusive:   "/d",
				}},
			}, request)
			return item.response, item.err
		}

		factory := &writeBatchFactory{
			execute:     execute,
			metrics:     metrics.NewMetrics(noop.NewMeterProvider()),
			maxByteSize: 1024,
		}
		batch := factory.newBatch(&shardId)

		var wg sync.WaitGroup
		wg.Add(3)

		var putResponse *proto.PutResponse
		var putErr error
		var deleteResponse *proto.DeleteResponse
		var deleteErr error
		var deleteRangeResponse *proto.DeleteRangeResponse
		var deleteRangeErr error

		putCallback := func(response *proto.PutResponse, err error) {
			putResponse = response
			putErr = err
			wg.Done()
		}
		deleteCallback := func(response *proto.DeleteResponse, err error) {
			deleteResponse = response
			deleteErr = err
			wg.Done()
		}
		deleteRangeCallback := func(response *proto.DeleteRangeResponse, err error) {
			deleteRangeResponse = response
			deleteRangeErr = err
			wg.Done()
		}

		batch.Add(model.PutCall{
			Key:               "/a",
			Value:             []byte{0},
			ExpectedVersionId: &one,
			Callback:          putCallback,
		})
		batch.Add(model.DeleteCall{
			Key:               "/b",
			ExpectedVersionId: &two,
			Callback:          deleteCallback,
		})
		batch.Add(model.DeleteRangeCall{
			MinKeyInclusive: "/callC",
			MaxKeyExclusive: "/d",
			Callback:        deleteRangeCallback,
		})
		assert.Equal(t, 3, batch.Size())

		batch.Complete()

		wg.Wait()

		assert.Equal(t, item.expectedPutResponse, putResponse)
		assert.ErrorIs(t, putErr, item.expectedPutErr)

		assert.Equal(t, item.expectedDeleteResponse, deleteResponse)
		assert.ErrorIs(t, deleteErr, item.expectedDeleteErr)

		assert.Equal(t, item.expectedDeleteRangeResponse, deleteRangeResponse)
		assert.ErrorIs(t, deleteRangeErr, item.expectedDeleteRangeErr)
	}
}

func TestWriteBatchRerouteOnShardDeleted(t *testing.T) {
	executeCount := 0

	execute := func(_ context.Context, _ *proto.WriteRequest) (*proto.WriteResponse, error) {
		executeCount++
		return nil, constant.ErrShardNotFound
	}

	var reroutedPuts []model.PutCall
	var reroutedDeletes []model.DeleteCall
	var reroutedDeleteRanges []model.DeleteRangeCall

	factory := &writeBatchFactory{
		execute: execute,
		reroute: func(puts []model.PutCall, deletes []model.DeleteCall, deleteRanges []model.DeleteRangeCall) {
			reroutedPuts = puts
			reroutedDeletes = deletes
			reroutedDeleteRanges = deleteRanges
		},
		metrics:        metrics.NewMetrics(noop.NewMeterProvider()),
		requestTimeout: 5 * time.Second,
		maxByteSize:    1024,
	}
	batch := factory.newBatch(&shardId)

	putCallback := func(*proto.PutResponse, error) {}
	deleteCallback := func(*proto.DeleteResponse, error) {}
	deleteRangeCallback := func(*proto.DeleteRangeResponse, error) {}

	batch.Add(model.PutCall{Key: "key-1", Value: []byte("v1"), Callback: putCallback})
	batch.Add(model.PutCall{Key: "key-2", Value: []byte("v2"), Callback: putCallback})
	batch.Add(model.DeleteCall{Key: "key-3", Callback: deleteCallback})
	batch.Add(model.DeleteRangeCall{MinKeyInclusive: "a", MaxKeyExclusive: "z", Callback: deleteRangeCallback})

	batch.Complete()

	assert.Equal(t, 2, len(reroutedPuts))
	assert.Equal(t, "key-1", reroutedPuts[0].Key)
	assert.Equal(t, "key-2", reroutedPuts[1].Key)
	assert.Equal(t, 1, len(reroutedDeletes))
	assert.Equal(t, "key-3", reroutedDeletes[0].Key)
	assert.Equal(t, 1, len(reroutedDeleteRanges))
	assert.Equal(t, 1, executeCount)
}

func TestWriteBatchNoRerouteOnOtherError(t *testing.T) {
	callCount := 0
	execute := func(_ context.Context, _ *proto.WriteRequest) (*proto.WriteResponse, error) {
		callCount++
		if callCount == 1 {
			return nil, constant.ErrInvalidStatus
		}
		return &proto.WriteResponse{
			Puts: []*proto.PutResponse{{Status: proto.Status_OK, Version: &proto.Version{VersionId: 1}}},
		}, nil
	}

	rerouted := false
	factory := &writeBatchFactory{
		execute: execute,
		reroute: func([]model.PutCall, []model.DeleteCall, []model.DeleteRangeCall) {
			rerouted = true
		},
		metrics:        metrics.NewMetrics(noop.NewMeterProvider()),
		requestTimeout: 5 * time.Second,
		maxByteSize:    1024,
	}
	batch := factory.newBatch(&shardId)

	var wg sync.WaitGroup
	wg.Add(1)
	batch.Add(model.PutCall{
		Key:   "key-1",
		Value: []byte("v1"),
		Callback: func(resp *proto.PutResponse, err error) {
			assert.Nil(t, resp)
			assert.ErrorIs(t, err, constant.ErrInvalidStatus)
			wg.Done()
		},
	})

	batch.Complete()
	wg.Wait()

	assert.False(t, rerouted)
	assert.Equal(t, 1, callCount)
}

func TestWriteBatchCanAdd(t *testing.T) {
	for _, item := range []struct {
		name         string
		dataSize     int
		expectCanAdd bool
	}{
		{"larger than maxBatchSize", 128, false},
		{"add to next", 50, false},
		{"add to current", 1, true},
	} {
		t.Run(item.name, func(t *testing.T) {
			factory := &writeBatchFactory{
				metrics:     metrics.NewMetrics(noop.NewMeterProvider()),
				maxByteSize: 100,
			}
			batch := factory.newBatch(&shardId)
			batch.Add(model.PutCall{
				Key:   "a",
				Value: make([]byte, 50),
			})

			canAdd := batch.CanAdd(model.PutCall{
				Key:   "b",
				Value: make([]byte, item.dataSize),
			})

			assert.Equal(t, item.expectCanAdd, canAdd)
		})
	}
}
