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
		execute := func(_ context.Context, _ int64, prepare func() (*proto.WriteRequest, error)) (*proto.WriteResponse, error) {
			request, err := prepare()
			assert.NoError(t, err)
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
					OpIndex:           1,
				}},
				DeleteRanges: []*proto.DeleteRangeRequest{{
					StartInclusive: "/callC",
					EndExclusive:   "/d",
					OpIndex:        2,
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

	execute := func(context.Context, int64, func() (*proto.WriteRequest, error)) (*proto.WriteResponse, error) {
		executeCount++
		return nil, constant.ErrShardNotFound
	}

	var reroutedShardId int64
	var reroutedPuts []model.PutCall
	var reroutedDeletes []model.DeleteCall
	var reroutedDeleteRanges []model.DeleteRangeCall

	factory := &writeBatchFactory{
		execute: execute,
		reroute: func(id int64, puts []model.PutCall, deletes []model.DeleteCall, deleteRanges []model.DeleteRangeCall) {
			reroutedShardId = id
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

	assert.Equal(t, shardId, reroutedShardId)
	assert.Equal(t, 2, len(reroutedPuts))
	assert.Equal(t, "key-1", reroutedPuts[0].Key)
	assert.Equal(t, "key-2", reroutedPuts[1].Key)
	assert.Equal(t, 1, len(reroutedDeletes))
	assert.Equal(t, "key-3", reroutedDeletes[0].Key)
	assert.Equal(t, 1, len(reroutedDeleteRanges))
	assert.Equal(t, 1, executeCount)

	// The rerouted calls keep their op index, to be re-added in the same order
	assert.EqualValues(t, 0, reroutedPuts[0].OpIndex)
	assert.EqualValues(t, 1, reroutedPuts[1].OpIndex)
	assert.EqualValues(t, 2, reroutedDeletes[0].OpIndex)
	assert.EqualValues(t, 3, reroutedDeleteRanges[0].OpIndex)
}

func TestWriteBatchOpIndex(t *testing.T) {
	var request *proto.WriteRequest
	factory := &writeBatchFactory{
		execute: func(_ context.Context, _ int64, prepare func() (*proto.WriteRequest, error)) (*proto.WriteResponse, error) {
			request, _ = prepare()
			return nil, errors.New("failure")
		},
		metrics:        metrics.NewMetrics(noop.NewMeterProvider()),
		requestTimeout: 5 * time.Second,
		maxByteSize:    1024,
	}
	batch := factory.newBatch(&shardId)

	putCallback := func(*proto.PutResponse, error) {}
	deleteCallback := func(*proto.DeleteResponse, error) {}
	deleteRangeCallback := func(*proto.DeleteRangeResponse, error) {}

	batch.Add(model.PutCall{Key: "a", Callback: putCallback})
	batch.Add(model.DeleteCall{Key: "a", Callback: deleteCallback})
	batch.Add(model.PutCall{Key: "a", Callback: putCallback})
	batch.Add(model.DeleteRangeCall{MinKeyInclusive: "a", MaxKeyExclusive: "b", Callback: deleteRangeCallback})
	batch.Add(model.DeleteCall{Key: "a", Callback: deleteCallback})

	batch.Complete()

	assert.Len(t, request.Puts, 2)
	assert.EqualValues(t, 0, request.Puts[0].OpIndex)
	assert.EqualValues(t, 2, request.Puts[1].OpIndex)
	assert.Len(t, request.Deletes, 2)
	assert.EqualValues(t, 1, request.Deletes[0].OpIndex)
	assert.EqualValues(t, 4, request.Deletes[1].OpIndex)
	assert.Len(t, request.DeleteRanges, 1)
	assert.EqualValues(t, 3, request.DeleteRanges[0].OpIndex)
}

func TestWriteBatchNoRerouteOnOtherError(t *testing.T) {
	callCount := 0
	execute := func(_ context.Context, _ int64, prepare func() (*proto.WriteRequest, error)) (*proto.WriteResponse, error) {
		callCount++
		if _, err := prepare(); err != nil {
			return nil, err
		}
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
		reroute: func(int64, []model.PutCall, []model.DeleteCall, []model.DeleteRangeCall) {
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

// A call whose caller gave up on it before the request is first handed to gRPC
// is dropped: it fails with an error telling that it was not sent, and the
// other calls are sent without it, keeping their op index.
func TestWriteBatchDropsTheCallsWhoseCallerGaveUp(t *testing.T) {
	gaveUpCtx, gaveUp := context.WithCancel(t.Context())
	gaveUp()
	waitingCtx, stopWaiting := context.WithCancel(t.Context())
	defer stopWaiting()
	gaveUpCall := model.NewCallContext(gaveUpCtx)
	waitingCall := model.NewCallContext(waitingCtx)

	var request *proto.WriteRequest
	factory := &writeBatchFactory{
		execute: func(_ context.Context, _ int64, prepare func() (*proto.WriteRequest, error)) (*proto.WriteResponse, error) {
			var err error
			if request, err = prepare(); err != nil {
				return nil, err
			}
			return &proto.WriteResponse{
				Puts:         []*proto.PutResponse{{Status: proto.Status_OK}, {Status: proto.Status_OK}},
				DeleteRanges: []*proto.DeleteRangeResponse{{Status: proto.Status_OK}},
			}, nil
		},
		metrics:        metrics.NewMetrics(noop.NewMeterProvider()),
		requestTimeout: 5 * time.Second,
		maxByteSize:    1024,
	}
	batch := factory.newBatch(&shardId)

	results := map[string]error{}
	putCallback := func(key string) func(*proto.PutResponse, error) {
		return func(_ *proto.PutResponse, err error) { results[key] = err }
	}
	batch.Add(model.PutCall{Key: "a", Callback: putCallback("a")})
	batch.Add(model.PutCall{Key: "b", CallContext: gaveUpCall, Callback: putCallback("b")})
	batch.Add(model.PutCall{Key: "c", CallContext: waitingCall, Callback: putCallback("c")})
	batch.Add(model.DeleteCall{Key: "d", CallContext: gaveUpCall, Callback: func(_ *proto.DeleteResponse, err error) {
		results["d"] = err
	}})
	batch.Add(model.DeleteRangeCall{MinKeyInclusive: "e", MaxKeyExclusive: "f", CallContext: waitingCall,
		Callback: func(_ *proto.DeleteRangeResponse, err error) { results["e"] = err }})
	batch.Complete()

	assert.Len(t, request.Puts, 2)
	assert.Equal(t, "a", request.Puts[0].Key)
	assert.EqualValues(t, 0, request.Puts[0].OpIndex)
	assert.Equal(t, "c", request.Puts[1].Key)
	assert.EqualValues(t, 2, request.Puts[1].OpIndex)
	assert.Empty(t, request.Deletes)
	assert.Len(t, request.DeleteRanges, 1)
	assert.EqualValues(t, 4, request.DeleteRanges[0].OpIndex)

	assert.Len(t, results, 5)
	for _, key := range []string{"a", "c", "e"} {
		assert.NoError(t, results[key], key)
	}
	for _, key := range []string{"b", "d"} {
		assert.ErrorIs(t, results[key], context.Canceled, key)
		assert.True(t, model.IsNotSent(results[key]), key)
	}

	// The calls that were sent cannot be dropped anymore
	stopWaiting()
	assert.False(t, model.IsNotSent(waitingCall.Cancel()))
}

func TestWriteBatchWithAllTheCallsDropped(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	callContext := model.NewCallContext(ctx)

	var prepareErr error
	factory := &writeBatchFactory{
		execute: func(_ context.Context, _ int64, prepare func() (*proto.WriteRequest, error)) (*proto.WriteResponse, error) {
			_, prepareErr = prepare()
			return nil, prepareErr
		},
		metrics:        metrics.NewMetrics(noop.NewMeterProvider()),
		requestTimeout: 5 * time.Second,
		maxByteSize:    1024,
	}
	batch := factory.newBatch(&shardId)

	var putErr, deleteErr error
	batch.Add(model.PutCall{Key: "a", CallContext: callContext, Callback: func(_ *proto.PutResponse, err error) {
		putErr = err
	}})
	batch.Add(model.DeleteCall{Key: "b", CallContext: callContext, Callback: func(_ *proto.DeleteResponse, err error) {
		deleteErr = err
	}})
	batch.Complete()

	// Nothing is left to send
	assert.Error(t, prepareErr)
	assert.True(t, model.IsNotSent(putErr))
	assert.True(t, model.IsNotSent(deleteErr))
}

// A request that was never handed to gRPC, e.g. because the leader could not
// be reached before the timeout, was not applied: its calls fail with an error
// telling that it was not sent. Once it was sent, the outcome of a failure is
// unknown.
func TestWriteBatchFailureTellsIfTheRequestWasSent(t *testing.T) {
	for _, tt := range []struct {
		name string
		sent bool
	}{
		{"not sent", false},
		{"sent", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			factory := &writeBatchFactory{
				execute: func(_ context.Context, _ int64, prepare func() (*proto.WriteRequest, error)) (*proto.WriteResponse, error) {
					if tt.sent {
						if _, err := prepare(); err != nil {
							return nil, err
						}
					}
					return nil, context.DeadlineExceeded
				},
				metrics:        metrics.NewMetrics(noop.NewMeterProvider()),
				requestTimeout: 5 * time.Second,
				maxByteSize:    1024,
			}
			batch := factory.newBatch(&shardId)

			var putErr error
			batch.Add(model.PutCall{Key: "a", Callback: func(_ *proto.PutResponse, err error) {
				putErr = err
			}})
			batch.Complete()

			assert.ErrorIs(t, putErr, context.DeadlineExceeded)
			assert.Equal(t, !tt.sent, model.IsNotSent(putErr))
		})
	}
}
