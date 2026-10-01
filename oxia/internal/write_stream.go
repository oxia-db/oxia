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
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/common/proto"
)

type streamWrapper struct {
	sync.Mutex

	// target is the shard leader the stream goes to
	target          string
	stream          proto.OxiaClient_WriteStreamClient
	cancel          context.CancelFunc
	pendingRequests []concurrent.Future[*proto.WriteResponse]
	failed          atomic.Bool
}

// newStreamWrapper wraps a write stream to target. cancel cancels the context of
// the stream, and it is invoked once the stream ends.
func newStreamWrapper(shard int64, target string, stream proto.OxiaClient_WriteStreamClient,
	cancel context.CancelFunc) *streamWrapper {
	sw := &streamWrapper{
		target:          target,
		stream:          stream,
		cancel:          cancel,
		pendingRequests: nil,
	}

	go process.DoWithLabels(stream.Context(), map[string]string{
		"oxia":  "write-stream-handle-response",
		"shard": fmt.Sprintf("%d", shard),
	}, sw.handleResponses)
	return sw
}

// Send hands the request returned by prepare to gRPC, and waits for its
// response. prepare is invoked right before, unless ctx is done: a request is
// never sent after its deadline.
func (sw *streamWrapper) Send(ctx context.Context, prepare func() (*proto.WriteRequest, error)) (*proto.WriteResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	req, err := prepare()
	if err != nil {
		return nil, err
	}

	f := concurrent.NewFuture[*proto.WriteResponse]()

	sw.Lock()
	sw.pendingRequests = append(sw.pendingRequests, f)
	if err := sw.stream.Send(req); err != nil {
		sw.failed.Store(true)
		sw.Unlock()
		return nil, err
	}

	sw.Unlock()

	return f.Wait(ctx)
}

func (sw *streamWrapper) handleResponses() {
	defer sw.cancel()

	for {
		response, err := sw.stream.Recv()
		sw.Lock()

		if err != nil {
			// Before failing the requests, so that their retries use a new stream
			sw.failed.Store(true)
			for _, f := range sw.pendingRequests {
				f.Fail(err)
			}
			sw.pendingRequests = nil
			sw.Unlock()
			return
		}

		if slog.Default().Enabled(context.Background(), slog.LevelDebug) {
			slog.Debug("got response",
				slog.Any("res", response),
				slog.Any("err", err),
			)
		}

		var f concurrent.Future[*proto.WriteResponse]
		f, sw.pendingRequests = sw.pendingRequests[0], sw.pendingRequests[1:]
		sw.Unlock()

		f.Complete(response)
	}
}
