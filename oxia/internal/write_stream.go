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

package internal

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/proto"
)

type streamWrapper struct {
	sync.Mutex

	stream          proto.OxiaClient_WriteStreamClient
	pendingRequests []concurrent.Future[*proto.WriteResponse]
	failed          atomic.Bool
}

func newStreamWrapper(shard int64, stream proto.OxiaClient_WriteStreamClient) *streamWrapper {
	sw := &streamWrapper{
		stream:          stream,
		pendingRequests: nil,
	}

	go process.DoWithLabels(stream.Context(), map[string]string{
		"oxia":  "write-stream-handle-response",
		"shard": fmt.Sprintf("%d", shard),
	}, sw.handleResponses)
	go process.DoWithLabels(stream.Context(), map[string]string{
		"oxia":  "write-stream-handle-stream-closed",
		"shard": fmt.Sprintf("%d", shard),
	}, sw.handleStreamClosed)
	return sw
}

func (sw *streamWrapper) Send(ctx context.Context, req *proto.WriteRequest) (*proto.WriteResponse, error) {
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

func (sw *streamWrapper) handleStreamClosed() {
	<-sw.stream.Context().Done()

	// Fail all pending requests
	sw.Lock()
	defer sw.Unlock()

	for _, f := range sw.pendingRequests {
		f.Fail(io.EOF)
	}
	sw.pendingRequests = nil
	sw.failed.Store(true)
}

func (sw *streamWrapper) handleResponses() {
	for {
		response, err := sw.stream.Recv()
		sw.Lock()

		if err != nil {
			sw.failed.Store(true)
			sw.Unlock()
			return
		}

		slog.Debug("got response",
			slog.Any("res", response),
			slog.Any("err", err),
		)

		var f concurrent.Future[*proto.WriteResponse]
		f, sw.pendingRequests = sw.pendingRequests[0], sw.pendingRequests[1:]
		sw.Unlock()

		f.Complete(response)
	}
}
