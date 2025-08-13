// Copyright 2025 StreamNative, Inc.
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

package server

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/common/logging"
	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/common/rpc"
	"github.com/oxia-db/oxia/proto"
	"github.com/oxia-db/oxia/server/kv"
)

type ReadableController interface {
	List(ctx context.Context, request *proto.ListRequest, cb concurrent.StreamCallback[string])

	Read(ctx context.Context, request *proto.ReadRequest, cb concurrent.StreamCallback[*proto.GetResponse])

	RangeScan(ctx context.Context, request *proto.RangeScanRequest, cb concurrent.StreamCallback[*proto.GetResponse])
}

func list0(ctx context.Context, request *proto.ListRequest, cb concurrent.StreamCallback[string], db kv.DB) {
	logger := logging.FromContext(ctx)
	go process.DoWithLabels(
		ctx,
		map[string]string{
			"oxia":  "list",
			"shard": fmt.Sprintf("%d", request.Shard),
			"peer":  rpc.GetPeer(ctx),
		},
		func() {
			logger.Debug("Received list request", slog.Any("request", request))

			var it kv.KeyIterator
			var err error

			if request.SecondaryIndexName != nil {
				it, err = newSecondaryIndexListIterator(request, db)
			} else {
				it, err = db.List(request)
			}
			if err != nil {
				logger.Warn(
					"Failed to process list request",
					slog.Any("error", err),
				)
				cb.OnComplete(err)
				return
			}

			defer func() {
				_ = it.Close()
			}()

			for ; it.Valid(); it.Next() {
				if err = cb.OnNext(it.Key()); err != nil {
					break
				}
				if err = ctx.Err(); err != nil {
					break
				}
			}
			cb.OnComplete(err)
		},
	)
}

func read0(ctx context.Context, request *proto.ReadRequest, cb concurrent.StreamCallback[*proto.GetResponse], db kv.DB) {
	logger := logging.FromContext(ctx)
	go process.DoWithLabels(
		ctx,
		map[string]string{
			"oxia":  "read",
			"shard": fmt.Sprintf("%d", request.Shard),
			"peer":  rpc.GetPeer(ctx),
		},
		func() {
			logger.Debug("Received read request")
			var response *proto.GetResponse
			var err error

			for _, get := range request.Gets {
				if get.SecondaryIndexName != nil {
					response, err = secondaryIndexGet(get, db)
				} else {
					response, err = db.Get(get)
				}
				if err != nil {
					break
				}
				if err = cb.OnNext(response); err != nil {
					break
				}
				if err = ctx.Err(); err != nil {
					break
				}
			}
			cb.OnComplete(err)
		},
	)
}

func rangeScan0(ctx context.Context, request *proto.RangeScanRequest, cb concurrent.StreamCallback[*proto.GetResponse], db kv.DB) {
	logger := logging.FromContext(ctx)
	go process.DoWithLabels(ctx,
		map[string]string{
			"oxia":  "range-scan",
			"shard": fmt.Sprintf("%d", request.Shard),
			"peer":  rpc.GetPeer(ctx),
		},
		func() {
			logger.Debug("Received list request", slog.Any("request", request))

			var it kv.RangeScanIterator
			var err error

			if request.SecondaryIndexName != nil {
				it, err = newSecondaryIndexRangeScanIterator(request, db)
			} else {
				it, err = db.RangeScan(request)
			}

			if err != nil {
				logger.Warn("Failed to process range-scan request", slog.Any("error", err))
				cb.OnComplete(err)
				return
			}

			defer func() {
				_ = it.Close()
			}()

			var gr *proto.GetResponse
			for ; it.Valid(); it.Next() {
				if gr, err = it.Value(); err != nil {
					break
				}
				if err = cb.OnNext(gr); err != nil {
					break
				}
				if err = ctx.Err(); err != nil {
					break
				}
			}
			cb.OnComplete(err)
		},
	)
}
