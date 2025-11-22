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

package rpc

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/proto"
)

type loggingClientRpc struct {
	target string
	client proto.OxiaClientClient
}

func (l *loggingClientRpc) decorateErr(err error) error {
	if s, ok := status.FromError(err); ok {
		return status.Errorf(s.Code(), "%s - target=%s", s.Message(), l.target)
	}

	return err
}

func (l *loggingClientRpc) GetShardAssignments(ctx context.Context, in *proto.ShardAssignmentsRequest, opts ...grpc.CallOption) (
	res proto.OxiaClient_GetShardAssignmentsClient, err error) {
	if res, err = l.client.GetShardAssignments(ctx, in, opts...); err != nil {
		return nil, l.decorateErr(err)
	}

	return res, err
}

func (l *loggingClientRpc) WriteStream(ctx context.Context, opts ...grpc.CallOption) (res proto.OxiaClient_WriteStreamClient, err error) {
	if res, err = l.client.WriteStream(ctx, opts...); err != nil {
		return nil, l.decorateErr(err)
	}

	return res, err
}

func (l *loggingClientRpc) Write(ctx context.Context, in *proto.WriteRequest, opts ...grpc.CallOption) (
	res *proto.WriteResponse, err error) {
	if res, err = l.client.Write(ctx, in, opts...); err != nil {
		return nil, l.decorateErr(err)
	}

	return res, err
}

func (l *loggingClientRpc) Read(ctx context.Context, in *proto.ReadRequest, opts ...grpc.CallOption) (
	res proto.OxiaClient_ReadClient, err error) {
	if res, err = l.client.Read(ctx, in, opts...); err != nil {
		return nil, l.decorateErr(err)
	}

	return res, err
}

func (l *loggingClientRpc) List(ctx context.Context, in *proto.ListRequest, opts ...grpc.CallOption) (
	res proto.OxiaClient_ListClient, err error) {
	if res, err = l.client.List(ctx, in, opts...); err != nil {
		return nil, l.decorateErr(err)
	}

	return res, err
}

func (l *loggingClientRpc) RangeScan(ctx context.Context, in *proto.RangeScanRequest, opts ...grpc.CallOption) (
	res proto.OxiaClient_RangeScanClient, err error) {
	if res, err = l.client.RangeScan(ctx, in, opts...); err != nil {
		return nil, l.decorateErr(err)
	}

	return res, err
}

func (l *loggingClientRpc) GetNotifications(ctx context.Context, in *proto.NotificationsRequest, opts ...grpc.CallOption) (
	res proto.OxiaClient_GetNotificationsClient, err error) {
	if res, err = l.client.GetNotifications(ctx, in, opts...); err != nil {
		return nil, l.decorateErr(err)
	}

	return res, err
}

func (l *loggingClientRpc) CreateSession(ctx context.Context, in *proto.CreateSessionRequest, opts ...grpc.CallOption) (
	res *proto.CreateSessionResponse, err error) {
	if res, err = l.client.CreateSession(ctx, in, opts...); err != nil {
		return nil, l.decorateErr(err)
	}

	return res, err
}

func (l *loggingClientRpc) KeepAlive(ctx context.Context, in *proto.SessionHeartbeat, opts ...grpc.CallOption) (
	res *proto.KeepAliveResponse, err error) {
	if res, err = l.client.KeepAlive(ctx, in, opts...); err != nil {
		return nil, l.decorateErr(err)
	}

	return res, err
}

func (l *loggingClientRpc) CloseSession(ctx context.Context, in *proto.CloseSessionRequest, opts ...grpc.CallOption) (
	res *proto.CloseSessionResponse, err error) {
	if res, err = l.client.CloseSession(ctx, in, opts...); err != nil {
		return nil, l.decorateErr(err)
	}

	return res, err
}

func (l *loggingClientRpc) GetSequenceUpdates(ctx context.Context, in *proto.GetSequenceUpdatesRequest, opts ...grpc.CallOption) (res proto.OxiaClient_GetSequenceUpdatesClient, err error) {
	if res, err = l.client.GetSequenceUpdates(ctx, in, opts...); err != nil {
		return nil, l.decorateErr(err)
	}

	return res, err
}
