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

package rpctest

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
)

var (
	testNode = &proto.DataServerIdentity{Internal: "node-0"}
	errTest  = errors.New("test failure")
)

func TestRpcProviderGetInfoOldNode(t *testing.T) {
	provider := NewRpcProvider()
	provider.GetNode(testNode).SetOldNode()

	res, err := provider.GetInfo(context.Background(), testNode, &proto.GetInfoRequest{})
	assert.Nil(t, res)
	require.ErrorIs(t, err, errNotImplemented)

	_, err = provider.Handshake(context.Background(), testNode, &proto.HandshakeRequest{})
	require.ErrorIs(t, err, errNotImplemented)
}

func TestRpcProviderHandshakeBlockedUntilContextDone(t *testing.T) {
	provider := NewRpcProvider()
	node := provider.GetNode(testNode)
	node.BlockHandshakes()
	defer node.ReleaseHandshakes()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := provider.Handshake(ctx, testNode, &proto.HandshakeRequest{})
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, int64(1), node.HandshakeCount.Load())
}

func TestRpcProviderCallsFailOnFailedNode(t *testing.T) {
	provider := NewRpcProvider()
	provider.FailNode(testNode, errTest)
	ctx := context.Background()

	_, err := provider.BecomeLeader(ctx, testNode, &proto.BecomeLeaderRequest{})
	require.ErrorIs(t, err, errTest)
	_, err = provider.DeleteShard(ctx, testNode, &proto.DeleteShardRequest{})
	require.ErrorIs(t, err, errTest)
	_, err = provider.AddFollower(ctx, testNode, &proto.AddFollowerRequest{})
	require.ErrorIs(t, err, errTest)
	_, err = provider.RemoveObserver(ctx, testNode, &proto.RemoveObserverRequest{})
	require.ErrorIs(t, err, errTest)
	_, err = provider.FreezeShard(ctx, testNode, &proto.FreezeShardRequest{})
	require.ErrorIs(t, err, errTest)

	provider.RecoverNode(testNode)
	res, err := provider.FreezeShard(ctx, testNode, &proto.FreezeShardRequest{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), res.HeadOffset)
}

func TestRpcProviderCallsReturnOnContextDone(t *testing.T) {
	provider := NewRpcProvider()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := provider.BecomeLeader(ctx, testNode, &proto.BecomeLeaderRequest{})
	require.ErrorIs(t, err, context.Canceled)
	_, err = provider.DeleteShard(ctx, testNode, &proto.DeleteShardRequest{})
	require.ErrorIs(t, err, context.Canceled)
	_, err = provider.AddFollower(ctx, testNode, &proto.AddFollowerRequest{})
	require.ErrorIs(t, err, context.Canceled)
	_, err = provider.RemoveObserver(ctx, testNode, &proto.RemoveObserverRequest{})
	require.ErrorIs(t, err, context.Canceled)
}

func TestRpcProviderCallsReturnOnContextDoneWhenTheRequestsAreNotRead(t *testing.T) {
	provider := NewRpcProvider()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	for name, call := range map[string]func() error{
		"NewTerm": func() error {
			_, err := provider.NewTerm(ctx, testNode, &proto.NewTermRequest{})
			return err
		},
		"BecomeLeader": func() error {
			_, err := provider.BecomeLeader(ctx, testNode, &proto.BecomeLeaderRequest{})
			return err
		},
		"GetStatus": func() error {
			_, err := provider.GetStatus(ctx, testNode, &proto.GetStatusRequest{})
			return err
		},
		"DeleteShard": func() error {
			_, err := provider.DeleteShard(ctx, testNode, &proto.DeleteShardRequest{})
			return err
		},
		"AddFollower": func() error {
			_, err := provider.AddFollower(ctx, testNode, &proto.AddFollowerRequest{})
			return err
		},
		"RemoveObserver": func() error {
			_, err := provider.RemoveObserver(ctx, testNode, &proto.RemoveObserverRequest{})
			return err
		},
		"FreezeShard": func() error {
			_, err := provider.FreezeShard(ctx, testNode, &proto.FreezeShardRequest{})
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			// Nobody reads the requests: the calls fill the buffer, then return
			for range 101 {
				_ = call()
			}
			require.ErrorIs(t, call(), context.Canceled)
		})
	}
}

func TestRpcProviderBecomeLeaderRejectsMissingFeatures(t *testing.T) {
	provider := NewRpcProvider()
	provider.GetNode(testNode).SetEnabledFeatures([]proto.Feature{proto.Feature_FEATURE_DB_CHECKSUM})

	_, err := provider.BecomeLeader(context.Background(), testNode, &proto.BecomeLeaderRequest{})
	require.ErrorIs(t, err, constant.ErrUnsupportedFeatures)
}

func TestRpcProviderRemoveObserverScriptedResponse(t *testing.T) {
	provider := NewRpcProvider()
	node := provider.GetNode(testNode)
	node.RemoveObserverResponse(errTest)

	_, err := provider.RemoveObserver(context.Background(), testNode, &proto.RemoveObserverRequest{Shard: 1, Term: 2})
	require.ErrorIs(t, err, errTest)
	node.ExpectRemoveObserverRequest(t, 1, 2)
}

func TestShardAssignmentClientEndStreamBeforeStart(t *testing.T) {
	stream := newShardAssignmentClient()
	stream.EndStream()

	require.ErrorIs(t, stream.RecvMsg(nil), errNotImplemented)
	assert.Nil(t, stream.Context())
}

func TestShardAssignmentClientUnsupportedMethodsPanic(t *testing.T) {
	stream := newShardAssignmentClient()

	assert.Panics(t, func() { _, _ = stream.CloseAndRecv() })
	assert.Panics(t, func() { _, _ = stream.Header() })
	assert.Panics(t, func() { _ = stream.Trailer() })
	assert.Panics(t, func() { _ = stream.CloseSend() })
	assert.Panics(t, func() { _ = stream.SendMsg(nil) })
}

func TestHealthClientList(t *testing.T) {
	client := newHealthClient()
	require.NoError(t, client.Close())

	res, err := client.List(context.Background(), &grpc_health_v1.HealthListRequest{})
	require.NoError(t, err)
	assert.NotNil(t, res)

	client.SetError(errTest)
	_, err = client.List(context.Background(), &grpc_health_v1.HealthListRequest{})
	require.ErrorIs(t, err, errTest)
}

func TestHealthWatchClientUnsupportedMethodsPanic(t *testing.T) {
	watch := newHealthWatchClient(context.Background())

	assert.Panics(t, func() { _, _ = watch.Header() })
	assert.Panics(t, func() { _ = watch.Trailer() })
	assert.Panics(t, func() { _ = watch.CloseSend() })
	assert.Panics(t, func() { _ = watch.Context() })
	assert.Panics(t, func() { _ = watch.SendMsg(nil) })
	assert.Panics(t, func() { _ = watch.RecvMsg(nil) })
}
