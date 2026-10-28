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

package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/oxia-db/oxia/oxiad/common/errs"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/health/grpc_health_v1"
	pb "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/health"
)

func TestMaelstromShardAssignmentClientSendResendsUntilAcked(t *testing.T) {
	previousInterval := assignmentsResendInterval
	assignmentsResendInterval = 50 * time.Millisecond
	t.Cleanup(func() { assignmentsResendInterval = previousInterval })

	lines := captureStdout(t)

	coordinator := newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider())
	stream, err := newRpcProvider(coordinator).PushShardAssignments(t.Context(), &proto.DataServerIdentity{Internal: "n4"})
	require.NoError(t, err)
	require.NoError(t, stream.Send(assignmentsWithLeader("n4")))

	// The push is lost in a partition: the stream sends it again
	push := receiveLines(t, lines, 2)
	require.Equal(t, push[0], push[1])

	// The data server gets it, and acks it
	dataServer := &dispatcher{}
	deliver(t, dataServer, push[1])
	require.Equal(t, "n4", dataServer.currentLeader)
	ack := ""
	for ack == "" {
		line := receiveLines(t, lines, 1)[0]
		if msgType, _, _ := parseLine(t, line); msgType == MsgTypeShardAssignmentsAck {
			ack = line
		} else {
			// A resend that went out before the ack
			require.Equal(t, push[0], line)
		}
	}

	// The ack stops the resends
	deliver(t, coordinator, ack)
	fmt.Fprintln(os.Stdout, "acked")
	for line := receiveLines(t, lines, 1)[0]; line != "acked"; line = receiveLines(t, lines, 1)[0] {
		require.Equal(t, push[0], line)
	}
	select {
	case line := <-lines:
		require.Failf(t, "the stream resent the acked assignments", "message: %s", line)
	case <-time.After(5 * assignmentsResendInterval):
	}
}

func TestMaelstromShardAssignmentClientSendFailsOnUnserializableAssignments(t *testing.T) {
	coordinator := newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider())
	stream, err := newRpcProvider(coordinator).PushShardAssignments(t.Context(), &proto.DataServerIdentity{Internal: "n4"})
	require.NoError(t, err)

	err = stream.Send(&proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{"\xff": {}},
	})

	require.ErrorContains(t, err, "failed to serialize proto to json")
}

func TestMaelstromShardAssignmentClientSendClosesStreamOnWriteFailure(t *testing.T) {
	closeStdout(t)
	coordinator := newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider())
	stream, err := newRpcProvider(coordinator).PushShardAssignments(t.Context(), &proto.DataServerIdentity{Internal: "n4"})
	require.NoError(t, err)

	require.NoError(t, stream.Send(assignmentsWithLeader("n4")))

	require.ErrorIs(t, stream.Context().Err(), context.Canceled)
}

func TestMaelstromHealthCheckClientCheckClosesAssignmentStreamsAtFailureThreshold(t *testing.T) {
	lines := captureStdout(t)

	coordinator := newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider())
	provider := newRpcProvider(coordinator)
	node := &proto.DataServerIdentity{Internal: "n4"}
	stream, err := provider.PushShardAssignments(t.Context(), node)
	require.NoError(t, err)
	healthClient, err := provider.GetHealthClient(node)
	require.NoError(t, err)
	dataServer := newMaelstromGrpcProvider()

	// The probes lost below the threshold keep the stream, and a probe that
	// gets answered starts the count again
	answerProbe(t, lines, coordinator, healthClient, dataServer)
	for range health.DefaultCheckPolicy.FailureThreshold - 1 {
		loseProbe(t, lines, healthClient)
	}
	answerProbe(t, lines, coordinator, healthClient, dataServer)
	for range health.DefaultCheckPolicy.FailureThreshold - 1 {
		loseProbe(t, lines, healthClient)
	}
	require.NoError(t, stream.Context().Err())

	loseProbe(t, lines, healthClient)
	require.ErrorIs(t, stream.Context().Err(), context.Canceled)
}

func TestMaelstromHealthCheckClientCheckClosesAssignmentStreamsOfRestartedDataServer(t *testing.T) {
	lines := captureStdout(t)

	coordinator := newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider())
	provider := newRpcProvider(coordinator)
	node := &proto.DataServerIdentity{Internal: "n4"}
	stream, err := provider.PushShardAssignments(t.Context(), node)
	require.NoError(t, err)
	healthClient, err := provider.GetHealthClient(node)
	require.NoError(t, err)
	dataServer := newMaelstromGrpcProvider()

	answerProbe(t, lines, coordinator, healthClient, dataServer)
	answerProbe(t, lines, coordinator, healthClient, dataServer)
	require.NoError(t, stream.Context().Err())

	// The kill nemesis restarts the data server, before any probe is lost
	restarted := newMaelstromGrpcProvider()
	restarted.processId = dataServer.processId + 1
	answerProbe(t, lines, coordinator, healthClient, restarted)
	require.ErrorIs(t, stream.Context().Err(), context.Canceled)

	// The coordinator re-opens the stream, and the new process keeps it
	stream, err = provider.PushShardAssignments(t.Context(), node)
	require.NoError(t, err)
	answerProbe(t, lines, coordinator, healthClient, restarted)
	require.NoError(t, stream.Context().Err())
}

func TestMaelstromCoordinatorRpcProviderDeleteShard(t *testing.T) {
	lines := captureStdout(t)

	coordinator := newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider())
	dataServer := newMaelstromGrpcProvider()
	deleted := make(chan *proto.DeleteShardRequest, 1)
	dataServer.RegisterService(&proto.OxiaCoordination_ServiceDesc, &fakeCoordinationServer{
		deleteShard: func(req *proto.DeleteShardRequest) (*proto.DeleteShardResponse, error) {
			deleted <- req
			return &proto.DeleteShardResponse{}, nil
		},
	})

	res := make(chan error, 1)
	go func() {
		_, err := newRpcProvider(coordinator).DeleteShard(t.Context(), &proto.DataServerIdentity{Internal: "n4"},
			&proto.DeleteShardRequest{Namespace: constant.DefaultNamespace, Shard: 1, Term: 2})
		res <- err
	}()

	msgType, request, protoMsg := parseLine(t, receiveLines(t, lines, 1)[0])
	require.Equal(t, MsgTypeDeleteShardRequest, msgType)
	dataServer.HandleOxiaRequest(msgType, request.(*Message[OxiaMessage]), protoMsg)
	deliver(t, coordinator, receiveLines(t, lines, 1)[0])
	require.NoError(t, <-res)
	require.EqualValues(t, 1, (<-deleted).Shard)
}

func TestMaelstromCoordinatorRpcProviderForwardsRequestsToDataServer(t *testing.T) {
	lines := captureStdout(t)

	coordinator := newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider())
	provider := newRpcProvider(coordinator)
	node := &proto.DataServerIdentity{Internal: "n4"}
	headEntry := &proto.EntryId{Term: 3, Offset: 9}
	dataServer := newMaelstromGrpcProvider()
	dataServer.RegisterService(&proto.OxiaCoordination_ServiceDesc, &fakeCoordinationServer{
		newTerm: func(req *proto.NewTermRequest) (*proto.NewTermResponse, error) {
			return &proto.NewTermResponse{HeadEntryId: &proto.EntryId{Term: req.Term, Offset: 9}}, nil
		},
		becomeLeader: func(*proto.BecomeLeaderRequest) (*proto.BecomeLeaderResponse, error) {
			return &proto.BecomeLeaderResponse{}, nil
		},
		addFollower: func(*proto.AddFollowerRequest) (*proto.AddFollowerResponse, error) {
			return &proto.AddFollowerResponse{}, nil
		},
		getStatus: func(req *proto.GetStatusRequest) (*proto.GetStatusResponse, error) {
			return &proto.GetStatusResponse{Term: 3, Status: proto.ServingStatus_FOLLOWER}, nil
		},
	})

	tests := []struct {
		name string
		call func() (pb.Message, error)
		want pb.Message
	}{
		{"NewTerm", func() (pb.Message, error) {
			return provider.NewTerm(t.Context(), node, &proto.NewTermRequest{Term: 3})
		}, &proto.NewTermResponse{HeadEntryId: headEntry}},
		{"BecomeLeader", func() (pb.Message, error) {
			return provider.BecomeLeader(t.Context(), node, &proto.BecomeLeaderRequest{Term: 3})
		}, &proto.BecomeLeaderResponse{}},
		{"AddFollower", func() (pb.Message, error) {
			return provider.AddFollower(t.Context(), node, &proto.AddFollowerRequest{Term: 3})
		}, &proto.AddFollowerResponse{}},
		{"GetStatus", func() (pb.Message, error) {
			return provider.GetStatus(t.Context(), node, &proto.GetStatusRequest{})
		}, &proto.GetStatusResponse{Term: 3, Status: proto.ServingStatus_FOLLOWER}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			done := callInBackground(tt.call)
			answerRequest(t, lines, coordinator, dataServer)
			result := <-done
			require.NoError(t, result.err)
			require.True(t, pb.Equal(tt.want, result.res), "response: %v", result.res)
		})
	}
}

func TestMaelstromCoordinatorRpcProviderRequestsFailWhenCancelled(t *testing.T) {
	captureStdout(t)

	provider := newRpcProvider(newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider()))
	node := &proto.DataServerIdentity{Internal: "n4"}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := provider.NewTerm(ctx, node, &proto.NewTermRequest{})
	require.ErrorIs(t, err, context.Canceled)
	_, err = provider.BecomeLeader(ctx, node, &proto.BecomeLeaderRequest{})
	require.ErrorIs(t, err, context.Canceled)
	_, err = provider.AddFollower(ctx, node, &proto.AddFollowerRequest{})
	require.ErrorIs(t, err, context.Canceled)
	_, err = provider.GetStatus(ctx, node, &proto.GetStatusRequest{})
	require.ErrorIs(t, err, context.Canceled)
	_, err = provider.DeleteShard(ctx, node, &proto.DeleteShardRequest{})
	require.ErrorIs(t, err, context.Canceled)
}

func TestMaelstromCoordinatorRpcProviderAnswersLocalRequests(t *testing.T) {
	provider := newRpcProvider(&dispatcher{})
	node := &proto.DataServerIdentity{Internal: "n4"}

	removeObserver, err := provider.RemoveObserver(t.Context(), node, &proto.RemoveObserverRequest{})
	require.NoError(t, err)
	require.NotNil(t, removeObserver)
	freeze, err := provider.FreezeShard(t.Context(), node, &proto.FreezeShardRequest{})
	require.NoError(t, err)
	require.NotNil(t, freeze)
	handshake, err := provider.Handshake(t.Context(), node, &proto.HandshakeRequest{})
	require.NoError(t, err)
	require.Equal(t, proto.HandshakeStatus_HANDSHAKE_STATUS_ALREADY_BOUND, handshake.Status)
	require.Empty(t, handshake.FeaturesSupported)
	require.NoError(t, provider.Close())
}

func TestMaelstromShardAssignmentClientRecvMsgBlocksUntilClosed(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	stream, err := newRpcProvider(&dispatcher{}).PushShardAssignments(ctx, &proto.DataServerIdentity{Internal: "n4"})
	require.NoError(t, err)

	received := make(chan error, 1)
	go func() { received <- stream.RecvMsg(&proto.CoordinationShardAssignmentsResponse{}) }()
	select {
	case err := <-received:
		require.Failf(t, "RecvMsg returned on an open stream", "error: %v", err)
	case <-time.After(10 * time.Millisecond):
	}

	cancel()
	require.ErrorIs(t, <-received, context.Canceled)
	res, err := stream.CloseAndRecv()
	require.NoError(t, err)
	require.NotNil(t, res)
}

func TestBaseStreamPanicsOnUnsupportedCalls(t *testing.T) {
	stream := &BaseStream{}
	require.NoError(t, stream.CloseSend())
	require.NoError(t, stream.Context().Err())
	require.PanicsWithValue(t, errs.ErrNotImplemented, func() { _, _ = stream.Header() })
	require.PanicsWithValue(t, errs.ErrNotImplemented, func() { _ = stream.Trailer() })
	require.PanicsWithValue(t, errs.ErrNotImplemented, func() { _ = stream.SendMsg(nil) })
	require.PanicsWithValue(t, errs.ErrNotImplemented, func() { _ = stream.RecvMsg(nil) })
}

func TestMaelstromHealthCheckClientWatchStreamsServingUntilClosed(t *testing.T) {
	lines := captureStdout(t)

	coordinator := newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider())
	healthClient, err := newRpcProvider(coordinator).GetHealthClient(&proto.DataServerIdentity{Internal: "n4"})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	watched := make(chan grpc_health_v1.Health_WatchClient, 1)
	go func() {
		stream, err := healthClient.Watch(ctx, &grpc_health_v1.HealthCheckRequest{})
		if err != nil {
			stream = nil
		}
		watched <- stream
	}()
	answerRequest(t, lines, coordinator, newMaelstromGrpcProvider())
	stream := <-watched
	require.NotNil(t, stream)

	res, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, res.Status)

	cancel()
	_, err = stream.Recv()
	require.ErrorIs(t, err, context.Canceled)
}

func TestMaelstromHealthCheckClientListAnswersWhenServing(t *testing.T) {
	lines := captureStdout(t)

	coordinator := newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider())
	healthClient, err := newRpcProvider(coordinator).GetHealthClient(&proto.DataServerIdentity{Internal: "n4"})
	require.NoError(t, err)

	done := callInBackground(func() (pb.Message, error) {
		return healthClient.List(t.Context(), &grpc_health_v1.HealthListRequest{})
	})
	answerRequest(t, lines, coordinator, newMaelstromGrpcProvider())
	result := <-done
	require.NoError(t, result.err)
	require.NotNil(t, result.res)
	require.NoError(t, healthClient.(*maelstromHealthCheckClient).Close())
}

func TestMaelstromHealthCheckClientFailsWhenCancelled(t *testing.T) {
	captureStdout(t)

	healthClient, err := newRpcProvider(newDispatcher(newMaelstromGrpcProvider(), newMaelstromReplicationRpcProvider())).
		GetHealthClient(&proto.DataServerIdentity{Internal: "n4"})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err = healthClient.Watch(ctx, &grpc_health_v1.HealthCheckRequest{})
	require.ErrorIs(t, err, context.Canceled)
	_, err = healthClient.List(ctx, &grpc_health_v1.HealthListRequest{})
	require.ErrorIs(t, err, context.Canceled)
}

// deliver hands a Maelstrom message to the dispatcher, as the main loop does
// with each line it reads.
func deliver(t *testing.T, d *dispatcher, line string) {
	t.Helper()
	require.NoError(t, d.ReceivedMessage(parseLine(t, line)))
}

func assignmentsWithLeader(leader string) *proto.ShardAssignments {
	return &proto.ShardAssignments{
		Namespaces: map[string]*proto.NamespaceShardsAssignment{
			constant.DefaultNamespace: {Assignments: []*proto.ShardAssignment{{Leader: leader}}},
		},
	}
}

// answerProbe runs a health check of the coordinator, and answers it from the
// data server.
func answerProbe(t *testing.T, lines <-chan string, coordinator *dispatcher,
	healthClient grpc_health_v1.HealthClient, dataServer *maelstromGrpcProvider) {
	t.Helper()
	checked := make(chan error, 1)
	go func() {
		_, err := healthClient.Check(t.Context(), &grpc_health_v1.HealthCheckRequest{})
		checked <- err
	}()

	msgType, probe, _ := parseLine(t, receiveLines(t, lines, 1)[0])
	require.Equal(t, MsgTypeHealthCheck, msgType)
	dataServer.HandleOxiaRequest(msgType, probe.(*Message[OxiaMessage]), nil)
	deliver(t, coordinator, receiveLines(t, lines, 1)[0])
	require.NoError(t, <-checked)
}

// loseProbe runs a health check of the coordinator that gets no answer, as
// across a partition.
func loseProbe(t *testing.T, lines <-chan string, healthClient grpc_health_v1.HealthClient) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	_, err := healthClient.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	receiveLines(t, lines, 1)
}

// answerRequest hands the next request of the coordinator to the data server,
// and its answer back to the coordinator.
func answerRequest(t *testing.T, lines <-chan string, coordinator *dispatcher, dataServer *maelstromGrpcProvider) {
	t.Helper()
	msgType, request, protoMsg := parseLine(t, receiveLines(t, lines, 1)[0])
	dataServer.HandleOxiaRequest(msgType, request.(*Message[OxiaMessage]), protoMsg)
	deliver(t, coordinator, receiveLines(t, lines, 1)[0])
}

type callResult struct {
	res pb.Message
	err error
}

func callInBackground(call func() (pb.Message, error)) <-chan callResult {
	done := make(chan callResult, 1)
	go func() {
		res, err := call()
		done <- callResult{res: res, err: err}
	}()
	return done
}
