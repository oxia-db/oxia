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
	"bufio"
	"context"
	"io"
	"math/rand/v2"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/oxia-db/oxia/common/proto"
)

// captureStdout redirects stdout, the channel every Maelstrom message is sent
// on, and returns the messages printed until the end of the test.
func captureStdout(t *testing.T) <-chan string {
	t.Helper()
	previousStdout := os.Stdout
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = writer

	lines := make(chan string, 1024)
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(reader)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
	}()

	t.Cleanup(func() {
		os.Stdout = previousStdout
		_ = writer.Close()
	})
	return lines
}

func receiveLines(t *testing.T, lines <-chan string, count int) []string {
	t.Helper()
	received := make([]string, 0, count)
	for len(received) < count {
		select {
		case line := <-lines:
			received = append(received, line)
		case <-time.After(5 * time.Second):
			require.Failf(t, "timed out waiting for messages", "got %d of %d", len(received), count)
		}
	}
	return received
}

// recordingReplicationServer is a follower that records the offsets of the
// entries its Replicate handler receives, in order.
type recordingReplicationServer struct {
	proto.UnimplementedOxiaLogReplicationServer

	offsets chan int64
	done    chan error
}

func newRecordingReplicationServer() *recordingReplicationServer {
	return &recordingReplicationServer{
		offsets: make(chan int64, 1024),
		done:    make(chan error, 1),
	}
}

func (r *recordingReplicationServer) Replicate(stream grpc.BidiStreamingServer[proto.Append, proto.Ack]) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			r.done <- err
			return err
		}
		r.offsets <- req.Entry.Offset
	}
}

// deliverToFollower feeds Maelstrom messages to the follower side of the
// replicate streams, as the dispatcher does with each line it reads.
func deliverToFollower(provider *maelstromGrpcProvider, lines []string) {
	for _, line := range lines {
		msgType, msg, protoMsg := parseRequest(line)
		provider.HandleOxiaStreamRequest(msgType, msg.(*Message[OxiaStreamMessage]), protoMsg)
	}
}

func TestReplicateStreamDeliversEntriesInSendOrder(t *testing.T) {
	lines := captureStdout(t)

	// The leader pushes a backlog back to back, e.g. after a partition heals
	const count = 20
	client, err := newMaelstromReplicationRpcProvider().GetReplicateStream(
		context.Background(), "n2", "default", 0, 1)
	require.NoError(t, err)
	for offset := range int64(count) {
		require.NoError(t, client.Send(&proto.Append{Term: 1, Entry: &proto.LogEntry{Term: 1, Offset: offset}}))
	}
	messages := receiveLines(t, lines, count)

	// Maelstrom delivers every message after its own random latency, so
	// messages sent back to back overtake each other
	rand.New(rand.NewPCG(1, 2)).Shuffle(len(messages), func(i, j int) {
		messages[i], messages[j] = messages[j], messages[i]
	})

	follower := newRecordingReplicationServer()
	provider := newMaelstromGrpcProvider()
	provider.RegisterService(&proto.OxiaLogReplication_ServiceDesc, follower)
	deliverToFollower(provider, messages)

	// Like on a gRPC stream, the follower gets the entries in the order the
	// leader sent them
	for offset := range int64(count) {
		select {
		case received := <-follower.offsets:
			require.Equal(t, offset, received)
		case <-time.After(5 * time.Second):
			require.Failf(t, "timed out waiting for entry", "offset %d", offset)
		}
	}
}

func TestReplicateStreamResetsWhenAppendsAreLost(t *testing.T) {
	previousGapTimeout := replicateStreamGapTimeout
	replicateStreamGapTimeout = 50 * time.Millisecond
	t.Cleanup(func() { replicateStreamGapTimeout = previousGapTimeout })

	lines := captureStdout(t)

	const count = 5
	client, err := newMaelstromReplicationRpcProvider().GetReplicateStream(
		context.Background(), "n2", "default", 0, 1)
	require.NoError(t, err)
	for offset := range int64(count) {
		require.NoError(t, client.Send(&proto.Append{Term: 1, Entry: &proto.LogEntry{Term: 1, Offset: offset}}))
	}
	messages := receiveLines(t, lines, count)

	// The append of entry 2 is lost in a partition
	messages = slices.Delete(messages, 2, 3)

	follower := newRecordingReplicationServer()
	provider := newMaelstromGrpcProvider()
	provider.RegisterService(&proto.OxiaLogReplication_ServiceDesc, follower)
	deliverToFollower(provider, messages)

	// The stream stops at the gap and gets reset, like a broken connection
	select {
	case err := <-follower.done:
		require.ErrorIs(t, err, io.EOF)
	case <-time.After(5 * time.Second):
		require.Fail(t, "the replicate stream was not reset")
	}
	var received []int64
	for len(follower.offsets) > 0 {
		received = append(received, <-follower.offsets)
	}
	require.Equal(t, []int64{0, 1}, received)

	// The leader is told, so that its cursor reconnects
	msgType, msg, _ := parseRequest(receiveLines(t, lines, 1)[0])
	require.Equal(t, MsgTypeStreamError, msgType)
	require.Equal(t, client.(*maelstromReplicateClient).streamId, msg.(*Message[OxiaStreamMessage]).Body.StreamId)
}
