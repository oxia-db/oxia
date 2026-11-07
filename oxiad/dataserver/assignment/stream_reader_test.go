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

package assignment

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestStreamReaderRunHandlesMessagesUntilEOF(t *testing.T) {
	for _, eof := range []struct {
		name string
		recv func() (*string, error)
	}{
		{name: "error", recv: func() (*string, error) { return nil, io.EOF }},
		{name: "nil message", recv: func() (*string, error) { return nil, nil }}, //nolint:nilnil // ends the stream
	} {
		t.Run(eof.name, func(t *testing.T) {
			var handled []string
			reader := ReadStream[string](t.Context(), newFakeStream(eof.recv, "a", "b"),
				func(message *string) error {
					handled = append(handled, *message)
					return nil
				}, nil, slog.Default())

			assert.NoError(t, reader.Run())
			assert.Equal(t, []string{"a", "b"}, handled)
		})
	}
}

func TestStreamReaderRunReturnsRecvError(t *testing.T) {
	for _, recvErr := range []error{errors.New("recv failure"), status.Error(codes.Canceled, "canceled")} {
		reader := ReadStream[string](t.Context(), newFakeStream(func() (*string, error) { return nil, recvErr }),
			func(*string) error { return nil }, nil, slog.Default())

		assert.ErrorIs(t, reader.Run(), recvErr)
	}
}

func TestStreamReaderRunReturnsHandlerError(t *testing.T) {
	handleErr := errors.New("handle failure")
	var handled []string
	reader := ReadStream[string](t.Context(), newFakeStream(nil, "a", "b"),
		func(message *string) error {
			handled = append(handled, *message)
			return handleErr
		}, nil, slog.Default())

	assert.ErrorIs(t, reader.Run(), handleErr)
	// The reader stops at the first failure
	assert.Equal(t, []string{"a"}, handled)
}

// Run returns once its context is done, and the reader fails its Recv after
// it: the report of that failure, with nobody left to receive it, must not
// park the reader goroutine for the life of the process.
func TestStreamReaderRunLeavesNoReaderBehindOnContextDone(t *testing.T) {
	runReturned := make(chan struct{})
	recvErr := status.Error(codes.Canceled, "server closing")
	ctx, cancel := context.WithCancel(t.Context())
	reader := ReadStream[string](ctx, newFakeStream(func() (*string, error) {
		<-runReturned
		return nil, recvErr
	}), func(*string) error { return nil }, nil, slog.Default())

	cancel()
	assert.NoError(t, reader.Run())
	close(runReturned)

	closeCh := reader.(*streamReader[string, any]).closeCh
	assert.Eventually(t, func() bool { return len(closeCh) == 1 }, 10*time.Second, time.Millisecond,
		"the reader did not report the failure of its Recv")
	assert.ErrorIs(t, <-closeCh, recvErr)
	_, open := <-closeCh
	assert.False(t, open, "the reader reports once, and closes the channel")
}

func TestProcessStreamSendsResponses(t *testing.T) {
	sender := &fakeSender{}
	reader := ProcessStream[string, int](t.Context(), newFakeStream(nil, "a", "bb"), sender,
		func(message *string) (*int, error) {
			length := len(*message)
			return &length, nil
		}, nil, slog.Default())

	assert.NoError(t, reader.Run())
	assert.Equal(t, []int{1, 2}, sender.sent)
}

func TestProcessStreamReturnsSendError(t *testing.T) {
	sendErr := errors.New("send failure")
	sender := &fakeSender{err: sendErr}
	reader := ProcessStream[string, int](t.Context(), newFakeStream(nil, "a", "bb"), sender,
		func(message *string) (*int, error) {
			length := len(*message)
			return &length, nil
		}, nil, slog.Default())

	assert.ErrorIs(t, reader.Run(), sendErr)
	assert.Equal(t, []int{1}, sender.sent)
}

// fakeStream returns its messages, then calls end, or returns io.EOF if end is nil.
type fakeStream struct {
	messages []string
	end      func() (*string, error)
}

func newFakeStream(end func() (*string, error), messages ...string) *fakeStream {
	return &fakeStream{messages: messages, end: end}
}

func (s *fakeStream) Recv() (*string, error) {
	if len(s.messages) > 0 {
		message := s.messages[0]
		s.messages = s.messages[1:]
		return &message, nil
	}
	if s.end != nil {
		return s.end()
	}
	return nil, io.EOF
}

type fakeSender struct {
	sent []int
	err  error
}

func (s *fakeSender) Send(message *int) error {
	s.sent = append(s.sent, *message)
	return s.err
}
