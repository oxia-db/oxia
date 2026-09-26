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

package model

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCallContext_WithoutContextIsSent(t *testing.T) {
	var callContext *CallContext
	assert.True(t, callContext.MarkSent())
}

func TestCallContext_SentCallIsNotDropped(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	callContext := NewCallContext(ctx)
	assert.True(t, callContext.MarkSent())

	cancel()
	err := callContext.Cancel()
	assert.ErrorIs(t, err, context.Canceled)
	assert.False(t, IsNotSent(err))
	assert.True(t, callContext.MarkSent())
}

func TestCallContext_CancelledCallIsDropped(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	callContext := NewCallContext(ctx)

	cancel()
	err := callContext.Cancel()
	assert.ErrorIs(t, err, context.Canceled)
	assert.True(t, IsNotSent(err))
	assert.False(t, callContext.MarkSent())
}

// The call is dropped once the context is done, even if the caller did not
// cancel it yet.
func TestCallContext_CallIsDroppedOnceTheContextIsDone(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	callContext := NewCallContext(ctx)

	cancel()
	assert.False(t, callContext.MarkSent())
	assert.True(t, IsNotSent(callContext.Err()))
	assert.True(t, IsNotSent(callContext.Cancel()))
}

func TestNotSent(t *testing.T) {
	assert.NoError(t, NotSent(nil))

	err := NotSent(context.DeadlineExceeded)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, context.DeadlineExceeded.Error(), err.Error())
	assert.True(t, IsNotSent(err))
	assert.False(t, IsNotSent(context.DeadlineExceeded))
	assert.False(t, IsNotSent(errors.New("failure")))
}
