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
	"sync/atomic"
)

const (
	callPending int32 = iota
	callSent
	callDropped
)

// CallContext is the context of the caller of a write call. The caller can give
// up on the call until it is handed to gRPC: once the context is done, the call
// is dropped instead, and it fails with an error telling that it was not sent.
type CallContext struct {
	ctx   context.Context
	state atomic.Int32
}

func NewCallContext(ctx context.Context) *CallContext {
	return &CallContext{ctx: ctx}
}

// MarkSent is invoked right before the call is handed to gRPC. It returns false
// if the call must be dropped instead, because its context is done. A call
// without CallContext is always sent.
func (c *CallContext) MarkSent() bool {
	if c == nil {
		return true
	}
	if c.ctx.Err() != nil {
		c.state.CompareAndSwap(callPending, callDropped)
	} else {
		c.state.CompareAndSwap(callPending, callSent)
	}
	return c.state.Load() == callSent
}

// Cancel is invoked by the caller once the context is done, if the call did not
// complete yet. It returns the error of the call: a call that was not handed to
// gRPC yet is dropped, while one that was may still be applied by the server.
func (c *CallContext) Cancel() error {
	c.state.CompareAndSwap(callPending, callDropped)
	if c.state.Load() == callDropped {
		return c.Err()
	}
	return c.ctx.Err()
}

// Err returns the error of a call that was dropped.
func (c *CallContext) Err() error {
	return NotSent(c.ctx.Err())
}

// notSentError is the failure of a call that was never handed to gRPC. Unlike
// a failure after the call was sent, e.g. a timeout waiting for the response,
// it tells that the server did not apply the call, and never will.
type notSentError struct {
	error
}

func (e notSentError) Unwrap() error {
	return e.error
}

// NotSent marks err as the failure of a call that was never handed to gRPC.
func NotSent(err error) error {
	if err == nil {
		return nil
	}
	return notSentError{err}
}

// IsNotSent tells if err is the failure of a call that was never handed to
// gRPC.
func IsNotSent(err error) bool {
	return errors.As(err, new(notSentError))
}
