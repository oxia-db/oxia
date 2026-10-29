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

package time

import (
	"context"
	"errors"
	"testing"
	stdtime "time"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/assert"
)

func TestConcurrentBackOff_RetryNotifyStopsWaitingOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	bo := NewConcurrentBackOff(NewBackOffWithInitialInterval(ctx, stdtime.Hour))

	done := make(chan error, 1)
	go func() {
		done <- backoff.RetryNotify(func() error {
			return errors.New("failed")
		}, bo, func(error, stdtime.Duration) {
			// Called right before RetryNotify waits out the backoff interval
			cancel()
		})
	}()

	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled)
	case <-stdtime.After(10 * stdtime.Second):
		assert.Fail(t, "RetryNotify must stop waiting as soon as the context is cancelled")
	}
}

func TestConcurrentBackOff_Context(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	assert.Equal(t, ctx, NewConcurrentBackOff(NewBackOff(ctx)).Context())
	assert.Equal(t, context.Background(), NewConcurrentBackOff(&backoff.ZeroBackOff{}).Context())
}
