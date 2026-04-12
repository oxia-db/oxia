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

package lead

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSession_Heartbeat_NonBlocking(t *testing.T) {
	s := &session{
		heartbeatCh: make(chan bool, 1),
		log:         slog.Default(),
	}
	s.ctx, s.ctxCancel = context.WithCancel(context.Background())

	// Fill the heartbeat channel buffer
	s.heartbeatCh <- true

	// Second heartbeat should not block (non-blocking select)
	done := make(chan struct{})
	go func() {
		s.heartbeat()
		close(done)
	}()

	select {
	case <-done:
		// Success: heartbeat returned without blocking
	case <-time.After(time.Second):
		t.Fatal("heartbeat() blocked when channel buffer was full")
	}
}

func TestSession_Heartbeat_AfterClose(t *testing.T) {
	ch := make(chan bool, 1)
	s := &session{
		heartbeatCh: ch,
		log:         slog.Default(),
	}
	s.ctx, s.ctxCancel = context.WithCancel(context.Background())
	s.latch.Add(1)

	// Drain heartbeats using local reference (mirrors waitForHeartbeats pattern)
	go func() {
		defer s.latch.Done()
		for range ch {
		}
	}()

	s.close()
	s.latch.Wait()

	// Calling heartbeat after close should not panic
	assert.NotPanics(t, func() {
		s.heartbeat()
	})
}

func TestSession_Heartbeat_ConcurrentCloseAndHeartbeat(t *testing.T) {
	// Run many iterations to increase chance of hitting the race
	for i := 0; i < 100; i++ {
		ch := make(chan bool, 1)
		s := &session{
			heartbeatCh: ch,
			log:         slog.Default(),
		}
		s.ctx, s.ctxCancel = context.WithCancel(context.Background())
		s.latch.Add(1)

		// Drain using local reference (mirrors waitForHeartbeats pattern)
		go func() {
			defer s.latch.Done()
			for range ch {
			}
		}()

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			s.close()
		}()
		go func() {
			defer wg.Done()
			s.heartbeat()
		}()
		wg.Wait()
		s.latch.Wait()
	}
}
