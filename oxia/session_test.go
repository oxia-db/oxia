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

package oxia

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"

	"github.com/oxia-db/oxia/node"
)

func TestSessionEphemeralKeysLeak(t *testing.T) {
	config := node.NewTestConfig(t.TempDir())
	standaloneServer, err := node.NewStandalone(config)
	assert.NoError(t, err)
	defer standaloneServer.Close()

	client, err := NewAsyncClient(standaloneServer.ServiceAddr(),
		// force the server cleanup the session to make the race-condition
		withSessionKeepAliveTicker(16*time.Second),
		WithSessionTimeout(10*time.Second))
	assert.NoError(t, err)

	after := time.After(40 * time.Second)
	limiter := rate.NewLimiter(rate.Limit(1000), 1000)
loop:
	for i := 0; ; i++ {
		select {
		case <-after:
			break loop
		default:
			err := limiter.Wait(context.Background())
			assert.NoError(t, err)
			_ = client.Put(fmt.Sprintf("/session-leak/%d", i), []byte{}, Ephemeral())
		}
	}
	err = client.Close()
	assert.NoError(t, err)

	syncClient, err := NewSyncClient(standaloneServer.ServiceAddr())
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		keys, err := syncClient.List(context.Background(), "/session-leak/", "/session-leak//")
		return assert.NoError(t, err) && assert.Empty(t, keys)
	}, 10*time.Second, 1*time.Second)
}
