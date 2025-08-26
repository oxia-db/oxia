package session

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/oxia-db/oxia/oxia"
	"github.com/oxia-db/oxia/server"
	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"
)

func TestSessionEphemeralKeysLeak(t *testing.T) {
	config := server.NewTestConfig(t.TempDir())
	standaloneServer, err := server.NewStandalone(config)
	assert.NoError(t, err)
	defer standaloneServer.Close()

	client, err := oxia.NewAsyncClient(fmt.Sprintf("localhost:%d", standaloneServer.RpcPort()),
		// force the server cleanup the session to make the race-condition
		oxia.WithSessionKeepAliveTicker(16*time.Second),
		oxia.WithSessionTimeout(10*time.Second))
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
			_ = client.Put(fmt.Sprintf("/session-leak/%d", i), []byte{}, oxia.Ephemeral())
		}
	}
	err = client.Close()
	assert.NoError(t, err)

	syncClient, err := oxia.NewSyncClient(fmt.Sprintf("localhost:%d", standaloneServer.RpcPort()))
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		keys, err := syncClient.List(context.Background(), "/session-leak/", "/session-leak//")
		return assert.NoError(t, err) && assert.Empty(t, keys)
	}, 10*time.Second, 1*time.Second)

}
