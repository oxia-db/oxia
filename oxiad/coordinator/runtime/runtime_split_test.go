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

package runtime

import (
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	shardcontroller "github.com/oxia-db/oxia/oxiad/coordinator/runtime/controller/shard"
)

type blockingCloseShardController struct {
	shardcontroller.Controller
	closeStarted chan struct{}
	closeRelease chan struct{}
}

func (c *blockingCloseShardController) Close() error {
	close(c.closeStarted)
	<-c.closeRelease
	return nil
}

func TestInitiateSplitFailsAfterRuntimeClose(t *testing.T) {
	rt := &runtime{closed: true}

	_, _, err := rt.InitiateSplit("default", 0, nil)

	require.ErrorIs(t, err, constant.ErrResourceUnavailable)
}

func TestSplitAbortedClosesChildControllersOutsideRuntimeLock(t *testing.T) {
	metadata := newTestMetadata(t, &proto.ClusterConfiguration{})
	childController := &blockingCloseShardController{
		closeStarted: make(chan struct{}),
		closeRelease: make(chan struct{}),
	}
	rt := &runtime{
		logger:           slog.Default(),
		metadata:         metadata,
		shardControllers: map[int64]shardcontroller.Controller{1: childController},
		assignmentsWatch: commonwatch.New(&proto.ShardAssignments{}),
	}
	var releaseOnce sync.Once
	releaseClose := func() {
		releaseOnce.Do(func() { close(childController.closeRelease) })
	}
	t.Cleanup(releaseClose)

	abortDone := make(chan struct{})
	go func() {
		rt.SplitAborted(0, 1, 2)
		close(abortDone)
	}()

	select {
	case <-childController.closeStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("child controller close did not start")
	}

	childStillRegistered := make(chan bool, 1)
	go func() {
		rt.RLock()
		_, exists := rt.shardControllers[1]
		rt.RUnlock()
		childStillRegistered <- exists
	}()

	select {
	case childExists := <-childStillRegistered:
		assert.False(t, childExists)
	case <-time.After(5 * time.Second):
		releaseClose()
		t.Fatal("runtime lock remained held while closing a child controller")
	}

	releaseClose()
	select {
	case <-abortDone:
	case <-time.After(5 * time.Second):
		t.Fatal("split abort did not complete")
	}
}
