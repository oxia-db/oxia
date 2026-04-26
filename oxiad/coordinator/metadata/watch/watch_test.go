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

package watch

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gproto "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/proto"
)

func TestWatch(t *testing.T) {
	w := New()
	r, err := w.Subscribe()
	require.NoError(t, err)

	value, ok := r.Load()
	assert.False(t, ok)
	assert.Nil(t, value)

	config := &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{
			Name:              "default",
			InitialShardCount: 1,
			ReplicationFactor: 1,
		}},
	}
	w.Publish(config)

	select {
	case <-r.Changed():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for watch change")
	}

	value, ok = r.Load()
	require.True(t, ok)
	require.True(t, gproto.Equal(config, value))

	value.(*proto.ClusterConfiguration).Namespaces[0].Name = "mutated"
	value, ok = r.Load()
	require.True(t, ok)
	assert.Equal(t, "default", value.(*proto.ClusterConfiguration).Namespaces[0].GetName())
}

func TestWatchClose(t *testing.T) {
	w := New()
	r, err := w.Subscribe()
	require.NoError(t, err)

	w.Close()

	_, err = w.Subscribe()
	assert.ErrorIs(t, err, ErrClosed)

	_, ok := <-r.Changed()
	assert.False(t, ok)
}
