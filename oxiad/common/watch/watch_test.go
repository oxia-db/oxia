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

	"github.com/oxia-db/oxia/common/proto"
)

func TestWatchLoad(t *testing.T) {
	w := New("initial")

	value := w.Load()
	assert.Equal(t, "initial", value)
}

func TestSubscribePublish(t *testing.T) {
	w := New(&proto.ClusterConfiguration{})
	r := w.Subscribe()

	value := r.Load()
	assert.NotNil(t, value)

	config := &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{
			Name:   "default",
			Policy: proto.NewHierarchyPolicies(1, 1, true, "hierarchical"),
		}},
	}
	w.Publish(config)

	select {
	case <-r.Changed():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for watch change")
	}

	value = r.Load()
	assert.Same(t, config, value)
}
