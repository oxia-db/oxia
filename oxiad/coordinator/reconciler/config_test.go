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

package reconciler

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
)

func TestConfigReconcilerAppliesInitialAndUpdatedConfig(t *testing.T) {
	initial := &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{Name: "ns-1"}},
	}
	updated := &proto.ClusterConfiguration{
		Namespaces: []*proto.Namespace{{Name: "ns-2"}},
	}

	watch := commonwatch.New(initial)
	applied := make(chan *proto.ClusterConfiguration, 2)
	reconciler := NewConfigReconciler(slog.Default(), watch, func(config *proto.ClusterConfiguration) {
		applied <- config
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go reconciler.Run(ctx)

	require.Eventually(t, func() bool {
		select {
		case got := <-applied:
			return got == initial
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	watch.Publish(updated)

	require.Eventually(t, func() bool {
		select {
		case got := <-applied:
			return got == updated
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
}

func TestConfigReconcilerSkipsNilConfig(t *testing.T) {
	watch := commonwatch.New[*proto.ClusterConfiguration](nil)
	called := false
	reconciler := NewConfigReconciler(slog.Default(), watch, func(*proto.ClusterConfiguration) {
		called = true
	})

	ctx, cancel := context.WithCancel(t.Context())
	go reconciler.Run(ctx)
	time.Sleep(50 * time.Millisecond)
	cancel()

	assert.False(t, called)
}
