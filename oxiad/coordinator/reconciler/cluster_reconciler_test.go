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

package reconciler

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/cache"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
)

type watchedNamespaceRuntime struct {
	*mockNamespaceRuntime
	failures       map[string]error
	attempts       chan string
	recomputations int
}

func (r *watchedNamespaceRuntime) CreateNamespace(name string, namespace *proto.Namespace) error {
	r.attempts <- name
	if err := r.failures[name]; err != nil {
		delete(r.failures, name)
		return err
	}
	return r.mockNamespaceRuntime.CreateNamespace(name, namespace)
}

func (r *watchedNamespaceRuntime) RecomputeAssignments() {
	r.recomputations++
}

type countingBackOff struct {
	backoff.ZeroBackOff
	resets int
}

func (b *countingBackOff) Reset() {
	b.resets++
}

func TestClusterReconcilerReturnsReconcileErrorForRetry(t *testing.T) {
	base := namespaceRuntimeForErrors("retry", "healthy")
	base.metadata.config = &proto.ClusterConfiguration{Namespaces: []*proto.Namespace{
		base.metadata.configNS["healthy"], base.metadata.configNS["retry"],
	}}
	runtime := &watchedNamespaceRuntime{
		mockNamespaceRuntime: base,
		failures:             map[string]error{"retry": errors.New("temporary namespace write failure")},
		attempts:             make(chan string, 16),
	}
	ctx, cancel := context.WithCancel(t.Context())
	reconciler := &clusterReconciler{
		ctx:         ctx,
		ctxCancel:   cancel,
		logger:      slog.Default(),
		runtime:     runtime,
		reconcilers: []Reconciler{&namespaceReconciler{runtime: runtime}},
	}
	config := cache.New(ctx, func(context.Context) (*provider.Versioned[*proto.ClusterConfiguration], error) {
		return &provider.Versioned[*proto.ClusterConfiguration]{Value: base.metadata.config}, nil
	}, nil)
	defer func() { require.NoError(t, config.Close()) }()
	subscription := config.Subscribe()
	bo := &countingBackOff{}
	require.EqualError(t, reconciler.bgWatchClusterConfiguration(subscription, bo), "temporary namespace write failure")
	require.Equal(t, []string{"healthy", "retry"}, []string{<-runtime.attempts, <-runtime.attempts})
	require.Zero(t, bo.resets)

	reconciler.wg.Go(func() { require.NoError(t, reconciler.bgWatchClusterConfiguration(subscription, bo)) })
	require.Equal(t, []string{"healthy", "retry"}, []string{<-runtime.attempts, <-runtime.attempts})
	require.NoError(t, reconciler.Close())
	require.Equal(t, 2, runtime.recomputations)
	require.Equal(t, 1, bo.resets)
	require.Len(t, base.added, 2)
	for _, name := range []string{"retry", "healthy"} {
		_, exists := base.metadata.GetNamespaceStatus(name)
		require.True(t, exists)
	}
}
