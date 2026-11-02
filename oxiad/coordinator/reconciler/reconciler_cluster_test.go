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

	commonobject "github.com/oxia-db/oxia/common/object"
	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/common/cache"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
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

// New fails when the first reconciliation fails, instead of starting without
// the configuration applied.
func TestClusterReconcilerNewReturnsReconcileError(t *testing.T) {
	base := namespaceRuntimeForErrors("failing")
	base.metadata.config = &proto.ClusterConfiguration{Namespaces: []*proto.Namespace{base.metadata.configNS["failing"]}}
	createErr := errors.New("namespace write failure")
	runtime := &watchedNamespaceRuntime{
		mockNamespaceRuntime: base,
		failures:             map[string]error{"failing": createErr},
		attempts:             make(chan string, 1),
	}

	reconciler, err := New(t.Context(), runtime)
	require.ErrorIs(t, err, createErr)
	require.Nil(t, reconciler)
	require.Equal(t, "failing", <-runtime.attempts)
	require.Empty(t, base.added)
}

// An unknown annotation is logged, and doesn't fail the pass.
func TestClusterReconcilerReconcileIgnoresUnknownAnnotations(t *testing.T) {
	runtime := &watchedNamespaceRuntime{mockNamespaceRuntime: namespaceRuntimeForErrors()}
	reconciler := &clusterReconciler{ctx: t.Context(), logger: slog.Default(), runtime: runtime}

	require.NoError(t, reconciler.Reconcile(t.Context(), &proto.ClusterConfiguration{
		Annotations: map[string]string{"unknown": "value"},
	}))
	require.Equal(t, 1, runtime.recomputations)
}

func TestClusterReconcilerReconcileWithRetriesRetriesReconcileError(t *testing.T) {
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
	reconciler.wg.Go(func() { reconciler.reconcileWithRetries(subscription, bo) })
	// The first reconciliation fails on "retry", and the retry reconciles both
	// namespaces again
	for _, expected := range []string{"healthy", "retry", "healthy", "retry"} {
		require.Equal(t, expected, <-runtime.attempts)
	}
	require.NoError(t, reconciler.Close())
	require.Equal(t, 2, runtime.recomputations)
	// Once when the retries start, and once after the reconciliation succeeds
	require.Equal(t, 2, bo.resets)
	require.Len(t, base.added, 2)
	for _, name := range []string{"retry", "healthy"} {
		_, exists, err := base.metadata.GetNamespaceStatus(name)
		require.NoError(t, err)
		require.True(t, exists)
	}
}

func TestClusterReconcilerNewReturnsConfigurationLoadError(t *testing.T) {
	base := namespaceRuntimeForErrors()
	loadErr := errors.New("configuration unavailable")
	runtime := &failingMetadataRuntime{
		mockNamespaceRuntime: base,
		metadata: &failingMetadata{
			Metadata:  base.metadata,
			getConfig: func() error { return loadErr },
		},
	}

	reconciler, err := New(t.Context(), runtime)
	require.ErrorIs(t, err, loadErr)
	require.Nil(t, reconciler)
}

func TestClusterReconcilerReconcileWithRetriesRetriesConfigurationLoadError(t *testing.T) {
	base := namespaceRuntimeForErrors("default")
	base.metadata.config = &proto.ClusterConfiguration{Namespaces: []*proto.Namespace{base.metadata.configNS["default"]}}
	loads := 0
	runtime := &failingMetadataRuntime{
		mockNamespaceRuntime: base,
		metadata: &failingMetadata{
			Metadata: base.metadata,
			getConfig: func() error {
				loads++
				if loads == 1 {
					return errors.New("configuration unavailable")
				}
				return nil
			},
		},
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
	subscription := config.Subscribe()

	// Closing the configuration closes the subscription, which ends the
	// reconciliation without closing the reconciler
	require.NoError(t, config.Close())
	reconciler.reconcileWithRetries(subscription, &countingBackOff{})

	require.Equal(t, 2, loads)
	_, exists, err := base.metadata.GetNamespaceStatus("default")
	require.NoError(t, err)
	require.True(t, exists)
	require.NoError(t, reconciler.Close())
}

// failingMetadataRuntime is a namespace runtime with the metadata replaced.
type failingMetadataRuntime struct {
	*mockNamespaceRuntime
	metadata coordmetadata.Metadata
}

func (r *failingMetadataRuntime) Metadata() coordmetadata.Metadata {
	return r.metadata
}

// failingMetadata wraps a metadata: each hook, when set and failing, fails the
// method instead of calling the wrapped metadata.
type failingMetadata struct {
	coordmetadata.Metadata
	getConfig           func() error
	listNamespaceStatus func() error
}

func (m *failingMetadata) GetConfig() (commonobject.Borrowed[*proto.ClusterConfiguration], error) {
	if m.getConfig != nil {
		if err := m.getConfig(); err != nil {
			return commonobject.Borrowed[*proto.ClusterConfiguration]{}, err
		}
	}
	return m.Metadata.GetConfig()
}

func (m *failingMetadata) ListNamespaceStatus() (map[string]commonobject.Borrowed[*proto.NamespaceStatus], error) {
	if m.listNamespaceStatus != nil {
		if err := m.listNamespaceStatus(); err != nil {
			return nil, err
		}
	}
	return m.Metadata.ListNamespaceStatus()
}
