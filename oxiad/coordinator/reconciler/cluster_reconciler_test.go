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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	commonwatch "github.com/oxia-db/oxia/oxiad/common/watch"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider"
)

type recoveringNamespaceRuntime struct {
	*failingNamespaceRuntime
	recomputations int
}

func (r *recoveringNamespaceRuntime) CreateNamespace(name string, namespace *proto.Namespace) error {
	err := r.failingNamespaceRuntime.CreateNamespace(name, namespace)
	delete(r.failures, name)
	return err
}

func (r *recoveringNamespaceRuntime) RecomputeAssignments() {
	r.recomputations++
}

func TestClusterReconcilerRetriesNamespaceCreationErrors(t *testing.T) {
	base := namespaceRuntimeForErrors("retry", "healthy")
	runtime := &recoveringNamespaceRuntime{
		failingNamespaceRuntime: &failingNamespaceRuntime{
			Runtime:  base,
			failures: map[string]error{"retry": errors.New("temporary namespace write failure")},
		},
	}
	snapshot := &proto.ClusterConfiguration{Namespaces: []*proto.Namespace{
		base.metadata.configNS["healthy"], base.metadata.configNS["retry"],
	}}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	reconciler := &clusterReconciler{
		ctx:         ctx,
		logger:      slog.Default(),
		runtime:     runtime,
		reconcilers: []Reconciler{&namespaceReconciler{runtime: runtime}},
	}
	receiver := commonwatch.New(provider.Versioned[*proto.ClusterConfiguration]{Value: snapshot}).Subscribe()

	reconciler.reconcile0(snapshot, receiver)

	require.NoError(t, ctx.Err())
	require.Equal(t, []string{"healthy", "retry", "retry"}, runtime.attempted)
	require.Len(t, base.added, 2)
	require.Equal(t, 1, runtime.recomputations)
	for _, name := range []string{"retry", "healthy"} {
		_, exists := base.metadata.GetNamespaceStatus(name)
		require.True(t, exists)
	}
}
