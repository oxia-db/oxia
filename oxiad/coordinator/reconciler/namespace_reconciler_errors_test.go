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
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/common/proto"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	coordruntime "github.com/oxia-db/oxia/oxiad/coordinator/runtime"
)

type failingNamespaceRuntime struct {
	coordruntime.Runtime
	failures  map[string]error
	attempted []string
}

func (r *failingNamespaceRuntime) CreateNamespace(name string, namespace *proto.Namespace) error {
	r.attempted = append(r.attempted, name)
	if err := r.failures[name]; err != nil {
		return err
	}
	return r.Runtime.CreateNamespace(name, namespace)
}

func namespaceRuntimeForErrors(names ...string) *mockNamespaceRuntime {
	metadata := &mockNamespaceMetadata{
		status:   proto.NewClusterStatus(),
		configNS: make(map[string]*proto.Namespace),
	}
	for _, name := range names {
		metadata.configNS[name] = &proto.Namespace{Name: name, InitialShardCount: 1, ReplicationFactor: 1}
	}
	return &mockNamespaceRuntime{
		metadata: metadata,
		selectNewEnsembleFn: func(*proto.Namespace, int64, *proto.ClusterStatus) ([]*proto.DataServerIdentity, error) {
			return []*proto.DataServerIdentity{s1}, nil
		},
	}
}

func TestNamespaceReconcilerReturnsCreationError(t *testing.T) {
	base := namespaceRuntimeForErrors("first", "second", "healthy")
	firstErr := errors.New("first namespace write failed")
	secondErr := errors.New("second namespace write failed")
	runtime := &failingNamespaceRuntime{
		Runtime: base,
		failures: map[string]error{
			"first":  firstErr,
			"second": secondErr,
		},
	}
	reconciler := &namespaceReconciler{runtime: runtime}
	snapshot := &proto.ClusterConfiguration{Namespaces: []*proto.Namespace{
		base.metadata.configNS["healthy"], base.metadata.configNS["first"], base.metadata.configNS["second"],
	}}

	err := reconciler.Reconcile(t.Context(), snapshot)
	require.Same(t, firstErr, err)
	require.NotErrorIs(t, err, secondErr)
	require.Equal(t, []string{"healthy", "first"}, runtime.attempted)
	require.Len(t, base.added, 1)
	_, exists := base.metadata.GetNamespaceStatus("healthy")
	require.True(t, exists)

	// Each retry skips namespaces already created and stops at the next error.
	delete(runtime.failures, "first")
	err = reconciler.Reconcile(t.Context(), snapshot)
	require.Same(t, secondErr, err)
	require.Equal(t, []string{"healthy", "first", "first", "second"}, runtime.attempted)
	require.Len(t, base.added, 2)

	delete(runtime.failures, "second")
	require.NoError(t, reconciler.Reconcile(t.Context(), snapshot))
	require.Equal(t, []string{"healthy", "first", "first", "second", "second"}, runtime.attempted)
	require.Len(t, base.added, 3)
	for _, name := range []string{"first", "second", "healthy"} {
		_, exists := base.metadata.GetNamespaceStatus(name)
		require.True(t, exists)
	}
}

func TestNamespaceReconcilerIgnoresAlreadyExists(t *testing.T) {
	base := namespaceRuntimeForErrors("existing", "healthy")
	runtime := &failingNamespaceRuntime{
		Runtime: base,
		failures: map[string]error{
			"existing": fmt.Errorf("namespace creation: %w", metadatacommon.ErrAlreadyExists),
		},
	}
	snapshot := &proto.ClusterConfiguration{Namespaces: []*proto.Namespace{
		base.metadata.configNS["existing"], base.metadata.configNS["healthy"],
	}}

	require.NoError(t, (&namespaceReconciler{runtime: runtime}).Reconcile(t.Context(), snapshot))
	require.Equal(t, []string{"existing", "healthy"}, runtime.attempted)
	require.Len(t, base.added, 1)
	_, exists := base.metadata.GetNamespaceStatus("healthy")
	require.True(t, exists)
}
