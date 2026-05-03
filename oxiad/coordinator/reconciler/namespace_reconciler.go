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

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime"
)

var _ Reconciler = (*namespaceReconciler)(nil)

type namespaceReconciler struct {
	runtime runtime.Runtime
}

func (*namespaceReconciler) Close() error { return nil }

func (r *namespaceReconciler) Reconcile(_ context.Context, snapshot *proto.ClusterConfiguration) error {
	metadata := r.runtime.Metadata()

	for _, namespace := range snapshot.GetNamespaces() {
		if _, exists := metadata.GetNamespaceStatus(namespace.GetName()); exists {
			continue
		}
		r.runtime.CreateNamespace(namespace.GetName(), namespace)
	}

	for name := range metadata.ListNamespaceStatus() {
		if _, exists := metadata.GetNamespace(name); exists {
			continue
		}
		r.runtime.DeleteNamespace(name)
	}

	return nil
}
