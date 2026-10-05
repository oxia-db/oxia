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
	"log/slog"

	"go.uber.org/multierr"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/common/validation"
	"github.com/oxia-db/oxia/oxiad/coordinator/runtime"
)

var _ Reconciler = (*namespaceReconciler)(nil)

type namespaceReconciler struct {
	runtime runtime.Runtime
}

func (*namespaceReconciler) Close() error { return nil }

func (r *namespaceReconciler) Reconcile(_ context.Context, snapshot *proto.ClusterConfiguration) error {
	metadata := r.runtime.Metadata()

	var errs error
	for _, namespace := range snapshot.GetNamespaces() {
		// A configuration file doesn't go through the management API checks.
		// Skip the namespace instead of failing, which would block the
		// reconciliation of the rest of the configuration.
		if err := validation.ValidateNamespace(namespace.GetName()); err != nil {
			slog.Error(
				"Cannot create namespace",
				slog.String("namespace", namespace.GetName()),
				slog.Any("error", err),
			)
			continue
		}
		// A previous write may have committed before returning an error. The
		// runtime also repairs missing controllers when the status already exists.
		if err := r.runtime.CreateNamespace(namespace.GetName(), namespace); err != nil {
			slog.Error(
				"Failed to create namespace",
				slog.String("namespace", namespace.GetName()),
				slog.Any("error", err),
			)
			errs = multierr.Append(errs, err)
		}
	}

	status, err := metadata.ListNamespaceStatus()
	if err != nil {
		return multierr.Append(errs, err)
	}
	for name := range status {
		if _, exists := metadata.GetNamespace(name); exists {
			continue
		}
		r.runtime.DeleteNamespace(name)
	}

	return errs
}
