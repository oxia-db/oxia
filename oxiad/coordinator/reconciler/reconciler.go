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
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	gproto "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/process"
	commonproto "github.com/oxia-db/oxia/common/proto"
	oxiatime "github.com/oxia-db/oxia/common/time"
	coordmetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
)

type coordinator interface {
	ReconcileClusterConfig(*commonproto.ClusterConfiguration) error
}

type Reconciler struct {
	*slog.Logger

	metadata    coordmetadata.Metadata
	coordinator coordinator

	mu      sync.RWMutex
	current *commonproto.ClusterConfiguration
}

func New(ctx context.Context, metadata coordmetadata.Metadata, coordinator coordinator) *Reconciler {
	config := metadata.LoadConfig()
	r := &Reconciler{
		Logger:      slog.With(slog.String("component", "cluster-config-reconciler")),
		metadata:    metadata,
		coordinator: coordinator,
		current:     gproto.Clone(config).(*commonproto.ClusterConfiguration),
	}
	if !metadata.DeclarativeConfigEnabled() {
		r.Info("Declarative cluster config is disabled")
		return r
	}
	events := metadata.Watch()
	if events == nil {
		r.Info("Declarative cluster config watch is disabled")
		return r
	}
	r.start(ctx, events)
	return r
}

func (r *Reconciler) start(ctx context.Context, events <-chan struct{}) {
	go process.DoWithLabels(ctx, map[string]string{
		"component": "cluster-config-reconciler",
	}, func() {
		for {
			select {
			case <-ctx.Done():
				r.Warn("exit declarative cluster config reconciler due to an error", slog.Any("error", ctx.Err()))
				return
			case _, ok := <-events:
				if !ok {
					r.Warn("exit declarative cluster config reconciler because the watch was closed")
					return
				}
				r.Info("Received declarative cluster config source event")
				reconcileErr := backoff.RetryNotify(func() error {
					config, err := r.metadata.Load()
					if err != nil {
						return err
					}
					return r.reconcile(config)
				}, oxiatime.NewBackOffWithInitialInterval(ctx, time.Second), func(err error, duration time.Duration) {
					r.Warn(
						"failed to reconcile declarative cluster configuration, retrying later",
						slog.Any("error", err),
						slog.Duration("retry-after", duration),
					)
				})
				if reconcileErr != nil {
					r.Warn("stopped reconciling declarative cluster configuration", slog.Any("error", reconcileErr))
				}
			}
		}
	})
}

func (r *Reconciler) reconcile(config *commonproto.ClusterConfiguration) error {
	r.mu.RLock()
	changed := !gproto.Equal(r.current, config)
	r.mu.RUnlock()
	if !changed {
		r.Info("No declarative cluster config changes detected")
		return nil
	}
	if err := r.coordinator.ReconcileClusterConfig(config); err != nil {
		return err
	}

	r.mu.Lock()
	r.current = gproto.Clone(config).(*commonproto.ClusterConfiguration) //nolint:revive
	r.mu.Unlock()
	return nil
}
