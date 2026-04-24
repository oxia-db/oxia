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

package config

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	gproto "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/process"
	commonproto "github.com/oxia-db/oxia/common/proto"
	oxiatime "github.com/oxia-db/oxia/common/time"
	rpc2 "github.com/oxia-db/oxia/oxiad/common/rpc"
	"github.com/oxia-db/oxia/oxiad/coordinator/option"
)

type source interface {
	Load(context.Context) ([]byte, error)
	Watch(context.Context, chan<- struct{}) error
}

type Handler interface {
	ReconcileClusterConfig(*commonproto.ClusterConfiguration) error
}

type Reconciler struct {
	*slog.Logger

	source       source
	sourceEvents chan struct{}

	mu      sync.RWMutex
	current *commonproto.ClusterConfiguration
}

func NewReconciler(ctx context.Context, cluster *option.ClusterOptions) (*Reconciler, error) {
	var configSource source
	if strings.HasPrefix(cluster.ConfigPath, configMapClusterConfigPrefix) {
		source, err := newConfigMapSource(cluster.ConfigPath)
		if err != nil {
			return nil, err
		}
		configSource = source
	} else {
		configSource = newFileSource(cluster.ConfigPath)
	}

	r := &Reconciler{
		Logger:       slog.With(slog.String("component", "cluster-config-reconciler")),
		source:       configSource,
		sourceEvents: make(chan struct{}, 1),
	}

	config, err := r.load(ctx)
	if err != nil {
		return nil, err
	}
	r.current = gproto.Clone(config).(*commonproto.ClusterConfiguration) //nolint:revive
	return r, nil
}

func (r *Reconciler) Load() (*commonproto.ClusterConfiguration, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.current == nil {
		return nil, errors.New("cluster configuration has not been loaded")
	}
	return gproto.Clone(r.current).(*commonproto.ClusterConfiguration), nil //nolint:revive
}

func (r *Reconciler) Start(ctx context.Context, handler Handler) error {
	if err := r.source.Watch(ctx, r.sourceEvents); err != nil {
		return err
	}

	go process.DoWithLabels(ctx, map[string]string{
		"component": "cluster-config-reconciler",
	}, func() {
		for {
			select {
			case <-ctx.Done():
				return

			case <-r.sourceEvents:
				r.Info("Received declarative cluster config source event")
				err := backoff.RetryNotify(func() error {
					return r.reconcile(ctx, handler)
				}, oxiatime.NewBackOffWithInitialInterval(ctx, time.Second), func(err error, duration time.Duration) {
					r.Warn(
						"failed to reconcile declarative cluster configuration, retrying later",
						slog.Any("error", err),
						slog.Duration("retry-after", duration),
					)
				})
				if err != nil {
					r.Warn("stopped reconciling declarative cluster configuration", slog.Any("error", err))
				}
			}
		}
	})
	return nil
}

func (r *Reconciler) reconcile(ctx context.Context, handler Handler) error {
	config, err := r.load(ctx)
	if err != nil {
		return err
	}

	r.mu.RLock()
	changed := !gproto.Equal(r.current, config)
	r.mu.RUnlock()
	if !changed {
		r.Info("No declarative cluster config changes detected")
		return nil
	}
	if err := handler.ReconcileClusterConfig(config); err != nil {
		return err
	}

	r.mu.Lock()
	r.current = gproto.Clone(config).(*commonproto.ClusterConfiguration) //nolint:revive
	r.mu.Unlock()
	return nil
}

func (r *Reconciler) load(ctx context.Context) (*commonproto.ClusterConfiguration, error) {
	data, err := r.source.Load(ctx)
	if err != nil {
		return nil, err
	}

	config, err := commonproto.UnmarshalClusterConfigurationYAML(data)
	if err != nil {
		return nil, err
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	for _, authority := range config.GetAllowExtraAuthorities() {
		if err := rpc2.ValidateAuthorityAddress(authority); err != nil {
			return nil, errors.Wrapf(err, "cluster configuration: invalid allowExtraAuthorities entry %q", authority)
		}
	}
	return config, nil
}
