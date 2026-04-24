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
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	k8s "k8s.io/client-go/kubernetes"

	"github.com/oxia-db/oxia/common/process"
	oxiatime "github.com/oxia-db/oxia/common/time"
	k8smetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata/provider/kubernetes"
)

const (
	configMapClusterConfigPrefix = "configmap:"
	clusterConfigMapKey          = "config.yaml"
	k8sRequestTimeout            = 30 * time.Second
)

var newK8SClient = func() k8s.Interface {
	k8sConfig := k8smetadata.NewK8SClientConfig()
	return k8smetadata.NewK8SClientset(k8sConfig)
}

type configMapSource struct {
	client          k8s.Interface
	namespace, name string
}

func newConfigMapSource(configPath string) (*configMapSource, error) {
	path := strings.TrimPrefix(configPath, configMapClusterConfigPrefix)
	namespace, name, ok := strings.Cut(path, "/")
	if !ok || namespace == "" || name == "" {
		return nil, errors.Errorf("invalid configmap cluster configuration path %q, expected configmap:<namespace>/<name>", configPath)
	}
	return &configMapSource{
		client:    newK8SClient(),
		namespace: namespace,
		name:      name,
	}, nil
}

func (s *configMapSource) Load(ctx context.Context) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, k8sRequestTimeout)
	defer cancel()

	configMap, err := s.client.CoreV1().ConfigMaps(s.namespace).Get(ctx, s.name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	data, found := configMap.Data[clusterConfigMapKey]
	if !found {
		return nil, errors.Errorf("path %q not found in config map: configmap:%s/%s", clusterConfigMapKey, s.namespace, s.name)
	}
	return []byte(data), nil
}

func (s *configMapSource) Watch(ctx context.Context, events chan<- struct{}) error {
	go process.DoWithLabels(ctx, map[string]string{
		"component": "k8s-configmap-watch",
	}, func() {
		bo := oxiatime.NewBackOffWithInitialInterval(ctx, time.Second)
		_ = backoff.RetryNotify(func() error {
			err := s.watchOnce(ctx, events)
			if err == nil {
				return errors.New("K8S config map watch closed")
			}
			return err
		}, bo, func(err error, duration time.Duration) {
			slog.Warn("K8S config map watch failed, reconnecting",
				slog.String("k8s-namespace", s.namespace),
				slog.String("k8s-config-map", s.name),
				slog.Any("error", err),
				slog.Duration("retry-after", duration),
			)
		})
	})

	return nil
}

func (s *configMapSource) watchOnce(ctx context.Context, events chan<- struct{}) error {
	w, err := s.client.CoreV1().ConfigMaps(s.namespace).Watch(
		ctx,
		metav1.SingleObject(metav1.ObjectMeta{Name: s.name, Namespace: s.namespace}),
	)
	if err != nil {
		return errors.Wrap(err, "failed to setup watch on config map")
	}
	defer w.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case res, ok := <-w.ResultChan():
			if !ok {
				return errors.New("K8S config map watch closed")
			}

			if res.Type == watch.Error {
				return errors.Errorf("watch error: %v", res.Object)
			}

			configMap, ok := res.Object.(*corev1.ConfigMap)
			if !ok {
				slog.Warn("Got wrong type of object notification",
					slog.String("k8s-namespace", s.namespace),
					slog.String("k8s-config-map", s.name),
					slog.Any("object", res),
				)
				continue
			}

			slog.Info("Got watch event from K8S",
				slog.String("k8s-namespace", s.namespace),
				slog.String("k8s-config-map", s.name),
				slog.Any("event-type", res.Type),
			)

			switch res.Type {
			case watch.Added, watch.Modified:
				if _, found := configMap.Data[clusterConfigMapKey]; !found {
					return errors.Errorf("path %q not found in config map: configmap:%s/%s", clusterConfigMapKey, s.namespace, s.name)
				}
				select {
				case events <- struct{}{}:
				case <-ctx.Done():
					return ctx.Err()
				}
			default:
				return errors.Errorf("unexpected event on config map: %v", res.Type)
			}
		}
	}
}
