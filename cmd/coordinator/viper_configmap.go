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

package coordinator

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	k8s "k8s.io/client-go/kubernetes"

	"github.com/oxia-db/oxia/oxiad/coordinator/metadata"

	"github.com/oxia-db/oxia/common/process"
	oxiatime "github.com/oxia-db/oxia/common/time"
)

type cmConfigProvider struct {
	onConfigChange func()
}

// OnConfigChange sets the event handler that is called when a config map changes.
// This mirrors viper's OnConfigChange API for file-based configs, filling in
// the gap where viper's WatchRemoteConfigOnChannel does not trigger callbacks.
func (c *cmConfigProvider) OnConfigChange(fn func()) {
	c.onConfigChange = fn
}

const filePath = "config.yaml"

func getNamespaceAndCmName(rp viper.RemoteProvider) (namespace, cmName string, err error) {
	p := strings.Split(strings.TrimPrefix(rp.Path(), "configmap:"), "/")
	if len(p) != 2 {
		return "", "", errors.New("Invalid configmap configuration")
	}

	return p[0], p[1], nil
}

func (*cmConfigProvider) Get(rp viper.RemoteProvider) (io.Reader, error) {
	kubernetes := metadata.NewK8SClientset(metadata.NewK8SClientConfig())
	namespace, configmap, err := getNamespaceAndCmName(rp)
	if err != nil {
		return nil, err
	}
	cmValue, err := metadata.K8SConfigMaps(kubernetes).Get(namespace, configmap)
	if err != nil {
		return nil, err
	}

	data, ok := cmValue.Data[filePath]
	if !ok {
		return nil, errors.Errorf("path not found in config map: %s", rp.Path())
	}
	return bytes.NewReader([]byte(data)), nil
}

func (c *cmConfigProvider) Watch(rp viper.RemoteProvider) (io.Reader, error) {
	return c.Get(rp)
}

func (c *cmConfigProvider) WatchChannel(rp viper.RemoteProvider) (<-chan *viper.RemoteResponse, chan bool) {
	kubernetes := metadata.NewK8SClientset(metadata.NewK8SClientConfig())
	namespace, configmap, _ := getNamespaceAndCmName(rp)

	ch := make(chan *viper.RemoteResponse)

	go process.DoWithLabels(context.Background(), map[string]string{
		"component": "k8s-configmap-watch",
	}, func() {
		bo := oxiatime.NewBackOffWithInitialInterval(context.Background(), 1*time.Second)
		_ = backoff.RetryNotify(func() error {
			err := c.watchConfigMap(kubernetes, namespace, configmap, ch)
			if err != nil {
				return err
			}
			bo.Reset()
			return errors.New("K8S config map watch closed")
		}, bo, func(err error, duration time.Duration) {
			slog.Warn("K8S config map watch failed, reconnecting",
				slog.String("k8s-namespace", namespace),
				slog.String("k8s-config-map", configmap),
				slog.Any("error", err),
				slog.Duration("retry-after", duration),
			)
		})
	})

	return ch, nil
}

func (c *cmConfigProvider) watchConfigMap(kubernetes k8s.Interface, namespace, configmap string, ch chan<- *viper.RemoteResponse) error {
	w, err := kubernetes.CoreV1().ConfigMaps(namespace).Watch(
		context.Background(),
		metav1.SingleObject(metav1.ObjectMeta{Name: configmap, Namespace: namespace}),
	)
	if err != nil {
		return errors.Wrap(err, "failed to setup watch on config map")
	}
	defer w.Stop()

	// Read the current state after establishing the watch to avoid
	// missing updates that occurred between disconnect and re-watch.
	currentCm, err := kubernetes.CoreV1().ConfigMaps(namespace).Get(context.Background(), configmap, metav1.GetOptions{})
	if err != nil {
		return errors.Wrap(err, "failed to get current config map after watch setup")
	}
	c.notifyChange(ch, currentCm.Data[filePath])

	for res := range w.ResultChan() {
		if res.Type == watch.Error {
			return errors.Errorf("watch error: %v", res.Object)
		}

		cm, ok := res.Object.(*corev1.ConfigMap)
		if !ok {
			slog.Warn("Got wrong type of object notification",
				slog.String("k8s-namespace", namespace),
				slog.String("k8s-config-map", configmap),
				slog.Any("object", res),
			)
			continue
		}

		slog.Info("Got watch event from K8S",
			slog.String("k8s-namespace", namespace),
			slog.String("k8s-config-map", configmap),
			slog.Any("event-type", res.Type),
		)

		switch res.Type {
		case watch.Added, watch.Modified:
			c.notifyChange(ch, cm.Data[filePath])
		default:
			ch <- &viper.RemoteResponse{
				Value: nil,
				Error: errors.Errorf("unexpected event on config map: %v", res.Type),
			}
		}
	}

	return nil
}

func (c *cmConfigProvider) notifyChange(ch chan<- *viper.RemoteResponse, data string) {
	ch <- &viper.RemoteResponse{
		Value: []byte(data),
		Error: nil,
	}
	if c.onConfigChange != nil {
		c.onConfigChange()
	}
}

func init() {
	viper.RemoteConfig = &cmConfigProvider{}
	viper.SupportedRemoteProviders = []string{"configmap"}
}
