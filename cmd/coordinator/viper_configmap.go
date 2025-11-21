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

	"github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/oxia-db/oxia/common/process"
)

type cmConfigProvider struct {
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

func (*cmConfigProvider) WatchChannel(rp viper.RemoteProvider) (<-chan *viper.RemoteResponse, chan bool) {
	kubernetes := metadata.NewK8SClientset(metadata.NewK8SClientConfig())
	namespace, configmap, _ := getNamespaceAndCmName(rp)

	ch := make(chan *viper.RemoteResponse)
	w, err := kubernetes.CoreV1().ConfigMaps(namespace).Watch(context.Background(), metav1.ListOptions{})
	if err != nil {
		slog.Error("Failed to setup watch on config map",
			slog.String("k8s-namespace", namespace),
			slog.String("k8s-config-map", configmap),
			slog.Any("error", err))
		ch <- &viper.RemoteResponse{Error: err}
		close(ch)
		return ch, nil
	}

	go process.DoWithLabels(context.Background(), map[string]string{
		"component": "k8s-configmap-watch",
	}, func() {
		for res := range w.ResultChan() {
			cm, ok := res.Object.(*v1.ConfigMap)
			if !ok {
				slog.Warn("Got wrong type of object notification",
					slog.String("k8s-namespace", namespace),
					slog.String("k8s-config-map", configmap),
					slog.Any("object", res),
				)
			}
			if cm.Name != configmap {
				continue
			}

			slog.Info("Got watch event from K8S",
				slog.String("k8s-namespace", namespace),
				slog.String("k8s-config-map", configmap),
				slog.Any("event-type", res.Type),
			)

			switch res.Type {
			case watch.Added, watch.Modified:
				ch <- &viper.RemoteResponse{
					Value: []byte(cm.Data[filePath]),
					Error: nil,
				}

				// Also notifies directly the oxia coordinator
				conf.ClusterConfigChangeNotifications <- nil
			default:
				ch <- &viper.RemoteResponse{
					Value: nil,
					Error: errors.Errorf("unexpected event on config map: %v", res.Type),
				}
			}
		}
	})

	return ch, nil
}

func init() {
	viper.RemoteConfig = &cmConfigProvider{}
	viper.SupportedRemoteProviders = []string{"configmap"}
}
