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

package k8s

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8swatch "k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"

	"github.com/oxia-db/oxia/common/channel"
)

// WatchConfigMap signals each time the config map is created, updated or
// deleted. The channel is closed when the watch ends or ctx is done.
func WatchConfigMap(ctx context.Context, kc kubernetes.Interface, namespace, name string) (<-chan struct{}, error) {
	w, err := kc.CoreV1().ConfigMaps(namespace).Watch(
		ctx,
		metav1.SingleObject(metav1.ObjectMeta{Name: name, Namespace: namespace}),
	)
	if err != nil {
		return nil, err
	}

	changes := make(chan struct{}, 1)
	go func() {
		defer close(changes)
		defer w.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case res, ok := <-w.ResultChan():
				if !ok || res.Type == k8swatch.Error {
					return
				}
				switch res.Type {
				case k8swatch.Added, k8swatch.Modified, k8swatch.Deleted:
					channel.PushNoBlock(changes, struct{}{})
				default:
				}
			}
		}
	}()
	return changes, nil
}
