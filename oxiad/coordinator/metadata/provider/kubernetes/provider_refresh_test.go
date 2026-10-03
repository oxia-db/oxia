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

package kubernetes

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8swatch "k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	commonproto "github.com/oxia-db/oxia/common/proto"
	metadatacommon "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common"
	metadatacodec "github.com/oxia-db/oxia/oxiad/coordinator/metadata/common/codec"
)

var configMapsResource = schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}

func refreshTestConfigMap(t *testing.T, version string, generator int64) *corev1.ConfigMap {
	t.Helper()
	data, err := metadatacodec.ClusterStatusCodec.MarshalYAML(&commonproto.ClusterStatus{ShardIdGenerator: generator})
	require.NoError(t, err)
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "status", Namespace: "oxia", ResourceVersion: version},
		Data:       map[string]string{metadatacodec.ClusterStatusCodec.GetKey(): string(data)},
	}
}

// The first write commits in the fake API server but loses its response. Later
// writes succeed normally, including resource-version conflict checks.
func interceptCommittedWrite(t *testing.T, client *fake.Clientset) *atomic.Int32 {
	t.Helper()
	writes := &atomic.Int32{}
	client.PrependReactor("*", "configmaps", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		desired := &corev1.ConfigMap{}
		var version int
		switch action := action.(type) {
		case k8stesting.CreateAction:
			desired = action.GetObject().(*corev1.ConfigMap).DeepCopy()
		case k8stesting.PatchAction:
			require.NoError(t, json.Unmarshal(action.GetPatch(), desired))
			existing, err := client.Tracker().Get(configMapsResource, "oxia", "status")
			require.NoError(t, err)
			if desired.ResourceVersion != existing.(*corev1.ConfigMap).ResourceVersion {
				return true, nil, k8serrors.NewConflict(corev1.Resource("configmaps"), "status", errors.New("stale version"))
			}
			version, err = strconv.Atoi(desired.ResourceVersion)
			require.NoError(t, err)
		default:
			return false, nil, nil
		}
		desired.Namespace = "oxia"
		desired.ResourceVersion = strconv.Itoa(version + 1)
		if action.GetVerb() == "create" {
			require.NoError(t, client.Tracker().Create(configMapsResource, desired, "oxia"))
		} else {
			require.NoError(t, client.Tracker().Update(configMapsResource, desired, "oxia"))
		}
		if writes.Add(1) == 1 {
			return true, nil, context.DeadlineExceeded
		}
		return true, desired, nil
	})
	return writes
}

func TestStoreRefreshesCommittedWriteAfterError(t *testing.T) {
	for _, create := range []bool{true, false} {
		t.Run(strconv.FormatBool(create), func(t *testing.T) {
			client := fake.NewSimpleClientset()
			if !create {
				require.NoError(t, client.Tracker().Create(configMapsResource, refreshTestConfigMap(t, "1", 0), "oxia"))
			}
			interceptCommittedWrite(t, client)
			p, err := NewProvider(t.Context(), client, "oxia", "status", metadatacodec.ClusterStatusCodec,
				metadatacommon.WatchDisabled, "review")
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, p.Close()) })
			snapshot := p.Watch().Load()
			snapshot.Value = &commonproto.ClusterStatus{ShardIdGenerator: 7}
			_, err = p.Store(snapshot)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			cached := p.Watch().Load()
			require.NotEqual(t, snapshot.Version, cached.Version)
			require.Equal(t, int64(7), cached.Value.ShardIdGenerator)
			cached.Value = &commonproto.ClusterStatus{ShardIdGenerator: 8}
			_, err = p.Store(cached)
			require.NoError(t, err)
			require.Equal(t, int64(8), p.Watch().Load().Value.ShardIdGenerator)
		})
	}
}

func TestWatchRetriesFailedRefreshWithoutWatchEvent(t *testing.T) {
	client := fake.NewSimpleClientset(refreshTestConfigMap(t, "1", 0))
	writes := interceptCommittedWrite(t, client)
	watcher := k8swatch.NewRaceFreeFake()
	started := make(chan string, 1)
	client.PrependWatchReactor("configmaps", func(action k8stesting.Action) (bool, k8swatch.Interface, error) {
		started <- action.(k8stesting.WatchAction).GetWatchRestrictions().ResourceVersion
		return true, watcher, nil
	})
	var failReads atomic.Bool
	var failedReads atomic.Int32
	watchReadFailed := make(chan struct{}, 1)
	readErr := errors.New("refresh unavailable")
	client.PrependReactor("get", "configmaps", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
		if !failReads.Load() {
			return false, nil, nil
		}
		if failedReads.Add(1) == 3 {
			watchReadFailed <- struct{}{}
		}
		return true, nil, readErr
	})
	p, err := NewProvider(t.Context(), client, "oxia", "status", metadatacodec.ClusterStatusCodec,
		metadatacommon.WatchEnabled, "review")
	require.NoError(t, err)
	t.Cleanup(func() { watcher.Stop(); require.NoError(t, p.Close()) })
	select {
	case version := <-started:
		require.Equal(t, "1", version)
	case <-time.After(5 * time.Second):
		t.Fatal("status watch did not start")
	}
	snapshot := p.Watch().Load()
	snapshot.Value = &commonproto.ClusterStatus{ShardIdGenerator: 7}
	failReads.Store(true)
	_, err = p.Store(snapshot)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorIs(t, err, readErr)
	require.Equal(t, int64(0), p.Watch().Load().Value.ShardIdGenerator)

	_, err = p.Store(snapshot)
	require.ErrorIs(t, err, metadatacommon.ErrBadVersion)
	require.ErrorIs(t, err, readErr)
	require.Equal(t, int32(1), writes.Load(), "the stale retry must not commit")
	select {
	case <-watchReadFailed:
	case <-time.After(5 * time.Second):
		t.Fatal("watch did not attempt refresh")
	}
	failReads.Store(false)
	require.Eventually(t, func() bool {
		return p.Watch().Load().Value.ShardIdGenerator == 7
	}, 5*time.Second, 10*time.Millisecond)
	current := p.Watch().Load()
	current.Value = &commonproto.ClusterStatus{ShardIdGenerator: 8}
	_, err = p.Store(current)
	require.NoError(t, err)
	require.Equal(t, int32(2), writes.Load())
}

func TestWatchRefreshesWriteCommittedAfterStoreError(t *testing.T) {
	client := fake.NewSimpleClientset(refreshTestConfigMap(t, "1", 0))
	watcher := k8swatch.NewRaceFreeFake()
	started := make(chan struct{}, 1)
	client.PrependWatchReactor("configmaps", func(k8stesting.Action) (bool, k8swatch.Interface, error) {
		started <- struct{}{}
		return true, watcher, nil
	})
	client.PrependReactor("patch", "configmaps", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
		return true, nil, context.DeadlineExceeded
	})
	p, err := NewProvider(t.Context(), client, "oxia", "status", metadatacodec.ClusterStatusCodec,
		metadatacommon.WatchEnabled, "review")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, p.Close()) })
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("status watch did not start")
	}
	snapshot := p.Watch().Load()
	snapshot.Value = &commonproto.ClusterStatus{ShardIdGenerator: 7}
	_, err = p.Store(snapshot)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, int64(0), p.Watch().Load().Value.ShardIdGenerator)

	// The server applies the request after the immediate refresh has completed.
	saved := refreshTestConfigMap(t, "2", 7)
	require.NoError(t, client.Tracker().Update(configMapsResource, saved, "oxia"))
	watcher.Modify(saved)
	require.Eventually(t, func() bool {
		return p.Watch().Load().Value.ShardIdGenerator == 7
	}, 5*time.Second, 10*time.Millisecond)
}
