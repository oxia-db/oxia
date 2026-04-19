package kubernetes

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gproto "google.golang.org/protobuf/proto"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	kubernetesclient "k8s.io/client-go/kubernetes"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	metadatapb "github.com/oxia-db/oxia/common/proto/metadata"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata_v2"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata_v2/document"
)

func TestBackendCommitLoadAndBadVersion(t *testing.T) {
	client := newVersionedClientset()
	backend := NewBackend(context.Background(), client, "default", "backend")
	t.Cleanup(func() {
		assert.NoError(t, backend.Close())
	})

	config := &document.Versioned[gproto.Message]{
		Value: &metadatapb.Cluster{
			AllowedExtraAuthorities: []string{"ca-1"},
		},
	}
	err := backend.Store(document.ConfigRecordName, config)
	require.NoError(t, err)

	loadedConfig := loadBackendRecord[*metadatapb.Cluster](t, backend, document.ConfigRecordName)
	require.NotNil(t, loadedConfig)
	require.NotNil(t, loadedConfig.Value)
	assert.Equal(t, []string{"ca-1"}, loadedConfig.Value.AllowedExtraAuthorities)

	status := &document.Versioned[gproto.Message]{
		Value: &metadatapb.ClusterState{
			Namespaces: map[string]*metadatapb.NamespaceState{
				"ns-1": {ReplicationFactor: 3},
			},
		},
	}
	err = backend.Store(document.StatusRecordName, status)
	require.NoError(t, err)

	loadedStatus := loadBackendRecord[*metadatapb.ClusterState](t, backend, document.StatusRecordName)
	require.NotNil(t, loadedStatus)
	require.NotNil(t, loadedStatus.Value)
	require.Contains(t, loadedStatus.Value.Namespaces, "ns-1")
	assert.EqualValues(t, 3, loadedStatus.Value.Namespaces["ns-1"].ReplicationFactor)

	configMap, err := client.CoreV1().ConfigMaps("default").Get(context.Background(), string(document.ConfigRecordName), metav1.GetOptions{})
	require.NoError(t, err)
	configMap.Data[documentDataKey] = "{\n  \"allowedExtraAuthorities\": [\n    \"remote\"\n  ]\n}\n"
	_, err = client.CoreV1().ConfigMaps("default").Update(context.Background(), configMap, metav1.UpdateOptions{})
	require.NoError(t, err)

	config = &document.Versioned[gproto.Message]{
		Version: loadedConfig.Version,
		Value: &metadatapb.Cluster{
			AllowedExtraAuthorities: []string{"local"},
		},
	}
	err = backend.Store(document.ConfigRecordName, config)
	require.ErrorIs(t, err, document.ErrBadVersion)

	reloaded := loadBackendRecord[*metadatapb.Cluster](t, backend, document.ConfigRecordName)
	require.NotNil(t, reloaded)
	require.NotNil(t, reloaded.Value)
	assert.Equal(t, []string{"remote"}, reloaded.Value.AllowedExtraAuthorities)

	config = &document.Versioned[gproto.Message]{
		Version: reloaded.Version,
		Value: &metadatapb.Cluster{
			AllowedExtraAuthorities: []string{"final"},
		},
	}
	err = backend.Store(document.ConfigRecordName, config)
	require.NoError(t, err)

	finalConfig := loadBackendRecord[*metadatapb.Cluster](t, backend, document.ConfigRecordName)
	require.NotNil(t, finalConfig)
	require.NotNil(t, finalConfig.Value)
	assert.Equal(t, []string{"final"}, finalConfig.Value.AllowedExtraAuthorities)
}

func TestStoreLeaseHandoff(t *testing.T) {
	restoreLeaseTimings(t, 1500*time.Millisecond, time.Second, 200*time.Millisecond)

	client := newVersionedClientset()
	primary := document.NewStore(context.Background(), NewBackend(context.Background(), client, "default", "primary"))
	primaryClosed := false
	t.Cleanup(func() {
		if !primaryClosed {
			assert.NoError(t, primary.Close())
		}
	})

	waitForLeaseState(t, primary, metadatapb.LeaseState_LEASE_STATE_HELD)
	requireEventuallyLeaseReady(t, func() error {
		return primary.CreateNamespaces([]*metadatapb.Namespace{{Name: "primary"}})
	})

	secondary := document.NewStore(context.Background(), NewBackend(context.Background(), client, "default", "secondary"))
	t.Cleanup(func() {
		assert.NoError(t, secondary.Close())
	})

	state, _ := secondary.LeaseWatch().Load()
	assert.Equal(t, metadatapb.LeaseState_LEASE_STATE_UNHELD, state)
	assert.ErrorIs(t, secondary.CreateNamespaces([]*metadatapb.Namespace{{Name: "secondary"}}), metadata_v2.ErrLeaseNotHeld)

	require.NoError(t, primary.Close())
	primaryClosed = true

	waitForLeaseState(t, secondary, metadatapb.LeaseState_LEASE_STATE_HELD)
	requireEventuallyLeaseReady(t, func() error {
		return secondary.CreateNamespaces([]*metadatapb.Namespace{{Name: "secondary"}})
	})

	namespace, err := secondary.GetNamespace("secondary")
	require.NoError(t, err)
	assert.Equal(t, "secondary", namespace.Name)
}

func TestBackendRevalidateLeaseRenewsHeldLease(t *testing.T) {
	client := newVersionedClientset()
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "metadata-lease",
			Namespace: "default",
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       ptr("holder"),
			LeaseDurationSeconds: ptr(int32(15)),
			RenewTime:            &metav1.MicroTime{Time: time.Unix(100, 0)},
		},
	}
	_, err := client.CoordinationV1().Leases("default").Create(context.Background(), lease, metav1.CreateOptions{})
	require.NoError(t, err)

	backend := newBackendForRevalidateTest(t, client, "holder")
	before, err := client.CoordinationV1().Leases("default").Get(context.Background(), "metadata-lease", metav1.GetOptions{})
	require.NoError(t, err)

	require.NoError(t, backend.LeaseRevalidate())

	after, err := client.CoordinationV1().Leases("default").Get(context.Background(), "metadata-lease", metav1.GetOptions{})
	require.NoError(t, err)
	require.NotNil(t, after.Spec.HolderIdentity)
	assert.Equal(t, "holder", *after.Spec.HolderIdentity)
	assert.NotEqual(t, before.ResourceVersion, after.ResourceVersion)
	require.NotNil(t, after.Spec.RenewTime)
	assert.True(t, after.Spec.RenewTime.Time.After(before.Spec.RenewTime.Time) || after.Spec.RenewTime.Time.Equal(before.Spec.RenewTime.Time))
}

func TestBackendRevalidateLeaseFailsWhenHolderChanges(t *testing.T) {
	client := newVersionedClientset()
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "metadata-lease",
			Namespace: "default",
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       ptr("other"),
			LeaseDurationSeconds: ptr(int32(15)),
		},
	}
	_, err := client.CoordinationV1().Leases("default").Create(context.Background(), lease, metav1.CreateOptions{})
	require.NoError(t, err)

	backend := newBackendForRevalidateTest(t, client, "holder")
	err = backend.LeaseRevalidate()
	require.ErrorIs(t, err, metadata_v2.ErrLeaseNotHeld)

	state, _ := backend.LeaseWatch().Load()
	assert.Equal(t, metadatapb.LeaseState_LEASE_STATE_UNHELD, state)
}

func TestBackendRevalidateLeaseFailsOnRenewConflict(t *testing.T) {
	client := newVersionedClientset()
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "metadata-lease",
			Namespace: "default",
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       ptr("holder"),
			LeaseDurationSeconds: ptr(int32(15)),
		},
	}
	_, err := client.CoordinationV1().Leases("default").Create(context.Background(), lease, metav1.CreateOptions{})
	require.NoError(t, err)

	conflicted := atomic.Bool{}
	client.PrependReactor("update", "leases", func(action ktesting.Action) (bool, runtime.Object, error) {
		if conflicted.CompareAndSwap(false, true) {
			return true, nil, k8serrors.NewConflict(schema.GroupResource{
				Group:    action.GetResource().Group,
				Resource: action.GetResource().Resource,
			}, action.(ktesting.UpdateAction).GetObject().(*coordinationv1.Lease).Name, errors.New("injected conflict"))
		}
		return false, nil, nil
	})

	backend := newBackendForRevalidateTest(t, client, "holder")
	err = backend.LeaseRevalidate()
	require.ErrorIs(t, err, metadata_v2.ErrLeaseNotHeld)
	assert.True(t, conflicted.Load())

	state, _ := backend.LeaseWatch().Load()
	assert.Equal(t, metadatapb.LeaseState_LEASE_STATE_UNHELD, state)
}

func loadBackendRecord[T gproto.Message](t *testing.T, backend *Backend, name document.MetaRecordName) *document.Versioned[T] {
	t.Helper()

	record := backend.Load(name)
	require.NotNil(t, record)

	value, ok := record.Value.(T)
	require.Truef(t, ok, "unexpected record type for %q: %T", name, record.Value)

	return &document.Versioned[T]{
		Version: record.Version,
		Value:   value,
	}
}

func restoreLeaseTimings(t *testing.T, duration time.Duration, deadline time.Duration, period time.Duration) {
	t.Helper()

	prevDuration := leaseDuration
	prevDeadline := renewDeadline
	prevPeriod := retryPeriod
	leaseDuration = duration
	renewDeadline = deadline
	retryPeriod = period
	t.Cleanup(func() {
		leaseDuration = prevDuration
		renewDeadline = prevDeadline
		retryPeriod = prevPeriod
	})
}

func waitForLeaseState(t *testing.T, store *Store, expected metadatapb.LeaseState) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	watch := store.LeaseWatch()
	state, version := watch.Load()
	for state != expected {
		var err error
		state, version, err = watch.Wait(ctx, version)
		require.NoError(t, err)
	}
}

func requireEventuallyLeaseReady(t *testing.T, op func() error) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		lastErr = op()
		if lastErr == nil {
			return
		}
		if !errors.Is(lastErr, metadata_v2.ErrLeaseNotHeld) {
			require.NoError(t, lastErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.NoError(t, lastErr)
}

func newVersionedClientset(objects ...runtime.Object) *kubernetesfake.Clientset {
	client := kubernetesfake.NewSimpleClientset(objects...)
	var versionSeq atomic.Uint64

	nextVersion := func() string {
		return strconv.FormatUint(versionSeq.Add(1), 10)
	}

	client.PrependReactor("create", "*", func(action ktesting.Action) (bool, runtime.Object, error) {
		createAction, ok := action.(ktesting.CreateAction)
		if !ok {
			return false, nil, nil
		}

		accessor, err := meta.Accessor(createAction.GetObject())
		if err != nil {
			return true, nil, err
		}
		accessor.SetResourceVersion(nextVersion())
		return false, nil, nil
	})

	client.PrependReactor("update", "*", func(action ktesting.Action) (bool, runtime.Object, error) {
		updateAction, ok := action.(ktesting.UpdateAction)
		if !ok {
			return false, nil, nil
		}

		accessor, err := meta.Accessor(updateAction.GetObject())
		if err != nil {
			return true, nil, err
		}

		existing, err := client.Tracker().Get(action.GetResource(), action.GetNamespace(), accessor.GetName())
		if err != nil {
			return true, nil, err
		}

		existingAccessor, err := meta.Accessor(existing)
		if err != nil {
			return true, nil, err
		}

		if accessor.GetResourceVersion() != existingAccessor.GetResourceVersion() {
			return true, nil, k8serrors.NewConflict(schema.GroupResource{
				Group:    action.GetResource().Group,
				Resource: action.GetResource().Resource,
			}, accessor.GetName(), errors.New("resourceVersion mismatch"))
		}

		accessor.SetResourceVersion(nextVersion())
		return false, nil, nil
	})

	client.PrependReactor("patch", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		patchAction, ok := action.(ktesting.PatchAction)
		if !ok {
			return false, nil, nil
		}
		if patchAction.GetPatchType() != types.ApplyPatchType {
			return true, nil, errors.New("expected apply patch")
		}

		existing, err := client.Tracker().Get(action.GetResource(), action.GetNamespace(), patchAction.GetName())
		if err != nil {
			return true, nil, err
		}

		existingConfigMap, ok := existing.(*corev1.ConfigMap)
		if !ok {
			return true, nil, errors.New("expected ConfigMap for patch")
		}

		var patch struct {
			Metadata struct {
				ResourceVersion string `json:"resourceVersion"`
			} `json:"metadata"`
			Data map[string]string `json:"data"`
		}
		if err := json.Unmarshal(patchAction.GetPatch(), &patch); err != nil {
			return true, nil, err
		}

		if patch.Metadata.ResourceVersion != existingConfigMap.ResourceVersion {
			return true, nil, k8serrors.NewConflict(schema.GroupResource{
				Group:    action.GetResource().Group,
				Resource: action.GetResource().Resource,
			}, patchAction.GetName(), errors.New("resourceVersion mismatch"))
		}

		updated := existingConfigMap.DeepCopy()
		if patch.Data != nil {
			updated.Data = patch.Data
		}
		updated.ResourceVersion = nextVersion()
		if err := client.Tracker().Update(action.GetResource(), updated, action.GetNamespace()); err != nil {
			return true, nil, err
		}
		return true, updated, nil
	})

	return client
}

func newBackendForRevalidateTest(t *testing.T, client kubernetesclient.Interface, identity string) *Backend {
	t.Helper()

	backend := &Backend{
		ctx:        context.Background(),
		client:     client,
		namespace:  "default",
		identity:   identity,
		leaseName:  "metadata-lease",
		leaseWatch: commonoption.NewWatch(metadatapb.LeaseState_LEASE_STATE_HELD),
	}
	backend.LeaseLock = &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      "metadata-lease",
			Namespace: "default",
		},
		Client: client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: identity,
		},
	}
	_, _, err := backend.Get(context.Background())
	require.NoError(t, err)
	return backend
}

func ptr[T any](v T) *T {
	return &v
}
