package kubernetes

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	coordinationv1 "k8s.io/api/coordination/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kubernetesclient "k8s.io/client-go/kubernetes"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	metadatapb "github.com/oxia-db/oxia/common/proto/metadata"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata_v2"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata_v2/document"
)

func TestBackendCommitLoadAndBadVersion(t *testing.T) {
	client := newVersionedClientset()
	backend := NewBackend(context.Background(), client, "default", "metadata")
	t.Cleanup(func() {
		assert.NoError(t, backend.Close())
	})

	_, err := backend.CommitConfig(&document.Versioned[*metadatapb.Cluster]{
		Value: &metadatapb.Cluster{
			AllowedExtraAuthorities: []string{"ca-1"},
		},
	})
	require.NoError(t, err)

	loadedConfig := backend.LoadConfig()
	require.NotNil(t, loadedConfig)
	require.NotNil(t, loadedConfig.Value)
	assert.Equal(t, []string{"ca-1"}, loadedConfig.Value.AllowedExtraAuthorities)

	_, err = backend.CommitStatus(&document.Versioned[*metadatapb.ClusterState]{
		Value: &metadatapb.ClusterState{
			Namespaces: map[string]*metadatapb.NamespaceState{
				"ns-1": {ReplicationFactor: 3},
			},
		},
	})
	require.NoError(t, err)

	loadedStatus := backend.LoadStatus()
	require.NotNil(t, loadedStatus)
	require.NotNil(t, loadedStatus.Value)
	require.Contains(t, loadedStatus.Value.Namespaces, "ns-1")
	assert.EqualValues(t, 3, loadedStatus.Value.Namespaces["ns-1"].ReplicationFactor)

	configMap, err := client.CoreV1().ConfigMaps("default").Get(context.Background(), "metadata-conf", metav1.GetOptions{})
	require.NoError(t, err)
	configMap.Data[documentDataKey] = "{\n  \"allowedExtraAuthorities\": [\n    \"remote\"\n  ]\n}\n"
	_, err = client.CoreV1().ConfigMaps("default").Update(context.Background(), configMap, metav1.UpdateOptions{})
	require.NoError(t, err)

	_, err = backend.CommitConfig(&document.Versioned[*metadatapb.Cluster]{
		Version: loadedConfig.Version,
		Value: &metadatapb.Cluster{
			AllowedExtraAuthorities: []string{"local"},
		},
	})
	require.ErrorIs(t, err, document.ErrBadVersion)

	reloaded := backend.LoadConfig()
	require.NotNil(t, reloaded)
	require.NotNil(t, reloaded.Value)
	assert.Equal(t, []string{"remote"}, reloaded.Value.AllowedExtraAuthorities)

	_, err = backend.CommitConfig(&document.Versioned[*metadatapb.Cluster]{
		Version: reloaded.Version,
		Value: &metadatapb.Cluster{
			AllowedExtraAuthorities: []string{"final"},
		},
	})
	require.NoError(t, err)

	finalConfig := backend.LoadConfig()
	require.NotNil(t, finalConfig)
	require.NotNil(t, finalConfig.Value)
	assert.Equal(t, []string{"final"}, finalConfig.Value.AllowedExtraAuthorities)
}

func TestStoreLeaseHandoff(t *testing.T) {
	restoreLeaseTimings(t, 1500*time.Millisecond, time.Second, 200*time.Millisecond)

	client := newVersionedClientset()
	primary := NewStore(context.Background(), client, "default", "metadata")
	primaryClosed := false
	t.Cleanup(func() {
		if !primaryClosed {
			assert.NoError(t, primary.Close())
		}
	})

	waitForLeaseState(t, primary, metadatapb.LeaseState_LEASE_STATE_HELD)
	require.NoError(t, primary.CreateNamespaces([]*metadatapb.Namespace{{Name: "primary"}}))

	secondary := NewStore(context.Background(), client, "default", "metadata")
	t.Cleanup(func() {
		assert.NoError(t, secondary.Close())
	})

	state, _ := secondary.LeaseWatch().Load()
	assert.Equal(t, metadatapb.LeaseState_LEASE_STATE_UNHELD, state)
	assert.ErrorIs(t, secondary.CreateNamespaces([]*metadatapb.Namespace{{Name: "secondary"}}), metadata_v2.ErrLeaseNotHeld)

	require.NoError(t, primary.Close())
	primaryClosed = true

	waitForLeaseState(t, secondary, metadatapb.LeaseState_LEASE_STATE_HELD)
	require.NoError(t, secondary.CreateNamespaces([]*metadatapb.Namespace{{Name: "secondary"}}))

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

	require.NoError(t, backend.RevalidateLease())

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
	err = backend.RevalidateLease()
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
	err = backend.RevalidateLease()
	require.ErrorIs(t, err, metadata_v2.ErrLeaseNotHeld)
	assert.True(t, conflicted.Load())

	state, _ := backend.LeaseWatch().Load()
	assert.Equal(t, metadatapb.LeaseState_LEASE_STATE_UNHELD, state)
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
		leaseLock:  newBackendLeaseLock(client, "default", "metadata-lease", identity),
		leaseWatch: commonoption.NewWatch(metadatapb.LeaseState_LEASE_STATE_HELD),
	}
	_, _, err := backend.leaseLock.Get(context.Background())
	require.NoError(t, err)
	return backend
}

func ptr[T any](v T) *T {
	return &v
}
