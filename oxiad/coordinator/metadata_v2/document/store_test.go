package document

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gproto "google.golang.org/protobuf/proto"

	metadatapb "github.com/oxia-db/oxia/common/proto/metadata"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata_v2"
)

type fakeBackend struct {
	mu sync.Mutex

	leaseWatch *commonoption.Watch[metadatapb.LeaseState]
	config     *metadatapb.Cluster
	status     *metadatapb.ClusterState

	revalidateLeaseFn func() error
	commitConfigFn    func(*metadatapb.Cluster) error
	commitStatusFn    func(*metadatapb.ClusterState) error

	revalidateLeaseCalls int
	commitConfigCalls    int
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{
		leaseWatch: commonoption.NewWatch(metadatapb.LeaseState_LEASE_STATE_HELD),
		config:     &metadatapb.Cluster{},
		status:     &metadatapb.ClusterState{},
	}
}

func (b *fakeBackend) Close() error {
	return nil
}

func (b *fakeBackend) LeaseWatch() *commonoption.Watch[metadatapb.LeaseState] {
	return b.leaseWatch
}

func (b *fakeBackend) RevalidateLease() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.revalidateLeaseCalls++
	if b.revalidateLeaseFn != nil {
		return b.revalidateLeaseFn()
	}
	return nil
}

func (b *fakeBackend) LoadConfig() *metadatapb.Cluster {
	b.mu.Lock()
	defer b.mu.Unlock()
	return gproto.CloneOf(b.config)
}

func (b *fakeBackend) CommitConfig(config *metadatapb.Cluster) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.commitConfigCalls++
	if b.commitConfigFn != nil {
		return b.commitConfigFn(config)
	}
	b.config = gproto.CloneOf(config)
	return nil
}

func (b *fakeBackend) LoadStatus() *metadatapb.ClusterState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return gproto.CloneOf(b.status)
}

func (b *fakeBackend) CommitStatus(status *metadatapb.ClusterState) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.commitStatusFn != nil {
		return b.commitStatusFn(status)
	}
	b.status = gproto.CloneOf(status)
	return nil
}

func TestStoreRetriesBadVersionAfterLeaseRevalidation(t *testing.T) {
	backend := newFakeBackend()
	backend.config = &metadatapb.Cluster{
		AllowedExtraAuthorities: []string{"base"},
	}
	backend.commitConfigFn = func(config *metadatapb.Cluster) error {
		if b := backend.commitConfigCalls; b == 1 {
			backend.config = &metadatapb.Cluster{
				AllowedExtraAuthorities: []string{"base", "remote"},
			}
			return ErrBadVersion
		}
		backend.config = gproto.CloneOf(config)
		return nil
	}

	store := NewStore(context.Background(), backend)
	t.Cleanup(func() {
		assert.NoError(t, store.Close())
	})

	require.NoError(t, store.AddExtraAllowedAuthorities([]string{"local"}))
	assert.Equal(t, []string{"base", "remote", "local"}, store.GetAllowedAuthorities())
	assert.Equal(t, 1, backend.revalidateLeaseCalls)
	assert.Equal(t, 2, backend.commitConfigCalls)
}

func TestStoreMarksLeaseUnheldWhenBadVersionRevalidationFails(t *testing.T) {
	backend := newFakeBackend()
	backend.config = &metadatapb.Cluster{
		AllowedExtraAuthorities: []string{"base"},
	}
	backend.commitConfigFn = func(*metadatapb.Cluster) error {
		return ErrBadVersion
	}
	backend.revalidateLeaseFn = func() error {
		backend.leaseWatch.Notify(metadatapb.LeaseState_LEASE_STATE_UNHELD)
		return metadata_v2.ErrLeaseNotHeld
	}

	store := NewStore(context.Background(), backend)
	t.Cleanup(func() {
		assert.NoError(t, store.Close())
	})

	err := store.AddExtraAllowedAuthorities([]string{"local"})
	require.ErrorIs(t, err, metadata_v2.ErrLeaseNotHeld)

	state, _ := store.LeaseWatch().Load()
	assert.Equal(t, metadatapb.LeaseState_LEASE_STATE_UNHELD, state)
	assert.Equal(t, 1, backend.revalidateLeaseCalls)

	err = store.AddExtraAllowedAuthorities([]string{"after"})
	require.ErrorIs(t, err, metadata_v2.ErrLeaseNotHeld)
}
