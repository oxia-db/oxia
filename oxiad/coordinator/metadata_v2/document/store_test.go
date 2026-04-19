package document

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	metadatapb "github.com/oxia-db/oxia/common/proto/metadata"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata_v2"
	gproto "google.golang.org/protobuf/proto"
)

type fakeBackend struct {
	mu sync.Mutex

	leaseWatch *commonoption.Watch[metadatapb.LeaseState]
	config     *Versioned[*metadatapb.Cluster]
	status     *Versioned[*metadatapb.ClusterState]

	leaseRevalidateFn func() error
	storeConfigFn     func(*Versioned[*metadatapb.Cluster]) error
	storeStatusFn     func(*Versioned[*metadatapb.ClusterState]) error

	leaseRevalidateCalls int
	storeConfigCalls     int
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{
		leaseWatch: commonoption.NewWatch(metadatapb.LeaseState_LEASE_STATE_HELD),
		config:     &Versioned[*metadatapb.Cluster]{Value: &metadatapb.Cluster{}},
		status:     &Versioned[*metadatapb.ClusterState]{Value: &metadatapb.ClusterState{}},
	}
}

func (b *fakeBackend) Close() error {
	return nil
}

func (b *fakeBackend) LeaseWatch() *commonoption.Watch[metadatapb.LeaseState] {
	return b.leaseWatch
}

func (b *fakeBackend) LeaseRevalidate() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.leaseRevalidateCalls++
	if b.leaseRevalidateFn != nil {
		return b.leaseRevalidateFn()
	}
	return nil
}

func (b *fakeBackend) Load(name MetaRecordName) *Versioned[gproto.Message] {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch name {
	case ConfigRecordName:
		return cloneVersionedProto(&Versioned[gproto.Message]{
			Version: b.config.Version,
			Value:   b.config.Value,
		})
	case StatusRecordName:
		return cloneVersionedProto(&Versioned[gproto.Message]{
			Version: b.status.Version,
			Value:   b.status.Value,
		})
	default:
		panic(fmt.Sprintf("unknown metadata record %q", name))
	}
}

func (b *fakeBackend) Store(name MetaRecordName, record *Versioned[gproto.Message]) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch name {
	case ConfigRecordName:
		config := typedVersioned[*metadatapb.Cluster](record, name)
		b.storeConfigCalls++
		if b.storeConfigFn != nil {
			if err := b.storeConfigFn(config); err != nil {
				return err
			}
		} else {
			b.config = cloneVersionedProto(config)
		}
		record.Version = config.Version
		return nil
	case StatusRecordName:
		status := typedVersioned[*metadatapb.ClusterState](record, name)
		if b.storeStatusFn != nil {
			if err := b.storeStatusFn(status); err != nil {
				return err
			}
		} else {
			b.status = cloneVersionedProto(status)
		}
		record.Version = status.Version
		return nil
	default:
		panic(fmt.Sprintf("unknown metadata record %q", name))
	}
}

func TestStoreRetriesBadVersionAfterLeaseRevalidation(t *testing.T) {
	backend := newFakeBackend()
	backend.config = &Versioned[*metadatapb.Cluster]{
		Version: "1",
		Value: &metadatapb.Cluster{
			AllowedExtraAuthorities: []string{"base"},
		},
	}
	backend.storeConfigFn = func(config *Versioned[*metadatapb.Cluster]) error {
		if b := backend.storeConfigCalls; b == 1 {
			backend.config = &Versioned[*metadatapb.Cluster]{
				Version: "2",
				Value: &metadatapb.Cluster{
					AllowedExtraAuthorities: []string{"base", "remote"},
				},
			}
			return ErrBadVersion
		}
		backend.config = cloneVersionedProto(config)
		config.Version = "3"
		backend.config.Version = "3"
		return nil
	}

	store := NewStore(context.Background(), backend)
	t.Cleanup(func() {
		assert.NoError(t, store.Close())
	})
	backend.leaseWatch.Notify(metadatapb.LeaseState_LEASE_STATE_HELD)

	require.NoError(t, waitForLeaseReadyError(func() error {
		return store.AddExtraAllowedAuthorities([]string{"local"})
	}))
	assert.Equal(t, []string{"base", "remote", "local"}, store.GetAllowedAuthorities())
	assert.Equal(t, 1, backend.leaseRevalidateCalls)
	assert.Equal(t, 2, backend.storeConfigCalls)
}

func TestStoreMarksLeaseUnheldWhenBadVersionRevalidationFails(t *testing.T) {
	backend := newFakeBackend()
	backend.config = &Versioned[*metadatapb.Cluster]{
		Version: "1",
		Value: &metadatapb.Cluster{
			AllowedExtraAuthorities: []string{"base"},
		},
	}
	backend.storeConfigFn = func(*Versioned[*metadatapb.Cluster]) error {
		return ErrBadVersion
	}
	backend.leaseRevalidateFn = func() error {
		backend.leaseWatch.Notify(metadatapb.LeaseState_LEASE_STATE_UNHELD)
		return metadata_v2.ErrLeaseNotHeld
	}

	store := NewStore(context.Background(), backend)
	t.Cleanup(func() {
		assert.NoError(t, store.Close())
	})
	backend.leaseWatch.Notify(metadatapb.LeaseState_LEASE_STATE_HELD)

	err := waitForLeaseReadyError(func() error {
		return store.AddExtraAllowedAuthorities([]string{"local"})
	})
	require.ErrorIs(t, err, metadata_v2.ErrLeaseNotHeld)

	state, _ := store.LeaseWatch().Load()
	assert.Equal(t, metadatapb.LeaseState_LEASE_STATE_UNHELD, state)
	assert.Equal(t, 1, backend.leaseRevalidateCalls)

	err = store.AddExtraAllowedAuthorities([]string{"after"})
	require.ErrorIs(t, err, metadata_v2.ErrLeaseNotHeld)
}

func TestStoreDoesNotExposeUncommittedConfigStateOnCommitFailure(t *testing.T) {
	backend := newFakeBackend()
	backend.config = &Versioned[*metadatapb.Cluster]{
		Version: "1",
		Value: &metadatapb.Cluster{
			AllowedExtraAuthorities: []string{"base"},
		},
	}
	backend.storeConfigFn = func(*Versioned[*metadatapb.Cluster]) error {
		return assert.AnError
	}

	store := NewStore(context.Background(), backend)
	t.Cleanup(func() {
		assert.NoError(t, store.Close())
	})
	backend.leaseWatch.Notify(metadatapb.LeaseState_LEASE_STATE_HELD)

	err := waitForLeaseReadyError(func() error {
		return store.AddExtraAllowedAuthorities([]string{"local"})
	})
	require.ErrorIs(t, err, assert.AnError)
	assert.Equal(t, []string{"base"}, store.GetAllowedAuthorities())
}

func waitForLeaseReadyError(op func() error) error {
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		lastErr = op()
		if !errors.Is(lastErr, metadata_v2.ErrLeaseNotHeld) {
			return lastErr
		}
		time.Sleep(10 * time.Millisecond)
	}
	return lastErr
}
