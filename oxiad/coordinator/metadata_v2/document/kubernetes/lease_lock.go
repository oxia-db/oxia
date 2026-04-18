package kubernetes

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubernetesclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	oxiatime "github.com/oxia-db/oxia/common/time"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata_v2"
)

type backendLeaseLock struct {
	mu sync.Mutex

	lock     *resourcelock.LeaseLock
	identity string
	record   *resourcelock.LeaderElectionRecord
}

func newBackendLeaseLock(client kubernetesclient.Interface, namespace string, name string, identity string) *backendLeaseLock {
	return &backendLeaseLock{
		lock: &resourcelock.LeaseLock{
			LeaseMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
			Client: client.CoordinationV1(),
			LockConfig: resourcelock.ResourceLockConfig{
				Identity: identity,
			},
		},
		identity: identity,
	}
}

func (l *backendLeaseLock) Get(ctx context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	var record *resourcelock.LeaderElectionRecord
	var raw []byte
	err := backoff.Retry(func() error {
		var err error
		record, raw, err = l.lock.Get(ctx)
		if err == nil {
			return nil
		}
		if k8serrors.IsNotFound(err) {
			return backoff.Permanent(err)
		}
		return err
	}, oxiatime.NewBackOff(ctx))
	if err != nil {
		return nil, nil, err
	}

	l.record = cloneLeaderElectionRecord(record)
	return record, raw, nil
}

func (l *backendLeaseLock) Create(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	err := backoff.Retry(func() error {
		err := l.lock.Create(ctx, ler)
		if err == nil {
			return nil
		}
		if k8serrors.IsAlreadyExists(err) || k8serrors.IsConflict(err) {
			return backoff.Permanent(err)
		}
		return err
	}, oxiatime.NewBackOff(ctx))
	if err != nil {
		return err
	}

	l.record = cloneLeaderElectionRecord(&ler)
	return nil
}

func (l *backendLeaseLock) Update(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	err := backoff.Retry(func() error {
		err := l.lock.Update(ctx, ler)
		if err == nil {
			return nil
		}
		if k8serrors.IsConflict(err) || k8serrors.IsNotFound(err) {
			return backoff.Permanent(err)
		}
		return err
	}, oxiatime.NewBackOff(ctx))
	if err != nil {
		return err
	}

	l.record = cloneLeaderElectionRecord(&ler)
	return nil
}

func (l *backendLeaseLock) RecordEvent(s string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lock.RecordEvent(s)
}

func (l *backendLeaseLock) Identity() string {
	return l.identity
}

func (l *backendLeaseLock) Describe() string {
	return l.lock.Describe()
}

func (l *backendLeaseLock) Revalidate(ctx context.Context, duration time.Duration) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.record == nil {
		return metadata_v2.ErrLeaseNotHeld
	}

	record := cloneLeaderElectionRecord(l.record)
	if record.HolderIdentity != l.identity {
		return metadata_v2.ErrLeaseNotHeld
	}

	record.HolderIdentity = l.identity
	record.RenewTime = metav1.Now()
	if record.LeaseDurationSeconds == 0 {
		record.LeaseDurationSeconds = int(duration / time.Second)
	}

	err := backoff.Retry(func() error {
		err := l.lock.Update(ctx, *record)
		if err == nil {
			return nil
		}
		if k8serrors.IsConflict(err) || k8serrors.IsNotFound(err) {
			return backoff.Permanent(err)
		}
		return err
	}, oxiatime.NewBackOff(ctx))
	if err != nil {
		return err
	}

	l.record = cloneLeaderElectionRecord(record)
	return nil
}

func cloneLeaderElectionRecord(record *resourcelock.LeaderElectionRecord) *resourcelock.LeaderElectionRecord {
	if record == nil {
		return nil
	}
	cloned := *record
	return &cloned
}
