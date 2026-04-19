package kubernetes

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"google.golang.org/protobuf/encoding/protojson"
	gproto "google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kubernetesclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"github.com/oxia-db/oxia/common/process"
	metadatapb "github.com/oxia-db/oxia/common/proto/metadata"
	oxiatime "github.com/oxia-db/oxia/common/time"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
	coordinatormetadata "github.com/oxia-db/oxia/oxiad/coordinator/metadata"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata_v2"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata_v2/document"
)

var _ document.Backend = (*Backend)(nil)
var _ resourcelock.Interface = (*Backend)(nil)

var (
	leaseDuration     = 15 * time.Second
	renewDeadline     = 10 * time.Second
	retryPeriod       = 2 * time.Second
	k8sRequestTimeout = 30 * time.Second
)

const (
	documentDataKey  = "document"
	defaultLeaseName = "metadata-lease"
	fieldManager     = "oxia-coordinator"
)

type Store = document.Store

type Backend struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	client    kubernetesclient.Interface
	namespace string
	identity  string

	leaseName   string
	leaseLock   *resourcelock.LeaseLock
	leaseRecord *resourcelock.LeaderElectionRecord
	leaseWatch  *commonoption.Watch[metadatapb.LeaseState]
	leaseMutex  sync.Mutex
}

func NewStore(ctx context.Context, namespace string, identity string) *Store {
	return document.NewStore(ctx, newBackend(ctx, coordinatormetadata.NewK8SClientset(coordinatormetadata.NewK8SClientConfig()), namespace, identity))
}

func NewBackend(ctx context.Context, namespace string, identity string) *Backend {
	return newBackend(ctx, coordinatormetadata.NewK8SClientset(coordinatormetadata.NewK8SClientConfig()), namespace, identity)
}

func newBackend(ctx context.Context, client kubernetesclient.Interface, namespace string, identity string) *Backend {
	backendCtx, cancel := context.WithCancel(ctx)
	b := &Backend{
		ctx:        backendCtx,
		cancel:     cancel,
		client:     client,
		namespace:  namespace,
		identity:   identity,
		leaseName:  defaultLeaseName,
		leaseWatch: commonoption.NewWatch(metadatapb.LeaseState_LEASE_STATE_UNHELD),
	}
	b.leaseLock = &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      b.leaseName,
			Namespace: b.namespace,
		},
		Client: client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: b.identity,
		},
	}

	b.wg.Go(func() {
		process.DoWithLabels(b.ctx, map[string]string{
			"oxia": "metadata-v2-document-kubernetes-lease",
		}, b.runLeaseLoop)
	})

	return b
}

func (b *Backend) Close() error {
	b.cancel()
	b.wg.Wait()
	return nil
}

func (b *Backend) LeaseWatch() *commonoption.Watch[metadatapb.LeaseState] {
	return b.leaseWatch
}

func (b *Backend) LeaseRevalidate() error {
	state, _ := b.leaseWatch.Load()
	if state != metadatapb.LeaseState_LEASE_STATE_HELD {
		b.setLeaseState(metadatapb.LeaseState_LEASE_STATE_UNHELD)
		return metadata_v2.ErrLeaseNotHeld
	}

	ctx, cancel := b.requestContext()
	defer cancel()

	b.leaseMutex.Lock()
	defer b.leaseMutex.Unlock()

	if b.leaseRecord == nil {
		b.setLeaseState(metadatapb.LeaseState_LEASE_STATE_UNHELD)
		return metadata_v2.ErrLeaseNotHeld
	}

	record := cloneLeaderElectionRecord(b.leaseRecord)
	if record.HolderIdentity != b.identity {
		b.setLeaseState(metadatapb.LeaseState_LEASE_STATE_UNHELD)
		return metadata_v2.ErrLeaseNotHeld
	}

	record.HolderIdentity = b.identity
	record.RenewTime = metav1.Now()
	if record.LeaseDurationSeconds == 0 {
		record.LeaseDurationSeconds = int(leaseDuration / time.Second)
	}

	err := backoff.Retry(func() error {
		err := b.leaseLock.Update(ctx, *record)
		if err == nil {
			return nil
		}
		if k8serrors.IsConflict(err) || k8serrors.IsNotFound(err) {
			return backoff.Permanent(err)
		}
		return err
	}, oxiatime.NewBackOff(ctx))
	if err == nil {
		b.leaseRecord = cloneLeaderElectionRecord(record)
		b.setLeaseState(metadatapb.LeaseState_LEASE_STATE_HELD)
		return nil
	}
	if k8serrors.IsConflict(err) || k8serrors.IsNotFound(err) || err == metadata_v2.ErrLeaseNotHeld {
		b.setLeaseState(metadatapb.LeaseState_LEASE_STATE_UNHELD)
		return metadata_v2.ErrLeaseNotHeld
	}
	return err
}

func (b *Backend) Load(name document.MetaRecordName) *document.Versioned[gproto.Message] {
	configMapName := string(name)
	msg := b.newRecordMessage(name)

	ctx, cancel := b.requestContext()
	defer cancel()

	var cm *corev1.ConfigMap
	err := backoff.Retry(func() error {
		var err error
		cm, err = b.client.CoreV1().ConfigMaps(b.namespace).Get(ctx, configMapName, metav1.GetOptions{})
		if err == nil {
			return nil
		}
		if k8serrors.IsNotFound(err) {
			return backoff.Permanent(err)
		}
		return err
	}, oxiatime.NewBackOff(ctx))
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		return nil
	}

	payload := ""
	if cm.Data != nil {
		payload = cm.Data[documentDataKey]
	}
	if len(bytes.TrimSpace([]byte(payload))) == 0 {
		return &document.Versioned[gproto.Message]{
			Version: cm.ResourceVersion,
			Value:   msg,
		}
	}

	if err := (protojson.UnmarshalOptions{
		DiscardUnknown: true,
	}).Unmarshal([]byte(payload), msg); err != nil {
		return &document.Versioned[gproto.Message]{
			Version: cm.ResourceVersion,
			Value:   msg,
		}
	}
	return &document.Versioned[gproto.Message]{
		Version: cm.ResourceVersion,
		Value:   msg,
	}
}

func (b *Backend) Store(name document.MetaRecordName, record *document.Versioned[gproto.Message]) error {
	configMapName := string(name)

	content, err := protojson.MarshalOptions{
		Indent:    "  ",
		Multiline: true,
	}.Marshal(record.Value)
	if err != nil {
		return err
	}
	content = append(content, '\n')

	ctx, cancel := b.requestContext()
	defer cancel()

	cm := b.makeDesiredConfigMap(configMapName, string(content), record.Version)

	var updated *corev1.ConfigMap
	patch, err := json.Marshal(cm)
	if err != nil {
		return err
	}

	force := true
	err = backoff.Retry(func() error {
		var err error
		updated, err = b.client.CoreV1().ConfigMaps(b.namespace).Patch(ctx, configMapName, types.ApplyPatchType, patch, metav1.PatchOptions{
			FieldManager: fieldManager,
			Force:        &force,
		})
		if err == nil {
			return nil
		}
		if k8serrors.IsConflict(err) {
			return backoff.Permanent(err)
		}
		if k8serrors.IsNotFound(err) {
			if record.Version != "" {
				return backoff.Permanent(err)
			}

			updated, err = b.client.CoreV1().ConfigMaps(b.namespace).Create(ctx, cm, metav1.CreateOptions{})
			if err == nil {
				return nil
			}
			if k8serrors.IsAlreadyExists(err) || k8serrors.IsConflict(err) {
				return backoff.Permanent(err)
			}
		}
		return err
	}, oxiatime.NewBackOff(ctx))
	if k8serrors.IsConflict(err) || k8serrors.IsAlreadyExists(err) || (record.Version != "" && k8serrors.IsNotFound(err)) {
		return document.ErrBadVersion
	}
	if err != nil {
		return err
	}

	record.Version = updated.ResourceVersion
	record.Value = gproto.Clone(record.Value)
	return nil
}

func (b *Backend) makeDesiredConfigMap(name string, payload string, version string) *corev1.ConfigMap {
	cm := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: b.namespace,
		},
		Data: map[string]string{
			documentDataKey: payload,
		},
	}
	if version != "" {
		cm.ResourceVersion = version
	}
	return cm
}

func (b *Backend) newRecordMessage(name document.MetaRecordName) gproto.Message {
	switch name {
	case document.ConfigRecordName:
		return &metadatapb.Cluster{}
	case document.StatusRecordName:
		return &metadatapb.ClusterState{}
	default:
		panic(fmt.Sprintf("unknown metadata record %q", name))
	}
}

func (b *Backend) runLeaseLoop() {
	for {
		select {
		case <-b.ctx.Done():
			b.setLeaseState(metadatapb.LeaseState_LEASE_STATE_UNHELD)
			return
		default:
		}

		b.newLeaderElector().Run(b.ctx)
		if b.ctx.Err() != nil {
			b.setLeaseState(metadatapb.LeaseState_LEASE_STATE_UNHELD)
			return
		}
	}
}

func (b *Backend) newLeaderElector() *leaderelection.LeaderElector {
	leaderElector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:            b,
		ReleaseOnCancel: true,
		LeaseDuration:   leaseDuration,
		RenewDeadline:   renewDeadline,
		RetryPeriod:     retryPeriod,
		Name:            b.leaseName,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(context.Context) {
				b.setLeaseState(metadatapb.LeaseState_LEASE_STATE_HELD)
			},
			OnStoppedLeading: func() {
				b.setLeaseState(metadatapb.LeaseState_LEASE_STATE_UNHELD)
			},
			OnNewLeader: func(string) {},
		},
	})
	if err != nil {
		panic(err)
	}
	return leaderElector
}

func (b *Backend) requestContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(b.ctx, k8sRequestTimeout)
}

func (b *Backend) Get(ctx context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	b.leaseMutex.Lock()
	defer b.leaseMutex.Unlock()

	var (
		record *resourcelock.LeaderElectionRecord
		raw    []byte
	)
	err := backoff.Retry(func() error {
		var err error
		record, raw, err = b.leaseLock.Get(ctx)
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

	b.leaseRecord = cloneLeaderElectionRecord(record)
	return record, raw, nil
}

func (b *Backend) Create(ctx context.Context, record resourcelock.LeaderElectionRecord) error {
	b.leaseMutex.Lock()
	defer b.leaseMutex.Unlock()

	err := backoff.Retry(func() error {
		err := b.leaseLock.Create(ctx, record)
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

	b.leaseRecord = cloneLeaderElectionRecord(&record)
	return nil
}

func (b *Backend) Update(ctx context.Context, record resourcelock.LeaderElectionRecord) error {
	b.leaseMutex.Lock()
	defer b.leaseMutex.Unlock()

	err := backoff.Retry(func() error {
		err := b.leaseLock.Update(ctx, record)
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

	b.leaseRecord = cloneLeaderElectionRecord(&record)
	return nil
}

func (b *Backend) RecordEvent(message string) {
	b.leaseMutex.Lock()
	defer b.leaseMutex.Unlock()
	b.leaseLock.RecordEvent(message)
}

func (b *Backend) Identity() string {
	return b.identity
}

func (b *Backend) Describe() string {
	return b.leaseLock.Describe()
}

func (b *Backend) setLeaseState(state metadatapb.LeaseState) {
	current, _ := b.leaseWatch.Load()
	if current != state {
		b.leaseWatch.Notify(state)
	}
}

func cloneLeaderElectionRecord(record *resourcelock.LeaderElectionRecord) *resourcelock.LeaderElectionRecord {
	if record == nil {
		return nil
	}
	cloned := *record
	return &cloned
}
