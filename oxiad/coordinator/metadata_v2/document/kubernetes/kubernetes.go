package kubernetes

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"google.golang.org/protobuf/encoding/protojson"
	gproto "google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubernetesclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"

	"github.com/oxia-db/oxia/common/process"
	metadatapb "github.com/oxia-db/oxia/common/proto/metadata"
	oxiatime "github.com/oxia-db/oxia/common/time"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata_v2"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata_v2/document"
)

var _ document.Backend = (*Backend)(nil)

var (
	leaseDuration     = 15 * time.Second
	renewDeadline     = 10 * time.Second
	retryPeriod       = 2 * time.Second
	k8sRequestTimeout = 30 * time.Second

	backendIdentitySeq atomic.Uint64
)

const (
	documentDataKey         = "document"
	defaultConfigNameSuffix = "-conf"
	defaultStatusNameSuffix = "-status"
	defaultLeaseNameSuffix  = "-lease"
)

type Store = document.Store

type Backend struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	client    kubernetesclient.Interface
	namespace string
	identity  string

	configName string
	statusName string

	leaseName       string
	leaseLock       *backendLeaseLock
	leaseWatch      *commonoption.Watch[metadatapb.LeaseState]
	revalidateMutex sync.Mutex
}

func NewStore(ctx context.Context, client kubernetesclient.Interface, namespace string, name string) *Store {
	return document.NewStore(ctx, NewBackend(ctx, client, namespace, name))
}

func NewBackend(ctx context.Context, client kubernetesclient.Interface, namespace string, name string) *Backend {
	backendCtx, cancel := context.WithCancel(ctx)
	b := &Backend{
		ctx:        backendCtx,
		cancel:     cancel,
		client:     client,
		namespace:  namespace,
		identity:   backendIdentity(),
		configName: name + defaultConfigNameSuffix,
		statusName: name + defaultStatusNameSuffix,
		leaseName:  name + defaultLeaseNameSuffix,
		leaseWatch: commonoption.NewWatch(metadatapb.LeaseState_LEASE_STATE_UNHELD),
	}
	b.leaseLock = newBackendLeaseLock(client, namespace, b.leaseName, b.identity)

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

func (b *Backend) RevalidateLease() error {
	state, _ := b.leaseWatch.Load()
	if state != metadatapb.LeaseState_LEASE_STATE_HELD {
		b.setLeaseState(metadatapb.LeaseState_LEASE_STATE_UNHELD)
		return metadata_v2.ErrLeaseNotHeld
	}

	b.revalidateMutex.Lock()
	defer b.revalidateMutex.Unlock()

	ctx, cancel := b.requestContext()
	defer cancel()

	err := b.leaseLock.Revalidate(ctx, leaseDuration)
	if err == nil {
		b.setLeaseState(metadatapb.LeaseState_LEASE_STATE_HELD)
		return nil
	}
	if k8serrors.IsConflict(err) || k8serrors.IsNotFound(err) || err == metadata_v2.ErrLeaseNotHeld {
		b.setLeaseState(metadatapb.LeaseState_LEASE_STATE_UNHELD)
		return metadata_v2.ErrLeaseNotHeld
	}
	return err
}

func (b *Backend) LoadConfig() *document.Versioned[*metadatapb.Cluster] {
	return loadDocument(b, b.configName, func() *metadatapb.Cluster {
		return &metadatapb.Cluster{}
	})
}

func (b *Backend) CommitConfig(config *document.Versioned[*metadatapb.Cluster]) (*document.Versioned[*metadatapb.Cluster], error) {
	return commitDocument(b, b.configName, config)
}

func (b *Backend) LoadStatus() *document.Versioned[*metadatapb.ClusterState] {
	return loadDocument(b, b.statusName, func() *metadatapb.ClusterState {
		return &metadatapb.ClusterState{}
	})
}

func (b *Backend) CommitStatus(status *document.Versioned[*metadatapb.ClusterState]) (*document.Versioned[*metadatapb.ClusterState], error) {
	return commitDocument(b, b.statusName, status)
}

func loadDocument[T gproto.Message](b *Backend, name string, newMessage func() T) *document.Versioned[T] {
	ctx, cancel := b.requestContext()
	defer cancel()

	var cm *corev1.ConfigMap
	err := backoff.Retry(func() error {
		var err error
		cm, err = b.client.CoreV1().ConfigMaps(b.namespace).Get(ctx, name, metav1.GetOptions{})
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
			return &document.Versioned[T]{}
		}
		return nil
	}

	msg := newMessage()
	payload := ""
	if cm.Data != nil {
		payload = cm.Data[documentDataKey]
	}
	if len(bytes.TrimSpace([]byte(payload))) == 0 {
		return &document.Versioned[T]{
			Version: cm.ResourceVersion,
			Value:   msg,
		}
	}

	if err := (protojson.UnmarshalOptions{
		DiscardUnknown: true,
	}).Unmarshal([]byte(payload), msg); err != nil {
		return &document.Versioned[T]{
			Version: cm.ResourceVersion,
			Value:   msg,
		}
	}
	return &document.Versioned[T]{
		Version: cm.ResourceVersion,
		Value:   msg,
	}
}

func commitDocument[T gproto.Message](b *Backend, name string, versioned *document.Versioned[T]) (*document.Versioned[T], error) {
	content, err := protojson.MarshalOptions{
		Indent:    "  ",
		Multiline: true,
	}.Marshal(versioned.Value)
	if err != nil {
		return nil, err
	}
	content = append(content, '\n')

	ctx, cancel := b.requestContext()
	defer cancel()

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       b.namespace,
			ResourceVersion: versioned.Version,
		},
		Data: map[string]string{
			documentDataKey: string(content),
		},
	}

	var updated *corev1.ConfigMap
	if versioned.Version == "" {
		err = backoff.Retry(func() error {
			var err error
			updated, err = b.client.CoreV1().ConfigMaps(b.namespace).Create(ctx, cm, metav1.CreateOptions{})
			if err == nil {
				return nil
			}
			if k8serrors.IsAlreadyExists(err) || k8serrors.IsConflict(err) {
				return backoff.Permanent(err)
			}
			return err
		}, oxiatime.NewBackOff(ctx))
		if k8serrors.IsAlreadyExists(err) || k8serrors.IsConflict(err) {
			return nil, document.ErrBadVersion
		}
	} else {
		err = backoff.Retry(func() error {
			var err error
			updated, err = b.client.CoreV1().ConfigMaps(b.namespace).Update(ctx, cm, metav1.UpdateOptions{})
			if err == nil {
				return nil
			}
			if k8serrors.IsConflict(err) || k8serrors.IsNotFound(err) {
				return backoff.Permanent(err)
			}
			return err
		}, oxiatime.NewBackOff(ctx))
		if k8serrors.IsConflict(err) || k8serrors.IsNotFound(err) {
			return nil, document.ErrBadVersion
		}
	}
	if err != nil {
		return nil, err
	}

	return &document.Versioned[T]{
		Version: updated.ResourceVersion,
		Value:   gproto.CloneOf(versioned.Value),
	}, nil
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
		Lock:            b.leaseLock,
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

func (b *Backend) setLeaseState(state metadatapb.LeaseState) {
	current, _ := b.leaseWatch.Load()
	if current != state {
		b.leaseWatch.Notify(state)
	}
}

func backendIdentity() string {
	host, _ := os.Hostname()
	if host == "" {
		host = "oxia"
	}
	return fmt.Sprintf("%s-%d-%d", host, os.Getpid(), backendIdentitySeq.Add(1))
}
