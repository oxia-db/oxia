package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata_v2/document/backend"
	"github.com/oxia-db/oxia/oxiad/coordinator/option"
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
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata_v2/document"
)

var (
	leaseDuration     = 15 * time.Second
	renewDeadline     = 10 * time.Second
	retryPeriod       = 2 * time.Second
	k8sRequestTimeout = 30 * time.Second
)

const (
	documentDataKey  = "document"
	defaultLeaseName = "metadata-lease"
	fieldManager     = "oxia-metadata"
)

type Backend struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	client    kubernetesclient.Interface
	namespace string
	identity  string

	leaseName string
	*resourcelock.LeaseLock
	leaseRecord *resourcelock.LeaderElectionRecord
	leaseWatch  *commonoption.Watch[metadatapb.LeaseState]
}

func NewBackend(ctx context.Context, kubernetes option.K8sMetadata) *Backend {
	client := coordinatormetadata.NewK8SClientset(coordinatormetadata.NewK8SClientConfig())
	backendCtx, cancel := context.WithCancel(ctx)
	var identity string
	var err error
	if identity, err = os.Hostname(); err != nil {
		identity = uuid.NewString()
	}
	b := &Backend{
		ctx:        backendCtx,
		cancel:     cancel,
		client:     client,
		namespace:  kubernetes.Namespace,
		identity:   identity,
		leaseName:  defaultLeaseName,
		leaseWatch: commonoption.NewWatch(metadatapb.LeaseState_LEASE_STATE_UNHELD),
	}
	b.LeaseLock = &resourcelock.LeaseLock{
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

func (b *Backend) Load(name backend.MetaRecordName) *backend.Versioned[gproto.Message] {
	var msg gproto.Message
	switch name {
	case backend.ConfigRecordName:
		msg = &metadatapb.Cluster{}
	case backend.StatusRecordName:
		msg = &metadatapb.ClusterState{}
	default:
		panic(fmt.Sprintf("unknown metadata record %q", name))
	}

	ctx, cancel := context.WithTimeout(b.ctx, k8sRequestTimeout)
	defer cancel()

	var cm *corev1.ConfigMap
	// CODEX: add retry logs
	_ = backoff.Retry(func() error {
		var err error
		cm, err = b.client.CoreV1().ConfigMaps(b.namespace).Get(ctx, string(name), metav1.GetOptions{})
		return err
	}, oxiatime.NewBackOff(ctx))

	payload := ""
	if cm.Data != nil {
		payload = cm.Data[documentDataKey]
	}
	if err := (protojson.UnmarshalOptions{
		DiscardUnknown: true,
	}).Unmarshal([]byte(payload), msg); err != nil {
		return &backend.Versioned[gproto.Message]{
			Version: cm.ResourceVersion,
			Value:   msg,
		}
	}
	return &backend.Versioned[gproto.Message]{
		Version: cm.ResourceVersion,
		Value:   msg,
	}
}

func (b *Backend) Store(name backend.MetaRecordName, record *backend.Versioned[gproto.Message]) error {
	content, err := protojson.MarshalOptions{
		Indent:    "  ",
		Multiline: true,
	}.Marshal(record.Value)
	if err != nil {
		return err
	}
	content = append(content, '\n')

	ctx, cancel := context.WithTimeout(b.ctx, k8sRequestTimeout)
	defer cancel()

	cm := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      string(name),
			Namespace: b.namespace,
		},
		Data: map[string]string{
			documentDataKey: string(content),
		},
	}
	if record.Version != "" {
		cm.ResourceVersion = record.Version
	}

	var updated *corev1.ConfigMap
	patch, err := json.Marshal(cm)
	if err != nil {
		return err
	}

	force := true
	err = backoff.Retry(func() error {
		var err error
		updated, err = b.client.CoreV1().ConfigMaps(b.namespace).Patch(ctx, string(name), types.ApplyPatchType, patch, metav1.PatchOptions{
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

func (b *Backend) runLeaseLoop() {
	for {
		select {
		case <-b.ctx.Done():
			b.setLeaseState(metadatapb.LeaseState_LEASE_STATE_UNHELD)
			return
		default:
		}

		leaderElector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
			Lock:            b,
			ReleaseOnCancel: true,
			LeaseDuration:   leaseDuration,
			RenewDeadline:   renewDeadline,
			RetryPeriod:     retryPeriod,
			Name:            b.leaseName,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(context.Context) {
					// CODEX: add logs
					b.setLeaseState(metadatapb.LeaseState_LEASE_STATE_HELD)
				},
				OnStoppedLeading: func() {
					// CODEX: add logs
					b.setLeaseState(metadatapb.LeaseState_LEASE_STATE_UNHELD)
				},
				OnNewLeader: func(string) {
					// CODEX: add logs
				},
			},
		})
		if err != nil {
			panic(err)
		}
		leaderElector.Run(b.ctx)
	}
}

func (b *Backend) setLeaseState(state metadatapb.LeaseState) {
	current, _ := b.leaseWatch.Load()
	if current != state {
		b.leaseWatch.Notify(state)
	}
}
