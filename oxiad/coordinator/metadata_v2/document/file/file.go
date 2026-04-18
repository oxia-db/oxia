package file

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/juju/fslock"
	"github.com/oxia-db/oxia/common/process"
	gproto "google.golang.org/protobuf/proto"

	metadatapb "github.com/oxia-db/oxia/common/proto/metadata"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata_v2"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata_v2/document"
)

var _ document.Backend = (*Backend)(nil)

const leaseRetryInterval = 100 * time.Millisecond

const (
	defaultConfigFileName = "conf"
	defaultStateFileName  = "status"
	defaultLockFileName   = "lease.lock"
)

type Store = document.Store

type Backend struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	configPath string
	statePath  string

	leaseLockPath string
	leaseLock     *fslock.Lock
	leaseWatch    *commonoption.Watch[metadatapb.LeaseState]
}

func NewStore(ctx context.Context, dir string) *Store {
	return document.NewStore(ctx, NewBackend(ctx, dir))
}

func NewBackend(ctx context.Context, dir string) *Backend {
	if dir == "" {
		panic(fmt.Errorf("%w: metadata document file dir is required", metadata_v2.ErrInvalidInput))
	}

	dir = filepath.Clean(dir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		panic(err)
	}

	backendCtx, cancel := context.WithCancel(ctx)
	b := &Backend{
		ctx:           backendCtx,
		cancel:        cancel,
		configPath:    filepath.Join(dir, defaultConfigFileName),
		statePath:     filepath.Join(dir, defaultStateFileName),
		leaseLockPath: filepath.Join(dir, defaultLockFileName),
		leaseWatch:    commonoption.NewWatch(metadatapb.LeaseState_LEASE_STATE_UNHELD),
	}

	b.wg.Go(func() {
		process.DoWithLabels(b.ctx, map[string]string{
			"oxia": "metadata-v2-document-file-lease",
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

func (b *Backend) LoadConfig() *document.Versioned[*metadatapb.Cluster] {
	cluster := &metadatapb.Cluster{}
	if err := metadata_v2.ReadProtoJSONFile(b.configPath, cluster); err != nil {
		return nil
	}
	return &document.Versioned[*metadatapb.Cluster]{
		Value: cluster,
	}
}

func (b *Backend) CommitConfig(config *document.Versioned[*metadatapb.Cluster]) (*document.Versioned[*metadatapb.Cluster], error) {
	if err := metadata_v2.WriteProtoJSONFile(b.configPath, config.Value); err != nil {
		return nil, err
	}
	return &document.Versioned[*metadatapb.Cluster]{
		Value: gproto.CloneOf(config.Value),
	}, nil
}

func (b *Backend) LoadStatus() *document.Versioned[*metadatapb.ClusterState] {
	status := &metadatapb.ClusterState{}
	if err := metadata_v2.ReadProtoJSONFile(b.statePath, status); err != nil {
		return nil
	}
	return &document.Versioned[*metadatapb.ClusterState]{
		Value: status,
	}
}

func (b *Backend) CommitStatus(status *document.Versioned[*metadatapb.ClusterState]) (*document.Versioned[*metadatapb.ClusterState], error) {
	if err := metadata_v2.WriteProtoJSONFile(b.statePath, status.Value); err != nil {
		return nil, err
	}
	return &document.Versioned[*metadatapb.ClusterState]{
		Value: gproto.CloneOf(status.Value),
	}, nil
}

func (b *Backend) RevalidateLease() error {
	if b.ctx.Err() != nil {
		return metadata_v2.ErrLeaseNotHeld
	}
	state, _ := b.leaseWatch.Load()
	if state != metadatapb.LeaseState_LEASE_STATE_HELD {
		return metadata_v2.ErrLeaseNotHeld
	}
	return nil
}

func (b *Backend) runLeaseLoop() {
	for {
		select {
		case <-b.ctx.Done():
			b.setLeaseState(metadatapb.LeaseState_LEASE_STATE_UNHELD)
			return
		default:
		}

		leaseLock := fslock.New(b.leaseLockPath)
		if err := leaseLock.TryLock(); err != nil {
			b.setLeaseState(metadatapb.LeaseState_LEASE_STATE_UNHELD)
			select {
			case <-b.ctx.Done():
				b.setLeaseState(metadatapb.LeaseState_LEASE_STATE_UNHELD)
				return
			case <-time.After(leaseRetryInterval):
			}
			continue
		}

		b.leaseLock = leaseLock
		b.setLeaseState(metadatapb.LeaseState_LEASE_STATE_HELD)
		<-b.ctx.Done()
		b.setLeaseState(metadatapb.LeaseState_LEASE_STATE_UNHELD)
		_ = leaseLock.Unlock()
		b.leaseLock = nil
	}
}

func (b *Backend) setLeaseState(state metadatapb.LeaseState) {
	current, _ := b.leaseWatch.Load()
	if current != state {
		b.leaseWatch.Notify(state)
	}
}
