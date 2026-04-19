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

const defaultLockFileName = "lease.lock"

type Store = document.Store

type Backend struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	dir           string
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
	if err := ensurePrivateDir(dir); err != nil {
		panic(err)
	}

	backendCtx, cancel := context.WithCancel(ctx)
	b := &Backend{
		ctx:           backendCtx,
		cancel:        cancel,
		dir:           dir,
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

func ensurePrivateDir(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	return os.Chmod(dir, 0o700)
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
	if b.ctx.Err() != nil {
		return metadata_v2.ErrLeaseNotHeld
	}
	state, _ := b.leaseWatch.Load()
	if state != metadatapb.LeaseState_LEASE_STATE_HELD {
		return metadata_v2.ErrLeaseNotHeld
	}
	return nil
}

func (b *Backend) Load(name document.MetaRecordName) *document.Versioned[gproto.Message] {
	msg := b.newRecordMessage(name)
	if err := metadata_v2.ReadProtoJSONFile(b.recordPath(name), msg); err != nil {
		return nil
	}
	return &document.Versioned[gproto.Message]{
		Value: msg,
	}
}

func (b *Backend) Store(name document.MetaRecordName, record *document.Versioned[gproto.Message]) error {
	msg := b.assertRecordMessage(name, record.Value)
	if err := metadata_v2.WriteProtoJSONFile(b.recordPath(name), msg); err != nil {
		return err
	}
	record.Value = gproto.Clone(msg)
	return nil
}

func (b *Backend) recordPath(name document.MetaRecordName) string {
	return filepath.Join(b.dir, string(name))
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

func (b *Backend) assertRecordMessage(name document.MetaRecordName, value gproto.Message) gproto.Message {
	switch name {
	case document.ConfigRecordName:
		msg, ok := value.(*metadatapb.Cluster)
		if !ok {
			panic(fmt.Sprintf("unexpected metadata record type for %q: %T", name, value))
		}
		return msg
	case document.StatusRecordName:
		msg, ok := value.(*metadatapb.ClusterState)
		if !ok {
			panic(fmt.Sprintf("unexpected metadata record type for %q: %T", name, value))
		}
		return msg
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
