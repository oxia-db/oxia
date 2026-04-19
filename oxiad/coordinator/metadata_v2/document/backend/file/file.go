package file

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/juju/fslock"
	"github.com/oxia-db/oxia/common/process"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata_v2/document/backend"
	"github.com/oxia-db/oxia/oxiad/coordinator/option"
	"google.golang.org/protobuf/encoding/protojson"
	gproto "google.golang.org/protobuf/proto"

	metadatapb "github.com/oxia-db/oxia/common/proto/metadata"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
)

const leaseRetryInterval = 100 * time.Millisecond

const defaultLockFileName = "lease.lock"

type Backend struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	dir           string
	leaseLockPath string
	leaseLock     *fslock.Lock
	leaseWatch    *commonoption.Watch[metadatapb.LeaseState]
}

func NewBackend(ctx context.Context, options option.FileMetadata) *Backend {
	// CODEX: ensure the dir exist.
	backendCtx, cancel := context.WithCancel(ctx)
	b := &Backend{
		ctx:           backendCtx,
		cancel:        cancel,
		dir:           options.Path,
		leaseLockPath: filepath.Join(options.Path, defaultLockFileName),
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

	if err := readProtoJSONFile(b.recordPath(name), msg); err != nil {
		return nil
	}
	return &backend.Versioned[gproto.Message]{
		Value: msg,
	}
}

func (b *Backend) Store(name backend.MetaRecordName, record *backend.Versioned[gproto.Message]) error {
	var msg gproto.Message
	var ok bool
	switch name {
	case backend.ConfigRecordName:
		msg, ok = record.Value.(*metadatapb.Cluster)
		if !ok {
			panic(fmt.Sprintf("unexpected metadata record type for %q: %T", name, record.Value))
		}
	case backend.StatusRecordName:
		msg, ok = record.Value.(*metadatapb.ClusterState)
		if !ok {
			panic(fmt.Sprintf("unexpected metadata record type for %q: %T", name, record.Value))
		}
	default:
		panic(fmt.Sprintf("unknown metadata record %q", name))
	}
	if err := writeProtoJSONFile(b.recordPath(name), msg); err != nil {
		return err
	}
	record.Value = gproto.Clone(msg)
	return nil
}

func readProtoJSONFile(path string, msg gproto.Message) error {
	content, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	if len(bytes.TrimSpace(content)) == 0 {
		return nil
	}

	return protojson.UnmarshalOptions{
		DiscardUnknown: true,
	}.Unmarshal(content, msg)
}

func writeProtoJSONFile(path string, msg gproto.Message) error {
	content, err := protojson.MarshalOptions{
		Indent:    "  ",
		Multiline: true,
	}.Marshal(msg)
	if err != nil {
		return err
	}
	content = append(content, '\n')

	dir := filepath.Dir(path)
	tmpFile, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}

	tmpName := tmpFile.Name()
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpName)
	}()

	if _, err := tmpFile.Write(content); err != nil {
		return err
	}
	if err := tmpFile.Chmod(0o600); err != nil {
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	return nil
}

// CODEX: inline this method
func (b *Backend) recordPath(name backend.MetaRecordName) string {
	return filepath.Join(b.dir, string(name))
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
