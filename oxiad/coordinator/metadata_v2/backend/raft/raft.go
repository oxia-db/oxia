package raft

import (
	"context"
	stderrors "errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	hashicorpraft "github.com/hashicorp/raft"
	"github.com/magodo/slog2hclog"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
	gproto "google.golang.org/protobuf/proto"

	"github.com/oxia-db/oxia/common/process"
	metadatapb "github.com/oxia-db/oxia/common/proto/metadata"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata_v2/backend"
	metadataerr "github.com/oxia-db/oxia/oxiad/coordinator/metadata_v2/error"
	"github.com/oxia-db/oxia/oxiad/coordinator/option"
)

const applyTimeout = 30 * time.Second

type Backend struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	log    *slog.Logger

	raft       *hashicorpraft.Raft
	store      *PebbleRaftStore
	state      *stateContainer
	leaseWatch *commonoption.Watch[metadatapb.LeaseState]
}

func NewBackend(ctx context.Context, options option.RaftMetadata) *Backend {
	backendCtx, cancel := context.WithCancel(ctx)
	b := &Backend{
		ctx:        backendCtx,
		cancel:     cancel,
		log:        slog.With(slog.String("component", "metadata-v2-raft")),
		state:      newStateContainer(),
		leaseWatch: commonoption.NewWatch(metadatapb.LeaseState_LEASE_STATE_UNHELD),
	}

	if err := b.init(options); err != nil {
		_ = b.Close()
		panic(err)
	}

	b.wg.Go(func() {
		process.DoWithLabels(b.ctx, map[string]string{
			"oxia": "metadata-v2-raft-lease",
		}, b.watchLeadership)
	})

	return b
}

func (b *Backend) init(options option.RaftMetadata) error {
	nodeID := options.Address
	dataDir := filepath.Join(options.DataDir, nodeID)
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return errors.Wrap(err, "failed to create raft data dir")
	}

	config := hashicorpraft.DefaultConfig()
	config.HeartbeatTimeout = 5 * time.Second
	config.ElectionTimeout = 10 * time.Second
	config.LocalID = hashicorpraft.ServerID(nodeID)
	config.LogLevel = "INFO"
	levelVar := &slog.LevelVar{}
	levelVar.Set(slog.LevelInfo)
	config.Logger = slog2hclog.New(b.log, levelVar)

	addr, err := net.ResolveTCPAddr("tcp", options.Address)
	if err != nil {
		return errors.Wrap(err, "failed to resolve raft address")
	}

	transport, err := hashicorpraft.NewTCPTransport(options.Address, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return errors.Wrap(err, "failed to create raft transport")
	}

	b.store, err = NewPebbleRaftStore(filepath.Join(dataDir, "store"))
	if err != nil {
		return errors.Wrap(err, "failed to create raft data store")
	}

	snapshotStore, err := hashicorpraft.NewFileSnapshotStoreWithLogger(dataDir, 2, config.Logger)
	if err != nil {
		return errors.Wrap(err, "failed to create raft snapshot store")
	}

	b.raft, err = hashicorpraft.NewRaft(config, b.state, b.store, b.store, snapshotStore, transport)
	if err != nil {
		return errors.Wrap(err, "failed to create raft node")
	}

	hasState, err := hashicorpraft.HasExistingState(b.store, b.store, snapshotStore)
	if err != nil {
		return errors.Wrap(err, "failed to check raft state")
	}
	if !hasState {
		servers := make([]hashicorpraft.Server, len(options.BootstrapNodes))
		for i, addr := range options.BootstrapNodes {
			servers[i] = hashicorpraft.Server{
				ID:      hashicorpraft.ServerID(addr),
				Address: hashicorpraft.ServerAddress(addr),
			}
		}
		future := b.raft.BootstrapCluster(hashicorpraft.Configuration{
			Servers: servers,
		})
		if err := future.Error(); err != nil {
			return errors.Wrap(err, "failed to bootstrap raft cluster")
		}
	}

	return nil
}

func (b *Backend) Close() error {
	b.cancel()
	b.wg.Wait()

	var raftErr error
	if b.raft != nil {
		raftErr = b.raft.Shutdown().Error()
	}
	var storeErr error
	if b.store != nil {
		storeErr = b.store.Close()
	}
	return multierr.Combine(raftErr, storeErr)
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

	record, ok := b.state.Load(name)
	if !ok {
		return &backend.Versioned[gproto.Message]{
			Value: msg,
		}
	}

	if len(record.Value) > 0 {
		if err := gproto.Unmarshal(record.Value, msg); err != nil {
			panic(fmt.Sprintf("failed to unmarshal raft metadata record %q: %v", name, err))
		}
	}

	return &backend.Versioned[gproto.Message]{
		Version: formatVersion(record.Version),
		Value:   msg,
	}
}

func (b *Backend) Store(name backend.MetaRecordName, record *backend.Versioned[gproto.Message]) error {
	payload, err := gproto.Marshal(record.Value)
	if err != nil {
		return err
	}

	future := b.raft.Apply(encodeRaftCommand(raftCommand{
		Name:            string(name),
		ExpectedVersion: record.Version,
		Value:           payload,
	}), applyTimeout)

	if err := future.Error(); err != nil {
		switch {
		case stderrors.Is(err, hashicorpraft.ErrNotLeader),
			stderrors.Is(err, hashicorpraft.ErrLeadershipLost),
			stderrors.Is(err, hashicorpraft.ErrLeadershipTransferInProgress),
			stderrors.Is(err, hashicorpraft.ErrRaftShutdown):
			return metadataerr.ErrLeaseNotHeld
		default:
			return err
		}
	}

	if applyErr, ok := future.Response().(error); ok && applyErr != nil {
		return applyErr
	}

	result, ok := future.Response().(storeResult)
	if !ok {
		return fmt.Errorf("unexpected raft apply response: %T", future.Response())
	}

	record.Version = result.Version
	record.Value = gproto.Clone(record.Value)
	return nil
}

func (b *Backend) watchLeadership() {
	if b.raft == nil || b.raft.State() != hashicorpraft.Leader {
		b.setLeaseState(metadatapb.LeaseState_LEASE_STATE_UNHELD)
	} else if err := b.raft.Barrier(applyTimeout).Error(); err != nil {
		b.log.Warn("failed to synchronize raft state before acquiring lease", slog.Any("error", err))
		b.setLeaseState(metadatapb.LeaseState_LEASE_STATE_UNHELD)
	} else if err := b.raft.VerifyLeader().Error(); err != nil {
		b.setLeaseState(metadatapb.LeaseState_LEASE_STATE_UNHELD)
	} else {
		b.setLeaseState(metadatapb.LeaseState_LEASE_STATE_HELD)
	}

	for {
		select {
		case <-b.ctx.Done():
			b.setLeaseState(metadatapb.LeaseState_LEASE_STATE_UNHELD)
			return
		case isLeader, ok := <-b.raft.LeaderCh():
			if !ok {
				b.setLeaseState(metadatapb.LeaseState_LEASE_STATE_UNHELD)
				return
			}
			if !isLeader {
				b.setLeaseState(metadatapb.LeaseState_LEASE_STATE_UNHELD)
				continue
			}
			if b.raft == nil || b.raft.State() != hashicorpraft.Leader {
				b.setLeaseState(metadatapb.LeaseState_LEASE_STATE_UNHELD)
			} else if err := b.raft.Barrier(applyTimeout).Error(); err != nil {
				b.log.Warn("failed to synchronize raft state before acquiring lease", slog.Any("error", err))
				b.setLeaseState(metadatapb.LeaseState_LEASE_STATE_UNHELD)
			} else if err := b.raft.VerifyLeader().Error(); err != nil {
				b.setLeaseState(metadatapb.LeaseState_LEASE_STATE_UNHELD)
			} else {
				b.setLeaseState(metadatapb.LeaseState_LEASE_STATE_HELD)
			}
		}
	}
}

func (b *Backend) setLeaseState(state metadatapb.LeaseState) {
	current, _ := b.leaseWatch.Load()
	if current != state {
		b.leaseWatch.Notify(state)
	}
}
