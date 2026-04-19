package document

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"

	"github.com/oxia-db/oxia/common/channel"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata_v2/document/backend"
	metadataerr "github.com/oxia-db/oxia/oxiad/coordinator/metadata_v2/error"
	gproto "google.golang.org/protobuf/proto"

	commonactor "github.com/oxia-db/oxia/oxiad/common/actor"
)

type Inflight[T gproto.Message] struct {
	op func(T) error
	r  chan error
	c  atomic.Bool
}

func (m *Inflight[T]) Complete(err error) {
	if !m.c.CompareAndSwap(false, true) {
		return
	}
	channel.PushNoBlock(m.r, err)
}
func NewInflight[T gproto.Message](apply func(T) error) *Inflight[T] {
	return &Inflight[T]{
		op: apply,
		r:  make(chan error, 1),
		c:  atomic.Bool{},
	}
}

var ErrBadVersion = errors.New("metadata bad version")

type Executor[T gproto.Message] struct {
	ctx    context.Context
	name   string
	actor  *commonactor.Actor[*Inflight[T]]
	record *backend.MetaRecord[T]
}

func NewExecutor[T gproto.Message](ctx context.Context, name string, record *backend.MetaRecord[T]) *Executor[T] {
	m := &Executor[T]{
		ctx:    ctx,
		name:   name,
		record: record,
	}

	var err error
	if m.actor, err = commonactor.New[*Inflight[T]](ctx, name, m.bgApply, commonactor.StatusPaused); err != nil {
		panic(err)
	}
	return m
}

func (m *Executor[T]) Execute(op *Inflight[T]) error {
	if err := m.actor.Send(op); err != nil {
		if errors.Is(err, commonactor.ErrPaused) || errors.Is(err, commonactor.ErrShuttingDown) {
			return metadataerr.ErrLeaseNotHeld
		}
		return err
	}

	select {
	case err := <-op.r:
		return err
	case <-m.ctx.Done():
		return m.ctx.Err()
	}
}

func (m *Executor[T]) Pause() error {
	return m.actor.Pause()
}

func (m *Executor[T]) Resume() error {
	m.record.SyncLoad()
	return m.actor.Resume()
}

func (m *Executor[T]) Close() error {
	return m.actor.Close()
}

func (m *Executor[T]) bgApply(ops []*Inflight[T]) {
	batchSize := len(ops)
	if batchSize == 0 {
		return
	}
	state := m.record.Load()
	for {
		failSize := 0
		for _, op := range ops {
			if err := op.op(state); err != nil {
				op.Complete(err)
				failSize++
			}
		}
		if failSize == batchSize {
			return
		}
		if err := m.record.Store(state); err != nil {
			if errors.Is(err, ErrBadVersion) {
				_ = m.actor.Pause()
				slog.Warn("metadata document commit hit bad version, pausing executor",
					slog.String("executor", m.name),
					slog.Int("ops", len(ops)),
				)
				err = metadataerr.ErrLeaseNotHeld
			}
			for _, op := range ops {
				op.Complete(err)
			}
			return
		}
		for _, op := range ops {
			op.Complete(nil)
		}
		return
	}
}
