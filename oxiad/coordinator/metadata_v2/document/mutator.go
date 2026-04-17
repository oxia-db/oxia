package document

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/oxia-db/oxia/common/channel"
	gproto "google.golang.org/protobuf/proto"

	commonactor "github.com/oxia-db/oxia/oxiad/common/actor"
)

type Operation[T gproto.Message] struct {
	apply func(T) error
	r     chan error
	c     atomic.Bool
}

func (m *Operation[T]) Complete(err error) {
	if !m.c.CompareAndSwap(false, true) {
		return
	}
	channel.PushNoBlock(m.r, err)
}

var ErrBadVersion = errors.New("metadata v2 bad version")

type Hooks[T gproto.Message] struct {
	Load         func() T
	Commit       func(T) error
	OnBadVersion func() (bool, error)
}

type Mutator[T gproto.Message] struct {
	ctx   context.Context
	actor *commonactor.Actor[*Operation[T]]
	hooks Hooks[T]
}

func NewMutator[T gproto.Message](ctx context.Context, name string, actorErrors commonactor.Errors, hooks Hooks[T]) *Mutator[T] {
	m := &Mutator[T]{
		ctx:   ctx,
		hooks: hooks,
	}

	actor, err := commonactor.New[*Operation[T]](ctx, name, m.handleBatch, actorErrors)
	if err != nil {
		panic(err)
	}
	m.actor = actor
	return m
}

func NewOperation[T gproto.Message](apply func(T) error) *Operation[T] {
	return &Operation[T]{
		apply: apply,
		r:     make(chan error, 1),
		c:     atomic.Bool{},
	}
}
func (m *Mutator[T]) Submit(op *Operation[T]) error {
	if err := m.actor.Send(op); err != nil {
		return err
	}

	select {
	case err := <-op.r:
		return err
	case <-m.ctx.Done():
		return m.ctx.Err()
	}
}

func (m *Mutator[T]) Pause() error {
	return m.actor.Pause()
}

func (m *Mutator[T]) Resume() error {
	return m.actor.Resume()
}

func (m *Mutator[T]) Close() error {
	return m.actor.Close()
}

func (m *Mutator[T]) handleBatch(ops []*Operation[T]) {
	if len(ops) == 0 {
		return
	}
	m.processBatch(ops)
}

func (m *Mutator[T]) processBatch(ops []*Operation[T]) {
	for {
		state := m.hooks.Load()
		for _, op := range ops {
			if err := op.apply(state); err != nil {
				op.Complete(err)
			}
		}
		if err := m.hooks.Commit(state); err != nil {
			if errors.Is(err, ErrBadVersion) && m.hooks.OnBadVersion != nil {
				retry, bvErr := m.hooks.OnBadVersion()
				if retry {
					continue
				}
				if bvErr != nil {
					err = bvErr
				}
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
