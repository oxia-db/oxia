package document

import (
	"context"
	"errors"

	gproto "google.golang.org/protobuf/proto"

	commonactor "github.com/oxia-db/oxia/oxiad/common/actor"
)

type Operation[T gproto.Message] struct {
	apply func(T)
	done  chan error
	err   error
}

var ErrBadVersion = errors.New("metadata v2 bad version")

type Hooks[T gproto.Message] struct {
	RequireLease func() error
	Load         func() (T, error)
	Commit       func(T) error
	OnBadVersion func() error
	OnFailure    func(error) error
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

func NewOperation[T gproto.Message](apply func(T)) *Operation[T] {
	return &Operation[T]{
		apply: apply,
		done:  make(chan error, 1),
	}
}

func NewErrorOperation[T gproto.Message](apply func(T, func(error))) *Operation[T] {
	op := &Operation[T]{
		done: make(chan error, 1),
	}
	op.apply = func(value T) {
		apply(value, op.fail)
	}
	return op
}

func (m *Mutator[T]) Submit(op *Operation[T]) error {
	if err := m.actor.Send(op); err != nil {
		return err
	}

	select {
	case err := <-op.done:
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

	results := m.processBatch(ops)
	for i, op := range ops {
		op.done <- results[i]
	}
}

func (m *Mutator[T]) processBatch(ops []*Operation[T]) []error {
	for {
		results := make([]error, len(ops))

		if err := m.ctx.Err(); err != nil {
			for i := range results {
				results[i] = err
			}
			return results
		}

		if err := m.hooks.RequireLease(); err != nil {
			for i := range results {
				results[i] = err
			}
			return results
		}

		base, err := m.hooks.Load()
		if err != nil {
			for i := range results {
				results[i] = err
			}
			return results
		}

		working := base
		successful := make([]int, 0, len(ops))

		for i, op := range ops {
			next := gproto.CloneOf(working)
			op.err = nil
			if op.apply != nil {
				op.apply(next)
			}
			if op.err != nil {
				results[i] = op.err
				continue
			}
			working = next
			successful = append(successful, i)
		}

		if gproto.Equal(base, working) {
			return results
		}

		if err := m.hooks.Commit(working); err != nil {
			if errors.Is(err, ErrBadVersion) && m.hooks.OnBadVersion != nil {
				if recoveryErr := m.hooks.OnBadVersion(); recoveryErr == nil {
					continue
				} else {
					err = recoveryErr
				}
			} else if m.hooks.OnFailure != nil {
				if failureErr := m.hooks.OnFailure(err); failureErr != nil {
					err = failureErr
				}
			}

			for _, i := range successful {
				results[i] = err
			}
		}

		return results
	}
}

func (o *Operation[T]) fail(err error) {
	if err == nil || o.err != nil {
		return
	}
	o.err = err
}
