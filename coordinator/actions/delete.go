package actions

import "sync"

type DeleteAction struct {
	Shard  int64
	Waiter *sync.WaitGroup
}

func (e *DeleteAction) Done(_ any) {
	e.Waiter.Done()
}

func (*DeleteAction) Type() Type {
	return Election
}

func (e *DeleteAction) Clone() *DeleteAction {
	return &DeleteAction{
		Shard:  e.Shard,
		Waiter: &sync.WaitGroup{},
	}
}
