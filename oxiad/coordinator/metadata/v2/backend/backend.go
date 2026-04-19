package backend

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	gproto "google.golang.org/protobuf/proto"

	metadatapb "github.com/oxia-db/oxia/common/proto/metadata"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
)

type Backend interface {
	io.Closer

	LeaseWatch() *commonoption.Watch[metadatapb.LeaseState]

	Load(name MetaRecordName) *Versioned[gproto.Message]
	Store(name MetaRecordName, record *Versioned[gproto.Message]) error
}

type MetaRecordName string

const (
	ConfigRecordName MetaRecordName = "conf"
	StatusRecordName MetaRecordName = "status"
)

type Versioned[T gproto.Message] struct {
	Version string
	Value   T
}

type MetaRecord[T gproto.Message] struct {
	mu      sync.Mutex
	value   atomic.Pointer[Versioned[T]]
	backend Backend
	name    MetaRecordName
}

func NewLazyMetaRecord[T gproto.Message](backend Backend, name MetaRecordName) MetaRecord[T] {
	return MetaRecord[T]{
		backend: backend,
		name:    name,
	}
}

func (v *MetaRecord[T]) Load() T {
	if current := v.value.Load(); current != nil {
		return gproto.CloneOf(current.Value)
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	if current := v.value.Load(); current != nil {
		return gproto.CloneOf(current.Value)
	}
	return v.SyncLoad()
}

func (v *MetaRecord[T]) Store(value T) error {
	current := v.value.Load()
	version := ""
	if current != nil {
		version = current.Version
	}

	record := &Versioned[gproto.Message]{
		Version: version,
		Value:   gproto.CloneOf(value),
	}
	if err := v.backend.Store(v.name, record); err != nil {
		return err
	}

	v.value.Store(&Versioned[T]{
		Version: record.Version,
		Value:   gproto.CloneOf(value),
	})
	return nil
}

func (v *MetaRecord[T]) SyncLoad() T {
	refreshed := &Versioned[T]{}
	if loaded := v.backend.Load(v.name); loaded != nil {
		refreshed.Version = loaded.Version
		if loaded.Value != nil {
			value, ok := loaded.Value.(T)
			if !ok {
				panic(fmt.Sprintf("unexpected record type for %q: %T", v.name, loaded.Value))
			}
			refreshed.Value = value
		}
	}
	v.value.Store(refreshed)
	return gproto.CloneOf(refreshed.Value)
}
