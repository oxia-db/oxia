package document

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"

	gproto "google.golang.org/protobuf/proto"
)

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

func newLazyMetaRecord[T gproto.Message](backend Backend, name MetaRecordName) MetaRecord[T] {
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
	refreshed := v.slowLoad()
	v.value.Store(refreshed)
	return gproto.CloneOf(refreshed.Value)
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

func (v *MetaRecord[T]) slowLoad() *Versioned[T] {
	loaded := typedVersioned[T](v.backend.Load(v.name), v.name)
	if loaded == nil {
		loaded = &Versioned[T]{}
	}
	if isNilProto(loaded.Value) {
		loaded.Value = newEmptyProto[T]()
	}
	return loaded
}

func typedVersioned[T gproto.Message](record *Versioned[gproto.Message], name MetaRecordName) *Versioned[T] {
	if record == nil {
		return nil
	}

	typed := &Versioned[T]{
		Version: record.Version,
	}
	if isNilProto(record.Value) {
		return typed
	}

	value, ok := record.Value.(T)
	if !ok {
		panic(fmt.Sprintf("unexpected record type for %q: %T", name, record.Value))
	}
	typed.Value = gproto.CloneOf(value)
	return typed
}

func newEmptyProto[T gproto.Message]() T {
	var zero T
	protoType := reflect.TypeOf(zero)
	if protoType == nil {
		return zero
	}
	if protoType.Kind() == reflect.Pointer {
		return reflect.New(protoType.Elem()).Interface().(T)
	}
	return reflect.New(protoType).Elem().Interface().(T)
}

func cloneVersionedProto[T gproto.Message](value *Versioned[T]) *Versioned[T] {
	if value == nil {
		return nil
	}

	cloned := &Versioned[T]{
		Version: value.Version,
	}
	if !isNilProto(value.Value) {
		cloned.Value = gproto.CloneOf(value.Value)
	}
	return cloned
}

func isNilProto[T gproto.Message](value T) bool {
	if any(value) == nil {
		return true
	}

	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
