// Copyright 2023-2026 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package blob

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

// MemoryScheme is the URL scheme of the in-process stores: "mem://<name>"
// opens the store of that name, shared by every opener in the process. The
// name is the host and the path of the URL, so "mem://a/x" and "mem://a/y"
// are two stores.
const MemoryScheme = "mem"

func init() {
	Register(MemoryScheme, func(_ context.Context, destination *url.URL) (Store, error) {
		return SharedMemory(destination.Host + destination.Path), nil
	})
}

var (
	sharedMemoryMu sync.Mutex
	sharedMemory   = map[string]*Memory{}
)

// SharedMemory returns the in-process store of the given name, creating it on
// the first call.
func SharedMemory(name string) *Memory {
	sharedMemoryMu.Lock()
	defer sharedMemoryMu.Unlock()
	store, ok := sharedMemory[name]
	if !ok {
		store = NewMemory()
		sharedMemory[name] = store
	}
	return store
}

type memoryObject struct {
	data         []byte
	lastModified time.Time
}

// Memory is an in-process Store, for tests. Its objects' last modification
// times come from its clock, which the tests can set.
type Memory struct {
	mu      sync.RWMutex
	objects map[string]memoryObject
	clock   func() time.Time
	// failPut, when set, fails the Put of the keys it returns true for. It
	// lets the tests interrupt a multi-step write at a chosen step.
	failPut func(key string) error
}

// NewMemory returns an empty in-process store.
func NewMemory() *Memory {
	return &Memory{
		objects: map[string]memoryObject{},
		clock:   time.Now,
	}
}

// SetClock makes the store stamp its objects with the given clock.
func (m *Memory) SetClock(clock func() time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.clock = clock
}

// FailPut makes Put fail with the error that fail returns for a key, when it
// returns one. A nil fail restores the normal behavior.
func (m *Memory) FailPut(fail func(key string) error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failPut = fail
}

// Keys returns every key of the store, in order.
func (m *Memory) Keys() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keys := make([]string, 0, len(m.objects))
	for key := range m.objects {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// Touch sets the last modification time of the object at key.
func (m *Memory) Touch(key string, lastModified time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if object, ok := m.objects[key]; ok {
		object.lastModified = lastModified
		m.objects[key] = object
	}
}

func (m *Memory) Put(ctx context.Context, key string, r io.Reader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.RLock()
	fail := m.failPut
	m.mu.RUnlock()
	if fail != nil {
		if err := fail(key); err != nil {
			return err
		}
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[key] = memoryObject{data: data, lastModified: m.clock()}
	return nil
}

func (m *Memory) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	object, ok := m.objects[key]
	if !ok {
		return nil, ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(object.data)), nil
}

func (m *Memory) List(ctx context.Context, prefix string, visit func(Object) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, object := range m.snapshot(prefix) {
		if err := visit(object); err != nil {
			return err
		}
	}
	return nil
}

func (m *Memory) ListDirs(ctx context.Context, prefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var dirs []string
	for _, object := range m.snapshot(prefix) {
		rest := strings.TrimPrefix(object.Key, prefix)
		slash := strings.Index(rest, "/")
		if slash < 0 {
			continue
		}
		dir := prefix + rest[:slash+1]
		if len(dirs) == 0 || dirs[len(dirs)-1] != dir {
			dirs = append(dirs, dir)
		}
	}
	return dirs, nil
}

func (m *Memory) Delete(ctx context.Context, keys []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, key := range keys {
		delete(m.objects, key)
	}
	return nil
}

func (*Memory) Close() error {
	return nil
}

// snapshot returns the objects under prefix, in key order.
func (m *Memory) snapshot(prefix string) []Object {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var objects []Object
	for key, object := range m.objects {
		if strings.HasPrefix(key, prefix) {
			objects = append(objects, Object{Key: key, Size: int64(len(object.data)), LastModified: object.lastModified})
		}
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].Key < objects[j].Key })
	return objects
}
