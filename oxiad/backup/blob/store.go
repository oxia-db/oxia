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

// Package blob is the object store that the backups are written to: a flat
// key space of objects, listed by prefix. The stores are opened from a
// destination URL, by scheme: "s3://" (package s3) and "mem://" (an
// in-process store, for tests).
package blob

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sync"
	"time"
)

// ErrNotFound is returned by Get for a key that holds no object.
var ErrNotFound = errors.New("blob: object not found")

// Object describes a stored object.
type Object struct {
	Key          string
	Size         int64
	LastModified time.Time
}

// Store is an object store. The keys are "/"-separated paths relative to the
// destination the store was opened for. Every method is safe for concurrent
// use.
type Store interface {
	io.Closer

	// Put stores the content of r under key, replacing the object there if
	// any. The size of the content needn't be known in advance.
	Put(ctx context.Context, key string, r io.Reader) error
	// Get returns the content of the object at key, or ErrNotFound.
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	// List calls visit for every object whose key starts with prefix, in key
	// order, and stops at the first error, which it returns.
	List(ctx context.Context, prefix string, visit func(Object) error) error
	// ListDirs returns the directories right under prefix: the distinct next
	// "/"-terminated path segments of the keys that start with prefix, in
	// order, each with the prefix included, e.g. "a/b/" for the key "a/b/c"
	// under the prefix "a/".
	ListDirs(ctx context.Context, prefix string) ([]string, error)
	// Delete removes the objects at keys. A missing object is not an error.
	Delete(ctx context.Context, keys []string) error
}

// Opener opens the store that a destination URL names.
type Opener func(ctx context.Context, destination *url.URL) (Store, error)

var (
	openersMu sync.RWMutex
	openers   = map[string]Opener{}
)

// Register makes Open use opener for the destinations of the given URL
// scheme. It panics when the scheme is registered already.
func Register(scheme string, opener Opener) {
	openersMu.Lock()
	defer openersMu.Unlock()
	if _, exists := openers[scheme]; exists {
		panic(fmt.Sprintf("blob: scheme %q registered twice", scheme))
	}
	openers[scheme] = opener
}

// Open opens the store at destination, e.g. "s3://bucket/prefix", with the
// opener registered for its scheme.
func Open(ctx context.Context, destination string) (Store, error) {
	u, err := url.Parse(destination)
	if err != nil {
		return nil, fmt.Errorf("blob: invalid destination %q: %w", destination, err)
	}
	openersMu.RLock()
	opener, ok := openers[u.Scheme]
	openersMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("blob: unsupported destination scheme %q in %q", u.Scheme, destination)
	}
	return opener(ctx, u)
}
