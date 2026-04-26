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

package watch

import (
	"sync"

	"github.com/pkg/errors"
	gproto "google.golang.org/protobuf/proto"
)

var ErrClosed = errors.New("watch closed")

type Watch struct {
	sync.Mutex

	value     gproto.Message
	hasValue  bool
	closed    bool
	receivers map[*Receiver]chan struct{}
}

func New() *Watch {
	return &Watch{
		receivers: map[*Receiver]chan struct{}{},
	}
}

func (w *Watch) Subscribe() (*Receiver, error) {
	w.Lock()
	defer w.Unlock()

	if w.closed {
		return nil, ErrClosed
	}

	ch := make(chan struct{}, 1)
	r := &Receiver{
		watch:   w,
		changed: ch,
	}
	w.receivers[r] = ch
	return r, nil
}

func (w *Watch) Publish(value gproto.Message) {
	w.Lock()
	defer w.Unlock()

	if w.closed {
		return
	}

	if value != nil {
		value = gproto.Clone(value)
	}
	w.value = value
	w.hasValue = true

	for _, ch := range w.receivers {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (w *Watch) Close() {
	w.Lock()
	defer w.Unlock()

	if w.closed {
		return
	}

	w.closed = true
	for r, ch := range w.receivers {
		close(ch)
		delete(w.receivers, r)
	}
}

func (w *Watch) load() (gproto.Message, bool) {
	w.Lock()
	defer w.Unlock()

	if !w.hasValue {
		return nil, false
	}
	if w.value == nil {
		return nil, true
	}
	return gproto.Clone(w.value), true
}

type Receiver struct {
	watch   *Watch
	changed chan struct{}
}

func (r *Receiver) Changed() <-chan struct{} {
	return r.changed
}

func (r *Receiver) Load() (gproto.Message, bool) {
	return r.watch.load()
}

func (r *Receiver) Close() {
	r.watch.closeReceiver(r)
}

func (w *Watch) closeReceiver(r *Receiver) {
	w.Lock()
	defer w.Unlock()

	ch, ok := w.receivers[r]
	if !ok {
		return
	}
	delete(w.receivers, r)
	close(ch)
}
