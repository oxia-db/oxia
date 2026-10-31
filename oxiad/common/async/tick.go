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

package async

import "github.com/oxia-db/oxia/common/channel"

// Tickable is a component whose background tasks make progress that its owner
// applies from its own event loop: they send a Signal to the loop, which then
// calls Tick.
type Tickable interface {
	// Tick applies the progress made since the last tick. It is called from
	// the event loop of the owner, which calls it again later if it fails.
	Tick() error
}

// Signal asks an event loop to tick a Tickable.
type Signal struct{}

// Signaler is the channel, with a buffer of one, that the owner of a Tickable
// creates and receives from in its event loop, and passes to the Tickable.
type Signaler chan<- Signal

// Signal sends a Signal without blocking: a signal already pending covers it.
func (s Signaler) Signal() {
	channel.PushNoBlock(s, Signal{})
}
