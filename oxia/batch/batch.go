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

package batch

type Batch interface {
	CanAdd(any) bool
	Add(any)
	Size() int
	Complete()
	Fail(error)
}

// AsyncBatch is a Batch whose request can be sent without waiting for its
// response, so that a batcher keeps several batches in flight.
type AsyncBatch interface {
	Batch
	// Send sends the request of the batch, and returns a function that waits
	// for its response and completes or fails the calls. A batcher sends its
	// batches in order, and invokes the returned functions in the same order.
	Send() (complete func())
}
