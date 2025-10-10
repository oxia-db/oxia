// Copyright 2023-2025 The Oxia Authors
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

package concurrent

type Callback[T any] interface {
	// OnComplete is invoked when the operation completes successfully with the result 't' of type T.
	OnComplete(t T)

	// OnCompleteError is invoked when the operation fails, providing an error 'err' indicating the failure reason.
	OnCompleteError(err error)
}

type StreamCallback[T any] interface {

	// OnNext is called whenever a new data item of type T is received from the stream.
	// It processes the received data item and returns an error if any issues occur during processing.
	// This method allows for custom logic to be applied to each individual data item in the stream.
	OnNext(t T) error

	// OnComplete is called when the stream has ended, either successfully or due to an error.
	// The 'err' parameter indicates the status of the stream completion.
	// If the stream ended successfully, 'err' will be nil. Otherwise,
	// it will contain the error that caused the stream to terminate.
	OnComplete(err error)
}
