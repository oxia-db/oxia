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

import (
	"github.com/oxia-db/oxia/oxiad/common/entity"
)

func ReadFromStreamCallback[T any](ch chan *entity.TWithError[T]) StreamCallback[T] {
	return NewStreamOnce(func(t T) error {
		ch <- &entity.TWithError[T]{
			T:   t,
			Err: nil,
		}
		return nil
	}, func(err error) {
		if err != nil {
			ch <- &entity.TWithError[T]{
				Err: err,
			}
		}
		close(ch)
	})
}
