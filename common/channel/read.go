// Copyright 2025 StreamNative, Inc.
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

package channel

import (
	"context"

	"github.com/oxia-db/oxia/common/entity"
)

func ReadAll[T any](ctx context.Context, ch chan *entity.TWithError[T]) ([]T, error) {
	container := make([]T, 0)
	for {
		select {
		case t, more := <-ch:
			if !more {
				return container, nil
			}
			if t.Err != nil {
				return nil, t.Err
			}
			container = append(container, t.T)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
