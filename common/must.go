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

package common //nolint:revive // Existing shared package name.

import "fmt"

// Must returns value when ok is true, otherwise it panics with an optional message.
func Must[T any](value T, ok bool, message ...any) T { //nolint:revive // Follows the standard Go value, ok convention.
	if !ok {
		if len(message) > 0 {
			panic(fmt.Sprint(message...))
		}
		panic("unexpected missing value")
	}
	return value
}
