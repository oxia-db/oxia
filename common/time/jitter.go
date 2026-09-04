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

package time

import (
	"math/rand/v2"
	"time"
)

// Jitter returns a duration uniformly distributed in
// [interval-maxDeviation, interval+maxDeviation). If maxDeviation is not
// positive, it returns interval unchanged.
func Jitter(interval time.Duration, maxDeviation time.Duration) time.Duration {
	if maxDeviation <= 0 {
		return interval
	}
	return interval - maxDeviation + rand.N(2*maxDeviation) //nolint:gosec
}
