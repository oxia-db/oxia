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

//nolint:revive // package name conflicts with standard library, but this is an internal package
package time

import (
	"sync/atomic"
	"time"
)

type Clock interface {
	Now() time.Time
}

type systemClock struct {
}

var SystemClock = &systemClock{}

func (systemClock) Now() time.Time {
	return time.Now()
}

type MockedClock struct {
	currentTime atomic.Int64
}

func (c *MockedClock) Set(currentTime int64) {
	c.currentTime.Store(currentTime)
}

func (c *MockedClock) Now() time.Time {
	return time.UnixMilli(c.currentTime.Load())
}
