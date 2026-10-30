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

package quorum

import "golang.org/x/exp/constraints"

// Majority is the smallest number of the n members that overlaps any other
// majority of them.
func Majority[T constraints.Integer](n T) T {
	return n/2 + 1
}

// MajorityOverlap is the smallest number of the n members that overlaps every
// majority of them: a majority leaves out n - Majority(n) members at most.
func MajorityOverlap[T constraints.Integer](n T) T {
	return n - Majority(n) + 1
}
