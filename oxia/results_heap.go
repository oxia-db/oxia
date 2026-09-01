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

package oxia

import "github.com/oxia-db/oxia/common/compare"

type ResultAndChannel struct {
	gr GetResult
	ch chan GetResult
}

type ResultHeap []*ResultAndChannel

func (h ResultHeap) Len() int {
	return len(h)
}

// Less follows the order the shards themselves produced: a scan through a
// secondary index is ordered by secondary key, ties by record key; a plain
// scan by record key.
func (h ResultHeap) Less(i, j int) bool {
	a, b := h[i].gr, h[j].gr
	if a.SecondaryIndexKey != "" && b.SecondaryIndexKey != "" {
		if c := compare.CompareWithSlash([]byte(a.SecondaryIndexKey), []byte(b.SecondaryIndexKey)); c != 0 {
			return c < 0
		}
	}
	return compare.CompareWithSlash([]byte(a.Key), []byte(b.Key)) < 0
}

func (h ResultHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *ResultHeap) Push(x any) {
	*h = append(*h, x.(*ResultAndChannel))
}

func (h *ResultHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
