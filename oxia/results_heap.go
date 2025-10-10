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

func (h ResultHeap) Less(i, j int) bool {
	return compare.CompareWithSlash([]byte(h[i].gr.Key), []byte(h[j].gr.Key)) < 0
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
