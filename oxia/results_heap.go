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

type ResultAndChannel struct {
	gr GetResult
	// The key of the result, in the form that the order of the heap compares
	sortKey []byte
	ch      chan GetResult
}

type ResultHeap struct {
	results []*ResultAndChannel
	order   keyOrder
}

func (h *ResultHeap) Len() int {
	return len(h.results)
}

func (h *ResultHeap) Less(i, j int) bool {
	return h.order.compareSortKeys(h.results[i].sortKey, h.results[j].sortKey) < 0
}

func (h *ResultHeap) Swap(i, j int) {
	h.results[i], h.results[j] = h.results[j], h.results[i]
}

func (h *ResultHeap) Push(x any) {
	h.results = append(h.results, x.(*ResultAndChannel))
}

func (h *ResultHeap) Pop() any {
	old := h.results
	n := len(old)
	x := old[n-1]
	h.results = old[0 : n-1]
	return x
}
