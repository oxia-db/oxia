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

// rangeScanResult is a result of the range scan of one shard. When the scan
// uses an index, the shard returns the records in the order of their secondary
// keys, and the servers send the secondary key with each record. Older servers
// don't, and leave it nil.
type rangeScanResult struct {
	gr                GetResult
	secondaryIndexKey *string
}

type ResultAndChannel struct {
	gr GetResult
	// The key of the result, and its secondary key if it has one, in the form
	// that the order of the heap compares
	sortKey          []byte
	sortSecondaryKey []byte
	hasSecondaryKey  bool
	ch               chan rangeScanResult
}

func newResultAndChannel(order keyOrder, r rangeScanResult, ch chan rangeScanResult) *ResultAndChannel {
	rc := &ResultAndChannel{gr: r.gr, sortKey: order.sortKey(r.gr.Key), ch: ch}
	if r.secondaryIndexKey != nil {
		rc.sortSecondaryKey = order.sortKey(*r.secondaryIndexKey)
		rc.hasSecondaryKey = true
	}
	return rc
}

type ResultHeap struct {
	results []*ResultAndChannel
	order   keyOrder
}

func (h *ResultHeap) Len() int {
	return len(h.results)
}

// Less orders the results by their secondary keys, then by their keys. The
// results without a secondary key are ordered by their keys only.
func (h *ResultHeap) Less(i, j int) bool {
	a, b := h.results[i], h.results[j]
	if a.hasSecondaryKey && b.hasSecondaryKey {
		if c := h.order.compareSortKeys(a.sortSecondaryKey, b.sortSecondaryKey); c != 0 {
			return c < 0
		}
	}
	return h.order.compareSortKeys(a.sortKey, b.sortKey) < 0
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
