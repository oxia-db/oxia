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

package single

import (
	"encoding/binary"
	"hash/fnv"
	"sort"

	"github.com/oxia-db/oxia/oxiad/coordinator/runtime/balancer/selector"
)

var _ selector.Selector[*Context, string] = &finalSelector{}

type finalSelector struct{}

func (*finalSelector) Select(ssContext *Context) (string, error) {
	candidatesArr := ssContext.Candidates.Values()
	if len(candidatesArr) == 0 {
		return "", selector.ErrNoFunctioning
	}

	sort.Strings(candidatesArr)
	replicaOrdinal := 0
	if ssContext.selected != nil {
		replicaOrdinal = ssContext.selected.Size()
	}
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(ssContext.Namespace))
	var buf [16]byte
	binary.LittleEndian.PutUint64(buf[:8], uint64(ssContext.Shard))
	binary.LittleEndian.PutUint64(buf[8:], uint64(replicaOrdinal))
	_, _ = hasher.Write(buf[:])
	startIdx := int(hasher.Sum64() % uint64(len(candidatesArr)))
	return candidatesArr[startIdx], nil
}
