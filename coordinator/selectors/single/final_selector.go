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

package single

import (
	"k8s.io/apimachinery/pkg/util/rand"

	"github.com/streamnative/oxia/coordinator/selectors"
)

var _ selectors.Selector[*Context, string] = &finalSelector{}

type finalSelector struct{}

func (*finalSelector) Select(ssContext *Context) (string, error) {
	status := ssContext.Status
	candidatesArr := ssContext.Candidates.Values()
	if len(candidatesArr) == 0 {
		return "", selectors.ErrNoFunctioning
	}
	if status != nil {
		startIdx := ssContext.Status.ServerIdx
		return candidatesArr[int(startIdx)%len(candidatesArr)], nil
	}
	return candidatesArr[rand.Intn(len(candidatesArr))], nil
}
