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
	"github.com/pkg/errors"

	"github.com/streamnative/oxia/coordinator/selectors"
)

var _ selectors.Selector[*Context, *string] = &server{}

type server struct {
	selectors []selectors.Selector[*Context, *string]
}

func (s *server) Select(selectorContext *Context) (*string, error) {
	var serverId *string
	var err error
	for _, selector := range s.selectors {
		if serverId, err = selector.Select(selectorContext); err != nil {
			if errors.Is(err, selectors.ErrNoFunctioning) || errors.Is(err, selectors.ErrMultipleResult) {
				continue
			}
			return nil, err
		}
		if serverId != nil {
			return serverId, nil
		}
	}
	if serverId == nil {
		panic("unexpected behaviour")
	}
	return serverId, nil
}

func NewSelector() selectors.Selector[*Context, *string] {
	return &server{
		selectors: []selectors.Selector[*Context, *string]{
			&serverAntiAffinitiesSelector{},
			&serverIdxSelector{},
		},
	}
}
