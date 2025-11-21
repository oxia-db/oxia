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
	"github.com/oxia-db/oxia/oxiad/coordinator/selectors"
	"github.com/pkg/errors"
)

var _ selectors.Selector[*Context, string] = &server{}

type server struct {
	selectors []selectors.Selector[*Context, string]
}

func (s *server) Select(selectorContext *Context) (string, error) {
	var serverId string
	var err error
	for _, selector := range s.selectors {
		if serverId, err = selector.Select(selectorContext); err != nil {
			if errors.Is(err, selectors.ErrNoFunctioning) || errors.Is(err, selectors.ErrMultipleResult) {
				continue
			}
			return "", err
		}
		if serverId != "" {
			return serverId, nil
		}
	}
	if serverId == "" {
		panic("unexpected behaviour")
	}
	return serverId, nil
}

func NewSelector() selectors.Selector[*Context, string] {
	return &server{
		selectors: []selectors.Selector[*Context, string]{
			&serverAntiAffinitiesSelector{},
			&lowerestLoadSelector{},
			&finalSelector{},
		},
	}
}
