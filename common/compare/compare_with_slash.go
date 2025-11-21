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

package compare

import (
	"bytes"
)

func CompareWithSlash(a, b []byte) int { //nolint:revive
	for len(a) > 0 && len(b) > 0 {
		idxA, idxB := bytes.IndexByte(a, '/'), bytes.IndexByte(b, '/')
		switch {
		case idxA < 0 && idxB < 0:
			return bytes.Compare(a, b)
		case idxA < 0 && idxB >= 0:
			return -1
		case idxA >= 0 && idxB < 0:
			return +1
		}

		// At this point, both slices have '/'
		spanA, spanB := a[:idxA], b[:idxB]
		spanRes := bytes.Compare(spanA, spanB)
		if spanRes != 0 {
			return spanRes
		}

		a, b = a[idxA+1:], b[idxB+1:]
	}

	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return +1
	}

	return 0
}
