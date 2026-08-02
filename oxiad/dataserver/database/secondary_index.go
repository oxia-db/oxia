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

package database

import (
	"net/url"
	"regexp"

	"github.com/pkg/errors"

	"github.com/oxia-db/oxia/common/constant"
)

const (
	idxKeyPrefix = constant.InternalKeyPrefix + "idx"
	idxSeparator = "\x01"
)

// The index name and the secondary key are stored as the client supplied them,
// so both can be empty and the secondary key can contain the separator. Only
// the primary key is URL-escaped, and that escaping never emits a separator, so
// the last one is the field boundary.
var secondaryIndexKeyRegex = regexp.MustCompile(
	"(?s)^" + idxKeyPrefix + "/[^/]*/(.*)" + idxSeparator + "([^" + idxSeparator + "]*)$",
)

// ErrInvalidSecondaryIndexKey indicates that a key does not use the persisted
// secondary index key format.
var ErrInvalidSecondaryIndexKey = errors.New("oxia db: failed to parse secondary index key")

// ParseSecondaryIndexKey extracts the primary and secondary keys from a
// persisted secondary index entry.
func ParseSecondaryIndexKey(key string) (primaryKey string, secondaryKey string, err error) {
	matches := secondaryIndexKeyRegex.FindStringSubmatch(key)
	if len(matches) != 3 {
		return "", "", ErrInvalidSecondaryIndexKey
	}

	primaryKey, err = url.PathUnescape(matches[2])
	return primaryKey, matches[1], err
}
