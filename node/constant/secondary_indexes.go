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

package constant

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"

	"github.com/oxia-db/oxia/proto"
)

var ErrFailedToParseSecondaryKey = errors.New("oxia db: failed to parse secondary index key")

const SecondaryIdxKeyPrefix = InternalKeyPrefix + "idx"

const SecondaryIdxSeparator = "\x01"
const SecondaryIdxRangePrefixFormat = SecondaryIdxKeyPrefix + "/%s/%s"
const SecondaryIdxFormat = SecondaryIdxRangePrefixFormat + SecondaryIdxSeparator + "%s"

const regex = "^" + SecondaryIdxKeyPrefix + "/[^/]+/([^" + SecondaryIdxSeparator + "]+)" + SecondaryIdxSeparator + "(.+)$"

var secondaryIdxFormatRegex = regexp.MustCompile(regex)

func SecondaryIndexKey(primaryKey string, si *proto.SecondaryIndex) string {
	return fmt.Sprintf(SecondaryIdxFormat, si.IndexName, si.SecondaryKey, url.PathEscape(primaryKey))
}
func SecondaryIndexPrimaryKey(completeKey string) (string, error) {
	matches := secondaryIdxFormatRegex.FindStringSubmatch(completeKey)
	if len(matches) != 3 {
		return "", ErrFailedToParseSecondaryKey
	}

	return url.PathUnescape(matches[2])
}
func SecondaryIndexPrimaryAndSecondaryKey(completeKey string) (primaryKey string, secondaryKey string, err error) {
	matches := secondaryIdxFormatRegex.FindStringSubmatch(completeKey)
	if len(matches) != 3 {
		return "", "", ErrFailedToParseSecondaryKey
	}

	secondaryKey = matches[1]
	primaryKey, err = url.PathUnescape(matches[2])
	return primaryKey, secondaryKey, err
}
