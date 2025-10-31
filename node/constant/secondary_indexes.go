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
