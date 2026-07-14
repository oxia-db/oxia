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

package compare

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"unsafe"

	"github.com/oxia-db/oxia/common/constant"
)

type Encoder interface {
	Name() string
	Encode(key string) []byte
	Decode(encodedKey []byte) string

	IsInternalKey(encodedKey []byte) bool

	// InternalKeyRange returns the encoded half-open range [start, end) that
	// contains every internal key. The region is always contiguous, so an
	// iterator can seek past it in one jump instead of stepping through it.
	//
	// end is nil when the region runs to the end of the keyspace — no user key
	// can sort after it — which lets the region be pruned with an upper bound
	// instead. That is the case for the hierarchical encoder, but NOT for the
	// natural one, where a user key is stored raw and can therefore sort after
	// the internal keys.
	InternalKeyRange() (start []byte, end []byte)
}

// keyRegionSuccessor returns the smallest key that sorts after every key with
// the given prefix, or nil when no such key exists (an all-0xff prefix).
func keyRegionSuccessor(prefix []byte) []byte {
	end := bytes.Clone(prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != maxByteValue {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

const (
	encodedSeparator      = 0xff
	internalKeysBitMarker = 1 << 15

	// maxByteValue is the largest byte value; a prefix made only of these has
	// no successor.
	maxByteValue = 0xff
)

var (
	separator             = []byte{'/'}
	encodedSeparatorSlice = []byte{encodedSeparator}
)

type encoderHierarchical struct{}

func (encoderHierarchical) Name() string {
	return "hierarchical"
}

func (encoderHierarchical) Encode(key string) []byte {
	l := len(key)
	buf := make([]byte, l+2)
	copy(buf[2:], key)

	// strings.Count appears to be faster than bytes.Count...
	sepCount := strings.Count(key, "/")

	// Skip replacement if we know the separators are not used
	if sepCount != 0 {
		for i := 2; i < len(buf); i++ {
			if buf[i] == '/' {
				buf[i] = encodedSeparator
			}
		}

		if l >= 2 && key[l-1] == '/' && key[l-2] == '/' {
			// Ignore the trailing separator as it doesn't create a new level
			sepCount--
		}

		if strings.HasPrefix(key, constant.InternalKeyPrefix) {
			// All internal keys at the end
			sepCount |= internalKeysBitMarker
		}
	}

	binary.BigEndian.PutUint16(buf, uint16(sepCount))
	return buf
}

func (encoderHierarchical) Decode(encoded []byte) string {
	if len(encoded) <= 2 {
		return ""
	}

	if encoded[0] == 0 && encoded[1] == 0 {
		return string(encoded[2:])
	}

	buf := bytes.ReplaceAll(encoded[2:], encodedSeparatorSlice, separator)
	// ReplaceAll always returns a fresh copy here (the separator count is
	// non-zero on this branch), so the slice can be re-typed as a string
	// without the second allocation+copy of a string(buf) conversion: it is
	// not aliased and never mutated after this point.
	return unsafe.String(unsafe.SliceData(buf), len(buf))
}

func (encoderHierarchical) IsInternalKey(encodedKey []byte) bool {
	if len(encodedKey) == 0 {
		return false
	}

	// If the first bit is set, it means it's an internal key
	return encodedKey[0]&(1<<7) != 0
}

// InternalKeyRange: the internal-key marker is the top bit of the 2-byte
// prefix, so every internal key sorts after every regular one (a regular key
// would need 32768 separators to reach that region) — the region runs to the
// end of the keyspace.
func (encoderHierarchical) InternalKeyRange() (start []byte, end []byte) {
	return []byte{internalKeysBitMarker >> 8}, nil
}

// EncoderHierarchical ensure that we can sort keys from same level together
// and thus we can easily return the children of a given path
// The encoding is done by prepending 2 bytes with the count of
// '/'. Slashes are also converted into special characters to make them sort
// after any other character.
var EncoderHierarchical Encoder = &encoderHierarchical{}

// /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const encodedInternalPrefix = 0xFF

var encodedInternalKeyPrefixBytes = []byte(strings.ReplaceAll(constant.InternalKeyPrefix, "__", "\xff\xff"))

type encoderNatural struct{}

func (encoderNatural) Name() string {
	return "natural"
}

func (encoderNatural) Encode(key string) []byte {
	if !strings.HasPrefix(key, constant.InternalKeyPrefix) {
		// Avoid copying the string
		return unsafe.Slice(unsafe.StringData(key), len(key))
	}

	// Encode internal keys so that they don't end up in the middle of ascii sorting
	// eg: a DeleteRange('A', 'Z') would also delete the '__oxia/...' keys
	b := []byte(key)
	b[0] = encodedInternalPrefix
	b[1] = encodedInternalPrefix
	return b
}

func (encoderNatural) Decode(encoded []byte) string {
	// The buffer must not be modified: it is Pebble's memory, handed out
	// under an explicit read-only contract ("the caller should not modify
	// the contents of the returned slice", Iterator.Key). Today it is the
	// iterator's own position buffer — which Pebble keeps using internally —
	// and Pebble's lower layers hand out direct block-cache references that
	// only an implementation detail of the current version copies before
	// they reach the caller.
	if bytes.HasPrefix(encoded, encodedInternalKeyPrefixBytes) {
		// Decode the internal key into a new string. The concatenation makes
		// a single allocation: the compiler does not allocate for a
		// []byte-to-string conversion used directly as a concat operand.
		return "__" + string(encoded[2:])
	}

	// Copy is necessary because the []byte memory is managed by Pebble
	// and will be released/overwritten when the iterator is closed
	// or moved
	return string(encoded)
}

func (encoderNatural) IsInternalKey(encodedKey []byte) bool {
	return bytes.HasPrefix(encodedKey, encodedInternalKeyPrefixBytes)
}

// InternalKeyRange: internal keys are the "\xff\xffoxia/" prefix region. A
// regular key is stored raw, so one whose bytes sort after that prefix (e.g.
// "\xff\xffz") lives *after* the internal keys — the region has a real upper
// end and cannot be pruned away with an upper bound.
func (encoderNatural) InternalKeyRange() (start []byte, end []byte) {
	return encodedInternalKeyPrefixBytes, encodedInternalKeyPrefixSuccessor
}

var encodedInternalKeyPrefixSuccessor = keyRegionSuccessor(encodedInternalKeyPrefixBytes)

var EncoderNatural Encoder = &encoderNatural{}

// /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func GetEncoder(name string) (Encoder, error) {
	switch name {
	case "hierarchical":
		return EncoderHierarchical, nil
	case "natural":
		return EncoderNatural, nil
	default:
		return nil, fmt.Errorf("unknown encoder %s", name)
	}
}
