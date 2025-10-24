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
	"encoding/binary"
	"fmt"
	"strings"
	"unsafe"

	"github.com/oxia-db/oxia/common/constant"
)

type Encoder interface {
	Name() string
	Encode(string) []byte
	Decode([]byte) string
}

const (
	encodedSeparator      = 0xff
	internalKeysBitMarker = 15
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
			sepCount |= 1 << internalKeysBitMarker
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
	return string(buf)
}

// EncoderHierarchical ensure that we can sort keys from same level together
// and thus we can easily return the children of a given path
// The encoding is done by prepending 2 bytes with the count of
// '/'. Slashes are also converted into special characters to make them sort
// after any other character.
var EncoderHierarchical = &encoderHierarchical{}

// /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type encoderNatural struct{}

func (encoderNatural) Name() string {
	return "natural"
}

func (encoderNatural) Encode(key string) []byte {
	// Avoid copying the string
	return unsafe.Slice(unsafe.StringData(key), len(key))
}

func (encoderNatural) Decode(encoded []byte) string {
	// Copy is necessary because the []byte memory is managed by Pebble
	// and will be released/overwritten when the iterator is closed
	// or moved
	return string(encoded)
}

var EncoderNatural = &encoderNatural{}

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
