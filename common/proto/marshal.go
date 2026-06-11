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

package proto

type sizedMarshaler interface {
	SizeVT() int
	MarshalToSizedBufferVT(dAtA []byte) (int, error)
}

// MarshalToBuffer serializes a vtproto message into buf, growing it when
// needed, and returns the (possibly grown) buffer together with the slice
// holding the serialized bytes. It avoids the per-message allocation of
// MarshalVT on hot paths where the caller can reuse the buffer across calls.
// As with the generated MarshalVT, a nil message yields no data and no error.
func MarshalToBuffer(buf []byte, m sizedMarshaler) (newBuf []byte, data []byte, err error) {
	if m == nil {
		return buf, nil, nil
	}

	size := m.SizeVT()
	if cap(buf) < size {
		buf = make([]byte, size)
	} else {
		buf = buf[:size]
	}

	n, err := m.MarshalToSizedBufferVT(buf)
	if err != nil {
		return buf, nil, err
	}
	return buf, buf[size-n:], nil
}
