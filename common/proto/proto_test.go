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

package proto

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

var appendRequest = &Append{
	Term: 1,
	Entry: &LogEntry{
		Term:      1,
		Offset:    128,
		Value:     make([]byte, 1024),
		Timestamp: uint64(time.Now().UnixNano()),
	},
	CommitOffset: 128,
}

func Benchmark_ProtoMarshall(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := proto.Marshal(appendRequest)
		assert.NoError(b, err)
	}
}

func Benchmark_VTProtoMarshall(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := appendRequest.MarshalVT()
		assert.NoError(b, err)
	}
}

func Benchmark_VTProtoMarshall_WithBuffer(b *testing.B) {
	buf := make([]byte, appendRequest.SizeVT())
	for i := 0; i < b.N; i++ {
		_, err := appendRequest.MarshalToSizedBufferVT(buf)
		assert.NoError(b, err)
	}
}
