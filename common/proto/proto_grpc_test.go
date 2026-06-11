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

import (
	"bytes"
	"testing"

	"github.com/planetscale/vtprotobuf/codec/grpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/mem"
)

func TestVtprotoCodec_Registration(t *testing.T) {
	// The codec must live in the CodecV2 registry: a legacy (V1) registration
	// would shadow it through the gRPC bridge, reintroducing a full copy of
	// every received message
	assert.Nil(t, encoding.GetCodec(grpc.Name))

	codec := encoding.GetCodecV2(grpc.Name)
	require.NotNil(t, codec)
	assert.IsType(t, vtprotoCodec{}, codec)
}

func TestVtprotoCodec_RoundTrip(t *testing.T) {
	codec := vtprotoCodec{}

	// Below and above the buffer-pooling threshold
	for _, size := range []int{16, 64 * 1024} {
		entry := &LogEntry{Term: 1, Offset: 42, Timestamp: 123, Value: bytes.Repeat([]byte("x"), size)}

		data, err := codec.Marshal(entry)
		require.NoError(t, err)

		decoded := &LogEntry{}
		require.NoError(t, codec.Unmarshal(data, decoded))
		data.Free()

		assert.EqualValues(t, 1, decoded.Term)
		assert.EqualValues(t, 42, decoded.Offset)
		assert.EqualValues(t, 123, decoded.Timestamp)
		assert.Equal(t, entry.Value, decoded.Value)
	}
}

func TestVtprotoCodec_UnmarshalMultiBuffer(t *testing.T) {
	codec := vtprotoCodec{}

	entry := &LogEntry{Term: 7, Offset: 1, Value: bytes.Repeat([]byte("y"), 4096)}
	raw, err := entry.MarshalVT()
	require.NoError(t, err)

	// A message split across multiple receive buffers gets reassembled
	mid := len(raw) / 2
	data := mem.BufferSlice{mem.SliceBuffer(raw[:mid]), mem.SliceBuffer(raw[mid:])}

	decoded := &LogEntry{}
	require.NoError(t, codec.Unmarshal(data, decoded))
	assert.Equal(t, entry.Value, decoded.Value)
}

func TestVtprotoCodec_StandardProtoFallback(t *testing.T) {
	codec := vtprotoCodec{}

	// Messages without the generated vtproto methods (e.g. the gRPC health
	// service) go through the standard proto marshaling
	req := &grpc_health_v1.HealthCheckRequest{Service: "oxia"}
	data, err := codec.Marshal(req)
	require.NoError(t, err)

	decoded := &grpc_health_v1.HealthCheckRequest{}
	require.NoError(t, codec.Unmarshal(data, decoded))
	data.Free()
	assert.Equal(t, "oxia", decoded.Service)
}

func TestVtprotoCodec_InvalidMessage(t *testing.T) {
	codec := vtprotoCodec{}

	_, err := codec.Marshal(struct{}{})
	assert.Error(t, err)
	assert.Error(t, codec.Unmarshal(mem.BufferSlice{}, struct{}{}))
}

func BenchmarkCodecUnmarshal(b *testing.B) {
	entry := &LogEntry{Term: 1, Offset: 1, Value: make([]byte, 4096)}
	raw, err := entry.MarshalVT()
	if err != nil {
		b.Fatal(err)
	}

	// The receive path of the legacy V1 registration: gRPC's bridge fully
	// materializes the buffer slice before invoking the codec
	b.Run("v1-bridge", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			data := mem.BufferSlice{mem.SliceBuffer(raw)}
			decoded := &LogEntry{}
			if err := decoded.UnmarshalVT(data.Materialize()); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("v2", func(b *testing.B) {
		codec := vtprotoCodec{}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			data := mem.BufferSlice{mem.SliceBuffer(raw)}
			decoded := &LogEntry{}
			if err := codec.Unmarshal(data, decoded); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkCodecMarshal(b *testing.B) {
	entry := &LogEntry{Term: 1, Offset: 1, Value: make([]byte, 4096)}

	// The send path of the legacy V1 registration: a fresh full-size buffer
	// per message
	b.Run("v1-bridge", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := entry.MarshalVT(); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("v2", func(b *testing.B) {
		codec := vtprotoCodec{}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			data, err := codec.Marshal(entry)
			if err != nil {
				b.Fatal(err)
			}
			data.Free()
		}
	})
}
