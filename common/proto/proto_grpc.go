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
	"fmt"

	"github.com/planetscale/vtprotobuf/codec/grpc"
	"google.golang.org/grpc/encoding"

	// Ensure the default proto codec registers first, so that this one
	// deterministically overrides it in the CodecV2 registry.
	_ "google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/mem"
	pb "google.golang.org/protobuf/proto"
)

// vtprotoCodec is an encoding.CodecV2 serializing with the generated
// vtprotobuf fast paths and the gRPC shared buffer pools. A legacy
// encoding.Codec registration would instead get wrapped by gRPC in a bridge
// that fully materializes (allocates and copies) every received message and
// sends from unpooled marshal buffers: going through CodecV2 saves an
// allocation and a full-payload copy per message, in each direction.
type vtprotoCodec struct{}

type vtprotoMessage interface {
	SizeVT() int
	MarshalToSizedBufferVT(dAtA []byte) (int, error)
	UnmarshalVT([]byte) error
}

func (vtprotoCodec) Marshal(v any) (mem.BufferSlice, error) {
	switch v := v.(type) {
	case vtprotoMessage:
		size := v.SizeVT()
		if mem.IsBelowBufferPoolingThreshold(size) {
			buf := make([]byte, size)
			if _, err := v.MarshalToSizedBufferVT(buf); err != nil {
				return nil, err
			}
			return mem.BufferSlice{mem.SliceBuffer(buf)}, nil
		}

		pool := mem.DefaultBufferPool()
		buf := pool.Get(size)
		*buf = (*buf)[:size]
		if _, err := v.MarshalToSizedBufferVT(*buf); err != nil {
			pool.Put(buf)
			return nil, err
		}
		return mem.BufferSlice{mem.NewBuffer(buf, pool)}, nil

	case pb.Message:
		buf, err := pb.Marshal(v)
		if err != nil {
			return nil, err
		}
		return mem.BufferSlice{mem.SliceBuffer(buf)}, nil

	default:
		return nil, fmt.Errorf("failed to marshal, message is %T, must satisfy the vtprotoMessage interface or want proto.Message", v)
	}
}

func (vtprotoCodec) Unmarshal(data mem.BufferSlice, v any) error {
	// In the common single-buffer case, MaterializeToBuffer just references
	// the received buffer, without copying it. The buffer can be freed right
	// after unmarshaling: UnmarshalVT (and pb.Unmarshal) copy the bytes
	// fields out of it.
	buf := data.MaterializeToBuffer(mem.DefaultBufferPool())
	defer buf.Free()

	switch v := v.(type) {
	case vtprotoMessage:
		return v.UnmarshalVT(buf.ReadOnlyData())
	case pb.Message:
		return pb.Unmarshal(buf.ReadOnlyData(), v)
	default:
		return fmt.Errorf("failed to unmarshal, message is %T, must satisfy the vtprotoMessage interface or want proto.Message", v)
	}
}

func (vtprotoCodec) Name() string {
	return grpc.Name
}

func init() {
	encoding.RegisterCodecV2(vtprotoCodec{})
}
