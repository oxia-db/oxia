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

package codec

import (
	gproto "google.golang.org/protobuf/proto"
	yamlv3 "gopkg.in/yaml.v3"

	commonproto "github.com/oxia-db/oxia/common/proto"
)

type Codec[T gproto.Message] interface {
	Clone(value T) T
	NewZero() T
	UnmarshalYAML(data []byte) (T, error)
	MarshalYAML(value T) ([]byte, error)
	UnmarshalJSON(data []byte) (T, error)
	MarshalJSON(value T) ([]byte, error)
	GetKey() string
}

type statusCodec struct{}

type configCodec struct{}

var (
	ClusterStatusCodec Codec[*commonproto.ClusterStatus]        = statusCodec{}
	ClusterConfigCodec Codec[*commonproto.ClusterConfiguration] = configCodec{}
)

// marshalYAML encodes the message through its yaml tags, which keep the
// numeric shapes of the 0.16.x metadata. It encodes a clone: yaml.v3 copies
// the message structs, which reads their protobuf state without
// synchronization, while the protobuf runtime stores into that state the first
// time it reflects a message, e.g. when another goroutine logs a sub-message
// that the value shares with it.
func marshalYAML(value gproto.Message) ([]byte, error) {
	return yamlv3.Marshal(gproto.Clone(value))
}
