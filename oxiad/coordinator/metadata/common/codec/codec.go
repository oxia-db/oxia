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
