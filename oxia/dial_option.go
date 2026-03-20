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

package oxia

import (
	"google.golang.org/grpc"
)

// DialOption configures how the Oxia client establishes connections to
// the server. Implementations are created by package-level functions
// such as WithResolver.
type DialOption interface {
	// toGrpcDialOption converts the Oxia dial option into one or more
	// gRPC dial options. This is intentionally unexported so that
	// callers do not depend on gRPC types directly.
	toGrpcDialOption() grpc.DialOption
}
