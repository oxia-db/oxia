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

package batch

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/constant"
)

func isRetriable(err error) bool {
	code := status.Code(err)
	switch code {
	case
		codes.Unavailable,            // Failure to connect is ok to re-attempt
		constant.CodeInvalidStatus,   // Leader has fenced the shard, though we expect a new leader to be elected
		constant.CodeAlreadyClosed,   // Leader is closing, though we expect a new leader to be elected
		constant.CodeNodeIsNotLeader: /* We're making a request to a node that is not leader anymore. Retry to make
		   the request to the new leader */
		return true
	default:
		return false
	}
}
