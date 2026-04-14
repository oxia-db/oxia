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

package rpc

import (
	"context"
	"net"
	"strings"

	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func GetAuthority(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		if authority := md.Get(":authority"); len(authority) > 0 {
			if err := validateAuthorityAddress(authority[0]); err != nil {
				return "", status.Errorf(codes.InvalidArgument, "oxia: invalid authority address: %v", err)
			}
			return authority[0], nil
		}
	}
	return "", status.Errorf(codes.InvalidArgument, "oxia: authority not identified")
}

func validateAuthorityAddress(addr string) error {
	if strings.Contains(addr, "://") {
		return errors.Errorf("authority address %q must not contain a scheme", addr)
	}
	if strings.Contains(addr, "/") {
		return errors.Errorf("authority address %q must not contain a path", addr)
	}
	if strings.Contains(addr, "?") {
		return errors.Errorf("authority address %q must not contain a query string", addr)
	}
	if strings.Contains(addr, "#") {
		return errors.Errorf("authority address %q must not contain a fragment", addr)
	}

	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return errors.Errorf("authority address %q is not a valid host:port pair: %v", addr, err)
	}
	if host == "" {
		return errors.Errorf("authority address %q has an empty host", addr)
	}

	return nil
}
