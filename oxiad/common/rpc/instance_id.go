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

	"github.com/emirpasic/gods/v2/sets/hashset"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/oxia-db/oxia/common/constant"
	"github.com/oxia-db/oxia/common/proto"
	manifestpkg "github.com/oxia-db/oxia/oxiad/dataserver/manifest"
)

var InsIDValidationMethodsWhiteList = hashset.New(
	proto.OxiaCoordination_Handshake_FullMethodName,
	grpc_health_v1.Health_Check_FullMethodName,
	grpc_health_v1.Health_Watch_FullMethodName,
)

func NewGrpcInsIDVerifyInterceptors(manifest *manifestpkg.Manifest) *Interceptors {
	validate := func(ctx context.Context, fullMethod string) error {
		if InsIDValidationMethodsWhiteList.Contains(fullMethod) {
			return nil
		}

		expected := manifest.GetInstanceID()
		if expected == "" {
			return constant.ErrNotInitialized
		}

		instanceID, err := getIncomingInstanceID(ctx)
		if err != nil {
			return err
		}
		if instanceID != expected {
			return status.Errorf(codes.PermissionDenied, "oxia: unexpected instance id %q", instanceID)
		}
		return nil
	}

	return &Interceptors{
		Unary: []grpc.UnaryServerInterceptor{
			func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
				if err := validate(ctx, info.FullMethod); err != nil {
					return nil, err
				}
				return handler(ctx, req)
			},
		},
		Stream: []grpc.StreamServerInterceptor{
			func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
				if err := validate(ss.Context(), info.FullMethod); err != nil {
					return err
				}
				return handler(srv, ss)
			},
		},
	}
}

func getIncomingInstanceID(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "oxia: instance id not identified")
	}

	values := md.Get(constant.MetadataInstanceId)
	switch len(values) {
	case 0:
		return "", status.Error(codes.Unauthenticated, "oxia: instance id not identified")
	case 1:
		if values[0] == "" {
			return "", status.Error(codes.InvalidArgument, "oxia: instance id must not be empty")
		}
		return values[0], nil
	default:
		return "", status.Errorf(codes.InvalidArgument, "oxia: instance id metadata %q must be provided only once", constant.MetadataInstanceId)
	}
}
