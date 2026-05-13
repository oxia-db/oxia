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
	"github.com/oxia-db/oxia/common/constant"
	"google.golang.org/grpc"
)

type OxiaErrorServerStreamingClient[T any] struct {
	grpc.ServerStreamingClient[T]
}

func (c OxiaErrorServerStreamingClient[T]) Recv() (*T, error) {
	response, err := c.ServerStreamingClient.Recv()
	oxiaErr, _ := constant.FromGrpcError(err)
	return response, oxiaErr
}

type OxiaErrorBidiStreamingClient[Req any, Res any] struct {
	grpc.BidiStreamingClient[Req, Res]
}

func (c OxiaErrorBidiStreamingClient[Req, Res]) Send(request *Req) error {
	oxiaErr, _ := constant.FromGrpcError(c.BidiStreamingClient.Send(request))
	return oxiaErr
}

func (c OxiaErrorBidiStreamingClient[Req, Res]) Recv() (*Res, error) {
	response, err := c.BidiStreamingClient.Recv()
	oxiaErr, _ := constant.FromGrpcError(err)
	return response, oxiaErr
}

type OxiaErrorClientStreamingClient[Req any, Res any] struct {
	grpc.ClientStreamingClient[Req, Res]
}

func (c OxiaErrorClientStreamingClient[Req, Res]) Send(request *Req) error {
	oxiaErr, _ := constant.FromGrpcError(c.ClientStreamingClient.Send(request))
	return oxiaErr
}

func (c OxiaErrorClientStreamingClient[Req, Res]) CloseAndRecv() (*Res, error) {
	response, err := c.ClientStreamingClient.CloseAndRecv()
	oxiaErr, _ := constant.FromGrpcError(err)
	return response, oxiaErr
}
