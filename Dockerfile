# Copyright 2023-2025 The Oxia Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM golang:1.25-alpine AS build

RUN apk add --no-cache make git build-base bash

WORKDIR /src/oxia

# Copy only Go mod files first (for better caching)
COPY go.work go.work.sum ./

# Download dependencies with BuildKit cache mounts
RUN --mount=type=cache,target=/go/pkg/mod \
    go work sync

# Copy the rest of the source
COPY . .

# Build using cache mounts for build cache and module cache
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    make

FROM alpine:3.22

RUN apk add --no-cache bash bash-completion jq
RUN apk upgrade --no-cache

WORKDIR /oxia

COPY --from=build /src/oxia/bin/oxia /oxia/bin/oxia
ENV PATH=$PATH:/oxia/bin

RUN oxia completion bash > ~/.bashrc

CMD [ "/bin/bash" ]
