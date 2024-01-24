# Copyright 2023 StreamNative, Inc.
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

FROM golang:1.21-alpine as build

RUN apk add --no-cache make git build-base bash

ENV PATH=$PATH:/go/bin
ADD . /src/oxia

RUN cd /src/oxia \
    && make

FROM alpine:3.19

RUN apk add --no-cache bash bash-completion

# Fix CVE-2023-5363 by upgrading to OpenSSL 3.1.4-r4
# We can remove once new Alpine image is released
RUN apk upgrade --no-cache libssl3 libcrypto3

RUN mkdir /oxia
WORKDIR /oxia

COPY --from=build /src/oxia/bin/oxia /oxia/bin/oxia
ENV PATH=$PATH:/oxia/bin

RUN oxia completion bash > ~/.bashrc

CMD /bin/bash