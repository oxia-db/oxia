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

.PHONY: build
build:
	go build -v -o bin/oxia ./cmd

.PHONY: maelstrom
maelstrom:
	go build -v -o bin/oxia-maelstrom ./maelstrom

test: build
	go test -cover -race ./...

lint:
	#brew install golangci-lint
	golangci-lint run

clean:
	rm -f bin/oxia bin/oxia-maelstrom
	rm -rf pkg/generated/*
	find . -type f -name '*.deepcopy.go' | xargs rm

docker:
	docker build -t oxia:latest .

docker_multi_arch:
	docker buildx build --platform linux/x86_64,linux/arm64 -t oxia:latest .

.PHONY: proto
proto:
	cd proto && \
	protoc \
		--go_out=. \
		--go_opt paths=source_relative \
		--plugin protoc-gen-go="${GOPATH}/bin/protoc-gen-go" \
    	--go-grpc_out=. \
    	--go-grpc_opt paths=source_relative \
    	--plugin protoc-gen-go-grpc="${GOPATH}/bin/protoc-gen-go-grpc" \
      	--go-vtproto_out=. \
      	--go-vtproto_opt paths=source_relative \
      	--plugin protoc-gen-go-vtproto="${GOPATH}/bin/protoc-gen-go-vtproto" \
  	 	--go-vtproto_opt=features=marshal+unmarshal+size+pool+equal+clone \
	    *.proto

proto_clean:
	rm -f */*.pb.go

proto_format:
	#brew install clang-format
	clang-format -i --style=Google proto/*.proto

proto_lint:
	#go install github.com/yoheimuta/protolint/cmd/protoc-gen-protolint
	protoc --proto_path ./proto \
		--protolint_out . \
		--protolint_opt config_dir_path=. \
		--protolint_opt proto_root=./proto \
		proto/*.proto

proto_doc:
	#go install github.com/pseudomuto/protoc-gen-doc/cmd/protoc-gen-doc
	protoc --doc_out=docs/proto --doc_opt=markdown,proto.md proto/*.proto

proto_quality: proto_format proto_lint

fetch-tla-tools:
	mkdir -p tlaplus/.tools
	cd tlaplus/.tools && \
		wget https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar && \
		wget https://github.com/tlaplus/CommunityModules/releases/download/202211012231/CommunityModules-deps.jar


tla:
	cd tlaplus && \
		java -XX:+UseParallelGC -DTLA-Library=.tools/CommunityModules-deps.jar -jar .tools/tla2tools.jar \
			-deadlock -workers auto\
			OxiaReplication.tla

license-check:
	# go install github.com/palantir/go-license@latest
	find . -type f -name '*.go' | grep -v '.pb.go' | xargs go-license --config=.github/license.yml --verify

license-format:
	# go install github.com/palantir/go-license@latest
	find . -type f -name '*.go' | grep -v '.pb.go' | xargs go-license --config=.github/license.yml
