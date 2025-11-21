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

.PHONY: build
build:
	go build -ldflags "-X main.version=$(shell git describe --tags --always | cut -c2-)" -v -o bin/oxia ./cmd

# Build target: iterate through PLATFORMS and perform cross-compilation for each combination.

PLATFORMS := \
	linux/amd64 \
	windows/amd64 \
	darwin/amd64

.PHONY: distribution
distribution:
	@echo "Starting Go multi-platform executable build..."

	@for platform in $(PLATFORMS); do \
		GOOS=$$(echo $$platform | cut -d'/' -f1); \
		GOARCH=$$(echo $$platform | cut -d'/' -f2); \
		OUTPUT_NAME=bin/oxia_$${GOOS}_$${GOARCH}; \
		if [ "$$GOOS" = "windows" ]; then OUTPUT_NAME=$${OUTPUT_NAME}.exe; fi; \
		echo "Building $$GOOS/$${GOARCH} to $${OUTPUT_NAME}"; \
		GOOS=$${GOOS} GOARCH=$${GOARCH} go build \
			-ldflags "-X main.version=$$(git describe --tags --always | cut -c2-)" \
			-v \
			-o $${OUTPUT_NAME} ./cmd; \
	done
	@echo "All platforms built successfully! Executables are located in the bin/ directory."

.PHONY: maelstrom
maelstrom:
	go build -v -o bin/oxia-maelstrom ./maelstrom

test: build
	go test -cover -race ./cmd/... ./common/... ./oxia/... ./oxiad/... ./proto/... ./tests/...

lint:
	#brew install golangci-lint
	golangci-lint run -v

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
	# go install github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto@latest
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
      	--go-vtproto_opt=features=marshal+unmarshal+unmarshal_unsafe+size+pool+equal+clone \
	    *.proto compat/*.proto

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
	@command -v go-license > /dev/null || go install github.com/palantir/go-license@latest
	find . -type f -name '*.go' | grep -v '.pb.go' | xargs go-license --config=.github/license.yml --verify

license-format:
	@command -v go-license > /dev/null || go install github.com/palantir/go-license@latest
	find . -type f -name '*.go' | grep -v '.pb.go' | xargs go-license --config=.github/license.yml
