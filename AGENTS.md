# AGENTS.md

## Scope
- This file applies to the entire repository unless a deeper `AGENTS.md` overrides it.

## Project Overview
- Oxia is a distributed metadata store and coordination system written in Go.
- It uses a sharded architecture for large-scale, cloud-native distributed systems.

## Project Structure
- The repository uses a Go workspace with five modules: `cmd`, `common`, `oxia`, `oxiad`, and `tests`.
- `cmd/` contains CLI and service entry points, including `server`, `coordinator`, `client`, `admin`, `standalone`, `health`, `wal`, and `perf`.
- `common/` contains shared libraries, protobuf definitions in `common/proto/`, RPC utilities, metrics, and security code.
- `oxia/` contains the client library with sync and async client APIs.
- `oxiad/` contains server implementations:
  - `coordinator/` for cluster management, shard orchestration, and leader election.
  - `dataserver/` for Pebble-backed data storage.
  - `dataserver/wal/` for write-ahead log implementation.
- `tests/` contains integration tests.
- `conf/` contains sample configs and generated schema output.

## Architecture
- Clients in `oxia/` communicate over gRPC with the coordinator in `oxiad/coordinator/`.
- The coordinator manages cluster state, shard assignments, and leader balancing.
- Data servers in `oxiad/dataserver/` handle storage and retrieval using WAL-backed Pebble storage.
- The client library provides sync and async APIs with automatic request batching.

## Working Rules
- Keep changes focused and minimal; do not refactor unrelated code.
- Follow existing Go style and package boundaries in the touched area.
- Prefer fixing root causes over adding narrow workarounds.
- Update nearby documentation when behavior, commands, or config shape changes.
- Do not edit generated files directly unless the task explicitly requires regenerated output.

## Build and Test
- Prefer targeted validation first, then broader validation if needed.
- Before creating a pull request, run `make lint` and include the result in the PR testing notes.
- If `make lint` cannot be run, explicitly state the reason before creating the PR.
- `make lint` requires both `jq` and `golangci-lint`.
- Useful commands:
  - `make build` to build the `bin/oxia` binary.
  - `make test` to run tests with coverage and race detection.
  - `make lint` to run `golangci-lint` across workspace modules.
  - `make proto` to regenerate gRPC code from `.proto` files.
  - `make docker` to build the `oxia:latest` Docker image.
  - `make license-check` to verify copyright headers.
  - `make license-format` to format copyright headers.
- Run a single test with `go test -v -run TestName ./path/to/package/...`.
- Avoid running heavyweight repo-wide commands unless they are relevant to the task.

## Code Style
- Keep lines within 120 characters.
- Use `goimports`; import groups should be standard library, external packages, then `github.com/oxia-db/oxia`.
- Existing lint rules expect Apache 2.0 license headers on Go files.
- Keep cyclomatic complexity at or below 17 and cognitive complexity at or below 25.

## Proto Files
- Protocol buffer definitions live in `common/proto/`.
- After modifying `.proto` files, run `make proto`.
- `make proto` requires `protoc-gen-go`, `protoc-gen-go-grpc`, and `protoc-gen-go-vtproto`.

## Generated and Derived Files
- Treat `*.pb.go`, `*.deepcopy.go`, `pkg/generated/`, and `conf/schema/` as generated or derived outputs.
- If source inputs change, regenerate with the existing project commands instead of editing generated output manually.

## Key Dependencies
- Pebble provides LSM-tree key-value storage for persistence.
- gRPC and vtprotobuf provide optimized RPC communication.
- Cobra and Viper provide CLI and configuration support.
- OpenTelemetry provides metrics and tracing support.

## Notes for Agents
- Check for nested `AGENTS.md` files before changing files in subdirectories.
- When adding code, match existing naming, error handling, and test structure in that package.
