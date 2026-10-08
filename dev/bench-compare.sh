#!/usr/bin/env bash
# Copyright 2023-2026 The Oxia Authors
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

# Compares BenchmarkE2E between a base ref and the current working tree.
#
#   dev/bench-compare.sh [base-ref]     (default: main)
#
# Env: COUNT (runs per side, default 6), BENCHTIME (default 3s), BENCH (default BenchmarkE2E)
#
# The benchmark file from the working tree is copied into the base tree, so the
# base ref doesn't need to contain it. Runs alternate base/head to spread any
# thermal or background noise evenly across both sides.

set -euo pipefail

BASE=${1:-main}
COUNT=${COUNT:-6}
BENCHTIME=${BENCHTIME:-3s}
BENCH=${BENCH:-BenchmarkE2E}
PKG=oxiad/dataserver
BENCH_FILE=$PKG/e2e_benchmark_test.go

ROOT=$(git rev-parse --show-toplevel)
OUT=$(mktemp -d)
trap 'rm -rf "$OUT/base-src"' EXIT

echo "Building base ($BASE) and head test binaries in $OUT"
mkdir "$OUT/base-src"
git -C "$ROOT" archive "$BASE" | tar -x -C "$OUT/base-src"
cp "$ROOT/$BENCH_FILE" "$OUT/base-src/$BENCH_FILE"
(cd "$OUT/base-src/$PKG" && go test -c -o "$OUT/base.test" .)
(cd "$ROOT/$PKG" && go test -c -o "$OUT/head.test" .)

for i in $(seq 1 "$COUNT"); do
  echo "Run $i/$COUNT"
  for side in base head; do
    "$OUT/$side.test" -test.run '^$' -test.bench "$BENCH" -test.benchtime "$BENCHTIME" \
      -test.count 1 >>"$OUT/$side.txt"
  done
done

go run golang.org/x/perf/cmd/benchstat@latest "base=$OUT/base.txt" "head=$OUT/head.txt"
echo "Raw results: $OUT/base.txt $OUT/head.txt"
