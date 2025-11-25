#!/usr/bin/env bash
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

set -euo pipefail

# Usage: ./tag-modules.sh v1.2.3
VERSION="${1:-}"

if [[ -z "$VERSION" ]]; then
  echo "❌ Version is required. Usage: ./tag-modules.sh v1.2.3"
  exit 1
fi

MODULES=(
  "oxia"
  "common"
)

NEW_TAGS=()

echo "Tagging version: $VERSION"
echo

# Always tag root module
ROOT_TAG="$VERSION"
echo "Creating tag for root module: $ROOT_TAG"
git tag "$ROOT_TAG"
NEW_TAGS+=("$ROOT_TAG")

# Tag submodules if they exist
for module in "${MODULES[@]}"; do
  if [[ -f "$module/go.mod" ]]; then
    TAG="${module}/${VERSION}"
    echo "Creating tag for module $module: $TAG"
    git tag "$TAG"
    NEW_TAGS+=("$TAG")
  else
    echo "⚠️  Skipping $module (no go.mod found)"
  fi
done

echo
echo "Pushing only newly created tags:"
printf ' - %s\n' "${NEW_TAGS[@]}"

for tag in "${NEW_TAGS[@]}"; do
  git push origin "$tag"
done

echo "✅ Done!"