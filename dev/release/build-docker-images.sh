#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Manual verification (before first use on a real release):
#
#   1. Run dev/release/build-release-comet.sh against a test branch to
#      produce staging jars.
#
#   2. Dry-run against the resulting LOCAL_REPO:
#        ./dev/release/build-docker-images.sh \
#          --version 0.0.0-test \
#          --maven-repo /tmp/comet-staging-repo-XXXXX \
#          --dry-run
#      Confirms arg parsing, jar path resolution, and buildx command shape
#      without touching Docker Hub.
#
#   3. End-to-end smoke test using a personal Docker Hub namespace:
#        - Temporarily edit IMAGE_REPO below to <myuser>/datafusion-comet.
#        - Run the script without --dry-run.
#        - Verify all five manifests pushed with both architectures via:
#            docker buildx imagetools inspect <tag>
#        - Pull one image for each arch and check the jar is present:
#            docker run --rm --platform=linux/amd64 <tag> \
#              ls /opt/spark/jars/ | grep comet
#            docker run --rm --platform=linux/arm64 <tag> \
#              ls /opt/spark/jars/ | grep comet
#        - Revert IMAGE_REPO before committing.

# Build and push multi-arch (linux/amd64 + linux/arm64) Docker images to
# Docker Hub for the five supported Spark x Scala combos, using uber-jars
# produced by dev/release/build-release-comet.sh.
#
# See docs/source/contributor-guide/release_process.md for the full flow.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." >/dev/null && pwd)"

IMAGE_REPO="apache/datafusion-comet"
DOCKERFILE="$REPO_ROOT/kube/release.Dockerfile"

usage() {
  cat <<EOF
Usage: $(basename "$0") --version <comet-version> --maven-repo <path> [--dry-run]

Build and push multi-arch Comet Docker images to Docker Hub.

Required:
  --version <v>       Comet version, e.g. 0.10.0 or 0.10.0-rc1
  --maven-repo <p>    Path to the staging Maven repo printed by
                      build-release-comet.sh, e.g. /tmp/comet-staging-repo-XXXXX

Optional:
  --dry-run           Print the docker buildx commands without executing
  -h, --help          Show this help and exit

Prerequisites:
  - Ran dev/release/build-release-comet.sh to produce the uber-jars
  - docker login to an account with push access to ${IMAGE_REPO}
  - docker buildx with a builder that supports linux/amd64 and linux/arm64

Published tags (one per combo):
  ${IMAGE_REPO}:<version>-spark3.4.3-scala2.12-java17
  ${IMAGE_REPO}:<version>-spark3.4.3-scala2.13-java17
  ${IMAGE_REPO}:<version>-spark3.5.8-scala2.12-java17
  ${IMAGE_REPO}:<version>-spark3.5.8-scala2.13-java17
  ${IMAGE_REPO}:<version>-spark4.0.1-scala2.13-java17
EOF
}

VERSION=""
MAVEN_REPO=""
DRY_RUN=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version)
      VERSION="${2:-}"
      shift 2
      ;;
    --maven-repo)
      MAVEN_REPO="${2:-}"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Error: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$VERSION" ]]; then
  echo "Error: --version is required" >&2
  usage >&2
  exit 2
fi

if [[ -z "$MAVEN_REPO" ]]; then
  echo "Error: --maven-repo is required" >&2
  usage >&2
  exit 2
fi

if [[ ! -d "$MAVEN_REPO" ]]; then
  echo "Error: --maven-repo path does not exist: $MAVEN_REPO" >&2
  exit 2
fi

echo "Version:    $VERSION"
echo "Maven repo: $MAVEN_REPO"
echo "Dry run:    $DRY_RUN"

if ! command -v docker >/dev/null 2>&1; then
  echo "Error: docker is not on PATH" >&2
  exit 2
fi

if ! docker buildx version >/dev/null 2>&1; then
  echo "Error: docker buildx is not available" >&2
  echo "  Install: https://docs.docker.com/build/install-buildx/" >&2
  exit 2
fi

builder_platforms=$(docker buildx inspect --bootstrap 2>/dev/null | sed -n 's/^[[:space:]]*Platforms:[[:space:]]*//p' | tr ',' '\n' | tr -d ' ')
if ! grep -q '^linux/amd64$' <<<"$builder_platforms" \
   || ! grep -q '^linux/arm64$' <<<"$builder_platforms"; then
  echo "Error: active buildx builder does not support both linux/amd64 and linux/arm64" >&2
  echo "  Current platforms: $(tr '\n' ',' <<<"$builder_platforms" | sed 's/,$//')" >&2
  echo "  Create one with: docker buildx create --use --name multiarch" >&2
  exit 2
fi

if [[ "$DRY_RUN" -eq 0 ]] && ! grep -q '"auths"' "${HOME}/.docker/config.json" 2>/dev/null; then
  echo "Warning: no docker credentials found in ~/.docker/config.json." >&2
  echo "  Run 'docker login' if push fails." >&2
fi

BUILD_CONTEXTS=()
cleanup_contexts() {
  for ctx in "${BUILD_CONTEXTS[@]:-}"; do
    [[ -n "${ctx:-}" && -d "${ctx}" ]] && rm -rf "${ctx}"
  done
}
trap cleanup_contexts EXIT

# spark_short spark_full scala base_image
COMBOS=(
  "3.4 3.4.3 2.12 apache/spark:3.4.3-java17"
  "3.4 3.4.3 2.13 apache/spark:3.4.3-scala2.13-java17"
  "3.5 3.5.8 2.12 apache/spark:3.5.8-java17"
  "3.5 3.5.8 2.13 apache/spark:3.5.8-scala2.13-java17"
  "4.0 4.0.1 2.13 apache/spark:4.0.1"
)

PUSHED_TAGS=()

for combo in "${COMBOS[@]}"; do
  read -r spark_short spark_full scala base_image <<<"$combo"

  artifact="comet-spark-spark${spark_short}_${scala}"
  jar_path="$MAVEN_REPO/org/apache/comet/${artifact}/${VERSION}/${artifact}-${VERSION}.jar"
  tag="${IMAGE_REPO}:${VERSION}-spark${spark_full}-scala${scala}-java17"

  if [[ ! -f "$jar_path" ]]; then
    echo "Error: Comet jar not found for Spark ${spark_full} / Scala ${scala}" >&2
    echo "  Expected: $jar_path" >&2
    echo "  Run dev/release/build-release-comet.sh first, and pass its" >&2
    echo "  printed LOCAL_REPO path as --maven-repo." >&2
    exit 1
  fi

  echo
  echo "=== Combo: Spark ${spark_full} / Scala ${scala} ==="
  echo "  Base image: ${base_image}"
  echo "  Jar path:   ${jar_path}"
  echo "  Tag:        ${tag}"

  build_context=$(mktemp -d)
  BUILD_CONTEXTS+=("$build_context")
  cp "$jar_path" "$build_context/comet.jar"

  buildx_cmd=(
    docker buildx build
    --platform linux/amd64,linux/arm64
    --build-arg "SPARK_IMAGE=${base_image}"
    --build-arg "COMET_JAR=comet.jar"
    -f "${DOCKERFILE}"
    -t "${tag}"
    --push
    "${build_context}"
  )

  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "  Would run:"
    for arg in "${buildx_cmd[@]}"; do
      printf '    %q\n' "$arg"
    done
  else
    "${buildx_cmd[@]}"
  fi

  PUSHED_TAGS+=("$tag")
done

echo
echo "=== Summary ==="
if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "Dry run - nothing was pushed. Would have pushed:"
else
  echo "Pushed tags:"
fi
for t in "${PUSHED_TAGS[@]}"; do
  echo "  $t"
done
