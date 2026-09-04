#!/usr/bin/env bash
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

# Host-side driver. Only source archives enter the fresh offline container.
set -euo pipefail
script_dir=$(cd "$(dirname "$0")" && pwd)
repo_dir=$(cd "$script_dir/../../.." && pwd)
source "$script_dir/versions.env"
default_image="comet-ci:spark-$SPARK_VERSION-jdk${JDK_VERSION%%.*}"
image=${1:-$default_image}
inputs=$(mktemp -d)
trap 'rm -rf "$inputs"' EXIT
bash "$script_dir/fetch-spark.sh" "$inputs/spark.tar.gz"
# Read tracked working files, including unstaged edits, but never host caches.
# Stage newly added source files with git add before running this script.
git -C "$repo_dir" ls-files -z | \
  tar -C "$repo_dir" --null -T - -czf "$inputs/comet.tar.gz"
exec_args=(
  --rm --platform linux/amd64 --network=none
  --mount "type=bind,src=$inputs,dst=/inputs,readonly"
  # Reproduce GitHub's empty runtime home and workspace mounts.
  --tmpfs /github/home
  --volume /__w
  --env "CARGO_BUILD_JOBS=${COMET_CI_BUILD_JOBS:-4}"
  --env SPARK_LOCAL_IP=127.0.0.1
  --workdir /__w/comet/comet
)
# docker exec/container steps use /github/home even though Java's user.home
# differs. Supply the container environment without changing the host's HOME.
docker run "${exec_args[@]}" --env HOME=/github/home "$image" \
  bash /opt/comet-ci/bin/validate-container.sh
