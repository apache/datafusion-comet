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

set -euo pipefail
script_dir=$(cd "$(dirname "$0")" && pwd)
repo_dir=$(cd "$script_dir/../../.." && pwd)
source "$script_dir/versions.env"
default_image="comet-ci:spark-$SPARK_VERSION-jdk${JDK_VERSION%%.*}"
image=${1:-$default_image}
target=${2:-ci}
case "$target" in ci|toolchain) ;; *) echo "Target must be ci or toolchain" >&2; exit 2 ;; esac
revision=$(git -C "$repo_dir" rev-parse HEAD)
if [ -n "$(git -C "$repo_dir" status --porcelain)" ]; then
  revision="$revision-dirty"
fi
exec docker build --platform linux/amd64 --progress plain \
  --build-arg "SOURCE_REVISION=$revision" \
  --build-arg "BUILD_JOBS=${COMET_CI_BUILD_JOBS:-4}" \
  --target "$target" --tag "$image" --file "$script_dir/Dockerfile" "$repo_dir"
