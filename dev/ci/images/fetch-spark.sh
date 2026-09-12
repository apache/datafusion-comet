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

# Download sources before starting the container with --network=none.
set -euo pipefail
script_dir=$(cd "$(dirname "$0")" && pwd)
source "$script_dir/versions.env"
if [ "$#" -ne 1 ]; then
  echo "Usage: $0 OUTPUT_TAR_GZ" >&2
  exit 2
fi
archive=$1
if [ ! -f "$archive" ]; then
  curl --fail --location --retry 3 --retry-delay 5 --connect-timeout 30 \
    --output "$archive.part" \
    "https://codeload.github.com/apache/spark/tar.gz/$SPARK_COMMIT"
  mv "$archive.part" "$archive"
fi
printf '%s  %s\n' "$SPARK_ARCHIVE_SHA256" "$archive" | sha256sum --check
