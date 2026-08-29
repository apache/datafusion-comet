#!/usr/bin/env bash
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

# Run from the patched Spark checkout after Comet's Maven install. SBT's
# update task resolves dependencies without compiling Spark or running tests.
set -euo pipefail

if [ "$#" -eq 0 ]; then
  echo "Usage: $0 catalyst|sql|hive [...]" >&2
  exit 2
fi

update_tasks=()
for project in "$@"; do
  case "$project" in
    catalyst|sql|hive) update_tasks+=("$project/Test/update") ;;
    *) echo "Unsupported Spark dependency project: $project" >&2; exit 2 ;;
  esac
done

script_dir=$(cd "$(dirname "$0")" && pwd)
export NOLINT_ON_COMPILE=true
exec "$script_dir/retry-download.sh" build/sbt -batch -Dsbt.log.noformat=true \
  -mem 1024 "${update_tasks[@]}"
