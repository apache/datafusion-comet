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

set -euo pipefail

if [ "$#" -gt 1 ]; then
  echo "Usage: $0 [git-ref]" >&2
  exit 1
fi

git_ref=${1:-HEAD}
RELEASE_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")"; pwd)
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/comet-source-tarball.XXXXXX")
trap 'rm -rf "${work_dir}"' EXIT

source_tarball=${work_dir}/apache-datafusion-comet-source.tar.gz
"${RELEASE_DIR}/create-source-tarball.sh" \
  "${git_ref}" apache-datafusion-comet-source "${source_tarball}"
"${RELEASE_DIR}/run-rat.sh" "${source_tarball}"
