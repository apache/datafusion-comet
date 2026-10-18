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

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <git-ref> <archive-prefix> <output-tarball>" >&2
  exit 1
fi

git_ref=$1
archive_prefix=${2%/}
output_tarball=$3

if [ -z "${archive_prefix}" ]; then
  echo "Archive prefix must not be empty" >&2
  exit 1
fi

RELEASE_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")"; pwd)
REPO_DIR=$(cd "${RELEASE_DIR}/../.."; pwd)

mkdir -p "$(dirname "${output_tarball}")"
temporary_tarball=$(mktemp "${output_tarball}.tmp.XXXXXX")
trap 'rm -f "${temporary_tarball}"' EXIT

(cd "${REPO_DIR}" && git archive "${git_ref}" --prefix="${archive_prefix}/") \
  | gzip > "${temporary_tarball}"
mv "${temporary_tarball}" "${output_tarball}"
trap - EXIT
