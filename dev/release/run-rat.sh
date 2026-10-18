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

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <source-tarball>" >&2
  exit 1
fi

source_tarball=$1
if [ ! -f "${source_tarball}" ]; then
  echo "Source tarball does not exist: ${source_tarball}" >&2
  exit 1
fi

RAT_VERSION=0.16.1
RELEASE_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")"; pwd)
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/comet-rat.XXXXXX")
trap 'rm -rf "${work_dir}"' EXIT

rat_jar=${work_dir}/apache-rat-${RAT_VERSION}.jar
rat_report=${work_dir}/rat.xml
filtered_report=${work_dir}/filtered-rat.txt

curl --fail --location --retry 4 --retry-all-errors --silent --show-error \
  --output "${rat_jar}" \
  "https://repo.maven.apache.org/maven2/org/apache/rat/apache-rat/${RAT_VERSION}/apache-rat-${RAT_VERSION}.jar"

java -jar "${rat_jar}" -x "${source_tarball}" > "${rat_report}"

if python3 "${RELEASE_DIR}/check-rat-report.py" \
  "${RELEASE_DIR}/rat_exclude_files.txt" "${rat_report}" > "${filtered_report}"; then
  echo "No unapproved licenses"
else
  cat "${filtered_report}"
  echo "Apache RAT found unapproved licenses" >&2
  exit 1
fi
