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

# builds a comet binary
REPO=$1
BRANCH=$2
ARCH=$3

function usage {
  local NAME=$(basename $0)
  echo "Usage: ${NAME} [git repo] [branch] [arm64 | amd64]"
  exit 1
}

if [ $# -ne 3 ]
then
  usage
fi

if [ "$ARCH" != "arm64" ] && [ "$ARCH" != "amd64" ]
then
  usage
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd)"

echo "Building architecture: ${ARCH} for ${REPO}/${BRANCH}"
rm -fr comet
git clone "$REPO" comet
cd comet
git checkout "$BRANCH"

# build comet binaries

make core-${ARCH}-libs
