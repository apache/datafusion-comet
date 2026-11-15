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

set -e

REPO=$1
COMMIT=$2
ARCH=$3

function usage {
  local NAME=$(basename $0)
  echo "Usage: ${NAME} [git repo] [commit] [arm64 | amd64]"
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

echo "Building architecture: ${ARCH} for ${REPO} at commit ${COMMIT}"
rm -fr comet
git clone "$REPO" comet
cd comet
git checkout "$COMMIT"

# Build the native library twice, once with the system allocator and once with the jemalloc
# allocator, and keep each build in its own directory under /opt/comet-rm/lib. The caller copies
# them out. Cargo reuses the compiled dependencies, so the second build only compiles jemalloc and
# the comet crate again.
LIB_DIR=/opt/comet-rm/lib
mkdir -p "$LIB_DIR/default" "$LIB_DIR/jemalloc"

make core-${ARCH}-libs
cp native/target/release/libcomet.so "$LIB_DIR/default/"

# A jemalloc built on a kernel with small memory pages fails every allocation on a kernel with
# larger pages, and some arm64 distributions use 64 KiB pages. The published arm64 library must
# load on all of them, so build jemalloc for 64 KiB pages. That also works on kernels with smaller
# pages, but can raise memory use there. amd64 kernels always use 4 KiB pages.
if [ "$ARCH" = "arm64" ]
then
  export JEMALLOC_SYS_WITH_LG_PAGE=16
fi

COMET_FEATURES=jemalloc make core-${ARCH}-libs
cp native/target/release/libcomet.so "$LIB_DIR/jemalloc/"

# The jemalloc library must differ from the default one, otherwise the feature was not applied
if cmp -s "$LIB_DIR/default/libcomet.so" "$LIB_DIR/jemalloc/libcomet.so"
then
  echo "Error: the jemalloc build of libcomet.so is identical to the default build"
  exit 1
fi
