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
source /opt/comet-ci/bin/versions.env
test "$(dpkg --print-architecture)" = amd64
apt-get update
apt-get install --no-install-recommends -y \
  "clang=$CLANG_PACKAGE_VERSION" "clang-19=$CLANG19_PACKAGE_VERSION" \
  "protobuf-compiler=$PROTOC_PACKAGE_VERSION" \
  cmake python3 unzip zip
rm -rf /var/lib/apt/lists/*
rustup component add --toolchain "$RUST_VERSION" rustfmt clippy
mkdir -p /opt/comet-ci/home /opt/comet-ci/maven /opt/comet-ci/cargo \
  /opt/comet-ci/coursier /opt/comet-ci/sbt/boot /opt/comet-ci/ivy/cache
ln -s /opt/comet-ci/maven /opt/comet-ci/home/.m2
bash /opt/comet-ci/bin/check-tools.sh
dpkg-query -W > /opt/comet-ci/os-packages.txt
