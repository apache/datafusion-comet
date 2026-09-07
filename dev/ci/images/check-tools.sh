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
[[ "$(rustc --version)" == "rustc $RUST_VERSION "* ]]
grep -Fx "JAVA_VERSION=\"$JDK_VERSION\"" "$JAVA_HOME/release"
test -r "$JAVA_HOME/lib/security/cacerts"
test "$(dpkg-query -W -f='${Version}' clang)" = "$CLANG_PACKAGE_VERSION"
test "$(dpkg-query -W -f='${Version}' clang-19)" = "$CLANG19_PACKAGE_VERSION"
test "$(dpkg-query -W -f='${Version}' protobuf-compiler)" = "$PROTOC_PACKAGE_VERSION"
rustfmt --version
cargo clippy --version
java -version
clang --version
protoc --version
