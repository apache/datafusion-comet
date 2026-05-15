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
# Rebuild Comet for Delta regression testing: native dylib + Scala jar,
# installed to the local Maven repo the Delta regression sbt build reads from.
#
# Usage: dev/build-comet-delta.sh [--native-only|--scala-only]
#
# Default: build both. `--native-only` skips the Scala `mvnw install` step for
# iteration on Rust-only changes; `--scala-only` skips cargo for Scala-only
# changes. Exits non-zero on any failure.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export JAVA_HOME="${JAVA_HOME:-$HOME/jdks/jdk-17.0.18+8/Contents/Home}"

MODE="${1:-all}"

build_native() {
  echo "==> cargo build --release (native)"
  cd "$REPO_ROOT/native"
  cargo build --release --lib
  echo "==> copy libcomet.dylib into common/target/classes"
  cp "$REPO_ROOT/native/target/release/libcomet.dylib" \
     "$REPO_ROOT/common/target/classes/org/apache/comet/darwin/aarch64/libcomet.dylib"
}

build_scala() {
  echo "==> mvnw install (spark module + deps, spotless, no tests)"
  cd "$REPO_ROOT"
  ./mvnw spotless:apply -Pspark-3.5 -pl spark -q
  ./mvnw install -Prelease -DskipTests -Pspark-3.5 -pl spark -am -q
}

case "$MODE" in
  --native-only) build_native ;;
  --scala-only)  build_scala ;;
  all|"")        build_native && build_scala ;;
  *) echo "usage: $0 [--native-only|--scala-only]"; exit 2 ;;
esac

echo "==> installed jar:"
ls -la "$HOME/.m2/repository/org/apache/datafusion/comet-spark-spark3.5_2.12/0.15.0-SNAPSHOT/" \
  | grep -v sources | grep jar | head -2
