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
# Run one or more Delta scalatest test selectors via build/sbt in the extracted
# Delta regression checkout (Delta 4.1.0 / Spark 4.1 by default; override with
# DELTA_VERSION).
#
# Usage: dev/run-delta-test.sh 'org.apache.spark.sql.delta.SomeSuite -- -z "test substring"' [...more testOnly selectors]
#
# Each argument is passed as a separate `spark/testOnly` command. Output goes to
# target/delta-regression-logs/test-<timestamp>.log (relative to this repo).
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
DELTA_VERSION="${DELTA_VERSION:-4.1.0}"
DELTA_DIR="${DELTA_DIR:-${DELTA_WORKDIR:-${TMPDIR:-/tmp}/delta-regression-${DELTA_VERSION}}}"
# Honour an existing JAVA_HOME; otherwise try the macOS java_home helper for a JDK >=17
# (Delta 4.x needs Java 17 for java.lang.Record). Do NOT hardcode a developer-specific
# path -- if nothing is found, leave JAVA_HOME unset and let sbt use `java` on PATH.
if [[ -z "${JAVA_HOME:-}" ]]; then
  JAVA_HOME="$(/usr/libexec/java_home -v 17 2>/dev/null || true)"
  [[ -n "$JAVA_HOME" ]] && export JAVA_HOME || unset JAVA_HOME
fi
export SPARK_LOCAL_IP=127.0.0.1
export RUST_BACKTRACE=1

if [ $# -lt 1 ]; then
  echo "usage: $0 'SuiteClass -- -z \"name\"' [...]"
  exit 2
fi

LOG="$REPO_ROOT/target/delta-regression-logs/test-$(date +%Y%m%d-%H%M%S).log"
mkdir -p "$(dirname "$LOG")"

cmds=()
for sel in "$@"; do
  cmds+=("spark/testOnly $sel")
done

cd "$DELTA_DIR"
echo "==> logging to $LOG"
build/sbt "${cmds[@]}" 2>&1 | tee "$LOG"
