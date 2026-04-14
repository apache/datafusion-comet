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

# Run Delta Lake's own test suite with Comet enabled as a regression check.
# Mirrors what .github/workflows/delta_regression_test.yml does in CI.
#
# Usage:
#   dev/run-delta-regression.sh [DELTA_VERSION] [TEST_FILTER]
#
# Examples:
#   dev/run-delta-regression.sh                             # smoke test on default version (3.3.2)
#   dev/run-delta-regression.sh 3.3.2                       # smoke test on Delta 3.3.2
#   dev/run-delta-regression.sh 3.3.2 full                  # full Delta test suite
#   dev/run-delta-regression.sh 3.3.2 DeltaTimeTravelSuite  # one specific test class
#   DELTA_WORKDIR=/tmp/my-delta dev/run-delta-regression.sh # reuse a checkout

set -euo pipefail

DELTA_VERSION="${1:-3.3.2}"
TEST_FILTER="${2:-smoke}"

# Map Delta version -> Spark short version -> SBT module -> expected default Scala
case "$DELTA_VERSION" in
  2.4.0) SPARK_SHORT="3.4"; SBT_MODULE="core" ;;
  3.3.2) SPARK_SHORT="3.5"; SBT_MODULE="spark" ;;
  4.0.0) SPARK_SHORT="4.0"; SBT_MODULE="spark" ;;
  *)
    echo "Error: unsupported Delta version '$DELTA_VERSION'"
    echo "Supported: 2.4.0 (Spark 3.4), 3.3.2 (Spark 3.5), 4.0.0 (Spark 4.0)"
    exit 1
    ;;
esac

COMET_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIFF_FILE="$COMET_ROOT/dev/diffs/delta/${DELTA_VERSION}.diff"
DELTA_WORKDIR="${DELTA_WORKDIR:-${TMPDIR:-/tmp}/delta-regression-${DELTA_VERSION}}"

if [[ ! -f "$DIFF_FILE" ]]; then
  echo "Error: diff file not found: $DIFF_FILE"
  exit 1
fi

echo "=========================================="
echo "Delta regression run"
echo "  Delta version : $DELTA_VERSION"
echo "  Spark profile : spark-$SPARK_SHORT"
echo "  SBT module    : $SBT_MODULE"
echo "  Test filter   : $TEST_FILTER"
echo "  Work dir      : $DELTA_WORKDIR"
echo "  Comet root    : $COMET_ROOT"
echo "=========================================="

# Step 1: build + install Comet to local Maven repo for the target Spark profile.
echo
echo "[1/4] Building and installing Comet (spark-$SPARK_SHORT)..."
cd "$COMET_ROOT"
./mvnw install -Prelease -DskipTests -Pspark-"$SPARK_SHORT"

# Step 2: clone Delta (or reuse existing checkout).
echo
echo "[2/4] Cloning Delta $DELTA_VERSION..."
if [[ -d "$DELTA_WORKDIR/.git" ]]; then
  echo "  Reusing existing checkout at $DELTA_WORKDIR"
  cd "$DELTA_WORKDIR"
  git fetch --depth 1 origin "refs/tags/v$DELTA_VERSION:refs/tags/v$DELTA_VERSION" 2>/dev/null || true
  git checkout -f "v$DELTA_VERSION"
  git clean -fd
else
  rm -rf "$DELTA_WORKDIR"
  git clone --depth 1 --branch "v$DELTA_VERSION" https://github.com/delta-io/delta.git "$DELTA_WORKDIR"
  cd "$DELTA_WORKDIR"
fi

# Step 3: apply the Comet diff.
echo
echo "[3/4] Applying diff $DIFF_FILE..."
git apply "$DIFF_FILE"

# Step 4: run tests.
echo
echo "[4/4] Running tests..."
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-localhost}"

# DELTA_JAVA_HOME lets the caller run Delta's SBT under a different JDK from the one
# that built Comet. Useful for Delta 2.4.0, whose pinned SBT 1.5.5 doesn't launch on
# JDK 17+. Typical usage: `DELTA_JAVA_HOME=$(/usr/libexec/java_home -v 1.8)`.
if [[ -n "${DELTA_JAVA_HOME:-}" ]]; then
  echo "  Using DELTA_JAVA_HOME=$DELTA_JAVA_HOME for SBT"
  export JAVA_HOME="$DELTA_JAVA_HOME"
  export PATH="$DELTA_JAVA_HOME/bin:$PATH"
fi

case "$TEST_FILTER" in
  smoke)
    build/sbt "$SBT_MODULE/testOnly org.apache.spark.sql.delta.CometSmokeTest"
    ;;
  full)
    build/sbt "$SBT_MODULE/test"
    ;;
  *)
    # Treat as a test class / glob - pass through to sbt testOnly.
    build/sbt "$SBT_MODULE/testOnly $TEST_FILTER"
    ;;
esac

echo
echo "Done."
