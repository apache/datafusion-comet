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
# This is the PR2 (contrib) variant: the install step bundles the Delta
# contrib via `-Pcontrib-delta` so the comet-spark JAR being installed
# carries DeltaScanRuleExtension/DeltaOperatorSerdeExtension and the
# matching JNI symbols (built into libcomet via `--features contrib-delta`
# on the native crate). Without `-Pcontrib-delta` the installed comet-spark
# JAR has no Delta wiring and Delta tests would just exercise vanilla Spark.
#
# Usage:
#   dev/run-delta-regression.sh [DELTA_VERSION] [TEST_FILTER]
#
# Examples:
#   dev/run-delta-regression.sh                             # smoke on default (4.1.0)
#   dev/run-delta-regression.sh 4.1.0                       # smoke on Delta 4.1.0
#   dev/run-delta-regression.sh 4.1.0 full                  # full Delta test suite
#   dev/run-delta-regression.sh 3.3.2 lite                  # 3.3.2 minus the Delta-only
#                                                           # clone families (~few hours
#                                                           # vs ~29h `full`) -- see
#                                                           # 3.3.2.diff comment
#   dev/run-delta-regression.sh 4.1.0 DeltaTimeTravelSuite  # one specific test class
#   DELTA_WORKDIR=/tmp/my-delta dev/run-delta-regression.sh # reuse a checkout

set -euo pipefail

DELTA_VERSION="${1:-4.1.0}"
TEST_FILTER="${2:-smoke}"

# Map Delta version -> Spark short version -> SBT module
case "$DELTA_VERSION" in
  2.4.0) SPARK_SHORT="3.4"; SBT_MODULE="core" ;;
  3.3.2) SPARK_SHORT="3.5"; SBT_MODULE="spark" ;;
  4.0.0) SPARK_SHORT="4.0"; SBT_MODULE="spark" ;;
  4.1.0) SPARK_SHORT="4.1"; SBT_MODULE="spark" ;;
  *)
    echo "Error: unsupported Delta version '$DELTA_VERSION'"
    echo "Supported: 2.4.0 (Spark 3.4), 3.3.2 (Spark 3.5), 4.0.0 (Spark 4.0), 4.1.0 (Spark 4.1)"
    exit 1
    ;;
esac

# Script lives at contrib/delta/dev/run-regression.sh, so COMET_ROOT is three levels up.
COMET_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
DIFF_FILE="$COMET_ROOT/contrib/delta/dev/diffs/${DELTA_VERSION}.diff"
DELTA_WORKDIR="${DELTA_WORKDIR:-${TMPDIR:-/tmp}/delta-regression-${DELTA_VERSION}}"

if [[ ! -f "$DIFF_FILE" ]]; then
  echo "Error: diff file not found: $DIFF_FILE"
  exit 1
fi

echo "=========================================="
echo "Delta regression run (contrib variant)"
echo "  Delta version : $DELTA_VERSION"
echo "  Spark profile : spark-$SPARK_SHORT"
echo "  SBT module    : $SBT_MODULE"
echo "  Test filter   : $TEST_FILTER"
echo "  Work dir      : $DELTA_WORKDIR"
echo "  Comet root    : $COMET_ROOT"
echo "=========================================="

# Step 1: build + install Comet to local Maven repo for the target Spark profile,
# with the Delta contrib bundled into comet-spark.jar.
#
# `FAST=1` skips plugin checks that aren't relevant during iteration:
#   - drop `-Prelease` (no source/javadoc/scaladoc jars, no GPG prep)
#   - skip spotless check (run `mvn spotless:apply` manually before commit)
#   - skip Apache RAT license header check
#   - skip javadoc / scaladoc generation
#   - skip source jar packaging
# Together these save ~60-120s per iteration. The canonical (no-FAST) invocation
# still runs the full lifecycle so CI parity is preserved.
echo
echo "[1/4] Building and installing Comet (spark-$SPARK_SHORT, contrib-delta)..."
cd "$COMET_ROOT"
# Spark 4.1 requires Java 17 (java.lang.Record). Comet's parent pom defaults
# java.version=11 — overriding here so the install works regardless of which JDK
# is on JAVA_HOME, as long as that JDK is ≥17.
JAVA_OVERRIDE=(
  -Djava.version=17
  -Dmaven.compiler.source=17
  -Dmaven.compiler.target=17
)
if [[ -n "${FAST:-}" ]]; then
  echo "  FAST=1: skipping spotless/RAT/javadoc/source-jar plugins"
  # Override `jni.dir` -> `native/target/release` because Comet's parent pom defaults it
  # to `native/target/debug`. The non-FAST path implicitly fixes this via `-Prelease`,
  # but FAST=1 drops that profile (it pulls in shade/javadoc), so without this override
  # mvn bundles a stale debug-tree dylib and contrib-delta's `#[ctor]` planner registration
  # silently goes missing -- every Delta scan then fails with "No contrib planner
  # registered for ContribOp.kind=delta-scan" at runtime.
  ./mvnw install -DskipTests -Pspark-"$SPARK_SHORT" -Pcontrib-delta \
    "${JAVA_OVERRIDE[@]}" \
    -Djni.dir="$COMET_ROOT/native/target/release" \
    -Dspotless.check.skip=true \
    -Drat.skip=true \
    -Dmaven.javadoc.skip=true \
    -Dmaven.source.skip=true
else
  ./mvnw install -Prelease -DskipTests -Pspark-"$SPARK_SHORT" -Pcontrib-delta \
    "${JAVA_OVERRIDE[@]}"
fi

# Sync Comet's just-installed artifacts to an ISOLATED publish dir. Pointing SBT
# directly at ~/.m2/repository/ triggers coursier's sticky-resolver: orphan
# pom-only entries left over from `mvn dependency:resolve` runs make it look for
# unrelated transitive JARs (parquet, guava, azure, ...) at local-m2 and refuse
# to fall through to maven-central. Isolating Comet's artifacts in a dedicated
# directory means local-comet only matches `org.apache.datafusion:*` -- no
# orphans to mistake.
#
# Hard-coded under /tmp (not $TMPDIR) because the path is also referenced in
# dev/diffs/delta/<DELTA_VERSION>.diff (build/sbt-config/repositories), which
# the diff applies into the Delta checkout. macOS's $TMPDIR is per-user under
# /var/folders/..., so substituting it here would diverge from the diff's
# literal path.
COMET_PUBLISH_DIR="${COMET_PUBLISH_DIR:-/tmp/comet-published-${SPARK_SHORT}}"
echo
echo "[1.5/4] Syncing Comet artifacts to $COMET_PUBLISH_DIR..."
rm -rf "$COMET_PUBLISH_DIR"
mkdir -p "$COMET_PUBLISH_DIR/org/apache/datafusion"
rsync -a "$HOME/.m2/repository/org/apache/datafusion/" "$COMET_PUBLISH_DIR/org/apache/datafusion/"
echo "  Published: $(ls -1 "$COMET_PUBLISH_DIR/org/apache/datafusion/" | wc -l | tr -d ' ') Comet modules"

# Step 2: clone Delta (or reuse existing checkout).
#
# `git clean -fd` here is intentional and cheap (sub-second): it removes
# untracked files left from the previous diff apply but respects gitignore,
# so Delta's `target/` (and SBT's zinc cache inside it) is preserved.
echo
echo "[2/4] Cloning Delta $DELTA_VERSION..."
# Robust reuse check: `[[ -d $WORKDIR/.git ]]` passes for an orphaned/empty .git/
# directory (seen in practice when a prior run was force-killed mid-checkout); the
# subsequent `git fetch` then dies with `not a git repository`. Use `git rev-parse
# --git-dir` to confirm the workdir is a usable repo before reusing it.
if [[ -d "$DELTA_WORKDIR" ]] \
   && git -C "$DELTA_WORKDIR" rev-parse --git-dir >/dev/null 2>&1; then
  echo "  Reusing existing checkout at $DELTA_WORKDIR"
  cd "$DELTA_WORKDIR"
  git fetch --depth 1 origin "refs/tags/v$DELTA_VERSION:refs/tags/v$DELTA_VERSION" 2>/dev/null || true
  git checkout -f "v$DELTA_VERSION"
  git clean -fd
  rm -rf spark/spark-warehouse
else
  if [[ -e "$DELTA_WORKDIR" ]]; then
    echo "  Existing $DELTA_WORKDIR is not a valid git repo; removing and re-cloning."
  fi
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
# Skip Delta's javaunidoc generation. Delta's `configureUnidoc` wires
# `(Test / test) := (Test / test) dependsOn (Compile / unidoc)`, and the
# javaunidoc step compiles auto-generated Java stubs from Scala test sources
# that fail to resolve `org.apache.spark.sql.test.SQLTestData` etc. -- Delta's
# own gap, not ours. Setting DISABLE_UNIDOC=1 short-circuits the helper
# (Unidoc.scala line 52) so the test target runs directly.
export DISABLE_UNIDOC=1

# Delta 4.1.0 mandates Java 17; Comet itself builds fine on 17+. If the user
# is iterating with a newer JDK on Comet, point this at a JDK 17 install for
# SBT. Typical usage: `DELTA_JAVA_HOME=$(/usr/libexec/java_home -v 17)`.
if [[ -n "${DELTA_JAVA_HOME:-}" ]]; then
  echo "  Using DELTA_JAVA_HOME=$DELTA_JAVA_HOME for SBT"
  export JAVA_HOME="$DELTA_JAVA_HOME"
  export PATH="$DELTA_JAVA_HOME/bin:$PATH"
fi

# Reset Gradle daemon + script cache. A daemon started with an older JDK
# sticks around and will be reused by Delta's `./gradlew` inside
# `icebergShaded/assembly`, and Gradle's compiled-build-script cache stores
# classfiles whose major version matches the JDK of the earlier run.
pkill -f 'GradleDaemon' 2>/dev/null || true
rm -rf ~/.gradle/caches/7.5.1/scripts ~/.gradle/caches/7.6.3/scripts 2>/dev/null || true

# Optional test sharding for the `full` sweep. Delta ships project/TestParallelization.scala,
# which splits Test/testGrouping into parallel forked JVMs and selects this run's shard --
# but only when NUM_SHARDS>1, SHARD_ID>=0, and TEST_PARALLELISM_COUNT>1 are ALL set (env).
# Set NUM_SHARDS>1 to split the ~29h Delta-3.3.2 sweep across parallel invocations (one per
# CI runner: matrix SHARD_ID=0..NUM_SHARDS-1); each invocation runs its slice in
# TEST_PARALLELISM_COUNT forks. Default the companions so `NUM_SHARDS=N` alone is enough.
#
# MEMORY: every fork gets the full Test/javaOptions -Xmx (4g) AND spark.memory.offHeap.size
# (10g). Keep TEST_PARALLELISM_COUNT low (1-2) on memory-constrained hosts -- the DV / 2B-row
# huge-table suites need ~4g heap + offheap PER fork, so over-parallelizing OOMs the box.
# Default (NUM_SHARDS unset) = single sequential fork, unchanged.
if [[ -n "${NUM_SHARDS:-}" && "${NUM_SHARDS}" -gt 1 ]]; then
  export NUM_SHARDS
  export SHARD_ID="${SHARD_ID:-0}"
  export TEST_PARALLELISM_COUNT="${TEST_PARALLELISM_COUNT:-2}"
  echo "  Sharding      : shard ${SHARD_ID} of ${NUM_SHARDS}, ${TEST_PARALLELISM_COUNT} fork(s)/shard"
fi

# Capture the SBT test phase into a timestamped log under the Comet repo, mirroring
# `run-test.sh`'s approach. Lets the user / background-task watchers tail progress in
# real time (the prior behaviour piped only to stdout, so any caller wrapping this in
# `... | tail` saw nothing until the run finished -- minutes to hours later for the
# full sweep). The Delta version and shard id (when sharded) go in the filename so
# parallel CI shards don't clobber one another.
LOG_DIR="$COMET_ROOT/target/delta-regression-logs"
mkdir -p "$LOG_DIR"
LOG_SUFFIX=""
if [[ -n "${SHARD_ID:-}" && -n "${NUM_SHARDS:-}" ]]; then
  LOG_SUFFIX="-shard${SHARD_ID}of${NUM_SHARDS}"
fi
LOG="$LOG_DIR/regression-${DELTA_VERSION}-${TEST_FILTER}${LOG_SUFFIX}-$(date +%Y%m%d-%H%M%S)-$$.log"
echo "==> logging to $LOG"

case "$TEST_FILTER" in
  smoke)
    build/sbt "$SBT_MODULE/testOnly org.apache.spark.sql.delta.CometSmokeTest" 2>&1 | tee "$LOG"
    ;;
  full)
    build/sbt "$SBT_MODULE/test" 2>&1 | tee "$LOG"
    ;;
  lite)
    # `lite` is `full` minus Delta-3.3.2-only clone families (Id/Name column-mapping,
    # WithCoordinatedCommits, RowTracking, WithDeletionVectors -- ~141 of 385 base
    # suites) that 4.1 dropped upstream. Maps to ~few hours instead of ~29h while still
    # covering every BASE suite. The DELTA_LITE env var is consumed by the
    # `Test/testOptions += Tests.Filter` block the 3.3.2 diff adds to `build.sbt`; the
    # column-mapping subset is excluded unconditionally (see diff comment) and the rest
    # only when DELTA_LITE is set. No-op on 4.0/4.1 (their diffs don't add the filter).
    export DELTA_LITE=1
    build/sbt "$SBT_MODULE/test" 2>&1 | tee "$LOG"
    ;;
  *)
    build/sbt "$SBT_MODULE/testOnly $TEST_FILTER" 2>&1 | tee "$LOG"
    ;;
esac

echo
echo "Done. Log: $LOG"
