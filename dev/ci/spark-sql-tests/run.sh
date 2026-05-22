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

# Runs Apache Spark's SQL test suites locally with Comet enabled, reproducing
# the spark_sql_test.yml GitHub Actions workflow for Spark 4.1.
#
# -e is intentionally not set: when running all module shards, one failing
# shard must not stop the rest. Build and setup failures are checked
# explicitly below.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=config.sh
source "$SCRIPT_DIR/config.sh"

usage() {
  cat <<EOF
Usage: $(basename "$0") [module]

Run Apache Spark SQL test suites locally with Comet enabled (Spark $SPARK_VERSION).

Arguments:
  module   One of: ${SPARK_SQL_MODULES[*]}
           or 'all' to run every shard sequentially (default).

Environment variables:
  SKIP_BUILD=1        Skip the Comet build; reuse existing artifacts.
  SKIP_SPARK_SETUP=1  Skip the Spark clone/reset/diff step.
  COMET_SPARK_DIR     Spark checkout path (default: \$HOME/.cache/datafusion-comet/apache-spark).
  SPARK_REF           Git ref for the Spark sources (default: v$SPARK_VERSION).
  SBT_MEM             sbt heap size in MB (default: 4096).
  LC_ALL              Locale for the sbt run (default: C.UTF-8; use en_US.UTF-8 on macOS).
  PYSPARK_PYTHON      Python interpreter for Spark. Defaults to a nonexistent
                      path so Spark 4.1's Python data source probe is skipped
                      (it can hang on machines that have python3). Export a
                      real interpreter to run the Python-dependent suites.
EOF
}

module="${1:-all}"
case "$module" in
  -h|--help) usage; exit 0 ;;
esac

# Resolve the list of modules to run.
modules_to_run=()
if [ "$module" = "all" ]; then
  modules_to_run=("${SPARK_SQL_MODULES[@]}")
elif module_sbt_args "$module" >/dev/null 2>&1; then
  modules_to_run=("$module")
else
  echo "ERROR: unknown module '$module'" >&2
  echo >&2
  usage >&2
  exit 1
fi

# --- JDK version check (warning only) --------------------------------------
jdk_version="$(java -version 2>&1 | head -n1 | sed -E 's/.*version "([0-9]+).*/\1/')"
if [ "$jdk_version" != "$REQUIRED_JDK" ]; then
  echo "WARNING: active JDK reports major version '$jdk_version'; Spark $SPARK_VERSION CI uses JDK $REQUIRED_JDK." >&2
  echo "         Set JAVA_HOME to a JDK $REQUIRED_JDK install to match CI exactly." >&2
fi

# --- Build Comet -----------------------------------------------------------
if [ "${SKIP_BUILD:-}" = "1" ]; then
  echo "SKIP_BUILD=1: skipping Comet build."
else
  echo "Building Comet (PROFILES=-Pspark-$SPARK_SHORT make release) ..."
  if ! ( cd "$COMET_REPO_ROOT" && PROFILES="-Pspark-$SPARK_SHORT" make release ); then
    echo "ERROR: Comet build failed." >&2
    exit 1
  fi
fi

# --- Purge partial Maven cache entries -------------------------------------
# Mirrors .github/actions/setup-spark-builder/action.yaml. Comet's Maven phase
# downloads POMs for transitive artifacts whose JARs it never needs. sbt's
# Coursier resolver then treats the POM-only entry as "found locally" and
# fails on the missing JAR instead of fetching it remotely. Delete those
# partial entries so sbt re-fetches the full artifact.
maven_repo="$HOME/.m2/repository"
if [ -d "$maven_repo" ]; then
  echo "Purging partial Maven cache entries ..."
  find "$maven_repo" -name '*.pom' | while read -r pom; do
    jar="${pom%.pom}.jar"
    [ -f "$jar" ] && continue
    grep -q '<packaging>jar</packaging>\|<packaging>bundle</packaging>' "$pom" 2>/dev/null || continue
    rm -f "$pom" "${pom}.sha1" "${pom%.pom}.pom.lastUpdated" \
      "$(dirname "$pom")/_remote.repositories"
  done
fi

# --- Set up the Spark checkout ---------------------------------------------
if [ "${SKIP_SPARK_SETUP:-}" = "1" ]; then
  echo "SKIP_SPARK_SETUP=1: using the existing Spark checkout as-is."
  if [ ! -d "$COMET_SPARK_DIR/.git" ]; then
    echo "ERROR: SKIP_SPARK_SETUP=1 but no Spark checkout at $COMET_SPARK_DIR" >&2
    exit 1
  fi
else
  if ! "$SCRIPT_DIR/setup-spark.sh"; then
    echo "ERROR: Spark setup failed." >&2
    exit 1
  fi
fi

# --- Run the selected module shards ----------------------------------------
log_dir="$SCRIPT_DIR/logs"
mkdir -p "$log_dir"

results=()
overall_status=0

for m in "${modules_to_run[@]}"; do
  sbt_args="$(module_sbt_args "$m")"
  log_file="$log_dir/${m}.log"
  echo
  echo "=================================================================="
  echo "Module:   $m"
  echo "sbt args: $sbt_args"
  echo "Log file: $log_file"
  echo "=================================================================="

  # Stale Parquet cache workaround (mirrors spark_sql_test.yml).
  rm -rf "$maven_repo/org/apache/parquet"

  # Spark 4.1's DataSourceManager probes for Python data sources during query
  # analysis by spawning a Python worker. The CI amd64/rust container has no
  # python3, so the probe is skipped there. On a developer machine that does
  # have python3 (every macOS install does) the worker can hang indefinitely:
  # the JVM-side read has no idle timeout by default, so suites such as
  # GlobalTempViewSuite stall forever instead of failing fast. Point PySpark at
  # a nonexistent interpreter so the probe is skipped, matching CI. A developer
  # who wants the Python suites can export PYSPARK_PYTHON themselves.
  no_python="/nonexistent/comet-disable-python-datasources"

  (
    cd "$COMET_SPARK_DIR" || exit 1
    NOLINT_ON_COMPILE=true \
    ENABLE_COMET=true \
    ENABLE_COMET_ONHEAP=true \
    ENABLE_COMET_LOG_FALLBACK_REASONS=false \
    SERIAL_SBT_TESTS=1 \
    PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-$no_python}" \
    PYSPARK_PYTHON="${PYSPARK_PYTHON:-$no_python}" \
      build/sbt -Dsbt.log.noformat=true -mem "$SBT_MEM" \
        'set Global / concurrentRestrictions := Seq(Tags.limit(Tags.ForkedTestGroup, 1))' \
        "$sbt_args"
  ) 2>&1 | tee "$log_file"
  status="${PIPESTATUS[0]}"

  if [ "$status" -eq 0 ]; then
    results+=("PASS  $m")
  else
    results+=("FAIL  $m (sbt exit $status)")
    overall_status=1
  fi
done

# --- Summary ---------------------------------------------------------------
echo
echo "=================================================================="
echo "Spark SQL test summary (Spark $SPARK_VERSION)"
echo "=================================================================="
for line in "${results[@]}"; do
  echo "  $line"
done
echo "Logs written to: $log_dir"
exit "$overall_status"
