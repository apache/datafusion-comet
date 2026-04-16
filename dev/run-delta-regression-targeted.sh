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
# Run the targeted set of Delta regression tests that correspond to each
# specific fix we've landed. One sbt session, one testOnly per fix.
#
# Intentionally does NOT run the full Delta suite — that takes hours.
#
# Usage: dev/run-delta-regression-full.sh
#
# Output: target/delta-regression-logs/targeted-YYYYMMDD-HHMMSS.log
# Exit code: 0 if every named test passes, non-zero if any fail.

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DELTA_VERSION="${DELTA_VERSION:-3.3.2}"
DELTA_DIR="${DELTA_DIR:-${DELTA_WORKDIR:-${TMPDIR:-/tmp}/delta-regression-${DELTA_VERSION}}}"
export JAVA_HOME="${JAVA_HOME:-$HOME/jdks/jdk-17.0.18+8/Contents/Home}"
export SPARK_LOCAL_IP=127.0.0.1
export RUST_BACKTRACE=1

if [ ! -d "$DELTA_DIR" ]; then
  echo "Delta checkout missing at $DELTA_DIR — run dev/run-delta-regression.sh 3.3.2 setup first." >&2
  exit 1
fi

LOG_DIR="$REPO_ROOT/target/delta-regression-logs"
mkdir -p "$LOG_DIR"
STAMP="$(date +%Y%m%d-%H%M%S)"
LOG="$LOG_DIR/targeted-$STAMP.log"

# Stop any stray sbt from prior runs so the project isn't locked.
pkill -f sbt-launch 2>/dev/null || true
pkill -f "build/sbt" 2>/dev/null || true
sleep 1

# Tests we've already fixed and verified PASS. Not run by default to keep the
# iteration loop short. Run them via `--with-fixed` or `--fixed-only` to guard
# against regressions once a fix lands (e.g. before committing).
FIXED_TESTS=(
  "partition-vector|org.apache.spark.sql.delta.DeltaColumnDefaultsInsertSuite -- -z \"Column DEFAULT value support with Delta Lake, positive tests\""
  "clone-path-concat|org.apache.spark.sql.delta.CloneParquetByNameSuite -- -z \"clone non-partitioned parquet to delta table - SHALLOW\""
  "dv-wrong-args|org.apache.spark.sql.delta.DeltaParquetFileFormatWithPredicatePushdownSuite -- -z \"read DV metadata columns: with rowIndexFilterType=IF_CONTAINED, with vectorized Parquet reader=true, with readColumnarBatchAsRows=true\""
  "cdc-transpose|org.apache.spark.sql.delta.DeltaCDCScalaSuite -- -z \"filtering cdc metadata columns\""
  "data-skipping-matcherror|org.apache.spark.sql.delta.stats.DataSkippingDeltaV1Suite -- -z \"support case insensitivity for partitioning filters\""
  "dpp-adaptive-broadcast|org.apache.spark.sql.delta.stats.DataSkippingDeltaV1Suite -- -z \"data skipping shouldn't use expressions involving a subquery\""
  "protobuf-recursion|org.apache.spark.sql.delta.stats.DataSkippingDeltaV1Suite -- -z \"remove redundant stats column references in data skipping expression\""
  "arrays-inconsistent|org.apache.spark.sql.delta.DescribeDeltaHistorySuite -- -z \"replaceWhere on data column\""
  "skipping-size0|org.apache.spark.sql.delta.stats.DataSkippingDeltaV1Suite -- -z \"loading data from Delta to parquet should skip data\""
  "insert-only-merge|org.apache.spark.sql.delta.MergeIntoSQLSuite -- -z \"insert only merge - target data skipping\""
  # TahoeBatchFileIndex coverage (DML post-join scans) — task #13.
  "merge-basic|org.apache.spark.sql.delta.MergeIntoSQLSuite -- -z \"basic case - merge to Delta table by path, isPartitioned: false\""
  "delete-basic|org.apache.spark.sql.delta.DeleteSQLSuite -- -z \"where data columns and partition columns\""
  "update-basic|org.apache.spark.sql.delta.UpdateSQLSuite -- -z \"basic update - Delta table by path - Partition=true\""
  # CdcAddFileIndex / TahoeRemoveFileIndex coverage (CDC reads) — task #14.
  "cdc-aggregate|org.apache.spark.sql.delta.DeltaCDCScalaSuite -- -z \"aggregating non-numeric cdc data columns\""
  "cdc-ending-version|org.apache.spark.sql.delta.DeltaCDCScalaSuite -- -z \"ending version not specified resolves to latest at execution time\""
  "cdc-cdf-disabled|org.apache.spark.sql.delta.DeltaCDCScalaSuite -- -z \"An error should be thrown when CDC is not enabled\""
)

# Active failure classes we're currently diagnosing. These are the tests that
# run by default. Move an entry up to FIXED_TESTS once its fix lands.
ACTIVE_TESTS=(
  "decoded-objects|org.apache.spark.sql.delta.DeltaCDCSQLIdColumnMappingSuite -- -z \"batch write: append, dynamic partition overwrite + CDF - column mapping id mode\""
  "tuple2-matcherror|org.apache.spark.sql.delta.DeltaIncrementalSetTransactionsSuite -- -z \"incremental set-transaction verification failures\""
  "row-id-change|org.apache.spark.sql.delta.rowid.RowTrackingMergeDVSuite -- -z \"MERGE preserves Row Tracking on tables enabled using backfill\""
  # row-id-lookup dropped: the underlying test ("Multiple merges into the same table")
  # is IGNORED in stock Delta by DeltaColumnMappingSelectedTestMixin, so matching its
  # behaviour isn't a real coverage gap. The scan-schema issue it exposed (_row_id_
  # appearing in requiredSchema without a FileSourceGeneratedMetadataStructField
  # marker) is tracked in #25.
  "streaming-batches|org.apache.spark.sql.delta.DeltaCDCStreamSuite -- -z \"rateLimit - maxFilesPerTrigger - overall\""
  "checksum-reconstruction|org.apache.spark.sql.delta.ChecksumSuite -- -z \"Incremental checksums: post commit snapshot should have a checksum without triggering state reconstruction\""
)

# Default: run ACTIVE only. `--with-fixed`: run both. `--fixed-only`: run FIXED only.
# `--labels a,b,c`: run just those labels (from either group).
TESTS=()
case "${1:-}" in
  --with-fixed)  TESTS=("${FIXED_TESTS[@]}" "${ACTIVE_TESTS[@]}") ;;
  --fixed-only)  TESTS=("${FIXED_TESTS[@]}") ;;
  --labels)
    shift
    want_csv="${1:-}"
    IFS=',' read -r -a want <<< "$want_csv"
    for entry in "${FIXED_TESTS[@]}" "${ACTIVE_TESTS[@]}"; do
      label="${entry%%|*}"
      for w in "${want[@]}"; do
        [ "$label" = "$w" ] && TESTS+=("$entry")
      done
    done
    if [ ${#TESTS[@]} -eq 0 ]; then
      echo "No labels matched '$want_csv'. Known labels:" >&2
      for e in "${FIXED_TESTS[@]}" "${ACTIVE_TESTS[@]}"; do echo "  ${e%%|*}"; done >&2
      exit 2
    fi
    ;;
  --list)
    echo "FIXED:"; for e in "${FIXED_TESTS[@]}"; do echo "  ${e%%|*}"; done
    echo "ACTIVE:"; for e in "${ACTIVE_TESTS[@]}"; do echo "  ${e%%|*}"; done
    exit 0
    ;;
  ""|--active) TESTS=("${ACTIVE_TESTS[@]}") ;;
  *)
    echo "usage: $0 [--active|--with-fixed|--fixed-only|--labels a,b,c|--list]" >&2
    exit 2
    ;;
esac

# Run each test in its own sbt invocation so a single failure doesn't stop the
# rest. Each sbt startup adds ~30s, but we get independent pass/fail signal for
# every fix. Concatenate all logs into one file with headers.
cd "$DELTA_DIR"
echo "==> log: $LOG"
echo "==> running ${#TESTS[@]} targeted tests (separate sbt sessions per fix)"
SBT_EXIT=0
for entry in "${TESTS[@]}"; do
  label="${entry%%|*}"
  sel="${entry#*|}"
  {
    echo ""
    echo "================================================================"
    echo "==> $label"
    echo "==> sbt: spark/testOnly $sel"
    echo "================================================================"
  } >> "$LOG"
  build/sbt "spark/testOnly $sel" >> "$LOG" 2>&1 || SBT_EXIT=$?
done

# Parse per-test result from the log. sbt emits a "SuiteName ... *** 1 TEST FAILED ***"
# line when a suite fails, and "Tests: succeeded N, failed 0" when a suite passes.
# We associate each test block by proximity to its "testOnly" echo.
echo ""
echo "==> Per-fix results:"
PASS_COUNT=0
FAIL_COUNT=0
for entry in "${TESTS[@]}"; do
  label="${entry%%|*}"
  # Each entry's output is bracketed by "==> <label>" (plain) vs "==> sbt: ..."
  # (the next line). Block runs from the label line to the NEXT "==> <label>"
  # line. We use a literal equality check to avoid regex metachar surprises.
  result="$(awk -v target="==> $label" '
    $0 == target { inblock=1; next }
    # Reset only on the NEXT block header ("==> some-label"), not on
    # "==> sbt: ..." which is the sub-line we write inside each block.
    /^==> [a-z][a-z0-9-]*$/ && $0 != target { inblock=0 }
    inblock && /Tests: succeeded [0-9]+, failed [0-9]+/ { last=$0 }
    END { print last }
  ' "$LOG")"
  if [[ "$result" == *"failed 0"* ]] && [[ ! "$result" == *"succeeded 0"* ]]; then
    printf "  PASS  %-30s %s\n" "$label" "${result##*Tests: }"
    PASS_COUNT=$((PASS_COUNT + 1))
  else
    printf "  FAIL  %-30s %s\n" "$label" "${result:-<no Tests: line in section>}"
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi
done

echo ""
echo "==> Summary: $PASS_COUNT passed, $FAIL_COUNT failed (sbt exit=$SBT_EXIT)"
echo "==> Full log: $LOG"

# Non-zero if any named test failed OR sbt errored.
if [ "$FAIL_COUNT" -gt 0 ] || [ "$SBT_EXIT" -ne 0 ]; then
  exit 1
fi
exit 0
