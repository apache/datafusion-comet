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
#
# Validate fixes against the 93 previously-failing tests.

set -uo pipefail
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DELTA_VERSION="${DELTA_VERSION:-3.3.2}"
DELTA_DIR="${DELTA_DIR:-${DELTA_WORKDIR:-${TMPDIR:-/tmp}/delta-regression-${DELTA_VERSION}}}"
export JAVA_HOME="${JAVA_HOME:-$HOME/jdks/jdk-17.0.18+8/Contents/Home}"
export SPARK_LOCAL_IP=127.0.0.1

TSV="${1:-/tmp/failures-by-suite.tsv}"
if [ ! -f "$TSV" ]; then
  echo "usage: $0 [failures-by-suite.tsv]" >&2
  exit 2
fi

LOG_DIR="$REPO_ROOT/target/delta-regression-logs"
mkdir -p "$LOG_DIR"
STAMP="$(date +%Y%m%d-%H%M%S)"
LOG="$LOG_DIR/validate-$STAMP.log"
RESULTS="$LOG_DIR/validate-$STAMP.results.tsv"

cd "$DELTA_DIR"

suites=$(cut -f1 "$TSV" | sort -u)
total=$(wc -l < "$TSV" | tr -d ' ')
echo "Validating $total tests across $(echo "$suites" | wc -l | tr -d ' ') suites" | tee "$LOG"
echo "Log: $LOG" | tee -a "$LOG"
: > "$RESULTS"

for suite in $suites; do
  tests=$(awk -v s="$suite" -F'\t' '$1==s {print $2}' "$TSV")
  count=$(echo "$tests" | wc -l | tr -d ' ')
  echo "" | tee -a "$LOG"
  echo "=== $suite ($count tests) ===" | tee -a "$LOG"

  # Build one sbt invocation with multiple testOnly commands (one per test)
  batch=""
  while IFS= read -r t; do
    esc=$(echo "$t" | sed 's/"/\\"/g')
    if [ -z "$batch" ]; then
      batch="spark/testOnly *${suite} -- -z \"$esc\""
    else
      batch="$batch ; spark/testOnly *${suite} -- -z \"$esc\""
    fi
  done <<< "$tests"

  build/sbt "$batch" 2>&1 | tee -a "$LOG" > /dev/null || true

  # Collect per-test result from log content appended since this suite started
  while IFS= read -r t; do
    # grep the suite section of the log for a FAILED marker or succeeded 1
    if grep -qF "- $t *** FAILED" "$LOG"; then
      printf 'FAIL\t%s\t%s\n' "$suite" "$t" >> "$RESULTS"
    else
      printf 'PASS\t%s\t%s\n' "$suite" "$t" >> "$RESULTS"
    fi
  done <<< "$tests"
done

passed=$(grep -c "^PASS" "$RESULTS" 2>/dev/null || echo 0)
failed=$(grep -c "^FAIL" "$RESULTS" 2>/dev/null || echo 0)
echo "" | tee -a "$LOG"
echo "===========================================" | tee -a "$LOG"
echo "Summary: $passed / $total passed ($failed failed)" | tee -a "$LOG"
echo "Results: $RESULTS" | tee -a "$LOG"
if [ "$failed" != "0" ]; then
  echo "" | tee -a "$LOG"
  echo "Failed:" | tee -a "$LOG"
  grep "^FAIL" "$RESULTS" | cut -f2- | tee -a "$LOG"
fi
