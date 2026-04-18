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
# Validate fixes against previously-failing tests.  Runs each test in its own
# sbt invocation so sbt's stop-on-failure behaviour doesn't abort remaining
# tests.  Slow (~40s/test startup), correct.  For faster iteration on a small
# subset use dev/run-delta-test.sh.

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

total=$(wc -l < "$TSV" | tr -d ' ')
echo "Validating $total tests (one sbt invocation per test)" | tee "$LOG"
echo "Log: $LOG" | tee -a "$LOG"
: > "$RESULTS"

count=0
while IFS=$'\t' read -r suite testname; do
  [ -z "$suite" ] && continue
  count=$((count+1))
  esc=$(echo "$testname" | sed 's/"/\\"/g')
  echo "" | tee -a "$LOG"
  echo "=== [$count/$total] $suite :: $testname ===" | tee -a "$LOG"

  # Capture this invocation's output separately so the per-test grep is scoped.
  run_log=$(mktemp)
  build/sbt "spark/testOnly *${suite} -- -z \"$esc\"" > "$run_log" 2>&1 || true
  cat "$run_log" >> "$LOG"

  if grep -qF -- "- $testname *** FAILED" "$run_log"; then
    printf 'FAIL\t%s\t%s\n' "$suite" "$testname" >> "$RESULTS"
    echo "  -> FAIL" | tee -a "$LOG"
  elif grep -qE "Tests: succeeded [1-9]" "$run_log"; then
    printf 'PASS\t%s\t%s\n' "$suite" "$testname" >> "$RESULTS"
    echo "  -> PASS" | tee -a "$LOG"
  else
    printf 'NOTRUN\t%s\t%s\n' "$suite" "$testname" >> "$RESULTS"
    echo "  -> NOTRUN (filter didn't match any test)" | tee -a "$LOG"
  fi
  rm -f "$run_log"
done < "$TSV"

pass=$(grep -c "^PASS" "$RESULTS" 2>/dev/null || echo 0)
fail=$(grep -c "^FAIL" "$RESULTS" 2>/dev/null || echo 0)
notrun=$(grep -c "^NOTRUN" "$RESULTS" 2>/dev/null || echo 0)
echo "" | tee -a "$LOG"
echo "===========================================" | tee -a "$LOG"
echo "Summary: $pass passed / $fail failed / $notrun not-run  (total $total)" | tee -a "$LOG"
echo "Results: $RESULTS" | tee -a "$LOG"
if [ "$fail" != "0" ] || [ "$notrun" != "0" ]; then
  echo "" | tee -a "$LOG"
  echo "Not-pass:" | tee -a "$LOG"
  grep -E "^(FAIL|NOTRUN)" "$RESULTS" | tee -a "$LOG"
fi
