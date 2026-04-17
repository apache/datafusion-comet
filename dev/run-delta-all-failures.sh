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
# Validate fixes against the 93 previously-failing tests.  Groups tests by
# suite (via /tmp/failures-by-suite.tsv: "suite\ttestname") so each suite's
# filters fire in one sbt session.  Logs to target/delta-regression-logs/
# validate-<stamp>.log and a per-test TSV of PASS/FAIL.
#
# Generate the TSV:
#   python3 dev/gen-failures-by-suite.py target/delta-regression-logs/full-*.log
# or inline with the Python one-liner used in dev/validate fixes notes.

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

# Group by suite
declare -A suite_tests
while IFS=$'\t' read -r suite testname; do
  [ -z "$suite" ] && continue
  existing="${suite_tests[$suite]-}"
  if [ -z "$existing" ]; then
    suite_tests[$suite]="$testname"
  else
    suite_tests[$suite]="$existing"$'\n'"$testname"
  fi
done < "$TSV"

total_tests=0
for suite in "${!suite_tests[@]}"; do
  while IFS= read -r _; do total_tests=$((total_tests+1)); done <<< "${suite_tests[$suite]}"
done
echo "Validating $total_tests tests across ${#suite_tests[@]} suites" | tee "$LOG"
echo "Log: $LOG" | tee -a "$LOG"

: > "$RESULTS"

for suite in $(printf '%s\n' "${!suite_tests[@]}" | sort); do
  tests="${suite_tests[$suite]}"
  count=$(echo "$tests" | wc -l | tr -d ' ')
  echo "" | tee -a "$LOG"
  echo "=== $suite ($count tests) ===" | tee -a "$LOG"

  # Build sbt testOnly line with -z for each test name (escaped)
  sbt_cmd="spark/testOnly org.apache.spark.sql.delta.*$suite"
  while IFS= read -r t; do
    # escape double quotes for sbt
    esc=$(echo "$t" | sed 's/"/\\"/g')
    sbt_cmd="$sbt_cmd -- -z \"$esc\""
  done <<< "$tests"

  # NB: sbt -- -z only accepts ONE filter per testOnly, so we run each test
  # as its own testOnly call but inside a single sbt invocation.
  batch_file=$(mktemp)
  while IFS= read -r t; do
    esc=$(echo "$t" | sed 's/"/\\"/g')
    echo "spark/testOnly *${suite} -- -z \"$esc\"" >> "$batch_file"
  done <<< "$tests"

  # Run all of this suite's tests in one sbt session
  build/sbt "$(cat "$batch_file" | tr '\n' ';' | sed 's/;$//')" 2>&1 | tee -a "$LOG" | \
    grep -E "Tests: succeeded|\*\*\* FAILED|Total number of tests run" > /tmp/suite-summary.txt || true

  # Parse results for this suite: look for "succeeded N" / "failed M" blocks
  # Each testOnly prints one summary; correlate to the input test list by order.
  test_idx=0
  while IFS= read -r t; do
    test_idx=$((test_idx+1))
    # Check if the test name appeared in a FAILED marker in the log
    if grep -qF "- $t *** FAILED" <<< "$(tail -c 200000 "$LOG")"; then
      printf 'FAIL\t%s\t%s\n' "$suite" "$t" >> "$RESULTS"
    else
      printf 'PASS\t%s\t%s\n' "$suite" "$t" >> "$RESULTS"
    fi
  done <<< "$tests"

  rm -f "$batch_file"
done

passed=$(grep -c "^PASS" "$RESULTS" 2>/dev/null || echo 0)
failed=$(grep -c "^FAIL" "$RESULTS" 2>/dev/null || echo 0)
echo "" | tee -a "$LOG"
echo "===========================================" | tee -a "$LOG"
echo "Summary: $passed / $total_tests passed ($failed failed)" | tee -a "$LOG"
echo "Log: $LOG" | tee -a "$LOG"
echo "Results: $RESULTS" | tee -a "$LOG"
if [ "$failed" != "0" ]; then
  echo "" | tee -a "$LOG"
  echo "Failed:" | tee -a "$LOG"
  grep "^FAIL" "$RESULTS" | cut -f2- | tee -a "$LOG"
  exit 1
fi
