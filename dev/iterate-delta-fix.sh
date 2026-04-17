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

# One-shot iterate loop: build Comet, run one or more delta tests, grep the
# resulting log for whatever we want to see. Approve once, then I edit the
# SELECTOR / GREP_PATTERN strings below between iterations.

set -uo pipefail

# ---- EDIT ME between iterations --------------------------------------------
SELECTOR='org.apache.spark.sql.delta.DeltaCDCStreamSuite -- -z "rateLimit - maxFilesPerTrigger - overall"'
GREP_PATTERN='DELTA-DEBUG|FAILED \*\*\*$|CometNativeException|Column count mismatch|Correct Answer'
# Set to 1 to reuse the existing Comet jar (native-only iteration or log re-grep).
SKIP_BUILD=0
GREP_CONTEXT=40
# ----------------------------------------------------------------------------

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

if [[ "$SKIP_BUILD" != "1" ]]; then
  echo "==> building Comet"
  dev/build-comet-delta.sh 2>&1 | tail -3
fi

echo "==> running $SELECTOR"
dev/run-delta-test.sh "$SELECTOR" 2>&1 | tail -3 || true

LOG="$(ls -t "$REPO_ROOT/target/delta-regression-logs/test-"*.log 2>/dev/null | head -1)"
if [[ -z "$LOG" ]]; then
  echo "no log found" >&2
  exit 1
fi

echo "==> log: $LOG"
echo "==> grep: $GREP_PATTERN"
grep -E -A"$GREP_CONTEXT" "$GREP_PATTERN" "$LOG" | head -200 || echo "no matches"
