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
# Print a focused diagnostic summary for one failing test label from the most
# recent targeted-*.log. Always emits the same shape so a single invocation
# gives what's needed.
#
# Usage:
#   dev/inspect-targeted-failure.sh                  # list labels
#   dev/inspect-targeted-failure.sh <label>          # focused summary
#   dev/inspect-targeted-failure.sh --full <label>   # whole section (long!)

set -uo pipefail
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$REPO_ROOT/target/delta-regression-logs"
LOG="$(ls -t "$LOG_DIR"/targeted-*.log 2>/dev/null | head -1)"

if [ -z "$LOG" ]; then
  echo "No targeted-*.log in $LOG_DIR. Run dev/run-delta-regression-targeted.sh first." >&2
  exit 1
fi

if [ $# -eq 0 ]; then
  echo "log: $LOG"
  echo "labels:"
  awk '/^==> [a-z][a-z0-9-]*$/ { print "  " substr($0,5) }' "$LOG"
  exit 0
fi

FULL=0
if [ "$1" = "--full" ]; then FULL=1; shift; fi
LABEL="${1:-}"
[ -z "$LABEL" ] && { echo "usage: $0 [--full] <label>" >&2; exit 2; }

# Extract just this label's section.
SECTION="$(awk -v t="==> $LABEL" '
  $0 == t { on=1; next }
  /^==> [a-z][a-z0-9-]*$/ && $0 != t { on=0 }
  on { print }
' "$LOG")"

if [ -z "$SECTION" ]; then
  echo "(no section for '$LABEL' — try without args to list labels)" >&2
  exit 1
fi

echo "==================== $LABEL ===================="
echo "log: $LOG"
echo ""

if [ "$FULL" = "1" ]; then
  echo "$SECTION"
  exit 0
fi

# Focused summary: test name, first error/exception, first ~20 Rust/Spark
# frames, first occurrence of common diagnostic patterns.
echo "----- test names -----"
echo "$SECTION" | grep -E '^\[info\] - .* \*\*\* FAILED \*\*\*' | head -5

echo ""
echo "----- first error / cause -----"
echo "$SECTION" | grep -E '(\*\*\* FAILED \*\*\*|Exception|Error: |reason:|Cause: |assertion|MatchError|requirement failed|Results do not match|Wrong arguments|transpose requires|Expected batches|did not equal|had size|Required column)' | head -10

echo ""
echo "----- schema / type evidence (if any) -----"
echo "$SECTION" | grep -E '(^\[info\] *left:| right:|Struct\(\[|DataType|Schema:)' | head -10

echo ""
echo "----- top frames (Comet / DataFusion / Delta / test) -----"
echo "$SECTION" | grep -E '^\[info\].*at (org\.apache\.comet|org\.apache\.spark\.sql\.(delta|comet)|comet::|datafusion|arrow_data)' | head -20

echo ""
echo "----- Tests: summary -----"
echo "$SECTION" | grep -E 'Tests: succeeded' | tail -1
