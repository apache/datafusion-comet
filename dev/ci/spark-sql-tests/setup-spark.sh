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

# Maintains the persistent apache/spark checkout used by the local Spark SQL
# test scripts, and applies the Comet diff. Idempotent and safe to re-run.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=config.sh
source "$SCRIPT_DIR/config.sh"

DIFF_FILE="$COMET_REPO_ROOT/dev/diffs/${SPARK_VERSION}.diff"
if [ ! -f "$DIFF_FILE" ]; then
  echo "ERROR: Comet diff not found: $DIFF_FILE" >&2
  exit 1
fi

if [ ! -d "$COMET_SPARK_DIR/.git" ]; then
  echo "Cloning apache/spark ($SPARK_REF) into $COMET_SPARK_DIR ..."
  mkdir -p "$(dirname "$COMET_SPARK_DIR")"
  git clone --depth 1 --branch "$SPARK_REF" \
    https://github.com/apache/spark.git "$COMET_SPARK_DIR"
else
  echo "Reusing existing Spark checkout at $COMET_SPARK_DIR"
fi

# Resolve the commit to reset to. A checkout created with a different
# SPARK_REF may not contain the requested ref; fetch it shallowly if missing.
reset_target="$SPARK_REF"
if ! git -C "$COMET_SPARK_DIR" rev-parse --verify --quiet "${SPARK_REF}^{commit}" >/dev/null; then
  echo "Ref $SPARK_REF not present locally; fetching ..."
  git -C "$COMET_SPARK_DIR" fetch --depth 1 origin "$SPARK_REF"
  reset_target="FETCH_HEAD"
fi

echo "Resetting Spark checkout to a clean $SPARK_REF ..."
# reset --hard reverts tracked-file edits from a previously applied diff.
git -C "$COMET_SPARK_DIR" reset --hard "$reset_target"
# clean -fd removes untracked files the previous diff added. Without -x it
# leaves gitignored build output in place, so Spark's compiled target/
# artifacts are reused across runs.
git -C "$COMET_SPARK_DIR" clean -fd

echo "Applying $DIFF_FILE ..."
# Pre-flight check so a drifted diff produces an actionable error rather than
# raw git apply output.
if ! git -C "$COMET_SPARK_DIR" apply --check "$DIFF_FILE" 2>/dev/null; then
  echo "ERROR: $DIFF_FILE does not apply cleanly to $SPARK_REF." >&2
  echo "       The Comet diff and the Spark ref may have drifted out of sync." >&2
  exit 1
fi
git -C "$COMET_SPARK_DIR" apply "$DIFF_FILE"

echo "Spark checkout ready: $COMET_SPARK_DIR ($SPARK_REF + Comet diff)"
