#!/usr/bin/env bash
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
# Run one group of the Comet JVM test suites. Each concrete suite carries one
# group tag, such as org.apache.comet.tags.ExecGroup, and CI runs each group as
# its own job.
# Arguments after the group go to Maven:
#
#   dev/ci/run-comet-tests.sh exec -Pspark-4.0
#
# The group tag does not select integration suites (@IntegrationTestSuite, such as
# ParquetEncryptionITCase). The ScalaTest plugin excludes them, and they run under
# maven-failsafe-plugin, which ignores the tag. So the default `test` goal never runs
# them, and `--goal install` runs all of them whichever group is named.
#
# Build the native library first: `make core`, or `make release` with -Prelease.
# Written for bash 3.2.

set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# Each group is a tag annotation here, such as ExecGroup.java for `exec`.
TAGS_DIR="$REPO/spark/src/test/java/org/apache/comet/tags"

usage() {
  groups=""
  for tag in "$TAGS_DIR"/*Group.java; do
    tag="$(basename "$tag" Group.java)"
    groups="$groups $(printf '%s' "$tag" | tr '[:upper:]' '[:lower:]')"
  done
  cat >&2 <<USAGE
Usage: dev/ci/run-comet-tests.sh [--goal <goal>] <group> [maven args...]

  groups   $groups
  --goal    the Maven goal to run (default: test). install also runs the
            spark-integration checks, which are not selected by the group:
            every *ITCase suite runs, whichever group is named.
USAGE
  exit 2
}

GOAL="test"
while [ $# -gt 0 ]; do
  case "$1" in
    --goal)
      [ $# -ge 2 ] || usage
      GOAL="$2"
      shift 2
      ;;
    -h | --help) usage ;;
    *) break ;;
  esac
done
[ $# -ge 1 ] || usage

# exec -> ExecGroup
TAG="$(printf '%s' "${1:0:1}" | tr '[:lower:]' '[:upper:]')${1:1}Group"
if [ ! -f "$TAGS_DIR/$TAG.java" ]; then
  echo "Unknown test group: $1" >&2
  usage
fi
shift

cd "$REPO"

export SPARK_HOME="${SPARK_HOME:-$REPO}"
export SPARK_LOCAL_HOSTNAME="${SPARK_LOCAL_HOSTNAME:-localhost}"
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"
export MAVEN_OPTS="${MAVEN_OPTS:--Xmx4G -Xms2G -XX:+UnlockDiagnosticVMOptions -XX:+ShowMessageBoxOnError -XX:+HeapDumpOnOutOfMemoryError -XX:ErrorFile=./hs_err_pid%p.log}"

set -x
exec ./mvnw -B "$GOAL" "-DtagsToInclude=org.apache.comet.tags.$TAG" "$@"
