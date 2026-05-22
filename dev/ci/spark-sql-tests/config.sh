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

# Shared configuration for the local Spark SQL test scripts. This file is
# sourced by setup-spark.sh and run.sh; it is not meant to be run directly.
#
# The variables below are consumed by the sourcing scripts, so shellcheck
# cannot see their use when checking this file in isolation.
# shellcheck disable=SC2034

# --- Spark version under test ----------------------------------------------
# Override with SPARK_VERSION=<full-version>. Each supported version has a
# matching dev/diffs/<version>.diff and mirrors a spark_sql_test.yml CI config.
SPARK_VERSION="${SPARK_VERSION:-4.1.1}"

# Per-version settings copied from the spark_sql_test.yml CI matrix: the short
# version (Maven/sbt profile suffix) and the JDK major version CI uses.
case "$SPARK_VERSION" in
  3.4.3) SPARK_SHORT="3.4"; REQUIRED_JDK="11" ;;
  3.5.8) SPARK_SHORT="3.5"; REQUIRED_JDK="11" ;;
  4.0.2) SPARK_SHORT="4.0"; REQUIRED_JDK="21" ;;
  4.1.1) SPARK_SHORT="4.1"; REQUIRED_JDK="17" ;;
  *)
    echo "ERROR: unsupported SPARK_VERSION '$SPARK_VERSION'." >&2
    echo "       Supported versions: 3.4.3, 3.5.8, 4.0.2, 4.1.1" >&2
    exit 1
    ;;
esac

# Git ref checked out for the Spark sources. Defaults to the released tag.
SPARK_REF="${SPARK_REF:-v${SPARK_VERSION}}"

# Test-group isolation, mirroring spark_sql_test.yml. Every CI config sets
# SERIAL_SBT_TESTS=1 except Spark 4.0 (JDK 21), which instead leaves it unset
# and forks a dedicated JVM per leak-prone Parquet/Orc suite to work around a
# cross-suite file-stream leak under JDK 21 (Comet issue #4327). run.sh reads
# DEDICATED_JVM_SUITES: when non-empty it passes DEDICATED_JVM_SBT_TESTS and
# omits SERIAL_SBT_TESTS; when empty it passes SERIAL_SBT_TESTS=1.
DEDICATED_JVM_SUITES=""
if [ "$SPARK_SHORT" = "4.0" ]; then
  DEDICATED_JVM_SUITES="\
org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormatV1Suite,\
org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormatV2Suite,\
org.apache.spark.sql.execution.datasources.orc.OrcSourceV1Suite,\
org.apache.spark.sql.execution.datasources.orc.OrcSourceV2Suite"
fi

# --- Paths -----------------------------------------------------------------
# Persistent apache/spark checkout, namespaced by Spark version so switching
# versions does not reset away each version's compiled target/ artifacts.
COMET_SPARK_DIR="${COMET_SPARK_DIR:-$HOME/.cache/datafusion-comet/apache-spark-${SPARK_VERSION}}"

# Directory containing these scripts, and the Comet repository root.
COMET_SQL_TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMET_REPO_ROOT="$(git -C "$COMET_SQL_TEST_DIR" rev-parse --show-toplevel)"

# --- sbt / locale ----------------------------------------------------------
# sbt heap size in MB. Higher than CI's 3072 since local machines are not
# constrained to 7 GB GitHub runners.
SBT_MEM="${SBT_MEM:-4096}"

# Locale for the sbt run. CI uses C.UTF-8; macOS users may need en_US.UTF-8.
export LC_ALL="${LC_ALL:-C.UTF-8}"

# --- Module shards ---------------------------------------------------------
# The seven module shards, copied verbatim from
# .github/workflows/spark_sql_test.yml. Order matches the CI matrix.
SPARK_SQL_MODULES=(
  catalyst
  sql_core-1
  sql_core-2
  sql_core-3
  sql_hive-1
  sql_hive-2
  sql_hive-3
)

# module_sbt_args <module>
# Echoes the single build/sbt argument for the given module shard.
# Returns non-zero for an unknown module.
module_sbt_args() {
  case "$1" in
    catalyst)
      echo 'catalyst/test' ;;
    sql_core-1)
      echo 'sql/testOnly * -- -l org.apache.spark.tags.ExtendedSQLTest -l org.apache.spark.tags.SlowSQLTest' ;;
    sql_core-2)
      echo 'sql/testOnly * -- -n org.apache.spark.tags.ExtendedSQLTest' ;;
    sql_core-3)
      echo 'sql/testOnly * -- -n org.apache.spark.tags.SlowSQLTest' ;;
    sql_hive-1)
      echo 'hive/testOnly * -- -l org.apache.spark.tags.ExtendedHiveTest -l org.apache.spark.tags.SlowHiveTest' ;;
    sql_hive-2)
      echo 'hive/testOnly * -- -n org.apache.spark.tags.ExtendedHiveTest' ;;
    sql_hive-3)
      echo 'hive/testOnly * -- -n org.apache.spark.tags.SlowHiveTest' ;;
    *)
      return 1 ;;
  esac
}
