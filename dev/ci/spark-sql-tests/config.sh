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
SPARK_VERSION="4.1.1"
SPARK_SHORT="4.1"

# Git ref checked out for the Spark sources. Defaults to the released tag.
SPARK_REF="${SPARK_REF:-v${SPARK_VERSION}}"

# JDK major version the CI workflow uses for this Spark version.
REQUIRED_JDK="17"

# --- Paths -----------------------------------------------------------------
# Persistent apache/spark checkout. Reused across runs to avoid re-cloning.
COMET_SPARK_DIR="${COMET_SPARK_DIR:-$HOME/.cache/datafusion-comet/apache-spark}"

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
