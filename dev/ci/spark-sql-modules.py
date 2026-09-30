# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# The test matrix for .github/workflows/spark_sql_test_reusable.yml.
#
# The rows used to be a literal `strategy.matrix.module` list in the workflow.
# They live here so that a caller can ask for a subset: the umbrella lets a
# pull request opt into only the Spark 4.1 `sql_hive` shards (see POLICY in
# dev/ci/compute-changes.py and issue #5870) and a job-level `if:` cannot see
# `matrix`, so the selection has to happen before the matrix is expanded. The
# `build` job runs this script and publishes the result as a job output that
# `spark-sql-test` reads with `fromJSON`, the same way the Iceberg reusable
# workflow sizes its shards.
#
# Usage:
#   spark-sql-modules.py --modules all|core|hive --github-output $GITHUB_OUTPUT
#   spark-sql-modules.py --modules core          (prints the matrix JSON)

import argparse
import json
import sys
from pathlib import Path

# `group` is what --modules selects on. `heap` and `metaspace` are read by the
# "Run Spark tests" step as per-row forked-test-JVM caps for SparkBuild.scala;
# the sql_core rows set them because those shards were the ones hitting the
# 7 GB runner budget.
#
# The tag filters split each module three ways, the same as Spark's own CI.
# That left two rows far longer than the rest, and the longest row is what a
# merge-queue run waits for, so the suites below are moved into rows of their
# own (issue #6388). Each is excluded from the row it came from with sbt's
# `-<glob>` testOnly syntax, and its row keeps that row's tag filters, so every
# test still runs exactly once. The globs are package prefixes, which also
# catch suites added to those packages later.
#
# HivePartitionFilteringSuites runs HivePartitionFilteringSuite against every
# Hive client version, 21 to 35 minutes of sql_hive-1 in September 2026. It is
# one class, so no name filter splits it further.
HIVE_MOVED = ["org.apache.spark.sql.hive.client.HivePartitionFilteringSuites"]
# The data source and connector suites were 13 to 19 minutes of sql_core-1 and,
# with their Extended-tagged schema-pruning suites, 11 minutes of sql_core-2.
CORE_MOVED = [
    "org.apache.spark.sql.execution.datasources.*",
    "org.apache.spark.sql.connector.*",
]

SQL_EXTENDED = "org.apache.spark.tags.ExtendedSQLTest"
SQL_SLOW = "org.apache.spark.tags.SlowSQLTest"
HIVE_EXTENDED = "org.apache.spark.tags.ExtendedHiveTest"
HIVE_SLOW = "org.apache.spark.tags.SlowHiveTest"


def excluding(globs):
    return " ".join(f"-{glob}" for glob in globs)


MODULES = [
    {"name": "catalyst", "group": "core", "args1": "catalyst/test", "args2": ""},
    {
        "name": "sql_core-1",
        "group": "core",
        "args1": "",
        "args2": f"sql/testOnly * {excluding(CORE_MOVED)} -- -l {SQL_EXTENDED} -l {SQL_SLOW}",
        "heap": "3g",
        "metaspace": "1g",
    },
    {
        "name": "sql_core-2",
        "group": "core",
        "args1": "",
        "args2": f"sql/testOnly * {excluding(CORE_MOVED)} -- -n {SQL_EXTENDED}",
        "heap": "3g",
        "metaspace": "1g",
    },
    {
        "name": "sql_core-3",
        "group": "core",
        "args1": "",
        "args2": f"sql/testOnly * -- -n {SQL_SLOW}",
        "heap": "3g",
        "metaspace": "1g",
    },
    {
        # The moved suites' untagged and Extended tests; their Slow tests stay
        # in sql_core-3 with every other Slow test.
        "name": "sql_core-4",
        "group": "core",
        "args1": "",
        "args2": f"sql/testOnly {' '.join(CORE_MOVED)} -- -l {SQL_SLOW}",
        "heap": "3g",
        "metaspace": "1g",
    },
    {
        "name": "sql_hive-1",
        "group": "hive",
        "args1": "",
        "args2": f"hive/testOnly * {excluding(HIVE_MOVED)} -- -l {HIVE_EXTENDED} -l {HIVE_SLOW}",
    },
    {
        "name": "sql_hive-2",
        "group": "hive",
        "args1": "",
        "args2": f"hive/testOnly * -- -n {HIVE_EXTENDED}",
    },
    {
        "name": "sql_hive-3",
        "group": "hive",
        "args1": "",
        "args2": f"hive/testOnly * -- -n {HIVE_SLOW}",
    },
    {
        "name": "sql_hive-4",
        "group": "hive",
        "args1": "",
        "args2": f"hive/testOnly {' '.join(HIVE_MOVED)} -- -l {HIVE_EXTENDED} -l {HIVE_SLOW}",
    },
]

GROUPS = ("all", "core", "hive")


def select(modules):
    """Return the matrix rows for a --modules value, or raise ValueError."""
    if modules not in GROUPS:
        raise ValueError(f"--modules must be one of {', '.join(GROUPS)}, got {modules!r}")
    rows = [row for row in MODULES if modules == "all" or row["group"] == modules]
    # Every row carries every key, so a `${{ matrix.module.heap }}` lookup on a
    # row without a cap is an empty string rather than a template error.
    keys = sorted({key for row in MODULES for key in row})
    return [{key: row.get(key, "") for key in keys} for row in rows]


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--modules", default="all", help="all, core (catalyst + sql_core) or hive (sql_hive)")
    parser.add_argument("--github-output", type=Path, help="append matrix=<json> to this $GITHUB_OUTPUT file")
    args = parser.parse_args(argv)
    try:
        rows = select(args.modules)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    matrix = json.dumps({"module": rows})
    if args.github_output:
        with args.github_output.open("a", encoding="utf-8") as out:
            out.write(f"matrix={matrix}\n")
    else:
        print(matrix)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
