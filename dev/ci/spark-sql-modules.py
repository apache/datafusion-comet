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
# They live here so that a caller can ask for a subset: the umbrella keeps the
# Spark 4.1 `sql_hive` shards out of the PR tier (see POLICY in
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
MODULES = [
    {"name": "catalyst", "group": "core", "args1": "catalyst/test", "args2": ""},
    {
        "name": "sql_core-1",
        "group": "core",
        "args1": "",
        "args2": "sql/testOnly * -- -l org.apache.spark.tags.ExtendedSQLTest -l org.apache.spark.tags.SlowSQLTest",
        "heap": "3g",
        "metaspace": "1g",
    },
    {
        "name": "sql_core-2",
        "group": "core",
        "args1": "",
        "args2": "sql/testOnly * -- -n org.apache.spark.tags.ExtendedSQLTest",
        "heap": "3g",
        "metaspace": "1g",
    },
    {
        "name": "sql_core-3",
        "group": "core",
        "args1": "",
        "args2": "sql/testOnly * -- -n org.apache.spark.tags.SlowSQLTest",
        "heap": "3g",
        "metaspace": "1g",
    },
    {
        "name": "sql_hive-1",
        "group": "hive",
        "args1": "",
        "args2": "hive/testOnly * -- -l org.apache.spark.tags.ExtendedHiveTest -l org.apache.spark.tags.SlowHiveTest",
    },
    {
        "name": "sql_hive-2",
        "group": "hive",
        "args1": "",
        "args2": "hive/testOnly * -- -n org.apache.spark.tags.ExtendedHiveTest",
    },
    {
        "name": "sql_hive-3",
        "group": "hive",
        "args1": "",
        "args2": "hive/testOnly * -- -n org.apache.spark.tags.SlowHiveTest",
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
