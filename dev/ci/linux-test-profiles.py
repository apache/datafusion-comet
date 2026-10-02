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

# The `profile` dimension of the `linux-test` matrix in
# .github/workflows/pr_build_linux.yml.
#
# The five Spark profiles cost about the same each, and together they are
# three quarters of what the Linux build spends on a pull request. Only one of
# them is the default build profile, so a pull request and the merge queue run
# the Comet test suites against that one and the nightly run covers the other
# four. A job-level `if:` cannot see `matrix`, so the selection has to happen
# before the matrix is expanded: the `lint` job runs this script and publishes
# the result as a job output that `linux-test` reads with `fromJSON`, the same
# way spark-sql-modules.py picks the Spark SQL shards.
#
# `lint-java` keeps its own literal profile list. It compiles every profile it
# can on every pull request (about five minutes each), which is what keeps a
# shim that fails to compile on Spark 3.x from reaching the queue; only the
# runtime suites move behind it.
#
# Usage:
#   linux-test-profiles.py --profiles all|pr|nightly --github-output $GITHUB_OUTPUT
#   linux-test-profiles.py --profiles pr          (prints the matrix JSON)

import argparse
import json
import sys
from pathlib import Path

# `tier` is what --profiles selects on: "pr" rows run on every pull request
# and in the queue, "nightly" rows only in the nightly run (or with the
# `run-all-spark-profiles` label). The goal of the list is coverage of every
# Java, Scala and Spark version without testing every combination.
PROFILES = [
    {"name": "Spark 3.4, JDK 17, Scala 2.12", "java_version": "17", "maven_opts": "-Pspark-3.4 -Pscala-2.12", "tier": "nightly"},
    {"name": "Spark 3.5, JDK 17, Scala 2.13", "java_version": "17", "maven_opts": "-Pspark-3.5 -Pscala-2.13", "tier": "nightly"},
    {"name": "Spark 4.0, JDK 21", "java_version": "21", "maven_opts": "-Pspark-4.0", "tier": "nightly"},
    # The default build profile, and the one a contributor builds locally.
    {"name": "Spark 4.1, JDK 17", "java_version": "17", "maven_opts": "-Pspark-4.1", "tier": "pr"},
    {"name": "Spark 4.2, JDK 17", "java_version": "17", "maven_opts": "-Pspark-4.2", "tier": "nightly"},
]

SELECTORS = ("all", "pr", "nightly")


def select(profiles):
    """Return the matrix rows for a --profiles value, or raise ValueError."""
    if profiles not in SELECTORS:
        raise ValueError(f"--profiles must be one of {', '.join(SELECTORS)}, got {profiles!r}")
    rows = [row for row in PROFILES if profiles == "all" or row["tier"] == profiles]
    return [{key: value for key, value in row.items() if key != "tier"} for row in rows]


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--profiles",
        default="all",
        help="all, pr (the profiles every pull request runs) or nightly (the ones it does not)",
    )
    parser.add_argument("--github-output", type=Path, help="append matrix=<json> to this $GITHUB_OUTPUT file")
    args = parser.parse_args(argv)
    try:
        rows = select(args.profiles)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    matrix = json.dumps(rows)
    if args.github_output:
        with args.github_output.open("a", encoding="utf-8") as out:
            out.write(f"matrix={matrix}\n")
    else:
        print(matrix)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
