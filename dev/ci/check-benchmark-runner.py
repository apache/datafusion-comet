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

"""Checks for benchmarks/micro/run.py, which no other CI job touches.

Importing the module is a syntax check. Beyond that this exercises the pure
helpers and, most importantly, verifies that the suite names hard coded in the
runner still match the benchmark sources, so that a rename does not silently
turn an exclusion into a suite that runs or a timeout override into one that
does not apply.

Run from the repository root:

    python3 dev/ci/check-benchmark-runner.py
"""

import importlib.util
import sys
import tempfile
from argparse import Namespace
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
RUNNER = REPO_ROOT / "benchmarks" / "micro" / "run.py"


def load_runner():
    spec = importlib.util.spec_from_file_location("comet_bench_runner", RUNNER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def check_suite_discovery(runner, failures):
    suites = runner.discover_suites(REPO_ROOT)
    # Guards against the regex silently matching nothing after a source reshuffle
    if len(suites) < 30:
        failures.append(f"only {len(suites)} benchmark suites discovered: {suites}")
    for expected in ("CometStringExpressionBenchmark", "CometC2RIsolatedBench"):
        if expected not in suites:
            failures.append(f"{expected} was not discovered in {runner.BENCH_SOURCE_DIR}")
    for helper in ("TPCDSSchemaHelper",):
        if helper in suites:
            failures.append(f"{helper} is not a benchmark but was discovered as one")
    failures.extend(runner.stale_suite_entries(REPO_ROOT))


def check_suite_selection(runner, failures):
    def selected(**overrides):
        args = Namespace(suites=None, only=None, skip=None)
        for key, value in overrides.items():
            setattr(args, key, value)
        return runner.load_suites(args, REPO_ROOT)

    default = selected()
    for excluded in runner.EXCLUDED_SUITES:
        if excluded in default:
            failures.append(f"{excluded} is excluded but is in the default set")
    if not all("Cast" in suite for suite in selected(only=["Cast"])):
        failures.append("--only did not restrict the selection")
    if any("Cast" in suite for suite in selected(skip=["Cast"])):
        failures.append("--skip did not remove the matching suites")


def check_helpers(runner, failures):
    cases = [
        (runner.format_duration(0), "0s"),
        (runner.format_duration(59), "59s"),
        (runner.format_duration(60), "1m00s"),
        (runner.format_duration(3661), "1h01m01s"),
        (runner.protoc_asset_name().startswith(f"protoc-{runner.PROTOC_VERSION}-"), True),
    ]
    for actual, expected in cases:
        if actual != expected:
            failures.append(f"expected {expected!r}, got {actual!r}")


def check_log_parsing(runner, failures, tmp_dir):
    log_path = tmp_dir / "suite.log"
    log_path.write_text(
        "$ ../mvnw exec:java\n"
        "[INFO] Scanning for projects...\n"
        "java.lang.OutOfMemoryError: Java heap space\n"
        "\tat org.apache.comet.Example.run(Example.scala:1)\n"
        "[INFO] BUILD FAILURE\n"
    )
    excerpt = runner.failure_excerpt(log_path)
    if "OutOfMemoryError" not in excerpt.splitlines()[0]:
        failures.append(f"failure_excerpt did not start at the exception: {excerpt!r}")

    maven_only = tmp_dir / "maven.log"
    maven_only.write_text("[INFO] ok\n[ERROR] Failed to execute goal exec:java\n")
    if "Failed to execute goal" not in runner.failure_excerpt(maven_only):
        failures.append("failure_excerpt did not fall back to the Maven failure")


def main():
    if not RUNNER.is_file():
        print(f"error: {RUNNER} not found", file=sys.stderr)
        return 1
    runner = load_runner()
    failures = []
    check_suite_discovery(runner, failures)
    check_suite_selection(runner, failures)
    check_helpers(runner, failures)

    with tempfile.TemporaryDirectory() as tmp_dir:
        check_log_parsing(runner, failures, Path(tmp_dir))

    for failure in failures:
        print(f"error: {failure}", file=sys.stderr)
    if failures:
        return 1
    print(f"benchmarks/micro/run.py checks passed ({len(runner.discover_suites(REPO_ROOT))} suites)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
