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

# Everything dev/local-ci.sh needs to know about the CI configuration.
#
# The shell used to parse ci.yml and dev/ci/ with its own awk and sed, and
# check-ci-config.py parsed them again in Python so preflight could compare the
# two. That is worse than one parser twice over: it is two implementations to
# keep in step, and when they share a blind spot -- both stripped only single
# quotes, so `spark-full: "4.1.3"` came out with the quotes attached -- they
# agree with each other and the comparison says nothing.
#
# So there is one parser, here. The shell evals `--shell`, and preflight
# imports `config()` directly and checks the values rather than a second
# rendering of them.
#
# Usage:
#   local-ci-config.py --shell spark 4.1   eval-able assignments for one job
#   local-ci-config.py --print             every value, for eyeballing

import argparse
import json
import re
import shlex
import subprocess
import sys
from pathlib import Path

CI_YML = Path(".github/workflows/ci.yml")
SPARK_YML = Path(".github/workflows/spark_sql_test_reusable.yml")
ICEBERG_YML = Path(".github/workflows/iceberg_spark_test_reusable.yml")
SHARDS_PY = Path("dev/ci/check-iceberg-shards.py")
MODULES_PY = Path("dev/ci/spark-sql-modules.py")
POLICY_PY = Path("dev/ci/compute-changes.py")

# Values are versions or a JDK major. Anything else means a quoting or
# indentation change upstream has fooled the parse, and the shell would go on
# to build a URL or a Gradle task name out of it.
VERSION = re.compile(r"^\d+\.\d+$")
FULL_VERSION = re.compile(r"^\d+\.\d+\.\d+$")
MAJOR = re.compile(r"^\d+$")


class ConfigError(Exception):
    pass


def _check(value, pattern, what):
    if not pattern.match(value or ""):
        raise ConfigError(f"{what} is {value!r}, which is not shaped like a version")
    return value


def _job_inputs():
    """The `with:` inputs of every ci.yml job, keyed by job name."""
    jobs, job, in_with = {}, None, False
    for line in CI_YML.read_text(encoding="utf-8").splitlines():
        header = re.match(r"^  ([A-Za-z0-9_-]+):\s*$", line)
        if header:
            job, in_with = header.group(1), False
        elif job and re.match(r"^    with:\s*$", line):
            in_with = True
        elif in_with:
            entry = re.match(r"^      ([a-z][a-z0-9-]*):\s*(\S.*?)\s*$", line)
            if entry:
                # YAML accepts either quote style.
                jobs.setdefault(job, {})[entry.group(1)] = entry.group(2).strip("'\"")
            elif re.match(r"^    [a-z]", line):
                in_with = False
    return jobs


def _load(name, path):
    import importlib.util

    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _queue_version(prefix, policy):
    """The version the merge queue gates on: the newest Comet fully supports.

    Only purely version-shaped keys, since `spark_4_1_hive` is queue-tier too.
    """
    found = [
        match.group(1).replace("_", ".")
        for key, events in policy.items()
        for match in [re.fullmatch(rf"{prefix}_(\d+(?:_\d+)*)", key)]
        if match and "queue" in events
    ]
    if not found:
        raise ConfigError(f"no queue-tier {prefix} version in {POLICY_PY}")
    return max(found, key=lambda v: [int(part) for part in v.split(".")])


def _dedicated_gate():
    """The one Spark version whose suites the workflow process-isolates.

    Returns (version, suites). Every other version legitimately gets nothing,
    so the guard is on the line being present and parseable.
    """
    text = SPARK_YML.read_text(encoding="utf-8")
    match = re.search(r"DEDICATED_JVM_SBT_TESTS:.*?spark-short == '([^']*)' && '([^']*)'", text)
    if not match:
        raise ConfigError(f"no parseable DEDICATED_JVM_SBT_TESTS in {SPARK_YML}")
    return _check(match.group(1), VERSION, "dedicated-jvm gate"), match.group(2)


def _iceberg_scala():
    """ci.yml leaves `scala` unset, so the reusable workflow default applies."""
    text = ICEBERG_YML.read_text(encoding="utf-8")
    match = re.search(r"^      scala:.*?^        default:\s*'?([0-9.]+)'?", text, re.S | re.M)
    if not match:
        raise ConfigError(f"no scala default in {ICEBERG_YML}")
    return _check(match.group(1), VERSION, "iceberg scala")


def _iceberg_shards():
    shards = _load("iceberg_shards", SHARDS_PY).SHARD_COUNT
    if not isinstance(shards, int) or shards < 1:
        raise ConfigError(f"SHARD_COUNT in {SHARDS_PY} is {shards!r}")
    return shards


def spark_rows(selectors=()):
    """The Spark SQL matrix rows a selector names, in workflow order."""
    modules = _load("spark_sql_modules", MODULES_PY)
    rows = modules.select("all")
    names = [row["name"] for row in rows]
    picked = []
    for want in selectors or ["all"]:
        if want in ("all", "core", "hive"):
            picked += [r for r in rows if want == "all" or r["group"] == want]
        elif want in names:
            picked += [r for r in rows if r["name"] == want]
        else:
            raise ConfigError(
                f"unknown module {want!r}; try: " + ", ".join(names + ["all", "core", "hive"])
            )
    # Overlapping selectors, `core sql_core-1` say, would otherwise name a row
    # twice. Each row's tree and log are keyed on its name, so the duplicate
    # would delete the tree out from under the copy already running in it.
    seen = set()
    return [r for r in picked if not (r["name"] in seen or seen.add(r["name"]))]


def config():
    """Every value the script reads, validated. Raises ConfigError on drift."""
    jobs = _job_inputs()
    policy = _load("compute_changes", POLICY_PY).POLICY
    gate_version, gate_suites = _dedicated_gate()

    spark, iceberg = {}, {}
    for job, given in jobs.items():
        version = job.split("_", 1)[1].replace("_", ".") if "_" in job else job
        if re.fullmatch(r"spark_\d+_\d+", job) and "spark-full" in given:
            spark[_check(version, VERSION, job)] = {
                "full": _check(given.get("spark-full"), FULL_VERSION, f"{job} spark-full"),
                "java": _check(given.get("java"), MAJOR, f"{job} java"),
            }
        elif re.fullmatch(r"iceberg_\d+_\d+", job) and "iceberg-full" in given:
            iceberg[_check(version, VERSION, job)] = {
                "full": _check(given.get("iceberg-full"), FULL_VERSION, f"{job} iceberg-full"),
                "spark": _check(given.get("spark-short"), VERSION, f"{job} spark-short"),
                "java": _check(given.get("java"), MAJOR, f"{job} java"),
            }
    if not spark or not iceberg:
        raise ConfigError(f"no spark_*/iceberg_* jobs found in {CI_YML}")

    return {
        "spark": spark,
        "iceberg": iceberg,
        "spark_default": _queue_version("spark", policy),
        "iceberg_default": _queue_version("iceberg", policy),
        "iceberg_scala": _iceberg_scala(),
        "iceberg_shards": _iceberg_shards(),
        "dedicated_gate_version": gate_version,
        "dedicated_gate_suites": gate_suites,
        "rows": [row["name"] for row in spark_rows()],
    }


def shell(suite, version, selectors):
    """Eval-able assignments for one run of dev/local-ci.sh."""
    conf = config()
    if version is None:
        version = conf[f"{suite}_default"]
    known = conf[suite]
    if version not in known:
        raise ConfigError(
            f"unknown {suite} version {version!r}; try: " + ", ".join(sorted(known))
        )

    out = {"VERSION": version, "DEFAULTED": "1" if version == conf[f"{suite}_default"] else ""}
    if suite == "spark":
        rows = spark_rows(selectors)
        out["FULL"] = known[version]["full"]
        out["JAVA"] = known[version]["java"]
        # Unit separated per field, newline per row: a tab is IFS whitespace, so
        # the shell's `read` would collapse the empty args1 of an sql_core row.
        out["ROWS"] = "\n".join(
            "\x1f".join([r["name"], r["args1"], r["args2"], r["heap"], r["metaspace"]])
            for r in rows
        )
        out["PROJECTS"] = " ".join(
            sorted({(r["args1"] or r["args2"]).split("/")[0] for r in rows})
        )
        out["DEDICATED"] = (
            conf["dedicated_gate_suites"] if version == conf["dedicated_gate_version"] else ""
        )
    else:
        out["FULL"] = known[version]["full"]
        out["SPARK"] = known[version]["spark"]
        out["JAVA"] = known[version]["java"]
        out["SCALA"] = conf["iceberg_scala"]
        out["SHARDS"] = str(conf["iceberg_shards"])
    return "\n".join(f"{k}={shlex.quote(v)}" for k, v in out.items())


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shell", action="store_true", help="emit eval-able assignments")
    parser.add_argument("--print", dest="show", action="store_true", help="show every value")
    parser.add_argument("suite", nargs="?", choices=("spark", "iceberg"))
    parser.add_argument("version", nargs="?")
    parser.add_argument("selectors", nargs="*")
    args = parser.parse_args(argv)
    try:
        if args.shell:
            if not args.suite:
                parser.error("--shell needs a suite")
            print(shell(args.suite, args.version or None, args.selectors))
        else:
            print(json.dumps(config(), indent=2, sort_keys=True))
    except ConfigError as err:
        print(f"local-ci config: {err}", file=sys.stderr)
        return 1
    except subprocess.CalledProcessError as err:
        print(f"local-ci config: {err}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
