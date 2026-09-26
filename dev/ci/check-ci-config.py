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

# Guards seven CI invariants that are silent when broken:
#
#   1. Change-filter routing. dev/ci/compute-changes.py decides which heavy
#      jobs run. A file that a job depends on but that no filter lists makes
#      that job skip, so the edit merges with only preflight having looked at
#      it. The table below pins the routing for the shared build inputs.
#
#   2. Event policy. The same script decides which events may run each job.
#      That used to be a `${{ }}` expression on every job in ci.yml, where it
#      could not be tested; POLICY_CASES below is the test it never had. The
#      expected sets are transcribed from the `if:` expressions ci.yml carried
#      before the policy moved, so a regression here is a behaviour change.
#
#   3. Required-check coverage. `Required Checks` in ci.yml is the job that
#      `.asf.yaml` can name in `required_status_checks` for main. A heavy job
#      missing from its `needs:` can fail without blocking the merge, and a
#      rename on either side of the ci.yml/.asf.yaml pair turns the required
#      context into one that never reports, which blocks *every* merge to main
#      until INFRA removes it by hand. The job's name must also route `labeled`
#      runs, which skip the PR tier by design, to a name nothing requires, so
#      a label run cannot stand in for the real verdict. And `labeled` runs
#      must come from a separate workflow (ci_label.yml) rather than ci.yml's
#      own trigger: with two ci.yml runs at one commit, GitHub evaluates the
#      required checks against only one of them, and when that is the label
#      run the required context never reports (issue #6159).
#
#   4. Artifact-name uniqueness. Artifact names are scoped to the *run*, not
#      to the calling workflow, and ci.yml calls the Spark SQL and Iceberg
#      reusable workflows several times in one run. Two producers sharing a
#      name make `download-artifact` pick by highest artifact ID rather than
#      by `needs`, and make the forced `overwrite` on an upload retry delete
#      a sibling's finished artifact.
#
#   5. Local actions resolve from the workspace, so a `uses: ./.github/...`
#      in a job that skipped the checkout cannot be loaded at all. Jobs that
#      run only under an input or a label can carry that for a long time
#      before anyone runs them.
#
#   6. Push-tier scope. On push to main, ci.yml calls pr_build_linux.yml with
#      `cache-refresh-only`, which reduces it to the jobs that write an
#      actions/cache entry; the merge queue already tested that tree. A job
#      added to that workflow without the guard starts running on every push
#      again and nothing fails, so nothing tells you.
#
#   7. Cache save scope. actions/cache entries are scoped to the ref that
#      wrote them, so one written from a pull request or from the queue's
#      throwaway branch can never be restored again -- but it still counts
#      against the repository's shared budget and evicts main's entries,
#      which is how every native build in the queue came to miss its cargo
#      cache and pay a cold ~26 minute compile.
#
# Run from the repository root: python3 dev/ci/check-ci-config.py

import importlib.util
import re
import subprocess
import sys
from pathlib import Path

WORKFLOWS = Path(".github/workflows")
ASF_YAML = Path(".asf.yaml")
LOCAL_CI = Path("dev/local-ci.sh")

# The ci.yml job that aggregates every other job's result.
AGGREGATOR_JOB = "required_checks"

# The aggregator's `name:` has to be this expression shape: label runs publish
# the first literal, every other event the second. See the comment on the job.
AGGREGATOR_NAME_EXPR = re.compile(
    r"^\$\{\{\s*github\.event\.action\s*==\s*'labeled'\s*&&\s*"
    r"'([^']+)'\s*\|\|\s*'([^']+)'\s*\}\}$"
)

# Jobs that legitimately stay out of the aggregator's `needs:`. `docs` deploys
# to asf-site on push to main; it gates nothing and is never part of a merge
# decision, so folding it in would only turn a failed site deploy into a red
# `Required Checks` on main. `nightly_report` runs *after* the aggregator, on
# the scheduled event only, to open an issue when the nightly tier fails.
AGGREGATOR_EXEMPT = {AGGREGATOR_JOB, "docs", "nightly_report"}

# Changed-file list -> the set of outputs compute-changes.py must report true.
# Every other output must be false. Keep one case per shared build input so a
# filter deletion cannot pass unnoticed.
BUILD_JOBS = {
    "build_linux",
    "build_linux_full",
    "build_linux_all_profiles",
    "build_macos",
    "spark_3_4",
    "spark_3_5",
    "spark_4_0",
    "spark_4_1",
    "spark_4_1_hive",
    "iceberg_1_8",
    "iceberg_1_9",
    "iceberg_1_10",
    "iceberg_1_11",
}

# The two contrib/UDF gates also run ./mvnw, but consume no shared artifact.
MVN_JOBS = BUILD_JOBS | {"delta_gate", "pyarrow_udf"}

ROUTING_CASES = [
    # The Maven wrapper and its config feed every job that runs ./mvnw: the
    # Linux/macOS builds, setup-spark-builder, the Iceberg `mvnw install`, the
    # Delta gate's effective-pom check and the PyArrow suite's `mvnw install`.
    ([".mvn/maven.config"], MVN_JOBS),
    ([".mvn/wrapper/maven-wrapper.properties"], MVN_JOBS),
    (["mvnw"], MVN_JOBS),
    # The artifact wrappers are used by every producer and consumer of a
    # shared artifact. Without these, an edit confined to one of them routes
    # to nothing at all and merges having been exercised by no consumer.
    ([".github/actions/upload-artifact-retry/action.yaml"], BUILD_JOBS),
    ([".github/actions/download-artifact-retry/action.yaml"], BUILD_JOBS),
    # Maven bootstrap runs inside setup-builder and setup-macos-builder, so it
    # reaches every job that runs ./mvnw, the Delta gate and PyArrow suite
    # included.
    ([".github/actions/maven-bootstrap/action.yaml"], MVN_JOBS),
    # Spot checks that the additions above did not widen unrelated routes.
    (["docs/source/user-guide/overview.md"], {"docs"}),
    (["native/core/benches/parquet_read.rs"], {"benchmark"}),
    # The mermaid guard is run by preflight, which is unconditional, and again
    # by the docs deploy, which is not, so the deploy has to be routed. The
    # build jobs come along because `dev/ci/**` already feeds them.
    (
        ["dev/ci/check-mermaid.py"],
        {
            "docs",
            "build_linux",
            "build_linux_full",
            "build_linux_all_profiles",
            "build_macos",
        },
    ),
    # The Delta gate script is read by nothing else; the contrib crate feeds
    # only the gate. The PyArrow pytest lives under spark/, so the Linux and
    # macOS builds see it too, but no Spark SQL or Iceberg suite does, and
    # neither does the Delta gate, which only inspects build output.
    (["dev/verify-contrib-delta-gate.sh"], {"delta_gate"}),
    (["contrib/delta/native/src/lib.rs"], {"delta_gate"}),
    (
        ["spark/src/test/resources/pyspark/test_pyarrow_udf.py"],
        {
            "build_linux",
            "build_linux_full",
            "build_linux_all_profiles",
            "build_macos",
            "pyarrow_udf",
        },
    ),
]

# Event policy. Each case is (event, expected set of jobs allowed to run),
# where "allowed" ignores path filters. Written out longhand rather than
# derived from POLICY, so that a change to the routing has to be stated twice
# and cannot be made by accident.
# The PR tier is the Linux build and nothing else. Every Spark SQL and Iceberg
# suite waits for the queue or the nightly run, or for its label.
PR_TIER = {"build_linux", "build_linux_full"}
# The queue adds one Spark version (4.1, the default profile) and one Iceberg
# version (1.11, the only Spark 4.1 coverage), plus the build-level gates.
QUEUE_TIER = PR_TIER | {
    "spark_4_1",
    "spark_4_1_hive",
    "iceberg_1_11",
    "build_macos",
    "benchmark",
    "delta_gate",
    "pyarrow_udf",
}
# Every other Spark and Iceberg version, and the linux-test matrix's other
# Spark profiles (`build_linux_all_profiles`, part of the Linux build's call
# rather than a job of its own), run once a night against main instead.
NIGHTLY_TIER = {
    "spark_3_5",
    "spark_4_0",
    "iceberg_1_8",
    "iceberg_1_9",
    "iceberg_1_10",
    "build_linux_all_profiles",
}
# Spark 3.4 is deprecated and sits outside the queue and nightly tiers
# entirely: a label on a pull request, or a workflow_dispatch, and nothing
# else. Keeping it in its own set is what makes the `merge_group` and
# `schedule` cases below assert its absence rather than quietly accept it
# coming back.
SPARK_DEPRECATED = {"spark_3_4"}
# One label opts a pull request into every Iceberg version, whichever tier
# each sits in.
ICEBERG_OPT_IN = {"iceberg_1_8", "iceberg_1_9", "iceberg_1_10", "iceberg_1_11"}
ALL_JOBS = QUEUE_TIER | NIGHTLY_TIER | SPARK_DEPRECATED | {"docs"}
assert not QUEUE_TIER & NIGHTLY_TIER, "a job is queue or nightly, never both"

POLICY_CASES = [
    # A manual run may exercise anything.
    ({"name": "workflow_dispatch"}, ALL_JOBS),
    # The merge queue is the authoritative gate: everything except the site
    # deploy, which can only run once the commit is actually on main, the
    # nightly tier, and the deprecated Spark 3.4 suite, which no longer gates
    # a merge.
    ({"name": "merge_group"}, QUEUE_TIER),
    # The scheduled run is the nightly tier and nothing else. The queue already
    # ran everything in QUEUE_TIER against the tree that is now main, so a
    # queue job showing up here is a suite being paid for twice a day.
    ({"name": "schedule"}, NIGHTLY_TIER),
    # Push to main is the site deploy plus the Linux build, which is there to
    # refresh main's actions/cache entries (see POLICY). `build_linux_full`
    # must stay out: it is what turns the lints and the test matrix back on,
    # and the queue has already run those against the tree that landed. Any
    # other test job showing up here means every merge is paying for it twice.
    ({"name": "push"}, {"docs", "build_linux"}),
    # A plain pull request: the PR tier only. docs must never run here, and the
    # opt-in suites stay off without their label.
    ({"name": "pull_request", "action": "opened", "labels": []}, PR_TIER),
    ({"name": "pull_request", "action": "synchronize", "labels": []}, PR_TIER),
    # Spark 3.5 runs nightly; its label is the escape hatch.
    (
        {"name": "pull_request", "action": "synchronize", "labels": ["run-spark-3.5-tests"]},
        PR_TIER | {"spark_3_5"},
    ),
    # The macOS build and the benchmark compile check are queue-only, each
    # with its own label. Neither label pulls in the other.
    (
        {"name": "pull_request", "action": "synchronize", "labels": ["run-macos-tests"]},
        PR_TIER | {"build_macos"},
    ),
    (
        {"name": "pull_request", "action": "synchronize", "labels": ["run-benchmark-check"]},
        PR_TIER | {"benchmark"},
    ),
    # The Delta build gate and the PyArrow UDF suite were standalone workflows
    # that ran on every pull request and again on push to main. Folded in as
    # queue-only jobs, each with its own label, they follow the same rules.
    (
        {"name": "pull_request", "action": "synchronize", "labels": ["run-delta-build-gate"]},
        PR_TIER | {"delta_gate"},
    ),
    (
        {"name": "pull_request", "action": "synchronize", "labels": ["run-pyarrow-udf-tests"]},
        PR_TIER | {"pyarrow_udf"},
    ),
    (
        {
            "name": "pull_request",
            "action": "labeled",
            "label": "run-pyarrow-udf-tests",
            "labels": ["run-pyarrow-udf-tests"],
        },
        {"pyarrow_udf"},
    ),
    (
        {
            "name": "pull_request",
            "action": "labeled",
            "label": "run-macos-tests",
            "labels": ["run-macos-tests"],
        },
        {"build_macos"},
    ),
    # The linux-test matrix's non-default Spark profiles run nightly, with
    # their own label. On a pushed commit the label adds them to the PR tier's
    # Linux build call (`profiles: all`); on the `labeled` event alone it is
    # the only output set, and ci.yml turns that into `profiles: nightly`
    # so the default profile, which already ran at this commit, is not repeated.
    (
        {"name": "pull_request", "action": "synchronize", "labels": ["run-all-spark-profiles"]},
        PR_TIER | {"build_linux_all_profiles"},
    ),
    (
        {
            "name": "pull_request",
            "action": "labeled",
            "label": "run-all-spark-profiles",
            "labels": ["run-all-spark-profiles"],
        },
        {"build_linux_all_profiles"},
    ),
    # Spark 4.1 is queue-only too. Two labels feed its one call: the suite
    # label selects every module, the hive label only the sql_hive shards.
    # Neither label pulls in any other Spark version.
    (
        {"name": "pull_request", "action": "synchronize", "labels": ["run-spark-4.1-tests"]},
        PR_TIER | {"spark_4_1", "spark_4_1_hive"},
    ),
    (
        {
            "name": "pull_request",
            "action": "labeled",
            "label": "run-spark-4.1-tests",
            "labels": ["run-spark-4.1-tests"],
        },
        {"spark_4_1", "spark_4_1_hive"},
    ),
    (
        {"name": "pull_request", "action": "synchronize", "labels": ["run-spark-4.1-hive-tests"]},
        PR_TIER | {"spark_4_1_hive"},
    ),
    (
        {
            "name": "pull_request",
            "action": "labeled",
            "label": "run-spark-4.1-hive-tests",
            "labels": ["run-spark-4.1-hive-tests"],
        },
        {"spark_4_1_hive"},
    ),
    # Adding the hive label on top of the suite label re-runs only the hive
    # rows: a `labeled` run selects what the new label gates, and the suite
    # label's earlier run already covered every module at this commit.
    (
        {
            "name": "pull_request",
            "action": "labeled",
            "label": "run-spark-4.1-hive-tests",
            "labels": ["run-spark-4.1-tests", "run-spark-4.1-hive-tests"],
        },
        {"spark_4_1_hive"},
    ),
    # An opt-in label present on a pushed commit adds just that suite. For the
    # deprecated Spark 3.4 suite the label is the *only* way it ever runs on a
    # pull request or the queue, so this case and the `labeled` one below are
    # what keep it reachable at all.
    (
        {"name": "pull_request", "action": "synchronize", "labels": ["run-spark-3.4-tests"]},
        PR_TIER | {"spark_3_4"},
    ),
    (
        {
            "name": "pull_request",
            "action": "labeled",
            "label": "run-spark-3.4-tests",
            "labels": ["run-spark-3.4-tests"],
        },
        {"spark_3_4"},
    ),
    (
        {"name": "pull_request", "action": "synchronize", "labels": ["run-iceberg-tests"]},
        PR_TIER | ICEBERG_OPT_IN,
    ),
    # Applying a gating label runs only what that label gates. The PR tier
    # already ran at this commit on opened/synchronize.
    (
        {
            "name": "pull_request",
            "action": "labeled",
            "label": "run-spark-4.0-tests",
            "labels": ["run-spark-4.0-tests"],
        },
        {"spark_4_0"},
    ),
    (
        {
            "name": "pull_request",
            "action": "labeled",
            "label": "run-iceberg-tests",
            "labels": ["run-iceberg-tests"],
        },
        ICEBERG_OPT_IN,
    ),
    # A label that gates nothing (dependabot's `dependencies`, a type label)
    # must not start a second pipeline. This is issue #5007.
    (
        {"name": "pull_request", "action": "labeled", "label": "dependencies", "labels": ["dependencies"]},
        set(),
    ),
    # ... not even when a gating label is already on the PR from earlier.
    (
        {
            "name": "pull_request",
            "action": "labeled",
            "label": "dependencies",
            "labels": ["dependencies", "run-spark-3.4-tests"],
        },
        set(),
    ),
]


# `uses:` values that publish an artifact, and the one that consumes it.
UPLOAD_USES = re.compile(r"uses:\s*(\./\.github/actions/upload-artifact-retry|actions/upload-artifact@)")
DOWNLOAD_USES = re.compile(r"uses:\s*(\./\.github/actions/download-artifact-retry|actions/download-artifact@)")
# The artifact name is the first `name:` key of the step's `with:` block. A
# following step starts with `- `, which distinguishes it from a `with:` key.
WITH_NAME = re.compile(r"^\s+name:\s*(\S.*?)\s*$")
NEW_STEP = re.compile(r"^\s*-\s")

# A job id in a workflow file, and the two `uses:` shapes the checkout guard
# below cares about. `./.github/workflows/` is deliberately not matched: that
# is a reusable-workflow call, which resolves from the repository rather than
# from the runner's workspace and so needs no checkout.
JOB_KEY = re.compile(r"^  ([A-Za-z0-9_-]+):\s*$")
LOCAL_ACTION_USES = re.compile(r"uses:\s*(\./\.github/actions/\S+)")
CHECKOUT_USES = re.compile(r"uses:\s*actions/checkout@")

# pr_build_linux.yml runs in two modes; see its header. These are the jobs that
# must survive `cache-refresh-only`, because each one writes an actions/cache
# entry that main needs warm for the next pull request. Anything else in that
# file has to carry the guard.
CACHE_REFRESH_WORKFLOW = WORKFLOWS / "pr_build_linux.yml"
CACHE_REFRESH_JOBS = {
    "lint": "gates build-native and linux-test-rust, and costs 40 seconds",
    "build-native": "writes the cargo-ci cache (native/target, CI profile)",
    "linux-test-rust": "writes the cargo-debug cache (native/target, debug)",
    "verify-benchmark-results-tpch": "writes the TPC-H SF=1 dataset and java-maven caches",
    "verify-benchmark-results-tpcds": "writes the TPC-DS SF=1 dataset and java-maven caches",
}
# Job-level `if:` only: step-level guards inside the two verify jobs are
# indented further, and those are expected rather than a reason to exempt the
# whole job.
CACHE_REFRESH_GUARD = re.compile(r"^    if:.*!\s*inputs\.cache-refresh-only")
# The same file's third mode: with `profiles: nightly` only the jobs the
# linux-test matrix needs run. See check_nightly_scope.
NIGHTLY_JOBS = {
    "lint": "publishes the profile matrix that linux-test reads",
    "build-native": "builds the native library the matrix loads",
    "linux-test": "the matrix itself",
}
NIGHTLY_GUARD = re.compile(r"^    if:.*inputs\.profiles\s*!=\s*'nightly'")
# The `schedule` case in ci.yml's `Detect changes` script, and what has to be
# in it. See check_nightly_base_fallback.
CI_WORKFLOW = WORKFLOWS / "ci.yml"
SCHEDULE_BRANCH = re.compile(r'^(\s+)elif \[\[ "\$EVENT_NAME" == "schedule" \]\]; then\s*$')
# Any base derived from a clock rather than from the previous run.
DATE_BASED_BASE = re.compile(r"--(before|since|after|until)\b")


def guarded_jobs(path, guard):
    """Return (job ids in `path`, the subset whose job-level `if:` matches `guard`)."""
    jobs, guarded, job, in_jobs = [], set(), None, False
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("jobs:"):
            in_jobs = True
            continue
        if not in_jobs or line.lstrip().startswith("#"):
            continue
        match = JOB_KEY.match(line)
        if match:
            job = match.group(1)
            jobs.append(job)
            continue
        if job and guard.match(line):
            guarded.add(job)
    return jobs, guarded
CACHE_REFRESH_INPUT = re.compile(r"^\s+cache-refresh-only:\s*\$\{\{")
# `profiles:` is passed as a folded scalar (`>-`) whose expression sits on the
# next line, so match the key alone.
PROFILES_INPUT = re.compile(r"^\s+profiles:\s*(>-|\$\{\{)")

# Cache paths big enough that writing one from a throwaway ref costs main its
# own entries. The repository's actions/cache budget is shared and evicted
# least-recently-used, and a Maven repository or a cargo tree runs to gigabytes
# apiece.
# Matched as substrings of the step's `path:`, so they have to survive the
# prefix being an expression: publish_snapshot.yml writes
# `${{ env.CARGO_HOME }}/registry`, which no `~/.cargo/...` literal would
# catch.
LARGE_CACHE_PATHS = (
    ".m2/repository",
    ".cargo/registry",
    ".cargo/git",
    "CARGO_HOME",
    "native/target",
)
# `actions/cache@vN` saves in an implicit post step that no `if:` can reach, so
# a large cache has to be split into `restore` plus a guarded `save`.
CACHE_RW_USES = re.compile(r"^\s*(?:-\s*)?uses:\s*actions/cache@v\d+\s*$")
CACHE_SAVE_USES = re.compile(r"^\s*(?:-\s*)?uses:\s*actions/cache/save@v\d+\s*$")
CACHE_MAIN_GUARD = re.compile(r"github\.ref\s*==\s*'refs/heads/main'")
# publish_snapshot.yml runs from main on a schedule, so its entries already
# land in the only scope that helps. (The TPC-H/TPC-DS dataset caches need no
# entry here: `./tpch` and `./tpcds-sf-1` are not dependency trees and are not
# in LARGE_CACHE_PATHS, so this check never looks at them.)
CACHE_SAVE_SCOPE_EXEMPT = {
    ("publish_snapshot.yml", "snapshot-cargo-"),
    ("publish_snapshot.yml", "snapshot-maven-"),
}


def load_filters():
    spec = importlib.util.spec_from_file_location("compute_changes", "dev/ci/compute-changes.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_ci_module(name, filename):
    """Import one of the hyphenated dev/ci scripts as a module."""
    spec = importlib.util.spec_from_file_location(name, f"dev/ci/{filename}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def check_local_ci_config():
    """dev/local-ci.sh reads ci.yml and dev/ci; make that drift fail here.

    The values live in dev/ci/local-ci-config.py, the single parser, which
    validates each one against a version shape. Importing it here is what turns
    a requoted `spark-full`, a reindented `with:` block or a renamed job into a
    preflight failure rather than a surprise the next time somebody needs the
    script. `bash -n` covers the shell, since actionlint runs with
    --shellcheck=off and looks only at workflow files.
    """
    if not LOCAL_CI.exists():
        return True
    failures = []

    syntax = subprocess.run(
        ["bash", "-n", str(LOCAL_CI)], capture_output=True, text=True, check=False
    )
    if syntax.returncode != 0:
        failures.append(f"{LOCAL_CI} is not valid bash: {syntax.stderr.strip()}")

    try:
        config = load_ci_module("local_ci_config", "local-ci-config.py").config()
        # Each default must name a job ci.yml actually defines, or the script
        # defaults to a version it then cannot look up.
        for suite in ("spark", "iceberg"):
            if not config[suite]:
                failures.append(f"local-ci-config.py found no {suite}_* jobs in ci.yml")
            elif config[f"{suite}_default"] not in config[suite]:
                failures.append(
                    f"local-ci-config.py defaults {suite} to "
                    f"{config[f'{suite}_default']}, which ci.yml has no job for"
                )
        if config["dedicated_gate_version"] not in config["spark"]:
            failures.append(
                "the DEDICATED_JVM_SBT_TESTS gate names Spark "
                f"{config['dedicated_gate_version']}, which ci.yml has no job for"
            )
        if not config["rows"]:
            failures.append("local-ci-config.py found no Spark SQL matrix rows")
    except Exception as err:  # noqa: BLE001 - any parse failure is the finding
        failures.append(f"dev/ci/local-ci-config.py could not read the CI config: {err}")

    for failure in failures:
        print(f"local-ci: {failure}")
    return not failures


def check_spark_sql_modules():
    """`--modules core` and `--modules hive` must partition `--modules all`.

    ci.yml maps its two Spark 4.1 POLICY outputs onto these three values, so a
    row that lands in no group, or in both, would either never run or run
    twice in the queue, and nothing else would notice.
    """
    spec = importlib.util.spec_from_file_location("spark_sql_modules", "dev/ci/spark-sql-modules.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    failures = []
    names = lambda rows: [row["name"] for row in rows]
    everything = names(module.select("all"))
    core, hive = names(module.select("core")), names(module.select("hive"))
    if not core or not hive:
        failures.append("a module group is empty (see MODULES in dev/ci/spark-sql-modules.py)")
    if sorted(core + hive) != sorted(everything):
        failures.append(
            f"core {core} + hive {hive} does not partition all {everything} "
            f"(see MODULES in dev/ci/spark-sql-modules.py)"
        )
    if len(set(everything)) != len(everything):
        failures.append(f"duplicate module names in {everything}")
    for failure in failures:
        print(f"spark sql modules: {failure}")
    return not failures


def check_linux_test_profiles():
    """`--profiles pr` and `--profiles nightly` must partition `--profiles all`.

    ci.yml maps `build_linux_full` and `build_linux_all_profiles` onto these
    three values. A profile in neither tier would never run anywhere; one in
    both would run twice on a labelled pull request. The `pr` tier also has
    to be the default build profile and nothing else, which is the reason the
    split exists. And the caller has to pass the input at all: its default is
    `all`, so a dropped `with:` line quietly puts every profile back on the
    pull request tier.
    """
    spec = importlib.util.spec_from_file_location("linux_test_profiles", "dev/ci/linux-test-profiles.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    failures = []
    names = lambda rows: [row["name"] for row in rows]
    everything = names(module.select("all"))
    pr, nightly = names(module.select("pr")), names(module.select("nightly"))
    if pr != ["Spark 4.1, JDK 17"]:
        failures.append(f"the pr tier must be the default build profile alone, got {pr}")
    if not nightly:
        failures.append("the nightly tier is empty (see PROFILES in dev/ci/linux-test-profiles.py)")
    if sorted(pr + nightly) != sorted(everything):
        failures.append(
            f"pr {pr} + nightly {nightly} does not partition all {everything} "
            f"(see PROFILES in dev/ci/linux-test-profiles.py)"
        )
    if len(set(everything)) != len(everything):
        failures.append(f"duplicate profile names in {everything}")
    for row in module.select("all"):
        if sorted(row) != ["java_version", "maven_opts", "name"]:
            failures.append(f"profile {row['name']!r} must carry exactly name, java_version and maven_opts")
    ci = (WORKFLOWS / "ci.yml").read_text(encoding="utf-8").splitlines()
    if not any(PROFILES_INPUT.match(line) for line in ci):
        failures.append(
            "ci.yml never passes `profiles:` to pr_build_linux.yml. The input "
            "defaults to all, so without it every pull request runs every profile again"
        )
    for failure in failures:
        print(f"linux test profiles: {failure}")
    return not failures


def check_change_filters():
    module = load_filters()
    failures = []
    for files, expected_true in ROUTING_CASES:
        for name, patterns in module.FILTERS.items():
            actual = module.matches(patterns, files)
            expected = name in expected_true
            if actual != expected:
                failures.append(
                    f"{files}: expected {name}={str(expected).lower()}, "
                    f"got {str(actual).lower()} (see FILTERS in dev/ci/compute-changes.py)"
                )
    for failure in failures:
        print(f"change filter: {failure}")
    return not failures


def check_event_policy():
    module = load_filters()
    failures = []
    for event, expected in POLICY_CASES:
        actual = {job for job in module.POLICY if module.event_allows(job, event)}
        if actual != expected:
            label = event.get("name")
            if event.get("action"):
                label += f"/{event['action']}"
            if event.get("label"):
                label += f" +{event['label']}"
            failures.append(
                f"{label} labels={event.get('labels', [])}: "
                f"unexpectedly allowed {sorted(actual - expected) or 'nothing'}, "
                f"unexpectedly blocked {sorted(expected - actual) or 'nothing'} "
                f"(see POLICY in dev/ci/compute-changes.py)"
            )
    missing = sorted(set(module.FILTERS) - set(module.POLICY))
    if missing:
        failures.append(
            f"jobs in FILTERS with no POLICY entry: {', '.join(missing)}; "
            f"they would never run on any event"
        )
    # "pr" next to a "label:" tier reads as "runs on every PR, and also when
    # labelled", but the label check wins and the "pr" is dead. Reject the
    # combination so it cannot be written by accident.
    for job, tiers in module.POLICY.items():
        if "pr" in tiers and module.gating_labels(job):
            failures.append(
                f"{job}: POLICY lists both 'pr' and a 'label:' tier. Those are "
                f"mutually exclusive; drop 'pr' if the job is opt-in, or drop "
                f"the label if it should run on every pull request"
            )
        # The nightly exists so the queue does not pay for these suites; a job
        # in both tiers would run twice a day for one verdict.
        if "queue" in tiers and "nightly" in tiers:
            failures.append(
                f"{job}: POLICY lists both 'queue' and 'nightly'. The queue "
                f"already tested the tree the nightly runs against, so pick one"
            )
    for failure in failures:
        print(f"event policy: {failure}")
    return not failures


def artifact_names(path):
    """Return ([upload names], [download names]) for one workflow file."""
    uploads, downloads = [], []
    lines = path.read_text(encoding="utf-8").splitlines()
    for index, line in enumerate(lines):
        if UPLOAD_USES.search(line):
            bucket = uploads
        elif DOWNLOAD_USES.search(line):
            bucket = downloads
        else:
            continue
        for following in lines[index + 1:]:
            if NEW_STEP.match(following):
                break  # step ended without a `name:`; download-all, or the default
            match = WITH_NAME.match(following)
            if match:
                bucket.append(match.group(1))
                break
    return uploads, downloads


def check_artifact_names():
    ci = (WORKFLOWS / "ci.yml").read_text(encoding="utf-8")
    call_counts = {}
    for called in re.findall(r"uses:\s*\./\.github/workflows/(\S+)", ci):
        call_counts[called] = call_counts.get(called, 0) + 1

    failures = []
    for path in sorted(WORKFLOWS.glob("*.y*ml")):
        uploads, downloads = artifact_names(path)
        if call_counts.get(path.name, 0) > 1:
            for name in uploads:
                if "inputs." not in name:
                    failures.append(
                        f"{path}: artifact '{name}' is uploaded by a workflow ci.yml calls "
                        f"{call_counts[path.name]} times; qualify the name with an input "
                        f"(e.g. ${{{{ inputs.spark-full }}}}) so the parallel producers stay distinct"
                    )
        for name in downloads:
            if name not in uploads:
                failures.append(
                    f"{path}: artifact '{name}' is downloaded but never uploaded in the same "
                    f"workflow; a producer rename probably missed its consumer"
                )
    for failure in failures:
        print(f"artifact name: {failure}")
    return not failures


def check_local_actions_have_checkout():
    """Every `uses: ./.github/actions/...` needs a checkout earlier in its job.

    A local action is loaded from the runner's workspace, not from the
    repository, so a job that has not checked out simply cannot find it. The
    failure is at step level and only on the jobs that skipped the checkout,
    which is easy to miss when those jobs are conditional.
    """
    failures = []
    for path in sorted(WORKFLOWS.glob("*.y*ml")):
        job = None
        checked_out = False
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.lstrip().startswith("#"):
                continue
            match = JOB_KEY.match(line)
            if match:
                job = match.group(1)
                checked_out = False
                continue
            if CHECKOUT_USES.search(line):
                checked_out = True
                continue
            match = LOCAL_ACTION_USES.search(line)
            if match and not checked_out:
                failures.append(
                    f"{path}: job `{job}` uses the local action {match.group(1)} "
                    f"with no preceding actions/checkout. Add the checkout, or "
                    f"call the underlying published action directly"
                )
    for failure in failures:
        print(f"local action: {failure}")
    return not failures


def ci_jobs_and_aggregator():
    """Return (all job ids in ci.yml, aggregator `needs:` ids, aggregator display name).

    Parsed line by line rather than with PyYAML, which is not installed on the
    preflight runner.
    """
    lines = (WORKFLOWS / "ci.yml").read_text(encoding="utf-8").splitlines()
    # GitHub job ids may contain letters, digits, `-` and `_`. Matching only
    # snake_case would let a `spark-4-2:` job escape the coverage check with no
    # error at all, which is the silent failure this checker exists to catch.
    job_key = re.compile(r"^  ([A-Za-z0-9_-]+):\s*$")
    needs_item = re.compile(r"^      - ([A-Za-z0-9_-]+)\s*$")
    name_key = re.compile(r"^    name:\s*(\S.*?)\s*$")

    jobs, needs, display_name = [], [], None
    in_jobs = False
    current = None
    in_needs_list = False
    for line in lines:
        if line.startswith("jobs:"):
            in_jobs = True
            continue
        if not in_jobs:
            continue
        match = job_key.match(line)
        if match:
            current = match.group(1)
            jobs.append(current)
            in_needs_list = False
            continue
        if current != AGGREGATOR_JOB:
            continue
        if line.strip() == "needs:":
            in_needs_list = True
            continue
        match = name_key.match(line)
        if match:
            display_name = match.group(1)
        match = needs_item.match(line)
        if in_needs_list and match:
            needs.append(match.group(1))
        elif in_needs_list and line.strip() and not line.startswith("      "):
            in_needs_list = False
    return jobs, needs, display_name


def asf_required_contexts():
    """Return the required_status_checks contexts .asf.yaml declares for main.

    Scoped to the `main:` entry under `protected_branches:`. Release branches
    (`branch-0.x`) have their own entries, and a context one of those requires
    says nothing about what `main` requires.
    """
    lines = ASF_YAML.read_text(encoding="utf-8").splitlines()
    branch_key = re.compile(r"^    ([^\s:#][^:]*):\s*$")
    context_item = re.compile(r'^\s+- "?(.+?)"?\s*$')
    contexts = []
    in_protected = False
    in_main = False
    collecting = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        if stripped == "protected_branches:":
            in_protected = True
            continue
        if not in_protected:
            continue
        # A new top-level or `github:`-level key ends the protected_branches map.
        if stripped and not line.startswith("    "):
            break
        match = branch_key.match(line)
        if match:
            in_main = match.group(1) == "main"
            collecting = False
            continue
        if not in_main:
            continue
        if stripped == "contexts:":
            collecting = True
            continue
        if not collecting:
            continue
        match = context_item.match(line)
        if match:
            contexts.append(match.group(1))
        else:
            collecting = False
    return contexts


def aggregator_check_names(raw_name):
    """Split the aggregator's `name:` into (required name, label-run name).

    Returns (None, None) when the value is not the expected expression, which
    includes a plain literal: a literal name is published on `labeled` runs too,
    and that is the failure mode the expression exists to prevent.
    """
    match = AGGREGATOR_NAME_EXPR.match(raw_name)
    if not match:
        return None, None
    return match.group(2), match.group(1)


def check_required_checks():
    jobs, needs, display_name = ci_jobs_and_aggregator()
    failures = []

    if AGGREGATOR_JOB not in jobs:
        failures.append(
            f"ci.yml has no `{AGGREGATOR_JOB}` job; it is the only context "
            f"`.asf.yaml` requires for main"
        )
    else:
        missing = [j for j in jobs if j not in AGGREGATOR_EXEMPT and j not in needs]
        if missing:
            failures.append(
                f"ci.yml jobs missing from `{AGGREGATOR_JOB}.needs`: "
                f"{', '.join(missing)}. A job outside the aggregator can fail "
                f"without blocking the merge queue; add it, or add it to "
                f"AGGREGATOR_EXEMPT here with a reason"
            )
        stale = [n for n in needs if n not in jobs]
        if stale:
            failures.append(
                f"`{AGGREGATOR_JOB}.needs` names jobs that no longer exist in "
                f"ci.yml: {', '.join(stale)}"
            )
        if display_name is None:
            failures.append(f"the `{AGGREGATOR_JOB}` job in ci.yml has no `name:`")
        else:
            required_name, label_run_name = aggregator_check_names(display_name)
            if required_name is None:
                failures.append(
                    f"`{AGGREGATOR_JOB}.name` is {display_name!r}; it must be the "
                    f"expression `${{{{ github.event.action == 'labeled' && "
                    f"'<label-run name>' || '<required name>' }}}}`. A label run "
                    f"skips the PR tier by design, and GitHub keeps only the most "
                    f"recent check run per name, so publishing the required name "
                    f"from a label run would overwrite the commit run's verdict"
                )
            elif label_run_name == required_name:
                failures.append(
                    f"`{AGGREGATOR_JOB}.name` publishes '{required_name}' on "
                    f"label runs too; the `labeled` branch of the expression must "
                    f"be a different name"
                )
            else:
                # An empty list means main does not require any status check
                # yet, which is a valid state: there is nothing to keep in sync.
                # Once a context is declared, it has to be the one this job
                # reports on commit runs, and never the label-run name.
                contexts = asf_required_contexts()
                if contexts and required_name not in contexts:
                    failures.append(
                        f"`{AGGREGATOR_JOB}` publishes the check name "
                        f"'{required_name}', but .asf.yaml requires {contexts} "
                        f"for main. A required context that never reports blocks every "
                        f"merge, including the one that would fix .asf.yaml"
                    )
                if label_run_name in contexts:
                    failures.append(
                        f".asf.yaml requires '{label_run_name}', which is the name "
                        f"`{AGGREGATOR_JOB}` publishes only on label runs. Those runs "
                        f"skip the PR tier, so requiring it would let a label mark an "
                        f"untested commit green"
                    )

    for failure in failures:
        print(f"required checks: {failure}")
    return not failures


def ci_triggers():
    """Return {event: its `types:` list, or None} from ci.yml's top-level `on:`."""
    lines = (WORKFLOWS / "ci.yml").read_text(encoding="utf-8").splitlines()
    event_key = re.compile(r"^  ([a-z_]+):\s*$")
    types_key = re.compile(r"^    types:\s*\[(.*)\]\s*$")
    triggers = {}
    in_on = False
    current = None
    for line in lines:
        if line.startswith("on:"):
            in_on = True
            continue
        if not in_on:
            continue
        if line and not line.startswith((" ", "#")):
            break
        match = event_key.match(line)
        if match:
            current = match.group(1)
            triggers[current] = None
            continue
        match = types_key.match(line)
        if match and current:
            triggers[current] = [t.strip() for t in match.group(1).split(",")]
    return triggers


def check_label_runs_separate():
    """`labeled` runs come from ci_label.yml, never from ci.yml's own trigger.

    When one workflow runs twice at the same commit, GitHub evaluates the pull
    request's required checks against only one of the two runs. A pull request
    opened with a label already applied fires `opened` and `labeled` together;
    if ci.yml took both, the label run could be the one GitHub picked, and it
    publishes `Required Checks (label run)`, so `Required Checks` showed as
    "Expected" forever and the merge queue never took the pull request (issue
    #6159). A separate calling workflow gets its own check suite.
    """
    failures = []
    triggers = ci_triggers()
    pull_request_types = triggers.get("pull_request") or []
    if "labeled" in pull_request_types:
        failures.append(
            "ci.yml triggers on `pull_request: labeled`. Two ci.yml runs at one "
            "commit let the label run hide `Required Checks` from the merge box; "
            "leave `labeled` to ci_label.yml"
        )
    if "workflow_call" not in triggers:
        failures.append(
            "ci.yml has no `workflow_call` trigger, so ci_label.yml cannot call it "
            "and applying a `run-*` label no longer runs anything"
        )

    label_workflow = WORKFLOWS / "ci_label.yml"
    if not label_workflow.exists():
        failures.append(f"{label_workflow} is missing; nothing runs on `labeled`")
    else:
        body = label_workflow.read_text(encoding="utf-8")
        if not re.search(r"^\s+uses:\s*\./\.github/workflows/ci\.yml\s*$", body, re.M):
            failures.append(f"{label_workflow} does not call ./.github/workflows/ci.yml")
        if not re.search(r"^\s+types:\s*\[\s*labeled\s*\]\s*$", body, re.M):
            failures.append(
                f"{label_workflow} must trigger on `pull_request: types: [labeled]` "
                f"and nothing else; any other type would run ci.yml twice per push"
            )

    for failure in failures:
        print(f"label runs: {failure}")
    return not failures


def check_cache_refresh_scope():
    """Every job in pr_build_linux.yml is either a cache writer or guarded.

    On push to main the merge queue has already tested the exact tree that
    landed, so the only thing left for that run to do is leave main's
    actions/cache entries warm -- a pull request can restore caches saved on
    its own branch or on main and nowhere else, and the queue's throwaway
    branch takes its own with it. ci.yml therefore calls the workflow with
    `cache-refresh-only` on push, and every job that is not a cache writer
    has to opt out with `if: ${{ !inputs.cache-refresh-only }}`.

    A job added without the guard runs on every push again. Nothing fails when
    that happens; the runner bill just quietly goes back up by up to ~500
    minutes a push, which is what this check exists to notice.
    """
    failures = []
    jobs, guarded = guarded_jobs(CACHE_REFRESH_WORKFLOW, CACHE_REFRESH_GUARD)

    for stale in sorted(set(CACHE_REFRESH_JOBS) - set(jobs)):
        failures.append(
            f"CACHE_REFRESH_JOBS names `{stale}`, which no longer exists in "
            f"{CACHE_REFRESH_WORKFLOW}; drop it here, or restore the job"
        )
    for name in jobs:
        if name in CACHE_REFRESH_JOBS and name in guarded:
            failures.append(
                f"{CACHE_REFRESH_WORKFLOW}: job `{name}` is listed in "
                f"CACHE_REFRESH_JOBS ({CACHE_REFRESH_JOBS[name]}) but carries "
                f"the cache-refresh-only guard, so it is skipped on push and "
                f"the cache it owns goes stale on main"
            )
        if name not in CACHE_REFRESH_JOBS and name not in guarded:
            failures.append(
                f"{CACHE_REFRESH_WORKFLOW}: job `{name}` has no "
                f"`if: ${{{{ !inputs.cache-refresh-only }}}}`, so it runs on "
                f"every push to main where the merge queue has already tested "
                f"the same tree. Add the guard, or add the job to "
                f"CACHE_REFRESH_JOBS with the cache entry it writes"
            )

    # The guards above do nothing unless the caller actually sets the input;
    # its default is false, so a dropped `with:` block silently restores the
    # full pipeline on push.
    ci = (WORKFLOWS / "ci.yml").read_text(encoding="utf-8").splitlines()
    if not any(CACHE_REFRESH_INPUT.match(line) for line in ci):
        failures.append(
            "ci.yml never passes `cache-refresh-only:` to pr_build_linux.yml. "
            "The input defaults to false, so without it every push to main runs "
            "the full pipeline again"
        )

    for failure in failures:
        print(f"cache refresh scope: {failure}")
    return not failures


def check_nightly_scope():
    """With `profiles: nightly`, pr_build_linux.yml runs the test matrix alone.

    That input value means the default-profile pipeline already ran at this
    commit: in the queue and the push run for the nightly, in the PR tier for
    a `run-all-spark-profiles` label run. The lints, the Rust tests, the
    Spark build and the TPC-H/TPC-DS runs would repeat a verdict, so every
    job other than the three the matrix needs carries
    `if: ${{ inputs.profiles != 'nightly' }}`. Silent when broken, like the
    cache-refresh guard: the nightly just costs 100-odd runner-minutes more.
    """
    failures = []
    jobs, guarded = guarded_jobs(CACHE_REFRESH_WORKFLOW, NIGHTLY_GUARD)
    for stale in sorted(set(NIGHTLY_JOBS) - set(jobs)):
        failures.append(
            f"NIGHTLY_JOBS names `{stale}`, which no longer exists in "
            f"{CACHE_REFRESH_WORKFLOW}; drop it here, or restore the job"
        )
    for name in jobs:
        if name in NIGHTLY_JOBS and name in guarded:
            failures.append(
                f"{CACHE_REFRESH_WORKFLOW}: job `{name}` is listed in "
                f"NIGHTLY_JOBS ({NIGHTLY_JOBS[name]}) but carries the "
                f"`profiles != 'nightly'` guard, so the nightly test matrix "
                f"cannot run"
            )
        if name not in NIGHTLY_JOBS and name not in guarded:
            failures.append(
                f"{CACHE_REFRESH_WORKFLOW}: job `{name}` has no "
                f"`if: ${{{{ inputs.profiles != 'nightly' }}}}`, so it repeats "
                f"on every nightly a verdict the queue already produced. Add "
                f"the guard, or add the job to NIGHTLY_JOBS with the reason "
                f"the test matrix needs it"
            )
    for failure in failures:
        print(f"nightly scope: {failure}")
    return not failures


def schedule_branch():
    """The body of the `schedule` case in ci.yml's `Detect changes` script.

    Ends at the next branch keyword indented the same as the `elif` itself, so
    the `if`/`else` nested inside the case stays part of the body.
    """
    body, indent = [], None
    for line in CI_WORKFLOW.read_text(encoding="utf-8").splitlines():
        if indent is None:
            match = SCHEDULE_BRANCH.match(line)
            if match:
                indent = match.group(1)
            continue
        if re.match(rf"^{indent}(elif|else|fi)\b", line):
            break
        body.append(line)
    return body


def check_nightly_base_fallback():
    """The nightly must run everything when it has no base to diff against.

    `dev/ci/nightly-base.py` returns the commit the last successful scheduled
    run tested, and the nightly diffs `main` against it. When it returns
    nothing -- the first nightly, an Actions API error, or a base that has
    left `main` -- the only safe base is no base at all: list the whole tree
    and let POLICY narrow it to the nightly tier.

    Guessing a narrower base is silently destructive, which is why it is
    pinned here. Say the last green nightly was three nights ago, a source
    change landed two nights ago and no nightly has covered it, and only docs
    have landed since. A one-day window starts after that source change, so
    its suites are skipped, the run goes green, and `nightly-base.py` then
    hands that green head out as tomorrow's base. The coverage is gone, the
    run that dropped it was green, and nothing says so.
    """
    failures = []
    body = schedule_branch()
    if not body:
        failures.append(
            f"{CI_WORKFLOW}: no `schedule` case in the `Detect changes` script "
            f"(SCHEDULE_BRANCH no longer matches); the nightly is unrouted"
        )
    code = [line for line in body if not line.lstrip().startswith("#")]
    text = "\n".join(code)
    if "dev/ci/nightly-base.py" not in text:
        failures.append(
            f"{CI_WORKFLOW}: the `schedule` case does not call "
            f"dev/ci/nightly-base.py, so the nightly is not based on the last "
            f"successful scheduled run and commits can be covered twice or not "
            f"at all"
        )
    if "git ls-tree -r --name-only HEAD" not in text:
        failures.append(
            f"{CI_WORKFLOW}: the `schedule` case has no "
            f"`git ls-tree -r --name-only HEAD` fallback, so a nightly with no "
            f"base does not run the whole tier"
        )
    for line in code:
        if DATE_BASED_BASE.search(line):
            failures.append(
                f"{CI_WORKFLOW}: the `schedule` case derives a base from the "
                f"clock (`{line.strip()}`). A window that starts after a commit "
                f"no nightly has covered yet skips that commit's suites, goes "
                f"green, and becomes tomorrow's base. Fall back to the whole "
                f"tree instead"
            )
    for failure in failures:
        print(f"nightly base: {failure}")
    return not failures


def _cache_steps(lines):
    """Yield (line_no, uses_line, step_lines) for every actions/cache* step.

    A step runs from the `- ` that opens it to the next line indented no
    further, which is enough structure to read its `if:`, `key:` and `path:`
    without a YAML parser (no other check here takes that dependency either).
    """
    for index, line in enumerate(lines):
        if not (CACHE_RW_USES.match(line) or CACHE_SAVE_USES.match(line)):
            continue
        start = index
        while start > 0 and not lines[start].lstrip().startswith("- "):
            start -= 1
        indent = len(lines[start]) - len(lines[start].lstrip())
        end = index + 1
        while end < len(lines):
            stripped = lines[end].strip()
            if stripped and not stripped.startswith("#"):
                if len(lines[end]) - len(lines[end].lstrip()) <= indent:
                    break
            end += 1
        yield start + 1, line, lines[start:end]


def check_cache_save_scope():
    """A multi-gigabyte cache is written only on push to main.

    Caches are scoped to the ref that wrote them: a pull request reads its own
    ref and main, and a `gh-readonly-queue/*` branch takes its entries to the
    grave when the queue deletes it. An entry written from a queue or
    pull-request ref can therefore never be restored by a later run, while
    still counting against the repository's shared budget and pushing main's
    entries out of it under least-recently-used eviction.

    That is not hypothetical. On 2026-09-15 the repository held 12.27 GiB
    across 14 entries, of which 9.22 GiB sat on one `gh-readonly-queue/*`
    branch and 3.01 GiB on `refs/pull/*/merge`. Nothing at all was on main, so
    every `cargo build --profile ci` in the merge queue missed its cache and
    paid a cold ~26 minute compile where a hit costs 2m21s -- eight times over
    in a single run, because each Spark and Iceberg caller builds its own copy.

    The cargo caches already carry `if: github.ref == 'refs/heads/main'` on
    their save. This holds every other large cache to the same rule, and
    rejects the bare `actions/cache@vN` form for them outright, since its save
    runs in an implicit post step that no `if:` can reach.
    """
    failures = []
    sources = sorted(WORKFLOWS.glob("*.yml")) + sorted(WORKFLOWS.glob("*.yaml"))
    sources += sorted(Path(".github/actions").glob("*/action.yaml"))
    sources += sorted(Path(".github/actions").glob("*/action.yml"))

    for source in sources:
        lines = source.read_text(encoding="utf-8").splitlines()
        for line_no, uses, step in _cache_steps(lines):
            body = "\n".join(step)
            if not any(path in body for path in LARGE_CACHE_PATHS):
                continue
            key = ""
            for entry in step:
                if entry.strip().startswith("key:"):
                    key = entry.split("key:", 1)[1].strip()
                    break
            if any(
                source.name == name and key.startswith(prefix)
                for name, prefix in CACHE_SAVE_SCOPE_EXEMPT
            ):
                continue
            where = f"{source}:{line_no}"
            if CACHE_RW_USES.match(uses):
                failures.append(
                    f"{where}: `{key}` caches a large path with "
                    f"`actions/cache@vN`, whose save runs in an implicit post "
                    f"step that no `if:` can reach. Split it into "
                    f"`actions/cache/restore` plus an `actions/cache/save` "
                    f"carrying `if: github.ref == 'refs/heads/main'`, or exempt "
                    f"the key in CACHE_SAVE_SCOPE_EXEMPT with the reason"
                )
            elif not CACHE_MAIN_GUARD.search(body):
                failures.append(
                    f"{where}: `{key}` saves a large path without "
                    f"`if: github.ref == 'refs/heads/main'`, so a pull request "
                    f"or merge-queue run writes an entry no later run can "
                    f"restore, evicting main's from the shared budget"
                )

    for failure in failures:
        print(f"cache save scope: {failure}")
    return not failures


if __name__ == "__main__":
    ok = check_change_filters()
    ok = check_event_policy() and ok
    ok = check_spark_sql_modules() and ok
    ok = check_linux_test_profiles() and ok
    ok = check_artifact_names() and ok
    ok = check_local_actions_have_checkout() and ok
    ok = check_required_checks() and ok
    ok = check_label_runs_separate() and ok
    ok = check_cache_refresh_scope() and ok
    ok = check_nightly_scope() and ok
    ok = check_nightly_base_fallback() and ok
    ok = check_cache_save_scope() and ok
    ok = check_local_ci_config() and ok
    if not ok:
        sys.exit(1)
    print("CI config checks passed")
