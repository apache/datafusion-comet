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

# Replacement for dorny/paths-filter, which is not on the apache org allow
# list. Reads a list of changed files (one per line) and emits per-job
# "<name>=true|false" lines suitable for $GITHUB_OUTPUT.
#
# Each output folds together two independent questions:
#
#   1. Did the change touch files this job covers?  FILTERS, below. Pattern
#      semantics match dorny/picomatch: "**" spans path segments, "*" stays
#      within a segment, and a leading "!" marks an exclude pattern.
#   2. Does this event permit the job to run at all?  POLICY, below.
#
# Question 2 used to live in ci.yml as a four-line `${{ }}` expression
# repeated on every heavy job. Keeping it here instead means the whole
# routing policy is in one place, is readable without evaluating GitHub
# expression syntax in your head, and is covered by the cases in
# dev/ci/check-ci-config.py, which YAML expressions never could be.

import json
import os
import re
import sys
from pathlib import Path

# Shared cache recipes affect every native producer. Their tests run in
# Preflight and retain the existing dev/ci/** routes, without adding consumers.
NATIVE_CACHE_RECIPES = (
    ".github/actions/build-native-ci/**",
    "dev/ci/native-cache-key.py", "dev/ci/compute-changes.py",
)

# Cargo validates optional contrib manifests against native/Cargo.lock even
# with their features disabled. Their Rust sources and standalone lockfiles
# do not enter the default CI/debug builds.
NATIVE_BUILD_INPUTS = (
    "native/**", "contrib/*/native/Cargo.toml", ".cargo/**",
    ".github/actions/setup-builder/**", *NATIVE_CACHE_RECIPES,
    "rust-toolchain", "rust-toolchain.toml", "!**.md",
)
NATIVE_LIBRARY_INPUTS = (*NATIVE_BUILD_INPUTS, "!**/benches/**")

FILTERS = {
    "build_linux": [
        "native/**",
        "common/**",
        "spark/**",
        "spark-integration/**",
        "pom.xml",
        "**/pom.xml",
        ".mvn/**",
        "mvnw",
        "Makefile",
        "rust-toolchain.toml",
        "dev/ci/**",
        ".github/workflows/ci.yml",
        ".github/workflows/pr_build_linux.yml",
        ".github/actions/setup-builder/**",
        ".github/actions/build-native-ci/**",
        ".github/actions/java-test/**",
        ".github/actions/maven-bootstrap/**",
        ".github/actions/rust-test/**",
        ".github/actions/upload-artifact-retry/**",
        ".github/actions/download-artifact-retry/**",
        "!**.md",
        "!native/core/benches/**",
        "!native/spark-expr/benches/**",
        "!spark/src/test/scala/org/apache/spark/sql/benchmark/**",
        "!spark/src/main/scala/org/apache/comet/GenerateDocs.scala",
    ],
    # Same inputs as build_linux: not a separate job but a second POLICY
    # decision for the same call, selecting the full pipeline rather than the
    # cache-populating subset. ci.yml folds it into the reusable workflow's
    # `cache-refresh-only` input. Populated below, after the dict, so the two
    # lists cannot drift.
    "build_linux_full": [],
    # A third POLICY decision on the same inputs: whether the linux-test matrix
    # runs every Spark profile or only the PR-tier one. ci.yml folds it into
    # the reusable workflow's `profiles` input. Populated below as well.
    "build_linux_all_profiles": [],
    "build_macos": [
        "native/**",
        "common/**",
        "spark/**",
        "spark-integration/**",
        "pom.xml",
        "**/pom.xml",
        ".mvn/**",
        "mvnw",
        "Makefile",
        "rust-toolchain.toml",
        "dev/ci/**",
        ".github/workflows/ci.yml",
        ".github/workflows/pr_build_macos.yml",
        ".github/actions/setup-macos-builder/**",
        ".github/actions/java-test/**",
        ".github/actions/upload-artifact-retry/**",
        ".github/actions/download-artifact-retry/**",
        "!**.md",
        "!native/core/benches/**",
        "!native/spark-expr/benches/**",
        "!spark/src/test/scala/org/apache/spark/sql/benchmark/**",
        "!spark/src/main/scala/org/apache/comet/GenerateDocs.scala",
    ],
    "benchmark": [
        "native/core/benches/**",
        "native/spark-expr/benches/**",
        "spark/src/test/scala/org/apache/spark/sql/benchmark/**",
    ],
    # dev/verify-contrib-delta-gate.sh proves the default cargo, Maven and
    # libcomet builds carry no Delta surface and that the gated build does.
    # It reads the cargo tree, the effective pom, the compiled classes and the
    # dylib symbol table: main sources and build inputs, never tests.
    "delta_gate": [
        "native/**",
        "common/src/main/**",
        "spark/src/main/**",
        "contrib/delta/**",
        "pom.xml",
        "**/pom.xml",
        ".mvn/**",
        "mvnw",
        "Makefile",
        "rust-toolchain.toml",
        "dev/verify-contrib-delta-gate.sh",
        ".github/workflows/ci.yml",
        ".github/workflows/delta_build_gate.yml",
        ".github/actions/setup-builder/**",
        "!**.md",
        "!native/core/benches/**",
        "!native/spark-expr/benches/**",
        "!spark/src/main/scala/org/apache/comet/GenerateDocs.scala",
    ],
    # A real Python worker against each Spark 4.x Arrow runner. The list is
    # deliberately narrow: the suite builds Comet three times, once per Spark
    # version, and only the map-in-batch wiring can change its verdict.
    "pyarrow_udf": [
        "pom.xml",
        "common/pom.xml",
        "native/shuffle/src/spark_unsafe/row.rs",
        "spark/pom.xml",
        "spark/src/main/java/org/apache/comet/vector/**",
        "spark/src/main/java/org/apache/spark/sql/comet/execution/shuffle/SpillWriter.java",
        "spark/src/main/scala/org/apache/comet/CometConf.scala",
        "spark/src/main/scala/org/apache/comet/rules/EliminateRedundantTransitions.scala",
        "spark/src/main/scala/org/apache/comet/vector/**",
        "spark/src/main/scala/org/apache/spark/sql/comet/CometMapInBatchExec.scala",
        "spark/src/main/scala/org/apache/spark/sql/comet/shims/MapInBatchInfo.scala",
        "spark/src/main/spark-3.4/org/apache/spark/sql/comet/shims/ShimCometMapInBatch.scala",
        "spark/src/main/spark-3.5/org/apache/spark/sql/comet/shims/ShimCometMapInBatch.scala",
        "spark/src/main/spark-4.0/org/apache/spark/sql/comet/shims/ShimCometMapInBatch.scala",
        "spark/src/main/spark-4.0/org/apache/spark/sql/execution/python/CometArrowPythonRunner.scala",
        "spark/src/main/spark-4.1/org/apache/spark/sql/comet/shims/ShimCometMapInBatch.scala",
        "spark/src/main/spark-4.1/org/apache/spark/sql/execution/python/CometArrowPythonRunner.scala",
        "spark/src/main/spark-4.2/org/apache/spark/sql/comet/shims/ShimCometMapInBatch.scala",
        "spark/src/main/spark-4.2/org/apache/spark/sql/execution/python/CometArrowPythonRunner.scala",
        "spark/src/main/spark-4.x/org/apache/spark/sql/comet/shims/Spark4xMapInBatchSupport.scala",
        "spark/src/main/spark-4.x/org/apache/spark/sql/execution/python/CometArrowPythonRunnerBase.scala",
        "spark/src/test/resources/pyspark/conftest.py",
        "spark/src/test/resources/pyspark/test_pyarrow_udf.py",
        "spark/src/test/resources/pyspark/test_pyarrow_udf_dictionary_shuffle.py",
        "spark/src/test/spark-3.5/org/apache/spark/sql/comet/CometMapInBatchSuite.scala",
        "spark/src/test/spark-4.x/org/apache/spark/sql/comet/CometMapInBatchSuite.scala",
        "spark/src/test/spark-4.x/org/apache/spark/sql/execution/python/CometArrowPythonRunnerSuite.scala",
        ".mvn/**",
        "mvnw",
        ".github/workflows/ci.yml",
        ".github/workflows/pyarrow_udf_test.yml",
        ".github/actions/setup-builder/**",
    ],
    "docs": [
        ".asf.yaml",
        ".github/workflows/docs.yaml",
        "docs/**",
        # Generated docs (configs.md, per-version expression compatibility pages) are
        # built from these Scala sources by GenerateDocs, so changes to them must
        # republish the site even when no docs/ file is touched.
        "spark/src/main/scala/org/apache/comet/CometConf.scala",
        "spark/src/main/scala/org/apache/comet/GenerateDocs.scala",
        "spark/src/main/scala/org/apache/comet/serde/**",
        "spark/src/main/scala/org/apache/comet/expressions/**",
        "spark/src/main/spark-*/**",
    ],
    "spark_3_4": [
        "native/**/src/**",
        "native/**/Cargo.toml",
        "native/Cargo.lock",
        "common/src/main/**",
        "common/pom.xml",
        "spark/src/main/**",
        "!spark/src/main/spark-3.5/**",
        "!spark/src/main/spark-4.0/**",
        "!spark/src/main/spark-4.1/**",
        "!spark/src/main/spark-4.2/**",
        "!spark/src/main/spark-4.x/**",
        "!spark/src/main/scala/org/apache/comet/GenerateDocs.scala",
        "spark/pom.xml",
        "dev/diffs/3.4.3.diff",
        "pom.xml",
        "rust-toolchain.toml",
        ".github/workflows/ci.yml",
        ".github/workflows/spark_sql_test_reusable.yml",
        "dev/ci/spark-sql-modules.py",
        ".github/actions/setup-builder/**",
        ".github/actions/setup-spark-builder/**",
        ".github/actions/upload-artifact-retry/**",
        ".github/actions/download-artifact-retry/**",
        ".mvn/**",
        "mvnw",
    ],
    "spark_3_5": [
        "native/**/src/**",
        "native/**/Cargo.toml",
        "native/Cargo.lock",
        "common/src/main/**",
        "common/pom.xml",
        "spark/src/main/**",
        "!spark/src/main/spark-3.4/**",
        "!spark/src/main/spark-4.0/**",
        "!spark/src/main/spark-4.1/**",
        "!spark/src/main/spark-4.2/**",
        "!spark/src/main/spark-4.x/**",
        "!spark/src/main/scala/org/apache/comet/GenerateDocs.scala",
        "spark/pom.xml",
        "dev/diffs/3.5.9.diff",
        "pom.xml",
        "rust-toolchain.toml",
        ".github/workflows/ci.yml",
        ".github/workflows/spark_sql_test_reusable.yml",
        "dev/ci/spark-sql-modules.py",
        ".github/actions/setup-builder/**",
        ".github/actions/setup-spark-builder/**",
        ".github/actions/upload-artifact-retry/**",
        ".github/actions/download-artifact-retry/**",
        ".mvn/**",
        "mvnw",
    ],
    "spark_4_0": [
        "native/**/src/**",
        "native/**/Cargo.toml",
        "native/Cargo.lock",
        "common/src/main/**",
        "common/pom.xml",
        "spark/src/main/**",
        "!spark/src/main/spark-3.4/**",
        "!spark/src/main/spark-3.5/**",
        "!spark/src/main/spark-3.x/**",
        "!spark/src/main/spark-4.1/**",
        "!spark/src/main/spark-4.2/**",
        "!spark/src/main/scala/org/apache/comet/GenerateDocs.scala",
        "spark/pom.xml",
        "dev/diffs/4.0.4.diff",
        "pom.xml",
        "rust-toolchain.toml",
        ".github/workflows/ci.yml",
        ".github/workflows/spark_sql_test_reusable.yml",
        "dev/ci/spark-sql-modules.py",
        ".github/actions/setup-builder/**",
        ".github/actions/setup-spark-builder/**",
        ".github/actions/upload-artifact-retry/**",
        ".github/actions/download-artifact-retry/**",
        ".mvn/**",
        "mvnw",
    ],
    "spark_4_1": [
        "native/**/src/**",
        "native/**/Cargo.toml",
        "native/Cargo.lock",
        "common/src/main/**",
        "common/pom.xml",
        "spark/src/main/**",
        "!spark/src/main/spark-3.4/**",
        "!spark/src/main/spark-3.5/**",
        "!spark/src/main/spark-3.x/**",
        "!spark/src/main/spark-4.0/**",
        "!spark/src/main/spark-4.2/**",
        "!spark/src/main/scala/org/apache/comet/GenerateDocs.scala",
        "spark/pom.xml",
        "dev/diffs/4.1.3.diff",
        "pom.xml",
        "rust-toolchain.toml",
        ".github/workflows/ci.yml",
        ".github/workflows/spark_sql_test_reusable.yml",
        "dev/ci/spark-sql-modules.py",
        ".github/actions/setup-builder/**",
        ".github/actions/setup-spark-builder/**",
        ".github/actions/upload-artifact-retry/**",
        ".github/actions/download-artifact-retry/**",
        ".mvn/**",
        "mvnw",
    ],
    # Same inputs as spark_4_1: this is not a separate job but a second
    # POLICY decision for the same call, selecting the sql_hive matrix rows.
    # ci.yml folds the two outputs into the reusable workflow's `modules`
    # input. Populated below, after the dict, so the two lists cannot drift.
    "spark_4_1_hive": [],
    "iceberg_1_8": [
        "native/**/src/**",
        "native/**/Cargo.toml",
        "native/Cargo.lock",
        "common/src/main/**",
        "common/pom.xml",
        "spark/src/main/**",
        "!spark/src/main/scala/org/apache/comet/GenerateDocs.scala",
        "spark/pom.xml",
        "dev/diffs/iceberg/**",
        "pom.xml",
        "rust-toolchain.toml",
        ".github/workflows/ci.yml",
        ".github/workflows/iceberg_spark_test_reusable.yml",
        ".github/actions/setup-builder/**",
        ".github/actions/setup-iceberg-builder/**",
        "dev/ci/iceberg-test-shards.gradle",
        "dev/ci/check-iceberg-shards.py",
        "dev/ci/test-iceberg-shards.py",
        ".github/actions/upload-artifact-retry/**",
        ".github/actions/download-artifact-retry/**",
        ".mvn/**",
        "mvnw",
    ],
    "iceberg_1_9": [
        "native/**/src/**",
        "native/**/Cargo.toml",
        "native/Cargo.lock",
        "common/src/main/**",
        "common/pom.xml",
        "spark/src/main/**",
        "!spark/src/main/scala/org/apache/comet/GenerateDocs.scala",
        "spark/pom.xml",
        "dev/diffs/iceberg/**",
        "pom.xml",
        "rust-toolchain.toml",
        ".github/workflows/ci.yml",
        ".github/workflows/iceberg_spark_test_reusable.yml",
        ".github/actions/setup-builder/**",
        ".github/actions/setup-iceberg-builder/**",
        "dev/ci/iceberg-test-shards.gradle",
        "dev/ci/check-iceberg-shards.py",
        "dev/ci/test-iceberg-shards.py",
        ".github/actions/upload-artifact-retry/**",
        ".github/actions/download-artifact-retry/**",
        ".mvn/**",
        "mvnw",
    ],
    "iceberg_1_10": [
        "native/**/src/**",
        "native/**/Cargo.toml",
        "native/Cargo.lock",
        "common/src/main/**",
        "common/pom.xml",
        "spark/src/main/**",
        "!spark/src/main/scala/org/apache/comet/GenerateDocs.scala",
        "spark/pom.xml",
        "dev/diffs/iceberg/**",
        "pom.xml",
        "rust-toolchain.toml",
        ".github/workflows/ci.yml",
        ".github/workflows/iceberg_spark_test_reusable.yml",
        ".github/actions/setup-builder/**",
        ".github/actions/setup-iceberg-builder/**",
        "dev/ci/iceberg-test-shards.gradle",
        "dev/ci/check-iceberg-shards.py",
        "dev/ci/test-iceberg-shards.py",
        ".github/actions/upload-artifact-retry/**",
        ".github/actions/download-artifact-retry/**",
        ".mvn/**",
        "mvnw",
    ],
    "iceberg_1_11": [
        "native/**/src/**",
        "native/**/Cargo.toml",
        "native/Cargo.lock",
        "common/src/main/**",
        "common/pom.xml",
        "spark/src/main/**",
        "!spark/src/main/scala/org/apache/comet/GenerateDocs.scala",
        "spark/pom.xml",
        "dev/diffs/iceberg/**",
        "pom.xml",
        "rust-toolchain.toml",
        ".github/workflows/ci.yml",
        ".github/workflows/iceberg_spark_test_reusable.yml",
        ".github/actions/setup-builder/**",
        ".github/actions/setup-iceberg-builder/**",
        "dev/ci/iceberg-test-shards.gradle",
        "dev/ci/check-iceberg-shards.py",
        "dev/ci/test-iceberg-shards.py",
        ".github/actions/upload-artifact-retry/**",
        ".github/actions/download-artifact-retry/**",
        ".mvn/**",
        "mvnw",
    ],
}
# Spark and Iceberg producers share these recipes. Linux routes the action
# above and already covers the Python helpers through dev/ci/**.
for _native_consumer in (
    "spark_3_4", "spark_3_5", "spark_4_0", "spark_4_1",
    "iceberg_1_8", "iceberg_1_9", "iceberg_1_10", "iceberg_1_11",
):
    FILTERS[_native_consumer].extend(NATIVE_CACHE_RECIPES)

FILTERS["spark_4_1_hive"] = FILTERS["spark_4_1"]
FILTERS["build_linux_full"] = FILTERS["build_linux"]
FILTERS["build_linux_all_profiles"] = FILTERS["build_linux"]

# Which events may run each job, independent of the path filters above.
#
#   "pr"              every pull request
#   "queue"           the merge queue, i.e. a merge_group event
#   "nightly"         the scheduled run against main, once a day
#   "push"            push to main
#   "label:<name>"    a pull request carrying that label
#
# workflow_dispatch always runs everything, so it is not listed. "pr" and
# "label:" are mutually exclusive -- a job is either unconditional on pull
# requests or opt-in, never both -- and check-ci-config.py rejects a job that
# lists both rather than letting the label quietly win.
#
# The merge queue is the authoritative gate: it tests the merge result rather
# than the PR head, and every "queue" job has to pass before a change lands.
# "nightly" is for the suites that catch a regression on a Spark or Iceberg
# version other than the default one: about 870 of the 1,900 runner-minutes a
# queue run cost in September 2026, and the most common reason a queue run
# went red on a good tree (issue #5870). A regression there is real but rare,
# and a day's delay in seeing it costs less than running the suites on every
# merge. The scheduled run diffs main against the commit the last successful
# scheduled run tested and routes through FILTERS like any other event. A job
# is "queue" or "nightly", never both; check-ci-config.py enforces that.
#
# "push" is reserved for work that can only happen once a commit is on main.
# Adding "push" back to a test job would make every merge run it twice, once
# in the queue and once after, which is the thing the queue was adopted to
# avoid.
POLICY = {
    # The one test job that also runs on push to main, and only because of
    # actions/cache scoping: a pull request can restore caches saved on its
    # own branch or on main, and nowhere else. The queue runs on a throwaway
    # gh-readonly-queue/* branch, so whatever it saves is deleted with that
    # branch. Without a push run, a Cargo.lock or pom.xml change would leave
    # main's cargo-ci, cargo-debug, Maven and TPC-H/TPC-DS caches stale
    # forever, and every later pull request would pay the delta on top of the
    # restore-keys prefix match.
    #
    # On push that is the *only* thing it is for. The queue already tested the
    # exact tree that landed, so re-running the lints and the linux-test
    # matrix there tests nothing, and they are 514 of the 587 runner-minutes a
    # push run costs. The split below keeps the cache writers on push and moves
    # everything else behind `build_linux_full`.
    "build_linux": ["pr", "queue", "push"],
    # The lints and the test matrix inside pr_build_linux.yml. Deliberately no
    # "push": ci.yml turns this output into the workflow's `cache-refresh-only`
    # input, so dropping "push" here is what trims the push tier down to the
    # jobs that write an actions/cache entry. See issue #5929.
    "build_linux_full": ["pr", "queue"],
    # The linux-test matrix's Spark profiles other than the default one. The
    # five profiles cost about the same each, roughly 2,300 runner-minutes a
    # day apiece on pull requests in mid-September 2026, and together they
    # were three quarters of the Linux build. A pull request and the queue run
    # the Comet test suites against Spark 4.1 only; the nightly run covers the
    # other four. The lint-java matrix still compiles Spark 3.4/3.5/4.0 on
    # every pull request, so what waits for the nightly is runtime behaviour,
    # not a shim that fails to build. ci.yml turns this output into the
    # workflow's `profiles` input.
    "build_linux_all_profiles": ["nightly", "label:run-all-spark-profiles"],
    # macOS runners are the scarcest capacity we have, and the Linux build
    # already covers rustfmt and the Rust/JVM compile on every PR. The label
    # is for a change that touches platform-specific code.
    "build_macos": ["queue", "label:run-macos-tests"],
    # Benchmark sources are compiled and linted, never run, so a break there
    # cannot affect a PR's correctness verdict; the queue catches it.
    "benchmark": ["queue", "label:run-benchmark-check"],
    # The Delta build gate only proves a build-system property, and the
    # PyArrow suite builds Comet once per Spark 4.x version to drive a real
    # Python worker. Neither changes often enough to earn a PR-tier slot; the
    # queue catches a regression before it lands, and the label is the escape
    # hatch for a change to the surface they cover.
    "delta_gate": ["queue", "label:run-delta-build-gate"],
    "pyarrow_udf": ["queue", "label:run-pyarrow-udf-tests"],
    # docs deploys to asf-site, so it must not run from a pull request or from
    # the queue's throwaway branch.
    "docs": ["push"],
    # Spark 3.4 is deprecated, so it is the one test job outside the queue
    # tier: a failure there no longer blocks a merge. It stays runnable on
    # demand -- the label on a pull request, or a workflow_dispatch -- so
    # anyone who wants to check a change against 3.4 still can.
    "spark_3_4": ["label:run-spark-3.4-tests"],
    # Spark 4.1 is the default build profile and the one Spark SQL suite the
    # queue runs; 3.5 and 4.0 run nightly, or on a pull request with their
    # label.
    "spark_3_5": ["nightly", "label:run-spark-3.5-tests"],
    "spark_4_0": ["nightly", "label:run-spark-4.0-tests"],
    # No Spark SQL suite runs on a plain pull request. Spark 4.1 was the last
    # one in the PR tier, first whole (issue #5870 pulled the sql_hive shards
    # out) and then catalyst and sql_core alone. What changed is how often a
    # pull request is pushed: with agent-driven review and agent-driven
    # replies to review, a PR now goes through several more rounds before it
    # is queued, and each round paid for the whole 4.1 build. The queue still
    # runs every shard before anything lands; the two labels bring the run
    # forward. `run-spark-4.1-tests` selects the whole suite, so it appears on
    # both outputs; `run-spark-4.1-hive-tests` selects only the hive shards.
    "spark_4_1": ["queue", "label:run-spark-4.1-tests"],
    "spark_4_1_hive": [
        "queue",
        "label:run-spark-4.1-tests",
        "label:run-spark-4.1-hive-tests",
    ],
    # Same shape for Iceberg: 1.11 is the only Spark 4.1 coverage, so it is
    # the one Iceberg version the queue runs; the three older versions run
    # nightly. One label opts a pull request into all four.
    "iceberg_1_8": ["nightly", "label:run-iceberg-tests"],
    "iceberg_1_9": ["nightly", "label:run-iceberg-tests"],
    "iceberg_1_10": ["nightly", "label:run-iceberg-tests"],
    "iceberg_1_11": ["queue", "label:run-iceberg-tests"],
}


def gating_labels(job):
    return [t[len("label:"):] for t in POLICY[job] if t.startswith("label:")]


def event_allows(job, event):
    """Does `event` permit `job` to run, ignoring which files changed?

    `event` is {"name", "action", "label", "labels"}: the workflow event name,
    the pull_request action, the label just added on a `labeled` event, and the
    labels currently on the pull request.
    """
    tiers = POLICY[job]
    name = event.get("name")

    if name == "workflow_dispatch":
        return True
    if name == "push":
        return "push" in tiers
    if name == "merge_group":
        return "queue" in tiers
    if name == "schedule":
        return "nightly" in tiers
    if name != "pull_request":
        return False

    gates = gating_labels(job)
    if gates:
        if not any(label in event.get("labels", []) for label in gates):
            return False
    elif "pr" not in tiers:
        return False

    # A `labeled` event fires at the same commit as the opened/synchronize run
    # that already tested it, and GitHub cannot filter a pull_request trigger
    # by label name. So on `labeled`, run only the job the new label gates;
    # everything else would be duplicating a pipeline. See issue #5007 for what
    # happens when this is expressed as a job-level `if:` instead.
    if event.get("action") == "labeled":
        return event.get("label") in gates
    return True


def compute(files, event):
    """Return job flags, including main's warmer for shared native cache inputs."""
    selected = {
        name: event_allows(name, event) and matches(patterns, files)
        for name, patterns in FILTERS.items()
    }
    # Use the fingerprint's exact patterns and matcher for main's producer,
    # including inputs owned by other workflows, without broadening PR jobs.
    if (event.get("name") == "push" and event_allows("build_linux", event)
            and matches(NATIVE_LIBRARY_INPUTS, files)):
        selected["build_linux"] = True
    return selected


def event_from_env():
    labels = os.environ.get("PR_LABELS", "")
    return {
        "name": os.environ.get("EVENT_NAME", ""),
        "action": os.environ.get("EVENT_ACTION", ""),
        "label": os.environ.get("LABEL_NAME", ""),
        "labels": json.loads(labels) if labels.strip() else [],
    }


def glob_to_regex(pat):
    # Translate a picomatch-style glob to a regex. "**/" at the start or
    # interior is optional ("(?:.*/)?") so that "**/pom.xml" matches at the
    # repo root; bare "**" is greedy across path separators.
    out = []
    i = 0
    while i < len(pat):
        c = pat[i]
        if c == "*" and i + 1 < len(pat) and pat[i + 1] == "*":
            if i + 2 < len(pat) and pat[i + 2] == "/":
                out.append("(?:.*/)?")
                i += 3
            else:
                out.append(".*")
                i += 2
        elif c == "*":
            out.append("[^/]*")
            i += 1
        elif c == "?":
            out.append("[^/]")
            i += 1
        elif c in r".+(){}[]^$|\\":
            out.append("\\" + c)
            i += 1
        else:
            out.append(c)
            i += 1
    return "^" + "".join(out) + "$"


def matches(patterns, files):
    includes = [re.compile(glob_to_regex(p)) for p in patterns if not p.startswith("!")]
    excludes = [re.compile(glob_to_regex(p[1:])) for p in patterns if p.startswith("!")]
    for f in files:
        if any(r.match(f) for r in includes) and not any(r.match(f) for r in excludes):
            return True
    return False


if __name__ == "__main__":
    event = event_from_env()
    # workflow_dispatch has no meaningful base to diff against, so the caller
    # passes an empty list and every path filter is treated as matched.
    if event["name"] == "workflow_dispatch":
        for name in FILTERS:
            print(f"{name}=true")
        sys.exit(0)
    files_path = Path(sys.argv[1])
    files = [line.strip() for line in files_path.read_text().splitlines() if line.strip()]
    for name, flag in compute(files, event).items():
        print(f"{name}={'true' if flag else 'false'}")
