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
        ".github/actions/java-test/**",
        ".github/actions/rust-test/**",
        ".github/actions/upload-artifact-retry/**",
        ".github/actions/download-artifact-retry/**",
        "!**.md",
        "!native/core/benches/**",
        "!native/spark-expr/benches/**",
        "!spark/src/test/scala/org/apache/spark/sql/benchmark/**",
        "!spark/src/main/scala/org/apache/comet/GenerateDocs.scala",
    ],
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
        ".github/actions/setup-builder/**",
        ".github/actions/setup-spark-builder/**",
        ".github/actions/upload-artifact-retry/**",
        ".github/actions/download-artifact-retry/**",
        ".mvn/**",
        "mvnw",
    ],
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

# Which events may run each job, independent of the path filters above.
#
#   "pr"              every pull request
#   "queue"           the merge queue, i.e. a merge_group event
#   "push"            push to main
#   "label:<name>"    a pull request carrying that label
#
# workflow_dispatch always runs everything, so it is not listed. "pr" and
# "label:" are mutually exclusive -- a job is either unconditional on pull
# requests or opt-in, never both -- and check-ci-config.py rejects a job that
# lists both rather than letting the label quietly win.
#
# Almost everything is "queue": the merge queue is the authoritative gate, and
# it tests the merge result rather than the PR head. "push" is reserved for
# work that can only happen once a commit is on main. Adding "push" back to a
# test job would make every merge run it twice, once in the queue and once
# after, which is the thing the queue was adopted to avoid.
POLICY = {
    # The one test job that also runs on push to main, and only because of
    # actions/cache scoping: a pull request can restore caches saved on its
    # own branch or on main, and nowhere else. The queue runs on a throwaway
    # gh-readonly-queue/* branch, so whatever it saves is deleted with that
    # branch. Without a push run, a Cargo.lock or pom.xml change would leave
    # main's cargo-registry, Maven and TPC-H/TPC-DS caches stale forever, and
    # every later pull request would pay the delta on top of the restore-keys
    # prefix match.
    "build_linux": ["pr", "queue", "push"],
    # macOS runners are the scarcest capacity we have, and the Linux build
    # already covers rustfmt and the Rust/JVM compile on every PR. The label
    # is for a change that touches platform-specific code.
    "build_macos": ["queue", "label:run-macos-tests"],
    # Benchmark sources are compiled and linted, never run, so a break there
    # cannot affect a PR's correctness verdict; the queue catches it.
    "benchmark": ["queue", "label:run-benchmark-check"],
    # docs deploys to asf-site, so it must not run from a pull request or from
    # the queue's throwaway branch.
    "docs": ["push"],
    "spark_3_4": ["queue", "label:run-spark-3.4-tests"],
    "spark_3_5": ["queue", "label:run-spark-3.5-tests"],
    "spark_4_0": ["queue", "label:run-spark-4.0-tests"],
    # Spark 4.1 is the default build profile, so it is the cheapest early
    # warning that a change is wrong and stays in the PR tier.
    "spark_4_1": ["pr", "queue"],
    "iceberg_1_8": ["queue", "label:run-iceberg-tests"],
    "iceberg_1_9": ["queue", "label:run-iceberg-tests"],
    "iceberg_1_10": ["queue", "label:run-iceberg-tests"],
    # Iceberg 1.11 is our only Spark 4.1 Iceberg coverage, so it is not opt-in.
    "iceberg_1_11": ["pr", "queue"],
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
    """Return {job: bool}, folding the path filter and the event policy."""
    return {
        name: event_allows(name, event) and matches(patterns, files)
        for name, patterns in FILTERS.items()
    }


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
