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

# Guards three CI invariants that are silent when broken:
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
#   3. Artifact-name uniqueness. Artifact names are scoped to the *run*, not
#      to the calling workflow, and ci.yml calls the Spark SQL and Iceberg
#      reusable workflows several times in one run. Two producers sharing a
#      name make `download-artifact` pick by highest artifact ID rather than
#      by `needs`, and make the forced `overwrite` on an upload retry delete
#      a sibling's finished artifact.
#
# Run from the repository root: python3 dev/ci/check-ci-config.py

import importlib.util
import re
import sys
from pathlib import Path

WORKFLOWS = Path(".github/workflows")

# Changed-file list -> the set of outputs compute-changes.py must report true.
# Every other output must be false. Keep one case per shared build input so a
# filter deletion cannot pass unnoticed.
BUILD_JOBS = {
    "build_linux",
    "build_macos",
    "spark_3_4",
    "spark_3_5",
    "spark_4_0",
    "spark_4_1",
    "iceberg_1_8",
    "iceberg_1_9",
    "iceberg_1_10",
    "iceberg_1_11",
}

ROUTING_CASES = [
    # The Maven wrapper and its config feed every job that runs ./mvnw: the
    # Linux/macOS builds, setup-spark-builder, and the Iceberg `mvnw install`.
    ([".mvn/maven.config"], BUILD_JOBS),
    ([".mvn/wrapper/maven-wrapper.properties"], BUILD_JOBS),
    (["mvnw"], BUILD_JOBS),
    # The upload wrapper is used by every producer of a shared artifact.
    ([".github/actions/upload-artifact-retry/action.yaml"], BUILD_JOBS),
    # Spot checks that the additions above did not widen unrelated routes.
    (["docs/source/user-guide/overview.md"], {"docs"}),
    (["native/core/benches/parquet_read.rs"], {"benchmark"}),
]

# Event policy. Each case is (event, expected set of jobs allowed to run),
# where "allowed" ignores path filters. Transcribed from the `if:` expressions
# ci.yml carried before POLICY moved into compute-changes.py, so these pin the
# pre-refactor behaviour rather than restating the new code.
PR_TIER = {"build_linux", "build_macos", "benchmark", "spark_3_5", "spark_4_1", "iceberg_1_11"}
ICEBERG_OPT_IN = {"iceberg_1_8", "iceberg_1_9", "iceberg_1_10"}
ALL_JOBS = PR_TIER | ICEBERG_OPT_IN | {"docs", "spark_3_4", "spark_4_0"}

POLICY_CASES = [
    # A manual run may exercise anything.
    ({"name": "workflow_dispatch"}, ALL_JOBS),
    # Push to main runs every job, docs included: it is the only event that
    # may deploy the site.
    ({"name": "push"}, ALL_JOBS),
    # A plain pull request: the PR tier only. docs must never run here, and the
    # opt-in suites stay off without their label.
    ({"name": "pull_request", "action": "opened", "labels": []}, PR_TIER),
    ({"name": "pull_request", "action": "synchronize", "labels": []}, PR_TIER),
    # An opt-in label present on a pushed commit adds just that suite.
    (
        {"name": "pull_request", "action": "synchronize", "labels": ["run-spark-3.4-tests"]},
        PR_TIER | {"spark_3_4"},
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
DOWNLOAD_USES = re.compile(r"uses:\s*actions/download-artifact@")
# The artifact name is the first `name:` key of the step's `with:` block. A
# following step starts with `- `, which distinguishes it from a `with:` key.
WITH_NAME = re.compile(r"^\s+name:\s*(\S.*?)\s*$")
NEW_STEP = re.compile(r"^\s*-\s")


def load_filters():
    spec = importlib.util.spec_from_file_location("compute_changes", "dev/ci/compute-changes.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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


if __name__ == "__main__":
    ok = check_change_filters()
    ok = check_event_policy() and ok
    ok = check_artifact_names() and ok
    if not ok:
        sys.exit(1)
    print("CI config checks passed")
