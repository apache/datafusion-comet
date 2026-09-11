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

# Guards four CI invariants that are silent when broken:
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
#      runs, which skip the PR tier by design, to a name nothing requires:
#      GitHub keeps the most recent check run per name per commit, so a label
#      run publishing the required name would overwrite the real verdict.
#
#   4. Artifact-name uniqueness. Artifact names are scoped to the *run*, not
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
ASF_YAML = Path(".asf.yaml")

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
# `Required Checks` on main.
AGGREGATOR_EXEMPT = {AGGREGATOR_JOB, "docs"}

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

# A group can combine changes from multiple PRs. Path filtering still applies,
# even though the queue permits all supported Spark and Iceberg versions.
QUEUE_ROUTING_CASES = [
    (["pom.xml", "native/core/benches/parquet_read.rs", "docs/source/index.md"], ALL_JOBS - {"docs"}),
    ([".mvn/maven.config"], BUILD_JOBS),
    (["native/core/benches/parquet_read.rs"], {"benchmark"}),
    (["docs/source/index.md"], set()),
    ([], set()),
]

POLICY_CASES = [
    # A manual run may exercise anything.
    ({"name": "workflow_dispatch"}, ALL_JOBS),
    # Push to main runs every job, docs included: it is the only event that
    # may deploy the site.
    ({"name": "push"}, ALL_JOBS),
    # A queue group runs every applicable suite, including PR opt-in versions,
    # but must never deploy the site from its temporary branch.
    ({"name": "merge_group", "action": "checks_requested"}, ALL_JOBS - {"docs"}),
    ({"name": "merge_group", "action": "destroyed"}, set()),
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
    for files, expected in QUEUE_ROUTING_CASES:
        outputs = module.compute(files, {"name": "merge_group", "action": "checks_requested"})
        actual = {job for job, enabled in outputs.items() if enabled}
        if actual != expected:
            failures.append(
                f"merge_group files={files}: expected {sorted(expected)}, got {sorted(actual)}"
            )
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


if __name__ == "__main__":
    ok = check_change_filters()
    ok = check_event_policy() and ok
    ok = check_artifact_names() and ok
    ok = check_required_checks() and ok
    if not ok:
        sys.exit(1)
    print("CI config checks passed")
