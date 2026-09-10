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

# Guards five CI invariants that are silent when broken:
#
#   1. Change-filter routing. dev/ci/compute-changes.py decides which heavy
#      jobs run. A file that a job depends on but that no filter lists makes
#      that job skip, so the edit merges with only preflight having looked at
#      it. The table below pins the routing for the shared build inputs.
#
#   2. Event policy. The routing script also decides which events may run
#      each job; POLICY_CASES pins the behavior of the former workflow gates.
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
#   4. Artifact-name uniqueness and explicit shared-producer wiring. Names
#      are scoped to the *run*, not the calling workflow. ci.yml calls the
#      Spark SQL and Iceberg
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
    # The artifact wrappers are used by every producer and consumer of a
    # shared artifact. Without these, an edit confined to one of them routes
    # to nothing at all and merges having been exercised by no consumer.
    ([".github/actions/upload-artifact-retry/action.yaml"], BUILD_JOBS),
    ([".github/actions/download-artifact-retry/action.yaml"], BUILD_JOBS),
    # Editing the shared Linux producer must exercise every Linux consumer.
    ([".github/workflows/build_linux_native.yml"], BUILD_JOBS - {"build_macos"}),
    # Spot checks that the additions above did not widen unrelated routes.
    (["docs/source/user-guide/overview.md"], {"docs"}),
    (["native/core/benches/parquet_read.rs"], {"benchmark"}),
]

# Event policy. Each case is (event, expected set of jobs allowed to run),
# where "allowed" ignores path filters. Written out longhand rather than
# derived from POLICY, so that a change to the routing has to be stated twice
# and cannot be made by accident.
PR_TIER = {"build_linux", "spark_4_1", "iceberg_1_11"}
SPARK_OPT_IN = {"spark_3_4", "spark_3_5", "spark_4_0"}
ICEBERG_OPT_IN = {"iceberg_1_8", "iceberg_1_9", "iceberg_1_10"}
BUILD_OPT_IN = {"build_macos", "benchmark"}
QUEUE_TIER = PR_TIER | SPARK_OPT_IN | ICEBERG_OPT_IN | BUILD_OPT_IN
ALL_JOBS = QUEUE_TIER | {"docs"}

POLICY_CASES = [
    # A manual run may exercise anything.
    ({"name": "workflow_dispatch"}, ALL_JOBS),
    # The merge queue is the authoritative gate: everything except the site
    # deploy, which can only run once the commit is actually on main.
    ({"name": "merge_group"}, QUEUE_TIER),
    # Push to main is the site deploy plus the Linux build, which is there to
    # refresh main's actions/cache entries (see POLICY). Any other test job
    # showing up here means every merge is paying for it twice.
    ({"name": "push"}, {"docs", "build_linux"}),
    # A plain pull request: the PR tier only. docs must never run here, and the
    # opt-in suites stay off without their label.
    ({"name": "pull_request", "action": "opened", "labels": []}, PR_TIER),
    ({"name": "pull_request", "action": "synchronize", "labels": []}, PR_TIER),
    # Spark 3.5 moved behind the queue; its label is the escape hatch.
    (
        {"name": "pull_request", "action": "synchronize", "labels": ["run-spark-3.5-tests"]},
        PR_TIER | {"spark_3_5"},
    ),
    # So did the macOS build and the benchmark compile check, each with its
    # own label. Neither label pulls in the other.
    (
        {"name": "pull_request", "action": "synchronize", "labels": ["run-macos-tests"]},
        PR_TIER | {"build_macos"},
    ),
    (
        {"name": "pull_request", "action": "synchronize", "labels": ["run-benchmark-check"]},
        PR_TIER | {"benchmark"},
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
DOWNLOAD_USES = re.compile(r"uses:\s*(\./\.github/actions/download-artifact-retry|actions/download-artifact@)")
# A following step starts with `- `, unlike the current step's `with:` keys.
NEW_STEP = re.compile(r"^\s*-\s")

# A job id in a workflow file, and the two `uses:` shapes the checkout guard
# below cares about. `./.github/workflows/` is deliberately not matched: that
# is a reusable-workflow call, which resolves from the repository rather than
# from the runner's workspace and so needs no checkout.
JOB_KEY = re.compile(r"^  ([A-Za-z0-9_-]+):\s*$")
LOCAL_ACTION_USES = re.compile(r"uses:\s*(\./\.github/actions/\S+)")
CHECKOUT_USES = re.compile(r"uses:\s*actions/checkout@")
SHARED_NATIVE_WORKFLOW = "build_linux_native.yml"
SHARED_NATIVE_JOB = "build_linux_native"
SHARED_NATIVE_INPUT = "native-library-artifact"
SHARED_NATIVE_ARTIFACT = "native-lib-linux"
SHARED_NATIVE_EXPRESSION = "${{ inputs.native-library-artifact }}"
SHARED_NATIVE_CONSUMERS = {
    "pr_build_linux.yml",
    "spark_sql_test_reusable.yml",
    "iceberg_spark_test_reusable.yml",
}


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


def artifact_steps(path):
    """Return artifact steps and their direct `with:` inputs."""
    artifacts = []
    lines = path.read_text(encoding="utf-8").splitlines()
    for index, line in enumerate(lines):
        if UPLOAD_USES.search(line):
            kind = "upload"
        elif DOWNLOAD_USES.search(line):
            kind = "download"
        else:
            continue
        indent = len(line) - len(line.lstrip())
        body = []
        for following in lines[index + 1:]:
            if following.strip() and not following.lstrip().startswith("#"):
                if NEW_STEP.match(following) or len(following) - len(following.lstrip()) < indent:
                    break
            body.append(following)
        text = "\n".join(body)
        with_key = re.search(r"^( +)with:\s*$", text, re.MULTILINE)
        if with_key:
            with_indent = len(with_key.group(1))
            inputs = block_mapping(block_mapping(text, with_indent)["with"][1], with_indent + 2)
            artifacts.append((kind, {key: scalar(value) for key, (value, _) in inputs.items()}))
    return artifacts


def artifact_names(path):
    """Return ([upload names], [download names]) for one workflow file."""
    steps = artifact_steps(path)
    return tuple([inputs["name"] for kind, inputs in steps if kind == expected and "name" in inputs]
                 for expected in ("upload", "download"))


def block_mapping(text, indent):
    """Read block-style keys at the workflow files' conventional indentation.

    This only inspects the small mapping subset needed by the guards below;
    actionlint remains responsible for validating GitHub Actions YAML syntax.
    Values are (inline value, indented body), so scalar inputs and nested
    workflow/job mappings can be checked without a third-party YAML dependency.
    """
    pattern = re.compile(r"^" + " " * indent + r"([\w-]+):[^\S\n]*(.*)$", re.MULTILINE)
    matches = list(pattern.finditer(text))
    return {
        match.group(1): (match.group(2).strip(),
                         text[match.end():matches[index + 1].start()
                              if index + 1 < len(matches) else len(text)])
        for index, match in enumerate(matches)
    }


def scalar(value):
    return value.strip().strip("\"'")


def dependencies(job):
    value, body = block_mapping(job, 4).get("needs", ("", ""))
    if value.startswith("[") and value.endswith("]"):
        return {scalar(item) for item in value[1:-1].split(",")}
    if value:
        return {scalar(value)}
    return {scalar(item) for item in re.findall(r"^\s+- (.+)$", body, re.MULTILINE)}


def shared_native_failures(workflows, jobs, artifacts):
    """Validate the sole allowed cross-workflow artifact producer/consumer edge."""
    failures = []
    producer_uses = f"./.github/workflows/{SHARED_NATIVE_WORKFLOW}"
    calls = [job_id for job_id, (_, body) in jobs.items()
             if scalar(block_mapping(body, 4).get("uses", ("", ""))[0]) == producer_uses]
    if calls != [SHARED_NATIVE_JOB]:
        failures.append(f"ci.yml: expected exactly one {SHARED_NATIVE_JOB} call to {producer_uses}")

    if SHARED_NATIVE_JOB in jobs:
        body = jobs[SHARED_NATIVE_JOB][1]
        if "changes" not in dependencies(body):
            failures.append(f"ci.yml: {SHARED_NATIVE_JOB} must need changes")
        if "strategy" in block_mapping(body, 4):
            failures.append(f"ci.yml: {SHARED_NATIVE_JOB} must not use a matrix")
    for filename, (uploads, _) in artifacts.items():
        if filename != SHARED_NATIVE_WORKFLOW and SHARED_NATIVE_ARTIFACT in uploads:
            failures.append(f"{filename}: only {SHARED_NATIVE_WORKFLOW} may upload '{SHARED_NATIVE_ARTIFACT}'")

    producer = workflows / SHARED_NATIVE_WORKFLOW
    if not producer.exists():
        failures.append(f"{producer}: shared native producer is missing")
    else:
        producer_jobs = block_mapping(
            block_mapping(producer.read_text(encoding="utf-8"), 0).get("jobs", ("", ""))[1], 2)
        uploads = artifacts[SHARED_NATIVE_WORKFLOW][0]
        if len(producer_jobs) != 1 or uploads != [SHARED_NATIVE_ARTIFACT]:
            failures.append(f"{producer}: expected one job uploading '{SHARED_NATIVE_ARTIFACT}' once")
        for _, body in producer_jobs.values():
            if "strategy" in block_mapping(body, 4):
                failures.append(f"{producer}: shared native producer must not use a matrix")

    seen_consumers = set()
    for job_id, (_, body) in jobs.items():
        fields = block_mapping(body, 4)
        called = scalar(fields.get("uses", ("", ""))[0]).removeprefix("./.github/workflows/")
        if called not in SHARED_NATIVE_CONSUMERS:
            continue
        seen_consumers.add(called)
        if not {"changes", SHARED_NATIVE_JOB}.issubset(dependencies(body)):
            failures.append(f"ci.yml: {job_id} must need changes and {SHARED_NATIVE_JOB}")
        inputs = block_mapping(fields.get("with", ("", ""))[1], 6)
        if scalar(inputs.get(SHARED_NATIVE_INPUT, ("", ""))[0]) != SHARED_NATIVE_ARTIFACT:
            failures.append(f"ci.yml: {job_id} must pass {SHARED_NATIVE_INPUT}: {SHARED_NATIVE_ARTIFACT}")

    for filename in sorted(SHARED_NATIVE_CONSUMERS):
        path = workflows / filename
        if filename not in seen_consumers:
            failures.append(f"ci.yml: shared native consumer {filename} is not called")
        if not path.exists():
            failures.append(f"{path}: shared native consumer is missing")
            continue
        text = path.read_text(encoding="utf-8")
        declaration = text
        for key, indent in (("on", 0), ("workflow_call", 2), ("inputs", 4),
                            (SHARED_NATIVE_INPUT, 6)):
            declaration = block_mapping(declaration, indent).get(key, ("", ""))[1]
        fields = block_mapping(declaration, 8)
        if (scalar(fields.get("required", ("", ""))[0]) != "true"
                or scalar(fields.get("type", ("", ""))[0]) != "string"):
            failures.append(f"{path}: {SHARED_NATIVE_INPUT} must be a required string input")
        uploads, downloads = artifacts[filename]
        if SHARED_NATIVE_EXPRESSION not in downloads:
            failures.append(f"{path}: must download {SHARED_NATIVE_EXPRESSION}")
        if any(name.startswith("native-lib") or name == SHARED_NATIVE_EXPRESSION for name in uploads):
            failures.append(f"{path}: native library must only be uploaded by {SHARED_NATIVE_WORKFLOW}")
        native_destinations = [inputs.get("name") for kind, inputs in artifact_steps(path)
                               if kind == "download" and inputs.get("path", "").startswith("native/target")]
        if (any(name.startswith("native-lib") for name in downloads)
                or any(name != SHARED_NATIVE_EXPRESSION for name in native_destinations)):
            failures.append(f"{path}: native downloads must use {SHARED_NATIVE_EXPRESSION}")
        if re.search(r"^\s*(?:cargo build\b|make (?:release|core)\b)", text, re.MULTILINE):
            failures.append(f"{path}: must consume the shared native library instead of building it")
    return failures


def artifact_failures(workflows):
    ci = (workflows / "ci.yml").read_text(encoding="utf-8")
    jobs = block_mapping(block_mapping(ci, 0).get("jobs", ("", ""))[1], 2)
    call_counts = {}
    for called in re.findall(r"uses:\s*\./\.github/workflows/(\S+)", ci):
        call_counts[called] = call_counts.get(called, 0) + 1

    artifacts = {path.name: artifact_names(path) for path in sorted(workflows.glob("*.y*ml"))}
    failures = shared_native_failures(workflows, jobs, artifacts)
    shared_wiring_valid = not failures
    for filename, (uploads, downloads) in artifacts.items():
        path = workflows / filename
        if call_counts.get(filename, 0) > 1:
            for name in uploads:
                if "inputs." not in name:
                    failures.append(
                        f"{path}: artifact '{name}' is uploaded by a workflow ci.yml calls "
                        f"{call_counts[filename]} times; qualify the name with an input "
                        f"(e.g. ${{{{ inputs.spark-full }}}}) so the parallel producers stay distinct"
                    )
        for name in downloads:
            explicitly_shared = (shared_wiring_valid and filename in SHARED_NATIVE_CONSUMERS
                                 and name == SHARED_NATIVE_EXPRESSION)
            if name not in uploads and not explicitly_shared:
                failures.append(
                    f"{path}: artifact '{name}' is downloaded but never uploaded in the same "
                    f"workflow; a producer rename probably missed its consumer"
                )
    return failures


def check_artifact_names():
    failures = artifact_failures(WORKFLOWS)
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


if __name__ == "__main__":
    ok = check_change_filters()
    ok = check_event_policy() and ok
    ok = check_artifact_names() and ok
    ok = check_local_actions_have_checkout() and ok
    ok = check_required_checks() and ok
    if not ok:
        sys.exit(1)
    print("CI config checks passed")
