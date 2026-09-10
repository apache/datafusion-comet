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

# Guards two CI invariants that are silent when broken:
#
#   1. Change-filter routing. dev/ci/compute-changes.py decides which heavy
#      jobs run. A file that a job depends on but that no filter lists makes
#      that job skip, so the edit merges with only preflight having looked at
#      it. The table below pins the routing for the shared build inputs.
#
#   2. Artifact-name uniqueness. Artifact names are scoped to the *run*, not
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
    ok = check_artifact_names() and ok
    if not ok:
        sys.exit(1)
    print("CI config checks passed")
