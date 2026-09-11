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

"""Validate the umbrella's results, including jobs legitimately skipped by policy."""

import json
import os
import sys

# Routing output -> reusable caller id. Keep synchronized with ci.yml.
JOBS = {
    "build_linux": "pr_build_linux",
    "build_macos": "pr_build_macos",
    "benchmark": "pr_benchmark_check",
    "docs": "docs",
    "spark_3_4": "spark_3_4",
    "spark_3_5": "spark_3_5",
    "spark_4_0": "spark_4_0",
    "spark_4_1": "spark_4_1",
    "iceberg_1_8": "iceberg_1_8",
    "iceberg_1_9": "iceberg_1_9",
    "iceberg_1_10": "iceberg_1_10",
    "iceberg_1_11": "iceberg_1_11",
}


def failures(needs):
    errors = []
    expected = {"preflight", "changes", *JOBS.values()}
    if set(needs) != expected:
        errors.append(f"Unexpected dependency set: missing {sorted(expected - set(needs))}, "
                      f"extra {sorted(set(needs) - expected)}")
    for job in ("preflight", "changes"):
        if needs.get(job, {}).get("result") != "success":
            errors.append(f"{job} must succeed")
    plan = needs.get("changes", {}).get("outputs", {})
    for output, job in JOBS.items():
        flag = plan.get(output)
        result = needs.get(job, {}).get("result")
        if flag not in ("true", "false"):
            errors.append(f"Missing or invalid plan for {job}: {flag!r}")
        elif flag == "true" and result != "success":
            errors.append(f"Required {job}: {result!r}")
        elif flag == "false" and result != "skipped":
            errors.append(f"Unplanned {job}: {result!r} (expected skipped)")
    return errors


if __name__ == "__main__":
    errors = failures(json.loads(os.environ["CI_NEEDS_JSON"]))
    for error in errors:
        print(error, file=sys.stderr)
    if errors:
        sys.exit(1)
    print("All planned CI checks passed")
