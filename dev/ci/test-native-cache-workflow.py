#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Check the native action's actual conditions for cache hits and misses."""

from pathlib import Path
import re
import shlex
import subprocess
import unittest


def condition_matches(expression, context):
    """Evaluate the action's string comparisons and boolean operators with Bash.

    Substitute fixed test context values into the expression read from YAML.
    Bash [[ ]] supports the comparisons, parentheses, && and || used here;
    unsupported syntax fails the test instead of emulating the Actions runner.
    """
    expression = expression.removeprefix("${{ ").removesuffix(" }}")
    for name, value in context.items():
        expression = expression.replace(name, shlex.quote(value))
    result = subprocess.run(["bash", "-c", f"[[ {expression} ]]"], capture_output=True, text=True)
    if result.returncode not in (0, 1):
        raise AssertionError(result.stderr)
    return result.returncode == 0


class NativeCacheWorkflowTests(unittest.TestCase):
    def test_cache_hit_decisions(self):
        """Protect compilation, lookup-only restores, and main-only publication."""
        project = Path(__file__).resolve().parents[2]
        action = (project / ".github/actions/build-native-ci/action.yaml").read_text()
        conditions = {}
        for block in re.split(r"^    - name: ", action, flags=re.MULTILINE)[1:]:
            name, _, body = block.partition("\n")
            for line in body.splitlines():
                if line.startswith("      if: "):
                    conditions[name] = line.removeprefix("      if: ")
                elif line.startswith("        lookup-only: "):
                    conditions["lookup-only"] = line.removeprefix("        lookup-only: ")
        names = ("lookup-only", "Restore incremental Cargo cache", "Build native library (CI profile)",
                 "Save native library cache", "Save incremental Cargo cache")
        self.assertEqual(set(conditions), set(names))
        # Expected: lookup only, restore Cargo, build, save library, save Cargo.
        cases = [
            ("pull_request", "refs/pull/1/merge", "true", "", (False, False, False, False, False)),
            ("pull_request", "refs/pull/1/merge", "", "true", (False, True, True, False, False)),
            ("merge_group", "refs/heads/gh-readonly-queue/main/test", "true", "", (False, False, False, False, False)),
            ("merge_group", "refs/heads/gh-readonly-queue/main/test", "", "false", (False, True, True, False, False)),
            ("push", "refs/heads/main", "true", "true", (True, True, False, False, False)),
            ("push", "refs/heads/main", "true", "false", (True, True, True, False, True)),
            ("push", "refs/heads/main", "true", "", (True, True, True, False, True)),
            ("push", "refs/heads/main", "", "true", (True, True, True, True, False)),
            ("push", "refs/heads/main", "", "", (True, True, True, True, True)),
            ("push", "refs/heads/branch", "", "", (False, True, True, False, False)),
            ("schedule", "refs/heads/main", "", "", (False, True, True, False, False)),
            ("workflow_dispatch", "refs/heads/main", "", "", (False, True, True, False, False)),
        ]
        for event, ref, library_hit, cargo_hit, expected in cases:
            with self.subTest(event=event, ref=ref, library_hit=library_hit, cargo_hit=cargo_hit):
                context = {"github.event_name": event, "github.ref": ref,
                           "steps.binary-cache.outputs.cache-hit": library_hit,
                           "steps.cargo-cache.outputs.cache-hit": cargo_hit}
                self.assertEqual(tuple(condition_matches(conditions[name], context) for name in names), expected)


if __name__ == "__main__":
    unittest.main()
