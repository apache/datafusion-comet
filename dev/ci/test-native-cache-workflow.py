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
import os
import re
import shlex
import subprocess
import tempfile
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
    def test_fingerprint_and_build_flags_stay_scoped_to_the_action_steps(self):
        """Run the key/build scripts with their declared env and observe every native invocation."""
        project = Path(__file__).resolve().parents[2]
        action = (project / ".github/actions/build-native-ci/action.yaml").read_text()
        blocks = {block.partition("\n")[0]: block
                  for block in re.split(r"^    - name: ", action, flags=re.MULTILINE)[1:]}
        key = blocks["Fingerprint native build inputs"]
        build = blocks["Build native library (CI profile)"]

        def rustflags(block):
            return re.search(r"^        RUSTFLAGS: (.+)$", block, re.MULTILINE).group(1)

        def script(block):
            return "\n".join(line[8:] for line in block.split("      run: |\n", 1)[1].splitlines()
                             if line.startswith("        "))

        configured_flags, = shlex.split(rustflags(key))
        self.assertEqual(rustflags(build), "${{ steps.key.outputs.rustflags }}")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "native").mkdir()
            (root / "bin").mkdir()
            for tool in ("python3", "cargo"):
                path = root / "bin" / tool
                path.write_text(f'#!/bin/sh\nprintf "{tool} %s\\n" "$RUSTFLAGS" >> "$FLAGS_LOG"\n')
                path.chmod(0o755)
            output, global_env, log = (root / name for name in ("output", "env", "flags"))
            global_env.touch()
            caller = {**os.environ, "PATH": f"{root / 'bin'}:{os.environ['PATH']}",
                      "RUSTFLAGS": "caller-original-flags", "GITHUB_OUTPUT": str(output),
                      "GITHUB_ENV": str(global_env), "FLAGS_LOG": str(log)}
            subprocess.run(["bash", "-e", "-o", "pipefail", "-c", script(key)], cwd=root,
                           env={**caller, "RUSTFLAGS": configured_flags}, check=True)
            outputs = dict(line.split("=", 1) for line in output.read_text().splitlines())
            self.assertEqual(outputs["rustflags"], configured_flags)
            subprocess.run(["bash", "-e", "-o", "pipefail", "-c", script(build)], cwd=root,
                           env={**caller, "RUSTFLAGS": outputs["rustflags"]}, check=True)
            self.assertEqual(log.read_text().splitlines(),
                             [f"{tool} {configured_flags}" for tool in ("python3", "cargo", "python3")])
            self.assertEqual(global_env.read_text(), "", "native flags must not leak to subsequent caller steps")

    def test_build_rejects_unfingerprinted_dependency(self):
        """Execute the actual build step; an omitted input must fail before publication."""
        project = Path(__file__).resolve().parents[2]
        action = (project / ".github/actions/build-native-ci/action.yaml").read_text()
        block = next(block for block in re.split(r"^    - name: ", action, flags=re.MULTILINE)
                     if block.startswith("Build native library (CI profile)\n"))
        script = "\n".join(line[8:] for line in block.split("      run: |\n", 1)[1].splitlines()
                           if line.startswith("        "))
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "native/src").mkdir(parents=True)
            (root / "native/src/lib.rs").write_text("pub fn example() {}\n")
            (root / "native/README.md").write_text("excluded from the fingerprint\n")
            (root / "dev").mkdir()
            (root / "dev/ci").symlink_to(project / "dev/ci", target_is_directory=True)
            (root / "jdk").mkdir()
            (root / "bin").mkdir()
            cargo = root / "bin/cargo"
            cargo.write_text('#!/bin/sh\nmkdir -p target/ci\n'
                             'printf "target/ci/libcomet.so: %s\\n" "$TEST_DEPENDENCY" > target/ci/libcomet.d\n')
            cargo.chmod(0o755)
            subprocess.run(["git", "init", "--quiet"], cwd=root, check=True)
            subprocess.run(["git", "add", "native"], cwd=root, check=True)
            env = {**os.environ, "PATH": f"{root / 'bin'}:{os.environ['PATH']}",
                   "JAVA_HOME": str(root / "jdk")}
            for dependency, success in (("native/src/lib.rs", True), ("native/README.md", False)):
                with self.subTest(dependency=dependency):
                    result = subprocess.run(
                        ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", script],
                        cwd=root, env={**env, "TEST_DEPENDENCY": str(root / dependency)},
                        capture_output=True, text=True)
                    self.assertEqual(result.returncode == 0, success, result.stdout + result.stderr)

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
                           "steps.library-cache.outputs.cache-hit": library_hit,
                           "steps.cargo-cache.outputs.cache-hit": cargo_hit}
                self.assertEqual(tuple(condition_matches(conditions[name], context) for name in names), expected)


if __name__ == "__main__":
    unittest.main()
