#!/usr/bin/env python3
#
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

"""Exercise the real native-cache action guards and shell steps without building Rust.

Only remote cache operations and Cargo compilation are simulated. The real
manifest helper, shell commands, step order, and guards run from the checkout.
The extractor deliberately supports this action's formatting, not general YAML.
"""

import ast
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
ACTION = ROOT / ".github/actions/build-native-ci/action.yaml"
STEPS = dict(part.split("\n", 1) for part in
             re.split(r"(?m)^    - name: ", ACTION.read_text())[1:])
VALIDATE = "Validate cached native library"
BUILD = "Build native library (CI profile)"
PREPARE = "Prepare native library cache"
RESTORE_TARGET = "Restore incremental Cargo cache"
SAVE_BINARY = "Save native library cache"
SAVE_TARGET = "Save incremental Cargo cache"


def field(block, name, indent=6):
    """Return one scalar field from a step block, or an empty string if absent.

    The supplied indentation distinguishes step fields from nested inputs.
    This read-only extractor handles this action's single-line fields only.
    """
    match = re.search(rf"(?m)^{' ' * indent}{re.escape(name)}: (.+)$", block)
    return match.group(1) if match else ""


def condition(expression, context):
    """Evaluate the action's comparisons/boolean operators against string outputs.

    Missing outputs become empty strings as in Actions. Unsupported syntax
    raises ValueError instead of silently inventing new GitHub semantics.
    Only literal comparisons and boolean operators are accepted; no calls run.
    """
    expression = expression.removeprefix("${{ ").removesuffix(" }}")
    expression = re.sub(r"\b(?:github\.[\w-]+|steps\.[\w-]+\.outputs\.[\w-]+)\b",
                        lambda match: repr(context.get(match.group(), "")), expression)
    expression = expression.replace("&&", " and ").replace("||", " or ")
    expression = re.sub(r"!(?!=)", " not ", expression).strip()
    tree = ast.parse(expression or "True", mode="eval")
    allowed = (ast.Expression, ast.BoolOp, ast.UnaryOp, ast.Compare, ast.Constant,
               ast.And, ast.Or, ast.Not, ast.Eq, ast.NotEq)
    if any(not isinstance(node, allowed) for node in ast.walk(tree)):
        raise ValueError(f"Unsupported action guard: {expression}")
    return eval(compile(tree, str(ACTION), "eval"), {"__builtins__": {}}, {})


def shell_step(name, workspace, environment):
    """Execute the named action's literal shell block in an isolated workspace.

    Apply its RUSTFLAGS override and return a captured subprocess result.
    Commands use bash's Actions-style fail-fast flags; no real Cargo runs.
    A missing multiline block raises ValueError before starting a process.
    """
    block = STEPS[name]
    match = re.search(r"(?m)^      run: \|\n((?:        .*\n|\n)*)", block + "\n")
    if not match:
        raise ValueError(f"Missing literal shell block: {name}")
    script = "\n".join(line[8:] for line in match.group(1).splitlines())
    environment = environment.copy()
    flags = field(block, "RUSTFLAGS", 8)
    if flags:
        environment["RUSTFLAGS"] = flags.strip("'\"")
    return subprocess.run(["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", script],
                          cwd=workspace, env=environment, capture_output=True, text=True)


def run_scenario(cache_hit="false", payload="missing", event="pull_request",
                 ref="refs/pull/123/merge", fail_cargo=False, target_hit="false"):
    """Return observed operations/output for one simulated remote-cache scenario.

    A temporary workspace holds real manifest-helper inputs and a fake Cargo
    executable. Remote actions record their invocation and supply cache outputs;
    all other relevant steps execute their actual shell bodies. Failed commands
    suppress subsequent steps, matching Actions' implicit success() guard.
    The workspace and payloads are deleted before returning copied observations.
    """
    with tempfile.TemporaryDirectory(prefix="comet-cache-workflow-") as temporary:
        workspace = Path(temporary)
        (workspace / "dev/ci").mkdir(parents=True)
        shutil.copyfile(ROOT / "dev/ci/native-library-cache.py",
                        workspace / "dev/ci/native-library-cache.py")
        library = workspace / "native/target/ci/libcomet.so"
        library.parent.mkdir(parents=True)
        executable = workspace / "bin/cargo"
        executable.parent.mkdir()
        executable.write_text("#!/bin/bash\nset -eu\n"
                              'printf "%s|%s|%s\\n" "$PWD" "$*" "$RUSTFLAGS" >> "$CARGO_LOG"\n'
                              '[ "$FAIL_CARGO" = 0 ] || exit 23\n'
                              'mkdir -p target/ci\nprintf built > target/ci/libcomet.so\n')
        executable.chmod(0o755)
        output = workspace / "step-output"
        environment = dict(os.environ, PATH=f"{executable.parent}:{os.environ['PATH']}",
                           RUNNER_TEMP=str(workspace), NATIVE_CACHE_KEY="fixture-native-key",
                           GITHUB_OUTPUT=str(output), CARGO_LOG=str(workspace / "cargo-log"),
                           FAIL_CARGO=str(int(fail_cargo)))
        if payload != "missing":
            library.write_bytes(b"cached")
            prepared = shell_step(PREPARE, workspace, environment)
            if prepared.returncode:
                raise AssertionError(prepared.stderr)
            library.unlink()
            if payload == "corrupt":
                (workspace / "comet-native-library/libcomet.so").write_bytes(b"damaged")
            elif payload == "wrong-key":
                environment["NATIVE_CACHE_KEY"] = "different-native-key"
        context = {"github.event_name": event, "github.ref": ref}
        operations, successful, lookup_only = [], True, False
        for name, block in STEPS.items():
            if name == "Fingerprint native build inputs":
                continue  # The dedicated key tests exercise tool/source fingerprinting.
            if not successful or not condition(field(block, "if"), context):
                continue
            operations.append(name)
            if field(block, "uses"):
                if name == "Restore native library cache":
                    lookup_only = condition(field(block, "lookup-only", 8), context)
                    context["steps.binary-cache.outputs.cache-hit"] = cache_hit
                elif name == RESTORE_TARGET:
                    context["steps.cargo-cache.outputs.cache-hit"] = target_hit
                continue
            output.write_text("")
            result = shell_step(name, workspace, environment)
            successful = result.returncode == 0
            for line in output.read_text().splitlines():
                key, value = line.split("=", 1)
                context[f"steps.{field(block, 'id')}.outputs.{key}"] = value
        log = workspace / "cargo-log"
        return dict(operations=operations, successful=successful, lookup_only=lookup_only,
                    cargo=log.read_text() if log.exists() else "",
                    library=library.read_bytes() if library.exists() else None)


class NativeCacheWorkflowTest(unittest.TestCase):
    """Check native reuse, compile fallback, and trusted cache ownership end to end."""

    def test_exact_valid_hit_skips_compilation_and_target_archive(self):
        """A verified exact hit installs cached bytes and avoids expensive target I/O."""
        result = run_scenario("true", "good")
        self.assertTrue(result["successful"])
        self.assertEqual(result["library"], b"cached")
        self.assertEqual(result["cargo"], "")
        for step in (BUILD, RESTORE_TARGET, SAVE_TARGET, SAVE_BINARY, PREPARE):
            self.assertNotIn(step, result["operations"])

    def test_all_misses_compile_even_with_an_exact_target_cache(self):
        """Missing, partial, damaged, and mismatched binary entries all run locked Cargo."""
        for hit, payload in (("", "missing"), ("false", "good"), ("true", "missing"),
                             ("true", "corrupt"), ("true", "wrong-key")):
            with self.subTest(hit=hit, payload=payload):
                result = run_scenario(hit, payload, target_hit="true")
                self.assertTrue(result["successful"])
                self.assertEqual(result["library"], b"built")
                self.assertIn(RESTORE_TARGET, result["operations"])
                self.assertRegex(result["cargo"], r"/native\|build --locked --profile ci\|")
                self.assertIn("-Ctarget-cpu=x86-64-v3 -Clink-arg=-fuse-ld=bfd", result["cargo"])

    def test_only_main_push_saves_caches(self):
        """Equivalent cold builds save only for push-to-main, including manual/main cases."""
        for event, ref in (("pull_request", "refs/pull/123/merge"),
                           ("merge_group", "refs/heads/gh-readonly-queue/main/test"),
                           ("schedule", "refs/heads/main"), ("workflow_dispatch", "refs/heads/main"),
                           ("push", "refs/heads/feature"), ("push", "refs/heads/main")):
            with self.subTest(event=event, ref=ref):
                result = run_scenario(event=event, ref=ref)
                writes = event == "push" and ref == "refs/heads/main"
                self.assertTrue(result["successful"])
                for step in (PREPARE, SAVE_BINARY, SAVE_TARGET):
                    self.assertEqual(step in result["operations"], writes)

    def test_main_push_warms_target_even_when_binary_exists(self):
        """Main does lookup-only for an existing binary, but still compiles and saves target."""
        result = run_scenario("true", "good", "push", "refs/heads/main")
        self.assertTrue(result["lookup_only"])
        self.assertEqual(result["library"], b"built")
        self.assertIn(BUILD, result["operations"])
        self.assertIn(SAVE_TARGET, result["operations"])
        for step in (VALIDATE, PREPARE, SAVE_BINARY):
            self.assertNotIn(step, result["operations"])

    def test_failed_cargo_cannot_prepare_or_save(self):
        """A failed main build stops before publishing either incomplete cache entry."""
        result = run_scenario(event="push", ref="refs/heads/main", fail_cargo=True)
        self.assertFalse(result["successful"])
        self.assertIsNone(result["library"])
        for step in (PREPARE, SAVE_BINARY, SAVE_TARGET):
            self.assertNotIn(step, result["operations"])


if __name__ == "__main__":
    unittest.main()
