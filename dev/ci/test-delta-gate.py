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

"""Run the actual Delta gate and retry helper without network or build tools."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


REPO = Path(__file__).resolve().parents[2]
PROFILES = ("spark-4.1", "spark-4.1,contrib-delta", "spark-3.5,contrib-delta", "spark-4.0,contrib-delta")

# All fake commands record their arguments. Only Maven model resolution can
# recover; a compiler failure includes the same network text to catch retries
# accidentally being widened to compilation or tests.
FAKE_TOOL = r'''
import json, os, sys
from pathlib import Path

tool, args = Path(sys.argv[0]).name, sys.argv[1:]
root = Path(os.environ["COMET_TEST_GATE_ROOT"])
scenario = os.environ["COMET_TEST_GATE_SCENARIO"]
calls_file = root / "calls.jsonl"
calls = [json.loads(line) for line in calls_file.read_text().splitlines()] if calls_file.exists() else []
with calls_file.open("a") as out:
    out.write(json.dumps([tool, *args]) + "\n")

def compile_stage(name):
    if scenario == name:
        for stream in (sys.stdout, sys.stderr):
            for index in range(100):
                print(f"compiler output {index}", file=stream)
        print("Connection reset during compiler fixture", file=sys.stderr)
        sys.exit(71)

if tool == "sleep":
    sys.exit(0)
if tool == "nm":
    if Path(args[0]).read_bytes().endswith(b"C"):
        print("000001 T comet_contrib_delta")
    sys.exit(0)
if tool == "cargo":
    if args[0] == "tree":
        print("datafusion-comet 0.1.0")
        print("Downloading delta_kernel registry metadata fixture", file=sys.stderr)
        if "--features" in args:
            print("comet-contrib-delta 0.1.0")
    elif args[0] == "build":
        contrib = "--features" in args
        compile_stage("cargo-build-contrib" if contrib else "cargo-build-default")
        lib = root / "native/target/debug/libcomet.so"
        lib.parent.mkdir(parents=True, exist_ok=True)
        lib.write_bytes(b"N" * 100 + (b"C" * 100 if contrib else b""))
    sys.exit(0)

assert tool == "mvnw", tool
if "help:effective-pom" in args:
    profile = next(arg[2:] for arg in args if arg.startswith("-P"))
    attempt = 1 + sum(call[0] == "mvnw" and "-P" + profile in call for call in calls)
    if scenario == "permanent:" + profile:
        print("Permanent Maven model failure: " + profile, file=sys.stderr)
        sys.exit(73)
    if profile == "spark-4.1" and (scenario == "exhausted" or (scenario == "recover" and attempt <= 2)):
        print(f"Could not transfer artifact: Connection reset attempt {attempt}", file=sys.stderr)
        sys.exit(74)
    pom = Path(next(arg[len("-Doutput="):] for arg in args if arg.startswith("-Doutput=")))
    lines = ["<project>", "<dependencyManagement>", "</dependencyManagement>", "<dependencies>",
             "<dependency>", "<groupId>org.apache.spark</groupId>", "</dependency>"]
    if "contrib-delta" in profile:
        version = {"spark-4.1": "4.1.0", "spark-3.5": "3.3.2", "spark-4.0": "4.0.0"}[profile.split(",")[0]]
        lines += ["<dependency>", "<groupId>io.delta</groupId>", "<artifactId>delta-spark_2.13</artifactId>",
                  f"<version>{version}</version>", "</dependency>"]
    lines += ["</dependencies>", "<profiles>", "<groupId>io.delta</groupId>", "</profiles>", "</project>"]
    pom.write_text("\n".join(lines) + "\n")
    print("effective-pom success: " + profile)
else:
    assert "test-compile" in args, args
    compile_stage("maven-test-compile")
    classes = root / "spark/target/classes"
    classes.mkdir(parents=True, exist_ok=True)
    if scenario == "leaked-class":
        leaked = classes / "org/apache/comet/contrib/Delta.class"
        leaked.parent.mkdir(parents=True, exist_ok=True)
        leaked.touch()
'''


class DeltaGateTest(unittest.TestCase):
    def run_gate(self, scenario="success"):
        with tempfile.TemporaryDirectory(prefix="comet-delta-gate-test-") as temp:
            root = Path(temp)
            (root / "dev/ci").mkdir(parents=True)
            (root / "native").mkdir()
            (root / "bin").mkdir()
            for script in ("dev/verify-contrib-delta-gate.sh", "dev/ci/retry-download.sh"):
                shutil.copy2(REPO / script, root / script)
            for tool in ("mvnw", "cargo", "nm", "sleep"):
                path = root / ("mvnw" if tool == "mvnw" else "bin/" + tool)
                path.write_text(f"#!{sys.executable}\n" + FAKE_TOOL)
                path.chmod(0o755)
            env = dict(
                os.environ,
                PATH=f"{root / 'bin'}{os.pathsep}{os.environ['PATH']}",
                COMET_TEST_GATE_ROOT=str(root),
                COMET_TEST_GATE_SCENARIO=scenario,
                COMET_DELTA_GATE_LOG_DIR=str(root / "logs"),
            )
            result = subprocess.run(
                ["bash", str(root / "dev/verify-contrib-delta-gate.sh")],
                cwd=root, env=env, capture_output=True, text=True, timeout=20,
            )
            calls = [json.loads(line) for line in (root / "calls.jsonl").read_text().splitlines()]
            logs = {path.name: path.read_text() for path in (root / "logs").glob("*.log")}
            return result, calls, logs

    def test_normal_gate_keeps_download_stderr_out_of_dependency_tree(self):
        result, calls, logs = self.run_gate()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("All gate checks passed", result.stdout)
        self.assertIn("delta_kernel", logs["cargo-tree-default.stderr.log"])
        self.assertFalse(any(call[0] == "sleep" for call in calls))

    def test_permanent_model_failures_keep_each_profile_status(self):
        for profile in PROFILES:
            with self.subTest(profile=profile):
                result, calls, _ = self.run_gate("permanent:" + profile)
                self.assertEqual(result.returncode, 73, result.stdout + result.stderr)
                self.assertIn("Permanent Maven model failure: " + profile, result.stderr)
                self.assertEqual(sum("help:effective-pom" in call and "-P" + profile in call for call in calls), 1)
                self.assertFalse(any(call[0] == "sleep" or "test-compile" in call for call in calls))

    def test_transient_model_download_recovers(self):
        result, calls, logs = self.run_gate("recover")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        model_calls = [call for call in calls if "help:effective-pom" in call]
        self.assertEqual(sum("-Pspark-4.1" in call for call in model_calls), 3)
        self.assertTrue(all("-U" in call for call in model_calls))
        self.assertEqual(sum(call[0] == "sleep" for call in calls), 2)
        self.assertIn("All gate checks passed", result.stdout)
        log = logs["maven-default-spark-4.1.stdout.log"]
        for message in ("attempt 1", "attempt 2", "effective-pom success"):
            self.assertIn(message, log)

    def test_transient_model_download_exhaustion_stays_fatal(self):
        result, calls, logs = self.run_gate("exhausted")
        self.assertEqual(result.returncode, 74, result.stdout + result.stderr)
        self.assertEqual(sum("help:effective-pom" in call for call in calls), 4)
        self.assertEqual(sum(call[0] == "sleep" for call in calls), 3)
        self.assertFalse(any("test-compile" in call for call in calls))
        self.assertIn("failed after 4 attempts", result.stderr)
        self.assertIn("attempt 1", logs["maven-default-spark-4.1.stdout.log"])
        self.assertIn("attempt 4", logs["maven-default-spark-4.1.stdout.log"])

    def test_compilation_is_not_retried_and_retains_complete_logs(self):
        for stage in ("maven-test-compile", "cargo-build-default", "cargo-build-contrib"):
            with self.subTest(stage=stage):
                result, calls, logs = self.run_gate(stage)
                self.assertEqual(result.returncode, 71, result.stdout + result.stderr)
                self.assertFalse(any(call[0] == "sleep" for call in calls))
                if stage == "maven-test-compile":
                    attempts = [call for call in calls if "test-compile" in call]
                else:
                    attempts = [call for call in calls if call[:2] == ["cargo", "build"]
                                and ("--features" in call) == (stage == "cargo-build-contrib")]
                self.assertEqual(len(attempts), 1)
                for stream in ("stdout", "stderr"):
                    expected = [f"compiler output {index}" for index in range(100)]
                    if stream == "stderr":
                        expected.append("Connection reset during compiler fixture")
                    self.assertEqual(logs[f"{stage}.{stream}.log"].splitlines(), expected)
                self.assertIn("Connection reset during compiler fixture", result.stderr)
                self.assertNotIn("compiler output 0\n", result.stdout + result.stderr)  # Bounded console tail.

    def test_delta_class_leak_still_fails_gate(self):
        result, calls, _ = self.run_gate("leaked-class")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("default Maven build compiled contrib classes", result.stdout)
        self.assertFalse(any(call[:2] == ["cargo", "build"] for call in calls))


if __name__ == "__main__":
    unittest.main()
