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

"""Check native cache boundaries and container checkout ownership with real Git."""

import importlib.util
import io
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location("native_cache_key", Path(__file__).with_name("native-cache-key.py"))
CACHE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CACHE)


class NativeCacheKeyTests(unittest.TestCase):
    """Use disposable Git repositories and mock only installed tool versions."""

    def setUp(self):
        """Create tracked native/JVM fixtures and JDK metadata; clean up after each test."""
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        subprocess.run(["git", "init", "--quiet", str(self.root)], check=True)
        self.inputs = {"native/Cargo.toml": "[workspace]\n", "native/Cargo.lock": "version = 4\n",
                       "native/lib.rs": "fn native() {}\n", "native/proto/expr.proto": "message Expr {}\n",
                       "spark/Plan.scala": "object Plan {}\n", "README.md": "Comet\n",
                       ".github/workflows/README.md": "CI documentation\n",
                       ".github/workflows/pr_build_linux.yml": "jobs: {}\n",
                       ".github/workflows/spark_sql_test_reusable.yml": "jobs: {}\n",
                       ".github/workflows/iceberg_spark_test_reusable.yml": "jobs: {}\n",
                       ".github/workflows/spark_sql_writer_tests.yml": "jobs: {}\n",
                       ".github/workflows/check_pr_title.yml": "jobs: {}\n",
                       ".github/actions/build-native-ci/action.yaml": "runs: {}\n",
                       ".github/actions/setup-builder/action.yaml": "runs: {}\n",
                       "dev/ci/compute-changes.py": "# shared native input rules\n",
                       "contrib/delta/native/Cargo.toml": '[package]\nname = "delta"\n',
                       "contrib/delta/native/src/lib.rs": "fn delta() {}\n",
                       "contrib/delta/native/Cargo.lock": "version = 4\n",
                       "contrib/a/b/native/Cargo.toml": '[package]\nname = "nested"\n',
                       "native/core/benches/perf.rs": "fn benchmark() {}\n"}
        for name, content in self.inputs.items():
            self.write(name, content)
        subprocess.run(["git", "add", "."], cwd=self.root, check=True)
        self.write("jdk/release", 'JAVA_VERSION="17.0.1"\n')
        self.env = {"JAVA_HOME": str(self.root / "jdk"), "CARGO_HOME": str(self.root / "cargo"),
                    "RUSTFLAGS": "-Ctarget-cpu=x86-64-v3 -Clink-arg=-fuse-ld=bfd"}
        self.versions = {"rustc": "rustc 1.90\nhost: x86_64-unknown-linux-gnu\n",
                         "cargo": "cargo 1.90\n", "rustfmt": "rustfmt 1.8\n",
                         "dpkg-query": "libc6\t2.40\tamd64\n", "uname": "x86_64\n"}

    def write(self, name, content):
        """Write fixture text under the temporary repository, creating its parents."""
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)

    def keys(self, profile="ci"):
        """Return keys from real tracked files and deterministic tool version responses."""
        dependencies, sources = CACHE.source_inputs(self.root, profile)
        with patch.object(CACHE, "command", side_effect=lambda args, cwd: self.versions[args[0]]):
            environment = CACHE.environment_inputs(self.root, self.env)
        return CACHE.cache_keys(profile, dependencies, sources, environment)

    def test_source_and_dependency_changes_invalidate_the_right_keys(self):
        """Native/protobuf edits retain the dependency prefix; dependency edits replace it."""
        before = self.keys()
        for name in ("native/lib.rs", "native/proto/expr.proto", "native/Cargo.toml", "native/Cargo.lock",
                     "contrib/delta/native/Cargo.toml", "dev/ci/compute-changes.py",
                     ".github/actions/build-native-ci/action.yaml", ".github/actions/setup-builder/action.yaml"):
            with self.subTest(name=name):
                self.write(name, self.inputs[name] + "changed\n")
                after = self.keys()
                self.assertNotEqual(before["source-key"], after["source-key"])
                self.assertNotEqual(before["binary-key"], after["binary-key"])
                if name.endswith(("Cargo.toml", "Cargo.lock")):
                    self.assertNotEqual(before["restore-prefix"], after["restore-prefix"])
                else:
                    self.assertEqual(before["restore-prefix"], after["restore-prefix"])
                self.write(name, self.inputs[name])

    def test_generated_files_and_unrelated_jvm_edits_preserve_keys(self):
        """Generated files and non-build edits preserve reuse; debug still tracks benchmarks."""
        before = self.keys()
        debug = self.keys("debug")
        for name in self.inputs:
            if name.startswith(".github/workflows/"):
                self.write(name, "unrelated test configuration\n")
        self.env["GITHUB_RUN_ID"] = "12345"
        self.assertEqual(before, self.keys())
        self.assertEqual(debug, self.keys("debug"))
        self.write("native/proto/src/generated/expr.rs", "generated Rust")
        self.write("native/target/ci/libcomet.so", "compiled library")
        self.write("spark/Plan.scala", "object NewPlan {}")
        self.write("README.md", "updated docs")
        self.write("contrib/delta/native/src/lib.rs", "fn changed_delta() {}")
        self.write("contrib/delta/native/Cargo.lock", "version = 3\n")
        self.write("contrib/a/b/native/Cargo.toml", '[package]\nname = "changed_nested"\n')
        self.write("native/core/benches/perf.rs", "fn changed_benchmark() {}")
        self.assertEqual(before, self.keys())
        self.assertNotEqual(debug["source-key"], self.keys("debug")["source-key"])

    def test_native_input_routing(self):
        """Library inputs warm main; helper tests retain Linux coverage without extra consumers."""
        route = CACHE.CHANGES.compute
        inputs = ("native/core/src/lib.rs", "native/proto/expr.proto",
                  "contrib/new/native/Cargo.toml", ".cargo/config.toml",
                  ".github/actions/setup-builder/action.yaml",
                  ".github/actions/build-native-ci/action.yaml",
                  "dev/ci/native-cache-key.py", "dev/ci/compute-changes.py",
                  "rust-toolchain", "rust-toolchain.toml")
        for name in inputs:
            self.assertTrue(CACHE.CHANGES.matches(CACHE.CHANGES.NATIVE_LIBRARY_INPUTS, [name]), name)
            self.assertTrue(route([name], {"name": "push"})["build_linux"], name)
        for name in ("native/core/README.md", "native/core/benches/perf.rs",
                     "contrib/delta/native/src/lib.rs", "contrib/delta/native/Cargo.lock",
                     "contrib/a/b/native/Cargo.toml", "contrib/a/b/native/x.rs"):
            self.assertFalse(CACHE.CHANGES.matches(CACHE.CHANGES.NATIVE_LIBRARY_INPUTS, [name]), name)
            self.assertFalse(route([name], {"name": "push"})["build_linux"], name)
        with patch.dict(CACHE.CHANGES.POLICY, {"build_linux": ["pr", "queue"]}):
            self.assertFalse(route([".cargo/config.toml"], {"name": "push"})["build_linux"])
        self.assertFalse(route(["contrib/new/native/Cargo.toml"], {"name": "pull_request"})["build_linux"])
        for event in ("merge_group", "schedule"):
            routed = route(["dev/ci/test-native-cache-key.py"], {"name": event})
            self.assertTrue(routed["build_linux" if event == "merge_group" else "build_linux_all_profiles"])
            self.assertFalse(any(selected for name, selected in routed.items()
                                 if name.startswith(("spark_", "iceberg_"))))

    def test_tools_jdk_flags_and_tracked_build_configuration_invalidate(self):
        """Capture build overrides only; tools, Java metadata and tracked configs invalidate keys."""
        before = self.keys()
        for tool in self.versions:
            with self.subTest(tool=tool):
                old = self.versions[tool]
                self.versions[tool] += "changed\n"
                self.assertNotEqual(before["binary-key"], self.keys()["binary-key"])
                self.versions[tool] = old
        self.write("jdk/release", 'JAVA_VERSION="17.0.2"\n')
        self.assertNotEqual(before["binary-key"], self.keys()["binary-key"])
        self.write("jdk/release", 'JAVA_VERSION="17.0.1"\n')
        self.env["RUSTFLAGS"] += " -Copt-level=1"
        self.assertNotEqual(before["binary-key"], self.keys()["binary-key"])
        self.env["RUSTFLAGS"] = "-Ctarget-cpu=x86-64-v3 -Clink-arg=-fuse-ld=bfd"
        overrides = ("CC", "CXX", "CFLAGS", "LDFLAGS", "AR", "PROTOC", "PROTOC_INCLUDE",
                     "RUSTC_WRAPPER", "CARGO_BUILD_TARGET", "CARGO_PROFILE_CI_OPT_LEVEL",
                     "CC_x86_64_unknown_linux_gnu", "HOST_CC", "TARGET_CFLAGS",
                     "HDFS_LIB_DIR", "HADOOP_HOME", "HDFS_STATIC", "DOCS_RS", "PATH")
        build_env = {**self.env, **dict.fromkeys(overrides, "build override")}
        with patch.object(CACHE, "command", side_effect=lambda args, cwd: self.versions[args[0]]):
            environment = CACHE.environment_inputs(
                self.root, {**build_env, "GITHUB_RUN_ID": "12345", "UNRELATED": "ignored"})
        self.assertEqual(environment["env"], build_env)
        self.env["TARGET_CFLAGS"] = "build override"
        after = self.keys()
        for key in ("binary-key", "source-key", "restore-prefix"):
            self.assertNotEqual(before[key], after[key])
        del self.env["TARGET_CFLAGS"]
        self.write(".cargo/config.toml", "[build]\nincremental = false\n")
        subprocess.run(["git", "add", ".cargo/config.toml"], cwd=self.root, check=True)
        self.assertNotEqual(before["binary-key"], self.keys()["binary-key"])

    def test_profiles_have_separate_cargo_caches(self):
        """CI/debug keys stay separate and only CI produces a reusable library key."""
        ci, debug = self.keys("ci"), self.keys("debug")
        self.assertNotEqual(ci["source-key"], debug["source-key"])
        self.assertNotEqual(ci["restore-prefix"], debug["restore-prefix"])
        self.assertEqual(debug["binary-key"], "")
        self.assertTrue(ci["binary-key"].startswith("Linux-native-ci-"))
        self.assertTrue(ci["source-key"].startswith(ci["restore-prefix"]))
        self.assertEqual(ci["cargo-home"], self.env["CARGO_HOME"])

    def test_container_ownership_works_without_global_git_config_changes(self):
        """A differently owned checkout permits helper root/inventory reads without global trust."""
        self.write("global.gitconfig", "[user]\n\tname = Cache Test\n")
        config = self.root / "global.gitconfig"
        original = config.read_bytes()
        output = self.root / "github-output"
        environment = {**self.env, "GIT_TEST_ASSUME_DIFFERENT_OWNER": "1",
                       "GIT_CONFIG_GLOBAL": str(config), "GIT_CONFIG_NOSYSTEM": "1"}
        arguments = ["native-cache-key.py", "--profile", "ci", "--github-output", str(output)]
        with patch.dict(os.environ, environment):
            ordinary = subprocess.run(["git", "rev-parse", "--show-toplevel"], cwd=self.root,
                                      text=True, capture_output=True)
            self.assertNotEqual(ordinary.returncode, 0)
            self.assertIn("dubious ownership", ordinary.stderr)
            with patch.object(CACHE.Path, "cwd", return_value=self.root), \
                    patch.object(sys, "argv", arguments), \
                    patch.object(CACHE, "environment_inputs", return_value={"cargo_home": self.env["CARGO_HOME"]}), \
                    patch.object(sys, "stdout", new_callable=io.StringIO):
                CACHE.main()
        self.assertIn("source-key=Linux-cargo-ci-", output.read_text())
        self.assertEqual(config.read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
