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
                       "native/proto/src/proto/expr.proto": "message Expr {}\n",
                       "native/core/README.md": "Native documentation\n",
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

    def depinfo(self, *names):
        """Write the absolute, space-escaped paths emitted by Cargo's top-level .d file."""
        path = self.root / "native/target/ci/libcomet.d"
        prerequisites = " ".join(str(self.root / name).replace(" ", "\\ ") for name in names)
        self.write(str(path.relative_to(self.root)), f"{path.with_suffix('.so')}: {prerequisites}\n")
        return path

    def test_source_and_dependency_changes_invalidate_the_right_keys(self):
        """Native/protobuf edits retain the dependency prefix; dependency edits replace it."""
        before = self.keys()
        for name in ("native/lib.rs", "native/proto/expr.proto", "native/Cargo.toml", "native/Cargo.lock",
                     "contrib/delta/native/Cargo.toml",
                     ".github/actions/build-native-ci/action.yaml", ".github/actions/setup-builder/action.yaml"):
            with self.subTest(name=name):
                self.write(name, self.inputs[name] + "changed\n")
                after = self.keys()
                self.assertNotEqual(before["cargo-key"], after["cargo-key"])
                self.assertNotEqual(before["library-key"], after["library-key"])
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
        self.assertNotEqual(debug["cargo-key"], self.keys("debug")["cargo-key"])

    def test_input_rules_change_keys_but_unrelated_routing_does_not(self):
        """Fingerprint the imported pattern/matcher contract, not the entire routing file."""
        before = self.keys()
        self.write("dev/ci/compute-changes.py", "# changed workflow policy and docs\n")
        self.assertEqual(before, self.keys())
        with patch.dict(CACHE.CHANGES.POLICY, {"build_linux": ["push"]}):
            self.assertEqual(before, self.keys())
        with patch.object(CACHE.CHANGES, "NATIVE_LIBRARY_INPUTS",
                          (*CACHE.CHANGES.NATIVE_LIBRARY_INPUTS, "extra-native/**")):
            changed_patterns = self.keys()
        self.assertNotEqual(before["library-key"], changed_patterns["library-key"])
        self.assertNotEqual(before["cargo-key"], changed_patterns["cargo-key"])
        self.assertEqual(before["restore-prefix"], changed_patterns["restore-prefix"])
        original = CACHE.CHANGES.compile_matcher

        def changed_matcher(patterns):
            return original(patterns)

        with patch.object(CACHE.CHANGES, "compile_matcher", changed_matcher):
            self.assertNotEqual(before["library-key"], self.keys()["library-key"])
        original_glob = CACHE.CHANGES.glob_to_regex

        def changed_glob(pattern):
            return original_glob(pattern)

        with patch.object(CACHE.CHANGES, "glob_to_regex", changed_glob):
            self.assertNotEqual(before["library-key"], self.keys()["library-key"])

    def test_depinfo_accepts_tracked_sources_generated_protobuf_and_jdk(self):
        """A completed native build may consume only fingerprinted or explicit derived inputs."""
        for name in CACHE.GENERATED_PROTO_FILES:
            self.write(name, "generated protobuf Rust\n")
        (self.root / "jdk/lib/server").mkdir(parents=True)
        path = self.depinfo("native/lib.rs", "native/proto/src/proto",
                            *sorted(CACHE.GENERATED_PROTO_FILES), "jdk/lib/server")
        CACHE.check_depinfo(self.root, path, self.env)

    def test_depinfo_rejects_excluded_or_untracked_sources(self):
        """Broad native globs must not admit files absent from the tracked fingerprint."""
        self.write("native/untracked.rs", "not tracked\n")
        self.write("native/proto/src/generated/extra.rs", "not a declared generated module\n")
        for name in ("native/core/README.md", "native/core/benches/perf.rs", "native/untracked.rs",
                     "contrib/delta/native/src/lib.rs", "native/proto/src/generated/extra.rs"):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "missing from"):
                CACHE.check_depinfo(self.root, self.depinfo("native/lib.rs", name), self.env)

    def test_depinfo_directory_requires_covered_descendants(self):
        """rerun-if-changed directories cannot hide ignored or untracked input files."""
        path = self.depinfo("native/proto/src/proto")
        CACHE.check_depinfo(self.root, path, self.env)
        for name in ("native/proto/src/proto/extra.proto", "native/proto/src/proto/notes.md"):
            with self.subTest(name=name):
                self.write(name, "unrepresented input\n")
                with self.assertRaisesRegex(ValueError, "missing from"):
                    CACHE.check_depinfo(self.root, path, self.env)
                (self.root / name).unlink()

    def test_depinfo_preserves_cargo_path_escaping(self):
        """Cargo escapes spaces only; #, $, colons and ordinary backslashes stay literal."""
        names = ("native/space name.rs", "native/hash#dollar$$colon:back\\slash.rs",
                 "native/back\\ space.rs")
        for name in names:
            self.write(name, "tracked source\n")
        subprocess.run(["git", "add", "native"], cwd=self.root, check=True)
        path = self.depinfo(*names)
        self.assertEqual(CACHE.depinfo_inputs(path.read_text()), [str(self.root / name) for name in names])
        CACHE.check_depinfo(self.root, path, self.env)

    def test_depinfo_rejects_paths_outside_the_checkout_and_generated_symlinks(self):
        """Canonical paths must remain inside their fingerprinted or explicit allowed location."""
        self.write("uncovered.rs", "outside native inputs\n")
        generated = self.root / "native/proto/src/generated/spark.spark_config.rs"
        generated.parent.mkdir(parents=True, exist_ok=True)
        generated.symlink_to(self.root / "uncovered.rs")
        external = tempfile.TemporaryDirectory()
        self.addCleanup(external.cleanup)
        outside = Path(external.name) / "external.rs"
        outside.write_text("outside checkout\n")
        linked = self.root / "native/linked.rs"
        linked.symlink_to(outside)
        subprocess.run(["git", "add", "native/linked.rs"], cwd=self.root, check=True)
        directory_link = self.root / "native/proto/src/proto/linked"
        directory_link.symlink_to(Path(external.name), target_is_directory=True)
        for name in (str(outside), str(generated), str(linked), "native/../uncovered.rs",
                     "native/proto/src/proto"):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "missing from"):
                CACHE.check_depinfo(self.root, self.depinfo(name), self.env)

    def test_depinfo_rejects_missing_relative_and_malformed_inputs(self):
        """Unknown formats and paths fail closed instead of producing a partial coverage check."""
        for text in ("", "target:\n", ": input\n", "missing separator\n",
                     "target: input\nsecond: input\n", "target: bad\0path\n"):
            with self.subTest(text=text), self.assertRaises(ValueError):
                CACHE.depinfo_inputs(text)
        path = self.depinfo("native/does-not-exist.rs")
        with self.assertRaisesRegex(ValueError, "missing or unresolvable"):
            CACHE.check_depinfo(self.root, path, self.env)
        path.write_text("libcomet.so: native/lib.rs\n")
        with self.assertRaisesRegex(ValueError, "relative dep-info paths are unsupported"):
            CACHE.check_depinfo(self.root, path, self.env)

    def test_depinfo_cli_works_from_root_and_native_and_fails_on_uncovered_input(self):
        """The action may validate from its Cargo cwd without re-snapshotting installed tools."""
        path = self.depinfo("native/lib.rs")
        for cwd in (self.root, self.root / "native"):
            with self.subTest(cwd=cwd):
                result = subprocess.run(
                    [sys.executable, CACHE.__file__, "--check-depinfo", str(path.relative_to(cwd))],
                    cwd=cwd, env={**os.environ, **self.env}, text=True, capture_output=True)
                self.assertEqual(result.returncode, 0, result.stderr)
        self.depinfo("native/core/README.md")
        result = subprocess.run([sys.executable, CACHE.__file__, "--check-depinfo", str(path)],
                                cwd=self.root, env={**os.environ, **self.env}, text=True, capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("native/core/README.md", result.stderr)

    def test_tools_jdk_flags_and_tracked_build_configuration_invalidate(self):
        """Capture build overrides only; tools, Java metadata and tracked configs invalidate keys."""
        before = self.keys()
        for tool in self.versions:
            with self.subTest(tool=tool):
                old = self.versions[tool]
                self.versions[tool] += "changed\n"
                self.assertNotEqual(before["library-key"], self.keys()["library-key"])
                self.versions[tool] = old
        self.write("jdk/release", 'JAVA_VERSION="17.0.2"\n')
        self.assertNotEqual(before["library-key"], self.keys()["library-key"])
        self.write("jdk/release", 'JAVA_VERSION="17.0.1"\n')
        self.env["RUSTFLAGS"] += " -Copt-level=1"
        self.assertNotEqual(before["library-key"], self.keys()["library-key"])
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
        for key in ("library-key", "cargo-key", "restore-prefix"):
            self.assertNotEqual(before[key], after[key])
        del self.env["TARGET_CFLAGS"]
        self.write(".cargo/config.toml", "[build]\nincremental = false\n")
        subprocess.run(["git", "add", ".cargo/config.toml"], cwd=self.root, check=True)
        self.assertNotEqual(before["library-key"], self.keys()["library-key"])

    def test_profiles_have_separate_cargo_caches(self):
        """CI/debug keys stay separate and only CI produces a reusable library key."""
        ci, debug = self.keys("ci"), self.keys("debug")
        self.assertNotEqual(ci["cargo-key"], debug["cargo-key"])
        self.assertNotEqual(ci["restore-prefix"], debug["restore-prefix"])
        self.assertNotIn("library-key", debug)
        self.assertTrue(ci["library-key"].startswith("Linux-native-ci-"))
        self.assertTrue(ci["cargo-key"].startswith(ci["restore-prefix"]))

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
                    patch.object(CACHE, "environment_inputs", return_value={}), \
                    patch.object(sys, "stdout", new_callable=io.StringIO):
                CACHE.main()
        self.assertIn("cargo-key=Linux-cargo-ci-", output.read_text())
        self.assertEqual(config.read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
