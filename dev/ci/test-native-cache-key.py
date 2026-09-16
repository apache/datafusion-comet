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

"""Check cache invalidation boundaries without installing native build tools."""

import importlib.util
import io
import os
from pathlib import Path
import subprocess
import shutil
import tempfile
import unittest
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location("native_cache_key", Path(__file__).with_name("native-cache-key.py"))
CACHE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CACHE)


class NativeCacheKeyTests(unittest.TestCase):
    """Exercise tracked inputs and mocked toolchains in disposable Git repos."""

    def setUp(self):
        """Create tracked build inputs and fake tools/JDK owned by this test.

        Git inventory is real; tool version queries alone are mocked. Cleanup
        removes the entire temporary tree even when an assertion fails.
        """
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.root = self.directory / "repo"
        self.root.mkdir()
        self.git("init", "--quiet")
        self.inputs = {
            "native/Cargo.toml": "[workspace]\nmembers = []\n",
            "native/Cargo.lock": "version = 4\n",
            "native/core/src/lib.rs": "pub fn value() -> i32 { 1 }\n",
            "native/proto/src/proto/expr.proto": 'syntax = "proto3";\n',
            "native/proto/build.rs": "fn main() {}\n",
            "contrib/lance/native/Cargo.toml": "[package]\nname = 'lance'\n",
            "common/src/main/java/Native.java": "class Native {}\n",
            "pom.xml": "<project/>\n",
            "rust-toolchain.toml": "[toolchain]\nchannel = 'stable'\n",
            ".github/actions/build-native-ci/action.yaml": "runs: {}\n",
            "dev/ci/native-cache-key.py": "# identity implementation\n",
            "spark/src/main/scala/Plan.scala": "object Plan {}\n",
            "README.md": "# Comet\n",
        }
        for name, content in self.inputs.items():
            self.write(self.root / name, content)
        self.git("add", ".")
        self.java_home = self.directory / "jdk"
        self.write(self.java_home / "release", 'JAVA_VERSION="17.0.1"\n')
        self.write(self.java_home / "lib/server/libjvm.so", "fake JVM library")
        self.write(self.java_home / "include/jni.h", "fake JNI headers")
        self.cargo_home = self.directory / "cargo"
        self.cargo_home.mkdir()
        locations = CACHE.cargo_config_directories
        config_patch = patch.object(CACHE, "cargo_config_directories",
                                    side_effect=lambda root, home: [path for path in locations(root, home)
                                                                   if path.is_relative_to(self.directory)])
        config_patch.start()
        self.addCleanup(config_patch.stop)
        self.tool_path = self.directory / "tools"
        self.rust_tool_path = self.directory / "toolchain/bin"
        for tool in CACHE.TOOLS:
            self.write(self.tool_path / tool, "fake tool binary " + tool)
        for tool in ("rustc", "cargo", "rustfmt"):
            self.write(self.rust_tool_path / tool, "fake resolved tool binary " + tool)
        self.env = {
            "HOME": str(self.directory / "home"), "PATH": os.environ["PATH"],
            "JAVA_HOME": str(self.java_home), "CARGO_HOME": str(self.cargo_home),
            "RUSTFLAGS": "-Ctarget-cpu=x86-64-v3 -Clink-arg=-fuse-ld=bfd",
        }
        self.versions = {name: f"{name} version 1\n".encode() for name in CACHE.TOOLS}
        self.versions.update({"uname -s": b"Linux\n", "uname -m": b"x86_64\n",
                              "dpkg-query": b"libc6\t1.0\tamd64\n"})

    def git(self, *args):
        """Run Git against the test repository; setup errors fail the test."""
        return subprocess.run(["git", *args], cwd=self.root, check=True,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def write(self, path, content):
        """Write fixture text, creating parents; mutation stays inside the temp tree."""
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    def fake_command(self, args, cwd, env):
        """Return fixture output for a required tool or fail on an unexpected query."""
        if args[:2] == ["rustup", "which"]:
            return str(self.rust_tool_path / args[2]).encode()
        name = " ".join(args) if args[0] == "uname" else args[0]
        return self.versions[name]

    def keys(self, profile="ci"):
        """Snapshot real files with a fake Linux toolchain and return output keys."""
        dependencies, sources = CACHE.tracked_inputs(self.root, dict(os.environ))
        with patch.object(CACHE, "command", side_effect=self.fake_command), \
                patch.object(CACHE.shutil, "which", side_effect=lambda name, path: str(self.tool_path / name)):
            cargo_home, environment = CACHE.environment_identity(self.root, profile, self.env)
        return CACHE.cache_keys(profile, dependencies, sources, environment, cargo_home)

    def test_native_and_build_inputs_invalidate_exact_keys(self):
        """Rust/proto/JNI/build edits invalidate binaries while preserving dependency reuse."""
        before = self.keys()
        for name in ("native/core/src/lib.rs", "native/proto/src/proto/expr.proto",
                     "native/proto/build.rs", "common/src/main/java/Native.java", "pom.xml",
                     "rust-toolchain.toml", ".github/actions/build-native-ci/action.yaml",
                     "dev/ci/native-cache-key.py"):
            with self.subTest(name=name):
                self.write(self.root / name, self.inputs[name] + "\n# changed\n")
                after = self.keys()
                self.assertNotEqual(before["source-key"], after["source-key"])
                self.assertNotEqual(before["binary-key"], after["binary-key"])
                self.assertEqual(before["restore-prefix"], after["restore-prefix"])
                self.write(self.root / name, self.inputs[name])

    def test_dependency_edits_invalidate_incremental_prefix(self):
        """Manifest/lockfile changes isolate both source and dependency caches."""
        before = self.keys()
        for name in ("native/Cargo.toml", "native/Cargo.lock", "contrib/lance/native/Cargo.toml"):
            with self.subTest(name=name):
                self.write(self.root / name, self.inputs[name] + "\n# changed\n")
                after = self.keys()
                self.assertNotEqual(before["restore-prefix"], after["restore-prefix"])
                self.assertNotEqual(before["binary-key"], after["binary-key"])
                self.write(self.root / name, self.inputs[name])

    def test_unrelated_jvm_and_untracked_generated_files_do_not_invalidate(self):
        """Spark/docs edits and generated protobuf/target files preserve reuse."""
        before = self.keys()
        self.write(self.root / "spark/src/main/scala/Plan.scala", "object NewPlan {}\n")
        self.write(self.root / "README.md", "new docs\n")
        self.write(self.root / "native/proto/src/generated/expr.rs", "generated Rust")
        self.write(self.root / "native/target/ci/libcomet.so", "built artifact")
        self.assertEqual(before, self.keys())

    def test_tracked_addition_deletion_and_executable_mode(self):
        """New/deleted native files and mode changes cannot keep an exact hit."""
        before = self.keys()
        new = self.root / "native/core/src/new.rs"
        self.write(new, "new tracked source")
        self.git("add", "native/core/src/new.rs")
        self.assertNotEqual(before["binary-key"], self.keys()["binary-key"])
        self.git("rm", "--cached", "native/core/src/new.rs")
        source = self.root / "native/core/src/lib.rs"
        source.chmod(0o755)
        self.assertNotEqual(before["binary-key"], self.keys()["binary-key"])
        source.chmod(0o644)
        source.unlink()
        with self.assertRaises(FileNotFoundError):
            self.keys()
        self.git("rm", "--cached", "native/core/src/lib.rs")
        self.assertNotEqual(before["binary-key"], self.keys()["binary-key"])

    def test_tool_jdk_and_platform_changes_invalidate_all_caches(self):
        """Tool/package versions, JVM bytes and JVM paths enter the environment key."""
        before = self.keys()
        for tool in self.versions:
            if tool.startswith("uname"):
                continue
            with self.subTest(tool=tool):
                old = self.versions[tool]
                self.versions[tool] += b"changed version\n"
                after = self.keys()
                self.assertNotEqual(before["restore-prefix"], after["restore-prefix"])
                self.assertNotEqual(before["binary-key"], after["binary-key"])
                self.versions[tool] = old
        for name in ("release", "lib/server/libjvm.so", "include/jni.h"):
            with self.subTest(jdk_input=name):
                path = self.java_home / name
                old = path.read_text()
                path.write_text(old + "changed\n")
                self.assertNotEqual(before["binary-key"], self.keys()["binary-key"])
                path.write_text(old)
        new_java_home = self.directory / "other-jdk"
        shutil.copytree(self.java_home, new_java_home)
        self.env["JAVA_HOME"] = str(new_java_home)
        self.assertNotEqual(before["binary-key"], self.keys()["binary-key"])
        self.env["JAVA_HOME"] = str(self.java_home)
        (self.tool_path / "cc").write_text("same version, different executable")
        self.assertNotEqual(before["binary-key"], self.keys()["binary-key"])
        self.versions["uname -m"] = b"aarch64\n"
        with self.assertRaisesRegex(ValueError, "Linux x86_64 only"):
            self.keys()

    def test_external_and_ancestor_cargo_configs_invalidate(self):
        """Cargo home and ancestor configs matter even though Git cannot list them."""
        before = self.keys()
        for directory in (self.cargo_home, self.directory / ".cargo", self.root / ".cargo"):
            with self.subTest(directory=directory):
                config = directory / "config.toml"
                self.write(config, "[build]\nincremental = false\n")
                after = self.keys()
                self.assertNotEqual(before["restore-prefix"], after["restore-prefix"])
                self.assertNotEqual(before["binary-key"], after["binary-key"])
                config.unlink()

    def test_unsupported_config_and_tool_overrides_fail_closed(self):
        """Unknown external build inputs cannot produce an apparently safe cache key."""
        for variable in ("RUSTC_WRAPPER", "CC", "PROTOC", "HDFS_LIB_DIR", "DOCS_RS",
                         "OPENSSL_LIB_DIR", "CARGO_BUILD_RUSTC_WRAPPER",
                         "CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER"):
            with self.subTest(variable=variable):
                self.env[variable] = "/untracked/tool"
                with self.assertRaisesRegex(ValueError, "unsupported build override"):
                    self.keys()
                del self.env[variable]
        config = self.cargo_home / "config.toml"
        self.write(config, "[build]\nrustc-wrapper = '/untracked/tool'\n")
        with self.assertRaisesRegex(ValueError, "unsupported Cargo build"):
            self.keys()
        self.write(config, "include = ['extra.toml']\n")
        with self.assertRaisesRegex(ValueError, "unsupported Cargo config"):
            self.keys()

    def test_flags_are_pinned_and_other_build_environment_is_hashed(self):
        """Only fixed compiler flags are reusable; safe Cargo build settings are hashed."""
        before = self.keys()
        self.env["CARGO_BUILD_JOBS"] = "2"
        self.assertNotEqual(before["binary-key"], self.keys()["binary-key"])
        self.env["RUSTFLAGS"] = "-Ctarget-cpu=native -Clink-arg=-fuse-ld=bfd"
        with self.assertRaisesRegex(ValueError, "fixed x86-64-v3"):
            self.keys()
        self.env["RUSTFLAGS"] = "-Clink-arg=-fuse-ld=bfd"
        debug = self.keys("debug")
        self.assertEqual(debug["binary-key"], "")
        self.assertNotEqual(before["source-key"], debug["source-key"])
        self.env["RUSTFLAGS"] += " -Clinker=/untracked/linker"
        with self.assertRaisesRegex(ValueError, "fixed bfd"):
            self.keys("debug")

    def test_external_compiler_and_library_inputs_are_rejected(self):
        """Untracked header/library changes cannot hide behind unchanged override strings."""
        for name, value in {
            "CFLAGS": "-include /tmp/header.h", "CXXFLAGS": "-I/tmp/include",
            "CPPFLAGS": "-I/tmp/include", "LDFLAGS": "-L/tmp/lib",
            "LIBRARY_PATH": "/tmp/lib", "CPATH": "/tmp/include",
            "C_INCLUDE_PATH": "/tmp/include", "CPLUS_INCLUDE_PATH": "/tmp/include",
            "LD_LIBRARY_PATH": "/tmp/lib", "LD_PRELOAD": "/tmp/lib/injected.so",
            "CARGO_BUILD_RUSTC": "/tmp/rustc", "CARGO_BUILD_RUSTFLAGS": "-Clinker=/tmp/ld",
            "CARGO_BUILD_TARGET": "/tmp/target.json", "CARGO_BUILD_FUTURE_OVERRIDE": "anything",
            "TARGET_CC": "/tmp/compiler", "HOST_CFLAGS": "-include /tmp/header.h",
            "CMAKE_TOOLCHAIN_FILE": "/tmp/toolchain.cmake",
        }.items():
            with self.subTest(name=name):
                self.env[name] = value
                with self.assertRaisesRegex(ValueError, "unsupported build override"):
                    self.keys()
                del self.env[name]
        self.env["LD_LIBRARY_PATH"] = str(self.java_home / "lib/server")
        before = self.keys()
        (self.java_home / "lib/server/libjvm.so").write_text("changed linked JVM")
        self.assertNotEqual(before["binary-key"], self.keys()["binary-key"])

    def test_rustup_proxies_hash_the_resolved_compiler(self):
        """A changed real Rust tool invalidates the key even with unchanged proxy/version."""
        before = self.keys()
        for tool in ("rustc", "cargo", "rustfmt"):
            with self.subTest(tool=tool):
                binary = self.rust_tool_path / tool
                old = binary.read_text()
                binary.write_text("changed underlying tool with the same version")
                self.assertNotEqual(before["binary-key"], self.keys()["binary-key"])
                binary.write_text(old)
        (self.rust_tool_path / "rustc").unlink()
        with self.assertRaises(FileNotFoundError):
            self.keys()

    def test_cargo_home_fallback_and_output_values(self):
        """Use effective CARGO_HOME, or HOME/.cargo, and publish bounded opaque keys."""
        outputs = self.keys()
        self.assertEqual(outputs["cargo-home"], str(self.cargo_home))
        self.assertTrue(outputs["source-key"].startswith(outputs["restore-prefix"]))
        del self.env["CARGO_HOME"]
        fallback = self.keys()
        self.assertEqual(fallback["cargo-home"], str(Path(self.env["HOME"]) / ".cargo"))
        for name, value in fallback.items():
            self.assertNotIn("\n", value)
            self.assertLess(len(value), 512)
            if name != "cargo-home":
                self.assertNotIn("JAVA_HOME", value)

    def test_cli_publishes_outputs_only_after_complete_snapshot(self):
        """Failed fingerprinting preserves GitHub outputs; success emits opaque keys."""
        output_file = self.directory / "github-output"
        output_file.write_text("previous=value\n")
        arguments = ["native-cache-key.py", "--profile", "ci", "--github-output", str(output_file)]
        with patch.object(CACHE.sys, "argv", arguments), \
                patch.object(CACHE, "command", return_value=str(self.root).encode()), \
                patch.object(CACHE, "tracked_inputs", return_value=({}, {})), \
                patch.object(CACHE, "environment_identity", side_effect=ValueError("missing tool")), \
                patch.object(CACHE.sys, "stdout", new_callable=io.StringIO) as stdout, \
                patch.object(CACHE.sys, "stderr", new_callable=io.StringIO):
            self.assertEqual(CACHE.main(), 1)
            self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(output_file.read_text(), "previous=value\n")
        with patch.object(CACHE.sys, "argv", arguments), \
                patch.object(CACHE, "command", return_value=str(self.root).encode()), \
                patch.object(CACHE, "tracked_inputs", return_value=({}, {})), \
                patch.object(CACHE, "environment_identity", return_value=(self.cargo_home, {})), \
                patch.object(CACHE.sys, "stdout", new_callable=io.StringIO) as stdout:
            self.assertEqual(CACHE.main(), 0)
        self.assertEqual(output_file.read_text(), "previous=value\n" + stdout.getvalue())
        self.assertIn("binary-key=Linux-native-ci-v1-", stdout.getvalue())

    def test_missing_tool_and_jvm_fail_closed(self):
        """Essential fingerprint failures never fall back to a partial identity."""
        with patch.object(CACHE.shutil, "which", return_value=None):
            with self.assertRaisesRegex(ValueError, "missing required tool"):
                CACHE.environment_identity(self.root, "ci", self.env)
        (self.java_home / "lib/server/libjvm.so").unlink()
        with self.assertRaises(FileNotFoundError):
            self.keys()


if __name__ == "__main__":
    unittest.main()
