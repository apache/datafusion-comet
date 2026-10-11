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

"""Mutation checks for the native builder contract and pre-merge routing."""

import importlib.util
from pathlib import Path
import unittest


SPEC = importlib.util.spec_from_file_location("ci_config", Path(__file__).with_name("check-ci-config.py"))
CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK)


class NativeCiConfigTests(unittest.TestCase):
    def setUp(self):
        root = Path(__file__).resolve().parents[2]
        self.sources = {path.name: path.read_text() for path in (root / ".github/workflows").glob("*.y*ml")}

    def mutate(self, filename, old, new):
        self.assertIn(old, self.sources[filename])
        sources = dict(self.sources)
        sources[filename] = sources[filename].replace(old, new, 1)
        return sources

    def test_current_producers_share_the_contract(self):
        self.assertEqual(CHECK.native_ci_failures(self.sources), [])

    def test_unknown_env_at_each_native_scope_is_rejected(self):
        filename = "spark_sql_test_reusable.yml"
        cases = [
            ("  RUST_BACKTRACE: 1", "  RUST_BACKTRACE: 1\n  AWS_LC_SYS_NO_ASM: 1"),
            ("    name: Build Native + JVM Test Classes", "    name: Build Native + JVM Test Classes\n    env:\n      LZMA_API_STATIC: 1"),
            ("    name: Build Native + JVM Test Classes", "    name: Build Native + JVM Test Classes\n    \"env\":\n      AWS_LC_SYS_NO_ASM: 1"),
            ("        uses: ./.github/actions/setup-builder", "        uses: ./.github/actions/setup-builder\n        env:\n          PKG_CONFIG_PATH: /custom"),
            (f"        uses: {CHECK.NATIVE_ACTION}", f"        uses: {CHECK.NATIVE_ACTION}\n        env:\n          CMAKE_BUILD_TYPE: Debug"),
        ]
        for old, new in cases:
            with self.subTest(new=new):
                self.assertTrue(CHECK.native_ci_failures(self.mutate(filename, old, new)))

    def test_runner_image_and_jdk_drift_are_rejected(self):
        cases = [
            ("spark_sql_test_reusable.yml", "runs-on: ubuntu-24.04", "runs-on: ubuntu-22.04"),
            ("spark_sql_test_reusable.yml", "image: amd64/rust", "image: rust:1.90"),
            ("iceberg_spark_test_reusable.yml", "jdk-version: 17", "jdk-version: 21"),
            ("ci.yml", "      java: 17", "      java: 21"),
            ("spark_sql_writer_tests.yml", "java=17", "java=21"),
            ("spark_sql_writer_tests.yml", "; java=17", ""),
        ]
        for filename, old, new in cases:
            with self.subTest(filename=filename, new=new):
                self.assertTrue(CHECK.native_ci_failures(self.mutate(filename, old, new)))
        for quote in ("", "'", '"'):
            with self.subTest(quote=quote):
                sources = dict(self.sources)
                sources["ci_label.yml"] += f"\n  extra:\n    uses: {quote}./.github/workflows/spark_sql_test_reusable.yml{quote}\n    with:\n      java: 21\n"
                self.assertTrue(CHECK.native_ci_failures(sources))

    def test_pre_native_setup_and_environment_writes_are_checked(self):
        filename = "spark_sql_test_reusable.yml"
        before = "      - name: Build or restore native library"
        for inserted in (
            "      - run: echo 'AWS_LC_SYS_NO_ASM=1' >> \"$GITHUB_ENV\"\n",
            "      - run: echo /custom >> \"$GITHUB_PATH\"\n",
            "      - uses: ./.github/actions/custom-native-setup\n",
        ):
            with self.subTest(inserted=inserted):
                self.assertTrue(CHECK.native_ci_failures(self.mutate(filename, before, inserted + before)))
        self.assertTrue(CHECK.native_ci_failures(self.mutate(
            filename, "        uses: ./.github/actions/setup-builder", "        uses: ./.github/actions/setup-builder\n        if: false")))
        self.assertTrue(CHECK.native_ci_failures(self.mutate(
            filename, "      - uses: actions/checkout@v7", "      - uses: actions/checkout@v7\n        with:\n          path: nested")))
        # A supported YAML layout outside this small parser's contract must
        # fail explicitly rather than hide a newly added producer.
        header, _, jobs = self.sources[filename].partition("\njobs:\n")
        sources = dict(self.sources)
        sources[filename] = header + "\njobs:\n" + "\n".join("  " + line if line else line for line in jobs.splitlines())
        self.assertTrue(CHECK.native_ci_failures(sources))

    def test_downstream_jvm_environment_remains_unrestricted(self):
        filename = "spark_sql_test_reusable.yml"
        old = "      - name: Setup Spark"
        new = "      - run: echo 'EXAMPLE=1' >> \"$GITHUB_ENV\"\n        env:\n          JVM_TEST_MODE: custom\n" + old
        self.assertEqual(CHECK.native_ci_failures(self.mutate(filename, old, new)), [])
        sources = dict(self.sources)
        sources["ci_label.yml"] += "\n  unrelated:\n    # Keep separate from ./.github/actions/build-native-ci\n    uses: ./.github/workflows/ci.yml\n"
        self.assertEqual(CHECK.native_ci_failures(sources), [])

    def test_native_inputs_get_locked_validation_before_main(self):
        routes = CHECK.load_filters()
        cases = [
            ({"name": "pull_request"}, {"build_linux", "build_linux_full"}),
            ({"name": "merge_group"}, {"build_linux", "build_linux_full"}),
            ({"name": "schedule"}, {"build_linux_all_profiles"}),
            ({"name": "push"}, {"build_linux"}),
            ({"name": "pull_request", "base": "branch-1.1"}, CHECK.LINUX_JOBS),
        ]
        for path in ("contrib/lance/native/Cargo.toml", ".cargo/config.toml", "rust-toolchain"):
            for event, expected in cases:
                with self.subTest(path=path, event=event):
                    self.assertEqual({job for job, selected in routes.compute([path], event).items() if selected}, expected)
        for event in ({"name": "pull_request"}, {"name": "merge_group"}):
            self.assertTrue(routes.compute(["contrib/delta/native/Cargo.toml"], event)["build_linux"])
        self.assertTrue(routes.compute(["native/shuffle/benches/shuffle.rs"], {"name": "pull_request"})["build_linux"])


if __name__ == "__main__":
    unittest.main()
