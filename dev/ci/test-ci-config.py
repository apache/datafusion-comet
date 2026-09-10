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

"""Regression tests for shared artifacts using mutations of the real workflows."""

import importlib.util
from pathlib import Path
import shutil
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location(
    "check_ci_config", Path(__file__).with_name("check-ci-config.py"))
CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK)
WORKFLOWS = Path(__file__).resolve().parents[2] / ".github/workflows"


class SharedNativeArtifactTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="comet-ci-config-test-")
        self.addCleanup(self.temp.cleanup)
        self.workflows = Path(self.temp.name) / "workflows"
        shutil.copytree(WORKFLOWS, self.workflows)

    def replace(self, filename, old, new):
        path = self.workflows / filename
        text = path.read_text(encoding="utf-8")
        self.assertIn(old, text, f"Fixture changed: {filename}")
        path.write_text(text.replace(old, new, 1), encoding="utf-8")

    def assert_rejected(self, expected):
        failures = CHECK.artifact_failures(self.workflows)
        self.assertTrue(any(expected in failure for failure in failures), failures)

    def test_real_workflows_have_valid_shared_artifacts(self):
        self.assertEqual(CHECK.artifact_failures(self.workflows), [])

    def test_missing_shared_producer_is_rejected(self):
        (self.workflows / CHECK.SHARED_NATIVE_WORKFLOW).unlink()
        self.assert_rejected("shared native producer is missing")

    def test_second_producer_call_is_rejected(self):
        path = self.workflows / "ci.yml"
        with path.open("a", encoding="utf-8") as stream:
            stream.write("\n  duplicate_native:\n    needs: changes\n"
                         "    uses: ./.github/workflows/build_linux_native.yml\n")
        self.assert_rejected("expected exactly one")

    def test_another_workflow_cannot_publish_the_shared_name(self):
        shutil.copyfile(self.workflows / CHECK.SHARED_NATIVE_WORKFLOW,
                        self.workflows / "duplicate_native.yml")
        self.assert_rejected("only build_linux_native.yml may upload")

    def test_producer_matrix_is_rejected(self):
        self.replace(CHECK.SHARED_NATIVE_WORKFLOW, "    runs-on:",
                     "    strategy:\n      matrix:\n        duplicate: [1, 2]\n    runs-on:")
        self.assert_rejected("shared native producer must not use a matrix")

    def test_producer_artifact_rename_is_rejected(self):
        self.replace(CHECK.SHARED_NATIVE_WORKFLOW, "name: native-lib-linux",
                     "name: native-lib-renamed")
        self.assert_rejected("uploading 'native-lib-linux' once")

    def test_missing_dependency_in_each_consumer_call_is_rejected(self):
        path = self.workflows / "ci.yml"
        original = path.read_text(encoding="utf-8")
        job_ids = {"pr_build_linux"} | (CHECK.BUILD_JOBS - {"build_linux", "build_macos"})
        for job_id in sorted(job_ids):
            with self.subTest(job=job_id):
                start = original.index(f"\n  {job_id}:\n")
                before, body = original[:start], original[start:]
                self.assertIn("needs: [changes, build_linux_native]", body)
                body = body.replace("needs: [changes, build_linux_native]", "needs: changes", 1)
                path.write_text(before + body, encoding="utf-8")
                self.assert_rejected(f"{job_id} must need changes and build_linux_native")
        path.write_text(original, encoding="utf-8")

    def test_wrong_caller_artifact_is_rejected(self):
        self.replace("ci.yml", "native-library-artifact: native-lib-linux",
                     "native-library-artifact: native-lib-wrong")
        self.assert_rejected("must pass native-library-artifact: native-lib-linux")

    def test_missing_caller_artifact_is_rejected(self):
        self.replace("ci.yml", "      native-library-artifact: native-lib-linux\n", "")
        self.assert_rejected("must pass native-library-artifact: native-lib-linux")

    def test_consumer_input_must_be_required(self):
        self.replace("pr_build_linux.yml", "required: true", "required: false")
        self.assert_rejected("native-library-artifact must be a required string input")

    def test_consumer_input_must_be_string(self):
        self.replace("pr_build_linux.yml", "type: string", "type: boolean")
        self.assert_rejected("native-library-artifact must be a required string input")

    def test_literal_native_download_is_rejected(self):
        self.replace("pr_build_linux.yml", "name: ${{ inputs.native-library-artifact }}",
                     "name: native-lib-linux")
        self.assert_rejected("native downloads must use")

    def test_jvm_artifact_cannot_replace_a_native_download(self):
        path = self.workflows / "spark_sql_test_reusable.yml"
        uploads, _ = CHECK.artifact_names(path)
        jvm_name = next(name for name in uploads if name.startswith("jvm-compiled-spark-"))
        self.replace(path.name, "name: ${{ inputs.native-library-artifact }}", f"name: {jvm_name}")
        self.assert_rejected("native downloads must use")

    def test_unrelated_input_does_not_bypass_producer_check(self):
        self.replace("pr_build_linux.yml", "name: ${{ inputs.native-library-artifact }}",
                     "name: ${{ inputs.unrelated-artifact }}")
        self.assert_rejected("artifact '${{ inputs.unrelated-artifact }}' is downloaded but never uploaded")

    def test_native_build_in_consumer_is_rejected(self):
        path = self.workflows / "iceberg_spark_test_reusable.yml"
        with path.open("a", encoding="utf-8") as stream:
            stream.write("\n      - run: |\n          cargo build --profile ci\n")
        self.assert_rejected("consume the shared native library instead of building it")

    def test_jvm_artifact_rename_still_fails(self):
        self.replace("spark_sql_test_reusable.yml", "name: jvm-compiled-spark-",
                     "name: renamed-jvm-compiled-spark-")
        self.assert_rejected("is downloaded but never uploaded in the same workflow")

    def test_unqualified_jvm_upload_still_fails(self):
        path = self.workflows / "spark_sql_test_reusable.yml"
        text = path.read_text(encoding="utf-8")
        uploads, _ = CHECK.artifact_names(path)
        name = next(name for name in uploads if name.startswith("jvm-compiled-spark-"))
        path.write_text(text.replace(f"name: {name}", "name: jvm-compiled-spark"), encoding="utf-8")
        self.assert_rejected("qualify the name with an input")


if __name__ == "__main__":
    unittest.main()
