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

"""Fast, dependency-free regression tests for the CI image export boundary."""

from contextlib import redirect_stdout
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

DIRECTORY = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("image_cache", DIRECTORY / "cache.py")
cache = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cache)


class ImageCacheTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "source"
        self.destination = self.root / "destination"
        self.source.mkdir()

    def write(self, relative, contents="artifact"):
        path = self.source / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents)
        return path

    def maven_artifact(self, relative, origin="central"):
        path = self.write(relative)
        marker = path.parent / "_remote.repositories"
        with marker.open("a") as stream:
            stream.write(f"{path.name}>{origin}=\n")
        return path

    def test_remote_jars_poms_checksums_and_origin_survive(self):
        self.maven_artifact("example/lib/1/lib-1.jar")
        self.maven_artifact("example/lib/1/lib-1.pom")
        self.write("example/lib/1/lib-1.jar.sha1")
        count = cache.export_maven(self.source, self.destination)
        self.assertEqual(count, 2)
        self.assertTrue((self.destination / "example/lib/1/lib-1.jar.sha1").is_file())
        marker = self.destination / "example/lib/1/_remote.repositories"
        self.assertIn("lib-1.jar>central=", marker.read_text())

    def test_remote_parent_pom_without_a_jar_survives(self):
        self.maven_artifact("example/parent/1/parent-1.pom")
        cache.export_maven(self.source, self.destination)
        self.assertTrue((self.destination / "example/parent/1/parent-1.pom").exists())

    def test_locally_installed_release_is_excluded(self):
        self.maven_artifact("example/lib/1/lib-1.jar", origin="")
        self.maven_artifact("example/lib/1/lib-1.pom", origin="")
        cache.export_maven(self.source, self.destination)
        self.assertEqual(list(self.destination.rglob("*")), [])

    def test_artifact_without_origin_is_excluded(self):
        self.write("example/lib/1/lib-1.jar")
        cache.export_maven(self.source, self.destination)
        self.assertEqual(list(self.destination.rglob("*")), [])

    def test_local_install_overwriting_a_download_is_excluded(self):
        self.maven_artifact("example/lib/1/lib-1.jar", origin="central")
        self.maven_artifact("example/lib/1/lib-1.jar", origin="")
        cache.export_maven(self.source, self.destination)
        self.assertEqual(list(self.destination.rglob("*")), [])

    def test_comet_release_is_excluded_even_if_downloaded(self):
        self.maven_artifact("org/apache/datafusion/comet-spark/1.0/comet-spark-1.0.jar")
        cache.export_maven(self.source, self.destination)
        self.assertEqual(list(self.destination.rglob("*")), [])

    def test_snapshot_and_timestamped_snapshot_are_excluded(self):
        self.maven_artifact("example/lib/1-SNAPSHOT/lib-1-SNAPSHOT.jar")
        self.maven_artifact("example/lib/1-SNAPSHOT/lib-1-20260827.123456-1.jar")
        cache.export_maven(self.source, self.destination)
        self.assertEqual(list(self.destination.rglob("*")), [])

    def test_missing_classifier_is_not_exported_as_present(self):
        self.maven_artifact("example/lib/1/lib-1.jar")
        self.write(
            "example/lib/1/_remote.repositories",
            "lib-1.jar>central=\nlib-1-tests.jar>central=\n",
        )
        cache.export_maven(self.source, self.destination)
        marker = self.destination / "example/lib/1/_remote.repositories"
        self.assertNotIn("tests.jar", marker.read_text())

    def test_remote_metadata_is_retained_but_local_metadata_is_not(self):
        self.write("example/lib/maven-metadata-central.xml")
        self.write("example/lib/maven-metadata-local.xml")
        cache.export_maven(self.source, self.destination)
        self.assertTrue(
            (self.destination / "example/lib/maven-metadata-central.xml").exists()
        )
        self.assertFalse(
            (self.destination / "example/lib/maven-metadata-local.xml").exists()
        )

    def test_maven_path_traversal_is_rejected(self):
        self.write("example/lib/1/_remote.repositories", "../outside.jar>central=\n")
        with self.assertRaisesRegex(ValueError, "Invalid Maven origin"):
            cache.export_maven(self.source, self.destination)

    def test_maven_symlink_is_rejected(self):
        path = self.maven_artifact("example/lib/1/lib-1.jar")
        path.unlink()
        path.symlink_to(self.write("outside.jar"))
        with self.assertRaisesRegex(ValueError, "symlink"):
            cache.export_maven(self.source, self.destination)

    def test_purge_preserves_parent_poms_and_current_comet(self):
        self.write(
            "example/parent.pom", "<project><packaging>pom</packaging></project>"
        )
        self.write("example/partial.pom", "<project/>")
        self.write(
            "example/bundle.pom", "<project><packaging>bundle</packaging></project>"
        )
        self.write("example/complete.pom", "<project/>")
        self.write("example/complete.jar")
        comet = "org/apache/datafusion/comet/1-SNAPSHOT/comet-1-SNAPSHOT"
        self.write(comet + ".pom", "<project/>")
        self.write(comet + ".jar")
        self.write("org/apache/parquet/parquet-common/1/lib.pom", "<project/>")
        self.write("org/apache/parquet/parquet-common/1/lib.jar")
        cache.purge_partial_maven(self.source)
        self.assertTrue((self.source / "example/parent.pom").exists())
        self.assertFalse((self.source / "example/partial.pom").exists())
        self.assertFalse((self.source / "example/bundle.pom").exists())
        self.assertTrue((self.source / "example/complete.pom").exists())
        self.assertTrue((self.source / (comet + ".jar")).exists())
        self.assertFalse((self.source / "org/apache/parquet").exists())

    def test_export_keeps_downloads_but_not_build_outputs(self):
        self.write("cargo/registry/src/public/lib.rs")
        self.write("cargo/git/checkouts/public/lib.rs")
        self.write("cargo/bin/locally-built-tool")
        self.write("maven/settings.xml", "credentials must not be exported")
        self.write("maven/wrapper/dists/maven/apache-maven/bin/mvn")
        self.write("coursier/https/central/example/1/lib.jar")
        self.write("coursier/https/central/example/1-SNAPSHOT/lib.jar")
        self.write("coursier/file/warmup/comet.jar")
        self.write("sbt/boot/scala/sbt.jar")
        self.write("sbt/boot/sbt.boot.lock")
        self.write("sbt/global/zinc/compiled-bridge.jar")
        self.write("sbt/launcher.jar")
        self.write("ivy/local/locally-built.jar")
        self.write("native/target/ci/libcomet.so")
        cache.export_caches(self.source, self.destination)
        self.assertTrue(
            (self.destination / "cargo/registry/src/public/lib.rs").exists()
        )
        self.assertTrue((self.destination / "sbt/launcher.jar").exists())
        self.assertTrue(
            (self.destination / "coursier/https/central/example/1/lib.jar").exists()
        )
        for forbidden in (
            "cargo/bin",
            "maven/settings.xml",
            "sbt/global",
            "ivy/local",
            "native",
            "coursier/file",
            "coursier/https/central/example/1-SNAPSHOT",
            "sbt/boot/sbt.boot.lock",
        ):
            self.assertFalse((self.destination / forbidden).exists(), forbidden)

    def test_audit_rejects_unsourced_maven_artifact(self):
        self.write("maven/repository/example/lib/1/lib.jar")
        with self.assertRaisesRegex(ValueError, "no download origin"):
            cache.audit(self.source)

    def test_audit_allows_only_checked_in_maven_settings(self):
        settings = self.write("maven/settings.xml", "unexpected user settings")
        with self.assertRaisesRegex(ValueError, "Unexpected Maven settings"):
            cache.audit(self.source)
        template = self.write("bin/maven-settings.xml", "credential-free CI settings")
        settings.write_bytes(template.read_bytes())
        cache.audit(self.source)

    def test_audit_rejects_snapshot_local_origin_and_symlink(self):
        for name, content in (
            ("maven/repository/example/1-SNAPSHOT/lib.jar", ""),
            ("maven/repository/example/1/_remote.repositories", "lib.jar>=\n"),
        ):
            with self.subTest(name=name):
                path = self.write(name, content)
                with self.assertRaises(ValueError):
                    cache.audit(self.source)
                path.unlink()
                # Remove empty SNAPSHOT directories too.
                for parent in list(path.parents):
                    if parent == self.source:
                        break
                    if parent.exists() and not list(parent.iterdir()):
                        parent.rmdir()
        path = self.source / "sbt/boot/link"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.symlink_to("/warmup/comet/target")
        with self.assertRaisesRegex(ValueError, "symlink"):
            cache.audit(self.source)

    def test_input_fingerprint_tracks_new_manifests_but_not_build_outputs(self):
        self.write("pom.xml")
        self.write("native/Cargo.toml")
        self.write("native/Cargo.lock")
        self.write("contrib/delta/native/Cargo.toml")
        self.write("dev/ci/images/Dockerfile")
        self.write("dev/ci/images/CometCiRepositories.scala")
        self.write("dev/ci/images/run-build.sh")
        self.write("apache-spark/pom.xml", "excluded")
        self.write("spark/target/pom.xml", "excluded")
        output = io.StringIO()
        with redirect_stdout(output):
            cache.input_hashes(self.source)
        self.assertIn("contrib/delta/native/Cargo.toml", output.getvalue())
        self.assertIn("dev/ci/images/Dockerfile", output.getvalue())
        self.assertIn("dev/ci/images/CometCiRepositories.scala", output.getvalue())
        self.assertIn("dev/ci/images/run-build.sh", output.getvalue())
        self.assertNotIn("excluded", output.getvalue())
        self.assertNotIn("apache-spark", output.getvalue())
        self.assertNotIn("target", output.getvalue())
        self.write("new-module/pom.xml")
        updated = io.StringIO()
        with redirect_stdout(updated):
            cache.input_hashes(self.source)
        self.assertNotEqual(output.getvalue(), updated.getvalue())

    def test_shell_scripts_parse(self):
        for script in DIRECTORY.glob("*.sh"):
            with self.subTest(script=script.name):
                subprocess.run(["bash", "-n", str(script)], check=True)

    def run_validator(self, docker_status=0, bad_checksum=False):
        """Exercise the host driver without Docker or external downloads."""
        repo = self.root / "repo"
        scripts = repo / "dev/ci/images"
        scripts.mkdir(parents=True)
        for name in ("validate.sh", "fetch-spark.sh"):
            shutil.copy2(DIRECTORY / name, scripts / name)
        checksum = hashlib.sha256(b"spark sources").hexdigest()
        if bad_checksum:
            checksum = "0" * 64
        (scripts / "versions.env").write_text(
            "SPARK_VERSION=4.1.3\nJDK_VERSION=17.0.20.1\n"
            f"SPARK_COMMIT={'1' * 40}\nSPARK_ARCHIVE_SHA256={checksum}\n"
        )
        (repo / "pom.xml").write_text("tracked working source")
        (repo / "untracked-secret").write_text("must not enter container")
        temporary = self.root / "tmp"
        temporary.mkdir()
        binaries = self.root / "bin"
        binaries.mkdir()
        stubs = {
            "git": "import sys\nassert sys.argv[-2:] == ['ls-files', '-z']\n"
            "sys.stdout.buffer.write(b'pom.xml\\0')\n",
            "curl": "import sys\nfrom pathlib import Path\n"
            "Path(sys.argv[sys.argv.index('--output') + 1]).write_bytes(b'spark sources')\n",
            "docker": "import json, os, sys, tarfile\nfrom pathlib import Path\n"
            "args = sys.argv[1:]\n"
            "mount = args[args.index('--mount') + 1]\n"
            "source = next(part[4:] for part in mount.split(',') if part.startswith('src='))\n"
            "with tarfile.open(Path(source) / 'comet.tar.gz') as archive:\n"
            "    files = archive.getnames()\n"
            "Path(os.environ['TEST_RECORD']).write_text(json.dumps({'args': args, 'files': files}))\n"
            "sys.exit(int(os.environ['TEST_DOCKER_STATUS']))\n",
        }
        for name, contents in stubs.items():
            executable = binaries / name
            executable.write_text("#!/usr/bin/env python3\n" + contents)
            executable.chmod(0o755)
        record = self.root / "docker.json"
        result = subprocess.run(
            ["bash", str(scripts / "validate.sh"), "ci-image:test"],
            env={
                **os.environ,
                "PATH": str(binaries) + os.pathsep + os.environ["PATH"],
                "TMPDIR": str(temporary),
                "TEST_RECORD": str(record),
                "TEST_DOCKER_STATUS": str(docker_status),
            },
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            list(temporary.iterdir()), [], "source staging leaked after exit"
        )
        return result, json.loads(record.read_text()) if record.exists() else None

    def test_validator_uses_only_sources_and_disables_network(self):
        result, record = self.run_validator()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(record["files"], ["pom.xml"])
        self.assertIn("--network=none", record["args"])
        self.assertIn("HOME=/github/home", record["args"])
        self.assertIn("/__w", record["args"])
        self.assertIn("ci-image:test", record["args"])
        mounts = [item for item in record["args"] if item.startswith("type=bind,")]
        self.assertEqual(len(mounts), 1)
        self.assertTrue(mounts[0].endswith(",dst=/inputs,readonly"))

    def test_validator_propagates_failure_and_cleans_sources(self):
        result, record = self.run_validator(docker_status=7)
        self.assertEqual(result.returncode, 7)
        self.assertIsNotNone(record)

    def test_validator_rejects_wrong_spark_archive_before_docker(self):
        result, record = self.run_validator(bad_checksum=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIsNone(record)


if __name__ == "__main__":
    unittest.main(verbosity=2)
