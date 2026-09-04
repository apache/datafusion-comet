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

"""Export downloaded dependencies only; never export a build workspace."""

import argparse
import hashlib
from pathlib import Path
import shutil
import sys
import xml.etree.ElementTree as ET

# These are caches of downloaded sources/distributions, not compiler outputs.
CACHE_PATHS = (
    "cargo/registry",
    "cargo/git",
    "maven/wrapper",
    "coursier",
    "sbt/boot",
    "ivy/cache",
)
BINARY_CACHES = ("maven", "coursier", "sbt", "ivy")


def forbidden(path):
    parts = path.parts
    return (
        any("SNAPSHOT" in part.upper() for part in parts)
        or "org.apache.datafusion" in parts
        or "org/apache/datafusion" in path.as_posix()
        or path.name.startswith("comet-")
        or path.name in {"maven-metadata-local.xml", "ivy-local.xml"}
        or path.name.endswith((".lastUpdated", ".part", ".lock", ".lck"))
    )


def copy_file(source, destination):
    if source.is_symlink():
        raise ValueError(f"Unexpected cache symlink: {source}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)


def export_maven(source, destination):
    """Use Maven Resolver's origin records to exclude locally installed files."""
    destination.mkdir(parents=True, exist_ok=True)
    count = 0
    for marker in sorted(source.rglob("_remote.repositories")):
        if forbidden(marker.relative_to(source)):
            continue
        records = []
        lines = marker.read_text().splitlines()
        local_files = {line.partition(">")[0] for line in lines if ">=" in line}
        for line in lines:
            if not line or line.startswith("#"):
                continue
            filename, separator, origin = line.partition(">")
            repository, equals, _ = origin.partition("=")
            if not separator or not equals or not repository or filename in local_files:
                continue  # An empty repository id denotes a local install.
            if Path(filename).name != filename:
                raise ValueError(f"Invalid Maven origin record: {marker}: {line}")
            artifact = marker.parent / filename
            relative = artifact.relative_to(source)
            if forbidden(relative) or not artifact.is_file():
                continue
            if artifact.is_symlink():
                raise ValueError(f"Unexpected Maven symlink: {artifact}")
            copy_file(artifact, destination / relative)
            records.append(line)
            count += 1
            for suffix in (".sha1", ".sha256", ".sha512", ".md5"):
                checksum = artifact.with_name(artifact.name + suffix)
                if checksum.is_file():
                    copy_file(checksum, destination / checksum.relative_to(source))
        if records:
            target = destination / marker.relative_to(source)
            target.write_text("\n".join(records) + "\n")
    # Version-range metadata is not recorded in _remote.repositories.
    for metadata in source.rglob("maven-metadata-*.xml"):
        relative = metadata.relative_to(source)
        if not forbidden(relative):
            copy_file(metadata, destination / relative)
    return count


def purge_partial_maven(repository):
    """Match the existing Spark setup workaround, but use one explicit repo."""
    for pom in repository.rglob("*.pom"):
        if pom.with_suffix(".jar").exists():
            continue
        try:
            root = ET.parse(pom).getroot()
        except ET.ParseError:
            continue
        packaging = root.find("{*}packaging")
        # Missing packaging means jar in Maven.
        if packaging is not None and packaging.text not in ("jar", "bundle"):
            continue
        for path in (pom, pom.with_name(pom.name + ".sha1")):
            path.unlink(missing_ok=True)
    # Main JARs alone are insufficient: Spark also requires tests classifiers.
    shutil.rmtree(repository / "org/apache/parquet", ignore_errors=True)


def export_caches(source, destination):
    for relative in CACHE_PATHS:
        src, dst = source / relative, destination / relative
        if src.exists():
            shutil.copytree(src, dst, symlinks=True, dirs_exist_ok=True)
        else:
            dst.mkdir(parents=True, exist_ok=True)
    copy_file(source / "sbt/launcher.jar", destination / "sbt/launcher.jar")
    for relative in BINARY_CACHES:
        for path in sorted((destination / relative).rglob("*"), reverse=True):
            if forbidden(path.relative_to(destination)):
                if path.is_dir() and not path.is_symlink():
                    shutil.rmtree(path)
                else:
                    path.unlink(missing_ok=True)
    # Never retain Coursier's file:// cache or an Ivy local publication.
    shutil.rmtree(destination / "coursier/file", ignore_errors=True)
    audit(destination)


def audit(root):
    settings = root / "maven/settings.xml"
    template = root / "bin/maven-settings.xml"
    if settings.exists() and (
        not template.is_file() or settings.read_bytes() != template.read_bytes()
    ):
        raise ValueError(
            "Unexpected Maven settings: only the checked-in CI settings are allowed"
        )
    for relative in BINARY_CACHES:
        for path in (root / relative).rglob("*"):
            if forbidden(path.relative_to(root)):
                raise ValueError(f"Forbidden cached artifact: {path}")
            if path.is_symlink():
                raise ValueError(f"Unexpected cache symlink: {path}")
    for relative in ("cargo/bin", "ivy/local", "sbt/global", "native", "apache-spark"):
        if (root / relative).exists():
            raise ValueError(f"Unexpected build output: {root / relative}")
    for marker in (root / "maven/repository").rglob("_remote.repositories"):
        for line in marker.read_text().splitlines():
            if not line.startswith("#") and ">=" in line:
                raise ValueError(f"Locally installed Maven artifact: {marker}")
    for artifact in (root / "maven/repository").rglob("*"):
        if not artifact.is_file() or artifact.suffix not in (".jar", ".pom", ".zip"):
            continue
        marker = artifact.parent / "_remote.repositories"
        if not marker.is_file() or not any(
            line.startswith(artifact.name + ">")
            and not line.startswith(artifact.name + ">=")
            for line in marker.read_text().splitlines()
        ):
            raise ValueError(f"Maven artifact has no download origin: {artifact}")


def input_hashes(root, spark_version="4.1.3"):
    """Changes to code need no new image, changes to dependency inputs do."""
    paths = {root / ".mvn/wrapper/maven-wrapper.properties"}
    for pattern in ("**/pom.xml", "**/Cargo.toml", "**/Cargo.lock"):
        paths.update(root.glob(pattern))
    paths.add(root / f"dev/diffs/{spark_version}.diff")
    paths.add(root / "dev/ci/images/versions.env")
    paths.add(root / "dev/ci/images/maven-settings.xml")
    paths.add(root / "dev/ci/images/repositories")
    paths.add(root / "dev/ci/images/CometCiRepositories.scala")
    paths.add(root / "dev/ci/images/Dockerfile")
    paths.add(root / "dev/ci/images/Dockerfile.dockerignore")
    paths.add(root / "dev/ci/images/cache.py")
    paths.update((root / "dev/ci/images").glob("*.sh"))
    for path in sorted(paths):
        relative = path.relative_to(root)
        if any(part in ("target", "apache-spark", ".git") for part in relative.parts):
            continue
        if path.is_file():
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            print(f"{digest}  {relative.as_posix()}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "operation", choices=("maven", "purge", "export", "audit", "inputs")
    )
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path, nargs="?")
    parser.add_argument("--spark-version", default="4.1.3")
    args = parser.parse_args()
    if args.operation in ("maven", "export") and args.destination is None:
        parser.error("destination is required for export")
    if args.operation == "maven":
        count = export_maven(args.source, args.destination)
        if not count:
            parser.error("no downloaded Maven artifacts were found")
    elif args.operation == "purge":
        purge_partial_maven(args.source)
    elif args.operation == "export":
        export_caches(args.source, args.destination)
    elif args.operation == "audit":
        audit(args.source)
    else:
        input_hashes(args.source, args.spark_version)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError) as error:
        sys.exit(str(error))
