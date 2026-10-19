#!/usr/bin/python
##############################################################################
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
##############################################################################

# This script clones release branches such as branch-0.9 and then copies the user guide markdown
# for inclusion in the published documentation so that we publish user guides for released versions
# of Comet.
#
# The published user guide URLs follow the convention used by Apache Spark and Apache Iceberg:
#
#   user-guide/dev/      the unreleased user guide on main
#   user-guide/latest/   the newest release (an alias, see alias_latest below)
#   user-guide/1.1/      each release, pinned
#
# Only the versioned URLs are permanent. What `latest` and `dev` show changes with every release,
# and pages can be added, moved or removed between releases.

import os
import shutil
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

# The newest release, and the older releases that are still published. Promote a new release by
# moving the current one into PREVIOUS_VERSIONS (see the release process guide).
LATEST_RELEASED_VERSION = "1.1.0"
PREVIOUS_VERSIONS = ["0.13.0", "0.14.0", "0.15.0", "0.16.0", "0.17.0", "1.0.0"]

def get_major_minor_version(version: str):
    parts = version.split('.')
    return f"{parts[0]}.{parts[1]}"

def get_version_from_pom():
    pom_path = Path(__file__).parent.parent / "pom.xml"
    tree = ET.parse(pom_path)
    root = tree.getroot()
    
    namespace = {'maven': 'http://maven.apache.org/POM/4.0.0'}
    version_element = root.find('maven:version', namespace)
    
    if version_element is not None:
        return version_element.text
    else:
        raise ValueError("Could not find version in pom.xml")

def replace_in_files(root: str, filename_pattern: str, search: str, replace: str):
    root_path = Path(root)
    for file in root_path.rglob(filename_pattern):
        text = file.read_text(encoding="utf-8")
        updated = text.replace(search, replace)
        if text != updated:
            file.write_text(updated, encoding="utf-8")
            print(f"Replaced {search} with {replace} in {file}")


def strip_conditional_blocks(root: str, mode: str):
    if mode == "snapshot":
        keep_marker = "<!-- IF_SNAPSHOT -->"
        drop_marker = "<!-- IF_RELEASE -->"
    elif mode == "release":
        keep_marker = "<!-- IF_RELEASE -->"
        drop_marker = "<!-- IF_SNAPSHOT -->"
    else:
        raise ValueError(f"mode must be 'snapshot' or 'release', got {mode!r}")
    end_marker = "<!-- ENDIF -->"

    root_path = Path(root)
    for file_pattern in ("*.md", "*.rst"):
        for file in root_path.rglob(file_pattern):
            lines = file.read_text(encoding="utf-8").splitlines(keepends=True)
            out = []
            state = None  # None, "keep", or "drop"
            changed = False
            for lineno, line in enumerate(lines, start=1):
                stripped = line.strip()
                if stripped == keep_marker or stripped == drop_marker:
                    if state is not None:
                        raise ValueError(
                            f"{file}:{lineno}: conditional block opened while already inside a block"
                        )
                    state = "keep" if stripped == keep_marker else "drop"
                    changed = True
                elif stripped == end_marker:
                    if state is None:
                        raise ValueError(
                            f"{file}:{lineno}: ENDIF with no matching opener"
                        )
                    state = None
                    changed = True
                elif state == "drop":
                    changed = True
                else:
                    out.append(line)
            if state is not None:
                raise ValueError(f"{file}: unclosed conditional block")
            if changed:
                file.write_text("".join(out), encoding="utf-8")
                print(f"Processed conditional blocks ({mode}) in {file}")

def insert_warning_after_asf_header(root: str, warning: str):
    root_path = Path(root)
    for file in root_path.rglob("*.md"):
        lines = file.read_text(encoding="utf-8").splitlines(keepends=True)
        new_lines = []
        inserted = False
        for line in lines:
            new_lines.append(line)
            if not inserted and "-->" in line:
                new_lines.append(warning + "\n")
                inserted = True
        file.write_text("".join(new_lines), encoding="utf-8")


def get_user_guide_dir(clone_dir: str):
    # Where a release branch keeps its user guide depends on when it was cut: directly under
    # user-guide/ before 0.10, under user-guide/latest/ until the 1.1 branch, and under
    # user-guide/dev/ after that.
    for candidate in ("docs/source/user-guide/dev", "docs/source/user-guide/latest"):
        if (Path(clone_dir) / candidate).is_dir():
            return candidate
    return "docs/source/user-guide"

def publish_released_version(version: str):
    major_minor = get_major_minor_version(version)
    clone_dir = f"comet-{major_minor}"
    os.system(f"git clone --depth 1 https://github.com/apache/datafusion-comet.git -b branch-{major_minor} {clone_dir}")
    dir = get_user_guide_dir(clone_dir)
    os.system(f"mkdir temp/user-guide/{major_minor}")
    os.system(f"cp -rf comet-{major_minor}/{dir}/* temp/user-guide/{major_minor}")
    # Strip snapshot-only content; keep release-only content
    strip_conditional_blocks(f"temp/user-guide/{major_minor}", "release")
    # Replace $COMET_VERSION with actual version
    for file_pattern in ["*.md", "*.rst"]:
        replace_in_files(f"temp/user-guide/{major_minor}", file_pattern, "$COMET_VERSION", version)

def generate_docs(snapshot_version: str, latest_released_version: str, previous_versions: list[str]):

    # Strip release-only content; keep snapshot-only content for the latest (snapshot) docs
    strip_conditional_blocks("temp/user-guide/dev", "snapshot")

    # Replace $COMET_VERSION with actual version for snapshot version
    for file_pattern in ["*.md", "*.rst"]:
        replace_in_files("temp/user-guide/dev", file_pattern, "$COMET_VERSION", snapshot_version)

    # Add user guide content for latest released versions
    publish_released_version(latest_released_version)

    # Add user guide content for older released versions
    for version in previous_versions:
        publish_released_version(version)
        # add warning that this is out-of-date documentation
        warning = f"""```{{warning}}
This is **out-of-date** documentation. The latest Comet release is version {latest_released_version}.
```"""
        major_minor = get_major_minor_version(version)
        insert_warning_after_asf_header(f"temp/user-guide/{major_minor}", warning)

def alias_latest(html_root: str):
    """Serve the newest release's built user guide at user-guide/latest as well.

    This copies the finished HTML rather than building a second copy of the sources, so Sphinx
    sees each document once. It is a copy and not a symlink because the site is published by
    rsync into the asf-site branch, and ASF's web server is not known to follow symlinks. Every
    link in the pages is relative, so the copy works unchanged one directory over.
    """
    user_guide = Path(html_root) / "user-guide"
    source = user_guide / get_major_minor_version(LATEST_RELEASED_VERSION)
    if not source.is_dir():
        raise FileNotFoundError(f"{source} was not built, so there is nothing to alias as latest")
    target = user_guide / "latest"
    if target.exists():
        shutil.rmtree(target)
    shutil.copytree(source, target)
    print(f"Aliased {source} as {target}")

if __name__ == "__main__":
    # `generate-versions.py --alias-latest <html dir>` runs after the Sphinx build.
    if len(sys.argv) == 3 and sys.argv[1] == "--alias-latest":
        alias_latest(sys.argv[2])
        sys.exit(0)
    if len(sys.argv) != 1:
        sys.exit("usage: generate-versions.py [--alias-latest <html dir>]")
    print("Generating versioned user guide docs...")
    snapshot_version = get_version_from_pom()
    generate_docs(snapshot_version, LATEST_RELEASED_VERSION, PREVIOUS_VERSIONS)
