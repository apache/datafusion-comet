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
# of Comet

import os
import xml.etree.ElementTree as ET
from pathlib import Path

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

def publish_released_version(version: str):
    major_minor = get_major_minor_version(version)
    os.system(f"git clone --depth 1 https://github.com/apache/datafusion-comet.git -b branch-{major_minor} comet-{major_minor}")
    os.system(f"mkdir temp/user-guide/{major_minor}")
    os.system(f"cp -rf comet-{major_minor}/docs/source/user-guide/* temp/user-guide/{major_minor}")
    # Replace $COMET_VERSION with actual version
    for file_pattern in ["*.md", "*.rst"]:
        replace_in_files(f"temp/user-guide/{major_minor}", file_pattern, "$COMET_VERSION", version)

def generate_docs(snapshot_version: str, latest_released_version: str, previous_versions: list[str]):

    # Replace $COMET_VERSION with actual version for snapshot version
    for file_pattern in ["*.md", "*.rst"]:
        replace_in_files(f"temp/user-guide/latest", file_pattern, "$COMET_VERSION", snapshot_version)

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

if __name__ == "__main__":
    print("Generating versioned user guide docs...")
    snapshot_version = get_version_from_pom()
    latest_released_version = "0.9.1"
    previous_versions = ["0.8.0"]
    generate_docs(snapshot_version, latest_released_version, previous_versions)