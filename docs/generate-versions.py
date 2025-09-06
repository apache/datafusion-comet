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
from pathlib import Path

current_version = "0.10.0-SNAPSHOT"
previous_versions = ["0.8", "0.9"]

def replace_in_files(root: str, filename_pattern: str, search: str, replace: str):
    root_path = Path(root)
    for md_file in root_path.rglob(filename_pattern):
        text = md_file.read_text(encoding="utf-8")
        updated = text.replace(search, replace)
        if text != updated:
            md_file.write_text(updated, encoding="utf-8")
            print(f"Replaced {search} with {replace} in {md_file}")

def generate_docs():

    # Replace $COMET_VERSION with actual version
    for file_pattern in ["*.md", "*.rst"]:
        replace_in_files(f"temp/user-guide/latest", file_pattern, "$COMET_VERSION", current_version)

    for version in previous_versions:
        os.system(f"git clone --depth 1 https://github.com/apache/datafusion-comet.git -b branch-{version} comet-{version}")
        os.system(f"mkdir temp/user-guide/{version}")
        os.system(f"cp -rf comet-{version}/docs/source/user-guide/* temp/user-guide/{version}")
        # Replace $COMET_VERSION with actual version
        for file_pattern in ["*.md", "*.rst"]:
            replace_in_files(f"temp/user-guide/{version}", file_pattern, "$COMET_VERSION", current_version)

if __name__ == "__main__":
    print("Generating versioned user guide docs...")
    generate_docs()