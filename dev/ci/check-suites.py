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

import re
import sys
from pathlib import Path

def file_to_class_name(path: Path) -> str | None:
    parts = path.parts
    if "org" not in parts or "apache" not in parts:
        return None
    org_index = parts.index("org")
    package_parts = parts[org_index:]
    class_name = ".".join(package_parts)
    class_name = class_name.replace(".scala", "")
    return class_name

if __name__ == "__main__":

    # ignore traits, abstract classes, and intentionally skipped test suites
    ignore_list = [
        "org.apache.comet.parquet.ParquetReadSuite", # abstract
        "org.apache.comet.parquet.ParquetReadFromS3Suite", # manual test suite
        "org.apache.comet.parquet.ParquetReadFromFakeHadoopFsSuite", # manual test suite (loads libhdfs, see #5023)
        "org.apache.comet.IcebergReadFromS3Suite", # manual test suite
        "org.apache.comet.cloud.s3.CometS3CredentialBridgeSuite", # manual test suite
        "org.apache.comet.shuffle.CelebornReflectionCompatibilitySuite", # dedicated version matrix
        "org.apache.spark.sql.comet.CometPlanStabilitySuite", # abstract
        "org.apache.spark.sql.comet.ParquetDatetimeRebaseSuite", # abstract
        "org.apache.comet.exec.CometColumnarShuffleSuite" # abstract
    ]

    for workflow_filename in [".github/workflows/pr_build_linux.yml", ".github/workflows/pr_build_macos.yml"]:
        workflow = open(workflow_filename, encoding="utf-8").read()

        root = Path(".")
        for path in root.rglob("*Suite.scala"):
            class_name = file_to_class_name(path)
            if class_name:
                if "Shim" in class_name:
                    continue
                if class_name in ignore_list:
                    continue
                if class_name not in workflow:
                    print(f"Suite not found in workflow {workflow_filename}: {class_name}")
                    sys.exit(-1)
                print(f"Found {class_name} in {workflow_filename}")

    # Forward check: every suite named in a workflow must be declared in a source file.
    # Filename-based discovery is not enough here, because several suites are declared
    # inside a file named after a different class (e.g. CometShuffleSuite is declared in
    # CometColumnarShuffleSuite.scala), and version-specific suites such as
    # CometStringDecodeSuite live only in the spark-3.x sourceset.
    declared = set()
    suite_declaration = re.compile(
        r"^\s*(?:(?:final|sealed|abstract|private|case)\s+)*(?:class|trait|object)\s+(\w*Suite)\b"
    )
    for path in Path(".").rglob("*.scala"):
        if "target" in path.parts:
            continue
        class_name = file_to_class_name(path)
        if class_name:
            declared.add(class_name.rsplit(".", 1)[-1])
        for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            match = suite_declaration.match(line)
            if match:
                declared.add(match.group(1))

    listed_suite = re.compile(r"\borg\.apache\.[A-Za-z0-9_.]*Suite\b")
    undeclared = []
    for workflow_filename in [".github/workflows/pr_build_linux.yml", ".github/workflows/pr_build_macos.yml"]:
        workflow = open(workflow_filename, encoding="utf-8").read()
        for name in sorted(set(listed_suite.findall(workflow))):
            if name.rsplit(".", 1)[-1] not in declared:
                undeclared.append((workflow_filename, name))

    if undeclared:
        for workflow_filename, name in undeclared:
            print(f"Workflow lists an undeclared suite {name} in {workflow_filename}")
        sys.exit(-1)
    print("All workflow-listed suites are declared")
