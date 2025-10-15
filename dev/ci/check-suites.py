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
