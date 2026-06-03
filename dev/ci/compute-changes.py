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

# Replacement for dorny/paths-filter, which is not on the apache org allow
# list. Reads a list of changed files (one per line) and emits per-job
# "<name>=true|false" lines suitable for $GITHUB_OUTPUT. Pattern semantics
# match dorny/picomatch: "**" spans path segments, "*" stays within a
# segment, and a leading "!" marks an exclude pattern.

import re
import sys
from pathlib import Path

FILTERS = {
    "build_linux": [
        "native/**",
        "common/**",
        "spark/**",
        "spark-integration/**",
        "pom.xml",
        "**/pom.xml",
        ".mvn/**",
        "mvnw",
        "Makefile",
        "rust-toolchain.toml",
        "dev/ci/**",
        ".github/workflows/ci.yml",
        ".github/workflows/pr_build_linux.yml",
        ".github/actions/setup-builder/**",
        ".github/actions/java-test/**",
        ".github/actions/rust-test/**",
        "!**.md",
        "!native/core/benches/**",
        "!native/spark-expr/benches/**",
        "!spark/src/test/scala/org/apache/spark/sql/benchmark/**",
        "!spark/src/main/scala/org/apache/comet/GenerateDocs.scala",
    ],
    "build_macos": [
        "native/**",
        "common/**",
        "spark/**",
        "spark-integration/**",
        "pom.xml",
        "**/pom.xml",
        ".mvn/**",
        "mvnw",
        "Makefile",
        "rust-toolchain.toml",
        "dev/ci/**",
        ".github/workflows/ci.yml",
        ".github/workflows/pr_build_macos.yml",
        ".github/actions/setup-macos-builder/**",
        ".github/actions/java-test/**",
        "!**.md",
        "!native/core/benches/**",
        "!native/spark-expr/benches/**",
        "!spark/src/test/scala/org/apache/spark/sql/benchmark/**",
        "!spark/src/main/scala/org/apache/comet/GenerateDocs.scala",
    ],
    "benchmark": [
        "native/core/benches/**",
        "native/spark-expr/benches/**",
        "spark/src/test/scala/org/apache/spark/sql/benchmark/**",
    ],
    "docs": [
        ".asf.yaml",
        ".github/workflows/docs.yaml",
        "docs/**",
    ],
    "spark_3_4": [
        "native/**/src/**",
        "native/**/Cargo.toml",
        "native/Cargo.lock",
        "!native/hdfs/**",
        "!native/fs-hdfs/**",
        "common/src/main/**",
        "common/pom.xml",
        "spark/src/main/**",
        "!spark/src/main/spark-3.5/**",
        "!spark/src/main/spark-4.0/**",
        "!spark/src/main/spark-4.1/**",
        "!spark/src/main/spark-4.2/**",
        "!spark/src/main/spark-4.x/**",
        "!spark/src/main/scala/org/apache/comet/GenerateDocs.scala",
        "spark/pom.xml",
        "dev/diffs/3.4.3.diff",
        "pom.xml",
        "rust-toolchain.toml",
        ".github/workflows/ci.yml",
        ".github/workflows/spark_sql_test_reusable.yml",
        ".github/actions/setup-builder/**",
        ".github/actions/setup-spark-builder/**",
    ],
    "spark_3_5": [
        "native/**/src/**",
        "native/**/Cargo.toml",
        "native/Cargo.lock",
        "!native/hdfs/**",
        "!native/fs-hdfs/**",
        "common/src/main/**",
        "common/pom.xml",
        "spark/src/main/**",
        "!spark/src/main/spark-3.4/**",
        "!spark/src/main/spark-4.0/**",
        "!spark/src/main/spark-4.1/**",
        "!spark/src/main/spark-4.2/**",
        "!spark/src/main/spark-4.x/**",
        "!spark/src/main/scala/org/apache/comet/GenerateDocs.scala",
        "spark/pom.xml",
        "dev/diffs/3.5.8.diff",
        "pom.xml",
        "rust-toolchain.toml",
        ".github/workflows/ci.yml",
        ".github/workflows/spark_sql_test_reusable.yml",
        ".github/actions/setup-builder/**",
        ".github/actions/setup-spark-builder/**",
    ],
    "spark_4_0": [
        "native/**/src/**",
        "native/**/Cargo.toml",
        "native/Cargo.lock",
        "!native/hdfs/**",
        "!native/fs-hdfs/**",
        "common/src/main/**",
        "common/pom.xml",
        "spark/src/main/**",
        "!spark/src/main/spark-3.4/**",
        "!spark/src/main/spark-3.5/**",
        "!spark/src/main/spark-3.x/**",
        "!spark/src/main/spark-4.1/**",
        "!spark/src/main/spark-4.2/**",
        "!spark/src/main/scala/org/apache/comet/GenerateDocs.scala",
        "spark/pom.xml",
        "dev/diffs/4.0.2.diff",
        "pom.xml",
        "rust-toolchain.toml",
        ".github/workflows/ci.yml",
        ".github/workflows/spark_sql_test_reusable.yml",
        ".github/actions/setup-builder/**",
        ".github/actions/setup-spark-builder/**",
    ],
    "spark_4_1": [
        "native/**/src/**",
        "native/**/Cargo.toml",
        "native/Cargo.lock",
        "!native/hdfs/**",
        "!native/fs-hdfs/**",
        "common/src/main/**",
        "common/pom.xml",
        "spark/src/main/**",
        "!spark/src/main/spark-3.4/**",
        "!spark/src/main/spark-3.5/**",
        "!spark/src/main/spark-3.x/**",
        "!spark/src/main/spark-4.0/**",
        "!spark/src/main/spark-4.2/**",
        "!spark/src/main/scala/org/apache/comet/GenerateDocs.scala",
        "spark/pom.xml",
        "dev/diffs/4.1.2.diff",
        "pom.xml",
        "rust-toolchain.toml",
        ".github/workflows/ci.yml",
        ".github/workflows/spark_sql_test_reusable.yml",
        ".github/actions/setup-builder/**",
        ".github/actions/setup-spark-builder/**",
    ],
    "iceberg_1_8": [
        "native/**/src/**",
        "native/**/Cargo.toml",
        "native/Cargo.lock",
        "!native/hdfs/**",
        "!native/fs-hdfs/**",
        "common/src/main/**",
        "common/pom.xml",
        "spark/src/main/**",
        "!spark/src/main/scala/org/apache/comet/GenerateDocs.scala",
        "spark/pom.xml",
        "dev/diffs/iceberg/**",
        "pom.xml",
        "rust-toolchain.toml",
        ".github/workflows/ci.yml",
        ".github/workflows/iceberg_spark_test_reusable.yml",
        ".github/actions/setup-builder/**",
        ".github/actions/setup-iceberg-builder/**",
    ],
    "iceberg_1_9": [
        "native/**/src/**",
        "native/**/Cargo.toml",
        "native/Cargo.lock",
        "!native/hdfs/**",
        "!native/fs-hdfs/**",
        "common/src/main/**",
        "common/pom.xml",
        "spark/src/main/**",
        "!spark/src/main/scala/org/apache/comet/GenerateDocs.scala",
        "spark/pom.xml",
        "dev/diffs/iceberg/**",
        "pom.xml",
        "rust-toolchain.toml",
        ".github/workflows/ci.yml",
        ".github/workflows/iceberg_spark_test_reusable.yml",
        ".github/actions/setup-builder/**",
        ".github/actions/setup-iceberg-builder/**",
    ],
    "iceberg_1_10": [
        "native/**/src/**",
        "native/**/Cargo.toml",
        "native/Cargo.lock",
        "!native/hdfs/**",
        "!native/fs-hdfs/**",
        "common/src/main/**",
        "common/pom.xml",
        "spark/src/main/**",
        "!spark/src/main/scala/org/apache/comet/GenerateDocs.scala",
        "spark/pom.xml",
        "dev/diffs/iceberg/**",
        "pom.xml",
        "rust-toolchain.toml",
        ".github/workflows/ci.yml",
        ".github/workflows/iceberg_spark_test_reusable.yml",
        ".github/actions/setup-builder/**",
        ".github/actions/setup-iceberg-builder/**",
    ],
}


def glob_to_regex(pat):
    # Translate a picomatch-style glob to a regex. "**/" at the start or
    # interior is optional ("(?:.*/)?") so that "**/pom.xml" matches at the
    # repo root; bare "**" is greedy across path separators.
    out = []
    i = 0
    while i < len(pat):
        c = pat[i]
        if c == "*" and i + 1 < len(pat) and pat[i + 1] == "*":
            if i + 2 < len(pat) and pat[i + 2] == "/":
                out.append("(?:.*/)?")
                i += 3
            else:
                out.append(".*")
                i += 2
        elif c == "*":
            out.append("[^/]*")
            i += 1
        elif c == "?":
            out.append("[^/]")
            i += 1
        elif c in r".+(){}[]^$|\\":
            out.append("\\" + c)
            i += 1
        else:
            out.append(c)
            i += 1
    return "^" + "".join(out) + "$"


def matches(patterns, files):
    includes = [re.compile(glob_to_regex(p)) for p in patterns if not p.startswith("!")]
    excludes = [re.compile(glob_to_regex(p[1:])) for p in patterns if p.startswith("!")]
    for f in files:
        if any(r.match(f) for r in includes) and not any(r.match(f) for r in excludes):
            return True
    return False


if __name__ == "__main__":
    files_path = Path(sys.argv[1])
    files = [line.strip() for line in files_path.read_text().splitlines() if line.strip()]
    for name, patterns in FILTERS.items():
        flag = "true" if matches(patterns, files) else "false"
        print(f"{name}={flag}")
