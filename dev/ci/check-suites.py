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
"""Check that every Comet test suite belongs to exactly one CI test group.

CI runs the Comet test suites in groups, one job per group, and
selects a group's suites by the ScalaTest tag they carry (see
spark/src/test/java/org/apache/comet/tags/*Group.java). A concrete suite with
no group tag would run in no job, so this fails on one, and on a suite with
two tags, which would run twice. Suites deliberately left out of CI are listed
in IGNORE_LIST and must carry no group tag, so that no group picks them up.
"""

import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

# One ScalaTest tag annotation per group, such as ExecGroup for the `exec` group.
TAGS_DIR = REPO_ROOT / "spark/src/test/java/org/apache/comet/tags"
GROUP_TAGS = sorted(path.stem for path in TAGS_DIR.glob("*Group.java"))

# Suites that CI does not run, each with the reason.
IGNORE_LIST = {
    "org.apache.comet.parquet.ParquetReadFromS3Suite": "manual test suite",
    "org.apache.comet.parquet.ParquetWriteWithMagicCommitterSuite": "manual test suite",
    "org.apache.comet.csv.CsvReadFromS3Suite": "manual test suite",
    "org.apache.comet.parquet.ParquetReadFromFakeHadoopFsSuite": "manual test suite (loads libhdfs, see #5023)",
    "org.apache.comet.IcebergReadFromS3Suite": "manual test suite",
    "org.apache.comet.CometIcebergSchedulerFailureSuite": "manual, multiple hosts + SIGKILL",
    "org.apache.comet.cloud.s3.CometS3CredentialBridgeSuite": "manual test suite",
    "org.apache.comet.cloud.s3.HadoopS3ACredentialProviderAdapterBridgeSuite": "manual test suite",
    "org.apache.comet.shuffle.CelebornReflectionCompatibilitySuite": "dedicated version matrix",
}

CLASS_DECL = re.compile(
    r"^(?P<mods>(?:(?:final|sealed|abstract|private(?:\[\w+\])?|case)\s+)*)"
    r"(?P<kind>class|trait|object)\s+(?P<name>\w+)"
)
ANNOTATION = re.compile(r"^@(\w+)")


def test_sources():
    """Every Scala test source outside contrib, in all source sets (scala, spark-3.4, spark-4.x, ...).

    The contrib modules run their suites from their own workflows, such as delta_contrib_test.yml.
    """
    for path in sorted(REPO_ROOT.rglob("*.scala")):
        parts = path.relative_to(REPO_ROOT).parts
        if parts[0] != "contrib" and "target" not in parts and "test" in parts:
            yield path


def declarations(text):
    """Yield (name, concrete, annotations, line) for each top-level type in a source file.

    `concrete` is true for a class that is neither abstract nor a trait or object.
    """
    lines = text.splitlines()
    for number, line in enumerate(lines):
        match = CLASS_DECL.match(line)
        if not match:
            continue
        annotations = []
        above = number - 1
        while above >= 0 and (annotation := ANNOTATION.match(lines[above])):
            annotations.append(annotation.group(1))
            above -= 1
        concrete = match.group("kind") == "class" and "abstract" not in match.group("mods")
        yield match.group("name"), concrete, annotations, number + 1


def main():
    failures = []
    members = {tag: set() for tag in GROUP_TAGS}
    seen_ignored = set()
    for path in test_sources():
        text = path.read_text(encoding="utf-8", errors="ignore")
        package = re.search(r"^package\s+([\w.]+)", text, re.M)
        prefix = package.group(1) + "." if package else ""
        for name, concrete, annotations, line in declarations(text):
            fqn = prefix + name
            where = f"{path.relative_to(REPO_ROOT)}:{line}"
            tags = [a for a in annotations if a in members]
            if not (concrete and re.search(r"(Suite|ITCase)$", name)):
                if tags:
                    failures.append(
                        f"{where}: {fqn} is not a concrete suite, so its @{tags[0]} does not reach "
                        "the suites that extend it. Tag each concrete suite instead."
                    )
                continue
            if fqn in IGNORE_LIST:
                seen_ignored.add(fqn)
                if tags:
                    failures.append(
                        f"{where}: {fqn} is in IGNORE_LIST ({IGNORE_LIST[fqn]}) but carries @{tags[0]}"
                    )
                continue
            if len(tags) != 1:
                found = ", ".join("@" + t for t in tags) if tags else "none"
                failures.append(
                    f"{where}: {fqn} needs exactly one group tag ({', '.join('@' + t for t in GROUP_TAGS)}); "
                    f"found {found}"
                )
                continue
            members[tags[0]].add(fqn)

    for fqn in sorted(set(IGNORE_LIST) - seen_ignored):
        failures.append(f"IGNORE_LIST names {fqn}, which no longer exists")

    if failures:
        for failure in failures:
            print(failure)
        sys.exit(1)
    summary = ", ".join(f"{tag} {len(suites)}" for tag, suites in members.items())
    print(f"Every suite has one group tag: {summary}")


if __name__ == "__main__":
    main()
