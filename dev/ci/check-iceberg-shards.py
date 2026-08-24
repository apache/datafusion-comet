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

"""Exercise the real Gradle/JUnit shard filter without building Spark or Iceberg.

Run with the Iceberg checkout's wrapper (or a Gradle executable):
    python3 dev/ci/check-iceberg-shards.py --gradle "$PWD/apache-iceberg/gradlew"

The fixture downloads only JUnit. --junit-classpath accepts local JUnit jars for
offline checks. --work-dir retains the fixture, logs, and comparison results.
"""

import argparse
from collections import Counter
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import xml.etree.ElementTree as ET


INIT_SCRIPT = Path(__file__).with_name("iceberg-test-shards.gradle")


def groovy_string(value):
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def create_fixture(root, junit_classpath):
    (root / "settings.gradle").write_text("rootProject.name = 'comet-iceberg-shard-fixture'\n")
    if junit_classpath:
        jars = [str(Path(p).resolve()) for p in junit_classpath.split(os.pathsep)]
        if not all(Path(p).is_file() for p in jars):
            raise ValueError("Every --junit-classpath entry must be an existing jar")
        dependencies = "testImplementation files(" + ", ".join(map(groovy_string, jars)) + ")"
    else:
        dependencies = """
          testImplementation 'org.junit.jupiter:junit-jupiter:5.11.4'
          testRuntimeOnly 'org.junit.platform:junit-platform-launcher:1.11.4'
        """
    (root / "build.gradle").write_text("""
        import groovy.json.JsonOutput
        plugins { id 'java' }
        repositories { mavenCentral() }
        dependencies { DEPENDENCIES }
        test {
          useJUnitPlatform { excludeTags 'excluded-tag' }
          include '**/Test*.class'
          exclude '**/TestExcludedByPattern.class', '**/TestOtherTask.class'
          exclude { it.name == 'TestExcludedBySpec.class' }
          systemProperty 'breakShardFixture', project.findProperty('breakShardFixture') ?: 'false'
          doFirst {
            def candidates = new TreeSet<String>()
            candidateClassFiles.visit { entry ->
              if (!entry.directory && entry.name.endsWith('.class')) {
                candidates.add(entry.relativePath.pathString)
              }
            }
            file('build/candidates.json').text = JsonOutput.toJson(candidates)
          }
        }
        tasks.register('otherTest', Test) {
          testClassesDirs = sourceSets.test.output.classesDirs
          classpath = sourceSets.test.runtimeClasspath
          useJUnitPlatform()
          include '**/TestOtherTask.class'
        }
    """.replace("DEPENDENCIES", dependencies))
    source_dir = root / "src/test/java/fixture"
    source_dir.mkdir(parents=True)
    sources = {
        "TestStructuredStreamingRead3": """
          @org.junit.jupiter.params.ParameterizedTest
          @org.junit.jupiter.params.provider.ValueSource(ints = {1, 2, 3})
          void parameterized(int value) { org.junit.jupiter.api.Assertions.assertTrue(value > 0); }
          @org.junit.jupiter.api.Nested class Nested {
            @org.junit.jupiter.api.Test void nested() {}
          }
        """,
        "TestAlpha": "@org.junit.jupiter.api.Test void alpha() {}",
        "TestBeta": "@org.junit.jupiter.api.Test void beta() {}",
        "TestDelta": "@org.junit.jupiter.api.Test void delta() {}",
        "TestNewlyAdded": "@org.junit.jupiter.api.Test void automaticallyDiscovered() {}",
        "TestInherited": "",
        "TestDynamic": """
          @org.junit.jupiter.api.TestFactory java.util.stream.Stream<org.junit.jupiter.api.DynamicTest> generated() {
            return java.util.stream.Stream.of("first", "second").map(name ->
                org.junit.jupiter.api.DynamicTest.dynamicTest(name, () -> {}));
          }
        """,
        "TestFailurePropagation": """
          @org.junit.jupiter.api.Test void failurePropagates() {
            org.junit.jupiter.api.Assertions.assertFalse(Boolean.getBoolean("breakShardFixture"));
          }
        """,
        "TestOtherTask": "@org.junit.jupiter.api.Test void unaffectedTask() {}",
        "TestExcludedByPattern": "@org.junit.jupiter.api.Test void excluded() { throw new AssertionError(); }",
        "TestExcludedBySpec": "@org.junit.jupiter.api.Test void excluded() { throw new AssertionError(); }",
        "NotIncluded": "@org.junit.jupiter.api.Test void excluded() { throw new AssertionError(); }",
        "TestExcludedByTag": """
          @org.junit.jupiter.api.Tag("excluded-tag")
          @org.junit.jupiter.api.Test void excluded() { throw new AssertionError(); }
        """,
    }
    for name, body in sources.items():
        superclass = " extends FixtureBase" if name == "TestInherited" else ""
        (source_dir / f"{name}.java").write_text(
            f"package fixture;\npublic class {name}{superclass} {{\n{body}\n}}\n")
    (source_dir / "FixtureBase.java").write_text("""
        package fixture;
        abstract class FixtureBase {
          @org.junit.jupiter.api.Test void inherited() {}
        }
    """)


def read_cases(root, task):
    cases = Counter()
    for report in (root / "build/test-results" / task).glob("TEST-*.xml"):
        for case in ET.parse(report).getroot().iter("testcase"):
            state = "failed" if case.find("failure") is not None else "passed"
            if case.find("skipped") is not None:
                state = "skipped"
            cases[case.attrib["classname"], case.attrib["name"], state] += 1
    return cases


def check(root, gradle, junit_classpath):
    create_fixture(root, junit_classpath)
    results = root / "results"
    results.mkdir()
    common = [gradle, "--project-dir", str(root), "--console=plain", "--no-daemon",
              "-Dorg.gradle.jvmargs=-Xmx256m", "--max-workers=2"]
    if junit_classpath:
        common.append("--offline")

    def run(label, extra=(), task="test", expect_failure=False):
        # Never let an UP-TO-DATE or NO-SOURCE task reuse another run's reports.
        reports = root / "build/test-results" / task
        if reports.exists():
            shutil.rmtree(reports)
        if task == "test":
            (root / "build/candidates.json").unlink(missing_ok=True)
        completed = subprocess.run(common + [task] + list(extra), text=True,
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        (results / f"{label}.log").write_text(completed.stdout)
        if expect_failure:
            if completed.returncode == 0:
                raise AssertionError(f"{label}: expected Gradle to fail")
        elif completed.returncode:
            raise AssertionError(f"{label} failed:\n{completed.stdout[-8000:]}")
        print(f"{label}: {'expected failure' if expect_failure else 'passed'}", flush=True)
        return read_cases(root, task)

    def shard_args(index, count=4, task=":test"):
        return ["--init-script", str(INIT_SCRIPT.resolve()), f"-PcometShardTask={task}",
                f"-PcometShardIndex={index}", f"-PcometShardCount={count}"]

    baseline = run("baseline")
    inventory = set(json.loads((root / "build/candidates.json").read_text()))
    assert sum(baseline.values()) == 12, baseline
    assert all(state == "passed" for _, _, state in baseline), baseline
    assert any(cls == "fixture.TestNewlyAdded" for cls, _, _ in baseline)
    assert any("$Nested" in cls for cls, _, _ in baseline)
    assert all("Excluded" not in cls and "NotIncluded" not in cls for cls, _, _ in baseline)

    combined_cases = Counter()
    combined_candidates = Counter()
    failure_owner = None
    for index in range(1, 5):
        cases = run(f"shard-{index}", shard_args(index))
        manifest = json.loads((root / f"build/comet-shards/test-{index}.json").read_text())
        candidates = json.loads((root / "build/candidates.json").read_text())
        assert candidates == manifest["candidates"]
        assert cases, f"empty fixture shard {index}"
        combined_cases.update(cases)
        combined_candidates.update(candidates)
        if any(cls == "fixture.TestFailurePropagation" for cls, _, _ in cases):
            failure_owner = index
        (results / f"shard-{index}.json").write_text(json.dumps(manifest, indent=2) + "\n")
    assert set(combined_candidates) == inventory, (set(combined_candidates), inventory)
    assert set(combined_candidates.values()) == {1}, combined_candidates
    assert combined_cases == baseline, (combined_cases, baseline)
    assert run("single-shard", shard_args(1, count=1)) == baseline

    other_baseline = run("other-baseline", task="otherTest")
    assert sum(other_baseline.values()) == 1
    assert run("other-unmodified", shard_args(1), task="otherTest") == other_baseline

    failed = run("failure-propagation", shard_args(failure_owner) + ["-PbreakShardFixture=true"],
                 expect_failure=True)
    assert any(cls == "fixture.TestFailurePropagation" and state == "failed"
               for cls, _, state in failed), failed
    run("invalid-index", shard_args(0), task="help", expect_failure=True)
    run("unknown-task", shard_args(1, task=":missing"), task="help", expect_failure=True)
    print(f"Verified {len(inventory)} candidate classes and {sum(baseline.values())} test cases: "
          "the four shards equal the unsharded inventory exactly once.", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gradle", default="gradle")
    parser.add_argument("--junit-classpath")
    parser.add_argument("--work-dir", type=Path)
    args = parser.parse_args()
    gradle = shutil.which(args.gradle)
    if not gradle:
        parser.error(f"Gradle executable not found: {args.gradle}")
    if args.work_dir:
        args.work_dir.mkdir(parents=True, exist_ok=False)
        check(args.work_dir.resolve(), gradle, args.junit_classpath)
    else:
        with tempfile.TemporaryDirectory(prefix="comet-iceberg-shards-") as tmp:
            check(Path(tmp), gradle, args.junit_classpath)


if __name__ == "__main__":
    main()
