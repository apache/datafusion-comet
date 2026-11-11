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

"""Compile current CometRegex sources and measure per-source admission rates.

Requires the default Spark 4.1 reactor's test dependency classpath. No Python
regex engine, scanner clone, or replacement SupportLevel types are used.
"""

import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import subprocess
from urllib.parse import unquote, urlparse


ROOT = Path(__file__).resolve().parents[2]
SOURCES = [
    ROOT / "spark/src/main/scala/org/apache/comet/expressions/CometRegex.scala",
    ROOT / "spark/src/main/scala/org/apache/comet/serde/SupportLevel.scala",
    Path(__file__).with_name("RegexCoverage.scala"),
]
MAIN_CLASS = "org.apache.comet.coverage.RegexCoverage"


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_jsonl(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True, ensure_ascii=True) + "\n")


def write_jsonl(path, rows):
    path.write_text("".join(json.dumps(row, sort_keys=True, ensure_ascii=True) + "\n"
                            for row in rows))


def summarize(rows, sources):
    def rate(admitted, total):
        return round(100.0 * admitted / total, 6) if total else None

    summaries = []
    for source in sources:
        group = [r for r in rows if r["source"] == source["id"]]
        eligible = [r for r in group if r["measurement_status"] == "eligible"]
        patterns = {}
        for row in eligible:
            value, decision = row["pattern_value"], row["admitted"]
            if value in patterns and patterns[value] != decision:
                raise ValueError("inconsistent admission decisions for an identical pattern")
            patterns[value] = decision
        admitted = sum(r["admitted"] for r in eligible)
        unique_admitted = sum(patterns.values())
        legacy = [r for r in eligible if "spark_escaped_literals_true" in r]
        summaries.append({
            "source": source["id"], "category": source["category"],
            "measurement_policy": source.get("measurement", {}),
            "observed_occurrences": len(group),
            "eligible_occurrences": len(eligible), "admitted_occurrences": admitted,
            "rejected_occurrences": len(eligible) - admitted,
            "occurrence_admission_percent": rate(admitted, len(eligible)),
            "unique_eligible_patterns": len(patterns),
            "unique_admitted_patterns": unique_admitted,
            "unique_rejected_patterns": len(patterns) - unique_admitted,
            "unique_admission_percent": rate(unique_admitted, len(patterns)),
            "excluded_occurrences": dict(sorted(Counter(
                r["measurement_status"] for r in group
                if r["measurement_status"] != "eligible").items())),
            "hive_crosschecked_occurrences": sum(
                r.get("hive_matches_spark_default", False) for r in eligible),
            "spark_legacy_decoding_changes": sum(
                r["spark_escaped_literals_true"]["decoding_differs"] for r in legacy),
            "spark_legacy_admission_changes": sum(
                r["spark_escaped_literals_true"]["java_valid"] and
                r["spark_escaped_literals_true"]["admitted"] != r["admitted"]
                for r in legacy),
            "spark_legacy_invalid_occurrences": sum(
                not r["spark_escaped_literals_true"]["java_valid"] for r in legacy),
        })
    return summaries


def prepare_requests(rows, sources):
    by_id = {source["id"]: source for source in sources}
    requests = []
    exclusions = {}
    for index, row in enumerate(rows):
        source = by_id[row["source"]]
        policy = source.get("measurement", {})
        classification = row.get("classification", "DIRECT_LITERAL" if
                                 row["status"] == "direct_literal_candidate" else "UNKNOWN")
        if classification not in {"DIRECT_LITERAL", "RESOLVED_CONSTANT"}:
            exclusions[index] = "not_a_direct_literal_candidate"
            continue
        if classification == "RESOLVED_CONSTANT" and not (
                policy.get("allow_resolved_constants") and row.get("constant_resolution_verified")):
            exclusions[index] = "unvalidated_resolved_constant"
            continue
        if "decoder" not in policy:
            exclusions[index] = policy.get("exclude", "no_measurement_policy")
            continue
        decoder = policy["decoder"]
        if decoder not in {"python-value", "spark-literal", "hive-literal-crosscheck"}:
            raise ValueError(f"unknown decoder: {decoder}")
        key = "pattern_value" if decoder == "python-value" else "literal_sql"
        if decoder == "python-value" and not row.get("api_binding_verified"):
            identity = (row.get("path"), row.get("line"), row.get("column"))
            reviewed = {(site["path"], site["line"], site["column"])
                        for site in source.get("reviewed_api_sites", [])}
            if identity not in reviewed:
                exclusions[index] = "unverified_python_api"
                continue
        value = row.get(key)
        if not isinstance(value, str):
            exclusions[index] = "missing_literal_value"
            continue
        requests.append({"input_index": index, "decoder": decoder, "value": value})
    return requests, exclusions


def run(input_path, manifest_path, classpath_path, output):
    output.mkdir(parents=True, exist_ok=True)
    manifest = json.loads(manifest_path.read_text())
    rows = read_jsonl(input_path)
    requests, exclusions = prepare_requests(rows, manifest["sources"])
    dependencies = classpath_path.read_text().strip().split(os.pathsep)
    libraries = [Path(p) for p in dependencies if Path(p).name.startswith("scala-library-")]
    if len(libraries) != 1:
        raise ValueError("classpath must contain exactly one Scala library")
    library = libraries[0]
    version = library.name[len("scala-library-"):-len(".jar")]
    if not version.startswith("2.13."):
        raise ValueError("use the default Spark 4.1 / Scala 2.13 test classpath")
    scala_repository = library.parent.parent.parent
    compiler = scala_repository / "scala-compiler" / version / f"scala-compiler-{version}.jar"
    reflect = scala_repository / "scala-reflect" / version / f"scala-reflect-{version}.jar"
    for path in [library, compiler, reflect]:
        if not path.is_file():
            raise FileNotFoundError(f"missing compiler dependency: {path}; compile Comet first")
    source_hashes = {str(p.relative_to(ROOT)): sha256(p) for p in SOURCES}
    build_key = hashlib.sha256(json.dumps(
        [source_hashes, sha256(compiler), sha256(classpath_path)], sort_keys=True).encode()).hexdigest()
    classes = output / "classes" / build_key[:16]
    classes.mkdir(parents=True, exist_ok=True)
    project_classes = [ROOT / "common/target/classes", ROOT / "spark/target/classes"]
    if not all(p.is_dir() for p in project_classes):
        raise FileNotFoundError("compile the whole Comet reactor before measurement")
    classpath = os.pathsep.join([str(p) for p in project_classes] + dependencies)
    compiler_classpath = os.pathsep.join(map(str, [compiler, library, reflect]))
    java_home = os.environ.get("JAVA_HOME")
    java = str(Path(java_home) / "bin/java") if java_home else "java"
    compile_command = [java, "-cp", compiler_classpath, "scala.tools.nsc.Main",
                       "-release", "17", "-classpath", classpath,
                       "-d", str(classes)] + [str(p) for p in SOURCES]
    with (output / "compile.log").open("w") as log:
        subprocess.run(compile_command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True)
    runtime_classpath = str(classes) + os.pathsep + classpath
    command = [java, "-cp", runtime_classpath, MAIN_CLASS]
    with (output / "component-checks.log").open("w") as log:
        subprocess.run(command + ["--self-test"], cwd=ROOT,
                       stdout=log, stderr=subprocess.STDOUT, check=True)
    request_path = output / "requests.jsonl"
    decision_path = output / "decisions.jsonl"
    runtime_path = output / "runtime.json"
    write_jsonl(request_path, requests)
    with (output / "measurement.log").open("w") as log:
        subprocess.run(command + list(map(str, [request_path, decision_path, runtime_path])),
                       cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True)
    runtime = json.loads(runtime_path.read_text())
    for key in ("analyzer_origin", "support_level_origin"):
        origin = urlparse(runtime[key])
        if origin.scheme != "file" or Path(unquote(origin.path)).resolve() != classes:
            raise ValueError("measurement loaded stale analyzer/support-level classes")
    decisions = read_jsonl(decision_path)
    expected_indices = {r["input_index"] for r in requests}
    by_index = {d["input_index"]: d for d in decisions}
    if set(by_index) != expected_indices or len(decisions) != len(expected_indices):
        raise ValueError("missing or duplicate JVM decisions")
    measured = []
    for index, row in enumerate(rows):
        if index in exclusions:
            result = {"measurement_status": exclusions[index]}
        else:
            result = dict(by_index[index])
            result["measurement_status"] = result.pop("status")
        measured.append({**row, **result})
    write_jsonl(output / "classified.jsonl", measured)
    summary = {"sources": summarize(measured, manifest["sources"]), "runtime": runtime,
               "input_sha256": sha256(input_path), "manifest_sha256": sha256(manifest_path),
               "measurement_tool_sha256": sha256(Path(__file__)),
               "classpath_sha256": sha256(classpath_path),
               "compiled_source_sha256": source_hashes,
               "compiler_sha256": sha256(compiler),
               "compiled_analyzer_sha256": sha256(
                   classes / "org/apache/comet/expressions/CometRegex$.class"),
               "git_head": subprocess.check_output(
                   ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
               "pooled_admission_rate": None}
    write_json(output / "summary.json", summary)
    for source in summary["sources"]:
        if source["observed_occurrences"]:
            print(json.dumps(source, sort_keys=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--manifest", type=Path,
                        default=Path(__file__).with_name("corpora_manifest.json"))
    parser.add_argument("--classpath-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    options = parser.parse_args()
    run(options.input.resolve(), options.manifest.resolve(),
        options.classpath_file.resolve(), options.output.resolve())
