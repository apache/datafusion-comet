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

"""Audit Wikimedia literals/settings and CMS dynamic inputs without promotion."""

import argparse
import ast
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
from urllib.parse import unquote, urlparse

import measure_coverage as measurement


def run(manifest, probe, baseline, archives, output, classpath_path):
    output.mkdir(parents=True, exist_ok=True)
    sources = {s["id"]: s for s in manifest["sources"]}
    rows = measurement.read_jsonl(probe / "occurrences.jsonl")
    evidence = []
    for item in manifest["validation_evidence"]:
        path = archives / item["cache_file"]
        if measurement.sha256(path) != item["sha256"]:
            raise ValueError("external validation evidence does not match pinned content")
        evidence.append(item)
    wiki = [r for r in rows if r["source"] == "wikimedia-refinery"]
    requests = [{"input_index": i, "decoder": "spark-literal", "value": r["literal_sql"]}
                for i, r in enumerate(wiki)]
    request_path, decision_path = output / "wiki-requests.jsonl", output / "wiki-decisions.jsonl"
    measurement.write_jsonl(request_path, requests)
    summary = json.loads((baseline / "summary.json").read_text())
    classes = unquote(urlparse(summary["runtime"]["analyzer_origin"]).path)
    classpath = os.pathsep.join([classes, str(measurement.ROOT / "common/target/classes"),
                                str(measurement.ROOT / "spark/target/classes"),
                                classpath_path.read_text().strip()])
    java_home = os.environ.get("JAVA_HOME")
    java = str(Path(java_home) / "bin/java") if java_home else "java"
    with (output / "wiki-parser.log").open("w") as log:
        subprocess.run([java, "-cp", classpath, measurement.MAIN_CLASS, str(request_path),
                        str(decision_path), str(output / "wiki-runtime.json")],
                       cwd=measurement.ROOT, stdout=log, stderr=subprocess.STDOUT, check=True)
    decisions = measurement.read_jsonl(decision_path)
    if [r["input_index"] for r in decisions] != list(range(len(wiki))):
        raise ValueError("missing or reordered Wikimedia decoder results")
    audited = []
    source = sources["wikimedia-refinery"]
    with tarfile.open(archives / source["archive"], "r:gz") as archive:
        root = source["archive_root"] + "/"
        root_licenses = [m.name for m in archive.getmembers() if m.isfile() and
                         m.name.startswith(root) and "/" not in m.name[len(root):] and
                         re.match(r"(?i)(license|copying)(\.|$)", m.name[len(root):])]
        for row, decision in zip(wiki, decisions):
            data = archive.extractfile(root + row["path"]).read()
            if hashlib.sha256(data).hexdigest() != row["file_sha256"]:
                raise ValueError("Wikimedia input file hash changed")
            text = data.decode("utf-8")
            if row["literal_sql"] not in text.splitlines()[row["line"] - 1]:
                raise ValueError("Wikimedia literal no longer at recorded location")
            if decision["status"] != "eligible":
                state = "literal_decoder_or_java_failure"
            elif "$$" in row["literal_sql"]:
                state = "dollar_template_preprocessing_unverified"
            elif decision["spark_escaped_literals_true"]["decoding_differs"]:
                state = "deployment_escape_setting_unverified"
            else:
                state = "source_literal_settings_invariant"
            audited.append({**row, "local_validation": state,
                            "parser_result": decision,
                            "file_template_parameters": sorted(set(re.findall(r"\$\{([^}]+)\}", text))),
                            "promoted_to_baseline": False, "redistribution_verified": False})
    cms = [r for r in rows if r["source"] == "cms-spark-survey"]
    defaults = []
    source = sources["cms-spark-survey"]
    with tarfile.open(archives / source["archive"], "r:gz") as archive:
        for path in sorted({r["path"] for r in cms}):
            data = archive.extractfile(source["archive_root"] + "/" + path).read()
            text = data.decode("utf-8")
            names = {h["expression"] for r in cms if r["path"] == path for h in r["pattern_holes"]}
            for node in ast.walk(ast.parse(text)):
                if isinstance(node, ast.FunctionDef):
                    args = node.args.posonlyargs + node.args.args
                    for arg, value in zip(args[len(args) - len(node.args.defaults):], node.args.defaults):
                        if arg.arg in names and isinstance(value, ast.Constant) and isinstance(value.value, str):
                            defaults.append({"path": path, "line": value.lineno,
                                             "kind": "function_default", "parameter": arg.arg,
                                             "value_literal": value.value, "query_occurrence": False})
                elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "option":
                    keys = {k.arg: k.value for k in node.keywords}
                    value = keys.get("default")
                    options = [a.value for a in node.args if isinstance(a, ast.Constant) and isinstance(a.value, str)]
                    if any(o.lstrip("-") in names for o in options) and isinstance(value, ast.Constant) and isinstance(value.value, str):
                        defaults.append({"path": path, "line": value.lineno,
                                         "kind": "cli_input_default", "parameter": options,
                                         "value_literal": value.value, "query_occurrence": False})
    measurement.write_jsonl(output / "wikimedia-literal-audit.jsonl", audited)
    measurement.write_jsonl(output / "cms-dynamic-audit.jsonl", cms)
    measurement.write_json(output / "cms-default-definitions.json", defaults)
    report = {
        "wikimedia": {"candidates": len(wiki), "validation_stages": dict(sorted(Counter(
            r["local_validation"] for r in audited).items())), "root_license_files": root_licenses,
            "baseline_promotions": 0, "deployed_sql_settings_verified": False,
            "redistribution_verified": False},
        "cms": {"intended_rlike_locations": len(cms), "classifications": dict(Counter(
            r["classification"] for r in cms)), "files": len({r["path"] for r in cms}),
            "default_definition_records": len(defaults), "defaults_used_as_query_occurrences": 0},
        "evidence": evidence, "probe_sha256": measurement.sha256(probe / "occurrences.jsonl"),
        "classpath_sha256": measurement.sha256(classpath_path),
        "baseline_summary_sha256": measurement.sha256(baseline / "summary.json"),
    }
    measurement.write_json(output / "validation-summary.json", report)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=Path(__file__).with_name("corpora_manifest.json"))
    parser.add_argument("--probe", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--archive-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--classpath-file", type=Path, required=True)
    args = parser.parse_args()
    run(json.loads(args.manifest.read_text()), args.probe.resolve(), args.baseline.resolve(),
        args.archive_dir.resolve(), args.output.resolve(), args.classpath_file.resolve())
