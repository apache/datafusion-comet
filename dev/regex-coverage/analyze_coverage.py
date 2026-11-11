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

"""Analyze measured structures and verify blockers/controls with CometRegex."""

import argparse
from collections import Counter
import contextlib
import itertools
import json
import math
import os
from pathlib import Path
import re
import subprocess
from urllib.parse import unquote, urlparse

import measure_coverage as measurement
from regex_structure import EXPERIMENTS, control_pattern, scan


def distribution(values):
    values = sorted(values)
    if not values:
        return {"n": 0, "min": None, "p50": None, "p90": None, "p99": None, "max": None}
    result = {"n": len(values), "min": values[0], "max": values[-1]}
    result.update({name: values[math.ceil(fraction * len(values)) - 1]
                   for name, fraction in (("p50", 0.5), ("p90", 0.9), ("p99", 0.99))})
    return result


def family(tag):
    if tag in {"ANCHOR_START", "ANCHOR_END"}:
        return "ANCHOR"
    if tag.startswith("SHORTHAND_"):
        return "SHORTHAND_CLASS"
    return tag


def summarize_group(rows):
    rejected = [r for r in rows if not r["admitted"]]
    complete = [r for r in rows if r["structure"]["complete"]]
    features = Counter(tag for r in rejected if r["structure"]["complete"]
                       for tag in r["structure"]["features"])
    pairs = Counter(" + ".join(pair)
                    for r in rejected if r["structure"]["complete"]
                    for pair in itertools.combinations(r["structure"]["features"], 2))
    signatures = Counter(" + ".join(r["structure"]["features"]) or "NONE_IDENTIFIED"
                         for r in rejected if r["structure"]["complete"])
    primary = Counter(r["primary_reason"] for r in rejected)
    assert sum(primary.values()) == len(rejected)
    controls = {}
    for name in EXPERIMENTS:
        tested = [r for r in rejected if name in r["controls"]]
        controls[name] = {
            "tested": len(tested),
            "syntax_controls_admitted": sum(r["controls"][name]["admitted"] for r in tested),
            "invalid_java_controls": sum(not r["controls"][name]["java_valid"] for r in tested),
        }
    metric_names = ("character_classes", "quantifiers", "groups", "max_group_depth",
                    "alternations", "max_quantifier_nesting", "max_counted_bound")
    metrics = {name: distribution(r["structure"]["metrics"][name] for r in complete)
               for name in metric_names}
    metrics["length_codepoints"] = distribution(len(r["pattern_value"]) for r in rows)
    metrics["scanner_expansion_admitted_only"] = distribution(
        r["scanner_expansion"] for r in rows if r["scanner_expansion"] is not None)
    return {
        "eligible": len(rows), "rejected": len(rejected), "complete_structures": len(complete),
        "unparsed_structures": len(rows) - len(complete),
        "ascii_patterns": sum(r["pattern_value"].isascii() for r in rows),
        "non_ascii_patterns": sum(not r["pattern_value"].isascii() for r in rows),
        "primary_reasons": dict(sorted(primary.items())),
        "verified_primary_blockers": sum(r["primary_verified"] for r in rejected),
        "unsupported_features_rejected": dict(sorted(features.items())),
        "feature_pairs_rejected": dict(sorted(pairs.items())),
        "feature_sets_rejected": dict(sorted(signatures.items())),
        "controls": controls, "metrics": metrics,
    }


def source_summaries(rows, sources):
    result = []
    for source in sources:
        group = [r for r in rows if r["source"] == source["id"]]
        unique = list({r["pattern_value"]: r for r in group}.values())
        result.append({"source": source["id"], "category": source["category"],
                       "occurrences": summarize_group(group), "unique_patterns": summarize_group(unique),
                       "by_admission": {
                           "admitted": summarize_group([r for r in group if r["admitted"]]),
                           "rejected": summarize_group([r for r in group if not r["admitted"]]),
                       }})
    return result


def run(input_path, manifest_path, classpath_path, reference_path, output, allow_new_records=False):
    output.mkdir(parents=True, exist_ok=True)
    baseline = output / "baseline"
    with (output / "baseline-console.log").open("w") as log, contextlib.redirect_stdout(log):
        measurement.run(input_path, manifest_path, classpath_path, baseline)
    rows = measurement.read_jsonl(baseline / "classified.jsonl")
    reference = measurement.read_jsonl(reference_path)
    keys = ("source", "path", "line", "column", "measurement_status", "pattern_value", "admitted")
    projection = lambda data: [tuple(r.get(k) for k in keys) for r in data]
    if allow_new_records:
        identity_keys = ("source", "path", "line", "column", "operator", "form",
                         "embedded_sql_line", "embedded_sql_column")
        identity = lambda row: tuple(row.get(k) for k in identity_keys)
        current = {identity(row): row for row in rows}
        if len(current) != len(rows):
            raise ValueError("ambiguous source record identity")
        for old in reference:
            new = current.get(identity(old))
            if new is None or projection([new]) != projection([old]):
                raise ValueError("previous source record changed eligibility, value or admission")
    elif projection(rows) != projection(reference):
        raise ValueError("measurement differs from the recorded admission baseline")
    eligible = [r for r in rows if r["measurement_status"] == "eligible"]
    patterns = sorted({r["pattern_value"] for r in eligible})
    structures = {p: scan(p) for p in patterns}
    probes = set(patterns)
    synthetic = {}
    for pattern, structure in structures.items():
        if not structure["complete"]:
            continue
        if structure["blockers"]:
            first = structure["blockers"][0]
            probes.add(pattern[:first["start"]])
            probes.add(pattern[:first["end"]])
        for name, selected in EXPERIMENTS.items():
            value = control_pattern(pattern, structure["blockers"], selected,
                                    leading_only=name == "leading_anchor")
            synthetic[pattern, name] = value
            probes.add(value)
    ordered = sorted(probes)
    requests = [{"input_index": i, "value": value} for i, value in enumerate(ordered)]
    request_path, response_path = output / "inspection-requests.jsonl", output / "inspection-results.jsonl"
    measurement.write_jsonl(request_path, requests)
    baseline_summary = json.loads((baseline / "summary.json").read_text())
    classes = Path(unquote(urlparse(baseline_summary["runtime"]["analyzer_origin"]).path))
    dependencies = classpath_path.read_text().strip()
    classpath = os.pathsep.join(map(str, [classes, measurement.ROOT / "common/target/classes",
                                         measurement.ROOT / "spark/target/classes"]))
    classpath += os.pathsep + dependencies
    java_home = os.environ.get("JAVA_HOME")
    java = str(Path(java_home) / "bin/java") if java_home else "java"
    with (output / "inspection.log").open("w") as log:
        subprocess.run([java, "-cp", classpath, measurement.MAIN_CLASS, "--inspect",
                        str(request_path), str(response_path)], cwd=measurement.ROOT,
                       stdout=log, stderr=subprocess.STDOUT, check=True)
    responses = measurement.read_jsonl(response_path)
    indices = [r["input_index"] for r in responses]
    if sorted(indices) != list(range(len(ordered))):
        raise ValueError("inspection responses are missing or duplicated")
    inspected = {ordered[r["input_index"]]: r for r in responses}
    analyzed = []
    for row in eligible:
        value = row["pattern_value"]
        actual = inspected[value]
        if actual["admitted"] != row["admitted"] or not actual["java_valid"]:
            raise ValueError("inspection disagrees with the measurement")
        structure = structures[value]
        primary, verified, witness = None, False, None
        if not row["admitted"]:
            primary = "UNATTRIBUTED"
            if structure["complete"] and structure["blockers"]:
                first = structure["blockers"][0]
                before, through = value[:first["start"]], value[:first["end"]]
                if (inspected[before]["java_valid"] and inspected[through]["java_valid"] and
                        inspected[before]["admitted"] and not inspected[through]["admitted"]):
                    primary, verified = family(first["tag"]), True
                    witness = {**first, "prefix_admitted": True, "prefix_through_token_admitted": False,
                               "prefix_java_valid": True, "prefix_through_token_java_valid": True}
        elif structure["blockers"]:
            raise ValueError("lexical blocker tags contradict an admitted pattern")
        controls = {}
        if not row["admitted"] and structure["complete"]:
            for name in EXPERIMENTS:
                control = synthetic[value, name]
                info = inspected[control]
                controls[name] = {"pattern_value": control, "java_valid": info["java_valid"],
                                  "admitted": info["admitted"] and info["java_valid"]}
        analyzed.append({**row, "structure": structure, "primary_reason": primary,
                         "primary_verified": verified, "primary_witness": witness,
                         "scanner_expansion": actual.get("structural_expansion"),
                         "controls": controls})
    measurement.write_jsonl(output / "analyzed.jsonl", analyzed)
    sources = json.loads(manifest_path.read_text())["sources"]
    analyzer_source = measurement.SOURCES[0].read_text()
    budgets = {}
    for name in ("MaxGroupDepth", "MaxCountedBound", "MaxExpansion", "MaxQuantifierNesting"):
        match = re.search(r"\bval " + name + r" = ([0-9]+)L?", analyzer_source)
        if not match:
            raise ValueError(f"analyzer budget definition changed: {name}")
        budgets[name] = int(match[1])
    summary = {
        "sources": source_summaries(analyzed, sources), "analyzer_budgets": budgets,
        "reference_decisions_unchanged": True,
        "reference_records": len(reference), "current_records": len(rows),
        "new_records_allowed": allow_new_records,
        "reference_classified_sha256": measurement.sha256(reference_path),
        "fresh_baseline_summary_sha256": measurement.sha256(baseline / "summary.json"),
        "compiled_source_sha256": baseline_summary["compiled_source_sha256"],
        "runtime": baseline_summary["runtime"],
        "analysis_tool_sha256": measurement.sha256(Path(__file__)),
        "lexical_tool_sha256": measurement.sha256(Path(__file__).with_name("regex_structure.py")),
        "control_experiments": {k: sorted(v) for k, v in EXPERIMENTS.items()},
        "primary_definition": "First lexical blocker with an admitted prefix and a rejected "
                              "prefix through the token, both Java-valid; not a Scanner error code.",
        "structural_expansion_scope": "Actual Scanner estimate for admitted patterns only; "
                                      "unknown for rejected patterns, not compiled bytes.",
        "percentile_method": "Nearest rank, separately by source and occurrence/unique mode.",
        "control_interpretation": "Synthetic syntax controls, not compatibility fixes or "
                                  "predicted additional native Rust admissions.",
        "pooled_workload_rate": None,
    }
    measurement.write_json(output / "analysis-summary.json", summary)
    for source in summary["sources"]:
        if source["occurrences"]["eligible"]:
            print(source["source"], json.dumps({key: source["occurrences"][key] for key in (
                "eligible", "rejected", "primary_reasons", "unparsed_structures", "controls")}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--manifest", type=Path,
                        default=Path(__file__).with_name("corpora_manifest.json"))
    parser.add_argument("--classpath-file", type=Path, required=True)
    parser.add_argument("--reference-baseline", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--allow-new-records", action="store_true",
                        help="Require old records to match, while permitting additional source records")
    args = parser.parse_args()
    run(args.input.resolve(), args.manifest.resolve(), args.classpath_file.resolve(),
        args.reference_baseline.resolve(), args.output.resolve(), args.allow_new_records)
