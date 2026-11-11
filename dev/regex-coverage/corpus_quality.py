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

"""Separate corpus populations; collection targets are not acceptance thresholds."""

import argparse
from collections import Counter
import json
from pathlib import Path

from measure_coverage import read_jsonl, sha256, write_json


def quality(rows, sources, non_rlike, targets):
    by_id = {s["id"]: s for s in sources}
    populations = {}
    for population in ("application", "toolkit", "engine_fixture"):
        scoped = [s for s in sources if s["population"] == population]
        ids = {s["id"] for s in scoped}
        observed = [r for r in rows if r["source"] in ids]
        eligible = [r for r in observed if r["measurement_status"] == "eligible"]
        eligible_sources = {r["source"] for r in eligible}
        populations[population] = {
            "source_entries": len(scoped),
            "inspected_repositories": len({s["repository"] for s in scoped}),
            "inspected_domains": sorted({s["domain"] for s in scoped}),
            "repositories_with_eligible_patterns": sorted({by_id[s]["repository"] for s in eligible_sources}),
            "eligible_independence_groups": sorted({by_id[s]["independence_group"] for s in eligible_sources}),
            "eligible_domains": sorted({by_id[s]["domain"] for s in eligible_sources}),
            "observed_rlike_candidates": len(observed), "eligible_occurrences": len(eligible),
            "unique_decoded_eligible_patterns": len({r["pattern_value"] for r in eligible}),
            "excluded_occurrences": len(observed) - len(eligible),
            "exclusion_percent": round(100.0 * (len(observed) - len(eligible)) / len(observed), 6)
                                 if observed else None,
            "exclusion_reasons": dict(sorted(Counter(r["measurement_status"] for r in observed
                                                     if r["measurement_status"] != "eligible").items())),
            "classifications": dict(sorted(Counter(r["classification"] for r in observed).items())),
            "non_rlike_audit_records": sum(r["source"] in ids for r in non_rlike),
            "admission_rate_pooled": None,
        }
    app = populations["application"]
    checks = {
        "application_independence_groups": len(app["eligible_independence_groups"]),
        "application_domains": len(app["eligible_domains"]),
        "application_unique_eligible_patterns": app["unique_decoded_eligible_patterns"],
    }
    return {"populations": populations, "collection_targets": {
        key: {"observed": value, "target": targets[key], "met": value >= targets[key]}
        for key, value in checks.items()}, "official_issue_threshold": False,
        "representativeness_established": False}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--classified", type=Path, required=True)
    parser.add_argument("--non-rlike", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, default=Path(__file__).with_name("corpora_manifest.json"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text())
    report = quality(read_jsonl(args.classified), manifest["sources"], read_jsonl(args.non_rlike),
                     manifest["collection_targets"])
    report.update(classified_sha256=sha256(args.classified), manifest_sha256=sha256(args.manifest))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    write_json(args.output, report)
    print(json.dumps(report, indent=2))
