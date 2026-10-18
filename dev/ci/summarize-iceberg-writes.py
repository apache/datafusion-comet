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

"""Summarize which writer ran the Iceberg writes of an Iceberg Spark test run.

The Iceberg Spark test jobs set COMET_ICEBERG_WRITE_REPORT_DIR, so Comet's
IcebergWriteReportListener appends one JSON line per Iceberg write to a file
in that directory. This prints how many writes ran on Comet's native writer,
how many Comet's split operator left on Iceberg's JVM writer and why, and how
many Spark planned without Comet's split operator:

    python3 dev/ci/summarize-iceberg-writes.py --title "iceberg-spark shard 1" DIR...

Files under a directory named like a shard artifact (...-shard-N-attempt-M)
are counted only for the latest attempt of each shard, so a rerun of failed
jobs does not count a shard twice. The latest attempt is the newest such
directory, whether or not it holds any report files, so a rerun that recorded
no writes is reported as missing rather than replaced by an earlier attempt.
The summary is also appended to $GITHUB_STEP_SUMMARY when that is set. It
never fails the job.
"""

import argparse
from collections import Counter
import json
import os
from pathlib import Path
import re


SHARD_ATTEMPT = re.compile(r"-shard-(\d+)-attempt-(\d+)$")
WRITERS = [
    ("native", "Comet native writer"),
    ("jvm", "Iceberg JVM writer under Comet's split operator"),
    ("spark", "Spark V2 write, not planned by Comet's split operator"),
]
TOP_REASONS = 20


def shard_attempt(path):
    """The (shard, attempt) of the shard artifact directory holding path, or None."""
    for part in path.parts:
        match = SHARD_ATTEMPT.search(part)
        if match:
            return int(match.group(1)), int(match.group(2))
    return None


def report_files(roots):
    """Every report file under roots, keeping only the latest attempt of each shard.

    Returns the files and the (shard, attempt) pairs whose latest attempt holds no report file.
    The latest attempt comes from the artifact directories rather than the report files, because
    an attempt that recorded no writes still uploads its shard inventory and test reports.
    """
    latest = {}
    files = []
    for root in map(Path, roots):
        for path in [root, *sorted(root.rglob("*"))]:
            key = shard_attempt(path)
            if key:
                latest[key[0]] = max(latest.get(key[0], 0), key[1])
            if path.suffix == ".jsonl" and path.is_file():
                files.append((path, key))
    kept = [(path, key) for path, key in files if key is None or latest[key[0]] == key[1]]
    reported = {key for _, key in kept}
    missing = sorted(key for key in latest.items() if key not in reported)
    return [path for path, _ in kept], missing


def load(roots):
    files, missing = report_files(roots)
    writes = []
    for path in files:
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                writes.append(json.loads(line))
    return writes, missing


def cell(text):
    return " ".join(text.split()).replace("|", "\\|")


def summarize(title, writes, missing=()):
    lines = [f"### Iceberg writes: {title}", ""]
    for shard, attempt in missing:
        lines += [
            f"Shard {shard} recorded no Iceberg writes in its latest attempt ({attempt}), "
            "so none of its writes are counted below.",
            "",
        ]
    if not writes:
        lines.append(
            "No Iceberg writes were recorded. Either the target ran none or "
            "COMET_ICEBERG_WRITE_REPORT_DIR did not reach the test JVMs."
        )
        return "\n".join(lines) + "\n"

    total = len(writes)
    by_writer = Counter(w["writer"] for w in writes)
    lines += ["| Writer | Writes | Share |", "| --- | ---: | ---: |"]
    for key, label in WRITERS:
        count = by_writer.get(key, 0)
        lines.append(f"| {label} | {count} | {100.0 * count / total:.1f}% |")
    lines += [f"| Total | {total} | |", ""]
    failed = sum(1 for w in writes if w.get("failed"))
    if failed:
        lines += [f"{failed} of the {total} writes ran in queries that failed.", ""]

    reasons = Counter()
    for w in writes:
        if w["writer"] == "jvm":
            for reason in w.get("reasons") or ["(no reason recorded)"]:
                reasons[reason] += 1
    if reasons:
        lines += [
            "#### Why the split operator kept the JVM writer",
            "",
            "A write can have several reasons, so the counts can add up to more than the "
            "JVM writes.",
            "",
            "| Writes | Reason |",
            "| ---: | --- |",
        ]
        for reason, count in reasons.most_common(TOP_REASONS):
            lines.append(f"| {count} | {cell(reason)} |")
        if len(reasons) > TOP_REASONS:
            lines.append(f"| | and {len(reasons) - TOP_REASONS} more reasons |")
        lines.append("")

    operators = Counter(w["node"] for w in writes if w["writer"] == "spark")
    if operators:
        lines += ["#### Spark V2 writes by operator", "", "| Writes | Operator |", "| ---: | --- |"]
        for node, count in operators.most_common():
            lines.append(f"| {count} | {cell(node)} |")
        lines.append("")

    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--title", required=True, help="heading for the summary")
    parser.add_argument("roots", nargs="+", help="directories holding the report files")
    args = parser.parse_args()

    summary = summarize(args.title, *load(args.roots))
    print(summary)
    step_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if step_summary:
        with open(step_summary, "a", encoding="utf-8") as out:
            out.write(summary + "\n")


if __name__ == "__main__":
    main()
