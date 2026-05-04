#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Regenerate dev/spark-commit-audit.md.

Enumerates commits on Apache Spark master since branch-4.2 was cut that
touch the sql/ subtree (excluding sql/connect and sql/hive-thriftserver),
and writes them into the marker block of dev/spark-commit-audit.md.

Idempotent: existing verdicts and prose notes are preserved by short hash.
"""

import argparse
import os
import re
import sys


BEGIN_MARKER = "<!-- BEGIN AUDIT LIST -->"
END_MARKER = "<!-- END AUDIT LIST -->"

LINE_RE = re.compile(r"^- `([0-9a-f]{8})`")


def parse_existing_block(body: str) -> dict[str, str]:
    """Return a map of short-hash to full line for every entry in the block."""
    if BEGIN_MARKER not in body or END_MARKER not in body:
        raise ValueError(
            f"audit log is missing one or both markers: {BEGIN_MARKER!r}, {END_MARKER!r}"
        )
    start = body.index(BEGIN_MARKER) + len(BEGIN_MARKER)
    end = body.index(END_MARKER)
    block = body[start:end]
    result: dict[str, str] = {}
    for line in block.splitlines():
        line = line.rstrip()
        m = LINE_RE.match(line)
        if m:
            result[m.group(1)] = line
    return result


MAX_SUBJECT_LEN = 200


def format_new_line(*, short_hash: str, date: str, subject: str) -> str:
    """Format a fresh [needs-triage] entry."""
    if len(subject) > MAX_SUBJECT_LEN:
        subject = subject[: MAX_SUBJECT_LEN - 3] + "..."
    return f"- `{short_hash}` {date} [needs-triage] {subject}"


def is_in_scope(file_paths: list[str]) -> bool:
    """True when the commit touches sql/ outside of connect/thriftserver."""
    for path in file_paths:
        if not path.startswith("sql/"):
            continue
        if path.startswith("sql/connect/"):
            continue
        if path.startswith("sql/hive-thriftserver/"):
            continue
        return True
    return False


def merge_lines(commits: list[dict], existing: dict[str, str]) -> list[str]:
    """Merge a chronological commit list with existing audit lines.

    Existing entries are emitted verbatim. Commits not in the existing map
    get a fresh [needs-triage] line. Output preserves the order of `commits`.
    """
    result: list[str] = []
    for commit in commits:
        short = commit["short"]
        if short in existing:
            result.append(existing[short])
        else:
            result.append(
                format_new_line(
                    short_hash=short,
                    date=commit["date"],
                    subject=commit["subject"],
                )
            )
    return result
