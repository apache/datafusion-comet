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

from __future__ import annotations

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


def replace_block(body: str, lines: list[str]) -> str:
    """Return body with the marker block replaced by the given lines."""
    if BEGIN_MARKER not in body or END_MARKER not in body:
        raise ValueError("audit log file is missing marker comments")
    before, _, rest = body.partition(BEGIN_MARKER)
    _, _, after = rest.partition(END_MARKER)
    block_body = "\n".join(lines)
    if block_body:
        block_body = "\n" + block_body + "\n"
    else:
        block_body = "\n"
    return before + BEGIN_MARKER + block_body + END_MARKER + after


def enumerate_spark_commits(token: str, limit: int | None = None) -> list[dict]:
    """Enumerate in-scope Spark master commits since branch-4.2 was cut.

    Returns a list of {short, date, subject} dicts in chronological order.
    """
    from github import Github  # local import keeps the script importable without PyGithub installed

    gh = Github(token)
    repo = gh.get_repo("apache/spark")

    # Resolve the branch-4.2 cut point as the merge base of master and branch-4.2.
    print("Resolving branch-4.2 merge base...", file=sys.stderr)
    cmp = repo.compare("branch-4.2", "master")
    base_sha = cmp.merge_base_commit.sha
    print(f"merge base: {base_sha}", file=sys.stderr)

    # Walk master commits that touch sql/, newest first, until we hit the merge base.
    print("Listing sql/ commits on master...", file=sys.stderr)
    paginated = repo.get_commits(sha="master", path="sql/")

    candidates: list = []
    seen = 0
    for c in paginated:
        if c.sha == base_sha:
            break
        seen += 1
        if limit is not None and seen > limit:
            break
        candidates.append(c)

    print(f"fetched {len(candidates)} candidate commits", file=sys.stderr)

    # Filter by in-scope file paths. This is N extra API calls (one per commit).
    out: list[dict] = []
    for i, c in enumerate(candidates):
        if i % 50 == 0:
            print(f"filtering {i}/{len(candidates)}...", file=sys.stderr)
        files = [f.filename for f in c.files]
        if not is_in_scope(files):
            continue
        date_str = c.commit.author.date.strftime("%Y-%m-%d")
        subject = c.commit.message.split("\n", 1)[0]
        out.append(
            {
                "short": c.sha[:8],
                "date": date_str,
                "subject": subject,
            }
        )

    # Reverse to chronological order (oldest first).
    out.reverse()
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print the merged block to stdout instead of writing the file",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="only consider the most recent N candidate commits (for testing)",
    )
    parser.add_argument(
        "--audit-log",
        default=os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "spark-commit-audit.md"
        ),
        help="path to the audit log file (default: dev/spark-commit-audit.md)",
    )
    args = parser.parse_args()

    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        print("GITHUB_TOKEN environment variable is required", file=sys.stderr)
        return 2

    with open(args.audit_log, "r", encoding="utf-8") as f:
        body = f.read()
    existing = parse_existing_block(body)
    print(f"existing entries: {len(existing)}", file=sys.stderr)

    commits = enumerate_spark_commits(token, limit=args.limit)
    print(f"in-scope commits: {len(commits)}", file=sys.stderr)

    merged = merge_lines(commits, existing)
    new_body = replace_block(body, merged)

    if args.dry_run:
        sys.stdout.write(new_body)
    else:
        with open(args.audit_log, "w", encoding="utf-8") as f:
            f.write(new_body)
        print(f"wrote {args.audit_log} ({len(merged)} entries)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
