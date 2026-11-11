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

"""Inventory SQL literals and Python constants at potential PySpark RLike sites.

This is a lexical discovery tool, not a SQL parser or a CometRegex replacement.
It reads pinned tarballs without extracting them or executing upstream code.
"""

import argparse
from bisect import bisect_right
from collections import Counter
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
import sys
import tarfile
import warnings

from python_patterns import inventory as python_inventory


PREDICATES = {"rlike", "regexp", "regexp_like"}
OTHER_REGEX = {
    "regexp_extract", "regexp_extract_all", "regexp_replace", "regexp_count",
    "regexp_substr", "regexp_instr",
}
TEMPLATE = re.compile(r"\$\{|\{\{|\{%")
# An infix RHS that continues with an expression needs a real parser.
LITERAL_FOLLOWERS = {
    ",", ")", ";", "and", "or", "then", "else", "end", "as", "from",
    "where", "group", "order", "having", "limit", "union", "when", "is",
    "=", "<", ">", "!",
}


@dataclass(frozen=True)
class Token:
    kind: str
    text: str
    start: int


def tokenize(sql):
    """Skip comments and retain quoted strings as indivisible tokens."""
    tokens = []
    i = 0
    while i < len(sql):
        c = sql[i]
        if c.isspace():
            i += 1
        elif sql.startswith("--", i):
            end = sql.find("\n", i)
            i = len(sql) if end == -1 else end + 1
        elif sql.startswith("/*", i):
            depth = 1
            i += 2
            while i < len(sql) and depth:
                if sql.startswith("/*", i):
                    depth += 1
                    i += 2
                elif sql.startswith("*/", i):
                    depth -= 1
                    i += 2
                else:
                    i += 1
            if depth:
                raise ValueError("unterminated block comment")
        elif c == "$" and (tag := re.match(r"\$(?:[A-Za-z_][A-Za-z_0-9]*)?\$", sql[i:])):
            marker = tag.group()
            end = sql.find(marker, i + len(marker))
            if end == -1:
                raise ValueError("unterminated dollar-quoted string")
            stop = end + len(marker)
            tokens.append(Token("opaque", sql[i:stop], i))
            i = stop
        elif c in "'\"`" or (c in "rR" and i + 1 < len(sql) and sql[i + 1] in "'\""):
            start = i
            raw = c in "rR"
            quote = sql[i + 1] if raw else c
            i += 2 if raw else 1
            closed = False
            while i < len(sql):
                if sql[i] == "\\" and not raw and quote != "`":
                    i += 2
                elif sql[i] == quote:
                    i += 1
                    if not raw and i < len(sql) and sql[i] == quote:
                        i += 1
                    else:
                        closed = True
                        break
                else:
                    i += 1
            if not closed:
                raise ValueError("unterminated quoted token")
            kind = "identifier" if quote == "`" else "string"
            tokens.append(Token(kind, sql[start:i], start))
        elif c.isalpha() or c == "_":
            start = i
            i += 1
            while i < len(sql) and (sql[i].isalnum() or sql[i] == "_"):
                i += 1
            tokens.append(Token("word", sql[start:i], start))
        else:
            tokens.append(Token("symbol", c, i))
            i += 1
    return tokens


def second_argument(tokens, opening):
    """Return second-argument token bounds, respecting nested function calls."""
    depth = 0
    commas = []
    for j in range(opening, len(tokens)):
        token = tokens[j]
        if token.kind != "symbol":
            continue
        if token.text in {"(", "["}:
            depth += 1
        elif token.text in {")", "]"}:
            depth -= 1
            if depth == 0:
                if not commas:
                    return []
                return tokens[commas[0] + 1:commas[1] if len(commas) > 1 else j]
        elif token.text == "," and depth == 1:
            commas.append(j)
    return []


def discover(sql, audit=None):
    tokens = tokenize(sql)
    newlines = [i for i, c in enumerate(sql) if c == "\n"]
    rows = []
    other = Counter()
    for i, token in enumerate(tokens):
        name = token.text.lower()
        if token.kind != "word" or name not in PREDICATES | OTHER_REGEX:
            continue
        previous = tokens[i - 1].text.lower() if i else ""
        following = tokens[i + 1] if i + 1 < len(tokens) else None
        if previous == "." or previous in {"function", "extended"}:
            continue
        function = following is not None and following.text == "("
        if name in OTHER_REGEX:
            if function:
                other[name] += 1
                if audit is not None:
                    audit.append({"line": bisect_right(newlines, token.start) + 1,
                                  "column": token.start - (newlines[bisect_right(newlines, token.start) - 1]
                                                            if bisect_right(newlines, token.start) else -1),
                                  "operator": name, "form": "sql-function",
                                  "classification": "NON_RLIKE_REGEX", "status": "non_rlike_regex"})
            continue
        if function:
            args = second_argument(tokens, i + 1)
        elif name in {"rlike", "regexp"}:
            args = [following] if following else []
        else:
            continue
        row = {
            "line": bisect_right(newlines, token.start) + 1,
            "column": token.start - (newlines[bisect_right(newlines, token.start) - 1]
                                      if bisect_right(newlines, token.start) else -1),
            "operator": name,
            "form": "function" if function else "infix",
        }
        literal = args[0] if len(args) == 1 and args[0].kind == "string" else None
        continuation = tokens[i + 2].text.lower() if i + 2 < len(tokens) else None
        if literal is None:
            row["status"] = "non_direct_literal_or_unparsed"
        elif not function and continuation is not None and continuation not in LITERAL_FOLLOWERS:
            row["status"] = "rhs_expression_needs_parser"
            row["literal_sql"] = literal.text
        elif TEMPLATE.search(literal.text):
            row["status"] = "templated_literal"
            row["literal_sql"] = literal.text
        else:
            row["status"] = "direct_literal_candidate"
            row["literal_sql"] = literal.text
        row["classification"] = ("DIRECT_LITERAL" if row["status"] == "direct_literal_candidate"
                                 else "DYNAMIC_PATTERN" if row["status"] == "non_direct_literal_or_unparsed"
                                 else "UNKNOWN")
        rows.append(row)
    return rows, other


def discover_pyspark(text, audit=None, include_templates=False, non_rlike_methods=(), rule_constructors=()):
    """Inventory proven function bindings and untyped Column-method candidates."""
    carriers = [] if include_templates else None
    found = python_inventory(text, audit_templates=carriers, non_rlike_methods=non_rlike_methods,
                             rule_constructors=rule_constructors)
    for carrier in carriers or []:
        embedded, _ = discover(carrier["template_text"])
        for item in embedded:
            expression = item.get("literal_sql", "")
            dynamic = any(hole["marker"] in expression for hole in carrier["template_holes"])
            # Even a constant-looking RHS is unproved when a dynamic hole elsewhere
            # could change SQL lexical context. These audit records never carry values.
            found.append({"line": carrier["line"], "column": carrier["column"],
                          "embedded_sql_line": item["line"], "embedded_sql_column": item["column"],
                          "operator": item["operator"], "form": carrier["form"],
                          "classification": "DYNAMIC_PATTERN" if dynamic else "UNKNOWN",
                          "status": "non_direct_literal_or_unparsed", "api_binding_verified": False,
                          "extraction_reason": "runtime_pattern_hole" if dynamic else "dynamic_sql_context",
                          "template_holes": carrier["template_holes"],
                          "pattern_holes": [h for h in carrier["template_holes"] if h["marker"] in expression],
                          "host_expression": carrier["host_expression"]})
    found.sort(key=lambda r: (r["line"], r["column"], r.get("embedded_sql_line", 0),
                              r.get("embedded_sql_column", 0)))
    non_rlike = [r for r in found if r["classification"] == "NON_RLIKE_REGEX"]
    if audit is not None:
        audit.extend(non_rlike)
    return ([r for r in found if r["classification"] != "NON_RLIKE_REGEX"],
            Counter(r["operator"] for r in non_rlike))


def literal_spelling(row):
    # SQL spelling and Python source spelling deliberately remain distinct.
    return row.get("literal_sql", row.get("literal_python"))


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=True) + "\n")


def run(manifest, archive_dir, output):
    output.mkdir(parents=True, exist_ok=True)
    all_rows = []
    all_other = []
    summaries = []
    files = []
    errors = []
    parse_warnings = []
    for source in manifest["sources"]:
        rows = []
        source_other = []
        other = Counter()
        scanned = 0
        raw_mentions = 0
        licenses = []
        extractor = source.get("extractor", "sql-lexical")
        if extractor not in {"sql-lexical", "pyspark-method-ast"}:
            raise ValueError(f"unknown extractor: {extractor}")
        digest = hashlib.sha256()
        archive_path = archive_dir / source["archive"]
        with tarfile.open(archive_path, "r:gz") as archive:
            root = source["archive_root"] + "/"
            members = sorted((m for m in archive.getmembers() if m.isfile()), key=lambda m: m.name)
            if any(not m.name.startswith(root) for m in members):
                raise ValueError(f"archive root does not match pinned commit: {archive_path}")
            for path in source.get("license_paths", []):
                license_bytes = archive.extractfile(root + path).read()
                licenses.append({"path": path, "sha256": hashlib.sha256(license_bytes).hexdigest(),
                                 "url": f'https://github.com/{source["repository"]}/blob/'
                                        f'{source["commit"]}/{path}'})
            for member in members:
                path = member.name[len(root):]
                if not (path.startswith(source["path_prefix"]) and path.endswith(source["suffix"])):
                    continue
                data = archive.extractfile(member).read()
                content_hash = hashlib.sha256(data).hexdigest()
                digest.update((path + "\0" + content_hash + "\n").encode())
                scanned += 1
                raw_count = len(re.findall(
                    rb"\b(?:rlike|regexp|regexp_like)\b", data, re.IGNORECASE))
                raw_mentions += raw_count
                try:
                    discoverer = discover if extractor == "sql-lexical" else discover_pyspark
                    audit = []
                    with warnings.catch_warnings(record=True) as captured:
                        warnings.simplefilter("always")
                        if extractor == "sql-lexical":
                            found, extra = discoverer(data.decode("utf-8"), audit=audit)
                        else:
                            found, extra = discoverer(
                                data.decode("utf-8"), audit=audit,
                                include_templates=source.get("include_sql_templates", False),
                                non_rlike_methods=source.get("non_rlike_methods", []),
                                rule_constructors=source.get("reviewed_rule_constructors", []))
                    for warning in captured:
                        parse_warnings.append({"source": source["id"], "path": path,
                                               "line": warning.lineno,
                                               "category": warning.category.__name__,
                                               "message": str(warning.message)})
                except (UnicodeDecodeError, ValueError, SyntaxError) as error:
                    # A raw keyword audit makes potentially lost candidates visible;
                    # it is not used as a substitute for token-based extraction.
                    errors.append({
                        "source": source["id"], "path": path, "error": str(error),
                        "raw_predicate_keyword_count": raw_count,
                    })
                    continue
                other.update(extra)
                if found or extra or raw_count:
                    files.append({"source": source["id"], "path": path,
                                  "sha256": content_hash, "predicate_mentions": len(found),
                                  "raw_predicate_keyword_count": raw_count,
                                  "other_regex_calls": dict(sorted(extra.items()))})
                for row in found + audit:
                    row.update(source=source["id"], path=path, file_sha256=content_hash,
                               url=f'https://github.com/{source["repository"]}/blob/'
                                   f'{source["commit"]}/{path}#L{row["line"]}')
                rows.extend(found)
                source_other.extend(audit)
        candidates = [r for r in rows if r["status"] == "direct_literal_candidate"]
        spellings = Counter(literal_spelling(r) for r in candidates)
        summaries.append({
            "source": source["id"], "repository": source["repository"],
            "commit": source["commit"], "category": source["category"],
            "extractor": extractor, "license_files": licenses,
            "scanned_files": scanned, "scoped_content_sha256": digest.hexdigest(),
            "raw_predicate_keyword_count": raw_mentions,
            "skipped_files": sum(e["source"] == source["id"] for e in errors),
            "files_with_candidates": len({r["path"] for r in candidates}),
            "predicate_mentions": len(rows), "direct_literal_occurrences": len(candidates),
            "unique_literal_spellings": len(spellings),
            "resolved_constant_occurrences": sum(r["classification"] == "RESOLVED_CONSTANT" for r in rows),
            "rlike_classification_counts": dict(sorted(Counter(r["classification"] for r in rows).items())),
            "non_rlike_occurrences": len(source_other),
            "status_counts": dict(sorted(Counter(r["status"] for r in rows).items())),
            "operators": dict(sorted(Counter(r["operator"] for r in candidates).items())),
            "other_regex_calls": dict(sorted(other.items())),
            "top_literal_spellings": spellings.most_common(10),
            "unique_python_pattern_values": len({r["pattern_value"] for r in candidates
                                                  if "pattern_value" in r}),
        })
        all_rows.extend(rows)
        all_other.extend(source_other)
    with (output / "occurrences.jsonl").open("w") as stream:
        for row in all_rows:
            stream.write(json.dumps(row, sort_keys=True, ensure_ascii=True) + "\n")
    with (output / "non-rlike-regex.jsonl").open("w") as stream:
        for row in all_other:
            stream.write(json.dumps(row, sort_keys=True, ensure_ascii=True) + "\n")
    write_json(output / "summary.json", {"sources": summaries, "lexical_errors": errors,
                                        "parse_warnings": parse_warnings,
                                        "python_version": sys.version.split()[0],
                                        "admission_measured": False})
    write_json(output / "files.json", files)
    print(json.dumps({"sources": summaries, "lexical_errors": errors,
                      "parse_warnings": parse_warnings}, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=Path(__file__).with_name("corpora_manifest.json"))
    parser.add_argument("--archive-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    options = parser.parse_args()
    run(json.loads(options.manifest.read_text()), options.archive_dir, options.output)
