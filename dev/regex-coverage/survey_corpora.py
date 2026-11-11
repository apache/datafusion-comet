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

"""First-stage keyword inventory: never runs a repository or the analyzer."""

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import re
import tarfile


KEYWORDS = re.compile(r"\b(?:rlike|regexp_like|regexp|has_pattern)\b", re.IGNORECASE)


def run(manifest, archive_dir, output):
    output.mkdir(parents=True, exist_ok=True)
    summaries, evidence = [], []
    for source in manifest.get("surveys", []):
        counts = Counter()
        files = 0
        matched = set()
        digest = hashlib.sha256()
        with tarfile.open(archive_dir / source["archive"], "r:gz") as archive:
            root = source["archive_root"] + "/"
            members = sorted((m for m in archive.getmembers() if m.isfile()), key=lambda m: m.name)
            if any(not m.name.startswith(root) for m in members):
                raise ValueError("archive does not match pinned source root")
            licenses = [{"path": path, "sha256": hashlib.sha256(
                archive.extractfile(root + path).read()).hexdigest()}
                for path in source["license_paths"]]
            for member in members:
                path = member.name[len(root):]
                if not path.startswith(source["path_prefix"]) or not path.endswith(tuple(source["suffixes"])):
                    continue
                data = archive.extractfile(member).read()
                file_hash = hashlib.sha256(data).hexdigest()
                digest.update((path + "\0" + file_hash + "\n").encode())
                text = data.decode("utf-8")
                files += 1
                for line_number, line in enumerate(text.splitlines(), 1):
                    words = KEYWORDS.findall(line)
                    if words:
                        counts.update(w.lower() for w in words)
                        matched.add(path)
                        evidence.append({"survey": source["id"], "path": path,
                                         "line": line_number, "sha256": file_hash,
                                         "keywords": [w.lower() for w in words],
                                         "text_excerpt": line[:400],
                                         "url": f'https://github.com/{source["repository"]}/blob/'
                                                f'{source["commit"]}/{path}#L{line_number}'})
            reviewed = []
            for item in source.get("review_evidence", []):
                data = archive.extractfile(root + item["path"]).read()
                reviewed.append({**item, "sha256": hashlib.sha256(data).hexdigest()})
        summaries.append({"survey": source["id"], "repository": source["repository"],
                          "commit": source["commit"], "files_scanned": files,
                          "files_with_keywords": len(matched), "keyword_counts": dict(sorted(counts.items())),
                          "scope_sha256": digest.hexdigest(), "license_files": licenses,
                          "review_evidence": reviewed, "disposition": source["disposition"]})
    (output / "survey-summary.json").write_text(json.dumps(summaries, indent=2, sort_keys=True) + "\n")
    (output / "survey-evidence.jsonl").write_text("".join(
        json.dumps(row, sort_keys=True, ensure_ascii=True) + "\n" for row in evidence))
    print(json.dumps(summaries, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=Path(__file__).with_name("corpora_manifest.json"))
    parser.add_argument("--archive-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    run(json.loads(args.manifest.read_text()), args.archive_dir, args.output)
