<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Regex corpus discovery and measurement for #5812

## Findings, 2026-11-11

The application sample remains six Gentropy call sites with five distinct decoded
patterns: **1/6 occurrences admitted (16.67%) and 1/5 patterns admitted (20%)**.
The second survey round adds evidence about unavailable or ineligible samples,
plus one Sumeh toolkit test rule. It adds no eligible application patterns.
These results are exploratory and do not establish representative Spark workload
coverage. Comet's whitelist and JVM/Rust implementations are unchanged.

Read the [source survey](application-survey.md),
[admission results](baseline-results.md) and
[rejection/structure analysis](analysis-results.md) separately. Application,
Toolkit/Framework and Engine Fixture populations have separate denominators.
No pooled admission rate, execution-frequency estimate or speedup is reported.

| Population | Recognized RLIKE candidates | Eligible occurrences | Excluded | Unique eligible decoded patterns |
| --- | ---: | ---: | ---: | ---: |
| Application | 33 | 6 | 27 | 5 |
| Toolkit / Framework | 4 | 1 | 3 | 1 |
| Engine Fixture | 116 | 114 | 2 | 21 |

The table describes source extraction, not every regex in these repositories.
The Sumeh occurrence is a reviewed test-rule definition, not a direct call site.
Spark's inherited Hive fixtures overlap Hive upstream. The internal application
collection targets of three independent sources, three domains and 30 unique
eligible patterns remain unmet: observed values are **1 / 1 / 5**. They are not
issue acceptance requirements or a statistical representativeness test.

## Extraction and eligibility

[`corpora_manifest.json`](corpora_manifest.json) pins commits, scopes, populations,
licenses, known biases, consumer reviews and measurement policies. Discovery reads
tar members without importing or executing upstream code. Every scoped file has
a content hash; skipped files and Python warnings are reported.

The SQL lexer recognizes infix RLIKE/REGEXP and function-form RLIKE aliases.
Literal spellings remain undecoded until the JVM driver uses actual Spark/Hive
literal decoders and Java regex validation. Only the real
`CometRegex.supportLevel(pattern, RegexFlavor.RLike)` decides admission.

Python AST extraction now supports:

- `Column.rlike("pattern")`, with manifest-reviewed receiver semantics before measurement.
- Verified `pyspark.sql.functions.rlike/regexp/regexp_like` calls whose second
  argument is a verified `functions.lit()` containing a string.
- Literal concatenation and earlier, single-assignment module string constants,
  classified as `RESOLVED_CONSTANT`; measurement requires explicit opt-in.
- Source-specific, manually reviewed rule constructors with a verified PySpark
  consumer. Currently only one Sumeh test definition is enabled.

`F.rlike("name", "regex")` treats `"regex"` as a column name and never as a
pattern literal. Reassignment, shadowing, wildcard imports and detected namespace
mutation disable binding proofs. Analysis is intentionally conservative and
file-wide, so it can miss safe uses. It is not a general Python type checker or
whole-program dataflow engine. It does not resolve function arguments, CLI
inputs, cross-function values or arbitrary settings.

Every recognized predicate receives `DIRECT_LITERAL`, `RESOLVED_CONSTANT`,
`DYNAMIC_PATTERN` or `UNKNOWN`. `NON_RLIKE_REGEX` calls are written to a separate
audit file. Dynamic f-strings and `.format()` SQL carriers are inventoried without
expansion; fixed-looking RHS text in an unverified dynamic context is `UNKNOWN`.
Generic embedded `F.expr`, Scala, notebooks and YAML/JSON consumers are not
promoted by this extractor. The fast survey searches such files for keywords
without interpreting them as patterns.

The original SQL scopes contain 25 lexer failures, all with zero raw whole-word
predicate keywords. CMSSpark has three Python parse failures, also with zero raw
predicate keywords. Zero hits or failures never prove a project has no runtime
regex. Unique spellings are distinct from unique decoded pattern values.

## Reproduce

Use Python 3.10+, curl, the Comet build prerequisites and a compatible JDK. The
Python tools use only the standard library. From the repository root, fetch each
pinned archive and the additional consumer/license evidence:

```sh
python3 - <<'PYCODE'
import hashlib
import json
from pathlib import Path
import subprocess

manifest = json.loads(Path("dev/regex-coverage/corpora_manifest.json").read_text())
archives = Path("target/regex-coverage-round2/archives")
archives.mkdir(parents=True, exist_ok=True)
sources = {s["archive"]: s for s in manifest["sources"] + manifest["surveys"]}
for filename, source in sorted(sources.items()):
    destination = archives / filename
    if not destination.exists():
        url = f'https://codeload.github.com/{source["repository"]}/tar.gz/{source["commit"]}'
        subprocess.run(["curl", "-L", "--fail", "--silent", "--show-error",
                        url, "-o", str(destination)], check=True)
for item in manifest["validation_evidence"]:
    destination = archives / item["cache_file"]
    if not destination.exists():
        url = f'https://raw.githubusercontent.com/{item["repository"]}/{item["commit"]}/{item["path"]}'
        subprocess.run(["curl", "-L", "--fail", "--silent", "--show-error",
                        url, "-o", str(destination)], check=True)
    assert hashlib.sha256(destination.read_bytes()).hexdigest() == item["sha256"]
PYCODE

python3 dev/regex-coverage/survey_corpora.py \
  --archive-dir target/regex-coverage-round2/archives \
  --output target/regex-coverage-round2/survey
python3 dev/regex-coverage/probe_corpora.py \
  --archive-dir target/regex-coverage-round2/archives \
  --output target/regex-coverage-round2/probe

make core
./mvnw test dependency:build-classpath -Pspark-4.1 -Dtest=none \
  -Dsuites=org.apache.comet.expressions.CometRegexSuite \
  -Dmdep.outputFile=target/regex-coverage-classpath.txt
python3 dev/regex-coverage/measure_coverage.py \
  --input target/regex-coverage-round2/probe/occurrences.jsonl \
  --classpath-file spark/target/regex-coverage-classpath.txt \
  --output target/regex-coverage-round2/baseline
python3 dev/regex-coverage/analyze_coverage.py \
  --input target/regex-coverage-round2/probe/occurrences.jsonl \
  --classpath-file spark/target/regex-coverage-classpath.txt \
  --reference-baseline target/regex-coverage-round2/baseline/classified.jsonl \
  --output target/regex-coverage-round2/analysis
python3 dev/regex-coverage/audit_existing_sources.py \
  --probe target/regex-coverage-round2/probe \
  --baseline target/regex-coverage-round2/analysis/baseline \
  --classpath-file spark/target/regex-coverage-classpath.txt \
  --archive-dir target/regex-coverage-round2/archives \
  --output target/regex-coverage-round2/validation
python3 dev/regex-coverage/corpus_quality.py \
  --classified target/regex-coverage-round2/analysis/baseline/classified.jsonl \
  --non-rlike target/regex-coverage-round2/probe/non-rlike-regex.jsonl \
  --output target/regex-coverage-round2/corpus-quality.json
python3 -m unittest discover -s dev/regex-coverage -p 'test_*.py' -v
```

The probe does not fetch missing archives. Local runs used the same pinned inputs
cached in `/private/tmp/regex5812-*`; that path can replace `--archive-dir`.
Use the whole Maven reactor without `-pl`. The JVM driver compiles the unchanged
current analyzer and real support types into an isolated directory, checks loaded
origins, and invokes no native regex kernel. See the baseline for runtime details.

Generated artifacts remain in ignored `target/`:

- `survey/`: first-stage keyword inventory, reviewed-source and license hashes.
- `probe/`: 153 predicate records, a separate 392-record non-RLIKE audit,
  inspected-file hashes, counts, skipped files and warnings.
- `baseline/` or `analysis/baseline/`: classified records, decoder/compiler inputs,
  component checks, runtime origins and provenance.
- `analysis/`: primary blocker witnesses, overlapping tags, length/structure
  distributions and separate synthetic controls.
- `validation/`: Wikimedia parser results and CMS template/default audits.
- `corpus-quality.json`: population exclusions and observed collection targets.

The 139 records from the previous round retain their eligibility, decoded values
and admission decisions. The expanded set has 121 eligible and 32 excluded records;
these totals are bookkeeping across separate populations. All 46 Python tests and
the JVM component checks pass. Two discovery runs produce byte-identical files;
repeated measurement and analysis preserve their recorded hashes. The occurrence
JSONL SHA-256 is
`ed0ccf12c3c1d7c6ef07ae60ab245878c0c89acf21cbb62baeb5c89a7d2f9f4f`.
The original `CometRegexSuite` passed 14 tests during the
baseline stage; production code has not changed since that run.

## Licensing and limits

The manifest records Apache-2.0 for Spark, Hive, Gentropy, Fink, Cuallee,
SparkUtils Quality and Sumeh, and MIT for Mozilla ETL and CMSSpark, with pinned
license evidence for added sources. Preserve applicable notices before checking
in a derived third-party corpus. This directory contains tools, provenance and
aggregate findings, not upstream SQL datasets.

No root LICENSE/COPYING was found in the pinned `analytics-refinery` snapshot.
The separate `analytics-refinery-source` license explicitly covers that source
project; it does not establish redistribution terms for the deployment repository.
Wikimedia outputs stay local and excluded. Deployment-specific SQL settings and
template preprocessing remain unverified. Databricks Labs sources with restrictive
service-specific licenses were not added.

A bounded exploratory PR can present these results and the documented sampling
limitations for maintainer review. The data does not justify a claim of ecosystem
coverage, a proposed whitelist relaxation, or closing #5812 without that review.
