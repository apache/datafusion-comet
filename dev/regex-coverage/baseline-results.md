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

# Exploratory CometRegex admission baseline

Measured on 2026-11-11 using the actual, unchanged `CometRegex.supportLevel` with
`RegexFlavor.RLike`. Gentropy's six application call sites have **16.67% occurrence
admission** and **20.00% unique-pattern admission**. The engine fixtures have very
different source-location distributions and are reported separately. These small,
selected sources do not establish representative real-world workload coverage.
The second round adds one Sumeh toolkit test-rule definition and no eligible
application patterns. All 139 records from the previous round retain their
eligibility, decoded values and admission decisions.

## Occurrence results

An eligible occurrence has a decoded literal value under the explicit source
policy and is accepted by `java.util.regex.Pattern.compile` on the measured JDK.
Excluded records are outside the admission-rate denominator. Occurrences are
checked-in source locations, not executed queries, rows or timing samples.

| Source | Observed | Excluded | Eligible | Admitted | Rejected | Admission |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Spark SQL input tests | 6 | 0 | 6 | 3 | 3 | 50.00% |
| Spark's inherited Hive fixtures | 40 | 0 | 40 | 22 | 18 | 55.00% |
| Hive clientpositive fixtures | 70 | 2 | 68 | 50 | 18 | 73.53% |
| Gentropy application code | 6 | 0 | 6 | 1 | 5 | 16.67% |
| Wikimedia query templates | 17 | 17 | 0 | — | — | — |
| CMSSpark dynamic SQL positions | 10 | 10 | 0 | — | — | — |
| Cuallee dynamic SQL positions (toolkit) | 2 | 2 | 0 | — | — | — |
| Sumeh runtime consumer (toolkit) | 1 | 1 | 0 | — | — | — |
| Sumeh PySpark test-rule definition (toolkit test) | 1 | 0 | 1 | 0 | 1 | 0.00% |

Mozilla and both Fink scopes have no predicate candidates. SparkUtils Quality's
separate first-stage survey has no whole-word hits. Cuallee has_pattern calls use
regexp_extract and appear only in the non-RLIKE audit. Zero denominators have
null rates; they are not assigned a 0% admission rate. Sumeh's one eligible test
definition does have a measured 0% rate. No pooled admission rate is reported.

## Unique-pattern results

Uniqueness is exact equality of decoded pattern strings within each source.
No regex normalization is performed. Repeated calls and aliases count repeatedly
in the occurrence table and once per pattern string in this table.

| Source | Unique eligible | Admitted | Rejected | Admission |
| --- | ---: | ---: | ---: | ---: |
| Spark SQL input tests | 2 | 1 | 1 | 50.00% |
| Spark's inherited Hive fixtures | 19 | 8 | 11 | 42.11% |
| Hive clientpositive fixtures | 19 | 8 | 11 | 42.11% |
| Gentropy application code | 5 | 1 | 4 | 20.00% |
| Sumeh PySpark test-rule definition (toolkit test) | 1 | 0 | 1 | 0.00% |

The two Hive fixture groups contain the same 19 eligible decoded pattern strings.
Hive upstream's much higher occurrence rate reflects repeated admitted patterns;
it does not indicate broader unique-pattern coverage. This overlap also prevents
treating the groups as independent workload samples.

## Literal decoding and exclusions

The manifest's `measurement` entries explicitly select the decoder or exclusion
policy for each source. There is no automatic fallback to treating undecoded SQL
tokens as patterns.

- **Gentropy:** use the Python AST's string values, after reviewing all six
  receivers as PySpark Columns. No SQL unescaping is applied to these values.
- **Sumeh:** use one AST-decoded test-rule value after explicit source-site and
  consumer review. RESOLVED_CONSTANT requires both verified resolution and
  manifest opt-in. The dataclass, registry and PySpark consumer are reviewed at
  the pinned commit; this is not a general rule/config propagation mechanism.
- **Other Python candidates:** runtime pattern columns, dynamic SQL contexts and
  unknown receivers are not promoted. Verified functions.rlike calls require
  a string-valued functions.lit wrapper; bare string arguments are column names.
- **Spark SQL:** use Spark 4.1.3's actual `CatalystSqlParser.parseExpression` and
  require a non-null string `Literal`. Set `escapedStringLiterals=false`,
  `ansi.enabled=false`, and `ansi.doubleQuotedIdentifiers=false` explicitly.
- **Hive fixtures:** use the reactor dependency's Hive 2.3.10
  `BaseSemanticAnalyzer.unescapeSQLString`, requiring the result to equal the
  Spark decoder above. All 110 fixture occurrences agree before Java validation;
  108 remain eligible after Java validation. This is not a replay of the pinned
  upstream Hive version's test runner.
- **Java-invalid patterns:** two Hive occurrences are excluded. The source
  [deliberately selects the Google regex engine](https://github.com/apache/hive/blob/3802476e88f74e7202925fe46d68690178872456/ql/src/test/queries/clientpositive/udf_configurable.q#L13)
  and uses `\p{Katakana}+` at
  [line 32](https://github.com/apache/hive/blob/3802476e88f74e7202925fe46d68690178872456/ql/src/test/queries/clientpositive/udf_configurable.q#L32)
  and [line 54](https://github.com/apache/hive/blob/3802476e88f74e7202925fe46d68690178872456/ql/src/test/queries/clientpositive/udf_configurable.q#L54).
  Java rejects this property spelling. These are not two Comet whitelist
  rejections or invalid Hive test cases.
- **Wikimedia:** all 17 records remain excluded. The separate local audit now
  verifies that 12 literals have identical decoded values under the two tested
  escape settings, one is sensitive, and four require dollar preprocessing review.
  Deployment semantics and redistribution terms remain unresolved. Its baseline
  rate is unknown, not 0%. See the [source survey](application-survey.md).

The probe still inventories the original source snapshots. The baseline measures
their extracted strings under the interpretation above; it does not establish
the configuration or query plan used in historical executions.

## Escape-setting sensitivity

For each measured SQL literal, the runner also parses it with Spark's
`escapedStringLiterals=true`. This is a separate sensitivity check, not a way to
select the higher admission rate.

| Source | Decoded values that change | Admission decisions that change |
| --- | ---: | ---: |
| Spark SQL input tests | 3 | 3 |
| Spark's inherited Hive fixtures | 1 | 0 |
| Hive eligible fixtures | 1 | 0 |

For Spark's six SQL occurrences, this alternative configuration would produce
100% admission: the three shorthand-class patterns become patterns matching
literal backslashes followed by ordinary letters. The queried pattern semantics
have changed. Reporting that as an improvement in whitelist coverage would be
incorrect. The declared baseline uses `escapedStringLiterals=false` throughout.

## Analyzer and reproducibility

The Python harness compiles these files together into an isolated class directory:

- The current repository's `CometRegex.scala`.
- The current repository's `SupportLevel.scala`, including the real result types.
- The offline `dev/regex-coverage/RegexCoverage.scala` driver.

It does not copy or reimplement the scanner and does not create stub support
types. The new class directory takes precedence over existing project classes.
The runner records both loaded origins and refuses results if either the analyzer
or `Compatible` came from another directory.

The measured analyzer source SHA-256 is
`15071820639b9353cc94264f96f0a143ef97b2833735b77533d2254de0603229`.
The runtime is Spark 4.1.3, Scala 2.13.17 and Java 21.0.12. Full source/compiler
hashes, input/manifest hashes, compiled analyzer hash, Git HEAD, interpreter origins
and decoding policies are recorded in the generated summary. These records
identify the actual source and binaries even when unrelated build outputs exist.

After [reproducing the source inventory](README.md#reproduce), run from the repo
root with the default Spark profile and a compatible JDK:

```sh
make core
./mvnw test dependency:build-classpath -Pspark-4.1 -Dtest=none \
  -Dsuites=org.apache.comet.expressions.CometRegexSuite \
  -Dmdep.outputFile=target/regex-coverage-classpath.txt

python3 dev/regex-coverage/measure_coverage.py \
  --input target/regex-coverage-round2/probe/occurrences.jsonl \
  --classpath-file spark/target/regex-coverage-classpath.txt \
  --output target/regex-coverage-round2/baseline

python3 -m unittest discover -s dev/regex-coverage -p 'test_*.py' -v
```

The classpath is produced by the entire reactor, without `-pl`. Scala compiler
jars are located relative to the resolved Scala library in the Maven repository.
The baseline driver itself starts no Spark session and invokes no native kernel.

Generated artifacts under ignored `target/regex-coverage-round2/baseline/`
(the analysis also creates its own `analysis/baseline/`):

- `classified.jsonl`: all 153 discovery records, with decisions or explicit
  exclusions; 121 are eligible and 32 are excluded. These are bookkeeping totals
  across separate populations, not one workload denominator.
- `summary.json`: per-source occurrence/unique counts, sensitivity counts and
  build provenance. It deliberately has no pooled rate.
- `runtime.json`, `requests.jsonl`, `decisions.jsonl`: component inputs and outputs.
- Compile, component-check and measurement logs, plus isolated compiled classes.

## Validation and limits

`make core` and the full Maven command succeeded; the existing `CometRegexSuite`
passed all 14 tests during the baseline stage; production sources have not
changed since. All 46 current Python tests pass. The Scala
component checks cover actual Spark/Hive decoding, raw and escaped literals,
lexer boundaries, Java-invalid and null patterns, and decoder disagreement.
Repeated measurements produced identical classified rows and summary data.
The expanded classified JSONL SHA-256 is
`c49f24d1ae34a93002241e35861a66f3a49b698c5a5b3966191ac09cee23fc36`.

Only plan-time pattern admission is measured. Subject/pattern collation, constant
folding, other expression gates, actual native routing, native compilation,
Java/Rust parity and performance are not measured. No whitelist, routing or
native implementation code changed.

The analyzer still returns no detailed rejection reason. The subsequent
[rejection and structure analysis](analysis-results.md) verifies offline prefix
witnesses for this corpus and reports feature co-occurrences and syntax controls.
It confirms that Gentropy's four anchored rejected occurrences contain both `^`
and `$`. Neither the witnesses nor the controls are a general Scanner diagnostic
API or a safe estimate of the benefit from relaxing a rule. Larger, more diverse
application sources are still needed before generalizing these rates.
