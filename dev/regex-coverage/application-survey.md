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

# Application and toolkit source survey, 2026-11-11

## Result

The second round adds **no eligible application patterns**. Gentropy remains the
only qualified application source: six call sites and five distinct decoded
patterns. New investigations cover data-quality toolkits, Wikimedia's existing
literals and CMS-generated SQL. One Sumeh PySpark test rule is eligible and is
reported in a separate Toolkit population. Sparse public fixed-pattern results
are a finding of this bounded survey, not evidence that Spark applications rarely
use regex.

Commits, scopes, license paths, populations, known biases and source-review
locations are pinned in [`corpora_manifest.json`](corpora_manifest.json).
No synthetic patterns, other dialects or CLI defaults inflate the baseline.

## Application scopes

| Repository / recursive scope | License | Files | Parse failures | Eligible occurrences / unique patterns | Disposition |
| --- | --- | ---: | ---: | ---: | --- |
| `opentargets/gentropy`, `src/gentropy/` Python | Apache-2.0 | 166 | 0 | 6 / 5 | Six reviewed Column.rlike literals |
| `mozilla/python_mozetl`, `mozetl/` Python | MIT | 48 | 0 | 0 / 0 | Zero raw predicate keywords |
| `astrolabsoftware/fink-broker`, `fink_broker/` Python | Apache-2.0 | 25 | 0 | 0 / 0 | Zero raw predicate keywords |
| `astrolabsoftware/fink-filters`, `fink_filters/` Python | Apache-2.0 | 129 | 0 | 0 / 0 | Same Fink ecosystem; zero raw predicate keywords |
| `dmwm/CMSSpark`, `src/python/CMSSpark/` Python | MIT | 59 | 3 | 0 / 0 | Ten intended predicates in dynamic SQL |
| `wikimedia/analytics-refinery`, `hql/` HQL | Unresolved | 335 | 0 | 0 / 0 | Seventeen literals audited; deployment and license unverified |

CMSSpark's three parse failures contain zero raw whole-word predicate keywords;
three invalid-escape warnings are retained in the output. Raw keyword counts
include strings and comments. Zero raw hits in a scoped package do not prove the
absence of patterns in notebooks, external inputs or deployments.

## Gentropy provenance and API review

Gentropy's [pinned dependency declaration](https://github.com/opentargets/gentropy/blob/78b7f1e050089216ed42b2767b5246814ccb845e/pyproject.toml)
requires PySpark `>=3.5.0,<3.6`; its
[license file](https://github.com/opentargets/gentropy/blob/78b7f1e050089216ed42b2767b5246814ccb845e/LICENSE.md)
contains Apache-2.0 terms. Every extracted receiver was inspected against PySpark
imports, `Column` annotations, `f.col` construction or transformations returning
Columns. This receiver review applies to this pinned snapshot, not arbitrary
Python methods named `rlike`.

| File / method | Call sites | Distinct string values in file | Pinned source |
| --- | ---: | ---: | --- |
| `common/processing.py`, `flag_non_atgc_alleles` | 2 | 1 | [Line 576](https://github.com/opentargets/gentropy/blob/78b7f1e050089216ed42b2767b5246814ccb845e/src/gentropy/common/processing.py#L576) |
| `dataset/l2g_prediction.py`, publication-date check | 1 | 1 | [Line 162](https://github.com/opentargets/gentropy/blob/78b7f1e050089216ed42b2767b5246814ccb845e/src/gentropy/dataset/l2g_prediction.py#L162) |
| `datasource/gwas_catalog/associations.py`, identifier/allele checks | 3 | 3 | [Line 200](https://github.com/opentargets/gentropy/blob/78b7f1e050089216ed42b2767b5246814ccb845e/src/gentropy/datasource/gwas_catalog/associations.py#L200), [450](https://github.com/opentargets/gentropy/blob/78b7f1e050089216ed42b2767b5246814ccb845e/src/gentropy/datasource/gwas_catalog/associations.py#L450), [508](https://github.com/opentargets/gentropy/blob/78b7f1e050089216ed42b2767b5246814ccb845e/src/gentropy/datasource/gwas_catalog/associations.py#L508) |

The two calls in `flag_non_atgc_alleles` check separate input columns with the
same literal. They count as two source locations and one unique pattern string.
Character-class orders remain verbatim: no semantic regex canonicalization is
performed. The five string values primarily describe allele, identifier and date
validation, so the source has a narrow domain bias.

The AST retains both `literal_python` (the original source spelling) and
`pattern_value` (the Python string value). Raw strings, escaped backslashes and
adjacent constant strings are handled by Python parsing, without importing any
upstream module or running a job. F-strings and other computed arguments are not
treated as direct constants. Comments and docstring examples do not count.

## New toolkit surveys

The fast survey covers `.py`, `.scala`, `.sql`, `.yaml`, `.yml`, `.md` and `.json`
files across each pinned repository. Keyword hits are candidates for review, not
pattern counts. Every matching file, including zero-hit files, contributes to the
scope digest. All three snapshots contain Apache-2.0 LICENSE files; hashes are
recorded in `survey-summary.json`.

| Repository / fixed commit | Files scanned | Whole-word hits | Qualified additions |
| --- | ---: | --- | --- |
| `canimus/cuallee` / `907d93c310c160de0c8858442452a4e3c2b8fbde` | 344 | has_pattern: 42; rlike: 3 | None |
| `sparkutils/quality` / `dd1a447353ef0b37db11e4052e5368317a25abc3` | 392 | None | None |
| `maltzsama/sumeh` / `da514b3ae953d8ea99ef1ad2c69c98c5d3ebd516` | 136 | has_pattern: 30; rlike: 3 | One toolkit test-rule definition |

### Cuallee

The reviewed [PySpark has_pattern implementation](https://github.com/canimus/cuallee/blob/907d93c310c160de0c8858442452a4e3c2b8fbde/cuallee/pyspark_validation.py#L161)
uses `length(regexp_extract(...)) > 0`, not RLIKE. Its README examples and
has_pattern tests therefore cannot measure the RLike whitelist. Two recognized
source calls and five PySpark test calls are recorded as `NON_RLIKE_REGEX`.
That audit is not an exhaustive count of all wrapper definitions or backend regex
calls. The is_legit default belongs to the same non-RLIKE contract.

The [bio SQL rule](https://github.com/canimus/cuallee/blob/907d93c310c160de0c8858442452a4e3c2b8fbde/cuallee/bio/checks.py#L50)
contains two intended RLIKE predicates. Their RHS text looks fixed, but the SQL
also interpolates an unchecked column expression. Both are `UNKNOWN`; the tool
neither assumes safe SQL context nor performs cross-function rule propagation.
The third raw rlike hit is text used by another backend's rewrite.

### SparkUtils Quality

The declared 392-file scope contains no whole-word RLIKE, REGEXP, REGEXP_LIKE or
has_pattern matches. Substring matches such as `RunnerLike` and the
[regExpTest method name](https://github.com/sparkutils/quality/blob/dd1a447353ef0b37db11e4052e5368317a25abc3/server/src/test/scala/com/sparkutils/quality/tests/MetaRuleSetTest.scala#L25)
are not SQL predicates. This negative result is retained in the first-stage survey;
it is not an eligible corpus with a 0% admission rate.

### Sumeh

One [PySpark test rule](https://github.com/maltzsama/sumeh/blob/da514b3ae953d8ea99ef1ad2c69c98c5d3ebd516/tests/engines/test_pyspark.py#L265)
contains a literal pattern and passes its rules to the imported PySpark validate
function. Manual source review follows direct dataclass construction, unchanged
rule forwarding in the engine, the
[has_pattern registry](https://github.com/maltzsama/sumeh/blob/da514b3ae953d8ea99ef1ad2c69c98c5d3ebd516/sumeh/engines/pyspark/registry.py#L57)
and the [Column.rlike consumer](https://github.com/maltzsama/sumeh/blob/da514b3ae953d8ea99ef1ad2c69c98c5d3ebd516/sumeh/engines/pyspark/analyzers.py#L343).
The manifest permits this exact constructor site as `RESOLVED_CONSTANT`.
This is source-level validation; the upstream test was not executed.

Its one unique pattern is Java-valid and rejected by the actual Comet analyzer:
**0/1 occurrences and 0/1 patterns admitted**. The runtime consumer itself has a
dynamic `rule.value`, inventoried separately and excluded. Shared fixtures,
README examples and other backend tests are not attributed to PySpark application
usage. Exact inspection scopes contain five backend files and one test file.

## Wikimedia validation

All 17 previously discovered literals retain their pinned file hashes and
locations. The audit sends the exact literal tokens to the actual Spark parser
and Java regex compiler without preprocessing. All 17 are locally decodable and
Java-valid under the declared default settings, but that alone does not establish
the patterns used by deployed jobs.

| Audit stage | Occurrences | Remaining question |
| --- | ---: | --- |
| Same decoded value under both tested escape settings | 12 | Actual SQL consumer, deployment/template settings and redistribution terms |
| Escape-setting-sensitive literal | 1 | Deployed escapedStringLiterals setting and consumer |
| Literal contains double-dollar syntax | 4 | Shell/template dollar preprocessing before SQL parsing |

The 12 invariant cases comprise six memory-limit, four version and two webrequest
locations. The user-agent literal is escape-sensitive; the four articleplaceholder
locations contain `$$`. Stages are exclusive and total 17. They are local
validation stages, not 12 promoted samples or a 0% baseline rate.

A pinned [HiveToGraphite consumer](https://github.com/wikimedia/analytics-refinery-source/blob/9615df82755beec64a6c6064acadbb33dba4b47c/refinery-job/src/main/scala/org/wikimedia/analytics/refinery/job/HiveToGraphite.scala)
uses SparkSQLNoCLIDriver with `SparkConf(loadDefaults=true)`, so its source does
not establish deployed SQL settings. No root LICENSE/COPYING was identified in
the deployment snapshot. The separate
[refinery/source license](https://github.com/wikimedia/analytics-refinery-source/blob/9615df82755beec64a6c6064acadbb33dba4b47c/LICENSE.md)
explicitly scopes its Apache grant to that source project. It does not resolve
licensing for the separate deployment repository. All 17 remain excluded and
extracted literals stay in ignored local output.

## CMSSpark dynamic-input audit

The ten intended predicates are now machine-inventoried from Python f-string and
format carriers across three files:

- [Site inclusion/exclusion](https://github.com/dmwm/CMSSpark/blob/7f32910b2f199bf03553a28d8eacc977b540f966/src/python/CMSSpark/condor_crab_unique_users.py#L78).
- [CPU-time site selection](https://github.com/dmwm/CMSSpark/blob/7f32910b2f199bf03553a28d8eacc977b540f966/src/python/CMSSpark/condor_hs06coreHrPlot.py#L76).
- [Tier/removal/skim query generation](https://github.com/dmwm/CMSSpark/blob/7f32910b2f199bf03553a28d8eacc977b540f966/src/python/CMSSpark/dbs_event_count_plot.py#L193).

Each pattern depends on runtime input and is `DYNAMIC_PATTERN`. Fifteen matching
string default definitions are separately recorded: eight function defaults and
seven CLI input defaults. Some defaults are lists subsequently transformed by
code, not final regex strings. None establishes actual query values, invocation
counts or execution frequency, and none is promoted to the baseline. Defaults
from different call layers are not counted as independent regex uses.

## Corpus quality and reproducibility

Six application repositories were inspected across five labeled domains. Only
Gentropy contributes eligible values: **one independent source, one domain and
five unique patterns**. Toolkit eligibility is separately one Sumeh test rule.
The internal 3-source / 3-domain / 30-pattern goals remain unmet. They are not
issue acceptance thresholds, and attaining them would not establish statistical
representativeness.

Application exclusions are 27/33 recognized candidates (81.82%): 17 Wikimedia
literals with unresolved source validation and ten CMS dynamic values. Toolkit
exclusions are 3/4 (75%); engine fixture exclusions are 2/116 (1.72%). Non-RLIKE
audit records are outside these denominators. Survey-only SparkUtils Quality is
not counted as a repository with eligible patterns.

Follow the [complete reproduction commands](README.md#reproduce). All source,
license and consumer-review hashes are generated from pinned inputs. The original
139 records preserve eligibility, decoded values and admission decisions. The
current 153 records include one newly eligible toolkit test definition and 13
new excluded dynamic/unknown records. No new application admission estimate or
whitelist change follows from this round.

Databricks Labs UCX remains excluded because its
[pinned license](https://github.com/databrickslabs/ucx/blob/06f771ef9efe35ef448f7e449e8251e511f12b88/LICENSE)
contains service-specific use restrictions. No DQX source patterns were collected.
A maintainer can review these bounded exploratory results and the documented
scarcity before deciding whether more corpus collection is needed for #5812.
