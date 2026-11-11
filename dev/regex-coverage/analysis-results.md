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

# Rejection and structure analysis for #5812

This report helps #5812 reviewers interpret the exploratory corpus measured on
2026-11-11. It analyzes the 121 eligible records in the
[admission baseline](baseline-results.md). All recorded decisions remain
unchanged. The second round adds one toolkit test-rule definition; the other
120 eligible records are unchanged. All 45 rejected occurrences have a verified
primary blocking feature under the offline definition below; all 121 structures
fit the lexical subset.
The two Google-regex-only Hive records and 17 unvalidated Wikimedia templates
remain outside the analysis.

The main application finding is **anchor co-occurrence**: all four rejected
Gentropy occurrences containing `^` also contain `$`. Removing only the leading
`^` in a syntax control leaves all four rejected. Removing both anchor types makes
four synthetic controls pass. These experiments identify remaining syntactic
obstacles; they do not establish the safety or benefit of adding anchor support.

## Primary blocking features

Each cell is **occurrences / unique pattern strings** within that source. Primary
counts sum to the rejected total. The Hive groups overlap and are not independent
workload samples.

| Source | Rejected | Anchor | Dot wildcard | Shorthand class |
| --- | ---: | ---: | ---: | ---: |
| Spark SQL input tests | 3 / 1 | 0 / 0 | 0 / 0 | 3 / 1 |
| Spark's inherited Hive fixtures | 18 / 11 | 10 / 7 | 8 / 4 | 0 / 0 |
| Hive clientpositive fixtures | 18 / 11 | 10 / 7 | 8 / 4 | 0 / 0 |
| Gentropy application/toolkit | 5 / 4 | 4 / 3 | 0 / 0 | 1 / 1 |
| Sumeh toolkit test-rule definition | 1 / 1 | 1 / 1 | 0 / 0 | 0 / 0 |

The primary feature is defined conservatively:

1. Find the first recognized blocker in a context-aware lexical scan of the
   decoded pattern.
2. Require the actual `CometRegex.supportLevel` to admit the prefix before that
   token and reject the prefix through that token, with both prefixes Java-valid.
3. Otherwise report `UNATTRIBUTED`, preserving it in the rejected denominator.

This is a verified prefix witness, **not an internal Scanner error code** or a
general diagnostic API. A blocker inside an unclosed group/class can lack an
admitted prefix, and an unrecognized grammar shape or budget-only failure may be
unattributed. The current corpus has zero such cases. The production analyzer is
unchanged, and the lexical tool never decides admission.

## Unsupported-feature tags and co-occurrence

Tags count a feature's presence once per pattern occurrence, even if its token
appears multiple times. Counts overlap, unlike primary counts. Every observed
shorthand-class tag in this corpus is `\d`.

| Source | `^` | `$` | Wildcard `.` | `\d` | Literal ASCII control character |
| --- | ---: | ---: | ---: | ---: | ---: |
| Spark SQL input tests | 0 / 0 | 0 / 0 | 0 / 0 | 3 / 1 | 0 / 0 |
| Spark's inherited Hive fixtures | 10 / 7 | 2 / 2 | 8 / 4 | 0 / 0 | 1 / 1 |
| Hive clientpositive fixtures | 10 / 7 | 2 / 2 | 8 / 4 | 0 / 0 | 1 / 1 |
| Gentropy application/toolkit | 4 / 3 | 4 / 3 | 0 / 0 | 1 / 1 | 0 / 0 |
| Sumeh toolkit test-rule definition | 1 / 1 | 1 / 1 | 0 / 0 | 0 / 0 | 0 / 0 |

Gentropy's four anchored occurrences represent three distinct patterns; all carry
both anchor tags. In each Hive group, two occurrences carry both anchor tags, and
one of those also contains a decoded newline. Its primary blocker is the initial
anchor, but removing the anchors still leaves the newline outside the whitelist.
The other Hive dot-wildcard occurrences have no anchor tags.

Escaped metacharacters, class-local literal dots and class negation are handled
lexically. `[.]`, `[^.]`, escaped `\.`, and literal-backslash `\\d` do not receive
wildcard, anchor or shorthand tags merely because their text contains those
characters. Groups beginning `(?:` are not counted as `?` quantifiers. Special
groups, quoted spans and unfamiliar classes/escapes produce explicit partial
results instead of guessed complete tag sets.

## Synthetic syntax controls

For each originally rejected pattern, the tool creates analysis-only controls.
Selected anchors are removed; selected wildcard/shorthand/control tokens are
replaced by the ordinary literal `a`. Every resulting control is checked with the
real analyzer and Java regex compiler. These patterns are not added to the source
corpus and are never substituted into application queries.

Each cell counts passing controls as **occurrences / unique original pattern
strings**. The columns are separate experiments, not additive estimates of future
workload coverage.

| Source | Remove leading `^` only | Remove both anchor types | Replace wildcard dots | Replace digit shorthands | Replace/remove all observed blockers |
| --- | ---: | ---: | ---: | ---: | ---: |
| Spark SQL input tests | 0 / 0 | 0 / 0 | 0 / 0 | 3 / 1 | 3 / 1 |
| Spark's inherited Hive fixtures | 8 / 5 | 9 / 6 | 8 / 4 | 0 / 0 | 18 / 11 |
| Hive clientpositive fixtures | 8 / 5 | 9 / 6 | 8 / 4 | 0 / 0 | 18 / 11 |
| Gentropy application/toolkit | 0 / 0 | 4 / 3 | 0 / 0 | 1 / 1 | 5 / 4 |
| Sumeh toolkit test-rule definition | 0 / 0 | 1 / 1 | 0 / 0 | 0 / 0 | 1 / 1 |

For example, leading-`^` frequency is 4/3 in Gentropy, but its leading-only syntax
control count is 0/0 because `$` remains. In Hive, removing both anchor types
admits one more control than removing leading `^` alone; the newline case remains
rejected. Replacing every observed blocker makes every control pass in this
small corpus. No control fails Java compilation.

These controls change matching semantics and can change structural cost. They
are not implementations of support for the original constructs. They do not
prove Java/Rust parity, Rust compilability, or actual additional admissions after
a future whitelist change. The report therefore does not convert their counts
into projected admission percentages or speedups.

## Length and structure

Length is the decoded Unicode code-point count. Percentiles use the nearest-rank
method, separately by source and by occurrence/unique mode. For these particular
sources the reported length percentiles happen to be identical in both modes;
the JSON records their separate denominators and other distributions.

| Source | Eligible occurrences | Unique patterns | Length P50 | P90 | P99 | Maximum |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Spark SQL input tests | 6 | 2 | 4 | 7 | 7 | 7 |
| Spark's inherited Hive fixtures | 40 | 19 | 3 | 6 | 7 | 7 |
| Hive clientpositive fixtures | 68 | 19 | 3 | 6 | 7 | 7 |
| Gentropy application/toolkit | 6 | 5 | 9 | 17 | 17 | 17 |
| Sumeh toolkit test-rule definition | 1 | 1 | 8 | 8 | 8 | 8 |

The tiny sample sizes make P99 equal to the sample maximum here. These are sample
descriptions, not estimates of population tails. Length alone does not explain
admission: Spark's admitted patterns are seven characters, while its rejected
patterns are four characters and contain digit shorthands.

The structural maxima are similarly small:

| Source | Classes per pattern | Quantifiers | Groups | Group depth | Alternations | Quantifier nesting | Largest explicit counted bound |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Spark SQL input tests | 1 | 1 | 0 | 0 | 0 | 1 | 0 |
| Spark's inherited Hive fixtures | 1 | 2 | 1 | 1 | 1 | 1 | 0 |
| Hive clientpositive fixtures | 1 | 2 | 1 | 1 | 1 | 1 | 0 |
| Gentropy application/toolkit | 1 | 3 | 0 | 0 | 0 | 1 | 4 |
| Sumeh toolkit test-rule definition | 1 | 1 | 0 | 0 | 0 | 1 | 0 |

The three fixture sources have no counted quantifiers; their maximum-bound entry is zero.
All eligible patterns are ASCII, including the two source-group occurrences of
the same pattern containing a decoded newline. Full P50/P90/P99 distributions,
including separate admitted/rejected and occurrence/unique views, are retained
in `analysis-summary.json`. No single invented complexity score is used.

## Structural budgets

The current analyzer caps group depth at 32, explicit counted bounds at 256,
aggregate structural expansion at 4096, and quantifier nesting at 8. The tool
reads these constants from the unchanged source and inspects the existing private
Scanner for its actual expansion estimate. It does not duplicate the cost model.
Reflection fails if the Scanner implementation contract changes.

The estimate is available only for patterns that finish the scan and are admitted:

| Source | Admitted occurrences with estimates | Expansion P50 | P90 | P99 | Maximum |
| --- | ---: | ---: | ---: | ---: | ---: |
| Spark SQL input tests | 3 | 3 | 3 | 3 | 3 |
| Spark's inherited Hive fixtures | 22 | 2 | 3 | 5 | 5 |
| Hive clientpositive fixtures | 50 | 3 | 3 | 5 | 5 |
| Gentropy application/toolkit | 1 | 4 | 4 | 4 | 4 |

Rejected patterns have no full expansion estimate. Their values remain unknown,
not zero. Sumeh's rejected test rule has no estimate. The observed admitted
maximum is 5 against the 4096 cap, and the
observed group/count/nesting shapes are shallow. This sample provides little
evidence about budget-driven rejection and no justification for raising caps.
The estimate is not compiled bytes, compilation memory, matching cost or time.

## Reproduce and validate

First follow [the baseline setup](baseline-results.md#analyzer-and-reproducibility)
to obtain the source inventory, reactor classpath and recorded baseline. Then run:

```sh
python3 dev/regex-coverage/analyze_coverage.py \
  --input target/regex-coverage-round2/probe/occurrences.jsonl \
  --classpath-file spark/target/regex-coverage-classpath.txt \
  --reference-baseline target/regex-coverage-round2/baseline/classified.jsonl \
  --output target/regex-coverage-round2/analysis

python3 -m unittest discover -s dev/regex-coverage -p 'test_*.py' -v
```

The analysis rebuilds its own baseline under the output directory and checks every
record's eligibility, decoded value and admission decision against the recorded
baseline. It then checks all original/probe/control strings against that freshly
compiled analyzer. No production Scala or Rust source is changed.

For the expansion regression check, the earlier 139-record baseline was also
used with `--allow-new-records`. Every earlier record must still exist with the
same eligibility, value and decision; source identities must be unambiguous.
All 139 passed this check. Newly recognized dynamic/unknown records stay excluded.

Generated output under ignored `target/regex-coverage-round2/analysis/` contains:

- `analyzed.jsonl`: 121 eligible records with lexical metrics, tags, primary
  prefix witnesses, actual admitted-pattern expansion estimates and controls.
- `analysis-summary.json`: per-source occurrence/unique distributions, tag
  co-occurrences, experiment results, budgets, source/tool hashes and runtime data.
- `inspection-requests.jsonl`, `inspection-results.jsonl`: original strings,
  prefix witnesses and synthetic controls, kept separate from the original corpus.
- `baseline/`: the fresh measurement and component-check logs.

All 46 Python tests and the fresh JVM component checks pass, including class and
escape boundaries, unknown-shape handling, UTF-16/code-point distinctions,
overlapping tags, duplicate handling, real Scanner costs and budget failures.
Every original admission decision matches the recorded baseline. Repeated
analysis produces identical analyzed records and summaries. The original 14-test
`CometRegexSuite` passed during the baseline stage; production sources remain
unchanged in this analysis stage.

## Follow-up priorities supported by this sample

For the small Gentropy sample, research paired `^`/`$` compatibility rather than
using leading-anchor frequency alone as a benefit estimate. Digit-class behavior
is a separate candidate affecting one application occurrence. Wildcard dots
explain eight rejected occurrences per Hive fixture group, but none in the
available application sample, so that is test-corpus evidence rather than a
population-wide priority ranking.

The added Sumeh toolkit test rule also contains both anchors; it supplies no
additional application evidence. Cuallee, CMS and Wikimedia exclusions do not
enter these feature counts. Broader application sampling and Java/Rust parity
evidence remain necessary
before selecting or implementing a compatibility change. A general Scanner
diagnostic API would be useful if future corpora introduce unattributed
budget/grammar failures; the current offline witnesses do not replace that API.
