---
name: suggest-native-expression
description: Use when picking the next Spark expression to implement natively in Comet, or when asked whether a specific codegen-dispatched expression is worth a native Rust implementation. Scores the candidate on how likely a native path can be 100% Spark-compatible and how much throughput or allocation it would save versus the JVM codegen dispatcher, records the verdict in the per-expression audit log so disqualified candidates are not re-litigated, and files a GitHub issue for the recommendation.
argument-hint: [expression-name]
---

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

Assess whether a Spark expression that Comet runs through the JVM codegen dispatcher is worth
implementing natively in Rust, and propose the best candidate as a GitHub issue.

This skill does **not** implement anything. Its deliverables are exactly three:

1. A verdict on two axes: **compatibility confidence** (can a native path be 100%
   Spark-compatible?) and **native upside** (how much throughput or allocation does the native
   path save versus codegen dispatch?).
2. A dated `Native candidate` line in the expression's page under
   `docs/source/contributor-guide/expression-audits/`, so a future run can see that this
   expression was already assessed and why it was or was not taken.
3. One filed GitHub issue for the recommended (or deferred-with-a-named-blocker) candidate.

`$ARGUMENTS` may name a single expression to assess. If it is empty, run in survey mode (Step 1)
and pick the best candidate from the whole pool.

## Background reading

Read these before scoring anything. Do not score from memory: the routing model and the
cost of the dispatcher are both easy to misremember, and every judgment in this skill depends
on them.

- `docs/source/contributor-guide/roadmap.md`, section "Native Coverage for Codegen-Dispatched
  Expressions". This is the project-level statement of why this work matters and it is the
  section the filed issue should reference.
- `docs/source/user-guide/latest/compatibility/index.md`, section "Native and codegen-dispatch
  implementations". Defines the two paths and which one is the default.
- `docs/source/user-guide/latest/expressions.md`, the Implementation legend and tables. The
  `Codegen dispatch` rows are the candidate pool.
- `docs/source/contributor-guide/optimizing_expressions.md`, the "Proven techniques" table. If
  no technique in that table applies to the candidate, the native upside claim is weak.
- `spark/src/main/scala/org/apache/comet/serde/CometExpressionSerde.scala` for the
  `CometCodegenDispatch` / `CodegenDispatchFallback` / `NativeOptInAvailable` contracts.
- `spark/src/main/scala/org/apache/comet/codegen/CometBatchKernelCodegen.scala` for what the
  dispatcher can and cannot compile (`isSupportedDataType`, `canHandle`).

## What the dispatcher actually costs

Getting this right is the whole point of the skill. Overstating the dispatcher's cost produces
issues that waste a contributor's week on a 2% win.

What codegen dispatch **does** cost:

- One JNI round trip per batch, out of native execution and back.
- Per-row evaluation of Spark's generated Java. No vectorization, no SIMD, no dictionary
  awareness, no bulk buffer operations.
- Per-row JVM heap allocation for every non-primitive intermediate and result: `UTF8String`,
  `Decimal`/`BigDecimal`, `GenericArrayData`, `ArrayBasedMapData`, boxed values, plus whatever
  Spark's own `doGenCode` allocates (formatters, `StringBuilder`, temporary arrays).
- That allocation lands on the JVM heap, outside Comet's native memory pool, so it is invisible
  to Comet's memory accounting and it feeds GC pressure that shows up as task-level jitter
  rather than as an expression cost.
- Per-row reads of nested Arrow data go through `CometArrayData` / `CometMapData` /
  `CometSpecializedGettersDispatch` wrappers, which allocate per row per nesting level.

What codegen dispatch **does not** cost, and therefore is not available as an argument for going
native:

- It does **not** convert the whole batch to rows and back. The kernel reads Arrow vectors
  directly.
- String reads are zero-copy (`UTF8String.fromAddress`).
- The Janino kernel is compiled once per `(expression, schema)` pair, not per batch.
- A `NullIntolerant` root short-circuits null rows without evaluating the body.
- It is byte-exact with Spark by construction, so it needs no compatibility work at all. A
  native replacement starts with a compatibility debt that the dispatcher does not have.

The corollary: a native implementation is worth proposing when the per-row JVM work is
**allocation-heavy or vectorizable**, not merely because "native is faster than JVM".

## Step 1: Choose the candidate

If `$ARGUMENTS` names an expression, skip to Step 2 but still run the exclusion checks below
against it.

### Build the pool

```bash
grep -n "| Codegen dispatch |" docs/source/user-guide/latest/expressions.md
```

Rows marked `Hybrid` are **out of scope**: a native path already exists there and the work is to
close its compatibility gap, which is `audit-comet-expression` territory. Rows marked `—` or
`🔜 Planned` are out of scope too: there is nothing to compare against, so the question is
"implement it at all", which is `implement-comet-expression`.

### Exclude

1. **Families the project has ruled out.** Read the "Not currently planned" section of
   `docs/source/user-guide/latest/expressions.md` and drop anything it covers. Proposing one of
   these needs a project-direction argument that this skill does not have.
2. **Already-assessed expressions.** This is the check that makes the skill cumulative:

   ```bash
   grep -rn "Native candidate (assessed" docs/source/contributor-guide/expression-audits/
   ```

   An expression with a `Disqualified` line is off the pool unless the **named blocker** in that
   line has demonstrably changed (an upstream kernel landed, DataFusion gained the function, the
   Spark semantics were simplified in a new version). If it has changed, say so explicitly in
   the new assessment and add a new dated line. Never delete or rewrite an old line.

3. **Expressions with an existing issue.** Search open and closed:

   ```bash
   gh issue list --repo apache/datafusion-comet --search "<function> native in:title,body" --state all --limit 10
   ```

   If a candidate match comes back, open it (`gh issue view <N> --repo apache/datafusion-comet`)
   and confirm it really covers a native implementation of this expression. If it does, that
   expression is done: pick another rather than filing a duplicate.

### Rank what is left

Rank the remaining pool by a cheap first-pass read of the two axes below, then assess the top
candidate properly. Prefer expressions that appear in TPC-H or TPC-DS, or in common ETL shapes
(string cleanup, date bucketing, JSON field extraction), over expressions that are rare in real
queries. To check workload presence:

```bash
grep -rli "<function>(" spark/src/test/resources/tpcds/ spark/src/test/resources/tpch/ 2>/dev/null
```

State the pool size, the exclusions, and why the chosen candidate topped the ranking. A ranking
you do not show is a ranking the reader cannot challenge.

## Step 2: Establish how the expression runs today

Find the serde and read it:

```bash
grep -rn "class\|object" spark/src/main/scala/org/apache/comet/serde/*.scala | grep -i "<Expr>"
```

Record:

- Which trait it uses. A plain `CometCodegenDispatch[T]` means dispatch is the only path.
  `CodegenDispatchFallback` means a native path exists for some inputs and dispatch catches the
  rest, which narrows the candidate to the uncovered inputs.
- Whether the dispatcher can actually compile it, per `CometBatchKernelCodegen.canHandle` and
  `isSupportedDataType`. If some input or output type is outside that surface, those cases fall
  back to **Spark**, not to the dispatcher. Whole-operator Spark fallback is a much larger cost
  than a JNI round trip, and it raises the upside score materially. Say which cases these are.
- Whether the expression is disabled or gated by any config
  (`spark.comet.expression.<Name>.enabled`, `allowIncompatible`,
  `spark.comet.exec.scalaUDF.codegen.enabled`).

## Step 3: Score compatibility confidence

The question is narrow: **can a native Rust implementation match Spark bit-for-bit, for every
input, on every supported Spark version?** "Close enough" is not a passing answer, because the
dispatcher it would replace is exact.

### Read the Spark implementation across versions

Reuse the clone recipe from `audit-comet-expression` (Spark 3.4.3, 3.5.8, 4.0.1, 4.1.1) and read
`eval` / `nullSafeEval` / `doGenCode`, `inputTypes`, `dataType`, ANSI branches, and any
config the expression reads. Note behavior that differs across versions: each divergence is a
shim the native path would need.

### Check upstream first

An existing upstream implementation is the single strongest positive signal, both for
compatibility and for effort:

```bash
grep -rn "fn name" ~/.cargo/registry/src/*/datafusion-spark-*/src/function/ 2>/dev/null | grep -i "<function>"
grep -rn "fn name" ~/.cargo/registry/src/*/datafusion-functions-*/src/ 2>/dev/null | grep -i "<function>"
```

If `datafusion-spark` has it, read the implementation and diff its semantics against Spark's
before crediting it. Upstream functions in that crate aim for Spark compatibility but are not
all complete. If it matches, the proposal becomes a wiring job (`wire-datafusion-function`),
which is a much cheaper issue to file and a much easier one to pick up.

### Hazard checklist

Each hazard that applies is a strike against compatibility confidence. Check every one and
report the result, including the ones that do not apply, so the reader can see the checklist was
run rather than skimmed.

| Hazard                                                                                                                                                      | Why it threatens 100% compatibility                                                                                     |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| JVM formatting APIs (`java.text.DecimalFormat`, `String.format`, `SimpleDateFormat`, `DateTimeFormatter`) as in `format_number`, `format_string`, `to_char` | Locale, pattern, and rounding behavior would have to be reimplemented, not merely ported                                |
| `java.util.regex` features (backreferences, lookaround, possessive quantifiers, named groups)                                                               | The Rust `regex` crate deliberately omits these, so some patterns cannot be supported at all                            |
| Collation (`StringTypeWithCollation`, `CollationSupport.X.exec`) on Spark 4.0+                                                                              | ICU-backed collation is not propagated natively today ([#4496](https://github.com/apache/datafusion-comet/issues/4496)) |
| `BigDecimal` / `MathContext` precision and rounding, precision-scale promotion, overflow-to-null                                                            | Rounding mode and overflow points are easy to get subtly wrong and produce silent wrong results                         |
| Timezone and calendar (JDK tzdb versus `chrono-tz`, DST edges, hybrid Julian/Gregorian rebase, `spark.sql.legacy.*` flags)                                  | Divergences appear only on specific dates and zones, so tests rarely catch them                                         |
| ANSI error class and message parity                                                                                                                         | An error with the wrong class or message is an observable difference; see the ANSI section of the compatibility guide   |
| Float and double text conversion (`Double.toString` shortest round-trip), `StrictMath` versus `f64` intrinsics                                              | Last-digit differences on string output, platform-dependent differences on transcendentals                              |
| Raw byte semantics of `UTF8String` (invalid UTF-8) ([#4764](https://github.com/apache/datafusion-comet/issues/4764))                                        | Arrow requires valid UTF-8, so byte-preserving behavior cannot be reproduced                                            |
| Lambda bodies or arbitrary child expression trees (`transform`, `aggregate`, `zip_with`)                                                                    | The native path would need to evaluate an arbitrary Catalyst tree, not a fixed kernel                                   |
| Dependence on JVM session state beyond a scalar config (SQLConf lookups, user classes, reflection)                                                          | State that cannot be serialized into the plan cannot cross into native code                                             |
| Nondeterminism or ordering-sensitivity (hash iteration order, unstable sorts)                                                                               | Output order must match Spark's, which is often an implementation detail rather than a spec                             |
| Behavior that differs across 3.4.3 / 3.5.8 / 4.0.1 / 4.1.1                                                                                                  | Each divergence is a shim; several divergences means the native path carries version branches forever                   |

### Rate it

- **High**: no hazard applies, or every hazard that applies is fully specified and mechanically
  reproducible (for example integer arithmetic with a documented overflow rule). A reviewer
  reading the Spark source could enumerate every case.
- **Medium**: hazards apply but are bounded and detectable **from the expression or the batch at
  runtime**, so the incompatible cases can be routed back through the dispatcher via
  `CodegenDispatchFallback` while the common case runs natively. Name the guard and say how it is
  detected. "We will document the difference" is not a guard.
- **Low**: matching Spark requires reimplementing a JVM library (ICU collation, `DecimalFormat`),
  or the incompatible cases cannot be detected before evaluating, or the only honest native path
  is `Incompatible` by default.

A `Low` rating is a hard disqualifier regardless of upside. The project's direction is the
opposite of shipping incompatible-by-default native paths (see
[#4506](https://github.com/apache/datafusion-comet/issues/4506) and the discussion in
[#4654](https://github.com/apache/datafusion-comet/issues/4654)). Verify both issues are still
open before citing them.

## Step 4: Score native upside

Read Spark's `doGenCode` (and `nullSafeEval`) for the expression and answer, concretely: **what
does the JVM allocate or recompute per row, and can a Rust kernel avoid it?**

Positive signals, strongest first:

1. **Per-row heap allocation in the generated code.** Count the allocation sites: `new
UTF8String`, `UTF8String.fromString`, `.getBytes()`, `Decimal.apply`, `new
GenericArrayData`, `new StringBuilder`, boxing. Multiply by batch size (8192) to state the
   allocation the native path removes. This is the most defensible upside argument, and it is
   the one that maps directly to the "reduction in memory allocation" question.
2. **Work that is constant across the batch but repeated per row.** A regex compiled per row, a
   formatter constructed per row, a literal argument re-parsed per row. Native kernels hoist
   this (see the technique table).
3. **A technique from `optimizing_expressions.md` that plainly applies.** Vectorized
   `unary`/`binary` kernels, zero-copy buffer reuse, `Cow` borrow, an ASCII byte-offset fast
   path, a lookup table, direct offset-buffer reads. Name the technique and the shape it helps.
4. **Nested or complex input/output types.** Per-row reads of `ListVector` / `StructVector` /
   `MapVector` through the JVM wrappers allocate per row per level. Native code operates on the
   child arrays and offsets directly.
5. **Cases that fall back to Spark entirely today** (found in Step 2). Removing a whole-operator
   fallback dwarfs removing a JNI round trip.
6. **Presence in real workloads.** TPC-H / TPC-DS / common ETL shapes. A large relative win on
   an expression nobody runs is not worth a contributor's week.

Anti-signals, each of which pushes the rating down:

- The generated code is straight primitive arithmetic with no allocation. The per-row JVM cost is
  already a few nanoseconds and the JNI round trip amortizes over 8192 rows.
- The cost is dominated by data-dependent work where Rust has no structural advantage (parsing a
  large JSON document, a heavy regex match).
- The result is a nested type that must be built element by element in either language.
- The expression is almost always folded away as a constant, or its argument is a literal that
  Spark's own optimizer already handles.

### Rate it

- **High**: allocation per row that scales with batch size and a named vectorization or zero-copy
  technique that removes it, on an expression that appears in mainstream workloads. Or removal
  of a whole-operator Spark fallback.
- **Medium**: real allocation or hoistable per-row work, but on an expression that is uncommon in
  practice, or where the achievable win is a modest constant factor.
- **Low**: primitive arithmetic with no allocation, or the win is confined to the per-batch JNI
  round trip, or the expression is rare.

## Step 5: Calibrate empirically when the verdict is borderline

Steps 3 and 4 are a static assessment and need no build. That is deliberate: the skill should be
cheap to run over a pool of candidates. But a `Medium` on either axis is exactly where a static
read is least trustworthy, so **when either rating is Medium, gather at least one piece of
empirical evidence before writing the verdict.** Building is required from here
(`make`; see `CLAUDE.md`).

Any one of these is sufficient, cheapest first:

1. **Count allocations in the actual generated kernel.** The dispatcher's emitted Java is
   inspectable without running a query, via `CometBatchKernelCodegen.generateSource`. Model a
   scratch test on `spark/src/test/scala/org/apache/comet/CometCodegenSourceSuite.scala`, dump
   the body for the candidate over a representative Arrow schema, and count the per-row
   allocation sites in the real code rather than in Spark's source. Delete the scratch test
   afterwards, or keep it only if it asserts something durable.
2. **Proxy benchmark against a comparable native expression.** Pick an already-`Native`
   expression with the same arity and types over the same data, and benchmark it against the
   candidate using the existing JVM harness (`CometBenchmarkBase.runExpressionBenchmark`, see
   `spark/src/test/scala/org/apache/spark/sql/benchmark/CometStringExpressionBenchmark.scala`).
   The gap between the two is an estimate of the ceiling for the native win. Say plainly that it
   is a proxy: it bounds the win, it does not predict it.

   ```bash
   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.Comet<Family>ExpressionBenchmark
   ```

3. **Allocation rate on a benchmark run.** Add `-Xlog:gc` (or a JFR recording) to the benchmark
   JVM and compare total allocated bytes for the candidate against the native comparison
   expression. Report bytes per row.

Whatever you measure, quote the numbers in the issue and the command that produced them. An
unquoted measurement is indistinguishable from a guess.

## Step 6: Verdict

Combine the two axes. Effort is a tiebreak, not a gate.

| Compatibility | Upside | Verdict                                                                                                        |
| ------------- | ------ | -------------------------------------------------------------------------------------------------------------- |
| High          | High   | **Recommended.** File the issue.                                                                               |
| High          | Medium | **Recommended** if the empirical calibration from Step 5 supports it, otherwise **Deferred** with the numbers. |
| High          | Low    | **Disqualified.** The dispatcher is already exact and the native win would be marginal.                        |
| Medium        | High   | **Recommended**, scoped to the compatible subset with a `CodegenDispatchFallback` guard for the rest.          |
| Medium        | Medium | **Deferred.** Record the blocker and what would change the answer.                                             |
| Medium        | Low    | **Disqualified.**                                                                                              |
| Low           | any    | **Disqualified.** Compatibility is a gate, not a trade-off.                                                    |

Every `Deferred` and `Disqualified` verdict must name the **specific** blocker, in a form a
future run can re-check: "the Rust `regex` crate has no lookaround, and Spark's pattern surface
allows it", not "compatibility risk". A vague blocker guarantees the expression gets re-assessed
from scratch, which is the exact waste this skill exists to prevent.

Also state the effort estimate (roughly: wiring an upstream function, a self-contained kernel, or
a kernel plus shims plus a new native code path) so a contributor can judge whether to pick it up.

## Step 7: File the issue

File exactly **one** issue per run.

- **Recommended**: file the proposal.
- **Deferred**: file it too, with the blocker as the headline and the "what would change the
  answer" condition spelled out. A deferral with a concrete unblocking condition is useful to the
  project; a deferral that lives only in this conversation is not.
- **Disqualified**: do **not** file an issue. The audit-log line in Step 8 is the record. In
  survey mode, go back to Step 1, take the next-ranked candidate, and assess that one, so the run
  still ends with one filed issue. In single-expression mode, report the disqualification to the
  user and stop after Step 8.

Check the labels exist before using them (`gh label list --repo apache/datafusion-comet`). Use
`enhancement`, `area:expressions`, and `performance`, plus the family label when one applies
(`temporal expressions`, `array expressions`, `json expressions`, `map expressions`). Do not
create new labels.

Title: `Implement <function> natively instead of JVM codegen dispatch`. For a deferral:
`Native candidate assessment: <function> deferred on <blocker>`.

Body sections, in this order:

1. **How it runs today.** The serde, the trait it uses, and what that means at runtime. Note any
   inputs that fall back to Spark entirely rather than to the dispatcher.
2. **Native upside.** The Step 4 rating with its evidence: allocation sites per row times batch
   size, the technique that removes them, workload presence, and any Step 5 measurement with the
   command that produced it.
3. **Compatibility assessment.** The Step 3 rating, the hazard checklist result (state which
   hazards apply and which were checked and cleared), cross-version differences that need shims,
   and whether an upstream `datafusion-spark` or `datafusion-functions` implementation exists.
4. **Proposed approach.** Either "wire the upstream function" (point at
   `wire-datafusion-function`) or "implement a native kernel" (point at
   `implement-comet-expression`). For a `Medium` compatibility rating, state the compatible
   subset, the runtime guard that detects the rest, and that the rest stays on
   `CodegenDispatchFallback`.
5. **Non-goals.** Anything the issue deliberately leaves out, most often the incompatible subset
   and any collation work.
6. **Acceptance criteria.** Bit-identical output for the covered subset including nulls and error
   behavior, a criterion benchmark in `native/spark-expr/benches/` covering the shapes required
   by `optimizing_expressions.md`, Comet SQL Tests for the expression, and an updated audit-log
   line.
7. **Footer.** A line noting the issue was produced by the `suggest-native-expression` skill,
   a link to the "Native Coverage for Codegen-Dispatched Expressions" section of the roadmap, and
   a pointer to the audit-log page where the assessment is recorded.

Use a temp file for the body to keep quoting sane:

```bash
gh issue create --repo apache/datafusion-comet \
  --title "Implement <function> natively instead of JVM codegen dispatch" \
  --label "enhancement,area:expressions,performance" \
  --body-file /tmp/native-candidate-<function>.md
```

Print the URL.

## Step 8: Record the assessment in the audit log

This step is not optional, and it runs for **every** verdict including `Disqualified`. It is what
makes the pool shrink over time instead of being re-walked on every run.

Add the line to the expression's category page under
`docs/source/contributor-guide/expression-audits/`, named after the Spark function-registry
category (`string_funcs.md`, `datetime_funcs.md`, and so on). Match the category used for the
expression in `docs/source/user-guide/latest/expressions.md`. If the page does not exist, create
it with the ASF license header, a `# <category> Expression Audits` title, and the standard intro
blockquote the other pages use. Add or reuse a `## <function_name>` heading, keeping headings
alphabetically ordered.

Get the date from the system, not from memory:

```bash
date -u +%Y-%m-%d
```

One line, in this shape:

```markdown
- Native candidate (assessed 2026-08-13): Disqualified. Compatibility: High (pure integer
  arithmetic, no locale or timezone dependence). Upside: Low (generated code allocates nothing
  per row; the only saving is the per-batch JNI round trip). Blocker: no measurable win to
  justify a native kernel. No issue filed.
```

```markdown
- Native candidate (assessed 2026-08-13): Recommended ([#5350](https://github.com/apache/datafusion-comet/issues/5350)).
  Compatibility: High (upstream `datafusion-spark` implementation matches Spark for all input
  types; no collation surface). Upside: High (two `UTF8String` allocations per row, 16k per
  batch, removable with a zero-copy `Cow` borrow; appears in TPC-DS q13).
```

Rules for the line:

- Always include the verdict, both ratings with a parenthesized one-clause reason each, and
  either the issue link or `No issue filed`.
- `Deferred` and `Disqualified` lines must carry a `Blocker:` clause naming the specific thing
  that would have to change.
- Never invent an issue number. Use the URL that `gh issue create` printed, or verify an existing
  one with `gh issue view <N> --repo apache/datafusion-comet`.
- Never edit or delete a previous `Native candidate` line. A re-assessment adds a new dated line
  below it, and says what changed since the last one.
- Keep it to one bullet. These pages are scanned, not read.

Then format the markdown, because CI checks it:

```bash
npx prettier "docs/source/contributor-guide/expression-audits/*.md" --write
```

Commit the audit-log change on a branch and open a PR for it (the repo's PR template applies).
The filed issue and the audit-log line are two halves of one deliverable: the issue is the call to
action, the log line is the durable record that the call was made.

## Red flags: stop and re-read

- **"Native is obviously faster than JVM."** The dispatcher already runs in-pipeline with
  zero-copy string reads and a compile-once kernel. If you cannot name the per-row allocation or
  the vectorization technique, the upside rating is `Low` and the verdict is `Disqualified`.
- **"We can match Spark for the common cases."** Then the compatibility rating is `Medium` at
  best, and the proposal must name the runtime guard and keep the rest on
  `CodegenDispatchFallback`. Replacing an exact path with an approximate one is a regression, not
  an optimization.
- **"The expression has no audit entry, so nobody has looked at it."** Run the Step 1 grep. A
  `Disqualified` line means somebody did look, and re-proposing it without addressing the
  recorded blocker wastes a reviewer's time.
- **"I will estimate the speedup."** An estimate presented as a measurement is the failure mode
  that produces abandoned issues. Either measure it (Step 5) or label it explicitly as a static
  estimate in both the issue and the log line.
- **"This is a big win, I will file issues for the whole family."** One issue per run. A family
  sweep needs a project-direction discussion, not a batch of generated issues.

## Rationalization table

| Excuse                                                           | Reality                                                                                                                                     |
| ---------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| "Recording a disqualification is busywork, nobody reads the log" | The log is the only thing that stops the next run from re-assessing the same expression. An unrecorded disqualification is a repeated cost. |
| "The blocker is obvious from the discussion"                     | The discussion evaporates when the session ends. A blocker that is not in the log line does not exist.                                      |
| "Upstream probably has it, I will assume it matches Spark"       | `datafusion-spark` functions are not all complete. Read the implementation and diff the semantics before crediting it.                      |
| "Compatibility can be sorted out during implementation"          | Compatibility is the gate. An issue that hides a `Low` rating sends someone down a path the project will not merge.                         |
| "I will file the issue and skip the audit-log PR"                | Then the pool never shrinks. Both deliverables or neither.                                                                                  |

## Related skills

- `wire-datafusion-function` — the follow-through when upstream already has the function.
- `implement-comet-expression` — the follow-through when a new native kernel is needed.
- `optimize-comet-expression` — for an expression that is already native and merely slow.
- `audit-comet-expression` — for a `Hybrid` expression whose native path has a compatibility gap.

## Tone and style

- Show the ranking, the checklist result, and the numbers. A verdict without its evidence cannot
  be challenged, and an unchallengeable verdict is not useful.
- Use backticks around function names, config keys, file paths, types, and class names.
- Be explicit about what is measured versus estimated.
- Avoid em dashes and semicolons; use separate sentences instead.
