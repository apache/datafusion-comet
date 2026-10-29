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

# Documentation Style Guide

This guide is for anyone who writes or reviews Comet documentation: pages under `docs/source`,
`README.md`, and the user-facing `.doc(...)` strings in `CometConf.scala` and in contrib config
classes. It defines the words Comet's docs use for how a query runs, and the conventions every page
follows. The vocabulary rules (`T`, `N`, `O`) and the accuracy rules (`P13`, `P16`) also apply to doc
comments, `AGENTS.md`, and the agent skills under `.ai/skills`, with the contributor guide's
allowances.

The sections cover how Comet runs a query, the vocabulary, the rule for "native", operator names,
page conventions, and generated content.

Each rule has an ID, such as `N1`. Reviews cite the ID, so IDs are never renumbered or reused.
Where this guide and the code disagree, the code is right and the guide needs a fix. The
background for the vocabulary is in [#4419](https://github.com/apache/datafusion-comet/issues/4419).

## How Comet Runs a Query

The terms below describe this model. [Comet Plugin Architecture](plugin_overview.md) has the details.

- Comet's planner rules replace the Spark operators Comet supports with Comet operators, most of
  them named `Comet*`, and serialize their expressions to protobuf. An operator Comet does not
  support stays a Spark operator.
- Most Comet operators run in Comet's Rust code, which is built on Apache DataFusion and is called
  from the JVM through JNI. Some do their own work on the JVM, such as broadcast exchanges.
- Comet's query operators usually pass data to each other as Apache Arrow batches. Some expressions
  run Spark's own generated JVM code over those batches.

## Vocabulary

Use the terms in the first column. They answer three separate questions: does Comet handle the
work, which language runs it, and what format is the data in. A term from one question says nothing
about the other two.

| ID  | Use                                               | For                                                                                                                   | Instead of                                      |
| --- | ------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------- |
| T1  | _Comet pipeline_, _runs in Comet_                 | Work that a Comet operator handles, whatever the language                                                             | "the native path", "runs natively"              |
| T2  | _falls back to Spark_, _Spark fallback_           | Work that stays in a Spark operator                                                                                   | —                                               |
| T3  | _Rust-implemented_, _runs in Rust_, _native Rust_ | Work done by Comet's Rust code, usually through DataFusion                                                            | "native" on its own                             |
| T4  | _JVM-side_, _runs on the JVM_                     | A Comet operator whose own work runs on the JVM, such as `CometBroadcastExchange`                                     | "non-native Comet operator"                     |
| T5  | _codegen dispatch_                                | Spark's generated JVM code for an expression, run over Arrow batches from inside a Rust-implemented plan              | "falls back" (it does not fall back)            |
| T6  | _Native_, _Codegen dispatch_, _Hybrid_            | The expression kinds in the [Implementation legend](../user-guide/latest/expressions.md#implementation-legend)        | Any other meaning of "Hybrid"                   |
| T7  | _Arrow-native_, _Arrow batches_, _Arrow columnar_ | Claims about data format: the data is held in Arrow columnar format                                                   | "vectorized" as a synonym for columnar (T10)    |
| T8  | _Arrow IPC_                                       | The format shuffle and broadcast write                                                                                | —                                               |
| T9  | _Spark rows_ (`UnsafeRow`), _Spark columnar_      | Spark's row format, or a Spark `ColumnarBatch` that is not backed by Arrow                                            | "row format", "JVM format" without saying which |
| T10 | _vectorized_                                      | Proper names, such as Spark's vectorized Parquet reader, and batch-at-a-time kernels contrasted with row-by-row loops | "vectorized" as a synonym for columnar          |

Notes on these terms:

- **Hybrid (T6)** describes an expression that has both a Rust path and a Spark-compatible path,
  usually codegen dispatch. Each use of the expression in a plan takes one path. The term comes
  from `GenerateDocs`, which writes the Implementation column of `expressions.md`. Do not use
  "hybrid" for a plan that mixes Comet and Spark operators. That is a plan with fallback (T2).
- **Arrow-native (T7)** is about format. It does not mean that no per-row work happens: codegen
  dispatch evaluates each row, and JVM shuffle buffers `UnsafeRow`s before encoding them to Arrow
  IPC.
- **Spark rows (T9)** can appear inside the Comet pipeline. JVM shuffle, for example, buffers
  `UnsafeRow`s. The format alone does not say whether Spark or Comet handles the work.
- **Expressions are not plan nodes.** An expression runs inside an operator such as `CometProject`
  or `CometFilter`. A codegen-dispatched expression leaves its operator Rust-implemented.

## The Rule for "Native"

"Native" has several meanings in Comet's docs, so on its own it does not tell the reader which one
is meant. Name the meaning instead.

- **N1.** Do not use _runs natively_, _natively in Comet_, _the native path_, _the native
  pipeline_, _native execution_, _native operator_, or _native Comet operator_, in any tense or
  form, such as _ran natively_, _go native_, _a native path_, or _Comet-native_. Write _in the Comet
  pipeline_ (T1) or _Rust-implemented_ (T3).
- **N2.** These compounds are allowed everywhere, because the second part fixes the meaning:
  _native Rust_; _native shuffle_ and _the native shuffle path_ (the counterpart of _JVM shuffle_,
  which need not appear beside it); "native", optionally followed by a format, followed by _scan_,
  _reader_, _write_, or _writer_, only for Comet's Rust-implemented scans and writers (_native scan_,
  _native Parquet reader_, _native Iceberg write_), not for a JVM-side scan such as
  `CometInMemoryTableScanExec`; _native memory_ and _native library_ (the terms Comet's own log
  lines use); _Arrow-native_ (T7); _Native_ as the expression kind (T6); and _native implementation_
  of an expression, meaning its Rust implementation.
- **N3.** In the contributor guide, "native" before a code noun is also allowed when it means the
  crates under `native/` or the JNI boundary: _native code_, _native side_ (and _native-side logs_),
  _native plan_, _native kernel_, and _JVM/native boundary_. The noun must name something under
  `native/`, such as a crate, file, type, function, or plan, or the JNI boundary. Pages outside the
  contributor guide, including `user-guide/`, `about/`, `index.md`, `README.md`, and `.doc(...)`
  strings, allow only the N2 compounds and write _Rust_ instead. N1 applies everywhere. Any other use
  of "native" is a finding: name the meaning with a term from the vocabulary above.
- **N4.** Do not use "native" as a noun. Write _the Rust side_ or name the component: "hands each
  block to the Rust side", not "hands each block to native".
- **N5.** Do not call Spark's own behavior native. Write _Spark's built-in shuffle_, not _Spark's
  native shuffle_.
- **N6.** Quote identifiers, config keys and values, API names, log lines, and plan output exactly
  as they are, in code font. `CometNativeScan`, `CometNativeExec`, and
  `spark.comet.shuffle.mode=native` are not prose and do not break N1. Text that quotes a phrase in
  order to discuss it, such as the "Before" column below, does not break these rules either.

Examples:

| Before                                                     | After                                                         |
| ---------------------------------------------------------- | ------------------------------------------------------------- |
| Runs your Spark queries on the DataFusion native engine.   | Speeds up your existing Spark queries with Apache DataFusion. |
| The surrounding native operators keep running natively.    | The surrounding operators stay in the Comet pipeline.         |
| Keeps evaluation inside the native pipeline.               | Keeps evaluation inside the Comet pipeline.                   |
| Takes columnar input directly from Comet native operators. | Takes Arrow batches directly from Comet operators.            |

## Operators, Plan Names, and Expressions

[Understanding Comet Plans](../user-guide/latest/understanding-comet-plans.md) describes the nodes
a plan can contain, grouped by role. Its
[Comet Operator Reference](../user-guide/latest/understanding-comet-plans.md#comet-operator-reference)
lists them. Link to it rather than copying it.

Do not infer an operator's kind from its class hierarchy. For example,
`CometTakeOrderedAndProjectExec` runs Rust plans without extending `CometNativeExec`. Most Comet
operators print a `Comet` prefix, but not all: `IcebergWrite` and `IcebergCommit` are Comet
operators too.

- **O1.** In prose about plans, use the name that plan output prints. Spark drops the `Exec` suffix
  from most names (`CometProjectExec` prints as `CometProject`), but some operators keep it, such as
  `CometWindowExec`, and `CometShuffleExchangeExec` prints as `CometExchange` or
  `CometColumnarExchange`. This is the `nodeName`, which formatted explain output and the Spark UI
  print. Some classes also override `simpleString`, which changes the line `df.explain()` prints:
  `CometTakeOrderedAndProjectExec` prints as `CometTakeOrderedAndProjectExec(...)` there. Quote plan
  output exactly as it was printed (`N6`), and check the operator reference when unsure.
- **O2.** Use the class name, with `Exec`, only when the text is about the code, including text
  about how a planner rule rewrites a plan.
- **O3.** The current names are the correct names. Do not describe renames that have not happened.
- **O4.** Expressions are not operators. Describe an expression by its kind (T6), not as a node in
  the plan.

## Page Conventions

- **P1.** Every new Markdown file starts with the ASF license header, as in this file.
- **P2.** A page has one H1, and heading levels are not skipped.
- **P3.** The first paragraph says who the page is for and what it covers. A long page then gives
  a short overview of its sections.
- **P4.** On pages under `docs/source`, link to another page by its path, either relative to the
  current page or starting with `/` for the `docs/source` root, not by the published docs URL,
  because the site's address can change. `README.md` is rendered by GitHub, so it links to the
  published docs.
- **P5.** Do not hand-edit generated content. Change the source it comes from instead. See
  [Generated Content](#generated-content).
- **P6.** Every new page is added to a toctree. Pages for users go under `docs/source/user-guide/`
  or `docs/source/about/`, and pages for contributors go under `docs/source/contributor-guide`.
- **P7.** The user guide describes what Comet does today, in the present tense. Plans go in the
  [roadmap](roadmap.md). Do not leave `TODO` or `XXX` markers in published pages.
- **P8.** A claim that depends on a version, such as "Spark 3.5 and later", "Spark 3.5+", or "Spark
  4.0 only", matches the versions the code supports. The same applies to other dependencies, such
  as Iceberg.
- **P9.** Prefer the active voice, and address the reader as "you".
- **P10.** Do not use marketing words such as "blazing fast", "seamless", or "powerful". State the
  measured result, or leave the claim out.
- **P11.** Use a table to compare several items, such as operators, configs, or support levels.
- **P12.** Plan output in a page is real output from `explain`, in Spark's tree format with `+-`
  connectors, and is shown in a code block.
- **P13.** Config keys, class names, and file paths named in a page exist in the code. Code
  examples are valid in the language their block declares. Follow
  [Configuration Conventions](config_conventions.md) for new configs.
- **P14.** A new user guide page includes at least one example a reader can run and links to
  related pages. A new contributor guide page that describes a procedure says how to check that it
  worked.
- **P15.** A change that adds an operator updates the
  [Comet Operator Reference](../user-guide/latest/understanding-comet-plans.md#comet-operator-reference)
  and [Supported Spark Operators](../user-guide/latest/operators.md) in the same pull request. A
  change that adds an expression updates its row in
  [Spark Expression Support](../user-guide/latest/expressions.md).
- **P16.** Describe the code as it is now. Remove text about code that has been removed, keep
  sample output, such as explain output and messages, in step with the code that produces it, and
  keep each page consistent with itself.
- **P17.** A change that moves, renames, or removes a page adds an entry to `redirects` in
  `docs/source/conf.py`, so that existing links keep working.
- **P18.** Text, diagrams, and images taken from another project or website say where they came
  from, and their license is compatible with the Apache License. See the ASF's
  [third-party licensing policy](https://www.apache.org/legal/resolved.html).

### Generated Content

These parts of the docs are written by tools. Edit the source they come from instead. The rest of
each file, including the prose around the markers and the `index.md` pages, is written by hand.

- **Config tables**, the text between `<!--BEGIN:CONFIG_TABLE[...]-->` and
  `<!--END:CONFIG_TABLE-->`. `GenerateDocs` builds them from the config entries in `CometConf.scala`
  and from the contrib config classes registered with `CometConfigProvider`. The `enable_expr` and
  `enable_agg_expr` tables come from the expression serde maps in `QueryPlanSerde` instead.
- **Expression compatibility sections**, the text between `<!--BEGIN:EXPR_COMPAT[...]-->` and
  `<!--END:EXPR_COMPAT-->` in `compatibility/expressions/_category_template/`. `GenerateDocs` builds
  them from the expression serdes.
- **Cast tables**, the text between the `CAST_*_TABLE` markers. `GenerateDocs` builds them from
  `CometCast.supportedTypes`, `CometCast.isSupported`, and Spark's `Cast.canCast`.
- **Per-version expression pages** built from `_category_template/`. On `main`, the docs build
  creates them in a temporary tree. Release branches commit a generated copy, made with
  `dev/generate-release-docs.sh`.
- **The Implementation column** in `expressions.md`, built by `GenerateDocs` from the expression
  serdes in `QueryPlanSerde`.
- **`changelog/`**, written by the release process.

The expression audits, data source audits, and benchmark results under `contributor-guide/` are
records of past work, not pages to restyle.

## Changing This Guide

Propose changes in a pull request. Add new rules with new IDs, and do not renumber or reuse an ID,
even when a rule is removed.
