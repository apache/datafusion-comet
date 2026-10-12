---
name: review-comet-pr
description: Use when reviewing a DataFusion Comet pull request. Covers the workflow that applies to every PR and routes to the area-specific review skills for expressions, FFI, memory management, shuffle, and Iceberg writes. Provides guidance to a human reviewer rather than posting comments.
argument-hint: <pr-number>
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

Review Comet PR #$ARGUMENTS

This is the entry point for every Comet PR review. It covers what applies regardless of what the PR
touches. Depth for a specific subsystem lives in a sibling skill, and step 1 tells you which ones to
load.

## 1. Route to the Area Skills

**Do this before reading the diff in detail.** Look at the list of changed files and load every
sibling skill whose area the PR touches. More than one usually applies. A shuffle change that alters
spilling is also a memory change. An expression that returns a new array type may also be an FFI
change.

| The PR touches                                                                                                                                                                             | Also use                        |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------- |
| `spark/src/main/scala/org/apache/comet/serde/`, `QueryPlanSerde.scala`, `native/spark-expr/`, `expr.proto`, `comet_scalar_funcs.rs`                                                        | `review-comet-expression-pr`    |
| `CometExecIterator`, `CometNativeArrowSource`, `NativeUtil`, `CometVector` subclasses, `jni_api.rs`, `scan.rs`, anything using `FFI_Arrow*`                                                | `review-comet-ffi-pr`           |
| `native/core/src/execution/memory_pools/`, `CometTaskMemoryManager`, `CometShuffleMemoryAllocator`, `MemoryConsumer` / `try_grow` / `reserve` call sites, pool configs                     | `review-comet-memory-pr`        |
| `native/shuffle/`, `spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/`, `spark/src/main/java/org/apache/spark/shuffle/`, `CometShuffleExchangeExec`                       | `review-comet-shuffle-pr`       |
| `IcebergWriteStrategy`, `IcebergWriteExec`, `IcebergCommitExec`, `CometIcebergWriteExec`, `CometIcebergNativeWrite`, `iceberg_write.rs`, `iceberg_partition_path.rs`, the iceberg-rust pin | `review-comet-iceberg-write-pr` |

For a new operator, read `docs/source/contributor-guide/adding_a_new_operator.md` alongside this
skill. There is no dedicated operator review skill yet.

For a PR that deals with timestamps or the session timezone, read
`docs/source/contributor-guide/timezones.md` as well, whichever areas it touches. Timezone handling
cuts across the serde, the native kernels, the scans and the JVM/native boundary, so no area skill
owns it. "Timestamps and timezones" in step 5 says what to check.

If the PR falls outside all of these, for example build, CI, docs only, or release tooling, this
skill on its own is the review.

## 2. Gather PR Metadata

Use `gh pr view` against `apache/datafusion-comet` to fetch the title, body, author, draft state,
state, and changed files.

## 3. Review Existing Comments First

Before forming your review:

1. **Read all existing review comments** on the PR
2. **Check the conversation tab** for any discussion
3. **Avoid duplicating feedback** that others have already provided
4. **Build on existing discussions** rather than starting new threads on the same topic
5. **If you have no additional concerns beyond what is already discussed, say so**
6. **Ignore Copilot reviews.** Do not reference or build upon comments from GitHub Copilot.

`gh pr view <pr> --repo apache/datafusion-comet --comments` shows them.

## 4. Read the Diff

`gh pr diff <pr> --repo apache/datafusion-comet`

Read the surrounding code, not just the diff hunks. Comet has strong local conventions, and the most
common review finding is a change that works but does not match how the neighbouring code solves the
same problem.

## 5. Checks That Apply to Every PR

### Spark compatibility

This is the point of the project. Comet must produce the same results as Spark, or declare that it
does not. Whatever the PR changes, ask what Spark does in the same situation and where the answer
came from. "It looks right" is not an answer. Reading the Spark source is.

### Support levels

Anything that reports `getSupportLevel()` must report it honestly:

- `Compatible()`, matches Spark exactly
- `Incompatible(Some("reason"))`, differs in documented ways and requires
  `spark.comet.expr.allowIncompatible=true`
- `Unsupported(Some("reason"))`, cannot be implemented

A PR that quietly marks something `Compatible` while the diff shows a known divergence is the single
most important thing to catch. Keep reasons concise and link a tracking issue when the behavior is
known to differ.

Two disguised forms of the same thing:

- A divergence documented in the compatibility guide on a path that is enabled by default. If the
  guide says the results differ, the default has to fall back for that case (#5469, #5507).
- A test switched to `ignore` in the PR because Comet's answer now differs from Spark's. An ignored
  test is a known divergence, so the code needs a fallback for that case (#5701).

### Behavior change against the latest release

Users upgrade from a release, not from `main`. So the question is not only what the PR changes
relative to `main`, but whether a user moving from the latest release to a build with this PR sees
different behavior. `main` may already carry unreleased changes in the same code, and a PR that
looks harmless against `main` can finish turning a released behavior into a different one.

Find the latest release branch, the highest `branch-X.Y`:

```shell
git ls-remote --heads https://github.com/apache/datafusion-comet 'branch-*' | sort -t- -k2 -V | tail -1
```

Fetch the PR head and diff the files the PR touches against that branch, not against `main`:

```shell
git fetch https://github.com/apache/datafusion-comet branch-X.Y:release-X.Y pull/<pr>/head:pr-<pr>
git diff release-X.Y pr-<pr> -- <changed files>
```

Then ask, for each code path the PR touches, whether any of these differ from the release:

- the result for some input, including null, NaN, `-0.0`, empty, overflow, and timezone cases
- whether a query raises an error, and which error
- whether an operator or expression runs natively or falls back to Spark
- a config default, a config name, or a support level
- performance or memory use on an existing path

The tests the PR adds are a good probe. If a new test would fail on the release branch, the PR
changes released behavior, and you should know which of those two cases it is. Running the test
against the release branch is the cheapest way to find out when the answer is not obvious from
the code.

Every behavior change against the release is one of two things:

- **Intended.** A bug fix that makes Comet match Spark, or a deliberate change. The PR description
  should say so, user-facing changes need a note in the user guide or compatibility docs, and a
  correctness fix should be considered for backport to the release branches per
  `docs/source/contributor-guide/backporting.md`.
- **Unintended.** The PR, alone or together with unreleased changes already on `main`, makes Comet
  diverge from Spark where the release did not, or makes a released path slower. This is a
  correctness or performance regression, and it falls under the request-changes rule below.

Look hardest when the PR is one of the three kinds of change behind most of the regressions that the
1.1.0 audit found ([#6399](https://github.com/apache/datafusion-comet/issues/6399)):

- **A path that becomes native by default.** The query used to fall back to Spark and was right.
  Find the inputs where the native path differs from Spark, and either test them or fall back for
  them. Making map and struct literals native exposed a type mismatch in the native `IF` (#6334).
  Constant metadata columns went native with a per-split value that DataFusion and Spark assign
  differently (#6505).
- **A removed fallback or guard.** List everything the fallback was shielding, not just the case the
  PR is about. Removing the Iceberg complex-type null-check fallback also exposed every `explode` of
  an Iceberg array to a schema-evolution bug in iceberg-rust (#6504).
- **A broad routing change.** A catch-all that sends more expressions down a path, such as the JVM
  codegen dispatcher, admits shapes the path never handled. List the new shapes and test one of each
  (#6424, #6425).

Report the comparison in the review even when nothing changed, so the reviewer knows it was done.

### Spark version coverage

Comet supports several Spark versions. Version-specific behavior belongs in the shims under
`spark/src/main/spark-{3.4,3.5,3.x,4.0,4.1,4.2}/org/apache/comet/shims/`, not in branches on a
version string in shared code, and not in native Rust. If the PR adds a shim for one 4.x version,
check that the sibling 4.x source sets got it too.

When Spark changed the behavior in a patch release, such as SPARK-55969 or SPARK-54918, a check on
the minor version is wrong for every earlier patch. CI builds only the newest patch of each line, so
it can't catch that (#6042, #5701). The pull request CI also runs only the default Spark profile, so
logic that depends on the Spark version needs the matching `run-spark-*` labels (#6156).

### Timestamps and timezones

Timezone bugs are easy to miss in review and in tests. A mislabelled timestamp column passes any
test that only projects it, and a result computed in the wrong timezone looks plausible. A PR deals
with timezones if it touches a datetime expression, a cast to or from a timestamp, how a scan reads
timestamps, the timestamp type at the JVM/native boundary, or anything that reads the session
timezone. Searching the diff flags most of these PRs:

```shell
gh pr diff <pr> --repo apache/datafusion-comet | grep -inE 'time_?zone|zoneid|chrono_tz|timestamp(ntz)?type|timestampmicro|timestamp\('
```

For such a PR, hold the diff against "The invariant" and "Guidelines" in
`docs/source/contributor-guide/timezones.md`. Look for these first:

- A `TimestampType` value labelled with the session timezone, or with no timezone. Inside a native
  plan every `TimestampType` value is labelled exactly `"UTC"`, and every `TimestampNTZType` value
  has no timezone. Check the declared type and the arrays the code builds, not only the values.
- A timezone taken from the JVM default or the host. An expression uses the `timeZoneId` Spark
  stamped on it, passed through `CometTimeZone.nativeId`, not `SQLConf.get.sessionLocalTimeZone`.
- A path gated on a UTC session. `Etc/UTC` is the session default on Ubuntu and Debian images, so
  check what the gate does with it, and that the output there is still labelled `"UTC"` rather than
  `"Etc/UTC"`.
- A timezone applied to a `TimestampNTZType` value, other than to convert it to `TimestampType`.
- Tests that use a single session timezone, or only project the result. "Testing timezone-sensitive
  code" in the same page says what to ask for.

`timezones.md` also describes specific code: where the `"UTC"` label is set, which serdes serialize
a timezone, how `CometTimeZone` rewrites timezone IDs, what `array_with_timezone` does, how the
scans adapt timestamps, and which expressions go through the codegen dispatcher. A PR that changes
any of these updates the page in the same PR. The page also documents some known limitations as
current behavior, such as chrono-tz's DST horizon and the timezone database versions. A fix for one
of the bugs tracked in [#6335](https://github.com/apache/datafusion-comet/issues/6335) usually
changes that text too.

### Configuration

New configs go in `CometConf.scala` and must follow
`docs/source/contributor-guide/config_conventions.md`. Check the naming, the default, the category,
and that the description reads as user-facing documentation, because it is. `configs.md` is
generated from it.

A new behavior that is on by default and trades performance for some workloads needs a supported
config, not a testing one, to turn it off before merge (#6466).

### Tests

- Does the PR test the thing it changed, or only that nothing else broke?
- Are the tests in the right framework for the area? The area skills say which.
- Does a bug fix come with a test that fails without the fix?
- Do the tests compare against Spark? A comparison with Comet's own accumulator, or with a second
  implementation such as iceberg-rust instead of the Iceberg Java that Spark runs, can agree on the
  wrong answer (#6423, #6426).
- New suites must be registered in both `.github/workflows/pr_build_linux.yml` and
  `pr_build_macos.yml`.

For a change on a default path, look for tests with the inputs that broke earlier changes:

- `-0.0` next to `0.0`, and NaN with different payloads, including inside arrays and structs (#5469,
  #5507, #5701)
- Values at a type or unit boundary, including negative timestamps before 1970 (#6426)
- A constant that binary floating point can't represent, such as 0.1, aggregated across partitions
  (#6423)
- A batch where every row takes the same branch (#6334)
- Output that the native side slices, so it reaches the JVM past the first batch with a non-zero
  offset (#6464)
- A Parquet file that Spark splits into several partitions, an empty file, and an Iceberg file
  written before a nested field was added (#6504, #6505, #6506)
- Data that defeats a heuristic, such as distinct keys at the start of a task followed by repeats
  (#6466)
- One of a pair of aliases configured without the other (#5825)

### CI

`gh pr checks <pr> --repo apache/datafusion-comet`, and again with `--failed` for detail.

Summarize failures in the review. Do not compare against failures on `main`.

Some paths have no CI coverage at all. Nothing in CI reads from a real object store, runs Iceberg's
forward-compatibility tables, or makes a spilled operator replay under a tight memory pool. A change
on one of those paths needs a local run, and the review should ask for one (#5759, #6254).

### Dependency upgrades

Review a dependency bump as a set of behavior changes, not an API migration. For DataFusion, arrow,
parquet and iceberg-rust, diff the upstream source of what Comet calls on default paths. Both
versions are in the local Cargo registry after a build of each side. A test that the bump changes to
`ignore` or to a new expected value marks a behavior change that needs a fallback or an explicit
decision. The DataFusion 55 upgrade changed signed-zero handling in `array_distinct` and
`array_union` (#5701), the spill replay of the final aggregate (#6254), and, through iceberg-rust,
task validation (#5759).

## 6. Documentation Freshness

Every PR carries a documentation question, and it has two halves.

**Half one: does the PR need to add user-facing documentation?** New user-visible behavior belongs in
the user guide.

**Half two: does the PR make existing contributor documentation wrong?** This half is the one that
gets missed. The contributor guide describes how subsystems actually work today. It contains class
tables, file paths, config defaults, diagrams, and stated invariants. A PR that renames a class,
moves a file between crates, changes a default, adds a case to a selection rule, or changes an
ownership rule silently turns a paragraph of that guide into a lie. The doc update belongs in the
same PR, because a follow-up that is not filed as an issue does not happen.

Each area skill names the doc for its subsystem and the specific claims in it that go stale. When you
load an area skill, read its doc and hold the diff against it.

**Do not ask contributors to hand-edit generated docs.** The compatibility guide pages under
`docs/source/user-guide/latest/compatibility/expressions/` and
`docs/source/user-guide/latest/configs.md` are produced by `GenerateDocs` and regenerated by CI on
every merge to `main`. Review the source that feeds them instead.

## Review Bar

Hold a high bar. Several rounds of review and revision before merge are normal and expected. Prefer
iterating on the PR over merging it and trusting a follow-up, because follow-ups that are not filed
as issues do not get done. Treat "we can fix that later" as "that will not get fixed."

Every finding must be actionable. Before writing a comment, decide whether it is worth addressing
before merge. If it is, raise it and expect a response. If it is not, cut it. There is no third tier.

- Never label feedback as "not a blocker", "nit", "minor", "optional", "low priority", or "feel free
  to ignore". Either raise it as something to address or drop it. That label tells the author to skip
  it and leaves nobody accountable for it.
- If a finding is real but genuinely belongs in separate work, say that plainly and ask for a
  tracking issue, then reference the issue link in the review. Do not leave it as a floating remark.
- Bikeshedding is worse than silence. A preference with no correctness, performance, compatibility,
  or maintainability argument behind it does not go in the review.
- This bar is about what gets raised, not about how it is worded. Keep the tone below. A question you
  expect an answer to still counts as something the author needs to address.

## Request Changes for Correctness and Performance Regressions

Two kinds of finding have to block the merge until the author addresses them: a correctness problem
that the PR introduces, and a performance regression. A correctness problem means Comet can return a
different answer from Spark, including a result where Spark raises an error, or `Compatible()` over a
known divergence. Crashes, hangs, leaks, and lost or corrupted data count too. A performance
regression means an existing path gets slower or uses more memory. A tracking issue for a later fix
does not address either one.

When any finding is one of these, the review is submitted as **Request changes**. `main` needs a
single approval to merge, and any committer's approval counts, including one given before this
review. A **Comment** review does not stop that approval from merging the PR over the finding. A
**Request changes** review from a reviewer with write access blocks the merge until the same reviewer
approves the PR or someone with write access dismisses the review.

Requesting changes commits the reviewer to the re-review. Once the author has addressed the findings
behind it, the reviewer approves the PR or dismisses the review. A later **Comment** review leaves
the block in place.

## Output Format

Present your review as guidance for the reviewer:

1. **PR Summary**, brief description of what the PR does
2. **Areas**, which sibling skills you loaded and why
3. **CI Status**, summary of CI check results
4. **Behavior vs Release**, which release branch you compared against, and every behavior change
   you found, each marked intended or unintended. Say "no change" when there is none.
5. **Findings**, organized by area
6. **Suggested Review Comments**, specific comments the reviewer could leave, with file and line
   references. Everything here is something you expect the author to address. Anything that did not
   clear the bar above should not appear.
7. **Review State**, how to submit the review. Use the first case that applies:
   - **Request changes** when any finding is a correctness problem that the PR introduces or a
     performance regression, including an unintended behavior change against the latest release.
     Name those findings.
   - **Approve**, or dismiss the earlier review, when this is a re-review and the findings behind the
     reviewer's earlier **Request changes** review have all been addressed.
   - **Comment** otherwise.

## Review Tone and Style

Write reviews that sound human and conversational. Avoid:

- Robotic or formulaic language
- Em dashes. Use separate sentences instead.
- Semicolons. Use separate sentences instead.

Instead:

- Write in flowing paragraphs using simple grammar
- Keep sentences short and separate rather than joining them with punctuation
- Be kind and constructive, even when raising concerns
- Use backticks around any code references such as function names, file paths, class names, types,
  and config keys
- **Suggest** adding tests rather than stating tests are missing
- **Ask questions** about edge cases rather than asserting they are not handled
- Frame concerns as questions or suggestions when possible
- Acknowledge what the PR does well before raising concerns

## Do Not Post Comments

**IMPORTANT: Never post comments or reviews on the PR directly.** This skill and all of its siblings
are for providing guidance to a human reviewer. Present all findings and suggested comments to the
user. The user will decide what to post.

When the user tells you to post the review for them, submit it in the review state from the output.
For **Request changes** that is
`gh pr review <pr> --repo apache/datafusion-comet --request-changes --body-file <file>`. A PR comment
or a **Comment** review does not block the merge.
