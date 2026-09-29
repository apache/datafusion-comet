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
| `CometExecIterator`, `CometNativeArrowSource`, `NativeUtil`, `CometVector` subclasses, `jni_api.rs`, `scan.rs`, `aligned_stream_reader.rs`, anything using `FFI_Arrow*`                    | `review-comet-ffi-pr`           |
| `native/core/src/execution/memory_pools/`, `CometTaskMemoryManager`, `CometShuffleMemoryAllocator`, `MemoryConsumer` / `try_grow` / `reserve` call sites, pool configs                     | `review-comet-memory-pr`        |
| `native/shuffle/`, `spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/`, `spark/src/main/java/org/apache/spark/shuffle/`, `CometShuffleExchangeExec`                       | `review-comet-shuffle-pr`       |
| `IcebergWriteStrategy`, `IcebergWriteExec`, `IcebergCommitExec`, `CometIcebergWriteExec`, `CometIcebergNativeWrite`, `iceberg_write.rs`, `iceberg_partition_path.rs`, the iceberg-rust pin | `review-comet-iceberg-write-pr` |

For a new operator, read `docs/source/contributor-guide/adding_a_new_operator.md` alongside this
skill. There is no dedicated operator review skill yet.

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

### Spark version coverage

Comet supports several Spark versions. Version-specific behavior belongs in the shims under
`spark/src/main/spark-{3.4,3.5,3.x,4.0,4.1,4.2}/org/apache/comet/shims/`, not in branches on a
version string in shared code, and not in native Rust. If the PR adds a shim for one 4.x version,
check that the sibling 4.x source sets got it too.

### Configuration

New configs go in `CometConf.scala` and must follow
`docs/source/contributor-guide/config_conventions.md`. Check the naming, the default, the category,
and that the description reads as user-facing documentation, because it is. `configs.md` is
generated from it.

### Tests

- Does the PR test the thing it changed, or only that nothing else broke?
- Are the tests in the right framework for the area? The area skills say which.
- Does a bug fix come with a test that fails without the fix?
- New suites must be registered in both `.github/workflows/pr_build_linux.yml` and
  `pr_build_macos.yml`.

### CI

`gh pr checks <pr> --repo apache/datafusion-comet`, and again with `--failed` for detail.

Summarize failures in the review. Do not compare against failures on `main`.

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

## Output Format

Present your review as guidance for the reviewer:

1. **PR Summary**, brief description of what the PR does
2. **Areas**, which sibling skills you loaded and why
3. **CI Status**, summary of CI check results
4. **Findings**, organized by area
5. **Suggested Review Comments**, specific comments the reviewer could leave, with file and line
   references. Everything here is something you expect the author to address. Anything that did not
   clear the bar above should not appear.

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
