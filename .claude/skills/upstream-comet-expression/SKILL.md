---
name: upstream-comet-expression
description: Use when a Comet-native Spark expression's semantics could instead be implemented once in the upstream `datafusion-spark` crate. Implements the expression in apache/datafusion, following upstream idioms. If the upstream function ends up with a real `invoke_with_args`, Comet can later drop its `native/spark-expr/` port and consume it via `wire-datafusion-function`; if it is instead a `simplify()`-only lowering, Comet keeps its own native copy. This skill covers only the upstream contribution.
argument-hint: <expression-name>
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

Upstream a native implementation of the `$ARGUMENTS` Spark expression from Comet's `native/spark-expr/` into the `datafusion-spark` crate in `apache/datafusion`. **This works in the `apache/datafusion` repo, not `datafusion-comet`.** Treat it as a normal external contribution to that project. Re-wiring Comet to consume the upstream function afterward is a separate, deferred step, and it only applies to some functions (see step 2 and the end of this skill).

## 1. Confirm it is a good upstream candidate

Don't duplicate work. Check the function is genuinely missing and not already in flight:

```bash
# Absent from datafusion-spark / datafusion-functions?
grep -rin "$ARGUMENTS" ~/git/apache/datafusion/datafusion/ ; echo "exit=$?"

# Not already scoped or claimed on the tracking epic?
gh issue view 15914 --repo apache/datafusion --json title,body | grep -i "$ARGUMENTS"

# No open or merged PR already doing this?
gh pr list --repo apache/datafusion --search "$ARGUMENTS in:title" --state all
```

`exit=1` on the grep and empty output on both `gh` commands means it's clear to proceed. A hit on any of them means stop and report back rather than duplicating effort: the function may already exist under a different search term, or someone else may be mid-PR.

## 2. Study both sides; adopt upstream idioms, not Comet's port

Read Comet's existing native implementation (`native/spark-expr/src/`) for the **semantics** only (ANSI branches, null handling, overflow behavior). Do not copy its Rust structure. It was written against an older `ScalarUDFImpl` shape and will not match what current `apache/datafusion` expects or reviewers will ask for.

Read the **current** analogous function in the target repo revision (see step 3 for how to locate the working copy) before writing anything:

```bash
grep -rln "fn name" ~/git/apache/datafusion/datafusion/spark/src/function/<category>/
```

Concrete idiom differences to watch for, in order of likelihood to bite:

- **`ScalarUDFImpl` trait shape drifts.** E.g. `as_any` was removed from the trait in datafusion#20812. Older code (including Comet's own historical ports and stale sample snippets) still implements it; current code must not, and doesn't need the `use std::any::Any;` import either. Always match what the sibling function in the _current_ checkout does, not what a search result or memory suggests.
- **Decide up front: `simplify()` or `invoke_with_args`. Implement exactly one, never both.** See "Choosing the implementation route" below; this decision drives the rest of the work, including whether step 8's Comet re-wire is even possible.
- **Signature choice is per-input-type, not one-size-fits-all.** Native-typed inputs (e.g. `Int32`) use `Signature::exact(vec![DataType::Int32], ...)`. Date-class inputs use `Signature::coercible` with `TypeSignatureClass::Native(logical_date())`, as `unix_date` does. Pick by matching a same-category sibling with the same input type, not by habit.
- **Return type: use `return_field_from_args` when nullability or the return type depends on the arguments.** A fixed return type can stay in `return_type`, but functions whose output nullability tracks the input, or whose type depends on argument types, implement `return_field_from_args` returning a `FieldRef`, as `unix.rs` does. Match the sibling that has the same shape.
- Use `datafusion_common` / `datafusion_expr` crate paths directly (this crate does not re-export through a Comet-style shim layer).

`datetime/unix.rs` (home of `SparkUnixDate`) is a good template function to read end-to-end before writing the new one, since several recent Spark date/time additions follow its shape.

### Choosing the implementation route

Ask one question: **can the expression's semantics be expressed as a rewrite over exprs DataFusion already has?** A pure cast, an arithmetic rearrangement, or a composition of existing functions all qualify. Anything needing genuine per-value work (custom parsing, Spark-specific overflow or rounding behavior, a kernel with no expr-level equivalent) does not.

**If yes: implement `simplify()` only.** Leave `invoke_with_args` as an `internal_err!("invoke_with_args should not be called on <Struct>")` stub, exactly as `SparkUnixDate` does. Do not carry a working `invoke_with_args` alongside it. Upstream reviewers do not want two implementations of the same semantics to keep in sync, and a physical implementation that the optimizer always rewrites away is dead code from DataFusion's point of view. Prefer matching upstream's idiom over engineering around it.

**If no: implement the real logic in `invoke_with_args`,** and do not add a `simplify()`.

**What this means for Comet.** Comet builds physical `ScalarFunctionExpr` nodes directly from protobuf and calls `invoke_with_args`; it never runs the logical `SimplifyExpressions` pass. So a `simplify()`-only function is unusable from Comet and would error at runtime the moment Comet invoked it. **That is an accepted outcome, not a problem to solve.** Comet keeps its own native implementation in `native/spark-expr/` for such expressions, and the step 8 re-wire simply does not apply. The upstream contribution still stands on its own: DataFusion gains a Spark-compatible function.

Say which route you took, and the consequence for Comet, when you report back at step 7. Only the `invoke_with_args` route ever leads to Comet dropping its port.

## 3. Locate the working copy

Use the user's existing local clone if one is configured (check project memory / `CLAUDE.md` for a path); otherwise clone:

```bash
DF_CLONE=~/git/apache/datafusion   # adjust if memory points elsewhere
cd "$DF_CLONE" && git remote -v    # expect apache + origin
git fetch apache main
git switch -c $ARGUMENTS apache/main
```

Confirm the baseline builds before changing anything:

```bash
cd "$DF_CLONE" && cargo build -p datafusion-spark --features core
```

Note: `cargo build -p datafusion-spark` **without** `--features core` can fail on an unrelated pre-existing gap (an unconditional `datafusion::` import in a file that should be feature-gated). That failure is not yours to fix; always build with `--features core`.

## 4. Implement upstream

In `datafusion/spark/src/function/<category>/<name>.rs`, add the `ScalarUDFImpl` following the idioms from step 2.

Registration has **four** touch points in `datafusion/spark/src/function/<category>/mod.rs`, all alphabetically placed among their neighbors:

1. `pub mod <name>;`
2. `make_udf_function!(<mod>::<Struct>, <name>);`
3. an `export_functions!((<name>, "<doc string>", <args>))` entry
4. `<name>()` added to the `functions()` vec

## 5. Test

Add `datafusion/sqllogictest/test_files/spark/<category>/<name>.slt`, following the Testing Guide in `datafusion/sqllogictest/test_files/spark/README.md`:

- Cover both scalar literals (with explicit input casts, e.g. `0::INT`, not bare untyped literals) and a multi-row array/column case.
- Include a NULL case.
- Wrap in ANSI-mode variants only if behavior actually differs under ANSI; don't add ANSI wrapping reflexively.
- Use `query D` for a `Date32` result column (match the result-type letter to the function's return type).

Because exactly one of `simplify()` and `invoke_with_args` is implemented (step 2), the SLT covers the live path either way and no supplementary Rust unit test is needed for coverage's sake. Add `#[cfg(test)]` tests only where they earn their place, e.g. an edge case that is awkward to express in SQL.

Run:

```bash
cd "$DF_CLONE" && cargo test --test sqllogictests -p datafusion-sqllogictest -- $ARGUMENTS
```

Confirm RED first (query fails with "Invalid function" before the mod.rs registration lands), then GREEN after wiring it up. Regenerate expected output after intentional changes with:

```bash
cargo test --test sqllogictests -p datafusion-sqllogictest -- $ARGUMENTS --complete
```

## 6. Pre-PR checks

```bash
cd "$DF_CLONE"
cargo fmt -p datafusion-spark
cargo clippy -p datafusion-spark --features core -- -D warnings
```

Plain `cargo clippy -p datafusion-spark` without `--features core` can fail to even reach this crate due to the same feature-gate gap noted in step 3, so always pass `--features core`. Pre-existing unrelated lints elsewhere in the workspace (e.g. in `physical-expr`) are not this contribution's concern; confirm by checking `git status --short` shows only your new/changed files before attributing a clippy failure to your change. The sqllogictest run from step 5 is the real correctness gate, not clippy.

## 7. Open the PR: confirm with the user first

This step files a PR against a third-party project (`apache/datafusion`), not this repo. **Stop and get explicit confirmation from the user before opening it.**

Once confirmed, push and open the PR using `gh`, following DataFusion's own PR conventions (not Comet's template): reference the datafusion-spark tracking epic [apache/datafusion#15914](https://github.com/apache/datafusion/issues/15914), and use DataFusion's standard PR sections (`Which issue does this PR close?`, `Rationale for this change`, `What changes are included in this PR?`, `Are these changes tested?`, `Are there any user-facing changes?`).

## 8. Deferred: re-wire Comet (only for the `invoke_with_args` route)

**If the upstream function is `simplify()`-only, there is nothing to defer.** Comet cannot call it, so Comet keeps its `native/spark-expr/` implementation and the two live on side by side. Say so explicitly when reporting back, so nobody later files a follow-up to delete a port that is still load-bearing.

Otherwise: once `$ARGUMENTS` ships in a `datafusion-spark` release that Comet's `native/Cargo.toml` bumps to, use the `wire-datafusion-function` skill to replace Comet's native port with the upstream function and remove the now-redundant implementation from `native/spark-expr/`. Do not attempt that re-wire as part of this skill; it depends on a released version Comet doesn't have yet.
