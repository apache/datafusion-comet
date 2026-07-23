---
name: upstream-comet-expression
description: Use when a Comet-native Spark expression's semantics could instead be implemented once in the upstream `datafusion-spark` crate. Implements the expression in apache/datafusion so Comet can later drop its `native/spark-expr/` port and consume the upstream function via `wire-datafusion-function`. This skill covers the upstream contribution; the Comet re-wire is a separate, deferred step.
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

Upstream a native implementation of the `$ARGUMENTS` Spark expression from Comet's `native/spark-expr/` into the `datafusion-spark` crate in `apache/datafusion`. **This works in the `apache/datafusion` repo, not `datafusion-comet`.** Treat it as a normal external contribution to that project. Re-wiring Comet to consume the upstream function afterward is a separate, deferred step (see the end of this skill).

## Pilot reference

Worked example: `date_from_unix_date`, upstreamed as [apache/datafusion#23852](https://github.com/apache/datafusion/pull/23852). Its real logic (an `Int32 -> Date32` reinterpret) lives in `invoke_with_args` so Comet can consume it; a `simplify()`-to-cast is layered on top only as an optional DataFusion optimization. See step 2 for why `invoke_with_args` cannot be a stub.

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

Concrete idiom differences the pilot surfaced, in order of likelihood to bite:

- **`ScalarUDFImpl` trait shape drifts.** E.g. `as_any` was removed from the trait in datafusion#20812. Older code (including Comet's own historical ports and stale sample snippets) still implements it; current code must not, and doesn't need the `use std::any::Any;` import either. Always match what the sibling function in the *current* checkout does, not what a search result or memory suggests.
- **The real implementation MUST live in `invoke_with_args`. `simplify()` is an optional optimization, never a substitute.** This is the single most important lesson from the pilot, because it is a trap. Some upstream functions (e.g. `unix.rs`'s `SparkUnixDate`) implement only `simplify()` to lower to casts and leave `invoke_with_args` as an `internal_err!(...)` stub. That works for pure DataFusion SQL, where the logical `SimplifyExpressions` rule rewrites the call before physical planning so `invoke_with_args` is never reached. **It does not work for Comet.** Comet builds physical `ScalarFunctionExpr` nodes directly from protobuf and calls `invoke_with_args`; it never runs the logical simplify pass. A `simplify()`-only function therefore errors at runtime the moment Comet invokes it, which defeats the entire purpose of upstreaming (step 8's re-wire). So: put the actual per-value logic (e.g. the `Int32 -> Date32` array/scalar reinterpret, since both are physically `i32`) in `invoke_with_args`. You MAY additionally implement `simplify()` to give DataFusion's optimizer a cast to fold, but only as a layer on top of a working `invoke_with_args`. If a reviewer objects to carrying both, drop `simplify()` and keep the real `invoke_with_args`, never the reverse.
- **Signature choice is per-input-type, not one-size-fits-all.** Native-typed inputs (e.g. `Int32`) use `Signature::exact(vec![DataType::Int32], ...)`. Date-class inputs use `Signature::coercible` with `TypeSignatureClass::Native(logical_date())`, as `unix_date` does. Pick by matching a same-category sibling with the same input type, not by habit.
- **Return type: use `return_field_from_args` when nullability or the return type depends on the arguments.** A fixed return type can stay in `return_type` (as the pilot's `Date32` does), but functions whose output nullability tracks the input, or whose type depends on argument types, implement `return_field_from_args` returning a `FieldRef`, as `unix.rs` does. Match the sibling that has the same shape.
- Use `datafusion_common` / `datafusion_expr` crate paths directly (this crate does not re-export through a Comet-style shim layer).

`datetime/unix.rs` (home of `SparkUnixDate`) is a good template function to read end-to-end before writing the new one, since several recent Spark date/time additions follow its shape.

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

The sqllogictest suite runs through DataFusion's SQL optimizer, so if you also implemented `simplify()` (step 2) it shadows `invoke_with_args` and the SLT never exercises the physical path Comet uses. Add a Rust `#[cfg(test)]` unit test in the function's module that calls `invoke_with_args` directly on both an array and a scalar, so that path is actually covered.

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

## 8. Deferred: re-wire Comet (not part of this skill)

Once `$ARGUMENTS` ships in a `datafusion-spark` release that Comet's `native/Cargo.toml` bumps to, use the `wire-datafusion-function` skill to replace Comet's native port with the upstream function and remove the now-redundant implementation from `native/spark-expr/`. Do not attempt that re-wire as part of this skill; it depends on a released version Comet doesn't have yet.
