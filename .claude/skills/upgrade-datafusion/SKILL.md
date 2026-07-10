---
name: upgrade-datafusion
description: Create or refresh a WIP branch that upgrades Comet to the latest DataFusion `main` (plus matching arrow/parquet/object_store versions). Use when the user asks to open a forward-looking DataFusion upgrade PR, or to rebase/refresh an existing one. Consults the DataFusion upgrade guides to explain and fix breaking changes surfaced by the build.
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

Open (or refresh) a forward-looking WIP branch that upgrades Comet to the current tip of `apache/datafusion` `main`. The goal is to run CI continuously against the latest DataFusion so that breaking changes are surfaced early and fixed incrementally, not in a single release-blocking crunch.

The skill covers two entry points:

- **Create** a new upgrade branch when none exists (or the previous one has already been merged / abandoned).
- **Refresh** an existing WIP upgrade branch to a newer DataFusion rev.

Do not merge these branches. Their purpose is signal. When DataFusion cuts a release, a separate `deps: Upgrade to DataFusion NN.0.0` PR is opened against a pinned crates.io version; that PR is out of scope for this skill.

---

## Step 1: Detect existing WIP upgrade branch

Before creating a new branch, check whether one already exists:

```bash
gh pr list --repo apache/datafusion-comet --state open \
  --search "upgrade datafusion in:title" \
  --json number,title,headRefName,author,updatedAt \
  | jq '.[] | select(.title | test("(?i)wip.*(datafusion|df).*(main|upgrade)"))'
```

If a matching draft PR exists:

- Confirm with the user whether to **refresh that PR** (skip to Step 6) or open a fresh one alongside it. Do not silently close someone else's branch.
- If refreshing, `git fetch` the PR's remote and `git checkout` its head branch (`gh pr checkout <N>`).

If nothing matches, proceed to Step 2 to create a new branch.

---

## Step 2: Determine the target versions

Fetch the current tip of `apache/datafusion` `main`:

```bash
gh api repos/apache/datafusion/commits/main \
  --jq '{sha: .sha, date: .commit.author.date, msg: (.commit.message | split("\n")[0])}'
```

Then read DataFusion's root `Cargo.toml` at that rev to learn the pinned versions of `arrow`, `parquet`, and `object_store`:

```bash
gh api repos/apache/datafusion/contents/Cargo.toml \
  | jq -r '.content' | base64 -d \
  | grep -E "^(arrow|parquet|object_store) " | head
```

Record:

- `DF_REV` — the 40-char SHA.
- `DF_DATE` — the commit date (used in the PR body).
- `ARROW_VERSION`, `PARQUET_VERSION`, `OBJECT_STORE_VERSION` — from DataFusion's `Cargo.toml`.

Compare against Comet's current pins in `native/Cargo.toml` (`[workspace.dependencies]`). Only bump a dependency if DataFusion's version differs. In particular:

- `arrow` and `parquet` almost always bump when DataFusion cuts a new minor.
- `object_store` moves less frequently — check before bumping.
- `sqlparser`, `hashbrown`, `rand` — Comet does not pin these at workspace scope today. Only bump if you find a transitive incompatibility.

---

## Step 3: Read the relevant DataFusion upgrade guides

DataFusion publishes per-release upgrade guides that document every breaking API change with before/after code snippets and migration advice. Read them **before** editing Comet's Cargo.toml so you know what to expect.

List the guides:

```bash
gh api repos/apache/datafusion/contents/docs/source/library-user-guide/upgrading \
  --jq '.[].name' | sort -V
```

Read every guide with a version number strictly greater than the version Comet is currently on. For example, if Comet is on DataFusion `54.0.0` and main is heading toward `55.0.0`, read `55.0.0.md`. The unreleased guide (the highest-numbered file) is the most important — it is the running changelog of breaking changes on `main` that have not yet been released.

```bash
gh api repos/apache/datafusion/contents/docs/source/library-user-guide/upgrading/55.0.0.md \
  --jq '.content' | base64 -d
```

For each documented breaking change, note whether Comet uses the affected API. `grep` from `native/` is the fastest way to check:

```bash
grep -rn "fill_null\|SessionContext::" native/ | head
```

Keep a short internal map of `{API change → Comet call sites}`. This map drives Step 5 (fixing breakage).

Also cross-reference the DataFusion `CHANGELOG.md` on `main` for anything the upgrade guide missed:

```bash
gh api repos/apache/datafusion/contents/dev/changelog/55.0.0.md \
  --jq '.content' | base64 -d | head -200
```

---

## Step 4: Create the branch and update Cargo.toml

Branch naming: `wip/upgrade-datafusion-main`. Reuse the same name across cycles — the previous PR having been closed does not require a fresh name.

```bash
git fetch apache main
git checkout apache/main
git checkout -b wip/upgrade-datafusion-main
```

Update `native/Cargo.toml` `[workspace.dependencies]`:

```toml
arrow = { version = "<ARROW_VERSION>", features = ["prettyprint", "ffi", "chrono-tz"] }
parquet = { version = "<PARQUET_VERSION>", default-features = false, features = ["experimental"] }
datafusion = { git = "https://github.com/apache/datafusion", rev = "<DF_REV>", default-features = false, features = ["unicode_expressions", "crypto_expressions", "nested_expressions", "parquet"] }
datafusion-datasource = { git = "https://github.com/apache/datafusion", rev = "<DF_REV>" }
datafusion-physical-expr-adapter = { git = "https://github.com/apache/datafusion", rev = "<DF_REV>" }
datafusion-spark = { git = "https://github.com/apache/datafusion", rev = "<DF_REV>", features = ["core"] }
```

If `object_store` needs to bump, update it here too. Preserve every existing feature flag — losing one silently disables a code path.

Then grep the tree for any DataFusion crate references that are pinned outside `[workspace.dependencies]` and switch them to the same git rev:

```bash
grep -rn "= \"5[0-9]\.[0-9]\.[0-9]\"\|datafusion.*version" native/*/Cargo.toml
```

Common offenders:

- `native/core/Cargo.toml` `[dev-dependencies]` — historically pins `datafusion-functions-nested` directly.
- Any `optional = true` DataFusion sub-crate.

Every DataFusion sub-crate on the branch must resolve to the **same git rev**, or Cargo will fail with duplicate crate errors.

---

## Step 5: Compile, triage, fix

Run:

```bash
cd native && cargo check --workspace 2>&1 | tee /tmp/comet-upgrade-check.log
```

Sort errors by unique kind:

```bash
grep -E "^error" /tmp/comet-upgrade-check.log | sort -u
```

For each unique error:

1. Find the DataFusion PR that introduced the change (`git log` on the DataFusion checkout Cargo pulled into `~/.cargo/git/checkouts/datafusion-*/<rev-prefix>/`).
2. Cross-reference the upgrade guide from Step 3 for the recommended migration.
3. Apply the fix at every Comet call site. Prefer minimal, mechanical edits — this branch is about surfacing breakage, not refactoring.
4. Do **not** add compatibility shims (`#[cfg]`-gated dual paths, feature-gated re-exports, etc.). Comet only supports one DataFusion version at a time.
5. Re-run `cargo check` and repeat.

Common upstream breakages Comet has hit before (non-exhaustive):

- `GroupsAccumulator::merge_batch` signature changes (parameter added or removed).
- `SessionContext` / `SessionState` builder API renames.
- `ScalarValue` and `ScalarUDF` accessor renames.
- `PhysicalExpr` object-safety changes (`Arc<dyn PhysicalExpr>` vs `&dyn PhysicalExpr`).
- Renames in the `arrow::compute::kernels::*` modules on every arrow minor.
- `MutableArrayData::extend` → `try_extend` (arrow started marking the panicking variants deprecated around arrow 59).

When you have compile errors that are **not** covered by an upgrade guide, that is worth calling out in the PR body under a "Regressions / undocumented" heading so the DataFusion maintainers can add a guide entry.

Do not chase deprecation warnings on this branch unless leaving them causes `cargo test` failures. Deprecations can be swept in a follow-up.

---

## Step 6: Refresh an existing WIP branch

If the branch already exists (from Step 1) you are just bumping `DF_REV` to a newer SHA.

```bash
gh pr checkout <PR_NUMBER>
git fetch apache main
git rebase apache/main   # resolve conflicts against native/Cargo.toml if needed
```

Then edit `native/Cargo.toml` and any sub-crate manifests to swap the old rev for the new one. `sed` works well for the mechanical bit:

```bash
sed -i '' "s/rev = \"<OLD_REV>\"/rev = \"<NEW_REV>\"/g" \
  native/Cargo.toml native/core/Cargo.toml
```

(Use `sed -i` without `''` on Linux.)

Re-run Step 5. The set of compile errors will typically be a superset of what was already fixed — the fixes from the previous rev stay, and new errors reflect what DataFusion changed in the interval.

---

## Step 7: Commit and push

One commit per refresh cycle is fine — the PR is WIP and will be squashed away if it ever lands. Format:

```
chore: WIP upgrade to DataFusion main (<SHORT_REV>) + arrow/parquet <ARROW_VERSION>
```

Push to the contributor's own fork, not `apache/`:

```bash
git push -u origin wip/upgrade-datafusion-main
```

If the branch already existed and you rebased, use `--force-with-lease` (never `--force`):

```bash
git push --force-with-lease origin wip/upgrade-datafusion-main
```

---

## Step 8: Open (or update) the draft PR

For a new PR, open it against `apache/datafusion-comet` `main` as a **draft**. Body template:

```markdown
## Summary

WIP draft tracking the upgrade to DataFusion `main` (pinned to `<DF_REV>`, dated `<DF_DATE>`).

Not for merge — opened as a draft so CI runs against the latest DataFusion continuously and breaking changes surface early.

## Dependency bumps

- `datafusion*` → git rev `<SHORT_REV>`
- `arrow` `<OLD_ARROW>` → `<ARROW_VERSION>`
- `parquet` `<OLD_PARQUET>` → `<PARQUET_VERSION>`
- `object_store` — (unchanged / bumped to `<OBJECT_STORE_VERSION>`)

## Upstream changes addressed

Reference each DataFusion upgrade guide entry that required a Comet edit, with a link to the guide section and the Comet files touched:

- **`GroupsAccumulator::merge_batch` signature change** — [DF upgrade guide 55.0.0](https://github.com/apache/datafusion/blob/main/docs/source/library-user-guide/upgrading/55.0.0.md) — updated `spark-expr/src/agg_funcs/{avg,stddev,...}.rs`.

## Regressions / undocumented

Anything not covered by an upgrade guide. Filing these as follow-up issues on `apache/datafusion` helps the whole ecosystem.

## Test plan

- [ ] `cd native && cargo check --workspace` compiles clean
- [ ] `make test-rust` passes
- [ ] `make test-jvm` passes on default profile
- [ ] Address any additional breakage that CI surfaces
```

For a **refresh** of an existing PR, don't recreate the PR — just update the body via `gh pr edit <N> --body-file <file>` (or append a comment describing the new rev if the body is stable). Keep the PR in draft state.

Command:

```bash
gh pr create --repo apache/datafusion-comet --draft \
  --title "WIP: upgrade to DataFusion main + arrow/parquet <ARROW_VERSION>" \
  --head <fork-owner>:wip/upgrade-datafusion-main \
  --base main \
  --body-file /tmp/comet-df-upgrade-body.md
```

Return the PR URL to the user.

---

## What NOT to do

- **Do not** mark the PR ready for review. It exists to run CI, not to land.
- **Do not** merge unrelated fixes (deprecation warnings, cosmetic refactors) into this branch. Every extra change makes the diff harder to rebase across DataFusion revisions. Address deprecations in separate follow-up PRs against `main`.
- **Do not** downgrade a feature flag to make the build pass. If a DataFusion feature was removed, that is signal — record it in the PR body.
- **Do not** push to `apache/`. Contributors push to their own fork; the PR comes from the fork.
- **Do not** touch `Cargo.lock` by hand — `cargo check` regenerates it. Commit the regenerated lockfile alongside the Cargo.toml changes.
