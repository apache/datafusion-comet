---
name: audit-expression-page
description: Use when verifying that the Comet user-guide expression support page is accurate and current, for example during release preparation or after adding or changing expression serdes. Sweeps the whole page rather than auditing a single expression.
argument-hint: [category]
---

# Audit the Expression Support Page

Audit `docs/source/user-guide/latest/expressions.md` for accuracy against the registered Comet expression serdes. This is a whole-page sweep: it checks that every registered expression is listed, that each listed expression's status is correct, and that no stale rows remain. For a deep correctness and test-coverage audit of one expression, use `audit-comet-expression` instead.

Optional argument: a single registry category to scope the audit (for example `array_funcs`, `agg_funcs`, `math_funcs`). With no argument, audit every category.

## What this page is

`expressions.md` is the source of truth for which Spark expressions Comet supports and at what status. It has one table per Spark function-registry category, keyed by SQL function name, with a Status column and a Notes column. A legend near the top defines the statuses.

Read the legend from the page at the start of every run and treat it, not this skill, as the authority on what each status means. At the time of writing the legend is:

- ✅ Supported: Comet produces Spark-compatible results by default. Some inputs or forms may fall back to Spark, and any incompatible behavior is opt-in (off by default).
- ⚠️ Incorrect by default: Comet runs natively by default but can return results that differ from Spark (a wrong value, or a native error on valid input).
- 🔜 Planned: intended, tracked by an open issue or pull request.
- 💤 Not currently planned.

## Where registration lives

The authoritative list of registered expressions is in `spark/src/main/scala/org/apache/comet/serde/QueryPlanSerde.scala`:

- The per-category maps (`arrayExpressions`, `mathExpressions`, `temporalExpressions`, `stringExpressions`, and so on) that combine into `exprSerdeMap`.
- `aggrSerdeMap` for aggregate expressions.

`spark/src/main/scala/org/apache/comet/GenerateDocs.scala` has a `categoryPages` map that groups these serde maps by documentation category. Use it to see which serde maps back a category, and to stay correct as the maps change. Its keys are documentation-page names (`aggregate`, `array`, `math`, and so on), not the `agg_funcs` / `array_funcs` header names the page uses, so map between them by meaning: `agg_funcs` is `aggrSerdeMap`, `array_funcs` is `arrayExpressions`, and so on.

Each map is keyed by Spark `Expression` class. Resolve a class to the SQL name(s) shown on the page via Spark's function registry, which lives in Spark source (`FunctionRegistry.scala`), not in this repository. Obvious cases come from the class name (`ArrayExcept` is `array_except`). For aliases and rewrites the name is not in the Comet serde (for example `bool_and` and `bool_or` are Spark rewrites to `Min` / `Max` over a boolean, and `count_if` rewrites to `Count`); resolve those from Spark's registry or the expression's `RuntimeReplaceable` definition.

Some serdes are injected at the operator level rather than called as SQL functions (for example `BloomFilterAggregate`). The page lists these in its operator-injected note rather than as a table row. Do not flag an operator-injected serde as missing coverage; check it against that note instead.

A few expressions are wired in the version shims via `versionSpecificExprToProtoInternal` rather than through a serde map (for example `WidthBucket`). Treat these as registered. Their effective status is ✅ unless the shim itself contains a guard that falls back.

## The three checks

### 1. Missing coverage

For each registered serde in scope, resolve its SQL name(s) and confirm the page lists each one. Search the whole page, not only the matching category section: Spark groups some functions under a different category than the serde map (for example `isnan` is in `mathExpressions` but appears under `predicate_funcs`). Report any registered expression that has no row anywhere.

### 2. Status accuracy

For each row in scope, determine the status the code implies and compare it to the documented status.

Runtime behavior is what decides the status. An expression falls back to Spark only when `getSupportLevel` returns `Unsupported`, or returns `Incompatible` while its `allowIncompatible` config is false (the default), or when a guard in `convert` returns `None`. Read those: `getSupportLevel` (including any data-type or eval-mode checks), the `None`-returning branches in `convert`, and the default of any `allowIncompatible` config.

`getIncompatibleReasons` and `getCompatibleNotes` only generate text for the compatibility guide. They do not by themselves cause fallback, so do not classify from them alone. An aggregate serde in particular may return a reason from `getIncompatibleReasons` while `getSupportLevel` stays `Compatible`, meaning it runs natively. When the two disagree, that is itself a finding worth reporting.

Apply the legend rule:

- **✅** when the expression is Spark-compatible by default: unsupported types or forms fall back (via `Unsupported` or a `None` branch in `convert`); an `Incompatible` path is gated behind an `allowIncompatible` config that defaults to false; or the only divergence is acceptable (NaN, signed-zero, or element ordering).
- **⚠️** when the expression is incorrect by default: it runs natively by default (`getSupportLevel` is `Compatible` and `convert` does not guard the case) and can return a value differing from Spark, or throws a native error on input that Spark accepts, with no opt-in.

Report each row whose documented status differs from the classified status, naming the method or branch that drove the decision.

### 3. Stale entries

Report any row marked ✅ or ⚠️ whose name does not resolve to a registered serde (serde removed or renamed), and any row whose documented status contradicts registration (for example 🔜 on an expression that is registered).

## Output

Produce a report grouped by category with three sections: Missing coverage, Wrong status, and Stale entries. For each finding give the expression name, the documented value, the expected value, and the code evidence (file and method or branch).

Then offer to apply the corrections to `expressions.md`: add missing rows, correct statuses, and remove or fix stale rows. Keep Notes short and link to the compatibility or audit guide following the page's existing convention. Do not edit the page until the offer is accepted.

## Scope and cost

Status accuracy reads serde source for every expression in scope, so a full run is large. Prefer the `[category]` argument to audit one category at a time, or dispatch one reader per category in parallel and synthesize. Note in the report any category you did not cover.

## Notes

- Do not change serde or Rust code. This skill only audits and edits the documentation page.
- Follow the project's prose conventions: no em dashes or semicolons.
