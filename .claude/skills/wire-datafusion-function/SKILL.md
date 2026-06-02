---
name: wire-datafusion-function
description: Use when wiring an existing DataFusion or datafusion-spark function into Comet for a Spark expression. Identifies the right wiring pattern (one-line passthrough, datafusion-spark UDF registration, or custom serde with input massaging / restrictions), applies the Scala serde, registers the UDF in jni_api when needed, and adds SQL file tests. Assumes the function already exists upstream — if not, switch to `implement-comet-expression`.
argument-hint: <expression-name>
---

Wire Comet support for the `$ARGUMENTS` Spark expression by reusing an existing DataFusion or `datafusion-spark` function. **No native Rust is written here** — if upstream coverage is missing, stop and run `implement-comet-expression`.

## Wiring patterns

Pick the lightest one that satisfies the Spark contract.

| Pattern                        | When                                                                                                                                                                                                                                                                     | What to change                                                                                                                                                                   |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **A** — passthrough            | DF built-in (e.g. `acos`), or `datafusion-spark` UDF already registered in `register_datafusion_spark_function`                                                                                                                                                          | one line in the right map of `QueryPlanSerde.scala`: `classOf[Foo] -> CometScalarFunction("foo")`                                                                                |
| **B** — register + passthrough | `datafusion-spark` UDF, _not_ yet registered, semantics already match Spark                                                                                                                                                                                              | Pattern A line **plus** `session_ctx.register_udf(ScalarUDF::new_from_impl(SparkFoo::default()));` in `native/core/src/execution/jni_api.rs::register_datafusion_spark_function` |
| **C** — custom serde           | Inputs need preprocessing (cast, `nullIfNegative`, `+0.0` flip), or you need to set return type / `failOnError`, restrict input types via `getSupportLevel`, enforce foldable-only args, or attach `getCompatibleNotes`/`getIncompatibleReasons`/`getUnsupportedReasons` | new `CometXxx` object in the topic file (`math.scala`, `strings.scala`, …); see `CometCeil`, `CometAtan2`, `CometLog`, `CometSha2`, `CometAbs`                                   |

Function names must match: the string passed to `CometScalarFunction("xyz")` equals `SparkXyz::name()` upstream.

## Workflow

### 1. Study the Spark contract

Find the `case class $ARGUMENTS` (PascalCase). Prefer the user's local Spark clone (`CLAUDE.md` / project memory); fall back to `git clone --depth 1 https://github.com/apache/spark.git /tmp/spark-master`. If the clone is sandbox-blocked and no local clone is found, **stop and ask** — Spark is the ground truth.

Note `inputTypes`, `dataType`, `eval` / `nullSafeEval`, `require` guards, ANSI mode branches, and foldable-only arguments. These define what Comet must reproduce.

### 2. Find the upstream function (at the pinned version)

The pinned versions in `native/Cargo.toml` are the ground truth — a function on upstream `main` may not exist in the version Comet depends on. Resolve versions first, then search sources matching those exact versions. **Pick one source path; do not mix.**

```bash
REPO_ROOT=$(git rev-parse --show-toplevel)
# Portable regex — BSD awk on macOS does not support \b.
DF_SPARK_VER=$(awk -F'"' '/^datafusion-spark[ =]/ {print $2; exit}' "$REPO_ROOT/native/Cargo.toml")
DF_FUNCS_VER=$(awk -F'"' '/^datafusion[ =]/        {print $2; exit}' "$REPO_ROOT/native/Cargo.toml")
[ -z "$DF_SPARK_VER" ] || [ -z "$DF_FUNCS_VER" ] && { echo "ERROR: version extraction failed"; exit 1; }
EXPR='$ARGUMENTS'
```

**Option A — cargo registry (preferred):**

```bash
DF_SPARK=$(ls -d ~/.cargo/registry/src/*/datafusion-spark-${DF_SPARK_VER}/ 2>/dev/null | head -1)
DF_FUNCS=$(ls -d ~/.cargo/registry/src/*/datafusion-functions-${DF_FUNCS_VER}/ 2>/dev/null | head -1)
# If empty, run `cargo fetch` from native/.
grep -rin "fn name" "$DF_SPARK/src/function/" 2>/dev/null | grep -i "$EXPR"
grep -rin "fn name" "$DF_FUNCS/src/"          2>/dev/null | grep -i "$EXPR"
```

**Option B — local DataFusion clone** (only if `CLAUDE.md` / memory points at one). Grep at the pinned tag — DataFusion uses lightweight tags (`<version>`, no `v` prefix), so use `tag -l` not `rev-parse --verify ^{tag}`:

```bash
DF_CLONE=<path from memory>
[ -z "$(git -C "$DF_CLONE" tag -l "$DF_SPARK_VER")" ] && echo "WARNING: tag missing — git fetch --tags or use Option A"
git -C "$DF_CLONE" grep -in "fn name" "$DF_SPARK_VER" -- 'datafusion/spark/src/function/' | grep -i "$EXPR"
git -C "$DF_CLONE" grep -in "fn name" "$DF_FUNCS_VER" -- 'datafusion/functions/src/'      | grep -i "$EXPR"
```

Never grep the clone's working tree directly — it may be on `main`.

### 2a. Decision gate — confirm the source crate

- **Found in `datafusion-spark`** → proceed (Spark-tuned by design). If listed in `register_datafusion_spark_function` → Pattern A; otherwise Pattern B.
- **Found only in `datafusion-functions`** → **STOP and `AskUserQuestion`**. Pure DataFusion follows standard SQL semantics and frequently diverges from Spark (NULL vs error, negatives, overflow, return type, NaN). Surface what you found in each crate and the divergences you've already identified, then offer:
  1. Wire from `datafusion-functions` with Pattern C bridging — list the divergences and the masking/casting that closes them.
  2. Stop and switch to `implement-comet-expression` to port to `datafusion-spark` upstream first.
- **Found in neither** → stop and run `implement-comet-expression`.

If semantics diverge in a way Pattern C can't bridge, escalate.

### 3. Apply the Scala wiring

Add the entry to the matching map in `spark/src/main/scala/org/apache/comet/serde/QueryPlanSerde.scala`: `mathExpressions`, `stringExpressions`, `arrayExpressions`, `mapExpressions`, `structExpressions`, `predicateExpressions`, `conditionalExpressions`, `bitwiseExpressions`, `temporalExpressions`, `hashExpressions`, `conversionExpressions`, `miscExpressions`.

Spark expression classes are in scope via `import org.apache.spark.sql.catalyst.expressions._`. For Pattern C, the topic file uses **explicit** imports — extend the existing `import …expressions.{…}` line and place the new object alongside its peers.

Helpers from `QueryPlanSerde`:

- `scalarFunctionExprToProto(name, args*)` — return type inferred, `failOnError = false`.
- `scalarFunctionExprToProtoWithReturnType(name, returnType, failOnError, args*)` — explicit type / fail-on-error.
- `optExprWithInfo(optExpr, expr, children*)` — wrap final result; propagates "why we couldn't convert" tags.
- `withInfo(expr, "reason")` — tag a fallback when returning `None`.

`getSupportLevel` returning `Incompatible(Some("…"))` gates behind `spark.comet.expr.<name>.allowIncompatible=true`. `Unsupported(…)` always falls back.

### 4. Register the UDF (Pattern B only)

In `native/core/src/execution/jni_api.rs::register_datafusion_spark_function`, add the `use` and the `register_udf` line, grouped with neighbors. Patterns A and C never touch native code.

### 5. Add SQL file tests

Create `spark/src/test/resources/sql-tests/expressions/<category>/$ARGUMENTS.sql`. Mandatory rules:

1. **Test only the directly supported input types** — read `inputTypes` from the Spark source. Do **not** add columns for implicit-cast widening (`ImplicitCastInputTypes`); the cast path has its own tests. E.g. `inputTypes = Seq(IntegerType)` (`Factorial`) → one `int` column, not byte/short/long/float/double/decimal. For `TypeCollection(A, B)`, add one column per member.
2. **All `SELECT`s read from a parquet table** — no literal-only queries (the constant folder skips the read path and column-vector kernel).
3. **Mix `NULL` into every column** — exercises the validity bitmap.
4. **Per type, cover the edge-case set:**
   - Float / Double: `NaN`, `+0.0`, `-0.0`, `±Infinity` via `cast('NaN' as double)` etc.
   - Integer / decimal: each type's max and min (`127`/`-128`, `2147483647`/`-2147483648`, `9223372036854775807`/`-9223372036854775808`, `Decimal(38, 0)` boundary) for overflow / wrap.
   - String / binary: empty, multi-byte UTF-8 (`'é'`, `'日本'`), embedded NULs if relevant.
5. **Cover the Spark-specific edges** found in step 1: in-range boundaries, just-out-of-range values, ANSI error paths, foldable-only args.

Use `query expect_fallback(...)` for inputs where the serde returns `None`. Format: `docs/source/contributor-guide/sql-file-tests.md`.

### 6. Update docs

Hand-curated (`make` does not regenerate these):

- `docs/source/user-guide/latest/expressions.md` — add `| <ExpressionClass> | \`<sql_name>\` |` to the matching category table, alphabetical.
- `docs/source/user-guide/latest/expressions.md` — update the function's Status from 🔜/💤 to ✅ (or ⚠️) in the matching category table.

Per-expression compatibility pages under `compatibility/expressions/*.md` are auto-generated from `getCompatibleNotes` / `getIncompatibleReasons` / `getUnsupportedReasons` — do not hand-edit.

### 7. Build, audit, finalize

```bash
make
cd native && cargo clippy --all-targets --workspace -- -D warnings
```

Then run `audit-comet-expression` on `$ARGUMENTS` to compare against Spark 3.4.3 / 3.5.8 / 4.0.1 and surface coverage gaps; iterate on tests.

The user generally runs tests themselves. If asked for a smoke test:

```bash
./mvnw test -Dsuites="org.apache.comet.CometSqlFileTestSuite $ARGUMENTS" -Dtest=none
```

PR: fill in `.github/pull_request_template.md`; under "What changes are included", note the skill was used and which upstream function (`datafusion::…` built-in or `datafusion-spark::function::…`) is being wired.
