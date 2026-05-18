---
name: wire-datafusion-function
description: Use when wiring an existing DataFusion or datafusion-spark function into Comet for a Spark expression. Identifies the right wiring pattern (one-line passthrough, datafusion-spark UDF registration, or custom serde with input massaging / restrictions), applies the Scala serde, registers the UDF in jni_api when needed, and adds SQL file tests. Assumes the function already exists upstream — if not, switch to `implement-comet-expression`.
argument-hint: <expression-name>
---

Wire Comet support for the `$ARGUMENTS` Spark expression by reusing an existing DataFusion or `datafusion-spark` function. **No native Rust implementation is written by this skill** — if upstream coverage is missing, stop and run `implement-comet-expression` instead.

## Background reading

- `docs/source/contributor-guide/adding_a_new_expression.md` — Scala serde, protobuf, support levels, SQL tests.
- `docs/source/contributor-guide/sql-file-tests.md` — Comet SQL Tests format.
- `spark/src/main/scala/org/apache/comet/serde/CometExpressionSerde.scala` — trait every serde implements.
- `spark/src/main/scala/org/apache/comet/serde/CometScalarFunction.scala` — the one-line passthrough wrapper.
- `spark/src/main/scala/org/apache/comet/serde/SupportLevel.scala` — `Compatible` / `Incompatible` / `Unsupported`.

## Wiring patterns

Pick the lightest pattern that satisfies the Spark contract. Steps below tell you which one to use.

### Pattern A — One-line passthrough (cleanest)

Spark expression maps directly to a function name resolvable at runtime. The function lives in either `datafusion-functions` (registered by default in `SessionContext`) or in `datafusion-spark` and is **already** registered in `register_datafusion_spark_function`. Single line in the right map of `QueryPlanSerde.scala`:

```scala
classOf[Acos] -> CometScalarFunction("acos"),                 // datafusion-functions built-in
classOf[Crc32] -> CometScalarFunction("crc32"),               // datafusion-spark SparkCrc32 (already registered)
classOf[DateAdd] -> CometScalarFunction[DateAdd]("date_add"), // see datetime.scala
```

### Pattern B — Pattern A plus a one-line UDF registration

Same one-liner in `QueryPlanSerde.scala`, **plus** registering the new `datafusion-spark` UDF in `native/core/src/execution/jni_api.rs::register_datafusion_spark_function`:

```rust
session_ctx.register_udf(ScalarUDF::new_from_impl(SparkXyz::default()));
```

Function names must match — the Scala name passed to `CometScalarFunction("xyz")` must equal `SparkXyz::name()` in the upstream crate.

### Pattern C — Custom `CometExpressionSerde`

Use when the Spark contract requires any of:

- preprocessing inputs (e.g. wrap with `nullIfNegative`, add `+0.0` to flip `-0.0`, cast),
- setting an explicit return type or `fail_on_error` flag → use `scalarFunctionExprToProtoWithReturnType`,
- restricting supported input types → implement `getSupportLevel` returning `Unsupported(notes)` / `Incompatible(notes)`,
- foldable / literal-only argument checks (e.g. `Sha2.numBits` must be a literal),
- documenting always-on differences via `getCompatibleNotes` / `getIncompatibleReasons` / `getUnsupportedReasons`.

Examples to study before writing one: `CometCeil` / `CometFloor` (decimal scale fallback), `CometAtan2` (input massaging), `CometLog` (null-if-negative), `CometSha2` (literal-only argument), `CometAbs` (`getSupportLevel`).

## Workflow

### 1. Study the Spark master implementation

Same as step 1 of `implement-comet-expression`. Shallow-clone if missing, then grep for the expression class. The class name is PascalCase (e.g. `Csc`, not `csc`); capitalize `$ARGUMENTS` before grepping, or use `-i`:

```bash
if [ ! -d /tmp/spark-master ]; then
  git clone --depth 1 https://github.com/apache/spark.git /tmp/spark-master
fi

CLASS_NAME="$(printf '%s' "$ARGUMENTS" | awk '{print toupper(substr($0,1,1)) substr($0,2)}')"
find /tmp/spark-master/sql -name "*.scala" | \
  xargs grep -l "case class ${CLASS_NAME}\b\|object ${CLASS_NAME}\b" 2>/dev/null
```

If the `git clone` is blocked (e.g. sandboxed environment), **stop and ask the user for an alternative source** — typically a local Spark clone path. Then point grep at that path instead. Do not silently skip Spark study — the Spark contract is the ground truth.

Note `inputTypes`, `dataType`, `eval` / `nullSafeEval`, ANSI mode branches, `require` guards, and any foldable-only arguments. These define the Spark contract Comet must match.

### 2. Find the upstream function

**Prefer a local DataFusion clone if one is available** — check `CLAUDE.md`, project memory, and the user's `~/dev` tree for an existing checkout (e.g. `~/dev/.../rust/datafusion`). The local clone is typically the latest source and avoids cargo-registry version mismatches. If no local clone is found, fall back to the cached registry crate.

Read the pinned `datafusion-spark` version from `native/Cargo.toml` rather than picking the first cached copy — `head -1` can land on an older release that lacks the function. Use a portable regex (BSD `awk` on macOS does not support `\b`):

```bash
REPO_ROOT=$(git rev-parse --show-toplevel)
DF_SPARK_VER=$(awk -F'"' '/^datafusion-spark[ =]/ {print $2; exit}' "$REPO_ROOT/native/Cargo.toml")
DF_SPARK=$(ls -d ~/.cargo/registry/src/*/datafusion-spark-${DF_SPARK_VER}/ 2>/dev/null | head -1)
echo "Using datafusion-spark $DF_SPARK_VER at $DF_SPARK"

DF_FUNCS_VER=$(awk -F'"' '/^datafusion[ =]/ {print $2; exit}' "$REPO_ROOT/native/Cargo.toml")
DF_FUNCS=$(ls -d ~/.cargo/registry/src/*/datafusion-functions-${DF_FUNCS_VER}/ 2>/dev/null | head -1)

# Sanity check — empty paths mean the awk match failed or the crate is not cached.
[ -z "$DF_SPARK" ] && echo "WARNING: datafusion-spark path empty — check Cargo.toml regex and run 'cargo fetch' from native/"
[ -z "$DF_FUNCS" ] && echo "WARNING: datafusion-functions path empty — check Cargo.toml regex and run 'cargo fetch' from native/"

# Search for the candidate. Replace EXPR with the SQL function name.
EXPR='$ARGUMENTS'
grep -rin "fn name" "$DF_SPARK/src/function/" 2>/dev/null | grep -i "$EXPR"
grep -rin "fn name" "$DF_FUNCS/src/" 2>/dev/null | grep -i "$EXPR"
```

If using a local DataFusion clone instead, point the grep at `<datafusion-clone>/datafusion/spark/src/function/` and `<datafusion-clone>/datafusion/functions/src/` respectively.

If the cached crate directory does not exist (fresh checkout), run `cargo fetch` from `native/` first, or fall back to the latest cached version and verify the function exists in the pinned version's git tag before relying on it.

### 2a. Decision gate — confirm the source crate with the user

Before wiring, classify the candidate:

- **Found in `datafusion-spark`** → proceed without prompting. This crate is explicitly Spark-compatible.
- **Found only in `datafusion-functions` (pure DataFusion)** → **STOP and ask the user before proceeding**. Pure DataFusion functions follow standard SQL semantics and frequently diverge from Spark on edge cases (NULL vs error, negative inputs, overflow, return type, NaN handling). Even when the divergences look bridgeable with Pattern C preprocessing, the user should explicitly approve relying on a non-Spark-tuned implementation rather than requesting an upstream `datafusion-spark` port.

  Use `AskUserQuestion`. Surface what you found in both crates and the specific divergences you've already identified, then offer:
  1. Wire from `datafusion-functions` with bridging preprocessing (Pattern C). List the divergences and the masking/casting that would close them.
  2. Stop and switch to `implement-comet-expression` so the function can be added to `datafusion-spark` upstream and then wired with Pattern A or B.
  3. Skip — leave the expression unsupported for now.

  Do not proceed past this gate without an explicit answer.

- **Found in neither** → stop and run `implement-comet-expression`.

Verify the chosen function's `Signature`, return type, and behavior match Spark across all relevant edge cases (NULL, overflow, non-finite floats, decimal scale, locale, ANSI mode). If semantics diverge in a way the Scala serde can't bridge with preprocessing or restrictions → **stop and run `implement-comet-expression`** instead.

For datafusion-spark candidates, also check whether the UDF is already pre-registered: see `register_datafusion_spark_function` in `native/core/src/execution/jni_api.rs`. If listed, you only need Pattern A. If missing, you need Pattern B.

### 3. Pick the wiring pattern

| Situation                                                                                             | Pattern |
| ----------------------------------------------------------------------------------------------------- | ------- |
| DF built-in (e.g. `acos`, `md5`, `replace`) matches Spark; or datafusion-spark UDF already registered | **A**   |
| New `datafusion-spark` UDF, semantics already match Spark                                             | **B**   |
| Inputs need preprocessing, restrictions, or explicit return type / failOnError                        | **C**   |

When in doubt, start with the lightest pattern and escalate only if the Spark contract demands it.

### 4. Apply the Scala wiring

Add the entry to the matching map in `spark/src/main/scala/org/apache/comet/serde/QueryPlanSerde.scala`:

- `mathExpressions`, `stringExpressions`, `arrayExpressions`, `mapExpressions`, `structExpressions`, `predicateExpressions`, `conditionalExpressions`, `bitwiseExpressions`, `temporalExpressions`, `hashExpressions`, `conversionExpressions`, `miscExpressions`.

Confirm the Spark expression class is in scope. `QueryPlanSerde.scala` already imports `org.apache.spark.sql.catalyst.expressions._` (wildcard), so most expressions are picked up for free. For Pattern C, the topic file (`math.scala`, `strings.scala`, `datetime.scala`, `hash.scala`, `arrays.scala`, etc.) uses **explicit** imports — add the new class name to the existing `import org.apache.spark.sql.catalyst.expressions.{...}` line, and place the serde object alongside its peers.

Helpers from `QueryPlanSerde`:

- `scalarFunctionExprToProto(funcName, args*)` — basic call, `failOnError = false`, return type inferred from args.
- `scalarFunctionExprToProtoWithReturnType(funcName, returnType, failOnError, args*)` — set explicit return type and/or `fail_on_error`.
- `optExprWithInfo(optExpr, expr, children*)` — attaches the "why we couldn't convert" tag if any child failed; always wrap your final result.
- `withInfo(expr, "reason")` — tag a fallback reason when returning `None`.

For `getSupportLevel`, returning `Incompatible(Some("..."))` gates the function behind `spark.comet.expr.<exprConfigName>.allowIncompatible=true`. `Unsupported(notes)` always falls back to Spark.

### 5. Apply the native registration (Pattern B only)

In `native/core/src/execution/jni_api.rs::register_datafusion_spark_function`, add one line with the upstream import already in scope (or extend the imports). Keep the list grouped logically with neighboring entries.

Pattern A and Pattern C **never** touch native code.

### 6. Build and smoke-test

Build the project (the user generally runs tests themselves — do not run them proactively unless asked):

```bash
make
```

If the user asks for a smoke test:

```bash
./mvnw test -Dsuites="org.apache.comet.CometSqlFileTestSuite $ARGUMENTS" -Dtest=none
```

### 7. Add SQL file tests

Create `spark/src/test/resources/sql-tests/expressions/<category>/$ARGUMENTS.sql`. Coverage requirements (mandatory — do not skip):

1. **Inspect the Spark source for accepted input datatypes.** Look at `inputTypes`, the parent class (`UnaryMathExpression`, `BinaryExpression`, `ImplicitCastInputTypes`, `ExpectsInputTypes`), and any `dataType` overrides. Add one column per supported type — e.g. for a numeric function: `ByteType`, `ShortType`, `IntegerType`, `LongType`, `FloatType`, `DoubleType`, `DecimalType` (varying precision/scale). For a string function: also cover `BinaryType` if accepted.
2. **All `SELECT`s must read from a parquet table.** Do not write literal-only queries like `SELECT f(1.0), f(NULL)` — encode the values you want to test as rows in a parquet-backed table and query that. This exercises the real read path, not just the constant-folder.
3. **Mix `NULL` into every column** so the kernel exercises the validity bitmap, not just the all-valid fast path.
4. **Floating-point (`FloatType` / `DoubleType`)** columns must include: `NaN`, `+0.0`, `-0.0`, `-Infinity`, `+Infinity`. Use `cast('NaN' as double)`, `cast('Infinity' as double)`, `cast('-Infinity' as double)`.
5. **Integer / decimal** columns must include each type's max and min (e.g. `127` / `-128` for byte, `2147483647` / `-2147483648` for int, `9223372036854775807` / `-9223372036854775808` for long, `Decimal(38, 0)` boundary for decimal) so overflow / wrap behavior is exercised.
6. Cover the **Spark-specific edge cases identified in step 1** (negative scale, empty string, ANSI-mode error paths, etc.) — again, as rows in the parquet table.

Use `query expect_fallback(...)` for inputs where the serde intentionally returns `None`. Format described in `docs/source/contributor-guide/sql-file-tests.md`.

### 8. Update the documentation

Two files are hand-curated (not regenerated by `make`) and must be updated for every newly wired expression:

- `docs/source/user-guide/latest/expressions.md` — add a row to the matching category table (`## Math Expressions`, `## String Expressions`, etc.) in alphabetical order: `| <ExpressionClass> | \`<sql_name>\` |`.
- `docs/source/contributor-guide/spark_expressions_support.md` — flip the entry from `- [ ] <name>` to `- [x] <name>` in the matching category section.

Per-category compatibility pages under `docs/source/user-guide/latest/compatibility/expressions/*.md` are auto-generated by `GenerateDocs.scala` from `getCompatibleNotes` / `getIncompatibleReasons` / `getUnsupportedReasons`. Do not hand-edit those — `make` regenerates them.

### 9. Run the audit skill

Run `audit-comet-expression` on `$ARGUMENTS`. It compares against Spark 3.4.3, 3.5.8, and 4.0.1 and produces a prioritized gap list — usually missing test coverage. Iterate on tests until the user is satisfied.

### 10. Final checks

```bash
make format
cd native && cargo clippy --all-targets --workspace -- -D warnings
```

### 11. Open the PR

Fill in every section of `.github/pull_request_template.md`. In "What changes are included in this PR?", note that the `wire-datafusion-function` skill was used and call out which upstream function (DataFusion built-in or `datafusion-spark::function::...`) is being wired.
