---
name: implement-comet-expression
description: Use when implementing a new Spark expression in DataFusion Comet. Walks through cloning latest Spark master to study the canonical implementation, building the initial Comet serde and Rust function from the contributor guide, then running audit-comet-expression to drive a test-coverage iteration loop.
argument-hint: <expression-name>
---

Implement Comet support for the `$ARGUMENTS` Spark expression.

## Background reading

The contributor guide is the canonical reference. Read these before writing code:

- `docs/source/contributor-guide/adding_a_new_expression.md` covers the Scala serde, protobuf, Rust scalar function flow, support levels, shims, and tests.
- `docs/source/contributor-guide/sql-file-tests.md` describes the Comet SQL Tests format.
- `docs/source/contributor-guide/spark_expressions_support.md` lists the coverage status for every expression.

## Workflow

### 1. Study the Spark master implementation first

Always start from the latest Spark `master`. Shallow clone if not already present:

```bash
if [ ! -d /tmp/spark-master ]; then
  git clone --depth 1 https://github.com/apache/spark.git /tmp/spark-master
fi
```

Find the expression class and tests:

```bash
find /tmp/spark-master/sql -name "*.scala" | \
  xargs grep -l "case class $ARGUMENTS\b\|object $ARGUMENTS\b" 2>/dev/null

find /tmp/spark-master/sql -name "*.scala" -path "*/test/*" | \
  xargs grep -l "$ARGUMENTS" 2>/dev/null
```

Read the source. Note `inputTypes`, `dataType`, `eval` / `nullSafeEval`, ANSI mode branches, and any `require` guards. These define the contract Comet must match.

### 2. Implement the initial version

Follow `adding_a_new_expression.md`:

1. Add a `CometExpressionSerde[T]` in the appropriate file under `spark/src/main/scala/org/apache/comet/serde/`.
2. Register it in the matching map in `QueryPlanSerde.scala`.
3. If the function name collides with a DataFusion built-in that has a different signature, use `scalarFunctionExprToProtoWithReturnType` (see "When to set the return type explicitly").
4. For a new scalar function, add a match case in `native/spark-expr/src/comet_scalar_funcs.rs::create_comet_physical_fun` and implement the function under `native/spark-expr/src/`.
5. Add at least one Comet SQL Test at `spark/src/test/resources/sql-tests/expressions/<category>/$ARGUMENTS.sql` exercising column references, literals, and `NULL`.

Build and smoke-test:

```bash
make
./mvnw test -Dsuites="org.apache.comet.CometSqlFileTestSuite $ARGUMENTS" -Dtest=none
```

### 3. Run the audit skill

Once the initial implementation passes its smoke test, run the `audit-comet-expression` skill on `$ARGUMENTS`. It compares the implementation and tests against Spark 3.4.3, 3.5.8, and 4.0.1 and produces a prioritized list of gaps.

### 4. Implement audit-recommended tests and iterate

Add the missing test cases the audit recommends, then re-run the targeted suite:

```bash
./mvnw test -Dsuites="org.apache.comet.CometSqlFileTestSuite $ARGUMENTS" -Dtest=none
```

Surface findings to the user and ask whether the coverage is sufficient. Continue iterating (adding tests, fixing bugs, refining `getSupportLevel` / `getIncompatibleReasons` / `getUnsupportedReasons`) until the user confirms they are happy.

### 5. Final checks

Before opening a PR:

```bash
make format
cd native && cargo clippy --all-targets --workspace -- -D warnings
```

Use the repo's PR template and fill in every section.
