<!---
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

# Adding a New Expression

There are a number of Spark expression that are not supported by DataFusion Comet yet, and implementing them is a good way to contribute to the project.

Before you start, have a look through [these slides](https://docs.google.com/presentation/d/1H0fF2MOkkBK8fPBlnqK6LejUeLcVD917JhVWfp3mb8A/edit#slide=id.p) as they provide a conceptual overview. And a video of a presentation on those slides is available [here](https://drive.google.com/file/d/1POU4lFAZfYwZR8zV1X2eoLiAmc1GDtSP/view?usp=sharing).

## Finding an Expression to Add

You may have a specific expression in mind that you'd like to add, but if not, you can review the [expression coverage document](spark_expressions_support.md) to see which expressions are not yet supported.

## Implementing the Expression

Once you have the expression you'd like to add, you should take inventory of the following:

1. What is the Spark expression's behavior across different Spark versions? These make good test cases and will inform you of any compatibility issues, such as an API change that will have to be addressed.
2. Check if the expression is already implemented in DataFusion and if it is compatible with the Spark expression.
   1. If it is, you can potentially reuse the existing implementation though you'll need to add tests to verify compatibility.
   2. If it's not, consider an initial version in DataFusion for expressions that are common across different engines. For expressions that are specific to Spark, consider an initial version in DataFusion Comet.
3. Test cases for the expression. As mentioned, you can refer to Spark's test cases for a good idea of what to test.

Once you know what you want to add, you'll need to update the query planner to recognize the new expression in Scala and potentially add a new expression implementation in the Rust package.

### Adding the Expression in Scala

DataFusion Comet uses a framework based on the `CometExpressionSerde` trait for converting Spark expressions to protobuf. Instead of a large match statement, each expression type has its own serialization handler. For aggregate expressions, use the `CometAggregateExpressionSerde` trait instead.

#### Creating a CometExpressionSerde Implementation

First, create an object that extends `CometExpressionSerde[T]` where `T` is the Spark expression type. This is typically added to one of the serde files in `spark/src/main/scala/org/apache/comet/serde/` (e.g., `math.scala`, `strings.scala`, `arrays.scala`, etc.).

For example, the `unhex` function looks like this:

```scala
object CometUnhex extends CometExpressionSerde[Unhex] {
  override def convert(
      expr: Unhex,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)
    val failOnErrorExpr = exprToProtoInternal(Literal(expr.failOnError), inputs, binding)

    val optExpr =
      scalarFunctionExprToProtoWithReturnType(
        "unhex",
        expr.dataType,
        false,
        childExpr,
        failOnErrorExpr)
    optExprWithInfo(optExpr, expr, expr.child)
  }
}
```

The `CometExpressionSerde` trait provides several methods you can override:

- `convert(expr: T, inputs: Seq[Attribute], binding: Boolean): Option[Expr]` - **Required**. Converts the Spark expression to protobuf. Return `None` if the expression cannot be converted.
- `getSupportLevel(expr: T): SupportLevel` - Optional. Returns the [support level](#support-levels) for the expression at planning time, based on a specific expression instance. See [Using getSupportLevel](#using-getsupportlevel) below for details.
- `getIncompatibleReasons(): Seq[String]` - Optional. Returns reasons why this expression may produce different results than Spark. Used to generate the Compatibility Guide. See [Documenting Incompatible and Unsupported Reasons](#documenting-incompatible-and-unsupported-reasons) below.
- `getUnsupportedReasons(): Seq[String]` - Optional. Returns reasons why this expression may not be supported by Comet (for example, unsupported data types or format strings). Used to generate the Compatibility Guide. See [Documenting Incompatible and Unsupported Reasons](#documenting-incompatible-and-unsupported-reasons) below.
- `getExprConfigName(expr: T): String` - Optional. Returns a short name for configuration keys. Defaults to the Spark class name.

For simple scalar functions that map directly to a DataFusion function, you can use the built-in `CometScalarFunction` implementation:

```scala
classOf[Cos] -> CometScalarFunction("cos")
```

#### When to set the return type explicitly

`CometScalarFunction(name)` and the lower-level `scalarFunctionExprToProto(name, args)` helper both produce a protobuf `ScalarFunc` message **without** a `return_type` field. That is fine when the function name does not collide with a DataFusion built-in, or when it does collide and the Spark and DataFusion versions take the same arity and types. In that case the native planner consults DataFusion's UDF registry only to resolve the return type, then swaps in Comet's UDF for execution.

It is **not** fine when the Spark function and the DataFusion built-in differ in arity or input types. The native planner calls `coerce_types` and `return_field_from_args` on DataFusion's UDF before Comet's UDF is selected, and a signature mismatch fails the query at execution time with an error like:

```
org.apache.comet.CometNativeException: Error from DataFusion:
Function 'levenshtein' expects 2 arguments but received 3.
```

The classic case is `levenshtein`. Spark accepts an optional 3rd `threshold` argument, DataFusion's built-in is 2-arg only, so the 3-arg form fails native execution unless the serde sets the return type explicitly. Other names that exist in both engines with potentially different signatures include `concat`, `coalesce`, `sha2`, and `regexp_replace`. If you are adding a function whose name is shared with `datafusion-functions`, check the upstream signature before deciding how to serialize.

To avoid the registry lookup, write a custom `CometExpressionSerde` and use `scalarFunctionExprToProtoWithReturnType`, passing the Spark expression's declared `dataType`:

```scala
object CometLevenshtein extends CometExpressionSerde[Levenshtein] {
  override def convert(
      expr: Levenshtein,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExprs = expr.children.map(exprToProtoInternal(_, inputs, binding))
    val optExpr = scalarFunctionExprToProtoWithReturnType(
      "levenshtein",
      expr.dataType,
      false,
      childExprs: _*)
    optExprWithInfo(optExpr, expr, expr.children: _*)
  }
}
```

When the return type is set on the proto, the native planner skips the registry lookup entirely and routes straight to the Comet UDF registered in `create_comet_physical_fun_with_eval_mode`.

#### Registering the Expression Handler

Once you've created your `CometExpressionSerde` implementation, register it in `QueryPlanSerde.scala` by adding it to the appropriate expression map (e.g., `mathExpressions`, `stringExpressions`, `predicateExpressions`, etc.):

```scala
private val mathExpressions: Map[Class[_ <: Expression], CometExpressionSerde[_]] = Map(
  // ... other expressions ...
  classOf[Unhex] -> CometUnhex,
  classOf[Hex] -> CometHex)
```

The `exprToProtoInternal` method will automatically use this mapping to find and invoke your handler when it encounters the corresponding Spark expression type.

A few things to note:

- The `convert` method is recursively called on child expressions using `exprToProtoInternal`, so you'll need to make sure that the child expressions are also converted to protobuf.
- `scalarFunctionExprToProtoWithReturnType` is for scalar functions that need to return type information. Your expression may use a different method depending on the type of expression.
- Use helper methods like `createBinaryExpr` and `createUnaryExpr` from `QueryPlanSerde` for common expression patterns.

#### Support Levels

The `SupportLevel` sealed trait has three possible values:

- **`Compatible(notes: Option[String] = None)`** - Comet supports this expression with full compatibility with Spark, or may have known differences in specific edge cases unlikely to affect most users. This is the default if you don't override `getSupportLevel`.
- **`Incompatible(notes: Option[String] = None)`** - Comet supports this expression but results can differ from Spark. The expression will only be used if `spark.comet.expr.allowIncompatible=true` or the expression-specific config `spark.comet.expr.<exprName>.allowIncompatible=true` is set.
- **`Unsupported(notes: Option[String] = None)`** - Comet does not support this expression under the current conditions. Spark will fall back to its native execution.

All three accept an optional `notes` parameter to provide additional context that is logged for debugging.

#### Using getSupportLevel

The `getSupportLevel` method allows you to control whether an expression should be executed by Comet based on various conditions such as data types, parameter values, or other expression-specific constraints. This is particularly useful when:

1. Your expression only supports specific data types
2. Your expression has known incompatibilities with Spark's behavior
3. Your expression has edge cases that aren't yet supported

##### Examples

**Example 1: Restricting to specific data types**

The `Abs` expression only supports numeric types:

```scala
object CometAbs extends CometExpressionSerde[Abs] {
  override def getSupportLevel(expr: Abs): SupportLevel = {
    expr.child.dataType match {
      case _: NumericType =>
        Compatible()
      case _ =>
        // Spark supports NumericType, DayTimeIntervalType, and YearMonthIntervalType
        Unsupported(Some("Only integral, floating-point, and decimal types are supported"))
    }
  }

  override def convert(
      expr: Abs,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    // ... conversion logic ...
  }
}
```

**Example 2: Validating parameter values**

The `TruncDate` expression only supports specific format strings:

```scala
object CometTruncDate extends CometExpressionSerde[TruncDate] {
  val supportedFormats: Seq[String] =
    Seq("year", "yyyy", "yy", "quarter", "mon", "month", "mm", "week")

  override def getSupportLevel(expr: TruncDate): SupportLevel = {
    expr.format match {
      case Literal(fmt: UTF8String, _) =>
        if (supportedFormats.contains(fmt.toString.toLowerCase(Locale.ROOT))) {
          Compatible()
        } else {
          Unsupported(Some(s"Format $fmt is not supported"))
        }
      case _ =>
        Incompatible(
          Some("Invalid format strings will throw an exception instead of returning NULL"))
    }
  }

  override def convert(
      expr: TruncDate,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    // ... conversion logic ...
  }
}
```

**Example 3: Marking known incompatibilities**

The `ArrayAppend` expression has known behavioral differences from Spark:

```scala
object CometArrayAppend extends CometExpressionSerde[ArrayAppend] {
  override def getSupportLevel(expr: ArrayAppend): SupportLevel = Incompatible(None)

  override def convert(
      expr: ArrayAppend,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    // ... conversion logic ...
  }
}
```

This expression will only be used when users explicitly enable incompatible expressions via configuration.

##### How getSupportLevel Affects Execution

When the query planner encounters an expression:

1. It first checks if the expression is explicitly disabled via `spark.comet.expr.<exprName>.enabled=false`
2. It then calls `getSupportLevel` on the expression handler
3. Based on the result:
   - `Compatible()`: Expression proceeds to conversion
   - `Incompatible()`: Expression is skipped unless `spark.comet.expr.allowIncompatible=true` or expression-specific allow config is set
   - `Unsupported()`: Expression is skipped and a fallback to Spark occurs

Any notes provided will be logged to help with debugging and understanding why an expression was not used.

#### Documenting Incompatible and Unsupported Reasons

In addition to `getSupportLevel`, which governs runtime planning decisions, the serde trait exposes two static documentation methods:

- `getIncompatibleReasons(): Seq[String]` - Reasons the expression may produce different results than Spark.
- `getUnsupportedReasons(): Seq[String]` - Reasons the expression, or certain usages of it, may not be supported by Comet.

These methods do not affect runtime behavior. They are called by `GenerateDocs` (`spark/src/main/scala/org/apache/comet/GenerateDocs.scala`) when building the user-facing Compatibility Guide pages under `docs/source/user-guide/latest/compatibility/expressions/` (for example, `math.md`, `datetime.md`, `array.md`, `aggregate.md`, `struct.md`). Each reason is rendered as a bullet in the corresponding page.

Key differences from `getSupportLevel`:

- **No expression instance.** Both methods take no arguments, so they describe the expression in general rather than a specific call site. Use `getSupportLevel` for checks that depend on data types, argument values, or other per-instance details.
- **Markdown-friendly.** Each returned string is written to a Markdown document, so you can embed backticks, links, and line breaks. Keep each reason self-contained, since they are rendered as separate bullets.
- **Regenerated by CI.** The lists are collected by `GenerateDocs` and published by CI on every merge to `main`. The generated Markdown is not committed to the repo, so you do not need to regenerate or commit it yourself. The reasons do not have to match the `notes` passed to `Compatible`, `Incompatible`, or `Unsupported`, but keeping them consistent avoids confusing users.

##### Example: Incompatibility note

```scala
object CometStructsToJson extends CometExpressionSerde[StructsToJson] {

  override def getIncompatibleReasons(): Seq[String] = Seq(
    "Does not support `+Infinity` and `-Infinity` for numeric types (float, double)." +
      " (https://github.com/apache/datafusion-comet/issues/3016)")

  override def getSupportLevel(expr: StructsToJson): SupportLevel =
    Incompatible(
      Some(
        "Does not support Infinity/-Infinity for numeric types" +
          " (https://github.com/apache/datafusion-comet/issues/3016)"))

  // ... convert ...
}
```

##### Example: Unsupported note

```scala
object CometSortArray extends CometExpressionSerde[SortArray] {

  override def getUnsupportedReasons(): Seq[String] = Seq(
    "Nested arrays with `Struct` or `Null` child values are not supported natively and will" +
      " fall back to Spark.")

  // ... convert ...
}
```

##### Example: Enumerating supported values

When the expression only supports a known set of argument values, build the reason from that set so the documentation stays in sync with the code:

```scala
override def getUnsupportedReasons(): Seq[String] = Seq(
  "Only the following formats are supported:" +
    supportedFormats.keys.toSeq.sorted
      .map(k => s"`$k`")
      .mkString("\n  - ", "\n  - ", ""))
```

#### Adding Spark-side Tests for the New Expression

It is important to verify that the new expression is correctly recognized by the native execution engine and matches the expected Spark behavior. The preferred way to add test coverage is to write a Comet SQL Test. This approach is simpler than writing Comet Scala Tests and makes it easy to cover many input combinations and edge cases.

##### Writing a Comet SQL Test

Create a `.sql` file under the appropriate subdirectory in `spark/src/test/resources/sql-tests/expressions/` (e.g., `string/`, `math/`, `array/`). The file should create a table with test data, then run queries that exercise the expression. Here is an example for the `unhex` expression:

```sql
statement
CREATE TABLE test_unhex(col string) USING parquet

statement
INSERT INTO test_unhex VALUES
  ('537061726B2053514C'),
  ('737472696E67'),
  ('\0'),
  (''),
  ('###'),
  ('G123'),
  ('hello'),
  ('A1B'),
  ('0A1B'),
  (NULL)

-- column argument
query
SELECT unhex(col) FROM test_unhex

-- literal arguments
query
SELECT unhex('48656C6C6F'), unhex(''), unhex(NULL)
```

Each `query` block automatically runs the SQL through both Spark and Comet and compares results, and also verifies that Comet executes the expression natively (not falling back to Spark).

Run the test with:

```shell
./mvnw test -Dsuites="org.apache.comet.CometSqlFileTestSuite unhex" -Dtest=none
```

For full documentation on the test file format, including directives like `ConfigMatrix`, query modes like `spark_answer_only` and `tolerance`, handling known bugs with `ignore(...)`, and tips for writing thorough tests, see the [Comet SQL Tests](sql-file-tests.md) guide.

##### Tips

- **Cover both column references and literals.** Comet often uses different code paths for each. The Comet SQL Tests suite automatically disables constant folding, so all-literal queries are evaluated natively.
- **Include edge cases** such as `NULL`, empty strings, boundary values, `NaN`, and multibyte UTF-8 characters.
- **Keep one file per expression** to make failures easy to locate.

##### Comet Scala Tests (alternative)

For cases that require programmatic setup or custom assertions beyond what SQL files support, you can also add Comet Scala Tests in `CometExpressionSuite` using the `checkSparkAnswerAndOperator` method:

```scala
test("unhex") {
  val table = "unhex_table"
  withTable(table) {
    sql(s"create table $table(col string) using parquet")

    sql(s"""INSERT INTO $table VALUES
      |('537061726B2053514C'),
      |('737472696E67'),
      |('\\0'),
      |(''),
      |('###'),
      |('G123'),
      |('hello'),
      |('A1B'),
      |('0A1B')""".stripMargin)

    checkSparkAnswerAndOperator(s"SELECT unhex(col) FROM $table")
  }
}
```

When writing Comet Scala Tests with literal values (e.g., `SELECT my_func('literal')`), Spark's constant folding optimizer may evaluate the expression at planning time, bypassing Comet. To prevent this, disable constant folding:

```scala
test("my_func with literals") {
  withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
      "org.apache.spark.sql.catalyst.optimizer.ConstantFolding") {
    checkSparkAnswerAndOperator("SELECT my_func('literal_value')")
  }
}
```

### Adding the Expression To the Protobuf Definition

Once you have the expression implemented in Scala, you might need to update the protobuf definition to include the new expression. You may not need to do this if the expression is already covered by the existing protobuf definition (e.g. you're adding a new scalar function that uses the `ScalarFunc` message).

You can find the protobuf definition in `native/proto/src/proto/expr.proto`, and in particular the `Expr` or potentially the `AggExpr` messages. If you were to add a new expression called `Add2`, you would add a new case to the `Expr` message like so:

```proto
message Expr {
  oneof expr_struct {
    ...
    Add2 add2 = 100;  // Choose the next available number
  }
}
```

Then you would define the `Add2` message like so:

```proto
message Add2 {
  Expr left = 1;
  Expr right = 2;
}
```

### Adding the Expression in Rust

With the serialization complete, the next step is to implement the expression in Rust and ensure that the incoming plan can make use of it.

How this works is somewhat dependent on the type of expression you're adding. Expression implementations live in the `native/spark-expr/src/` directory, organized by category (e.g., `math_funcs/`, `string_funcs/`, `array_funcs/`).

#### Generally Adding a New Expression

If you're adding a new expression that requires custom protobuf serialization, you may need to:

1. Add a new message to the protobuf definition in `native/proto/src/proto/expr.proto`
2. Add a native expression handler in `expression_registry.rs` to deserialize the new protobuf message type and
   create a native expression

For most expressions, you can skip this step if you're using the existing scalar function infrastructure.

#### Adding a New Scalar Function Expression

For a new scalar function, you can reuse a lot of code by updating the `create_comet_physical_fun` method in `native/spark-expr/src/comet_scalar_funcs.rs`. Add a match case for your function name:

```rust
match fun_name {
    // ... other functions ...
    "unhex" => {
        let func = Arc::new(spark_unhex);
        make_comet_scalar_udf!("unhex", func, without data_type)
    }
    // ... more functions ...
}
```

The `make_comet_scalar_udf!` macro has several variants depending on whether your function needs:

- A data type parameter: `make_comet_scalar_udf!("ceil", spark_ceil, data_type)`
- No data type parameter: `make_comet_scalar_udf!("unhex", func, without data_type)`
- An eval mode: `make_comet_scalar_udf!("decimal_div", spark_decimal_div, data_type, eval_mode)`
- A fail_on_error flag: `make_comet_scalar_udf!("spark_modulo", func, without data_type, fail_on_error)`

#### Implementing the Function

Then implement your function in an appropriate module under `native/spark-expr/src/`. The function signature will look like:

```rust
pub fn spark_unhex(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    // Do the work here
}
```

If your function uses the data type or eval mode, the signature will include those as additional parameters:

```rust
pub fn spark_ceil(
    args: &[ColumnarValue],
    data_type: &DataType
) -> Result<ColumnarValue, DataFusionError> {
    // Implementation
}
```

### API Differences Between Spark Versions

If the expression you're adding has different behavior across different Spark versions, you'll need to account for that in your implementation. There are two tools at your disposal to help with this:

1. Shims that exist in `spark/src/main/spark-$SPARK_VERSION/org/apache/comet/shims/CometExprShim.scala` for each Spark version. These shims are used to provide compatibility between different Spark versions.
2. Variables that correspond to the Spark version, such as `isSpark33Plus`, which can be used to conditionally execute code based on the Spark version.

## Shimming to Support Different Spark Versions

If the expression you're adding has different behavior across different Spark versions, you can use the shim system located in `spark/src/main/spark-$SPARK_VERSION/org/apache/comet/shims/CometExprShim.scala` for each Spark version.

The `CometExprShim` trait provides several mechanisms for handling version differences:

1. **Version-specific methods** - Override methods in the trait to provide version-specific behavior
2. **Version-specific expression handling** - Use `versionSpecificExprToProtoInternal` to handle expressions that only exist in certain Spark versions

For example, the `StringDecode` expression only exists in certain Spark versions. The shim handles this:

```scala
trait CometExprShim {
  def versionSpecificExprToProtoInternal(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    expr match {
      case s: StringDecode =>
        stringDecode(expr, s.charset, s.bin, inputs, binding)
      case _ => None
    }
  }
}
```

The `QueryPlanSerde.exprToProtoInternal` method calls `versionSpecificExprToProtoInternal` first, allowing shims to intercept and handle version-specific expressions before falling back to the standard expression maps.

Your `CometExpressionSerde` implementation can also access shim methods by mixing in the `CometExprShim` trait, though in most cases you can directly access the expression properties if they're available across all supported Spark versions.

## Resources

- [Variance PR](https://github.com/apache/datafusion-comet/pull/297)
  - Aggregation function
- [Unhex PR](https://github.com/apache/datafusion-comet/pull/342)
  - Basic scalar function with shims for different Spark versions
