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

You may have a specific expression in mind that you'd like to add, but if not, you can review the [expression coverage document](https://github.com/apache/datafusion-comet/blob/main/docs/spark_expressions_support.md) to see which expressions are not yet supported.

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

The `CometExpressionSerde` trait provides three methods you can override:

- `convert(expr: T, inputs: Seq[Attribute], binding: Boolean): Option[Expr]` - **Required**. Converts the Spark expression to protobuf. Return `None` if the expression cannot be converted.
- `getSupportLevel(expr: T): SupportLevel` - Optional. Returns the level of support for the expression. See "Using getSupportLevel" section below for details.
- `getExprConfigName(expr: T): String` - Optional. Returns a short name for configuration keys. Defaults to the Spark class name.

For simple scalar functions that map directly to a DataFusion function, you can use the built-in `CometScalarFunction` implementation:

```scala
classOf[Cos] -> CometScalarFunction("cos")
```

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

#### Using getSupportLevel

The `getSupportLevel` method allows you to control whether an expression should be executed by Comet based on various conditions such as data types, parameter values, or other expression-specific constraints. This is particularly useful when:

1. Your expression only supports specific data types
2. Your expression has known incompatibilities with Spark's behavior
3. Your expression has edge cases that aren't yet supported

The method returns one of three `SupportLevel` values:

- **`Compatible(notes: Option[String] = None)`** - Comet supports this expression with full compatibility with Spark, or may have known differences in specific edge cases that are unlikely to be an issue for most users. This is the default if you don't override `getSupportLevel`.
- **`Incompatible(notes: Option[String] = None)`** - Comet supports this expression but results can be different from Spark. The expression will only be used if `spark.comet.expr.allowIncompatible=true` or the expression-specific config `spark.comet.expr.<exprName>.allowIncompatible=true` is set.
- **`Unsupported(notes: Option[String] = None)`** - Comet does not support this expression under the current conditions. The expression will not be used and Spark will fall back to its native execution.

All three support levels accept an optional `notes` parameter to provide additional context about the support level.

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

#### Adding Spark-side Tests for the New Expression

It is important to verify that the new expression is correctly recognized by the native execution engine and matches the expected spark behavior. To do this, you can add a set of test cases in the `CometExpressionSuite`, and use the `checkSparkAnswerAndOperator` method to compare the results of the new expression with the expected Spark results and that Comet's native execution engine is able to execute the expression.

For example, this is the test case for the `unhex` expression:

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

With the serialization complete, the next step is to implement the expression in Rust. Comet now uses a modular expression registry pattern that provides better organization and type safety compared to monolithic match statements.

#### File Organization

The expression-related code is organized as follows:

- `native/core/src/execution/planner/expression_registry.rs` - Contains `ExpressionBuilder` trait, `ExpressionType` enum, and `ExpressionRegistry`
- `native/core/src/execution/planner/macros.rs` - Contains shared macros (`extract_expr!`, `binary_expr_builder!`, `unary_expr_builder!`)
- `native/core/src/execution/expressions/` - Individual expression builder implementations organized by category
- `native/spark-expr/src/` - Scalar function implementations organized by category (e.g., `math_funcs/`, `string_funcs/`)

#### Option A: Using the Expression Registry (Recommended for Complex Expressions)

For expressions that need custom protobuf handling or complex logic, use the modular registry pattern.

##### Create an ExpressionBuilder

Create or update a file in `native/core/src/execution/expressions/` (e.g., `comparison.rs`, `arithmetic.rs`):

```rust
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_proto::spark_expression::Expr;

use crate::execution::{
    operators::ExecutionError,
    planner::{expression_registry::ExpressionBuilder, PhysicalPlanner},
};
use crate::extract_expr;

/// Builder for YourNewExpression expressions
pub struct YourExpressionBuilder;

impl ExpressionBuilder for YourExpressionBuilder {
    fn build(
        &self,
        spark_expr: &Expr,
        input_schema: SchemaRef,
        planner: &PhysicalPlanner,
    ) -> Result<Arc<dyn PhysicalExpr>, ExecutionError> {
        // Use extract_expr! macro for type-safe extraction
        let expr = extract_expr!(spark_expr, YourNewExpression);

        // Convert child expressions
        let left = planner.create_expr(expr.left.as_ref().unwrap(), Arc::clone(&input_schema))?;
        let right = planner.create_expr(expr.right.as_ref().unwrap(), input_schema)?;

        // Create and return the DataFusion physical expression
        Ok(Arc::new(YourDataFusionExpr::new(left, right)))
    }
}
```

For simple binary expressions, you can use the `binary_expr_builder!` macro:

```rust
use datafusion::logical_expr::Operator as DataFusionOperator;
use crate::binary_expr_builder;

// This generates a complete ExpressionBuilder implementation
binary_expr_builder!(YourBinaryExprBuilder, YourBinaryExpr, DataFusionOperator::Plus);
```

##### Register the ExpressionBuilder

Add the ExpressionType to the enum in `native/core/src/execution/planner/expression_registry.rs`:

```rust
/// Enum to identify different expression types for registry dispatch
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExpressionType {
    // Arithmetic expressions
    Add,
    Subtract,
    // ... existing expressions ...
    YourNewExpression, // Add your expression type here
}
```

Register your builder in the `ExpressionRegistry` implementation:

```rust
/// Register all expression builders
fn register_all_expressions(&mut self) {
    self.register_arithmetic_expressions();
    self.register_comparison_expressions();
    self.register_your_expressions(); // Add this line
}

/// Register your new expressions
fn register_your_expressions(&mut self) {
    use crate::execution::expressions::your_category::YourExpressionBuilder;

    self.builders
        .insert(ExpressionType::YourNewExpression, Box::new(YourExpressionBuilder));
}
```

Update the `get_expression_type` function to map your protobuf expression:

```rust
fn get_expression_type(spark_expr: &Expr) -> Option<ExpressionType> {
    use datafusion_comet_proto::spark_expression::expr::ExprStruct;

    match spark_expr.expr_struct.as_ref()? {
        ExprStruct::Add(_) => Some(ExpressionType::Add),
        ExprStruct::Subtract(_) => Some(ExpressionType::Subtract),
        // ... existing expressions ...
        ExprStruct::YourNewExpression(_) => Some(ExpressionType::YourNewExpression),
    }
}
```

**Note**: See existing implementations in `native/core/src/execution/expressions/` for working examples, such as `arithmetic.rs`, `comparison.rs`, etc.

#### Option B: Using Scalar Functions (Recommended for Simple Functions)

For expressions that map directly to scalar functions, use the existing scalar function infrastructure. This approach is simpler for basic functions but less flexible than the registry pattern.

**When to use the Registry Pattern (Option A):**

- Complex expressions that need custom deserialization logic
- Expressions with multiple variants or complex parameter handling
- Binary/unary expressions that benefit from type-safe extraction
- Expressions that need custom DataFusion physical expression implementations

**When to use Scalar Functions (Option B):**

- Simple functions that map directly to DataFusion scalar functions
- Functions that don't need complex parameter handling
- Functions where the existing `CometScalarFunction` pattern is sufficient

**Benefits of the Registry Pattern:**

- **Better Organization**: Each expression's logic is isolated
- **Type Safety**: The `extract_expr!` macro ensures compile-time correctness
- **Extensibility**: New expressions can be added without modifying core planner logic
- **Code Reuse**: Macros like `binary_expr_builder!` reduce boilerplate
- **Graceful Fallback**: Unregistered expressions automatically fall back to the monolithic match

#### Option C: Fallback to Monolithic Match (Legacy)

If you need to add an expression but prefer not to use the registry pattern, expressions that aren't registered will automatically fall back to the legacy monolithic match statement in `create_expr()`. However, the registry pattern (Option A) is strongly recommended for new expressions.

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
