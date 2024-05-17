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

# Adding a Expression

There are a number of Spark expression that are not supported by DataFusion Comet yet, and implementing them is a good way to contribute to the project.

Before you start, have a look through [these slides](https://docs.google.com/presentation/d/1H0fF2MOkkBK8fPBlnqK6LejUeLcVD917JhVWfp3mb8A/edit#slide=id.p) as they provide a conceptual overview. And a video of a presentation on those slides is available [here](https://drive.google.com/file/d/1POU4lFAZfYwZR8zV1X2eoLiAmc1GDtSP/view?usp=sharing).

## Finding an Expression to Add

You may have a specific expression in mind that you'd like to add, but if not, you can review the [expression coverage document](https://github.com/apache/datafusion-comet/blob/f08fcadd5fbdb5b04293d33e654f6c16f81b70c4/doc/spark_builtin_expr_coverage.txt) to see which expressions are not yet supported.

## Adding the Expression

Once you have the expression you'd like to add, you should take inventory of the following:

1. What is the Spark expressions behavior across different Spark versions? These make good test cases, and will inform you of any compatibility issues such as an API change that will have to be addressed.
2. Check if the expression is already implemented in DataFusion and if the is compatible with the Spark expression. If it is, you can potentially reuse the existing implementation. If it's not, consider an initial version in DataFusion Comet and potentially backport it into DataFusion if the expression would be supported by it.
3. Test cases for the expression. As mentioned, you can refer to Spark's test cases for a good idea of what to test.

Once you know what you want to add, you'll need to update the query planner to recognize the new expression in Scala and potentially add a new expression implementation `core/` in Rust.

### Adding the Expression in Scala

The `QueryPlanSerde` object has a method `exprToProto`, which is responsible for converting a Spark expression to a protobuf expression. Within that method is an `exprToProtoInternal` method that contains a large match statement for each expression type. You'll need to add a new case to this match statement for your new expression.

For example, the `unhex` function looks like this:

```scala
case e: Unhex if !isSpark32 =>
  val unHex = unhexSerde(e)

  val childExpr = exprToProtoInternal(unHex._1, inputs)
  val failOnErrorExpr = exprToProtoInternal(unHex._2, inputs)

  val optExpr =
    scalarExprToProtoWithReturnType("unhex", e.dataType, childExpr, failOnErrorExpr)
  optExprWithInfo(optExpr, expr, unHex._1)
```

A few things to note here:

* The `isSpark32` check is used to fall back to Spark's implementation of `unhex` in Spark 3.2, as only versions after that have the `failOnError` parameter.
* The function is recursively called on child expressions, so you'll need to make sure that the child expressions are also converted to protobuf.
* `scalarExprToProtoWithReturnType` is for scalar functions that need return type information. Your expression may use a different method depending on the type of expression.

#### Adding Spark-side Tests for the New Expression

It is important to verify that the new expression is correctly recognized by the native execution engine and matches the expected spark behavior. To do this, you can add a set of test cases in the `CometExpressionSuite`, and use the `checkSparkAnswerAndOperator` method to compare the results of the new expression with the expected Spark results and that Comet's native execution engine is able to execute the expression.

For example, this is the test case for the `unhex` expression:

```scala
test("unhex") {
  assume(!isSpark32, "unhex function has incorrect behavior in 3.2")  // used to skip the test in Spark 3.2

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

Once you have the expression implemented in Scala, you might need to update the protobuf definition to include the new expression. You may not need to do this if the expression is already covered by the existing protobuf definition (e.g. you're adding a new scalar function).

You can find the protobuf definition in `expr.proto`, and in particular the `Expr` or potentially the `AggExpr`. These are similar in theory to the large case statement in `QueryPlanSerde`, but in protobuf format. So if you were to add a new expression called `Add2`, you would add a new case to the `Expr` message like so:

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

How this works, is somewhat dependent on the type of expression you're adding, so see the `core/src/execution/datafusion/expressions` directory for examples of how to implement different types of expressions.

#### Generally Adding a New Expression

If you're adding a new expression, you'll need to review `create_plan` and `create_expr`. `create_plan` is responsible for translating the incoming plan into a DataFusion plan, and may delegate to `create_expr` to create the physical expressions for the plan.

If you added a new message to the protobuf definition, you'll add a new match case to the `create_expr` method to handle the new expression. For example, if you added an `Add2` expression, you would add a new case like so:

```rust
match spark_expr.expr_struct.as_ref().unwrap() {
  ...
  ExprStruct::Add2(add2) => self.create_binary_expr(...)
}
```

`self.create_binary_expr` is for a binary expression, but if something out of the box is needed, you can create a new `PhysicalExpr` implementation. For example, see `if_expr.rs` for an example of an implementation that doesn't fit the `create_binary_expr` mold.

#### Adding a New Scalar Function Expression

For a new scalar function, you can reuse a lot of code by updating the `create_comet_physical_fun` method to match on the function name and make the scalar UDF to be called. For example, the diff to add the `unhex` function is:

```diff
macro_rules! make_comet_scalar_udf {
    ($name:expr, $func:ident, $data_type:ident) => {{

+       "unhex" => {
+           let func = Arc::new(spark_unhex);
+           make_comet_scalar_udf!("unhex", func, without data_type)
+       }

    }}
}
```

With that addition, you can now implement the spark function in Rust. This function will look very similar to DataFusion code. For examples, see the `core/src/execution/datafusion/expressions/scalar_funcs` directory.

Without getting into the internals, the function signature will look like:

```rust
pub(super) fn spark_unhex(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    // Do the work here
}
```

> **_NOTE:_**  If you call the `make_comet_scalar_udf` macro with the data type, the function signature will look include the data type as a second argument.

### API Differences Between Spark Versions

If the expression you're adding has different behavior across different Spark versions, you'll need to account for that in your implementation. There are two tools at your disposal to help with this:

1. Shims that exist in `spark/src/main/spark-$SPARK_VERSION/org/apache/comet/shims/CometExprShim.scala` for each Spark version. These shims are used to provide compatibility between different Spark versions.
2. Variables that correspond to the Spark version, such as `isSpark32`, which can be used to conditionally execute code based on the Spark version.

## Shimming to Support Different Spark Versions

By adding shims for each Spark version, you can provide a consistent interface for the expression across different Spark versions. For example, `unhex` added a new optional parameter is Spark 3.4, for if it should `failOnError` or not. So for version 3.2 and 3.3, the shim is:

```scala
trait CometExprShim {
    /**
      * Returns a tuple of expressions for the `unhex` function.
      */
    def unhexSerde(unhex: Unhex): (Expression, Expression) = {
        (unhex.child, Literal(false))
    }
}
```

And for version 3.4, the shim is:

```scala
trait CometExprShim {
    /**
      * Returns a tuple of expressions for the `unhex` function.
      */
    def unhexSerde(unhex: Unhex): (Expression, Expression) = {
        (unhex.child, unhex.failOnError)
    }
}
```

Then when `unhexSerde` is called in the `QueryPlanSerde` object, it will use the correct shim for the Spark version.

## Resources

* [Variance PR](https://github.com/apache/datafusion-comet/pull/297)
  * Aggregation function
* [Unhex PR](https://github.com/apache/datafusion-comet/pull/342)
  * Basic scalar function with shims for different Spark versions
