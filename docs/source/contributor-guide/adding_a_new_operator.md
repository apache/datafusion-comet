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

# Adding a New Operator

This guide explains how to add support for a new Spark physical operator in Apache DataFusion Comet.

## Overview

`CometExecRule` is responsible for replacing Spark operators with Comet operators. There are different approaches to implementing Comet operators depending on where they execute and how they integrate with the native execution engine.

### Types of Comet Operators

#### 1. Comet Native Operators

These operators run entirely in native Rust code and are the primary way to accelerate Spark workloads. For native operators, `CometExecRule` delegates to `QueryPlanSerde.operator2Proto` to:

- Check if the operator is enabled or disabled via configuration
- Validate if the operator can be supported
- Tag the operator with fallback reasons if conversion fails
- Serialize the operator to protobuf for native execution

Examples: `ProjectExec`, `FilterExec`, `SortExec`, `HashAggregateExec`, `SortMergeJoinExec`

#### 2. Comet JVM Operators

These operators run in the JVM but are part of the Comet execution path. For JVM operators, all checks happen in `CometExecRule` rather than `QueryPlanSerde`, because they don't need protobuf serialization.

Examples: `CometBroadcastExchangeExec`, `CometShuffleExchangeExec`

#### 3. Comet Sinks

Some operators serve as "sinks" or entry points for native execution, meaning they can be leaf nodes that feed data into native execution blocks.

Examples: `CometScanExec`, `CometBatchScanExec`, `UnionExec`, `CometSparkToColumnarExec`

## Implementing a Native Operator

This section focuses on adding a native operator, which is the most common and complex case.

### Step 1: Define the Protobuf Message

First, add the operator definition to `native/proto/src/proto/operator.proto`.

#### Add to the Operator Message

Add your new operator to the `oneof op_struct` in the main `Operator` message:

```proto
message Operator {
  repeated Operator children = 1;
  uint32 plan_id = 2;

  oneof op_struct {
    Scan scan = 100;
    Projection projection = 101;
    Filter filter = 102;
    // ... existing operators ...
    YourNewOperator your_new_operator = 112;  // Choose next available number
  }
}
```

#### Define the Operator Message

Create a message for your operator with the necessary fields:

```proto
message YourNewOperator {
  // Fields specific to your operator
  repeated spark.spark_expression.Expr expressions = 1;
  // Add other configuration fields as needed
}
```

For reference, see existing operators like `Filter` (simple), `HashAggregate` (complex), or `Sort` (with ordering).

### Step 2: Create a CometOperatorSerde Implementation

Create a new Scala file in `spark/src/main/scala/org/apache/comet/serde/operator/` (e.g., `CometYourOperator.scala`) that extends `CometOperatorSerde[T]` where `T` is the Spark operator type.

The `CometOperatorSerde` trait provides three key methods:

- `enabledConfig: Option[ConfigEntry[Boolean]]` - Configuration to enable/disable this operator
- `getSupportLevel(operator: T): SupportLevel` - Determines if the operator is supported
- `convert(op: T, builder: Operator.Builder, childOp: Operator*): Option[Operator]` - Converts to protobuf

#### Simple Example (Filter)

```scala
object CometFilter extends CometOperatorSerde[FilterExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_EXEC_FILTER_ENABLED)

  override def convert(
      op: FilterExec,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = {
    val cond = exprToProto(op.condition, op.child.output)

    if (cond.isDefined && childOp.nonEmpty) {
      val filterBuilder = OperatorOuterClass.Filter
        .newBuilder()
        .setPredicate(cond.get)
      Some(builder.setFilter(filterBuilder).build())
    } else {
      withInfo(op, op.condition, op.child)
      None
    }
  }
}
```

#### More Complex Example (Project)

```scala
object CometProject extends CometOperatorSerde[ProjectExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_EXEC_PROJECT_ENABLED)

  override def convert(
      op: ProjectExec,
      builder: Operator.Builder,
      childOp: Operator*): Option[OperatorOuterClass.Operator] = {
    val exprs = op.projectList.map(exprToProto(_, op.child.output))

    if (exprs.forall(_.isDefined) && childOp.nonEmpty) {
      val projectBuilder = OperatorOuterClass.Projection
        .newBuilder()
        .addAllProjectList(exprs.map(_.get).asJava)
      Some(builder.setProjection(projectBuilder).build())
    } else {
      withInfo(op, op.projectList: _*)
      None
    }
  }
}
```

#### Using getSupportLevel

Override `getSupportLevel` to control operator support based on specific conditions:

```scala
override def getSupportLevel(operator: YourOperatorExec): SupportLevel = {
  // Check for unsupported features
  if (operator.hasUnsupportedFeature) {
    return Unsupported(Some("Feature X is not supported"))
  }

  // Check for incompatible behavior
  if (operator.hasKnownDifferences) {
    return Incompatible(Some("Known differences in edge case Y"))
  }

  Compatible()
}
```

Support levels:

- **`Compatible()`** - Fully compatible with Spark (default)
- **`Incompatible()`** - Supported but may differ; requires explicit opt-in
- **`Unsupported()`** - Not supported under current conditions

### Step 3: Register the Operator

Add your operator to the `opSerdeMap` in `QueryPlanSerde.scala`:

```scala
private val opSerdeMap: Map[Class[_ <: SparkPlan], CometOperatorSerde[_]] =
  Map(
    classOf[ProjectExec] -> CometProject,
    classOf[FilterExec] -> CometFilter,
    // ... existing operators ...
    classOf[YourOperatorExec] -> CometYourOperator,
  )
```

### Step 4: Add Configuration Entry

Add a configuration entry in `common/src/main/scala/org/apache/comet/CometConf.scala`:

```scala
val COMET_EXEC_YOUR_OPERATOR_ENABLED: ConfigEntry[Boolean] =
  conf("spark.comet.exec.yourOperator.enabled")
    .doc("Whether to enable your operator in Comet")
    .booleanConf
    .createWithDefault(true)
```

### Step 5: Implement the Native Operator in Rust

#### Update the Planner

In `native/core/src/execution/planner.rs`, add a match case in the operator deserialization logic to handle your new protobuf message:

```rust
use datafusion_comet_proto::spark_operator::operator::OpStruct;

// In the create_plan or similar method:
match op.op_struct.as_ref() {
    Some(OpStruct::Scan(scan)) => {
        // ... existing cases ...
    }
    Some(OpStruct::YourNewOperator(your_op)) => {
        create_your_operator_exec(your_op, children, session_ctx)
    }
    // ... other cases ...
}
```

#### Implement the Operator

Create the operator implementation, either in an existing file or a new file in `native/core/src/execution/operators/`:

```rust
use datafusion::physical_plan::{ExecutionPlan, ...};
use datafusion_comet_proto::spark_operator::YourNewOperator;

pub fn create_your_operator_exec(
    op: &YourNewOperator,
    children: Vec<Arc<dyn ExecutionPlan>>,
    session_ctx: &SessionContext,
) -> Result<Arc<dyn ExecutionPlan>, ExecutionError> {
    // Deserialize expressions and configuration
    // Create and return the execution plan

    // Option 1: Use existing DataFusion operator
    // Ok(Arc::new(SomeDataFusionExec::try_new(...)?))

    // Option 2: Implement custom operator (see ExpandExec for example)
    // Ok(Arc::new(YourCustomExec::new(...)))
}
```

For custom operators, you'll need to implement the `ExecutionPlan` trait. See `native/core/src/execution/operators/expand.rs` or `scan.rs` for examples.

### Step 6: Add Tests

#### Scala Integration Tests

Add tests in `spark/src/test/scala/org/apache/comet/exec/CometExecSuite.scala` or a related test suite:

```scala
test("your operator") {
  withTable("test_table") {
    sql("CREATE TABLE test_table(col1 INT, col2 STRING) USING parquet")
    sql("INSERT INTO test_table VALUES (1, 'a'), (2, 'b')")

    // Test query that uses your operator
    checkSparkAnswerAndOperator(
      "SELECT * FROM test_table WHERE col1 > 1"
    )
  }
}
```

The `checkSparkAnswerAndOperator` helper verifies:

1. Results match Spark's native execution
2. Your operator is actually being used (not falling back)

#### Rust Unit Tests

Add unit tests in your Rust implementation file:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_your_operator() {
        // Test operator creation and execution
    }
}
```

### Step 7: Update Documentation

Add your operator to the supported operators list in `docs/source/user-guide/latest/compatibility.md` or similar documentation.

## Implementing a JVM Operator

For operators that run in the JVM:

1. Create a new operator class extending appropriate Spark base classes in `spark/src/main/scala/org/apache/comet/`
2. Add matching logic in `CometExecRule.scala` to transform the Spark operator
3. No protobuf or Rust implementation needed

Example pattern from `CometExecRule.scala`:

```scala
case s: ShuffleExchangeExec if nativeShuffleSupported(s) =>
  CometShuffleExchangeExec(s, shuffleType = CometNativeShuffle)
```

## Common Patterns and Helpers

### Expression Conversion

Use `QueryPlanSerde.exprToProto` to convert Spark expressions to protobuf:

```scala
val protoExpr = exprToProto(sparkExpr, inputSchema)
```

### Handling Fallback

Use `withInfo` to tag operators with fallback reasons:

```scala
if (!canConvert) {
  withInfo(op, "Reason for fallback", childNodes: _*)
  return None
}
```

### Child Operator Validation

Always check that child operators were successfully converted:

```scala
if (childOp.isEmpty) {
  // Cannot convert if children failed
  return None
}
```

## Debugging Tips

1. **Enable verbose logging**: Set `spark.comet.explain.verbose=true` to see detailed plan transformations
2. **Check fallback reasons**: Set `spark.comet.logFallbackReasons=true` to log why operators fall back to Spark
3. **Verify protobuf**: Add debug prints in Rust to inspect deserialized operators
4. **Use EXPLAIN**: Run `EXPLAIN EXTENDED` on queries to see the physical plan
