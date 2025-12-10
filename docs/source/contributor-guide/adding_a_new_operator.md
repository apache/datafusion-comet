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

`CometExecRule` is responsible for replacing Spark operators with Comet operators. There are different approaches to
implementing Comet operators depending on where they execute and how they integrate with the native execution engine.

### Types of Comet Operators

`CometExecRule` maintains two distinct maps of operators:

#### 1. Native Operators (`nativeExecs` map)

These operators run entirely in native Rust code and are the primary way to accelerate Spark workloads. Native
operators are registered in the `nativeExecs` map in `CometExecRule.scala`.

Key characteristics of native operators:

- They are converted to their corresponding native protobuf representation
- They execute as DataFusion operators in the native engine
- The `CometOperatorSerde` implementation handles enable/disable checks, support validation, and protobuf serialization

Examples: `ProjectExec`, `FilterExec`, `SortExec`, `HashAggregateExec`, `SortMergeJoinExec`, `ExpandExec`, `WindowExec`

#### 2. Sink Operators (`sinks` map)

Sink operators serve as entry points (data sources) for native execution blocks. They are registered in the `sinks`
map in `CometExecRule.scala`.

Key characteristics of sinks:

- They become `ScanExec` operators in the native plan (see `operator2Proto` in `CometExecRule.scala`)
- They can be leaf nodes that feed data into native execution blocks
- They are wrapped with `CometScanWrapper` or `CometSinkPlaceHolder` during plan transformation
- Examples include operators that bring data from various sources into native execution

Examples: `UnionExec`, `CoalesceExec`, `CollectLimitExec`, `TakeOrderedAndProjectExec`

Special sinks (not in the `sinks` map but also treated as sinks):

- `CometScanExec` - File scans
- `CometSparkToColumnarExec` - Conversion from Spark row format
- `ShuffleExchangeExec` / `BroadcastExchangeExec` - Exchange operators

#### 3. Comet JVM Operators

These operators run in the JVM but are part of the Comet execution path. For JVM operators, all checks happen
in `CometExecRule` rather than using `CometOperatorSerde`, because they don't need protobuf serialization.

Examples: `CometBroadcastExchangeExec`, `CometShuffleExchangeExec`

### Choosing the Right Operator Type

When adding a new operator, choose based on these criteria:

**Use Native Operators when:**

- The operator transforms data (e.g., project, filter, sort, aggregate, join)
- The operator has a direct DataFusion equivalent or custom implementation
- The operator consumes native child operators and produces native output
- The operator is in the middle of an execution pipeline

**Use Sink Operators when:**

- The operator serves as a data source for native execution (becomes a `ScanExec`)
- The operator brings data from non-native sources (e.g., `UnionExec` combining multiple inputs)
- The operator is typically a leaf or near-leaf node in the execution tree
- The operator needs special handling to interface with the native engine

**Implementation Note for Sinks:**

Sink operators are handled specially in `CometExecRule.operator2Proto`. Instead of converting to their own operator
type, they are converted to `ScanExec` in the native plan. This allows them to serve as entry points for native
execution blocks. The original Spark operator is wrapped with `CometScanWrapper` or `CometSinkPlaceHolder` which
manages the boundary between JVM and native execution.

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

The `CometOperatorSerde` trait provides several key methods:

- `enabledConfig: Option[ConfigEntry[Boolean]]` - Configuration to enable/disable this operator
- `getSupportLevel(operator: T): SupportLevel` - Determines if the operator is supported
- `convert(op: T, builder: Operator.Builder, childOp: Operator*): Option[Operator]` - Converts to protobuf
- `createExec(nativeOp: Operator, op: T): CometNativeExec` - Creates the Comet execution operator wrapper

The validation workflow in `CometExecRule.isOperatorEnabled`:

1. Checks if the operator is enabled via `enabledConfig`
2. Calls `getSupportLevel()` to determine compatibility
3. Handles Compatible/Incompatible/Unsupported cases with appropriate fallback messages

#### Simple Example (Filter)

```scala
object CometFilterExec extends CometOperatorSerde[FilterExec] {

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

  override def createExec(nativeOp: Operator, op: FilterExec): CometNativeExec = {
    CometFilterExec(nativeOp, op, op.output, op.condition, op.child, SerializedPlan(None))
  }
}

case class CometFilterExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    override val output: Seq[Attribute],
    condition: Expression,
    child: SparkPlan,
    override val serializedPlanOpt: SerializedPlan)
    extends CometUnaryExec {

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)
}
```

#### More Complex Example (Project)

```scala
object CometProjectExec extends CometOperatorSerde[ProjectExec] {

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

  override def createExec(nativeOp: Operator, op: ProjectExec): CometNativeExec = {
    CometProjectExec(nativeOp, op, op.output, op.projectList, op.child, SerializedPlan(None))
  }
}

case class CometProjectExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    override val output: Seq[Attribute],
    projectList: Seq[NamedExpression],
    child: SparkPlan,
    override val serializedPlanOpt: SerializedPlan)
    extends CometUnaryExec
    with PartitioningPreservingUnaryExecNode {

  override def producedAttributes: AttributeSet = outputSet

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)
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

Note that Comet will treat an operator as incompatible if any of the child expressions are incompatible.

### Step 3: Register the Operator

Add your operator to the appropriate map in `CometExecRule.scala`:

#### For Native Operators

Add to the `nativeExecs` map (`CometExecRule.scala`):

```scala
val nativeExecs: Map[Class[_ <: SparkPlan], CometOperatorSerde[_]] =
  Map(
    classOf[ProjectExec] -> CometProjectExec,
    classOf[FilterExec] -> CometFilterExec,
    // ... existing operators ...
    classOf[YourOperatorExec] -> CometYourOperator,
  )
```

#### For Sink Operators

If your operator is a sink (becomes a `ScanExec` in the native plan), add to the `sinks` map (`CometExecRule.scala`):

```scala
val sinks: Map[Class[_ <: SparkPlan], CometOperatorSerde[_]] =
  Map(
    classOf[CoalesceExec] -> CometCoalesceExec,
    classOf[UnionExec] -> CometUnionExec,
    // ... existing operators ...
    classOf[YourSinkOperatorExec] -> CometYourSinkOperator,
  )
```

Note: The `allExecs` map automatically combines both `nativeExecs` and `sinks`, so you only need to add to one of the two maps.

### Step 4: Add Configuration Entry

Add a configuration entry in `common/src/main/scala/org/apache/comet/CometConf.scala`:

```scala
val COMET_EXEC_YOUR_OPERATOR_ENABLED: ConfigEntry[Boolean] =
  conf("spark.comet.exec.yourOperator.enabled")
    .doc("Whether to enable your operator in Comet")
    .booleanConf
    .createWithDefault(true)
```

Run `make` to update the user guide. The new configuration option will be added to `docs/source/user-guide/latest/configs.md`.

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

## Implementing a Sink Operator

Sink operators are converted to `ScanExec` in the native plan and serve as entry points for native execution. The implementation is simpler than native operators because sink operators extend the `CometSink` base class which provides the conversion logic.

### Step 1: Create a CometOperatorSerde Implementation

Create a new Scala file in `spark/src/main/scala/org/apache/spark/sql/comet/` (e.g., `CometYourSinkOperator.scala`):

```scala
import org.apache.comet.serde.operator.CometSink

object CometYourSinkOperator extends CometSink[YourSinkExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_EXEC_YOUR_SINK_ENABLED)

  // Optional: Override if the data produced is FFI safe
  override def isFfiSafe: Boolean = false

  override def createExec(
      nativeOp: OperatorOuterClass.Operator,
      op: YourSinkExec): CometNativeExec = {
    CometSinkPlaceHolder(
      nativeOp,
      op,
      CometYourSinkExec(op, op.output, /* other parameters */, op.child))
  }

  // Optional: Override getSupportLevel if you need custom validation beyond data types
  override def getSupportLevel(operator: YourSinkExec): SupportLevel = {
    // CometSink base class already checks data types in convert()
    // Add any additional validation here
    Compatible()
  }
}

/**
 * Comet implementation of YourSinkExec that supports columnar processing
 */
case class CometYourSinkExec(
    override val originalPlan: SparkPlan,
    override val output: Seq[Attribute],
    /* other parameters */,
    child: SparkPlan)
    extends CometExec
    with UnaryExecNode {

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    // Implement columnar execution logic
    val rdd = child.executeColumnar()
    // Apply your sink operator's logic
    rdd
  }

  override def outputPartitioning: Partitioning = {
    // Define output partitioning
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)
}
```

**Key Points:**

- Extend `CometSink[T]` which provides the `convert()` method that transforms the operator to `ScanExec`
- The `CometSink.convert()` method (in `CometSink.scala`) automatically handles:
  - Data type validation
  - Conversion to `ScanExec` in the native plan
  - Setting FFI safety flags
- You must implement `createExec()` to wrap the operator appropriately
- You typically need to create a corresponding `CometYourSinkExec` class that implements columnar execution

### Step 2: Register the Sink

Add your sink to the `sinks` map in `CometExecRule.scala`:

```scala
val sinks: Map[Class[_ <: SparkPlan], CometOperatorSerde[_]] =
  Map(
    classOf[CoalesceExec] -> CometCoalesceExec,
    classOf[UnionExec] -> CometUnionExec,
    classOf[YourSinkExec] -> CometYourSinkOperator,
  )
```

### Step 3: Add Configuration

Add a configuration entry in `CometConf.scala`:

```scala
val COMET_EXEC_YOUR_SINK_ENABLED: ConfigEntry[Boolean] =
  conf("spark.comet.exec.yourSink.enabled")
    .doc("Whether to enable your sink operator in Comet")
    .booleanConf
    .createWithDefault(true)
```

### Step 4: Add Tests

Test that your sink operator correctly feeds data into native execution:

```scala
test("your sink operator") {
  withTable("test_table") {
    sql("CREATE TABLE test_table(col1 INT, col2 STRING) USING parquet")
    sql("INSERT INTO test_table VALUES (1, 'a'), (2, 'b')")

    // Test query that uses your sink operator followed by native operators
    checkSparkAnswerAndOperator(
      "SELECT col1 + 1 FROM (/* query that produces YourSinkExec */)"
    )
  }
}
```

**Important Notes for Sinks:**

- Sinks extend the `CometSink` base class, which provides the `convert()` method implementation
- The `CometSink.convert()` method automatically handles conversion to `ScanExec` in the native plan
- You don't need to add protobuf definitions for sink operators - they use the standard `Scan` message
- You don't need Rust implementation for sinks - they become standard `ScanExec` operators that read from the JVM
- Sink implementations should provide a columnar-compatible execution class (e.g., `CometCoalesceExec`)
- The `createExec()` method wraps the operator with `CometSinkPlaceHolder` to manage the JVM-to-native boundary
- See `CometCoalesceExec.scala` or `CometUnionExec` in `spark/src/main/scala/org/apache/spark/sql/comet/` for reference implementations

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

1. **Enable verbose logging**: Set `spark.comet.explain.format=verbose` to see detailed plan transformations
2. **Check fallback reasons**: Set `spark.comet.logFallbackReasons.enabled=true` to log why operators fall back to Spark
3. **Verify protobuf**: Add debug prints in Rust to inspect deserialized operators
4. **Use EXPLAIN**: Run `EXPLAIN EXTENDED` on queries to see the physical plan
