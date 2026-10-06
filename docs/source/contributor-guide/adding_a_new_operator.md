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

#### Local TopK inside a sink

`CometTakeOrderedAndProjectExec` owns local candidate selection, the optional shuffle, and final
selection with offset and projection. When `spark.comet.exec.topK.fusion.enabled` is enabled for an
eligible native Parquet scan, its conversion inserts `CometLocalTopKExec` before native blocks are
serialized. The local node extends `CometUnaryExec` and serializes a bounded sort above the native
scan, so they execute in one block. It preserves the scan's output and partitioning and advertises
the local sort order.

The inserted node keeps the original Spark TopK for plan bookkeeping, but only the outer TopK owns
Spark's offset and projection. Both transition reversion and aggregate-buffer restoration therefore
remove the inserted local node when restoring Spark execution. Restoring its `originalPlan` would
apply the global TopK twice. A single input partition uses a final native limit and projection;
multiple input partitions still require a final TopK after the shuffle.

With `spark.comet.exec.topK.dynamicFilter.enabled`, only the local sort sets the protobuf
`Sort.dynamic_filter_enabled` flag. The native planner wraps an eligible sort in
`TopKReaderFilterExec`. Its permanent plan contains an unexecuted sort template; each execution
creates a fresh sort and live predicate, then attaches that predicate to the Parquet reader when
eligible. The stream owns the heap and predicate until completion, error, or cancellation. The
final TopK after an exchange does not share this state. Reader work stays on scan metrics while
attachment counters belong to the local TopK.

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

### Operators That Should Not Be Converted

Before adding an operator, check that converting it would speed anything up. Comet deliberately
leaves three kinds of Spark plan nodes in place.

**Wrappers and scheduling nodes.** `AdaptiveSparkPlanExec`, the AQE query stages
(`ShuffleQueryStageExec`, `BroadcastQueryStageExec`, `TableCacheQueryStageExec`, and, on Spark 4.0
and later, `ResultQueryStageExec`), `AQEShuffleReadExec`, `InputAdapter`, `WholeStageCodegenExec`,
`ReusedExchangeExec`, and `ReusedSubqueryExec` do no data processing of their own. They schedule
stages, mark whole-stage code generation boundaries, choose which shuffle blocks each task reads,
or point at a plan that runs elsewhere. AQE creates the query stages itself, after Comet's rules
have run on the plan inside them, and depends on their exact class. For example, it casts the root
of the final plan to `ResultQueryStageExec`. When a query stage wraps a Comet shuffle, broadcast,
or cached relation, `CometExecRule` reads from it as a native input through `CometExchangeSink`
and leaves the stage itself in place.

**Operators that run user JVM code on JVM objects.** The typed `Dataset` API plans
`DeserializeToObjectExec`, `SerializeFromObjectExec`, `MapElementsExec`, `MapPartitionsExec`,
`AppendColumnsExec`, `AppendColumnsWithObjectExec`, `MapGroupsExec`, and `CoGroupExec`. They
convert rows to JVM objects, run an arbitrary user function on those objects, or convert them back,
and most of them pass the objects to the next operator as an `ObjectType` column, which has no
Arrow representation. None of this can run natively, so these operators stay on Spark. Every typed
operation ends in `SerializeFromObjectExec`, though, whose output is ordinary rows. With
`spark.comet.convert.typedDataset.enabled`, `CometExecRule` puts a `CometSparkToColumnarExec` above
it, so the operators above the typed operation can run natively. Spark inserts no columnar
transitions below a `RowToColumnarTransition`, so the rule inserts them for the typed operation's
own operators itself. Spark computes a typed operation's rows one at a time, as they are read, while
the conversion fills a whole Arrow batch first. So the rule leaves the output unconverted where a
limit, a `mapPartitions` function, or code reading `Dataset.rdd` could stop reading it early, unless
an operator that reads all of its input first, such as an exchange, a sort, or a hash aggregate,
sits in between. Fusing the deserializer, the `Invoke` that calls the user function, and the
serializer of `Dataset.map` into one projection in the JVM codegen dispatcher was tried in
[#5714](https://github.com/apache/datafusion-comet/pull/5714) and dropped. The dispatcher only calls
into Spark's own classes, and the conversion gets nearly the same speedup for `map` while also
covering the operations that pass the user function an iterator or a whole group. A typed `filter`
is planned as an ordinary `FilterExec`, not as one of these operators.

**Driver-side commands.** `ExecutedCommandExec` runs a `RunnableCommand`, such as DDL or `SET`, on
the driver, so there is no data path for Comet to accelerate.

If a new Spark version adds a wrapper node, do not write a serde for it. Add it to the nodes that
`ExtendedExplainInfo.generateTreeString` skips when counting operators, and to the wrapper list in
[Understanding Comet Plans](../user-guide/latest/understanding-comet-plans.md), so the coverage
summary does not count it as a Spark operator. If `CometExecRule` visits the node, also add it to
the operators it leaves in place without recording a fallback reason. `ExtendedExplainInfo`
already skips every `QueryStageExec`, so a new query stage type needs no change there.

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

Create a new Scala file in `spark/src/main/scala/org/apache/spark/sql/comet/` (e.g., `CometYourOperatorExec.scala`) holding both the `CometOperatorSerde[T]` object, where `T` is the Spark operator type, and the `CometNativeExec` case class it creates. Scan and sink serdes live in `spark/src/main/scala/org/apache/comet/serde/operator/` instead.

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
import com.google.common.base.Objects

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

  override def stringArgs: Iterator[Any] =
    Iterator(output, condition, child)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometFilterExec =>
        this.output == other.output &&
        this.condition == other.condition && this.child == other.child &&
        this.serializedPlanOpt == other.serializedPlanOpt
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Objects.hashCode(output, condition, child)
}
```

#### More Complex Example (Project)

```scala
import com.google.common.base.Objects

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

  override def stringArgs: Iterator[Any] = Iterator(output, projectList, child)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometProjectExec =>
        this.output == other.output &&
        this.projectList == other.projectList &&
        this.child == other.child &&
        this.serializedPlanOpt == other.serializedPlanOpt
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Objects.hashCode(output, projectList, child)

  override protected def outputExpressions: Seq[NamedExpression] = projectList
}
```

#### Plan Identity and Exchange Reuse

Spark's `ReuseExchangeAndSubquery` identifies equivalent plans through canonicalization and may
reuse one exchange for both branches. If `equals` omits a parameter that changes results, different
operators can appear equivalent and silently return the wrong rows. If it includes execution state,
equivalent operators can fail to reuse an exchange.

For operators such as Filter and Project, compare the children, output and every parameter that
affects results in `equals`, and include those semantic fields in `hashCode`. Capture semantic flags
on the Comet operator itself: a value stored only in the protobuf or original Spark plan cannot
participate in this field-based identity. Keep `equals` and `hashCode` consistent: equal operators
must have equal hashes. Do not rely on the default case-class implementations.

The examples above follow the implementations in
[`operators.scala`](https://github.com/apache/datafusion-comet/blob/main/spark/src/main/scala/org/apache/spark/sql/comet/operators.scala):

- `nativeOp` is the protobuf representation and `originalPlan` is the source Spark plan; neither is
  compared by these operators. `CometNativeExec.canonicalizePlans` clears the non-child Spark plan
  references, including their `originalPlan`.
- `serializedPlanOpt` holds the bytes for a native execution block. These operators compare it in
  `equals`, but omit it from `hashCode`; `CometNativeExec.doCanonicalize` clears the block's serialized
  plan. The bytes therefore do not distinguish canonicalized plans.
- `stringArgs` controls the plan's displayed arguments. Show the semantic fields and children
  rather than serialization state; this method does not define equality.

Some operators use a different convention. `CometNativeScanExec` compares `originalPlan` to retain
scan identity, and its `doCanonicalize` canonicalizes that plan while removing unused dynamic
pruning filters. `CometBroadcastExchangeExec` also compares `originalPlan` before canonicalization,
but its `doCanonicalize` clears that reference and retains the canonicalized child. Follow each
operator's equality and canonicalization together rather than copying an exclusion in isolation.

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

`ProjectionBuilder` prunes a DataFusion filter's output for column-only projections, including
empty projections such as the input to `count(*)`. Each required output column is filtered once,
then the projection restores its order, duplicates and aliases. The predicate still sees its
original input schema. Both native plans remain for metrics, including when they share a Spark
plan ID. Computed projections and projections that use every input column are unchanged.

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

For custom operators, you'll need to implement the `ExecutionPlan` trait. Operators that need nothing else from `core` live in the `datafusion-comet-operators` crate under `native/operators/src/`. See `native/operators/src/expand.rs` or `native/core/src/execution/operators/scan.rs` for examples.

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

#### Plan Identity Regression Tests

If the operator has a result-affecting parameter beyond its children and output, add an
exchange-reuse regression. Build two branches that differ in that parameter, verify that both
execute with Comet, and assert the expected distinct results and that `sameResult` is false.
Also test equivalent branches with fresh expression IDs or aliases: `sameResult` should be true,
`semanticHash` values should match, and the executed plan should contain a reused exchange. Cover AQE both enabled and disabled when applicable.

Use the "aggregate canonicalization preserves result expressions and equivalent reuse" tests in
[`CometAggregateSuite`](https://github.com/apache/datafusion-comet/blob/main/spark/src/test/scala/org/apache/comet/exec/CometAggregateSuite.scala)
as a model. Inspect the executed plans: an optimizer rewrite can make the branches differ for an
unrelated reason, allowing a test to pass even when the intended field is missing from `equals`.
Choose inputs and query shapes that preserve the parameter difference, and traverse adaptive and
query-stage wrappers when checking the native operator and reused exchange.

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

Add your operator to the supported operators list in `docs/source/user-guide/latest/compatibility/operators.md` or similar documentation.

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
case s: ShuffleExchangeExec =>
  CometShuffleExchangeExec.shuffleSupported(s) match {
    case Some(CometNativeShuffle) =>
      CometShuffleExchangeExec(s, shuffleType = CometNativeShuffle)
    case Some(CometColumnarShuffle) =>
      CometShuffleExchangeExec(s, shuffleType = CometColumnarShuffle)
    case None => s
  }
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
2. **Check fallback reasons**: Set `spark.comet.explain.fallback.log.enabled=true` to log why operators fall back to Spark
3. **Verify protobuf**: Add debug prints in Rust to inspect deserialized operators
4. **Use EXPLAIN**: Run `EXPLAIN EXTENDED` on queries to see the physical plan
