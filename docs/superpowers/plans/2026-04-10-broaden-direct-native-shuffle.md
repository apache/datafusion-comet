# Broaden Direct Native Shuffle Execution

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Broaden the direct native shuffle optimization to support any native plan with `CometNativeScanExec` or `CometIcebergNativeScanExec` inputs (including multiple inputs like joins), and fix per-partition data injection after the `PlanDataInjector` refactor on main.

**Architecture:** The direct native shuffle optimization composes a child native plan directly with the `ShuffleWriter` operator in a single native execution, avoiding JNI round-trips. Currently it only supports single-source `CometNativeScanExec` plans. After merging main, `CometNativeScanExec` and `CometIcebergNativeScanExec` use split serialization (common data at planning time, per-partition file data injected at execution time via `PlanDataInjector`). The shuffle writer must now carry plan data maps and inject per-partition data before sending the plan to native. The input source check is broadened to accept any combination of `CometNativeScanExec` and `CometIcebergNativeScanExec` inputs.

**Tech Stack:** Scala (Spark plugin), Protobuf (plan serialization)

---

### Task 1: Add per-partition data to DirectNativeExecutionInfo and fix injection in shuffle writer

The core fix: `DirectNativeExecutionInfo` needs to carry per-partition plan data, and the shuffle writer needs to inject it before sending the plan to native. Without this, the native planner fails with `"NativeScan missing file_partition"` since the `PlanDataInjector` refactor moved file data out of `nativeOp`.

**Files:**
- Modify: `spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometShuffleExchangeExec.scala` (DirectNativeExecutionInfo, directNativeExecutionInfo, prepareShuffleDependency)
- Modify: `spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometShuffleDependency.scala`
- Modify: `spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometShuffleManager.scala`
- Modify: `spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometNativeShuffleWriter.scala`

- [ ] **Step 1: Update `DirectNativeExecutionInfo` to carry plan data maps**

In `CometShuffleExchangeExec.scala`, update the case class at line ~961:

```scala
private[shuffle] case class DirectNativeExecutionInfo(
    childNativePlan: Operator,
    numPartitions: Int,
    commonByKey: Map[String, Array[Byte]],
    perPartitionByKey: Map[String, Array[Array[Byte]]])
```

- [ ] **Step 2: Update `directNativeExecutionInfo` to collect plan data**

In `CometShuffleExchangeExec.scala`, update the `directNativeExecutionInfo` lazy val. Replace the section inside `case nativeChild: CometNativeExec =>` (after the subquery check, around lines 135-149) with:

```scala
              // Collect all input sources
              val inputSources = scala.collection.mutable.ArrayBuffer.empty[SparkPlan]
              nativeChild.foreachUntilCometInput(nativeChild)(inputSources += _)

              // Check for subqueries
              val containsSubquery = nativeChild.exists { p =>
                p.expressions.exists(_.exists(_.isInstanceOf[ScalarSubquery]))
              }
              if (containsSubquery) {
                None
              } else {
                // Check that ALL input sources are native scans (file-reading, no JNI)
                val allNativeScans = inputSources.nonEmpty && inputSources.forall {
                  case _: CometNativeScanExec => true
                  case _: CometIcebergNativeScanExec => true
                  case _ => false
                }
                if (allNativeScans) {
                  // Collect per-partition plan data from all native scans
                  val (commonByKey, perPartitionByKey) =
                    nativeChild.findAllPlanData(nativeChild)
                  // All scans must have the same partition count
                  val partitionCounts = perPartitionByKey.values.map(_.length).toSet
                  if (partitionCounts.size <= 1) {
                    val numPartitions = partitionCounts.headOption.getOrElse(0)
                    Some(DirectNativeExecutionInfo(
                      nativeChild.nativeOp, numPartitions, commonByKey, perPartitionByKey))
                  } else {
                    None // Partition count mismatch across scans
                  }
                } else {
                  None
                }
              }
```

Note: `findAllPlanData` is currently private. It needs to be made package-private or have a public accessor. See step 3.

- [ ] **Step 3: Make `findAllPlanData` accessible from `CometShuffleExchangeExec`**

In `operators.scala`, change the visibility of `findAllPlanData` (line ~664) from `private` to `private[comet]`:

```scala
  private[comet] def findAllPlanData(
      plan: SparkPlan): (Map[String, Array[Byte]], Map[String, Array[Array[Byte]]]) = {
```

- [ ] **Step 4: Update `prepareShuffleDependency` to pass plan data**

In `CometShuffleExchangeExec.scala`, update the `prepareShuffleDependency` signature (line ~672) to accept plan data:

```scala
  def prepareShuffleDependency(
      rdd: RDD[ColumnarBatch],
      outputAttributes: Seq[Attribute],
      outputPartitioning: Partitioning,
      serializer: Serializer,
      metrics: Map[String, SQLMetric],
      childNativePlan: Option[Operator] = None,
      commonByKey: Map[String, Array[Byte]] = Map.empty,
      perPartitionByKey: Map[String, Array[Array[Byte]]] = Map.empty)
      : ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
```

Update the `CometShuffleDependency` construction (line ~733) to pass the new fields:

```scala
    val dependency = new CometShuffleDependency[Int, ColumnarBatch, ColumnarBatch](
      rdd.map((0, _)),
      serializer = serializer,
      shuffleWriterProcessor = ShuffleExchangeExec.createShuffleWriteProcessor(metrics),
      shuffleType = CometNativeShuffle,
      partitioner = partitioner,
      decodeTime = metrics("decode_time"),
      outputPartitioning = Some(outputPartitioning),
      outputAttributes = outputAttributes,
      shuffleWriteMetrics = metrics,
      numParts = numParts,
      rangePartitionBounds = rangePartitionBounds,
      childNativePlan = childNativePlan,
      commonByKey = commonByKey,
      perPartitionByKey = perPartitionByKey)
```

Update the call site in `shuffleDependency` (line ~227) to pass the data:

```scala
      val dep = CometShuffleExchangeExec.prepareShuffleDependency(
        inputRDD.asInstanceOf[RDD[ColumnarBatch]],
        child.output,
        outputPartitioning,
        serializer,
        metrics,
        directNativeExecutionInfo.map(_.childNativePlan),
        directNativeExecutionInfo.map(_.commonByKey).getOrElse(Map.empty),
        directNativeExecutionInfo.map(_.perPartitionByKey).getOrElse(Map.empty))
```

- [ ] **Step 5: Add plan data fields to `CometShuffleDependency`**

In `CometShuffleDependency.scala`, add after the `childNativePlan` field (line ~56):

```scala
    val childNativePlan: Option[Operator] = None,
    val commonByKey: Map[String, Array[Byte]] = Map.empty,
    @transient val perPartitionByKey: Map[String, Array[Array[Byte]]] = Map.empty)
```

Note: `perPartitionByKey` is `@transient` because the per-partition arrays are large and should not be serialized with the dependency to every task. Instead, each task will receive only its partition's data (see step 7).

- [ ] **Step 6: Update `CometShuffleManager` to pass plan data to writer**

In `CometShuffleManager.scala`, update the `CometNativeShuffleWriter` construction (line ~232):

```scala
      case cometShuffleHandle: CometNativeShuffleHandle[K @unchecked, V @unchecked] =>
        val dep = cometShuffleHandle.dependency.asInstanceOf[CometShuffleDependency[_, _, _]]
        new CometNativeShuffleWriter(
          dep.outputPartitioning.get,
          dep.outputAttributes,
          dep.shuffleWriteMetrics,
          dep.numParts,
          dep.shuffleId,
          mapId,
          context,
          metrics,
          dep.rangePartitionBounds,
          dep.childNativePlan,
          dep.commonByKey,
          dep.perPartitionByKey)
```

- [ ] **Step 7: Update `CometNativeShuffleWriter` to inject per-partition data**

In `CometNativeShuffleWriter.scala`, add the new parameters to the constructor (after `childNativePlan`):

```scala
    childNativePlan: Option[Operator] = None,
    commonByKey: Map[String, Array[Byte]] = Map.empty,
    perPartitionByKey: Map[String, Array[Array[Byte]]] = Map.empty)
```

Add an import at the top of the file:

```scala
import org.apache.spark.sql.comet.PlanDataInjector
```

In the `write()` method, after `val nativePlan = getNativePlan(...)` (line ~82), inject per-partition data before serializing:

```scala
    val nativePlan = getNativePlan(tempDataFilename, tempIndexFilename)

    // Inject per-partition file data if this is a direct native execution plan
    val actualPlan = if (commonByKey.nonEmpty && perPartitionByKey.nonEmpty) {
      val partitionIdx = context.partitionId()
      val partitionByKey = perPartitionByKey.map { case (key, arr) =>
        key -> arr(partitionIdx)
      }
      val injected = PlanDataInjector.injectPlanData(nativePlan, commonByKey, partitionByKey)
      CometExec.serializeNativePlan(injected)
    } else {
      CometExec.serializeNativePlan(nativePlan)
    }
```

Then change the `getCometIterator` call to use the pre-serialized plan (use the `Array[Byte]` overload):

```scala
    val cometIter = CometExec.getCometIterator(
      Seq(newInputs.asInstanceOf[Iterator[ColumnarBatch]]),
      outputAttributes.length,
      actualPlan,
      numParts,
      context.partitionId())
```

- [ ] **Step 8: Handle `@transient perPartitionByKey` serialization**

Since `perPartitionByKey` is `@transient` on `CometShuffleDependency`, it won't be available on executors. We need a different approach: store the per-partition data in the RDD partitions (like `CometExecRDD` does).

Instead of `@transient`, keep `perPartitionByKey` non-transient but pass it through to the shuffle writer via a broadcast or by embedding each partition's data in the shuffle handle.

Actually, the simpler approach: the `CometNativeShuffleWriter` runs on the executor. The shuffle dependency is serialized to executors. So `commonByKey` (small) and `perPartitionByKey` (array of arrays) need to survive serialization.

Remove `@transient` from `perPartitionByKey` in `CometShuffleDependency.scala` — the data is already partitioned arrays of byte arrays, which are serializable. The total size is proportional to the number of file splits, which is manageable.

```scala
    val childNativePlan: Option[Operator] = None,
    val commonByKey: Map[String, Array[Byte]] = Map.empty,
    val perPartitionByKey: Map[String, Array[Array[Byte]]] = Map.empty)
```

- [ ] **Step 9: Build and verify compilation**

Run: `make core && ./mvnw compile -DskipTests`
Expected: Successful compilation with no errors.

- [ ] **Step 10: Run existing direct native shuffle tests**

Run: `./mvnw test -Dsuites="org.apache.comet.exec.CometDirectNativeShuffleSuite" -Dtest=none`
Expected: All existing tests pass (they test `CometNativeScanExec` which should now work correctly with per-partition injection).

- [ ] **Step 11: Commit**

```bash
git add spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometShuffleExchangeExec.scala \
  spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometShuffleDependency.scala \
  spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometShuffleManager.scala \
  spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometNativeShuffleWriter.scala \
  spark/src/main/scala/org/apache/spark/sql/comet/operators.scala
git commit -m "fix: add per-partition data injection to direct native shuffle and broaden to support any native scan inputs"
```

---

### Task 2: Update config description and comments

**Files:**
- Modify: `common/src/main/scala/org/apache/comet/CometConf.scala`
- Modify: `spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometNativeShuffleWriter.scala`

- [ ] **Step 1: Update config doc string**

In `CometConf.scala`, update the `COMET_SHUFFLE_DIRECT_NATIVE_ENABLED` doc (line ~348):

```scala
  val COMET_SHUFFLE_DIRECT_NATIVE_ENABLED: ConfigEntry[Boolean] =
    conf(s"$COMET_EXEC_CONFIG_PREFIX.shuffle.directNative.enabled")
      .category(CATEGORY_SHUFFLE)
      .doc(
        "When enabled, the native shuffle writer will directly execute the child native plan " +
          "instead of reading intermediate batches via JNI. This optimization avoids the " +
          "JNI round-trip for native plans whose inputs are all native scans " +
          "(CometNativeScanExec, CometIcebergNativeScanExec). Supports single and multi-source " +
          "plans (e.g., joins over native scans). " +
          "This is an experimental feature and is disabled by default.")
      .internal()
      .booleanConf
      .createWithDefault(false)
```

- [ ] **Step 2: Update shuffle writer class doc**

In `CometNativeShuffleWriter.scala`, update the class doc (line ~44):

```scala
/**
 * A [[ShuffleWriter]] that will delegate shuffle write to native shuffle.
 *
 * @param childNativePlan
 *   When provided, the shuffle writer will execute this native plan directly and pipe its output
 *   to the ShuffleWriter, avoiding the JNI round-trip for intermediate batches. Used when all
 *   input sources are native scans (CometNativeScanExec, CometIcebergNativeScanExec).
 * @param commonByKey
 *   Common planning data (schemas, filters) keyed by source identifier, for PlanDataInjector.
 * @param perPartitionByKey
 *   Per-partition planning data (file lists) keyed by source identifier, for PlanDataInjector.
 */
```

- [ ] **Step 3: Commit**

```bash
git add common/src/main/scala/org/apache/comet/CometConf.scala \
  spark/src/main/scala/org/apache/spark/sql/comet/execution/shuffle/CometNativeShuffleWriter.scala
git commit -m "docs: update config and class docs for broadened direct native shuffle"
```

---

### Task 3: Add tests for broadened direct native shuffle

**Files:**
- Modify: `spark/src/test/scala/org/apache/comet/exec/CometDirectNativeShuffleSuite.scala`

- [ ] **Step 1: Add test for Iceberg native scan**

Add a test that exercises `CometIcebergNativeScanExec` with direct native shuffle. This requires an Iceberg table setup. Check if the existing test infrastructure supports Iceberg tables (look at existing Iceberg test suites for patterns).

If the Iceberg test infrastructure is not available in this suite (it likely requires a separate test harness with Iceberg catalog), add a comment-only placeholder and focus on the multi-source test:

```scala
  // TODO: Add Iceberg native scan test when Iceberg test infrastructure is available
  // in this suite. CometIcebergNativeScanExec is supported by the optimization but
  // requires SparkCatalog setup. See CometIcebergSuite for patterns.
```

- [ ] **Step 2: Add test verifying multi-source native scans are accepted**

```scala
  test("direct native execution: join of two native scans") {
    withParquetTable((0 until 100).map(i => (i, s"left_$i")), "left_tbl") {
      withParquetTable((0 until 100).map(i => (i, s"right_$i")), "right_tbl") {
        // Broadcast join with two native scans
        // The join itself may or may not use direct native execution depending on
        // whether broadcast creates a non-native-scan input, but the query should
        // execute correctly regardless
        val df = sql("""
            |SELECT l._1, l._2, r._2
            |FROM left_tbl l JOIN right_tbl r ON l._1 = r._1
            |WHERE l._1 > 50
            |""".stripMargin)
          .repartition(10, col("_1"))

        checkSparkAnswer(df)
      }
    }
  }
```

- [ ] **Step 3: Add test verifying non-native-scan inputs fall back**

```scala
  test("direct native execution disabled: shuffle input source") {
    withParquetTable((0 until 100).map(i => (i, (i + 1).toLong)), "tbl") {
      // Force a shuffle before the final shuffle by using a repartition + aggregation
      // The second shuffle reads from the first shuffle's output (not a native scan)
      val df = sql("SELECT _1, SUM(_2) as s FROM tbl GROUP BY _1")
        .repartition(5, col("_1"))
        .filter(col("s") > 10)
        .repartition(3, col("_1"))

      // The final shuffle should NOT use direct native execution because its input
      // comes from a shuffle read, not a native scan
      checkSparkAnswer(df)
    }
  }
```

- [ ] **Step 4: Run all tests**

Run: `./mvnw test -Dsuites="org.apache.comet.exec.CometDirectNativeShuffleSuite" -Dtest=none`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```bash
git add spark/src/test/scala/org/apache/comet/exec/CometDirectNativeShuffleSuite.scala
git commit -m "test: add tests for broadened direct native shuffle (multi-source, fallback)"
```
