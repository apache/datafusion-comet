/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.comet

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.comet.execution.shuffle.CometShuffledBatchRDD
import org.apache.spark.sql.execution.ScalarSubquery
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

import org.apache.comet.CometExecIterator
import org.apache.comet.serde.OperatorOuterClass

/**
 * Partition that carries per-partition planning data, avoiding closure capture of all partitions.
 */
private[spark] class CometExecPartition(
    override val index: Int,
    val inputPartitions: Array[Partition],
    val planDataByKey: Map[String, Array[Byte]])
    extends Partition

/**
 * Unified RDD for Comet native execution.
 *
 * Solves the closure capture problem: instead of capturing all partitions' data in the closure
 * (which gets serialized to every task), each Partition object carries only its own data.
 *
 * Handles three cases:
 *   - With inputs + per-partition data: injects planning data into operator tree
 *   - With inputs + no per-partition data: just zips inputs (no injection overhead)
 *   - No inputs: uses numPartitions to create partitions
 *
 * NOTE: This RDD does not handle DPP (InSubqueryExec), which is resolved in
 * CometIcebergNativeScanExec.serializedPartitionData before this RDD is created. It also handles
 * ScalarSubquery expressions by registering them with CometScalarSubquery before execution.
 */
private[spark] class CometExecRDD(
    sc: SparkContext,
    var inputRDDs: Seq[RDD[ColumnarBatch]],
    commonByKey: Map[String, Array[Byte]],
    @transient perPartitionByKey: Map[String, Array[Array[Byte]]],
    serializedPlan: Array[Byte],
    defaultNumPartitions: Int,
    numOutputCols: Int,
    nativeMetrics: CometMetricNode,
    subqueries: Seq[ScalarSubquery],
    broadcastedHadoopConfForEncryption: Option[Broadcast[SerializableConfiguration]] = None,
    encryptedFilePaths: Seq[String] = Seq.empty,
    shuffleScanIndices: Set[Int] = Set.empty)
    extends RDD[ColumnarBatch](sc, inputRDDs.map(rdd => new OneToOneDependency(rdd))) {

  // Determine partition count: from inputs if available, otherwise from parameter
  private val numPartitions: Int = if (inputRDDs.nonEmpty) {
    inputRDDs.head.partitions.length
  } else if (perPartitionByKey.nonEmpty) {
    perPartitionByKey.values.head.length
  } else {
    defaultNumPartitions
  }

  // Validate all per-partition arrays have the same length to prevent
  // ArrayIndexOutOfBoundsException in getPartitions (e.g., from broadcast scans with
  // different partition counts after DPP filtering)
  require(
    perPartitionByKey.values.forall(_.length == numPartitions),
    s"All per-partition arrays must have length $numPartitions, but found: " +
      perPartitionByKey.map { case (key, arr) => s"$key -> ${arr.length}" }.mkString(", "))

  override protected def getPartitions: Array[Partition] = {
    (0 until numPartitions).map { idx =>
      val inputParts = inputRDDs.map(_.partitions(idx)).toArray
      val planData = perPartitionByKey.map { case (key, arr) => key -> arr(idx) }
      new CometExecPartition(idx, inputParts, planData)
    }.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val partition = split.asInstanceOf[CometExecPartition]

    val inputs = inputRDDs.zip(partition.inputPartitions).map { case (rdd, part) =>
      rdd.iterator(part, context)
    }

    // Only inject if we have per-partition planning data
    val actualPlan = if (commonByKey.nonEmpty) {
      val basePlan = OperatorOuterClass.Operator.parseFrom(serializedPlan)
      val injected =
        PlanDataInjector.injectPlanData(basePlan, commonByKey, partition.planDataByKey)
      PlanDataInjector.serializeOperator(injected)
    } else {
      serializedPlan
    }

    // Invoke registered per-partition metadata handlers. This is the SPI hook contribs
    // use to populate executor thread-locals (e.g. `InputFileBlockHolder`) from their
    // serialized per-partition payloads before the native iterator runs. The Delta
    // contrib uses it so `input_file_name()` and Delta's `_metadata.file_path` resolve
    // correctly; without this, UPDATE/DELETE/MERGE/CDC paths see empty file_path and
    // throw `DELTA_FILE_TO_OVERWRITE_NOT_FOUND`. Handlers are called for every
    // partition with non-empty plan data and are expected to no-op when the partition
    // doesn't carry their proto. The registered handlers list is a `@volatile` read.
    CometExecRDD.runPartitionMetadataHandlers(partition.planDataByKey, context)

    // Create shuffle block iterators for inputs that are CometShuffledBatchRDD
    val shuffleBlockIters = shuffleScanIndices.flatMap { idx =>
      inputRDDs(idx) match {
        case rdd: CometShuffledBatchRDD =>
          Some(idx -> rdd.computeAsShuffleBlockIterator(partition.inputPartitions(idx), context))
        case _ => None
      }
    }.toMap

    val it = new CometExecIterator(
      CometExec.newIterId,
      inputs,
      numOutputCols,
      actualPlan,
      nativeMetrics,
      numPartitions,
      partition.index,
      broadcastedHadoopConfForEncryption,
      encryptedFilePaths,
      shuffleBlockIters)

    // Register ScalarSubqueries so native code can look them up
    subqueries.foreach(sub => CometScalarSubquery.setSubquery(it.id, sub))

    Option(context).foreach { ctx =>
      ctx.addTaskCompletionListener[Unit] { _ =>
        subqueries.foreach(sub => CometScalarSubquery.removeSubquery(it.id, sub))
      }
    }

    it
  }

  // Duplicates logic from Spark's ZippedPartitionsBaseRDD.getPreferredLocations
  override def getPreferredLocations(split: Partition): Seq[String] = {
    if (inputRDDs == null || inputRDDs.isEmpty) return Nil

    val idx = split.index
    val prefs = inputRDDs.map(rdd => rdd.preferredLocations(rdd.partitions(idx)))
    // Prefer nodes where all inputs are local; fall back to any input's preferred location
    val intersection = prefs.reduce((a, b) => a.intersect(b))
    if (intersection.nonEmpty) intersection else prefs.flatten.distinct
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    inputRDDs = null
  }
}

object CometExecRDD {

  /**
   * SPI hook signature: a callback contribs register to inspect a partition's per-partition
   * planning data BEFORE the native iterator starts producing rows on this task. Receives the
   * `Map[String, Array[Byte]]` of serialized per-partition payloads keyed by `sourceKey` (the
   * same shape contribs serialize into `perPartitionByKey` at planning time). Plus the active
   * `TaskContext` so handlers can register completion listeners.
   *
   * Canonical use: the Delta contrib reads its `DeltaScan` payload, extracts the AddFile path,
   * and calls `InputFileBlockHolder.set` so `input_file_name()` and Delta's `_metadata.file_path`
   * resolve to the file being read (otherwise UPDATE/DELETE/MERGE/CDC throw
   * `DELTA_FILE_TO_OVERWRITE_NOT_FOUND`).
   *
   * The signature is deliberately the data shape, NOT a Spark-internal partition type, so contribs
   * don't have to live under `org.apache.spark.*` to see it. Handlers MUST:
   *   - be stateless and free of contrib-specific assumptions on partitions that don't carry
   *     their proto (no-op silently when their expected key/payload shape isn't present),
   *   - register a task-completion listener for any thread-local they set, so the value is
   *     cleared at the end of the task, and
   *   - tolerate parse failures defensively -- another contrib may own this key.
   */
  type PartitionMetadataHandler = (Map[String, Array[Byte]], TaskContext) => Unit

  @volatile private var partitionMetadataHandlers: Vector[PartitionMetadataHandler] = Vector.empty

  /**
   * Register a per-partition metadata handler. Called once per contrib at extension-load
   * time (from `CometOperatorSerdeExtension.init`). Registration is idempotent on the
   * same function reference but does not de-duplicate equivalent lambdas; contribs are
   * expected to register exactly once.
   */
  def registerPartitionMetadataHandler(h: PartitionMetadataHandler): Unit = synchronized {
    if (!partitionMetadataHandlers.contains(h)) {
      partitionMetadataHandlers = partitionMetadataHandlers :+ h
    }
  }

  /**
   * Test-only / contrib reset. Visibility is `public` to mirror `resetForTesting` on the registry.
   */
  def clearPartitionMetadataHandlers(): Unit = synchronized {
    partitionMetadataHandlers = Vector.empty
  }

  private[comet] def runPartitionMetadataHandlers(
      planDataByKey: Map[String, Array[Byte]],
      context: TaskContext): Unit = {
    val hs = partitionMetadataHandlers
    if (hs.nonEmpty) hs.foreach(_(planDataByKey, context))
  }

  /**
   * Creates an RDD for native execution with optional per-partition planning data.
   */
  // scalastyle:off
  def apply(
      sc: SparkContext,
      inputRDDs: Seq[RDD[ColumnarBatch]],
      commonByKey: Map[String, Array[Byte]],
      perPartitionByKey: Map[String, Array[Array[Byte]]],
      serializedPlan: Array[Byte],
      numPartitions: Int,
      numOutputCols: Int,
      nativeMetrics: CometMetricNode,
      subqueries: Seq[ScalarSubquery],
      broadcastedHadoopConfForEncryption: Option[Broadcast[SerializableConfiguration]] = None,
      encryptedFilePaths: Seq[String] = Seq.empty,
      shuffleScanIndices: Set[Int] = Set.empty): CometExecRDD = {
    // scalastyle:on

    new CometExecRDD(
      sc,
      inputRDDs,
      commonByKey,
      perPartitionByKey,
      serializedPlan,
      numPartitions,
      numOutputCols,
      nativeMetrics,
      subqueries,
      broadcastedHadoopConfForEncryption,
      encryptedFilePaths,
      shuffleScanIndices)
  }
}
