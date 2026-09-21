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

package org.apache.spark.sql.comet.execution.shuffle

import org.apache.spark._
import org.apache.spark.rdd.{DeterministicLevel, RDD}
import org.apache.spark.sql.comet.{CometExecRDD, CometMetricNode}
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.CometShuffleBlockIterator

/**
 * Thin scheduling-anchor RDD for the native-shuffle path. Declares `OneToOneDependency` on each
 * leaf input RDD (so the DAGScheduler triggers prior stages, broadcasts, etc.) and resolves the
 * per-partition native input slots in `compute`, packaged into a
 * [[CometNativeShuffleInputIterator]]. The iterator reports `hasNext = false`;
 * [[CometNativeShuffleWriter]] downcasts it and reads those slots directly to drive the unified
 * `ShuffleWriter(child = childNativeOp)` plan.
 *
 * @param positionalRoundRobin
 *   whether the writer fed by this RDD will place rows by position instead of by content; see
 *   [[CometShuffleExchangeExec.usesPositionalRoundRobin]] and `getOutputDeterministicLevel`.
 */
private[shuffle] class CometNativeShuffleInputRDD(
    sc: SparkContext,
    var inputRDDs: Seq[RDD[_]],
    numPartitionsParam: Int,
    shuffleScanIndices: Set[Int],
    spillMetricNode: CometMetricNode,
    @transient perPartitionByKey: Map[String, Array[Array[Byte]]] = Map.empty,
    positionalRoundRobin: Boolean = false)
    extends RDD[Product2[Int, ColumnarBatch]](
      sc,
      inputRDDs.map(rdd => new OneToOneDependency(rdd))) {

  /**
   * Give local fallback its own scheduling RDD over the original upstream inputs. Spark aborts
   * jobs whose RDD ancestry contains the failed stage's RDD, so reusing this instance or wrapping
   * it in a narrow dependency lets a late remote failure abort the local replacement as well.
   */
  private[shuffle] def copyForLocalShuffle(): CometNativeShuffleInputRDD =
    new CometNativeShuffleInputRDD(
      context,
      inputRDDs,
      numPartitionsParam,
      shuffleScanIndices,
      spillMetricNode,
      perPartitionByKey,
      positionalRoundRobin)

  /**
   * Batch-granular round robin assigns each Arrow batch to an output partition by position, so
   * the placement of a row depends on the order and the framing of the batches the upstream
   * operator produced, not on the row itself. Re-running one map task against a differently
   * ordered or differently framed input therefore sends rows somewhere else, which duplicates and
   * drops rows once any of the replaced output has already been fetched.
   *
   * Spark states that risk declaratively rather than defending against it per-operator: a
   * round-robin repartition is wrapped in a `MapPartitionsRDD` with `isOrderSensitive = true`
   * (Comet's own JVM path does this in `prepareJVMShuffleDependency`), and that RDD reports
   * `INDETERMINATE` whenever its parent is `UNORDERED`. The DAGScheduler then rolls the whole
   * stage back, or aborts the job when a result stage has already consumed output, instead of
   * re-running a single task. The native path has no `MapPartitionsRDD` to carry the flag, so
   * apply the same rule here.
   *
   * The parent level does the discriminating, exactly as it does for Spark. A plain scan is
   * `DETERMINATE`: it replays identically, so positional assignment is reproducible and ordinary
   * per-task retry stays cheap. Anything below another exchange is `UNORDERED`, because reduce
   * tasks see shuffle blocks in arrival order, and that is when positional assignment stops being
   * reproducible.
   *
   * Content-hash round robin (the default) needs none of this: it places rows by hash, so its
   * output is a pure function of the rows regardless of how they arrive.
   */
  override protected def getOutputDeterministicLevel: DeterministicLevel.Value = {
    val inheritedLevel = super.getOutputDeterministicLevel
    if (positionalRoundRobin && inheritedLevel != DeterministicLevel.DETERMINATE) {
      DeterministicLevel.INDETERMINATE
    } else {
      inheritedLevel
    }
  }

  override protected def getPartitions: Array[Partition] =
    (0 until numPartitionsParam).map { i =>
      // Resolve leaf-RDD partitions on the driver here (where their @transient fields are still
      // populated). Stashing them on the partition lets `compute` avoid touching
      // `leafRdd.partitions` on the executor, which would otherwise trigger getPartitions and
      // hit the @transient-null trap (e.g. CometExecRDD.perPartitionByKey).
      val inputParts = inputRDDs.map(_.partitions(i)).toArray
      // Slice this partition's plan data off the @transient full map here on the driver. Carrying
      // only the per-partition slice on the Partition object (serialized per task) keeps the full
      // O(numPartitions) map out of the broadcast task binary, which otherwise blows the 2GB
      // ByteArrayOutputStream limit on jobs with tens of millions of partitions. Mirrors
      // CometExecRDD.getPartitions.
      val planDataByKey = perPartitionByKey.map { case (key, arr) => key -> arr(i) }
      new CometNativeShuffleInputPartition(i, inputParts, planDataByKey)
    }.toArray

  override def compute(
      split: Partition,
      context: TaskContext): Iterator[Product2[Int, ColumnarBatch]] = {
    spillMetricNode.reportSpillMetrics(context)
    val partition = split.asInstanceOf[CometNativeShuffleInputPartition]
    val (inputObjects, shuffleBlockIters) =
      CometExecRDD.resolveInputObjects(
        inputRDDs,
        partition.inputPartitions,
        shuffleScanIndices,
        context)
    new CometNativeShuffleInputIterator(
      partition.index,
      inputObjects,
      shuffleBlockIters,
      partition.planDataByKey)
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    if (inputRDDs == null || inputRDDs.isEmpty) return Nil
    val partition = split.asInstanceOf[CometNativeShuffleInputPartition]
    val prefs = inputRDDs.zip(partition.inputPartitions).map { case (rdd, part) =>
      rdd.preferredLocations(part)
    }
    val intersection = prefs.reduce((a, b) => a.intersect(b))
    if (intersection.nonEmpty) intersection else prefs.flatten.distinct
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    inputRDDs = null
  }
}

private[shuffle] class CometNativeShuffleInputPartition(
    override val index: Int,
    val inputPartitions: Array[Partition],
    val planDataByKey: Map[String, Array[Byte]])
    extends Partition

/**
 * Iterator handed to [[CometNativeShuffleWriter.write]] via Spark's ShuffleMapTask. Reports no
 * elements; the writer downcasts and reads `partitionIndex`, `inputObjects`,
 * `shuffleBlockIterators`, and `planDataByKey` directly to drive the unified native plan.
 * `inputObjects` are the already-resolved native input slots (see
 * [[CometExecRDD.resolveInputObjects]]). `planDataByKey` is this partition's slice of the scan
 * plan data (one entry per scan key); the writer injects it into the native plan.
 */
private[shuffle] class CometNativeShuffleInputIterator(
    val partitionIndex: Int,
    val inputObjects: Array[Object],
    val shuffleBlockIterators: Map[Int, CometShuffleBlockIterator],
    val planDataByKey: Map[String, Array[Byte]])
    extends Iterator[Product2[Int, ColumnarBatch]] {

  override def hasNext: Boolean = false

  override def next(): Product2[Int, ColumnarBatch] =
    throw new NoSuchElementException(
      "CometNativeShuffleInputIterator should never be drained as an iterator. Reaching this " +
        "code means a non-Comet ShuffleWriter is consuming the input, which is a bug.")
}
