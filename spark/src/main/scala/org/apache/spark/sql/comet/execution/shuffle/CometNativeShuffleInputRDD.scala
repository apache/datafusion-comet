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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.{CometRuntimeException, CometShuffleBlockIterator}

/**
 * Thin scheduling-anchor RDD for the native-shuffle path. Declares `OneToOneDependency` on each
 * leaf input RDD (so the DAGScheduler triggers prior stages, broadcasts, etc.) and constructs
 * per-partition leaf iterators in `compute`, packaged into a [[CometNativeShuffleInputIterator]].
 * The iterator reports `hasNext = false`; [[CometNativeShuffleWriter]] downcasts it and reads the
 * leaf iterators directly to drive the unified `ShuffleWriter(child = childNativeOp)` plan.
 */
private[shuffle] class CometNativeShuffleInputRDD(
    sc: SparkContext,
    var inputRDDs: Seq[RDD[ColumnarBatch]],
    numPartitionsParam: Int,
    shuffleScanIndices: Set[Int])
    extends RDD[Product2[Int, ColumnarBatch]](
      sc,
      inputRDDs.map(rdd => new OneToOneDependency(rdd))) {

  override protected def getPartitions: Array[Partition] =
    (0 until numPartitionsParam).map { i =>
      // Resolve leaf-RDD partitions on the driver here (where their @transient fields are still
      // populated). Stashing them on the partition lets `compute` avoid touching
      // `leafRdd.partitions` on the executor, which would otherwise trigger getPartitions and
      // hit the @transient-null trap (e.g. CometExecRDD.perPartitionByKey).
      val inputParts = inputRDDs.map(_.partitions(i)).toArray
      new CometNativeShuffleInputPartition(i, inputParts)
    }.toArray

  override def compute(
      split: Partition,
      context: TaskContext): Iterator[Product2[Int, ColumnarBatch]] = {
    val partition = split.asInstanceOf[CometNativeShuffleInputPartition]
    val leafIterators = inputRDDs.zip(partition.inputPartitions).map { case (rdd, part) =>
      rdd.iterator(part, context)
    }
    val shuffleBlockIters: Map[Int, CometShuffleBlockIterator] =
      shuffleScanIndices.map { si =>
        inputRDDs(si) match {
          case rdd: CometShuffledBatchRDD =>
            si -> rdd.computeAsShuffleBlockIterator(partition.inputPartitions(si), context)
          case other =>
            // A slot classified as a shuffle scan in the proto must be wired to a
            // CometShuffledBatchRDD; anything else is a wiring bug, not something to skip
            // (native would read the wrong input for that slot).
            throw new CometRuntimeException(
              s"Input slot $si is classified as a shuffle scan but its RDD is " +
                s"${other.getClass.getName}, expected CometShuffledBatchRDD")
        }
      }.toMap
    new CometNativeShuffleInputIterator(partition.index, leafIterators, shuffleBlockIters)
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
    val inputPartitions: Array[Partition])
    extends Partition

/**
 * Iterator handed to [[CometNativeShuffleWriter.write]] via Spark's ShuffleMapTask. Reports no
 * elements; the writer downcasts and reads `partitionIndex`, `leafIterators`, and
 * `shuffleBlockIterators` directly to drive the unified native plan.
 */
private[shuffle] class CometNativeShuffleInputIterator(
    val partitionIndex: Int,
    val leafIterators: Seq[Iterator[ColumnarBatch]],
    val shuffleBlockIterators: Map[Int, CometShuffleBlockIterator])
    extends Iterator[Product2[Int, ColumnarBatch]] {

  override def hasNext: Boolean = false

  override def next(): Product2[Int, ColumnarBatch] =
    throw new NoSuchElementException(
      "CometNativeShuffleInputIterator should never be drained as an iterator. Reaching this " +
        "code means a non-Comet ShuffleWriter is consuming the input, which is a bug.")
}
