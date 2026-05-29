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

import org.apache.comet.CometShuffleBlockIterator

/**
 * Thin RDD that anchors Spark scheduling for the native-shuffle path. Native execution itself is
 * driven by [[CometNativeShuffleWriter]] using the unified `ShuffleWriter(child = childNativeOp)`
 * plan.
 *
 * The RDD's role here is twofold:
 *   - declare `OneToOneDependency` on each leaf input RDD so the DAGScheduler walks the lineage
 *     and triggers prior stages, broadcast materialization, etc.
 *   - construct the per-partition leaf iterators (and shuffle-block iterators where applicable)
 *     in `compute`, packaged into a [[CometNativeShuffleInputIterator]] that the writer downcasts
 *     to extract the inputs it needs to feed the native plan.
 *
 * The iterator returned by `compute` always reports `hasNext = false`. Spark's `ShuffleMapTask`
 * will hand it to `writer.write`; the writer ignores it as an iterator and reads its exposed
 * fields directly.
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
      shuffleScanIndices.flatMap { si =>
        inputRDDs(si) match {
          case rdd: CometShuffledBatchRDD =>
            Some(si -> rdd.computeAsShuffleBlockIterator(partition.inputPartitions(si), context))
          case _ => None
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
      "CometNativeShuffleInputIterator does not produce elements; CometNativeShuffleWriter " +
        "drives native execution via the iterator's exposed fields.")
}
