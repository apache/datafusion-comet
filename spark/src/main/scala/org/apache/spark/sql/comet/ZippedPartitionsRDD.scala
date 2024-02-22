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

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.{RDD, RDDOperationScope, ZippedPartitionsBaseRDD, ZippedPartitionsPartition}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Similar to Spark `ZippedPartitionsRDD[1-4]` classes, this class is used to zip partitions of
 * the multiple RDDs into a single RDD. Spark `ZippedPartitionsRDD[1-4]` classes only support at
 * most 4 RDDs. This class is used to support more than 4 RDDs. This ZipPartitionsRDD is used to
 * zip the input sources of the Comet physical plan. So it only zips partitions of ColumnarBatch.
 */
private[spark] class ZippedPartitionsRDD(
    sc: SparkContext,
    var f: (Seq[Iterator[ColumnarBatch]]) => Iterator[ColumnarBatch],
    var zipRdds: Seq[RDD[ColumnarBatch]],
    preservesPartitioning: Boolean = false)
    extends ZippedPartitionsBaseRDD[ColumnarBatch](sc, zipRdds, preservesPartitioning) {

  override def compute(s: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val partitions = s.asInstanceOf[ZippedPartitionsPartition].partitions
    val iterators =
      zipRdds.zipWithIndex.map(pair => pair._1.iterator(partitions(pair._2), context))
    f(iterators)
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    zipRdds = null
    f = null
  }
}

object ZippedPartitionsRDD {
  def apply(sc: SparkContext, rdds: Seq[RDD[ColumnarBatch]])(
      f: (Seq[Iterator[ColumnarBatch]]) => Iterator[ColumnarBatch]): RDD[ColumnarBatch] =
    withScope(sc) {
      new ZippedPartitionsRDD(sc, f, rdds)
    }

  private[spark] def withScope[U](sc: SparkContext)(body: => U): U =
    RDDOperationScope.withScope[U](sc)(body)
}
