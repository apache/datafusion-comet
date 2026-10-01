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

package org.apache.comet.local

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, Descending, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, SinglePartition}
import org.apache.spark.sql.execution.{ColumnarToRowExec, LeafExecNode}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.vectorized.ColumnarBatch

private[local] sealed trait LocalQuerySpec extends Serializable {
  def batchSize: Int
  def columns: Int
}

private[local] case class LocalParquetSpec(
    plan: Array[Byte],
    filePartitions: Array[Array[Byte]],
    batchSize: Int,
    columns: Int,
    rowFilterPushdown: Boolean)
    extends LocalQuerySpec

private[local] case class LocalRangeSpec(
    start: Long,
    end: Long,
    step: Long,
    partitions: Int,
    batchSize: Int,
    columns: Int)
    extends LocalQuerySpec

/** The one Spark result task owns a whole execution, including all native partitions. */
case class CometLocalExec private[local] (
    override val output: Seq[Attribute],
    spec: LocalQuerySpec)
    extends LeafExecNode {
  override def supportsColumnar: Boolean = true
  override def outputPartitioning: Partitioning = SinglePartition
  // The native range adapter uses a sort-preserving merge before the result boundary.
  override def outputOrdering: Seq[SortOrder] =
    spec match {
      case range: LocalRangeSpec =>
        Seq(SortOrder(output.head, if (range.step > 0) Ascending else Descending))
      case _: LocalParquetSpec => Nil
    }
  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override protected def doExecute(): RDD[InternalRow] = ColumnarToRowExec(this).execute()

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val rows = longMetric("numOutputRows")
    new LocalQueryRDD(sparkContext, spec).mapPartitions { input =>
      input.map { batch =>
        rows += batch.numRows().toLong
        batch
      }
    }
  }
}

private[local] class LocalQueryRDD(sc: SparkContext, spec: LocalQuerySpec)
    extends RDD[ColumnarBatch](sc, Nil) {
  override protected def getPartitions: Array[Partition] = Array(new Partition {
    override def index: Int = 0
  })

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    // A retried result task creates an entirely fresh native graph. No partition of an
    // existing graph is replayed. Creation stays lazy, so explain alone allocates nothing.
    new LocalQueryIterator(spec, context)
  }
}
