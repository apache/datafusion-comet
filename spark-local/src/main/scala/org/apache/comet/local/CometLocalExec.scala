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
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.{ColumnarToRowExec, LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch

/** What the result task needs to create the native graph: no plan objects. */
private[local] case class LocalQuerySpec(plan: Array[Byte], partitions: Int, columns: Int)

/**
 * A whole query run as one native graph by the single Spark task of its result. `block` is the
 * Comet native block the graph is created from, kept for explain only.
 */
case class CometLocalExec private[local] (
    override val output: Seq[Attribute],
    spec: LocalQuerySpec,
    ordering: Seq[SortOrder],
    block: SparkPlan)
    extends LeafExecNode {
  override def supportsColumnar: Boolean = true

  // The result is one partition, but its rows are not clustered by anything. A query over a
  // cached local result must still exchange to get its required distribution.
  override def outputPartitioning: Partitioning = UnknownPartitioning(1)

  // The ordering of the whole result, which admission derives from the block's partitions.
  override def outputOrdering: Seq[SortOrder] = ordering

  override def innerChildren: Seq[QueryPlan[_]] = Seq(block)

  override def stringArgs: Iterator[Any] = Iterator(output, s"partitions=${spec.partitions}")

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"))

  override protected def doExecute(): RDD[InternalRow] = ColumnarToRowExec(this).execute()

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val rows = longMetric("numOutputRows")
    val batches = longMetric("numOutputBatches")
    new LocalQueryRDD(sparkContext, spec).mapPartitions { input =>
      input.map { batch =>
        rows += batch.numRows().toLong
        batches += 1L
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
