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
import org.apache.spark.sql.comet.CometNativeExec
import org.apache.spark.sql.execution.{ColumnarToRowExec, LeafExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * What the result task needs to create the native graph: no plan objects. For a native file scan,
 * `scanPartitions` holds each partition's serialized `NativeScan` with its files, and `filePaths`
 * each partition's file paths, which name the files of a partition that fails to read one.
 */
private[local] case class LocalQuerySpec(
    plan: Array[Byte],
    partitions: Int,
    outputTypes: Seq[DataType],
    scanPartitions: Array[Array[Byte]] = Array.empty,
    filePaths: Array[Seq[String]] = Array.empty) {
  def columns: Int = outputTypes.size
}

/**
 * A whole query run as one native graph by the single Spark task of its result. `block` is the
 * Comet native block the graph is created from, which explain shows.
 */
case class CometLocalExec private[local] (
    override val output: Seq[Attribute],
    ordering: Seq[SortOrder],
    block: CometNativeExec)
    extends LeafExecNode {

  /**
   * The graph's spec, made when the query runs, as ordinary execution lists a file scan's files
   * only then. `None` when the block has no partitions or more than local execution runs, and the
   * result then comes from ordinary execution.
   */
  @transient private[local] lazy val spec: Option[LocalQuerySpec] = CometLocalRule.spec(block)

  override def supportsColumnar: Boolean = true

  // The result is one partition, but its rows are not clustered by anything.
  override def outputPartitioning: Partitioning = UnknownPartitioning(1)

  // The ordering of the whole result, which admission derives from the block's partitions.
  override def outputOrdering: Seq[SortOrder] = ordering

  override def innerChildren: Seq[QueryPlan[_]] = Seq(block)

  // A file scan's partitions are known only once its files are listed, when the query runs.
  override def stringArgs: Iterator[Any] = {
    val partitions = block.collectLeaves().head.outputPartitioning.numPartitions
    if (partitions > 0) Iterator(output, s"partitions=$partitions") else Iterator(output)
  }

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"))

  override protected def doExecute(): RDD[InternalRow] = ColumnarToRowExec(this).execute()

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val rows = longMetric("numOutputRows")
    val batches = longMetric("numOutputBatches")
    val spec = this.spec.getOrElse(
      throw new IllegalStateException("A local query without a local spec does not run locally"))
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
