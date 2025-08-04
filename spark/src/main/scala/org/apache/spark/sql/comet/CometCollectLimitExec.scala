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

import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, SinglePartition}
import org.apache.spark.sql.comet.execution.shuffle.{CometShuffledBatchRDD, CometShuffleExchangeExec}
import org.apache.spark.sql.execution.{ColumnarToRowExec, SparkPlan, UnaryExecNode, UnsafeRowSerializer}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics, SQLShuffleReadMetricsReporter, SQLShuffleWriteMetricsReporter}
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.google.common.base.Objects

/**
 * Comet physical plan node for Spark `CollectLimitExec`.
 *
 * Similar to `CometTakeOrderedAndProjectExec`, it contains two native executions seperated by a
 * Comet shuffle.
 */
case class CometCollectLimitExec(
    override val originalPlan: SparkPlan,
    limit: Int,
    offset: Int,
    child: SparkPlan)
    extends CometExec
    with UnaryExecNode {
  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = SinglePartition

  private lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
  private lazy val readMetrics =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)
  override lazy val metrics: Map[String, SQLMetric] = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "numPartitions" -> SQLMetrics.createMetric(
      sparkContext,
      "number of partitions")) ++ readMetrics ++ writeMetrics ++ CometMetricNode.shuffleMetrics(
    sparkContext)

  private lazy val serializer: Serializer =
    new UnsafeRowSerializer(child.output.size, longMetric("dataSize"))

  override def executeCollect(): Array[InternalRow] = {
    if (limit >= 0) {
      if (offset > 0) {
        ColumnarToRowExec(child).executeTake(limit).drop(offset)
      } else {
        ColumnarToRowExec(child).executeTake(limit)
      }
    } else {
      ColumnarToRowExec(child).executeCollect().drop(offset)
    }
  }

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val childRDD = child.executeColumnar()
    if (childRDD.getNumPartitions == 0) {
      CometExecUtils.emptyRDDWithPartitions(sparkContext, 1)
    } else {
      val singlePartitionRDD = if (childRDD.getNumPartitions == 1) {
        childRDD
      } else {
        val localLimitedRDD = if (limit >= 0) {
          CometExecUtils.getNativeLimitRDD(childRDD, output, limit)
        } else {
          childRDD
        }
        // Shuffle to Single Partition using Comet shuffle
        val dep = CometShuffleExchangeExec.prepareShuffleDependency(
          localLimitedRDD,
          child.output,
          outputPartitioning,
          serializer,
          metrics)
        metrics("numPartitions").set(dep.partitioner.numPartitions)

        new CometShuffledBatchRDD(dep, readMetrics)
      }
      CometExecUtils.getNativeLimitRDD(singlePartitionRDD, output, limit, offset)
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)

  override def stringArgs: Iterator[Any] = Iterator(limit, offset, child)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometCollectLimitExec =>
        this.limit == other.limit && this.offset == other.offset &&
        this.child == other.child
      case _ =>
        false
    }
  }

  override def hashCode(): Int =
    Objects.hashCode(limit: java.lang.Integer, offset: java.lang.Integer, child)
}
