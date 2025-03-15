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

import org.apache.spark.TaskContext
import org.apache.spark.rdd.{ParallelCollectionRDD, RDD}
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.comet.execution.shuffle.{CometShuffledBatchRDD, CometShuffleExchangeExec}
import org.apache.spark.sql.execution.{SparkPlan, TakeOrderedAndProjectExec, UnaryExecNode, UnsafeRowSerializer}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics, SQLShuffleReadMetricsReporter, SQLShuffleWriteMetricsReporter}
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.serde.QueryPlanSerde.exprToProto
import org.apache.comet.serde.QueryPlanSerde.supportedSortType

/**
 * Comet physical plan node for Spark `TakeOrderedAndProjectExec`.
 *
 * It is used to execute a `TakeOrderedAndProjectExec` physical operator by using Comet native
 * engine. It is not like other physical plan nodes which are wrapped by `CometExec`, because it
 * contains two native executions separated by a Comet shuffle exchange.
 */
case class CometTakeOrderedAndProjectExec(
    override val originalPlan: SparkPlan,
    override val output: Seq[Attribute],
    limit: Int,
    sortOrder: Seq[SortOrder],
    projectList: Seq[NamedExpression],
    child: SparkPlan)
    extends CometExec
    with UnaryExecNode {
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

  // Exposed for testing.
  lazy val orderingSatisfies: Boolean =
    SortOrder.orderingSatisfies(child.outputOrdering, sortOrder)

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val childRDD = child.executeColumnar()
    if (childRDD.getNumPartitions == 0) {
      new ParallelCollectionRDD(sparkContext, Seq.empty[ColumnarBatch], 1, Map.empty)
    } else {
      val singlePartitionRDD = if (childRDD.getNumPartitions == 1) {
        childRDD
      } else {
        val localTopK = if (orderingSatisfies) {
          CometExecUtils.getNativeLimitRDD(childRDD, child.output, limit)
        } else {
          val numParts = childRDD.getNumPartitions
          childRDD.mapPartitionsWithIndexInternal { case (idx, iter) =>
            val topK =
              CometExecUtils
                .getTopKNativePlan(child.output, sortOrder, child, limit)
                .get
            CometExec.getCometIterator(Seq(iter), child.output.length, topK, numParts, idx)
          }
        }

        // Shuffle to Single Partition using Comet native shuffle
        val dep = CometShuffleExchangeExec.prepareShuffleDependency(
          localTopK,
          child.output,
          outputPartitioning,
          serializer,
          metrics)
        metrics("numPartitions").set(dep.partitioner.numPartitions)

        new CometShuffledBatchRDD(dep, readMetrics)
      }

      singlePartitionRDD.mapPartitionsInternal { iter =>
        val topKAndProjection = CometExecUtils
          .getProjectionNativePlan(projectList, child.output, sortOrder, child, limit)
          .get
        val it = CometExec.getCometIterator(Seq(iter), output.length, topKAndProjection, 1, 0)
        setSubqueries(it.id, this)

        Option(TaskContext.get()).foreach { context =>
          context.addTaskCompletionListener[Unit] { _ =>
            it.close()
            cleanSubqueries(it.id, this)
          }
        }

        it
      }
    }
  }

  override def simpleString(maxFields: Int): String = {
    val orderByString = truncatedString(sortOrder, "[", ",", "]", maxFields)
    val outputString = truncatedString(output, "[", ",", "]", maxFields)

    s"CometTakeOrderedAndProjectExec(limit=$limit, orderBy=$orderByString, output=$outputString)"
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)
}

object CometTakeOrderedAndProjectExec {
  // TODO: support offset for Spark 3.4
  def isSupported(plan: TakeOrderedAndProjectExec): Boolean = {
    val exprs = plan.projectList.map(exprToProto(_, plan.child.output))
    val sortOrders = plan.sortOrder.map(exprToProto(_, plan.child.output))
    exprs.forall(_.isDefined) && sortOrders.forall(_.isDefined) && plan.offset == 0 &&
    supportedSortType(plan, plan.sortOrder)
  }
}
