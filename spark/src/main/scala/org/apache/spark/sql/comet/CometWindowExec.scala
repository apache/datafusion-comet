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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec.{METRIC_NATIVE_TIME_DESCRIPTION, METRIC_NATIVE_TIME_NAME}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics, SQLShuffleReadMetricsReporter, SQLShuffleWriteMetricsReporter}

import com.google.common.base.Objects

import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * Comet physical plan node for Spark `WindowsExec`.
 *
 * It is used to execute a `WindowsExec` physical operator by using Comet native engine. It is not
 * like other physical plan nodes which are wrapped by `CometExec`, because it contains two native
 * executions separated by a Comet shuffle exchange.
 */
case class CometWindowExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    override val output: Seq[Attribute],
    windowExpression: Seq[NamedExpression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: SparkPlan,
    override val serializedPlanOpt: SerializedPlan)
    extends CometUnaryExec {

  override def nodeName: String = "CometWindowExec"

  private lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
  private lazy val readMetrics =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)
  override lazy val metrics: Map[String, SQLMetric] = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    METRIC_NATIVE_TIME_NAME ->
      SQLMetrics.createNanoTimingMetric(sparkContext, METRIC_NATIVE_TIME_DESCRIPTION),
    "numPartitions" -> SQLMetrics.createMetric(
      sparkContext,
      "number of partitions")) ++ readMetrics ++ writeMetrics

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)

  override def stringArgs: Iterator[Any] =
    Iterator(output, windowExpression, partitionSpec, orderSpec, child)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometWindowExec =>
        this.windowExpression == other.windowExpression && this.child == other.child &&
        this.partitionSpec == other.partitionSpec && this.orderSpec == other.orderSpec &&
        this.serializedPlanOpt == other.serializedPlanOpt
      case _ =>
        false
    }
  }

  override def hashCode(): Int =
    Objects.hashCode(windowExpression, partitionSpec, orderSpec, child)
}
