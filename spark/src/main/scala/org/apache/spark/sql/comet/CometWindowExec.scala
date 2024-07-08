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

import scala.collection.JavaConverters.asJavaIterableConverter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, NamedExpression, SortOrder, WindowExpression}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.comet.CometWindowExec.getNativePlan
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec.{METRIC_NATIVE_TIME_DESCRIPTION, METRIC_NATIVE_TIME_NAME}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics, SQLShuffleReadMetricsReporter, SQLShuffleWriteMetricsReporter}
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.serde.OperatorOuterClass
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.{exprToProto, serializeDataType, windowExprToProto}

/**
 * Comet physical plan node for Spark `WindowsExec`.
 *
 * It is used to execute a `WindowsExec` physical operator by using Comet native engine. It is not
 * like other physical plan nodes which are wrapped by `CometExec`, because it contains two native
 * executions separated by a Comet shuffle exchange.
 */
case class CometWindowExec(
    override val output: Seq[Attribute],
    windowExpression: Seq[NamedExpression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: SparkPlan)
    extends CometExec
    with UnaryExecNode {

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

  override def supportsColumnar: Boolean = true

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val childRDD = child.executeColumnar()

    childRDD.mapPartitionsInternal { iter =>
      CometExec.getCometIterator(
        Seq(iter),
        getNativePlan(output, windowExpression, partitionSpec, orderSpec, child).get)
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)

}

object CometWindowExec {
  def getNativePlan(
      outputAttributes: Seq[Attribute],
      windowExpression: Seq[NamedExpression],
      partitionSpec: Seq[Expression],
      orderSpec: Seq[SortOrder],
      child: SparkPlan): Option[Operator] = {

    val orderSpecs = orderSpec.map(exprToProto(_, child.output))
    val partitionSpecs = partitionSpec.map(exprToProto(_, child.output))
    val scanBuilder = OperatorOuterClass.Scan.newBuilder()
    val scanOpBuilder = OperatorOuterClass.Operator.newBuilder()

    val scanTypes = outputAttributes.flatten { attr =>
      serializeDataType(attr.dataType)
    }

    val windowExprs = windowExpression.map(w =>
      windowExprToProto(
        w.asInstanceOf[Alias].child.asInstanceOf[WindowExpression],
        outputAttributes))

    val windowBuilder = OperatorOuterClass.Window
      .newBuilder()

    if (windowExprs.forall(_.isDefined)) {
      windowBuilder
        .addAllWindowExpr(windowExprs.map(_.get).asJava)

      if (orderSpecs.forall(_.isDefined)) {
        windowBuilder.addAllOrderByList(orderSpecs.map(_.get).asJava)
      }

      if (partitionSpecs.forall(_.isDefined)) {
        windowBuilder.addAllPartitionByList(partitionSpecs.map(_.get).asJava)
      }

      scanBuilder.addAllFields(scanTypes.asJava)

      val opBuilder = OperatorOuterClass.Operator
        .newBuilder()
        .addChildren(scanOpBuilder.setScan(scanBuilder))

      Some(opBuilder.setWindow(windowBuilder).build())
    } else None
  }
}
