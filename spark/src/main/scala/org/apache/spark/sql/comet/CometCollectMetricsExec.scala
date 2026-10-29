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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.{AggregatingAccumulator, CollectMetricsExec, ColumnarToRowExec, SparkPlan}
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus
import org.apache.comet.serde.{Compatible, OperatorOuterClass, SupportLevel, Unsupported}
import org.apache.comet.serde.operator.CometSink

object CometCollectMetricsExec extends CometSink[CollectMetricsExec] {

  val REQUIRES_ROW_CONSUMPTION: TreeNodeTag[Boolean] =
    TreeNodeTag[Boolean]("comet.collectMetrics.requiresRowConsumption")

  // Keep Spark's own accumulator for observation discovery and reset lifecycle. The accessor
  // is private on supported Spark 3/4 versions, so resolve it once rather than duplicating
  // those version-specific contracts.
  private lazy val accumulatorAccessor = {
    val method = classOf[CollectMetricsExec].getDeclaredMethod("accumulator")
    method.setAccessible(true)
    method
  }

  private def accumulator(op: CollectMetricsExec): AggregatingAccumulator =
    accumulatorAccessor.invoke(op).asInstanceOf[AggregatingAccumulator]

  // Spark's setState is public on the JVM but scoped to the execution package in Scala.
  private lazy val setStateMethod =
    classOf[AggregatingAccumulator].getMethod("setState", classOf[AggregatingAccumulator])

  private def publishTaskState(
      collector: AggregatingAccumulator,
      updater: AggregatingAccumulator): Unit = {
    if (collector.isZero) {
      // Adopt the first buffer without copying large aggregate states such as collect_list.
      val _ = setStateMethod.invoke(collector, updater)
    } else {
      collector.merge(updater)
    }
  }

  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_EXEC_COLLECT_METRICS_ENABLED)

  override def getSupportLevel(op: CollectMetricsExec): SupportLevel = {
    if (op.getTagValue(REQUIRES_ROW_CONSUMPTION).contains(true)) {
      Unsupported(Some("Observation below a short-circuiting consumer requires Spark rows"))
    } else {
      Compatible()
    }
  }

  override def createExec(
      nativeOp: OperatorOuterClass.Operator,
      op: CollectMetricsExec): CometNativeExec = {
    val collector = new CometCollectMetricsExec(op.name, op.metricExpressions, op.child)
    collector.copyTagsFrom(op)
    op.logicalLink.foreach(collector.setLogicalLink)
    CometSinkPlaceHolder(nativeOp, op, collector)
  }
}

/**
 * Evaluate observation expressions with Spark while forwarding the original Arrow batches.
 * Extending CollectMetricsExec keeps the node visible to Spark's observation discovery.
 */
class CometCollectMetricsExec(
    name: String,
    metricExpressions: Seq[NamedExpression],
    child: SparkPlan)
    extends CollectMetricsExec(name, metricExpressions, child)
    with CometPlan {

  override def supportsColumnar: Boolean = true

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val collector = CometCollectMetricsExec.accumulator(this)
    // Spark 3 resets observations before execution; Spark 4 does so in resetMetrics().
    if (!isSpark40Plus) {
      collector.reset()
    }
    child.executeColumnar().mapPartitions { batches =>
      // Publish only at task completion, as Spark does, so heartbeats cannot expose partial
      // updates. Spark's accumulator registration also excludes failed task attempts.
      val updater = collector.copyAndReset()
      val _ = TaskContext.get().addTaskCompletionListener[Unit] { _ =>
        CometCollectMetricsExec.publishTaskState(collector, updater)
      }
      batches.map { batch =>
        val rows = batch.rowIterator()
        while (rows.hasNext) {
          updater.add(rows.next())
        }
        batch
      }
    }
  }

  override protected def doExecute(): RDD[InternalRow] =
    ColumnarToRowExec(this).execute()

  override def copy(
      name: String = this.name,
      metricExpressions: Seq[NamedExpression] = this.metricExpressions,
      child: SparkPlan = this.child): CometCollectMetricsExec =
    new CometCollectMetricsExec(name, metricExpressions, child)

  override protected def withNewChildInternal(newChild: SparkPlan): CometCollectMetricsExec =
    copy(child = newChild)

  def toSpark: CollectMetricsExec = {
    val restored = CollectMetricsExec(name, metricExpressions, child)
    restored.copyTagsFrom(this)
    restored
  }

  // Spark's case-class equality would otherwise consider this equal to the row operator and
  // allow a tree rewrite to discard the columnar replacement.
  override def canEqual(other: Any): Boolean = other.isInstanceOf[CometCollectMetricsExec]

  override def equals(other: Any): Boolean = other match {
    case _: CometCollectMetricsExec => super.equals(other)
    case _ => false
  }

  override def hashCode(): Int = super.hashCode()

  override def productPrefix: String = "CometCollectMetricsExec"
}
