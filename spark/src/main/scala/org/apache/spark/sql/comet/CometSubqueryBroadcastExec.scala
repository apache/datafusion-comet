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

import java.util.concurrent.{Future => JFuture}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.util.ThreadUtils
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * Comet replacement for SubqueryBroadcastExec that consumes Arrow broadcast data from a
 * CometBroadcastExchangeExec instead of HashedRelation from BroadcastExchangeExec.
 *
 * This enables broadcast exchange reuse between DPP subqueries and broadcast hash joins when
 * CometExecRule converts BroadcastExchangeExec to CometBroadcastExchangeExec. Without this, the
 * two exchanges have different types and canonical forms, so Spark's ReuseExchangeAndSubquery
 * (which runs after Comet rules) cannot match them.
 *
 * @param indices
 *   the indices of the join keys in the list of keys from the build side
 * @param buildKeys
 *   the join keys from the build side of the join
 * @param child
 *   the CometBroadcastExchangeExec (or ReusedExchangeExec after reuse)
 */
case class CometSubqueryBroadcastExec(
    name: String,
    indices: Seq[Int],
    buildKeys: Seq[Expression],
    child: SparkPlan)
    extends BaseSubqueryExec
    with UnaryExecNode {

  override def output: Seq[Attribute] = {
    indices.map { idx =>
      val key = buildKeys(idx)
      val attrName = key match {
        case n: NamedExpression => n.name
        case Cast(n: NamedExpression, _, _, _) => n.name
        case _ => s"key_$idx"
      }
      AttributeReference(attrName, key.dataType, key.nullable)()
    }
  }

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "dataSize" -> SQLMetrics.createMetric(sparkContext, "data size (bytes)"),
    "collectTime" -> SQLMetrics.createMetric(sparkContext, "time to collect (ms)"))

  override def doCanonicalize(): SparkPlan = {
    val keys = buildKeys.map(k => QueryPlan.normalizeExpressions(k, child.output))
    CometSubqueryBroadcastExec("dpp", indices, keys, child.canonicalized)
  }

  @transient
  private lazy val relationFuture: JFuture[Array[InternalRow]] = {
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLExecution.withThreadLocalCaptured[Array[InternalRow]](
      session,
      CometSubqueryBroadcastExec.executionContext) {
      SQLExecution.withExecutionId(session, executionId) {
        val beforeCollect = System.nanoTime()

        // Get the Arrow broadcast from CometBroadcastExchangeExec
        val broadcasted = child.executeBroadcast[Array[ChunkedByteBuffer]]()
        val arrowBatches = broadcasted.value

        // Decode Arrow batches to rows using the same approach as ColumnarToRowExec:
        // batch.rowIterator() + UnsafeProjection handles all types including structs/arrays.
        val broadcastOutput = child.output
        val toUnsafe = UnsafeProjection.create(broadcastOutput, broadcastOutput)

        // Project key columns from the full broadcast output
        val keyExprs = indices.map { idx =>
          val key = buildKeys(idx)
          key match {
            case attr: Attribute =>
              val colIdx = broadcastOutput.indexWhere(_.exprId == attr.exprId)
              BoundReference(colIdx, key.dataType, key.nullable)
            case Cast(attr: Attribute, dt, tz, ansi) =>
              val colIdx = broadcastOutput.indexWhere(_.exprId == attr.exprId)
              Cast(BoundReference(colIdx, attr.dataType, attr.nullable), dt, tz, ansi)
            case _ =>
              BoundReference(idx, key.dataType, key.nullable)
          }
        }
        val keyProj = UnsafeProjection.create(keyExprs)

        val rows = arrowBatches.iterator
          .flatMap(Utils.decodeBatches(_, this.getClass.getSimpleName))
          .flatMap { batch =>
            batch.rowIterator().asScala.map(toUnsafe).map(keyProj).map(_.copy())
          }
          .toArray
          .distinct

        val beforeBuild = System.nanoTime()
        longMetric("collectTime") += (beforeBuild - beforeCollect) / 1000000
        longMetric("numOutputRows") += rows.length
        val dataSize = rows.map(_.asInstanceOf[UnsafeRow].getSizeInBytes.toLong).sum
        longMetric("dataSize") += dataSize
        SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)

        rows.asInstanceOf[Array[InternalRow]]
      }
    }
  }

  protected override def doPrepare(): Unit = {
    relationFuture
  }

  protected override def doExecute(): RDD[InternalRow] = {
    throw QueryExecutionErrors.executeCodePathUnsupportedError("CometSubqueryBroadcastExec")
  }

  override def executeCollect(): Array[InternalRow] = {
    ThreadUtils.awaitResult(relationFuture, Duration.Inf)
  }

  override def stringArgs: Iterator[Any] = super.stringArgs ++ Iterator(s"[id=#$id]")

  override protected def withNewChildInternal(newChild: SparkPlan): CometSubqueryBroadcastExec =
    copy(child = newChild)
}

object CometSubqueryBroadcastExec {
  private[comet] val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool(
      "comet-dynamicpruning",
      SQLConf.get.getConf(StaticSQLConf.BROADCAST_EXCHANGE_MAX_THREAD_THRESHOLD)))
}
