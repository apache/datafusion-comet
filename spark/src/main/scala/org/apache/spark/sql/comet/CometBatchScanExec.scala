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

import org.apache.spark.rdd._
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.expressions.{Attribute, DynamicPruningExpression, Expression, Literal, SortOrder}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.execution.metric._
import org.apache.spark.sql.vectorized._

import com.google.common.base.Objects

import org.apache.comet.{DataTypeSupport, MetricsSupport}

case class CometBatchScanExec(wrapped: BatchScanExec, runtimeFilters: Seq[Expression])
    extends DataSourceV2ScanExecBase
    with CometPlan {
  def ordering: Option[Seq[SortOrder]] = wrapped.ordering

  wrapped.logicalLink.foreach(setLogicalLink)

  def keyGroupedPartitioning: Option[Seq[Expression]] = wrapped.keyGroupedPartitioning

  def inputPartitions: Seq[InputPartition] = wrapped.inputPartitions

  override lazy val inputRDD: RDD[InternalRow] = wrappedScan.inputRDD

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val scanTime = longMetric("scanTime")
    inputRDD.asInstanceOf[RDD[ColumnarBatch]].mapPartitionsInternal { batches =>
      new Iterator[ColumnarBatch] {

        override def hasNext: Boolean = {
          // The `FileScanRDD` returns an iterator which scans the file during the `hasNext` call.
          val startNs = System.nanoTime()
          val res = batches.hasNext
          scanTime += System.nanoTime() - startNs
          res
        }

        override def next(): ColumnarBatch = {
          val batch = batches.next()
          numOutputRows += batch.numRows()
          batch
        }
      }
    }
  }

  // `ReusedSubqueryExec` in Spark only call non-columnar execute.
  override def doExecute(): RDD[InternalRow] = {
    ColumnarToRowExec(this).doExecute()
  }

  override def executeCollect(): Array[InternalRow] = {
    ColumnarToRowExec(this).executeCollect()
  }

  override def readerFactory: PartitionReaderFactory = wrappedScan.readerFactory

  override def scan: Scan = wrapped.scan

  override def output: Seq[Attribute] = wrapped.output

  override def equals(other: Any): Boolean = other match {
    case other: CometBatchScanExec =>
      // `wrapped` in `this` and `other` could reference to the same `BatchScanExec` object,
      // therefore we need to also check `runtimeFilters` equality here.
      this.wrappedScan == other.wrappedScan && this.runtimeFilters == other.runtimeFilters
    case _ =>
      false
  }

  override def hashCode(): Int = {
    Objects.hashCode(wrappedScan, runtimeFilters)
  }

  override def doCanonicalize(): CometBatchScanExec = {
    this.copy(
      wrapped = wrappedScan.doCanonicalize(),
      runtimeFilters = QueryPlan.normalizePredicates(
        runtimeFilters.filterNot(_ == DynamicPruningExpression(Literal.TrueLiteral)),
        output))
  }

  override def nodeName: String = {
    wrapped.nodeName.replace("BatchScan", "CometBatchScan")
  }

  override def simpleString(maxFields: Int): String = {
    val truncatedOutputString = truncatedString(output, "[", ", ", "]", maxFields)
    val runtimeFiltersString =
      s"RuntimeFilters: ${runtimeFilters.mkString("[", ",", "]")}"
    val result = s"$nodeName$truncatedOutputString ${scan.description()} $runtimeFiltersString"
    redact(result)
  }

  private def wrappedScan: BatchScanExec = {
    // The runtime filters in this scan could be transformed by optimizer rules such as
    // `PlanAdaptiveDynamicPruningFilters`, while the one in the wrapped scan is not. And
    // since `inputRDD` uses the latter and therefore will be incorrect if we don't set it here.
    //
    // There is, however, no good way to modify `wrapped.runtimeFilters` since it is immutable.
    // It is not good to use `wrapped.copy` here since it will also re-initialize those lazy val
    // in the `BatchScanExec`, e.g., metrics.
    //
    // TODO: find a better approach than this hack
    val f = classOf[BatchScanExec].getDeclaredField("runtimeFilters")
    f.setAccessible(true)
    f.set(wrapped, runtimeFilters)
    wrapped
  }

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "scanTime" -> SQLMetrics.createNanoTimingMetric(
      sparkContext,
      "scan time")) ++ wrapped.customMetrics ++ {
    wrapped.scan match {
      case s: MetricsSupport => s.initMetrics(sparkContext)
      case _ => Map.empty
    }
  }

  @transient override lazy val partitions: Seq[Seq[InputPartition]] = wrappedScan.partitions

  override def supportsColumnar: Boolean = true
}

object CometBatchScanExec extends DataTypeSupport
