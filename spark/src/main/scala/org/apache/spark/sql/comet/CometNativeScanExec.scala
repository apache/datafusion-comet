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
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.comet.shims.ShimCometScanExec
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.util.collection._

import com.google.common.base.Objects

import org.apache.comet.parquet.CometParquetUtils
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.operator.CometNativeScan
import org.apache.comet.shims.ShimSubqueryBroadcast

/**
 * Comet fully native scan node for DataSource V1 that delegates to DataFusion's DataSourceExec.
 *
 * Wraps FileSourceScanExec directly (similar to CometIcebergNativeScanExec wrapping
 * BatchScanExec). Supports Dynamic Partition Pruning (DPP) by deferring partition serialization
 * to execution time.
 */
case class CometNativeScanExec(
    override val nativeOp: Operator,
    @transient relation: HadoopFsRelation,
    override val output: Seq[Attribute],
    requiredSchema: StructType,
    partitionFilters: Seq[Expression],
    optionalBucketSet: Option[BitSet],
    optionalNumCoalescedBuckets: Option[Int],
    dataFilters: Seq[Expression],
    tableIdentifier: Option[TableIdentifier],
    disableBucketedScan: Boolean = false,
    @transient originalPlan: FileSourceScanExec,
    override val serializedPlanOpt: SerializedPlan)
    extends CometLeafExec
    with DataSourceScanExec
    with ShimCometScanExec
    with ShimSubqueryBroadcast {

  override lazy val metadata: Map[String, String] = originalPlan.metadata

  // Required by ShimCometScanExec for shim-compatible file splitting methods
  override def wrapped: FileSourceScanExec = originalPlan

  override val nodeName: String =
    s"CometNativeScan $relation ${tableIdentifier.map(_.unquotedString).getOrElse("")}"

  // exposed for testing
  lazy val bucketedScan: Boolean = originalPlan.bucketedScan && !disableBucketedScan

  /** Unique identifier for this scan, used to match planning data at execution time. */
  def scanId: String = {
    relation.location.rootPaths.headOption
      .map(_.toString)
      .getOrElse(originalPlan.simpleStringWithNodeId())
  }

  /**
   * Prepare DPP subquery plans. Called by Spark's prepare() before doExecuteColumnar().
   */
  override protected def doPrepare(): Unit = {
    partitionFilters.foreach {
      case DynamicPruningExpression(e: InSubqueryExec) =>
        e.plan.prepare()
      case _ =>
    }
    super.doPrepare()
  }

  /**
   * Lazy partition serialization - deferred until execution time for DPP support.
   */
  @transient private lazy val serializedPartitionData: (Array[Byte], Array[Array[Byte]]) = {
    // Wait for DPP subqueries to resolve before accessing partitions
    partitionFilters.foreach {
      case DynamicPruningExpression(e: InSubqueryExec) if e.values().isEmpty =>
        e.plan match {
          case sab: SubqueryAdaptiveBroadcastExec =>
            resolveSubqueryAdaptiveBroadcast(sab, e)
          case _: SubqueryBroadcastExec | _: SubqueryExec | _: ReusedSubqueryExec =>
            e.updateResult()
          case other =>
            throw new IllegalStateException(
              s"Unexpected subquery plan type: ${other.getClass.getName}")
        }
      case _ =>
    }

    val filePartitions = getFilePartitions()
    CometNativeScan.serializePartitions(this, filePartitions)
  }

  /** Get file partitions with DPP filtering applied. */
  private def getFilePartitions(): Seq[FilePartition] = {
    getDppFilteredFilePartitions(relation, partitionFilters, originalPlan.selectedPartitions)
  }

  def commonData: Array[Byte] = serializedPartitionData._1
  def perPartitionData: Array[Array[Byte]] = serializedPartitionData._2

  override lazy val outputPartitioning: Partitioning = {
    if (bucketedScan) {
      originalPlan.outputPartitioning
    } else {
      UnknownPartitioning(perPartitionData.length)
    }
  }

  override lazy val outputOrdering: Seq[SortOrder] = originalPlan.outputOrdering

  /** Executes using CometExecRDD - planning data is computed lazily on first access. */
  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val nativeMetrics = CometMetricNode.fromCometPlan(this)
    val serializedPlan = CometExec.serializeNativePlan(nativeOp)

    // Handle encryption: broadcast hadoop conf if encryption is enabled
    val hadoopConf = relation.sparkSession.sessionState
      .newHadoopConfWithOptions(relation.options)
    val (broadcastedHadoopConfForEncryption, encryptedFilePaths) =
      if (CometParquetUtils.encryptionEnabled(hadoopConf)) {
        val broadcastedConf = relation.sparkSession.sparkContext
          .broadcast(new SerializableConfiguration(hadoopConf))
        (Some(broadcastedConf), relation.inputFiles.toSeq)
      } else {
        (None, Seq.empty[String])
      }

    CometExecRDD(
      sparkContext,
      inputRDDs = Seq.empty,
      commonByKey = Map(scanId -> commonData),
      perPartitionByKey = Map(scanId -> perPartitionData),
      serializedPlan = serializedPlan,
      numPartitions = perPartitionData.length,
      numOutputCols = output.length,
      nativeMetrics = nativeMetrics,
      subqueries = Seq.empty,
      broadcastedHadoopConfForEncryption = broadcastedHadoopConfForEncryption,
      encryptedFilePaths = encryptedFilePaths)
  }

  override def doCanonicalize(): CometNativeScanExec = {
    CometNativeScanExec(
      nativeOp,
      relation,
      output.map(QueryPlan.normalizeExpressions(_, output)),
      requiredSchema,
      QueryPlan.normalizePredicates(
        CometScanUtils.filterUnusedDynamicPruningExpressions(partitionFilters),
        output),
      optionalBucketSet,
      optionalNumCoalescedBuckets,
      QueryPlan.normalizePredicates(dataFilters, output),
      None,
      disableBucketedScan,
      null, // Don't need originalPlan for canonicalization
      SerializedPlan(None))
  }

  override def stringArgs: Iterator[Any] = Iterator(output)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometNativeScanExec =>
        this.originalPlan == other.originalPlan &&
        this.serializedPlanOpt == other.serializedPlanOpt
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Objects.hashCode(originalPlan, serializedPlanOpt)

  override lazy val metrics: Map[String, SQLMetric] =
    CometMetricNode.nativeScanMetrics(sparkContext)

  /**
   * See [[org.apache.spark.sql.execution.DataSourceScanExec.inputRDDs]]. Only used for tests.
   */
  override def inputRDDs(): Seq[RDD[InternalRow]] = originalPlan.inputRDDs()
}

object CometNativeScanExec {

  /**
   * Create CometNativeScanExec from a FileSourceScanExec.
   */
  def apply(nativeOp: Operator, scanExec: FileSourceScanExec): CometNativeScanExec = {
    val exec = CometNativeScanExec(
      nativeOp,
      scanExec.relation,
      scanExec.output,
      scanExec.requiredSchema,
      scanExec.partitionFilters,
      scanExec.optionalBucketSet,
      scanExec.optionalNumCoalescedBuckets,
      scanExec.dataFilters,
      scanExec.tableIdentifier,
      scanExec.disableBucketedScan,
      scanExec,
      SerializedPlan(None))
    scanExec.logicalLink.foreach(exec.setLogicalLink)
    exec
  }
}
