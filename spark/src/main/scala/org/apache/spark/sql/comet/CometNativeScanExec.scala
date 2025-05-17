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

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types._
import org.apache.spark.util.collection._

import com.google.common.base.Objects

import org.apache.comet.{CometConf, DataTypeSupport}
import org.apache.comet.parquet.CometParquetFileFormat
import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * Comet fully native scan node for DataSource V1 that delegates to DataFusion's DataSourceExec.
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
    originalPlan: FileSourceScanExec,
    override val serializedPlanOpt: SerializedPlan)
    extends CometLeafExec
    with DataSourceScanExec {

  override lazy val metadata: Map[String, String] = originalPlan.metadata

  override val nodeName: String =
    s"CometNativeScan $relation ${tableIdentifier.map(_.unquotedString).getOrElse("")}"

  override lazy val outputPartitioning: Partitioning =
    UnknownPartitioning(originalPlan.inputRDD.getNumPartitions)

  override lazy val outputOrdering: Seq[SortOrder] = originalPlan.outputOrdering

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
      originalPlan.doCanonicalize(),
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

  override lazy val metrics: Map[String, SQLMetric] = {
    // We don't append CometMetricNode.baselineMetrics because
    // elapsed_compute has no counterpart on the native side.
    Map(
      "output_rows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "time_elapsed_opening" ->
        SQLMetrics.createNanoTimingMetric(
          sparkContext,
          "Wall clock time elapsed for file opening"),
      "time_elapsed_scanning_until_data" ->
        SQLMetrics.createNanoTimingMetric(
          sparkContext,
          "Wall clock time elapsed for file scanning + " +
            "first record batch of decompression + decoding"),
      "time_elapsed_scanning_total" ->
        SQLMetrics.createNanoTimingMetric(
          sparkContext,
          "Elapsed wall clock time for for scanning " +
            "+ record batch decompression / decoding"),
      "time_elapsed_processing" ->
        SQLMetrics.createNanoTimingMetric(
          sparkContext,
          "Wall clock time elapsed for data decompression + decoding"),
      "file_open_errors" ->
        SQLMetrics.createMetric(sparkContext, "Count of errors opening file"),
      "file_scan_errors" ->
        SQLMetrics.createMetric(sparkContext, "Count of errors scanning file"),
      "predicate_evaluation_errors" ->
        SQLMetrics.createMetric(
          sparkContext,
          "Number of times the predicate could not be evaluated"),
      "row_groups_matched_bloom_filter" ->
        SQLMetrics.createMetric(
          sparkContext,
          "Number of row groups whose bloom filters were checked and matched (not pruned)"),
      "row_groups_pruned_bloom_filter" ->
        SQLMetrics.createMetric(sparkContext, "Number of row groups pruned by bloom filters"),
      "row_groups_matched_statistics" ->
        SQLMetrics.createMetric(
          sparkContext,
          "Number of row groups whose statistics were checked and matched (not pruned)"),
      "row_groups_pruned_statistics" ->
        SQLMetrics.createMetric(sparkContext, "Number of row groups pruned by statistics"),
      "bytes_scanned" ->
        SQLMetrics.createSizeMetric(sparkContext, "Number of bytes scanned"),
      "pushdown_rows_pruned" ->
        SQLMetrics.createMetric(
          sparkContext,
          "Rows filtered out by predicates pushed into parquet scan"),
      "pushdown_rows_matched" ->
        SQLMetrics.createMetric(sparkContext, "Rows passed predicates pushed into parquet scan"),
      "row_pushdown_eval_time" ->
        SQLMetrics.createNanoTimingMetric(
          sparkContext,
          "Time spent evaluating row-level pushdown filters"),
      "statistics_eval_time" ->
        SQLMetrics.createNanoTimingMetric(
          sparkContext,
          "Time spent evaluating row group-level statistics filters"),
      "bloom_filter_eval_time" ->
        SQLMetrics.createNanoTimingMetric(
          sparkContext,
          "Time spent evaluating row group Bloom Filters"),
      "page_index_rows_pruned" ->
        SQLMetrics.createMetric(sparkContext, "Rows filtered out by parquet page index"),
      "page_index_rows_matched" ->
        SQLMetrics.createMetric(sparkContext, "Rows passed through the parquet page index"),
      "page_index_eval_time" ->
        SQLMetrics.createNanoTimingMetric(
          sparkContext,
          "Time spent evaluating parquet page index filters"),
      "metadata_load_time" ->
        SQLMetrics.createNanoTimingMetric(
          sparkContext,
          "Time spent reading and parsing metadata from the footer"))
  }

  /**
   * See [[org.apache.spark.sql.execution.DataSourceScanExec.inputRDDs]]. Only used for tests.
   */
  override def inputRDDs(): Seq[RDD[InternalRow]] = originalPlan.inputRDDs()
}

object CometNativeScanExec extends DataTypeSupport {
  def apply(
      nativeOp: Operator,
      scanExec: FileSourceScanExec,
      session: SparkSession): CometNativeScanExec = {
    // TreeNode.mapProductIterator is protected method.
    def mapProductIterator[B: ClassTag](product: Product, f: Any => B): Array[B] = {
      val arr = Array.ofDim[B](product.productArity)
      var i = 0
      while (i < arr.length) {
        arr(i) = f(product.productElement(i))
        i += 1
      }
      arr
    }

    // Replacing the relation in FileSourceScanExec by `copy` seems causing some issues
    // on other Spark distributions if FileSourceScanExec constructor is changed.
    // Using `makeCopy` to avoid the issue.
    // https://github.com/apache/arrow-datafusion-comet/issues/190
    def transform(arg: Any): AnyRef = arg match {
      case _: HadoopFsRelation =>
        scanExec.relation.copy(fileFormat = new CometParquetFileFormat)(session)
      case other: AnyRef => other
      case null => null
    }

    val newArgs = mapProductIterator(scanExec, transform)
    val wrapped = scanExec.makeCopy(newArgs).asInstanceOf[FileSourceScanExec]
    val batchScanExec = CometNativeScanExec(
      nativeOp,
      wrapped.relation,
      wrapped.output,
      wrapped.requiredSchema,
      wrapped.partitionFilters,
      wrapped.optionalBucketSet,
      wrapped.optionalNumCoalescedBuckets,
      wrapped.dataFilters,
      wrapped.tableIdentifier,
      wrapped.disableBucketedScan,
      wrapped,
      SerializedPlan(None))
    scanExec.logicalLink.foreach(batchScanExec.setLogicalLink)
    batchScanExec
  }

  override def isTypeSupported(
      dt: DataType,
      name: String,
      fallbackReasons: ListBuffer[String]): Boolean = {
    dt match {
      case ByteType | ShortType if !CometConf.COMET_SCAN_ALLOW_INCOMPATIBLE.get() =>
        fallbackReasons += s"${CometConf.SCAN_NATIVE_DATAFUSION} scan cannot read $dt when " +
          s"${CometConf.COMET_SCAN_ALLOW_INCOMPATIBLE.key} is false. ${CometConf.COMPAT_GUIDE}."
        false
      case _ =>
        super.isTypeSupported(dt, name, fallbackReasons)
    }
  }
}
