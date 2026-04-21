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

import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.comet.shims.ShimStreamSourceAwareSparkPlan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.util.collection._

import com.google.common.base.Objects

import org.apache.comet.parquet.{CometParquetFileFormat, CometParquetUtils}
import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * Native scan operator for DataSource V1 Parquet files using DataFusion's ParquetExec.
 *
 * Replaces Spark's FileSourceScanExec to enable native execution. File planning runs in Spark to
 * produce FilePartitions (handling bucketing, partition pruning, etc.), which are serialized to
 * protobuf for DataFusion to execute using its ParquetExec. This provides better performance than
 * reading through Spark's FileFormat abstraction.
 *
 * Uses split-mode serialization introduced in PR #3349: common scan metadata (schemas, filters,
 * projections) is serialized once at planning time, while per-partition file lists are lazily
 * serialized at execution time. This reduces memory when scanning tables with many partitions, as
 * each executor task receives only its partition's file list rather than all files.
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
    override val serializedPlanOpt: SerializedPlan,
    @transient scan: CometScanExec, // Lazy access to file partitions without serializing with plan
    sourceKey: String) // Key for PlanDataInjector to match common+partition data at runtime
    extends CometLeafExec
    with DataSourceScanExec
    with ShimStreamSourceAwareSparkPlan {

  override lazy val metadata: Map[String, String] = originalPlan.metadata

  /**
   * Prepare DPP subquery plans before execution.
   *
   * For non-AQE DPP, partitionFilters contains DynamicPruningExpression(InSubqueryExec(...))
   * inserted by PlanDynamicPruningFilters (which runs before Comet rules). We call
   * e.plan.prepare() here so that the subquery plans are set up before execution begins.
   *
   * Note: doPrepare() alone is NOT sufficient for DPP resolution. serializedPartitionData can be
   * triggered from findAllPlanData (via commonData) on a BroadcastExchangeExec thread, outside
   * the normal prepare() -> executeSubqueries() flow. The actual DPP resolution (updateResult)
   * happens in serializedPartitionData below.
   */
  override protected def doPrepare(): Unit = {
    partitionFilters.foreach {
      case DynamicPruningExpression(e: InSubqueryExec) =>
        e.plan.prepare()
      case _ =>
    }
    super.doPrepare()
  }

  override val nodeName: String =
    s"CometNativeScan $relation ${tableIdentifier.map(_.unquotedString).getOrElse("")}"

  override def verboseStringWithOperatorId(): String = {
    val metadataStr = metadata.toSeq.sorted
      .filterNot {
        case (_, value) if (value.isEmpty || value.equals("[]")) => true
        case (key, _) if (key.equals("DataFilters") || key.equals("Format")) => true
        case (_, _) => false
      }
      .map {
        case (key, _) if (key.equals("Location")) =>
          val location = relation.location
          val numPaths = location.rootPaths.length
          val abbreviatedLocation = if (numPaths <= 1) {
            location.rootPaths.mkString("[", ", ", "]")
          } else {
            "[" + location.rootPaths.head + s", ... ${numPaths - 1} entries]"
          }
          s"$key: ${location.getClass.getSimpleName} ${redact(abbreviatedLocation)}"
        case (key, value) => s"$key: ${redact(value)}"
      }

    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Output", output)}
       |${metadataStr.mkString("\n")}
       |""".stripMargin
  }

  // exposed for testing
  lazy val bucketedScan: Boolean = originalPlan.bucketedScan && !disableBucketedScan

  override lazy val outputPartitioning: Partitioning = {
    if (bucketedScan) {
      originalPlan.outputPartitioning
    } else {
      // Use perPartitionData.length instead of originalPlan.inputRDD.getNumPartitions.
      //
      // originalPlan.inputRDD triggers FileSourceScanExec's full scan pipeline including
      // codegen on partition filter expressions. With DPP, this calls
      // InSubqueryExec.doGenCode which requires the subquery to have finished — but
      // outputPartitioning can be accessed before prepare() runs (e.g., by
      // ValidateRequirements during plan validation).
      //
      // perPartitionData goes through serializedPartitionData, which explicitly resolves
      // DPP subqueries (via updateResult()) before accessing file partitions. This is the
      // same pattern CometIcebergNativeScanExec uses.
      //
      // This is also more correct: perPartitionData.length reflects the post-DPP partition
      // count, matching what CometExecRDD actually uses in doExecuteColumnar().
      UnknownPartitioning(perPartitionData.length)
    }
  }

  override lazy val outputOrdering: Seq[SortOrder] = originalPlan.outputOrdering

  /**
   * Lazy partition serialization - deferred until execution time to reduce driver memory.
   *
   * Split-mode serialization pattern:
   * {{{
   * Planning time:
   *   - CometNativeScan.convert() serializes common data (schemas, filters, projections)
   *   - commonData embedded in nativeOp protobuf
   *   - File partitions NOT serialized yet
   *
   * Execution time:
   *   - doExecuteColumnar() accesses commonData and perPartitionData
   *   - Forces serializedPartitionData evaluation (here)
   *   - Each partition's file list serialized separately
   *   - CometExecRDD receives per-partition data and injects at runtime
   * }}}
   *
   * This pattern reduces memory usage for tables with many partitions - instead of serializing
   * all files for all partitions in the driver, we serialize only common metadata (once) and each
   * partition's files (lazily, as tasks are scheduled).
   */
  @transient private lazy val serializedPartitionData: (Array[Byte], Array[Array[Byte]]) = {
    // Ensure DPP subqueries are resolved before accessing file partitions.
    // serializedPartitionData can be triggered from findAllPlanData (via commonData) on a
    // different execution path than the standard prepare() -> executeSubqueries() flow
    // (e.g., from a BroadcastExchangeExec thread). We must resolve DPP here explicitly.
    partitionFilters.foreach {
      case DynamicPruningExpression(e: InSubqueryExec) if e.values().isEmpty =>
        logDebug(s"Resolving DPP subquery: plan=${e.plan.getClass.getSimpleName}")
        try {
          e.updateResult()
          logDebug("DPP subquery resolved successfully")
        } catch {
          case ex: Exception =>
            logError(s"DPP subquery resolution failed: ${ex.getMessage}")
            throw ex
        }
      case _ =>
    }
    // CometNativeScanExec.partitionFilters and CometScanExec.partitionFilters contain
    // different InSubqueryExec instances. convertSubqueryBroadcasts replaced the former with
    // CometSubqueryBroadcastExec, but the latter still has the original SubqueryBroadcastExec.
    // Both need resolution because CometScanExec.dynamicallySelectedPartitions evaluates its
    // own partitionFilters. updateResult() is a no-op if already resolved.
    if (scan != null) {
      scan.partitionFilters.foreach {
        case DynamicPruningExpression(e: InSubqueryExec) if e.values().isEmpty =>
          logDebug(
            "Resolving CometScanExec DPP subquery: " +
              s"plan=${e.plan.getClass.getSimpleName}")
          e.updateResult()
        case _ =>
      }
    }
    // Extract common data from nativeOp
    val commonBytes = nativeOp.getNativeScan.getCommon.toByteArray

    // Get file partitions from CometScanExec (handles bucketing, etc.)
    val filePartitions = scan.getFilePartitions()

    // Serialize each partition's files
    import org.apache.comet.serde.operator.partition2Proto
    val perPartitionBytes = filePartitions.map { filePartition =>
      val partitionProto = partition2Proto(filePartition, relation.partitionSchema)
      val partitionNativeScan = org.apache.comet.serde.OperatorOuterClass.NativeScan
        .newBuilder()
        .setFilePartition(partitionProto)
        .build()

      partitionNativeScan.toByteArray
    }.toArray

    (commonBytes, perPartitionBytes)
  }

  def commonData: Array[Byte] = serializedPartitionData._1
  def perPartitionData: Array[Array[Byte]] = serializedPartitionData._2

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val nativeMetrics = CometMetricNode.fromCometPlan(this)
    val serializedPlan = CometExec.serializeNativePlan(nativeOp)

    // Encryption config must be passed to each executor task
    val hadoopConf = relation.sparkSession.sessionState
      .newHadoopConfWithOptions(relation.options)
    val encryptionEnabled = CometParquetUtils.encryptionEnabled(hadoopConf)
    val (broadcastedHadoopConfForEncryption, encryptedFilePaths) = if (encryptionEnabled) {
      val broadcastedConf = relation.sparkSession.sparkContext
        .broadcast(new SerializableConfiguration(hadoopConf))
      (Some(broadcastedConf), relation.inputFiles.toSeq)
    } else {
      (None, Seq.empty)
    }

    new CometExecRDD(
      sparkContext,
      Seq.empty,
      Map(sourceKey -> commonData),
      Map(sourceKey -> perPartitionData),
      serializedPlan,
      perPartitionData.length,
      output.length,
      nativeMetrics,
      Seq.empty,
      broadcastedHadoopConfForEncryption,
      encryptedFilePaths) {
      override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
        val res = super.compute(split, context)

        // Report scan input metrics after the iterator is fully consumed.
        Option(context).foreach(nativeMetrics.reportScanInputMetrics)

        res
      }
    }
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
      originalPlan.doCanonicalize(),
      SerializedPlan(None),
      null, // Transient scan not needed for canonicalization
      ""
    ) // sourceKey not needed for canonicalization
  }

  override def stringArgs: Iterator[Any] = Iterator(output)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometNativeScanExec =>
        this.originalPlan == other.originalPlan &&
        this.serializedPlanOpt == other.serializedPlanOpt &&
        this.partitionFilters == other.partitionFilters
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Objects.hashCode(originalPlan, serializedPlanOpt)

  private val driverMetricKeys =
    Set(
      "numFiles",
      "filesSize",
      "numPartitions",
      "metadataTime",
      "staticFilesNum",
      "staticFilesSize",
      "pruningTime")

  override lazy val metrics: Map[String, SQLMetric] = {
    val nativeMetrics = CometMetricNode.nativeScanMetrics(session.sparkContext)
    // Map native metric names to Spark metric names
    val withAlias = nativeMetrics.get("output_rows") match {
      case Some(metric) => nativeMetrics + ("numOutputRows" -> metric)
      case None => nativeMetrics
    }
    withAlias ++ scan.metrics.filterKeys(driverMetricKeys)
  }

  /**
   * See [[org.apache.spark.sql.execution.DataSourceScanExec.inputRDDs]]. Only used for tests.
   */
  override def inputRDDs(): Seq[RDD[InternalRow]] = originalPlan.inputRDDs()
}

object CometNativeScanExec {
  def apply(
      nativeOp: Operator,
      scanExec: FileSourceScanExec,
      session: SparkSession,
      scan: CometScanExec): CometNativeScanExec = {
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

    // Generate unique key for this scan so PlanDataInjector can match common+partition data.
    // Multiple scans of same table with different projections/filters get different keys.
    val common = nativeOp.getNativeScan.getCommon
    val source = common.getSource
    val keyComponents = Seq(
      common.getRequiredSchemaList.toString,
      common.getDataFiltersList.toString,
      common.getProjectionVectorList.toString,
      common.getFieldsList.toString)
    val hashCode = keyComponents.mkString("|").hashCode
    val sourceKey = s"${source}_${hashCode}"

    // Replacing the relation in FileSourceScanExec by `copy` seems causing some issues
    // on other Spark distributions if FileSourceScanExec constructor is changed.
    // Using `makeCopy` to avoid the issue.
    // https://github.com/apache/arrow-datafusion-comet/issues/190
    def transform(arg: Any): AnyRef = arg match {
      case _: HadoopFsRelation =>
        scanExec.relation.copy(fileFormat = new CometParquetFileFormat(session))(session)
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
      SerializedPlan(None),
      scan,
      sourceKey)
    scanExec.logicalLink.foreach(batchScanExec.setLogicalLink)
    batchScanExec
  }
}
