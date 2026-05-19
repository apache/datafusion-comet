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

import scala.collection.mutable.HashMap
import scala.concurrent.duration.NANOSECONDS

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.comet.shims.ShimCometScanExec
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.metric._
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.collection._

/**
 * Comet physical scan node for DataSource V1. This node is created by CometScanRule as a planning
 * intermediate and is always replaced before execution: CometExecRule converts it to a
 * [[CometNativeScanExec]], or falls back to the wrapped [[FileSourceScanExec]] on failure. It is
 * not a runtime exec node and its `doExecute` / `doExecuteColumnar` will throw.
 */
case class CometScanExec(
    @transient relation: HadoopFsRelation,
    output: Seq[Attribute],
    requiredSchema: StructType,
    partitionFilters: Seq[Expression],
    optionalBucketSet: Option[BitSet],
    optionalNumCoalescedBuckets: Option[Int],
    dataFilters: Seq[Expression],
    tableIdentifier: Option[TableIdentifier],
    disableBucketedScan: Boolean = false,
    wrapped: FileSourceScanExec)
    extends DataSourceScanExec
    with ShimCometScanExec
    with CometPlan {

  override val nodeName: String =
    s"CometScan $relation ${tableIdentifier.map(_.unquotedString).getOrElse("")}"

  // FIXME: ideally we should reuse wrapped.supportsColumnar, however that fails many tests
  override lazy val supportsColumnar: Boolean =
    relation.fileFormat.supportBatch(relation.sparkSession, schema)

  override def vectorTypes: Option[Seq[String]] = wrapped.vectorTypes

  private lazy val driverMetrics: HashMap[String, Long] = HashMap.empty

  /**
   * Send the driver-side metrics. Before calling this function, selectedPartitions has been
   * initialized. See SPARK-26327 for more details.
   */
  private def sendDriverMetrics(): Unit = {
    driverMetrics.foreach(e => metrics(e._1).set(e._2))
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(
      sparkContext,
      executionId,
      metrics.filter(e => driverMetrics.contains(e._1)).values.toSeq)
  }

  private def isDynamicPruningFilter(e: Expression): Boolean =
    e.find(_.isInstanceOf[PlanExpression[_]]).isDefined

  @transient lazy val selectedPartitions: Array[PartitionDirectory] = {
    val optimizerMetadataTimeNs = relation.location.metadataOpsTimeNs.getOrElse(0L)
    val startTime = System.nanoTime()
    val ret =
      relation.location.listFiles(partitionFilters.filterNot(isDynamicPruningFilter), dataFilters)
    setFilesNumAndSizeMetric(ret, true)
    val timeTakenMs =
      NANOSECONDS.toMillis((System.nanoTime() - startTime) + optimizerMetadataTimeNs)
    driverMetrics("metadataTime") = timeTakenMs
    ret
  }.toArray

  // We can only determine the actual partitions at runtime when a dynamic partition filter is
  // present. This is because such a filter relies on information that is only available at run
  // time (for instance the keys used in the other side of a join).
  @transient private lazy val dynamicallySelectedPartitions: Array[PartitionDirectory] = {
    val dynamicPartitionFilters = partitionFilters.filter(isDynamicPruningFilter)

    if (dynamicPartitionFilters.nonEmpty) {
      val startTime = System.nanoTime()
      // call the file index for the files matching all filters except dynamic partition filters
      val predicate = dynamicPartitionFilters.reduce(And)
      val partitionColumns = relation.partitionSchema
      val boundPredicate = Predicate.create(
        predicate.transform { case a: AttributeReference =>
          val index = partitionColumns.indexWhere(a.name == _.name)
          BoundReference(index, partitionColumns(index).dataType, nullable = true)
        },
        Nil)
      val ret = selectedPartitions.filter(p => boundPredicate.eval(p.values))
      setFilesNumAndSizeMetric(ret, false)
      val timeTakenMs = (System.nanoTime() - startTime) / 1000 / 1000
      driverMetrics("pruningTime") = timeTakenMs
      ret
    } else {
      selectedPartitions
    }
  }

  // exposed for testing
  lazy val bucketedScan: Boolean = wrapped.bucketedScan

  override lazy val (outputPartitioning, outputOrdering): (Partitioning, Seq[SortOrder]) = {
    if (bucketedScan) {
      (wrapped.outputPartitioning, wrapped.outputOrdering)
    } else {
      val files = selectedPartitions.flatMap(partition => partition.files)
      val numPartitions = files.length
      (UnknownPartitioning(numPartitions), wrapped.outputOrdering)
    }
  }

  /**
   * Returns the data filters that are supported for this scan. Excludes dynamic pruning filters
   * (subqueries) and null checks on array columns (see [[isNullCheckOnArrayColumn]]).
   */
  lazy val supportedDataFilters: Seq[Expression] = {
    dataFilters
      .filterNot(isDynamicPruningFilter)
      .filterNot(isNullCheckOnArrayColumn)
  }

  /**
   * Returns true for IsNotNull/IsNull predicates on ArrayType columns.
   *
   * These must be excluded from native scan data filters because:
   *
   *   1. Parquet does not support predicate pushdown on repeated columns. The Parquet library's
   *      SchemaCompatibilityValidator rejects filter predicates on repeated fields entirely
   *      (SPARK-39393, PARQUET-34). Spark's own ParquetFilters excludes REPEATED columns from
   *      pushdown for the same reason.
   *
   * 2. When Comet attaches these filters via ParquetSource.with_predicate(), DataFusion's list
   * predicate pushdown (PR #19545) considers IsNotNull on List columns a supported predicate and
   * pushes it into the Parquet reader as a RowFilter. This triggers an arrow-rs bug where
   * ListArrayReader crashes on bare repeated primitives ("item_reader def levels are None").
   *
   * 3. Even without the arrow-rs bug, the filter is redundant: a bare repeated field is never
   * null (an empty repeated field means zero elements, not null), and DataFusion's optimizer
   * would eliminate the filter if it went through the normal planning path.
   *
   * Filtering these out is safe -- the predicate is still evaluated after reading, so correctness
   * is preserved.
   */
  private def isNullCheckOnArrayColumn(expr: Expression): Boolean = expr match {
    case IsNotNull(child) => child.dataType.isInstanceOf[ArrayType]
    case IsNull(child) => child.dataType.isInstanceOf[ArrayType]
    case _ => false
  }

  override lazy val metadata: Map[String, String] =
    if (wrapped == null) Map.empty else wrapped.metadata

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

  override def inputRDDs(): Seq[RDD[InternalRow]] =
    throw new UnsupportedOperationException(
      "CometScanExec is a planning intermediate and should never reach execution")

  /** Helper for computing total number and size of files in selected partitions. */
  private def setFilesNumAndSizeMetric(
      partitions: Seq[PartitionDirectory],
      static: Boolean): Unit = {
    val filesNum = partitions.map(_.files.size.toLong).sum
    val filesSize = partitions.map(_.files.map(_.getLen).sum).sum
    if (!static || !partitionFilters.exists(isDynamicPruningFilter)) {
      driverMetrics("numFiles") = filesNum
      driverMetrics("filesSize") = filesSize
    } else {
      driverMetrics("staticFilesNum") = filesNum
      driverMetrics("staticFilesSize") = filesSize
    }
    if (relation.partitionSchema.nonEmpty) {
      driverMetrics("numPartitions") = partitions.length
    }
  }

  override lazy val metrics: Map[String, SQLMetric] =
    wrapped.driverMetrics ++ CometMetricNode.baseScanMetrics(session.sparkContext)

  protected override def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException(
      "CometScanExec is a planning intermediate and should never reach execution")

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] =
    throw new UnsupportedOperationException(
      "CometScanExec is a planning intermediate and should never reach execution")

  override def executeCollect(): Array[InternalRow] =
    throw new UnsupportedOperationException(
      "CometScanExec is a planning intermediate and should never reach execution")

  /**
   * Get the file partitions for this scan without instantiating readers or RDD. This is useful
   * for native scans that only need partition metadata.
   */
  def getFilePartitions(): Seq[FilePartition] = {
    val filePartitions = if (bucketedScan) {
      createFilePartitionsForBucketedScan(
        relation.bucketSpec.get,
        dynamicallySelectedPartitions,
        relation)
    } else {
      createFilePartitionsForNonBucketedScan(dynamicallySelectedPartitions, relation)
    }
    sendDriverMetrics()
    filePartitions
  }

  /**
   * Create file partitions for bucketed scans without instantiating readers.
   *
   * @param bucketSpec
   *   the bucketing spec.
   * @param selectedPartitions
   *   Hive-style partition that are part of the read.
   * @param fsRelation
   *   [[HadoopFsRelation]] associated with the read.
   */
  private def createFilePartitionsForBucketedScan(
      bucketSpec: BucketSpec,
      selectedPartitions: Array[PartitionDirectory],
      fsRelation: HadoopFsRelation): Seq[FilePartition] = {
    logInfo(s"Planning with ${bucketSpec.numBuckets} buckets")
    val filesGroupedToBuckets =
      selectedPartitions
        .flatMap { p =>
          p.files.map { f =>
            getPartitionedFile(f, p)
          }
        }
        .groupBy { f =>
          BucketingUtils
            .getBucketId(new Path(f.filePath.toString()).getName)
            .getOrElse(throw QueryExecutionErrors.invalidBucketFile(f.filePath.toString()))
        }

    val prunedFilesGroupedToBuckets = if (optionalBucketSet.isDefined) {
      val bucketSet = optionalBucketSet.get
      filesGroupedToBuckets.filter { f =>
        bucketSet.get(f._1)
      }
    } else {
      filesGroupedToBuckets
    }

    optionalNumCoalescedBuckets
      .map { numCoalescedBuckets =>
        logInfo(s"Coalescing to ${numCoalescedBuckets} buckets")
        val coalescedBuckets = prunedFilesGroupedToBuckets.groupBy(_._1 % numCoalescedBuckets)
        Seq.tabulate(numCoalescedBuckets) { bucketId =>
          val partitionedFiles = coalescedBuckets
            .get(bucketId)
            .map {
              _.values.flatten.toArray
            }
            .getOrElse(Array.empty)
          FilePartition(bucketId, partitionedFiles)
        }
      }
      .getOrElse {
        Seq.tabulate(bucketSpec.numBuckets) { bucketId =>
          FilePartition(bucketId, prunedFilesGroupedToBuckets.getOrElse(bucketId, Array.empty))
        }
      }
  }

  /**
   * Create file partitions for non-bucketed scans without instantiating readers.
   *
   * @param selectedPartitions
   *   Hive-style partition that are part of the read.
   * @param fsRelation
   *   [[HadoopFsRelation]] associated with the read.
   */
  private def createFilePartitionsForNonBucketedScan(
      selectedPartitions: Array[PartitionDirectory],
      fsRelation: HadoopFsRelation): Seq[FilePartition] = {
    val openCostInBytes = fsRelation.sparkSession.sessionState.conf.filesOpenCostInBytes
    val maxSplitBytes =
      FilePartition.maxSplitBytes(fsRelation.sparkSession, selectedPartitions)
    logInfo(
      s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
        s"open cost is considered as scanning $openCostInBytes bytes.")

    // Filter files with bucket pruning if possible
    val bucketingEnabled = fsRelation.sparkSession.sessionState.conf.bucketingEnabled
    val shouldProcess: Path => Boolean = optionalBucketSet match {
      case Some(bucketSet) if bucketingEnabled =>
        // Do not prune the file if bucket file name is invalid
        filePath => BucketingUtils.getBucketId(filePath.getName).forall(bucketSet.get)
      case _ =>
        _ => true
    }

    val splitFiles = selectedPartitions
      .flatMap { partition =>
        partition.files.flatMap { file =>
          // getPath() is very expensive so we only want to call it once in this block:
          val filePath = file.getPath

          if (shouldProcess(filePath)) {
            val isSplitable = relation.fileFormat.isSplitable(
              relation.sparkSession,
              relation.options,
              filePath) &&
              // SPARK-39634: Allow file splitting in combination with row index generation once
              // the fix for PARQUET-2161 is available.
              !isNeededForSchema(requiredSchema)
            super.splitFiles(
              sparkSession = relation.sparkSession,
              file = file,
              filePath = filePath,
              isSplitable = isSplitable,
              maxSplitBytes = maxSplitBytes,
              partitionValues = partition.values)
          } else {
            Seq.empty
          }
        }
      }
      .sortBy(_.length)(implicitly[Ordering[Long]].reverse)

    FilePartition.getFilePartitions(relation.sparkSession, splitFiles, maxSplitBytes)
  }

  override def doCanonicalize(): CometScanExec = {
    CometScanExec(
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
      null)
  }
}

object CometScanExec {

  def apply(scanExec: FileSourceScanExec, session: SparkSession): CometScanExec = {
    val batchScanExec = CometScanExec(
      scanExec.relation,
      scanExec.output,
      scanExec.requiredSchema,
      scanExec.partitionFilters,
      scanExec.optionalBucketSet,
      scanExec.optionalNumCoalescedBuckets,
      scanExec.dataFilters,
      scanExec.tableIdentifier,
      scanExec.disableBucketedScan,
      scanExec)
    scanExec.logicalLink.foreach(batchScanExec.setLogicalLink)
    batchScanExec
  }

  def isFileFormatSupported(fileFormat: FileFormat): Boolean = {
    // Only support Spark's built-in Parquet scans, not others such as Delta which use a subclass
    // of ParquetFileFormat.
    fileFormat.getClass().equals(classOf[ParquetFileFormat])
  }

}
