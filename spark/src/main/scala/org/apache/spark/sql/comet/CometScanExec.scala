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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.comet.shims.ShimCometScanExec
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ParquetOptions}
import org.apache.spark.sql.execution.metric._
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.collection._

import org.apache.comet.{CometConf, MetricsSupport}
import org.apache.comet.parquet.CometParquetFileFormat

/**
 * Comet physical scan node for DataSource V1. Most of the code here follow Spark's
 * [[FileSourceScanExec]].
 *
 * This is a hybrid scan where the native plan will contain a `ScanExec` that reads batches of
 * data from the JVM via JNI. The ultimate source of data may be a JVM implementation such as
 * Spark readers, or could be the `native_iceberg_compat` native scan.
 *
 * Note that scanImpl can only be `native_datafusion` after CometScanRule runs and before
 * CometExecRule runs. It will never be set to `native_datafusion` at execution time
 */
case class CometScanExec(
    scanImpl: String,
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

  assert(scanImpl != CometConf.SCAN_AUTO)

  override val nodeName: String =
    s"CometScan [$scanImpl] $relation ${tableIdentifier.map(_.unquotedString).getOrElse("")}"

  // FIXME: ideally we should reuse wrapped.supportsColumnar, however that fails many tests
  override lazy val supportsColumnar: Boolean =
    relation.fileFormat.supportBatch(relation.sparkSession, schema)

  override def vectorTypes: Option[Seq[String]] = wrapped.vectorTypes

  @transient lazy val planner: FilePartitionPlanner = new FilePartitionPlanner(
    relation,
    requiredSchema,
    partitionFilters,
    dataFilters,
    optionalBucketSet,
    optionalNumCoalescedBuckets,
    bucketedScan)

  private def isDynamicPruningFilter(e: Expression): Boolean =
    e.find(_.isInstanceOf[PlanExpression[_]]).isDefined

  @transient lazy val selectedPartitions: Array[PartitionDirectory] =
    planner.selectedPartitions

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
   * Returns the data filters that are supported for this scan implementation. For
   * native_datafusion scans, this excludes dynamic pruning filters (subqueries)
   */
  lazy val supportedDataFilters: Seq[Expression] = {
    if (scanImpl == CometConf.SCAN_NATIVE_DATAFUSION) {
      dataFilters.filterNot(isDynamicPruningFilter)
    } else {
      dataFilters
    }
  }

  @transient
  private lazy val pushedDownFilters = {
    getPushedDownFilters(relation, supportedDataFilters)
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

  lazy val inputRDD: RDD[InternalRow] = {
    val options = relation.options +
      (FileFormat.OPTION_RETURNING_BATCH -> supportsColumnar.toString)
    val readFile: (PartitionedFile) => Iterator[InternalRow] =
      relation.fileFormat.buildReaderWithPartitionValues(
        sparkSession = relation.sparkSession,
        dataSchema = relation.dataSchema,
        partitionSchema = relation.partitionSchema,
        requiredSchema = requiredSchema,
        filters = pushedDownFilters,
        options = options,
        hadoopConf =
          relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options))

    val filePartitions = getFilePartitions()
    prepareRDD(relation, readFile, filePartitions)
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    inputRDD :: Nil
  }

  override lazy val metrics: Map[String, SQLMetric] =
    wrapped.driverMetrics ++ CometMetricNode.baseScanMetrics(
      session.sparkContext) ++ (relation.fileFormat match {
      case m: MetricsSupport => m.getMetrics
      case _ => Map.empty
    })

  protected override def doExecute(): RDD[InternalRow] = {
    ColumnarToRowExec(this).doExecute()
  }

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val rdd = inputRDD.asInstanceOf[RDD[ColumnarBatch]]

    // These metrics are important for streaming solutions.
    // despite there being similar metrics published by the native reader.
    val numOutputRows = longMetric("numOutputRows")
    val scanTime = longMetric("scanTime")
    rdd.mapPartitionsInternal { batches =>
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

  override def executeCollect(): Array[InternalRow] = {
    ColumnarToRowExec(this).executeCollect()
  }

  /**
   * Get the file partitions for this scan without instantiating readers or RDD. This is useful
   * for native scans that only need partition metadata.
   */
  def getFilePartitions(): Seq[FilePartition] = {
    val result = planner.getFilePartitions()
    planner.sendDriverMetrics(metrics, sparkContext)
    result
  }

  private def prepareRDD(
      fsRelation: HadoopFsRelation,
      readFile: (PartitionedFile) => Iterator[InternalRow],
      partitions: Seq[FilePartition]): RDD[InternalRow] = {
    val sqlConf = fsRelation.sparkSession.sessionState.conf
    newFileScanRDD(
      fsRelation,
      readFile,
      partitions,
      new StructType(requiredSchema.fields ++ fsRelation.partitionSchema.fields),
      new ParquetOptions(CaseInsensitiveMap(relation.options), sqlConf))
  }

  override def doCanonicalize(): CometScanExec = {
    CometScanExec(
      scanImpl,
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

  def apply(
      scanExec: FileSourceScanExec,
      session: SparkSession,
      scanImpl: String): CometScanExec = {
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
        scanExec.relation.copy(fileFormat = new CometParquetFileFormat(session))(session)
      case other: AnyRef => other
      case null => null
    }

    val newArgs = mapProductIterator(scanExec, transform)
    val wrapped = scanExec.makeCopy(newArgs).asInstanceOf[FileSourceScanExec]
    val batchScanExec = CometScanExec(
      scanImpl,
      wrapped.relation,
      wrapped.output,
      wrapped.requiredSchema,
      wrapped.partitionFilters,
      wrapped.optionalBucketSet,
      wrapped.optionalNumCoalescedBuckets,
      wrapped.dataFilters,
      wrapped.tableIdentifier,
      wrapped.disableBucketedScan,
      wrapped)
    scanExec.logicalLink.foreach(batchScanExec.setLogicalLink)
    batchScanExec
  }

  def isFileFormatSupported(fileFormat: FileFormat): Boolean = {
    // Only support Spark's built-in Parquet scans, not others such as Delta which use a subclass
    // of ParquetFileFormat.
    fileFormat.getClass().equals(classOf[ParquetFileFormat])
  }

}
