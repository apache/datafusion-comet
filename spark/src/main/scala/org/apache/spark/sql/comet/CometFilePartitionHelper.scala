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
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.comet.shims.ShimCometScanExec
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.{FileSourceScanExec, LeafExecNode, SQLExecution}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

/**
 * Computes [[FilePartition]]s from a [[FileSourceScanExec]] without going through `inputRDD` /
 * `buildReaderWithPartitionValues`.
 *
 * `CometNativeScanExec` only needs the post-DPP `Seq[FilePartition]` for native execution; it
 * never invokes the JVM Parquet reader. Going through `originalPlan.inputRDD` would build a
 * Parquet reader closure (captured by `FileScanRDD` but never invoked here), broadcast a
 * `SerializableConfiguration` per scan that lives until session shutdown, and walk
 * `FileSourceScanLike.pushedDownFilters` -- which calls `ScalarSubquery.toLiteral` on
 * `dataFilters` and requires the subqueries to have been resolved through Spark's standard
 * `prepare -> waitForSubqueries` lifecycle. Because `originalPlan` is `@transient`, the wrapper's
 * prepare walk does not reach it, so its scalar-subquery instances stay unresolved and the lazy
 * val throws "Subquery has not finished".
 *
 * Logic mirrors Spark's `FileSourceScanLike` in
 * `org.apache.spark.sql.execution.DataSourceScanExec` -- specifically `selectedPartitions`,
 * `dynamicallySelectedPartitions`, `setFilesNumAndSizeMetric`, `sendDriverMetrics`, plus
 * `createBucketedReadRDD` / `createReadRDD` reduced to their `FilePartition`-producing subset.
 * Code ported second-hand from `CometScanExec` on `apache/main`, which ports the same methods out
 * of Spark for the legacy V1 scan path. When `CometScanExec` is deleted, this helper becomes the
 * single home for the partition-computation port.
 *
 * A single implementation works for Spark 3.x and 4.x. Spark 4.x's `selectedPartitions` returns
 * `ScanFileListing` (with `filterAndPruneFiles`, `toPartitionArray`, `filePartitionIterator`)
 * while 3.x returns `Seq[PartitionDirectory]`. This helper operates on `Seq[PartitionDirectory]`
 * directly via `relation.location.listFiles`, which exists on both versions. Per-Spark-version
 * `splitFiles` / `getPartitionedFile` signature differences (Spark 3.5.5 changed signatures via
 * reflection) are handled by `ShimCometScanExec`. While Comet still supports Spark 3.x, this is
 * the right shape; once 3.x is dropped, this helper can adopt Spark 4.x's `ScanFileListing` API
 * directly. If Spark drifts on partition-computation logic, the canonical reference is
 * `FileSourceScanLike` in Spark's `DataSourceScanExec.scala`.
 *
 * One known intentional divergence from Spark 4.x: `dynamicallySelectedPartitions` calls
 * `selectedPartitions.filterAndPruneFiles(boundPredicate, dynamicDataFilters)`, which also
 * applies dynamic data filters for file-level pruning. This helper applies only dynamic partition
 * filters, matching `CometScanExec`. For native scans this has zero practical gap.
 * `dynamicDataFilters` is `dataFilters.filter(isDynamicPruningFilter)`, where
 * `isDynamicPruningFilter` matches anything containing a `PlanExpression`. By class:
 * `ScalarSubquery` (`org.apache.spark.sql.execution.ScalarSubquery`, aliased
 * `ExecScalarSubquery`) is handled separately -- `CometNativeScanExec.serializedPartitionData`
 * filters `dataFilters` for it, calls `updateResult()`, transforms each to a `Literal`, and
 * pushes via `commonBuilder.addDataFilters(proto)`, after which DataFusion does row-group / page
 * / row pruning natively. `InSubqueryExec` in `dataFilters` (i.e. `WHERE non_partition_col IN
 * (subquery)`) is rewritten to semi-joins / hash joins by `RewritePredicateSubquery` during
 * logical optimization and never reaches physical `dataFilters`; Spark's `PartitionPruning` rule
 * only inserts `InSubqueryExec` on partition columns. `DynamicPruningExpression` on data columns
 * is not generated by base Spark. `Exists`, `ListQuery`, `LateralSubquery` are rewritten to joins
 * before physical planning. In practice the only `PlanExpression` subclass that lands in
 * `dataFilters` is `ScalarSubquery`, and we already handle it. Risk surface: a future Spark
 * version could land a new `PlanExpression` subclass in `dataFilters` that the scalar-subquery
 * resolution block silently skips. If diagnostic gaps appear here, add an
 * unknown-`PlanExpression` log in `CometNativeScanExec.serializedPartitionData` as a tripwire.
 *
 * `getFilePartitions` writes directly into the `SQLMetric` instances that Spark constructed on
 * `wrapped.driverMetrics` and posts driver-side updates through the SQL listener bus.
 * `CometNativeScanExec.metrics` exposes those same instances by reference (via
 * `originalPlan.metrics.filterKeys(driverMetricKeys)`), so `numFiles`, `filesSize`,
 * `metadataTime`, `pruningTime`, and `numPartitions` show actual values once
 * `serializedPartitionData` has run. Plan-info dumps that read metrics before execution see
 * zeros; the runtime listener-bus update is what populates the SQL UI.
 *
 * DPP `InSubqueryExec` values in `wrapped.partitionFilters` must be resolved before
 * `getFilePartitions()` runs; `dynamicallySelectedPartitions` evaluates bound predicates against
 * partition values and a still-empty subquery would throw.
 */
private[comet] case class CometFilePartitionHelper(wrapped: FileSourceScanExec)
    extends LeafExecNode
    with ShimCometScanExec {

  private def relation = wrapped.relation
  private def partitionFilters: Seq[Expression] = wrapped.partitionFilters
  private def dataFilters: Seq[Expression] = wrapped.dataFilters
  private def requiredSchema = wrapped.requiredSchema
  private def optionalBucketSet = wrapped.optionalBucketSet
  private def optionalNumCoalescedBuckets = wrapped.optionalNumCoalescedBuckets
  private def bucketedScan = wrapped.bucketedScan

  private val driverMetrics: HashMap[String, Long] = HashMap.empty

  private def isDynamicPruningFilter(e: Expression): Boolean =
    e.find(_.isInstanceOf[PlanExpression[_]]).isDefined

  @transient private lazy val selectedPartitions: Array[PartitionDirectory] = {
    val optimizerMetadataTimeNs = relation.location.metadataOpsTimeNs.getOrElse(0L)
    val startTime = System.nanoTime()
    val ret =
      relation.location.listFiles(partitionFilters.filterNot(isDynamicPruningFilter), dataFilters)
    setFilesNumAndSizeMetric(ret, static = true)
    val timeTakenMs =
      NANOSECONDS.toMillis((System.nanoTime() - startTime) + optimizerMetadataTimeNs)
    driverMetrics("metadataTime") = timeTakenMs
    ret
  }.toArray

  @transient private lazy val dynamicallySelectedPartitions: Array[PartitionDirectory] = {
    val dynamicPartitionFilters = partitionFilters.filter(isDynamicPruningFilter)
    if (dynamicPartitionFilters.nonEmpty) {
      val startTime = System.nanoTime()
      val predicate = dynamicPartitionFilters.reduce(And)
      val partitionColumns = relation.partitionSchema
      val boundPredicate = Predicate.create(
        predicate.transform { case a: AttributeReference =>
          val index = partitionColumns.indexWhere(a.name == _.name)
          BoundReference(index, partitionColumns(index).dataType, nullable = true)
        },
        Nil)
      val ret = selectedPartitions.filter(p => boundPredicate.eval(p.values))
      setFilesNumAndSizeMetric(ret, static = false)
      val timeTakenMs = (System.nanoTime() - startTime) / 1000 / 1000
      driverMetrics("pruningTime") = timeTakenMs
      ret
    } else {
      selectedPartitions
    }
  }

  def getFilePartitions(): Seq[FilePartition] = {
    val parts = if (bucketedScan) {
      createFilePartitionsForBucketedScan(dynamicallySelectedPartitions)
    } else {
      createFilePartitionsForNonBucketedScan(dynamicallySelectedPartitions)
    }
    sendDriverMetrics()
    parts
  }

  private def createFilePartitionsForBucketedScan(
      partitions: Array[PartitionDirectory]): Seq[FilePartition] = {
    val bucketSpec = relation.bucketSpec.get
    logInfo(s"Planning with ${bucketSpec.numBuckets} buckets")
    val filesGroupedToBuckets =
      partitions
        .flatMap(p => p.files.map(f => getPartitionedFile(f, p)))
        .groupBy { f =>
          BucketingUtils
            .getBucketId(new Path(f.filePath.toString()).getName)
            .getOrElse(throw QueryExecutionErrors.invalidBucketFile(f.filePath.toString()))
        }

    val pruned = optionalBucketSet match {
      case Some(bucketSet) => filesGroupedToBuckets.filter { case (id, _) => bucketSet.get(id) }
      case None => filesGroupedToBuckets
    }

    optionalNumCoalescedBuckets
      .map { numCoalescedBuckets =>
        logInfo(s"Coalescing to $numCoalescedBuckets buckets")
        val coalesced = pruned.groupBy(_._1 % numCoalescedBuckets)
        Seq.tabulate(numCoalescedBuckets) { bucketId =>
          val files =
            coalesced.get(bucketId).map(_.values.flatten.toArray).getOrElse(Array.empty)
          FilePartition(bucketId, files)
        }
      }
      .getOrElse {
        Seq.tabulate(bucketSpec.numBuckets) { bucketId =>
          FilePartition(bucketId, pruned.getOrElse(bucketId, Array.empty))
        }
      }
  }

  private def createFilePartitionsForNonBucketedScan(
      partitions: Array[PartitionDirectory]): Seq[FilePartition] = {
    val sparkSession = relation.sparkSession
    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    val maxSplitBytes = FilePartition.maxSplitBytes(sparkSession, partitions)
    logInfo(
      s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
        s"open cost is considered as scanning $openCostInBytes bytes.")

    val bucketingEnabled = sparkSession.sessionState.conf.bucketingEnabled
    val shouldProcess: Path => Boolean = optionalBucketSet match {
      case Some(bucketSet) if bucketingEnabled =>
        filePath => BucketingUtils.getBucketId(filePath.getName).forall(bucketSet.get)
      case _ =>
        _ => true
    }

    val splitFilesList = partitions
      .flatMap { partition =>
        partition.files.flatMap { file =>
          val filePath = file.getPath
          if (shouldProcess(filePath)) {
            val isSplitable = relation.fileFormat.isSplitable(
              sparkSession,
              relation.options,
              filePath) && !isNeededForSchema(requiredSchema)
            splitFiles(
              sparkSession = sparkSession,
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

    FilePartition.getFilePartitions(sparkSession, splitFilesList, maxSplitBytes)
  }

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

  /**
   * Mirror computed driverMetrics into the SQLMetric instances on `wrapped` so
   * `CometNativeScanExec.metrics` (which forwards `originalPlan.metrics.filterKeys(...)`) shows
   * accurate values. Posts to the Spark UI listener bus so the values appear at runtime.
   */
  private def sendDriverMetrics(): Unit = {
    val updated = driverMetrics.flatMap { case (key, value) =>
      wrapped.metrics.get(key).map { metric =>
        metric.set(value)
        metric
      }
    }.toSeq
    if (updated.nonEmpty) {
      val sc: SparkContext = relation.sparkSession.sparkContext
      val executionId = sc.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
      SQLMetrics.postDriverMetricUpdates(sc, executionId, updated)
    }
  }

  override def output: Seq[Attribute] = wrapped.output
  override def metrics: Map[String, SQLMetric] = wrapped.metrics
  override protected def doExecute()
      : org.apache.spark.rdd.RDD[org.apache.spark.sql.catalyst.InternalRow] =
    throw new UnsupportedOperationException("CometFilePartitionHelper is not executable")
}
