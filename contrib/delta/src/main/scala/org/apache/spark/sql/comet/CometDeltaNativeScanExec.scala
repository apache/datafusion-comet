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

import java.util.Locale

import scala.jdk.CollectionConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.{FileSourceScanExec, InSubqueryExec, SparkPlan}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.AccumulatorV2

import com.google.common.base.Objects

import org.apache.comet.serde.OperatorOuterClass
import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * Native Delta Lake scan operator with split-mode serialization and DPP support.
 *
 * Common scan metadata (schemas, filters, projections, storage options, column mappings) is
 * serialized once at planning time in `nativeOp`. Per-partition file lists are materialized
 * lazily in `serializedPartitionData` at execution time so each Spark task receives only its own
 * slice of the file list, reducing driver memory.
 *
 * DPP (Dynamic Partition Pruning) is supported by deferring partition pruning of DPP expressions
 * to execution time. Static partition filters are applied at planning time in
 * `CometDeltaNativeScan.prunePartitions`; DPP filters are resolved in `serializedPartitionData`.
 */
case class CometDeltaNativeScanExec(
    override val nativeOp: Operator,
    override val output: Seq[Attribute],
    override val serializedPlanOpt: SerializedPlan,
    @transient originalPlan: FileSourceScanExec,
    tableRoot: String,
    @transient taskListBytes: Array[Byte],
    @transient dppFilters: Seq[Expression] = Seq.empty,
    partitionSchema: StructType = new StructType(),
    /**
     * When true, `packTasks` emits one group (= one partition) per task so the native plan's
     * per-file file-groups stay 1:1 with Spark partitions (Spark consumes a single DataFusion
     * partition per Spark partition, so multiple files in one partition would drop the 2nd+
     * files' rows). Set by `CometDeltaNativeScan.createExec` when the scan projects per-file
     * `_metadata.file_path`, reads materialized row-tracking columns, or otherwise needs
     * per-file groups.
     */
    oneTaskPerPartition: Boolean = false)
    extends CometLeafExec
    with org.apache.spark.sql.comet.CometScanWithPlanData {

  // Per-partition task list bytes are exposed via the public accessors below; core's
  // CometExecRDD reads them directly when serialising the Comet plan for execution.
  // (Was a PlanDataSource SPI implementation under PR1; the SPI was rejected so we
  // expose the helpers as plain methods on this exec class.)
  def planDataSourceKey: String = sourceKey
  def planDataCommonBytes: Array[Byte] = commonData
  def planDataPerPartitionBytes: Array[Array[Byte]] = perPartitionData

  override val supportsColumnar: Boolean = true

  override val nodeName: String = s"CometDeltaNativeScan $tableRoot"

  // DPP support. The AQE DPP subquery on a partitioned Delta scan arrives as an
  // unexecutable placeholder: CometExecRule wraps Spark's
  // SubqueryAdaptiveBroadcastExec into CometSubqueryAdaptiveBroadcastExec, and
  // CometPlanAdaptiveDynamicPruningFilters rewrites it to an executable
  // (Comet)SubqueryBroadcastExec with proper broadcast reuse. That rewrite would
  // normally produce a copy of this scan, but the copy is dropped when the
  // enclosing native block is rebuilt (TreeNode.makeCopy can't carry @transient
  // fields, #3510). So the rule installs the rewrite IN PLACE via
  // `withDynamicPruningFilters` (below), which updates this transient
  // side-channel and returns `this` -- landing the executable subqueries on the
  // SAME instance that executes. `dppFilters` (the case-class field) is left
  // untouched so node equality/canonicalization is unaffected; everything at
  // execution reads `effectiveDppFilters`.
  // `@volatile`: set during query-stage optimization and read during execution
  // (driver-thread-confined in practice, but volatile guards against AQE re-planning
  // on a different thread).
  @transient @volatile private var dppFiltersOverride: Seq[Expression] = null

  private def effectiveDppFilters: Seq[Expression] =
    if (dppFiltersOverride != null) dppFiltersOverride else dppFilters

  override def dynamicPruningFilters: Seq[Expression] = effectiveDppFilters

  override def withDynamicPruningFilters(filters: Seq[Expression]): SparkPlan = {
    dppFiltersOverride = filters
    this
  }

  /** True when a DPP subquery is an adaptive-broadcast placeholder we can't
   *  execute: the unwrapped Spark `SubqueryAdaptiveBroadcastExec` or the
   *  Comet-wrapped `CometSubqueryAdaptiveBroadcastExec`. Both throw from
   *  `doExecute()`. Normally the rule rewrites them in place (see above) before
   *  execution; this guard skips any that slip through (e.g. the rule didn't
   *  run) so we read all partitions instead of crashing. */
  private def isUnexecutableDpp(plan: SparkPlan): Boolean =
    plan.isInstanceOf[org.apache.spark.sql.execution.SubqueryAdaptiveBroadcastExec] ||
      plan.isInstanceOf[CometSubqueryAdaptiveBroadcastExec]

  override protected def doPrepare(): Unit = {
    // `prepare()` (not execute) is safe for any subquery plan, including a
    // placeholder.
    effectiveDppFilters.foreach {
      case DynamicPruningExpression(e: InSubqueryExec) =>
        e.plan.prepare()
      case _ =>
    }
    super.doPrepare()
  }

  // Resolve only the DPP subqueries we can execute; skip adaptive-broadcast
  // PLACEHOLDERS (CometSubqueryAdaptiveBroadcastExec / SubqueryAdaptiveBroadcastExec),
  // which throw from doExecute(). When the optimizer rule's in-place rewrite reached
  // this instance, `effectiveDppFilters` holds the executable form and pruning applies;
  // otherwise the placeholder is skipped and the scan reads all partitions (correct, the
  // surrounding Filter/join still prunes). `applyDppFilters` enforces the same skip.
  private def resolveExecutableDppSubqueries(): Unit = {
    effectiveDppFilters.foreach {
      case DynamicPruningExpression(inSub: InSubqueryExec)
          if !isUnexecutableDpp(inSub.plan) && inSub.values().isEmpty =>
        inSub.updateResult()
      case _ =>
    }
  }

  // Comet's native-scan subquery lifecycle (see CometLeafExec): used when this scan is
  // fused inside a parent native block (findAllPlanData path).
  override def ensureSubqueriesResolved(): Unit = {
    prepare()
    resolveExecutableDppSubqueries()
  }

  // Standard Spark lifecycle path (executeColumnar -> executeQuery -> waitForSubqueries),
  // used when this scan is a native-block ROOT executed directly (e.g. the child of a
  // CometNativeColumnarToRowExec, as in a MERGE target read). The default would execute
  // EVERY collected subquery -- including an unconverted CometSubqueryAdaptiveBroadcastExec
  // (the in-place DPP rewrite is lost whenever the plan is copied after the rule runs,
  // since `dppFiltersOverride` is not a constructor field) -- and crash. Override to
  // resolve only the executable ones, mirroring `ensureSubqueriesResolved`. The native
  // scan has no subqueries other than its DPP partition filters, so not delegating to
  // `super` is safe.
  override def waitForSubqueries(): Unit = resolveExecutableDppSubqueries()

  @transient private lazy val commonBytes: Array[Byte] = {
    // The typed DeltaScan variant of OpStruct carries the common block directly.
    nativeOp.getDeltaScan.getCommon.toByteArray
  }

  @transient private lazy val allTasks: Seq[OperatorOuterClass.DeltaScanTask] =
    OperatorOuterClass.DeltaScanTaskList
      .parseFrom(taskListBytes)
      .getTasksList
      .asScala
      .toSeq

  /**
   * Synthesise a `Seq[FilePartition]` from this scan's tasks, with each task becoming one
   * `PartitionedFile` carrying its partition values as an `InternalRow`. Delta tests (e.g.
   * `DeltaSinkSuite`) inspect `executedPlan.collect[DataSourceScanExec]` and read
   * `inputRDDs.head.asInstanceOf[FileScanRDD].filePartitions` to verify partition pruning; those
   * tests find nothing under Comet because we replace the scan with this exec. The test diff in
   * `dev/diffs/delta/<version>.diff` patches the helper to fall back to this accessor, so the
   * same partition-pruning assertions pass against Comet's scan.
   */
  def synthesizedFilePartitions: Seq[org.apache.spark.sql.execution.datasources.FilePartition] = {
    if (allTasks.isEmpty) return Nil
    val sessionTz = java.time.ZoneId.of(SQLConf.get.sessionLocalTimeZone)
    val files = allTasks.zipWithIndex.map { case (task, _) =>
      val pvRow = InternalRow.fromSeq(partitionSchema.fields.toSeq.map { f =>
        val proto = task.getPartitionValuesList.asScala.find(_.getName == f.name)
        val s = if (proto.exists(_.hasValue)) Some(proto.get.getValue) else None
        org.apache.comet.contrib.delta.DeltaReflection
          .castPartitionString(s, f.dataType, sessionTz)
      })
      val sparkPath =
        org.apache.spark.paths.SparkPath.fromUrlString(task.getFilePath)
      org.apache.spark.sql.execution.datasources.PartitionedFile(
        partitionValues = pvRow,
        filePath = sparkPath,
        start = if (task.hasByteRangeStart) task.getByteRangeStart else 0L,
        length = {
          if (task.hasByteRangeStart && task.hasByteRangeEnd) {
            task.getByteRangeEnd - task.getByteRangeStart
          } else task.getFileSize
        },
        modificationTime = 0L,
        fileSize = task.getFileSize)
    }
    files.zipWithIndex.map { case (pf, i) =>
      org.apache.spark.sql.execution.datasources.FilePartition(i, Array(pf))
    }
  }

  /**
   * Build per-partition bytes from the current DPP-pruned task list. DPP filters that are still
   * `SubqueryAdaptiveBroadcastExec` placeholders at planning time materialise lazily once AQE
   * runs the broadcast; by recomputing this at `doExecuteColumnar` (rather than memoising the
   * result in a lazy val) we pick up the resolved values and actually skip partitions, instead of
   * reading the full table every time AQE is in the loop.
   */
  private def buildPerPartitionBytes(): Array[Array[Byte]] = {
    // Group ALL tasks once (`taskGroups`) so the partition COUNT is fixed
    // regardless of DPP -- Spark pins `numPartitions` at planning and the native
    // RDD's partition count must not change at execution. DPP pruning then
    // happens WITHIN each group: pruned-out tasks are removed, and a group whose
    // tasks are all pruned becomes an empty DeltaScan (0 rows) -- but the group
    // (= partition slot) remains, keeping the count stable. This lets DPP prune
    // even when the scan executes inside a parent native block (MERGE/join),
    // where the parent reads `perPartitionData` rather than running the scan's
    // own `doExecuteColumnar`.
    val groups = taskGroups
    if (groups.isEmpty) return Array.empty[Array[Byte]]
    // Gate on `effectiveDppFilters` (the rule's in-place rewrite), not the raw
    // `dppFilters`, so pruning uses the executable converted form when present.
    val survivorPaths: Option[Set[String]] =
      if (effectiveDppFilters.nonEmpty && partitionSchema.nonEmpty) {
        Some(applyDppFilters(allTasks).map(_.getFilePath).toSet)
      } else None
    groups.map { group =>
      val kept = survivorPaths match {
        case Some(s) => group.filter(t => s.contains(t.getFilePath))
        case None => group
      }
      val builder = OperatorOuterClass.DeltaScan.newBuilder()
      // Thread the table root through to the executor; required by the executor-side
      // DV decoder (kernel `absolute_path` joins `_delta_log/deletion_vectors/...` onto
      // this) and harmless to set even when no task in this partition has a DV.
      if (tableRoot != null && tableRoot.nonEmpty) builder.setTableRoot(tableRoot)
      kept.foreach(builder.addTasks)
      builder.build().toByteArray
    }.toArray
  }

  // When `oneTaskPerPartition` is set (per-file `_metadata.file_path` / materialized
  // row-tracking / per-file groups), short-circuit packing so each task gets its own
  // partition, keeping the native plan's per-file file-groups 1:1 with Spark partitions.
  private def packTasks(
      tasks: Seq[OperatorOuterClass.DeltaScanTask]): Seq[Seq[OperatorOuterClass.DeltaScanTask]] = {
    if (oneTaskPerPartition) return tasks.map(t => Seq(t))
    val conf = originalPlan.relation.sparkSession.sessionState.conf
    val openCostInBytes = conf.filesOpenCostInBytes
    val maxPartitionBytes = conf.filesMaxPartitionBytes
    val minPartitionNum = conf.filesMinPartitionNum
      .getOrElse(originalPlan.relation.sparkSession.sparkContext.defaultParallelism)
    def taskSize(t: OperatorOuterClass.DeltaScanTask): Long = {
      if (t.hasByteRangeStart && t.hasByteRangeEnd) {
        math.max(0L, t.getByteRangeEnd - t.getByteRangeStart)
      } else t.getFileSize
    }
    val totalBytes = tasks.map(t => taskSize(t) + openCostInBytes).sum
    val bytesPerCore = totalBytes / math.max(1, minPartitionNum)
    val msb = math.min(maxPartitionBytes, math.max(openCostInBytes, bytesPerCore))
    val out = scala.collection.mutable.ArrayBuffer[Seq[OperatorOuterClass.DeltaScanTask]]()
    val current = scala.collection.mutable.ArrayBuffer[OperatorOuterClass.DeltaScanTask]()
    var currentSize = 0L
    tasks.foreach { task =>
      val size = taskSize(task)
      if (currentSize + size > msb && current.nonEmpty) {
        out += current.toList
        current.clear()
        currentSize = 0L
      }
      current += task
      currentSize += size + openCostInBytes
    }
    if (current.nonEmpty) out += current.toList
    out.toSeq
  }

  // Stable task grouping = the partition layout. Computed once from ALL tasks so
  // the partition count is fixed across planning and execution (DPP prunes
  // tasks WITHIN groups, never changing the group count). `numPartitions` reads
  // this directly so counting partitions never triggers DPP broadcast
  // resolution.
  @transient private lazy val taskGroups: Seq[Seq[OperatorOuterClass.DeltaScanTask]] =
    if (allTasks.isEmpty) Seq.empty else packTasks(allTasks)

  private def applyDppFilters(
      tasks: Seq[OperatorOuterClass.DeltaScanTask]): Seq[OperatorOuterClass.DeltaScanTask] = {
    // Resolve each DPP subquery to its runtime pruning values, then prune tasks
    // by evaluating the partition predicate below. By execution time the rule
    // has installed executable (Comet)SubqueryBroadcastExec subqueries in place
    // (see `withDynamicPruningFilters`); we resolve them here. If an
    // unexecutable placeholder slipped through (rule didn't run), skip pruning
    // and read all tasks (correct, just unpruned) rather than crashing.
    if (effectiveDppFilters.exists {
        case DynamicPruningExpression(inSub: InSubqueryExec) => isUnexecutableDpp(inSub.plan)
        case _ => false
      }) {
      return tasks
    }
    val resolvedFilters: Seq[Expression] =
      try {
        effectiveDppFilters.map {
          case DynamicPruningExpression(inSub: InSubqueryExec) =>
            if (inSub.values().isEmpty) inSub.updateResult()
            inSub
          case DynamicPruningExpression(e) => e
          case other => other
        }
      } catch {
        case scala.util.control.NonFatal(_) => return tasks
      }
    if (resolvedFilters.isEmpty) return tasks

    val caseSensitive = SQLConf.get.getConf[Boolean](SQLConf.CASE_SENSITIVE)
    val combined = resolvedFilters.reduce(And)
    val bound = combined.transform { case a: AttributeReference =>
      val idx = partitionSchema.fields.indexWhere(f =>
        if (caseSensitive) f.name == a.name
        else f.name.toLowerCase(Locale.ROOT) == a.name.toLowerCase(Locale.ROOT))
      if (idx < 0) return tasks
      BoundReference(idx, partitionSchema(idx).dataType, partitionSchema(idx).nullable)
    }
    val predicate = InterpretedPredicate(bound)
    predicate.initialize(0)

    val sessionZoneId = java.time.ZoneId.of(SQLConf.get.sessionLocalTimeZone)
    tasks.filter { task =>
      val row = InternalRow.fromSeq(partitionSchema.fields.toSeq.map { field =>
        val proto = task.getPartitionValuesList.asScala.find(_.getName == field.name)
        val strValue =
          if (proto.exists(_.hasValue)) Some(proto.get.getValue) else None
        org.apache.comet.contrib.delta.DeltaReflection
          .castPartitionString(strValue, field.dataType, sessionZoneId)
      })
      predicate.eval(row)
    }
  }

  def commonData: Array[Byte] = commonBytes
  // Recomputed (not memoised) so that when a parent native block reads this at
  // execution -- after AQE has materialised the DPP broadcast -- the returned
  // per-partition task lists reflect DPP pruning. The partition COUNT is fixed
  // by `taskGroups`; only the tasks within each group are pruned.
  def perPartitionData: Array[Array[Byte]] = buildPerPartitionBytes()

  // Surface per-partition file paths to the unified `CometExecRDD` path so a per-file read
  // failure can be reported as `FAILED_READ_FILE.NO_HINT` with the offending path (see
  // `SparkErrorConverter.convertToSparkException`), matching Spark's own error for that file.
  override def perPartitionFilePaths: Array[Seq[String]] = {
    perPartitionData.map { bytes =>
      OperatorOuterClass.DeltaScan.parseFrom(bytes)
        .getTasksList.asScala.map(_.getFilePath).toSeq
    }
  }

  /**
   * Unique key for matching this scan's common/per-partition data to its operator in the native
   * plan. Must be distinct across multiple Delta scans in the same plan tree -- e.g. a self-join
   * reading two snapshot versions of the same table, where `tableRoot` alone is not unique.
   *
   * Derived identically in `DeltaPlanDataInjector.getKey` from the serialized `DeltaScanCommon`
   * proto so the driver-side map and the executor-side lookup agree.
   *
   * Mirrors the pattern used by `CometNativeScanExec.sourceKey`.
   */
  def sourceKey: String = CometDeltaNativeScanExec.computeSourceKey(nativeOp)

  def numPartitions: Int = taskGroups.length

  override lazy val outputPartitioning: Partitioning =
    UnknownPartitioning(math.max(1, numPartitions))

  override lazy val outputOrdering: Seq[SortOrder] = Nil

  private class ImmutableSQLMetric(metricType: String) extends SQLMetric(metricType, 0) {
    override def merge(other: AccumulatorV2[Long, Long]): Unit = {}
    override def reset(): Unit = {}
  }

  override lazy val metrics: Map[String, SQLMetric] = {
    val taskList =
      if (taskListBytes != null) {
        OperatorOuterClass.DeltaScanTaskList.parseFrom(taskListBytes)
      } else {
        null
      }

    // Key these under both the Comet-native-side name (`output_rows`, used by the metric
    // collector on the native side) and the Spark streaming ProgressReporter name
    // (`numOutputRows`, read by `extractSourceToNumInputRows` to populate
    // `q.recentProgress.numInputRows`). Without the `numOutputRows` alias, streaming
    // workloads that this scan feeds report 0 input rows per batch even when data flows
    // correctly -- DeltaSourceSuiteBase.CheckProgress then fails with
    // "Execute: 0 did not equal N Expected batches don't match".
    val outputRowsMetric = SQLMetrics.createMetric(sparkContext, "number of output rows")
    val baseMetrics = Map(
      "output_rows" -> outputRowsMetric,
      "numOutputRows" -> outputRowsMetric,
      "num_splits" -> SQLMetrics.createMetric(sparkContext, "number of file splits processed"))

    val planningMetrics = if (taskList != null) {
      val totalFiles = new ImmutableSQLMetric("sum")
      totalFiles.set(taskList.getTasksCount.toLong)
      sparkContext.register(totalFiles, "total files")

      val dvFiles = new ImmutableSQLMetric("sum")
      dvFiles.set(taskList.getTasksList.asScala.count(_.hasDv).toLong)
      sparkContext.register(dvFiles, "files with deletion vectors")

      // `numFiles` alias mirrors Spark's `FileSourceScanExec` metric name so
      // tests like DeltaSuite.scala "query with predicates should skip
      // partitions" -- which read `metrics.get("numFiles")` to verify
      // partition skipping -- find the same value on Comet's scan exec.
      Map("total_files" -> totalFiles, "numFiles" -> totalFiles, "dv_files" -> dvFiles)
    } else {
      Map.empty[String, SQLMetric]
    }

    baseMetrics ++ planningMetrics
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val nativeMetrics = CometMetricNode.fromCometPlan(this)
    val serializedPlan = CometExec.serializeNativePlan(nativeOp)
    // Recompute DPP pruning at execution time so we pick up broadcast results AQE has now
    // materialised (the lazy `planningPerPartitionBytes` was computed before AQE ran). When DPP
    // is absent or was already resolved at planning time, the two arrays are identical.
    val execPerPartitionBytes = buildPerPartitionBytes()
    // Mirror `CometNativeScanExec`'s encryption wiring: when parquet encryption is
    // enabled on the table's hadoop conf, broadcast the conf to executors and
    // gather every input file path (so the parquet reader can decrypt per file).
    val sparkSession = originalPlan.relation.sparkSession
    val hadoopConf = sparkSession.sessionState
      .newHadoopConfWithOptions(originalPlan.relation.options)
    val (broadcastedHadoopConfForEncryption, encryptedFilePaths) =
      if (org.apache.comet.parquet.CometParquetUtils.encryptionEnabled(hadoopConf)) {
        val broadcastedConf = sparkSession.sparkContext
          .broadcast(new org.apache.spark.util.SerializableConfiguration(hadoopConf))
        val paths = execPerPartitionBytes.flatMap { bytes =>
          OperatorOuterClass.DeltaScan.parseFrom(bytes).getTasksList.asScala.map(_.getFilePath)
        }.toSeq
        (Some(broadcastedConf), paths)
      } else {
        (None, Seq.empty[String])
      }
    // Per-partition file paths so `CometExecRDD` can report a per-file read failure as
    // `FAILED_READ_FILE.NO_HINT` with the offending path (see
    // `SparkErrorConverter.convertToSparkException`).
    val perPartitionFilePaths: Array[Seq[String]] = execPerPartitionBytes.map { bytes =>
      OperatorOuterClass.DeltaScan.parseFrom(bytes)
        .getTasksList.asScala.map(_.getFilePath).toSeq
    }
    val baseRDD = CometExecRDD(
      sparkContext,
      inputRDDs = Seq.empty,
      commonByKey = Map(sourceKey -> commonData),
      perPartitionByKey = Map(sourceKey -> execPerPartitionBytes),
      serializedPlan = serializedPlan,
      numPartitions = execPerPartitionBytes.length,
      numOutputCols = output.length,
      nativeMetrics = nativeMetrics,
      subqueries = Seq.empty,
      broadcastedHadoopConfForEncryption = broadcastedHadoopConfForEncryption,
      encryptedFilePaths = encryptedFilePaths,
      perPartitionFilePaths = perPartitionFilePaths)

    baseRDD
  }

  override def convertBlock(): CometDeltaNativeScanExec = {
    val newSerializedPlan = if (serializedPlanOpt.isEmpty) {
      val bytes = CometExec.serializeNativePlan(nativeOp)
      SerializedPlan(Some(bytes))
    } else {
      serializedPlanOpt
    }
    // IMPORTANT: forward `oneTaskPerPartition` to the rebuilt exec. The case
    // class has `oneTaskPerPartition: Boolean = false` as the last constructor
    // param with a default; if we don't pass it explicitly here, every call to
    // `convertBlock()` silently downgrades the flag to false, packing multiple
    // files into one partition and dropping the 2nd+ files' rows for scans that
    // emit per-file `_metadata.file_path` / materialized row-tracking columns.
    CometDeltaNativeScanExec(
      nativeOp,
      output,
      newSerializedPlan,
      originalPlan,
      tableRoot,
      taskListBytes,
      dppFilters,
      partitionSchema,
      oneTaskPerPartition)
  }

  override protected def doCanonicalize(): CometDeltaNativeScanExec = {
    copy(
      output = output.map(QueryPlan.normalizeExpressions(_, output)),
      serializedPlanOpt = SerializedPlan(None),
      originalPlan = null,
      taskListBytes = null,
      dppFilters = Seq.empty)
  }

  override def stringArgs: Iterator[Any] = {
    val taskCount =
      if (taskListBytes != null) {
        OperatorOuterClass.DeltaScanTaskList.parseFrom(taskListBytes).getTasksCount
      } else {
        0
      }
    val dppStr = if (dppFilters.nonEmpty) {
      s", dpp=${dppFilters.mkString("[", ", ", "]")}"
    } else {
      ""
    }
    Iterator(output, s"$tableRoot ($taskCount files$dppStr)")
  }

  override def equals(obj: Any): Boolean = obj match {
    case other: CometDeltaNativeScanExec =>
      // Include `sourceKey` so two scans of the same table at different snapshot versions
      // are NOT considered equal. Without this, Spark's ReuseExchangeAndSubquery rule
      // collapses a self-join across versions into a single exchange and reuses v0's
      // shuffle output for both sides of the join.
      tableRoot == other.tableRoot &&
      output == other.output &&
      serializedPlanOpt == other.serializedPlanOpt &&
      sourceKey == other.sourceKey
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hashCode(tableRoot, output.asJava, serializedPlanOpt, sourceKey)
}

object CometDeltaNativeScanExec {

  /**
   * Compute a stable, per-scan unique key from a `DeltaScan` operator proto. Must be
   * deterministic and identical between the driver side (`CometDeltaNativeScanExec.sourceKey`)
   * and the injector side (`DeltaPlanDataInjector.getKey`).
   *
   * Includes `snapshot_version` so that two scans of the same table at different time-travel
   * versions produce distinct keys -- otherwise `findAllPlanData` collapses their per-partition
   * data into a single map entry and one scan inherits the other's file list.
   */
  def computeSourceKey(nativeOp: Operator): String = {
    val common = nativeOp.getDeltaScan.getCommon
    val components = Seq(
      common.getTableRoot,
      common.getSnapshotVersion.toString,
      common.getRequiredSchemaList.toString,
      common.getDataFiltersList.toString,
      common.getProjectionVectorList.toString,
      common.getColumnMappingsList.toString)
    s"${common.getSource}_${components.mkString("|").hashCode}"
  }
}
