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

package org.apache.comet.serde.operator

import java.util.Locale

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, BoundReference, InterpretedPredicate}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.comet.{CometDeltaNativeScanExec, CometNativeExec, CometScanExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.{CometConf, ConfigEntry, Native}
import org.apache.comet.delta.DeltaReflection
import org.apache.comet.objectstore.NativeConfig
import org.apache.comet.serde.{CometOperatorSerde, Compatible, ExprOuterClass, OperatorOuterClass, SupportLevel}
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.OperatorOuterClass.{DeltaScan, DeltaScanCommon, DeltaScanTaskList, Operator}
import org.apache.comet.serde.QueryPlanSerde.exprToProto

/**
 * Validation and serde logic for the native Delta Lake scan.
 *
 * `convert()` calls `Native.planDeltaScan` to enumerate files via `delta-kernel-rs`, builds the
 * `DeltaScanCommon` proto with schemas/filters/options, applies static partition pruning, and
 * stashes the task list in a ThreadLocal. `createExec()` retrieves it and builds a
 * `CometDeltaNativeScanExec` with split-mode serialization: common data serialized once at
 * planning time, per-partition task lists materialized lazily at execution time. DPP filters are
 * applied at execution time in the exec's `serializedPartitionData`.
 */
object CometDeltaNativeScan extends CometOperatorSerde[CometScanExec] with Logging {

  /** Private lazy handle to the native library - one instance per JVM. */
  private lazy val nativeLib = new Native()

  // Phase 5: stash the raw task-list bytes between convert() and createExec()
  // so the exec can do per-partition splitting at execution time. Single-threaded
  // during planning so a simple ThreadLocal is safe.
  private val lastTaskListBytes = new ThreadLocal[Array[Byte]]()

  override def enabledConfig: Option[ConfigEntry[Boolean]] = Some(
    CometConf.COMET_DELTA_NATIVE_ENABLED)

  override def getSupportLevel(operator: CometScanExec): SupportLevel = Compatible()

  override def convert(
      scan: CometScanExec,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = {

    // Resolve the table root via the HadoopFsRelation API - standard Spark, no spark-delta
    // compile-time dep required.
    val relation = scan.relation
    val tableRoot = DeltaReflection.extractTableRoot(relation).getOrElse {
      logWarning(
        s"CometDeltaNativeScan: unable to extract table root from relation " +
          s"${relation.location}; falling back to Spark's Delta reader.")
      return None
    }

    // Belt-and-suspenders DV-rewrite gate. The primary gate runs earlier in
    // CometScanRule so the scan never becomes a CometScanExec in the first place.
    // This is a defensive check in case a caller constructs a DV-rewritten
    // CometScanExec by some other path.
    if (scan.requiredSchema.fieldNames.contains(DeltaReflection.IsRowDeletedColumnName)) {
      logWarning(
        "CometDeltaNativeScan: DV-rewritten schema reached serde; this should have " +
          "been caught in CometScanRule. Falling back.")
      return None
    }

    // Cloud storage options, keyed identically to NativeScan. Kernel's DefaultEngine picks
    // up aws_* / azure_* keys; anything else is ignored on the native side (for now).
    //
    // We key off the table root URI rather than `inputFiles.head` because data file names
    // can contain characters that aren't URI-safe when Spark's test harness injects
    // prefixes like `test%file%prefix-` (breaks `java.net.URI.create`). The table root
    // string comes straight from `HadoopFsRelation.location.rootPaths.head.toUri` inside
    // `DeltaReflection.extractTableRoot`, so it's already properly encoded. Storage options
    // are bucket-level anyway - any file under the same root resolves to the same config.
    val hadoopConf =
      relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options)
    val tableRootUri = java.net.URI.create(tableRoot)
    val storageOptions: java.util.Map[String, String] =
      NativeConfig.extractObjectStoreOptions(hadoopConf, tableRootUri).asJava

    // Honor Delta's time-travel options (versionAsOf / timestampAsOf) via the Delta-
    // resolved snapshot version sitting on the FileIndex. Delta's analysis phase pins
    // the exact snapshot before we ever see the plan, so by the time `CometScanExec` is
    // built, `relation.location` is a `PreparedDeltaFileIndex` whose toString looks like
    // `Delta[version=0, file:/...]`. We parse the version out via
    // `DeltaReflection.extractSnapshotVersion` and pass it through to kernel.
    //
    // When no version can be extracted (non-Delta file index, parser miss, etc.) we pass
    // -1 which asks kernel for the current latest snapshot.
    val snapshotVersion: Long =
      DeltaReflection.extractSnapshotVersion(relation).getOrElse(-1L)

    // Phase 2: serialize the data filters so kernel can apply stats-based file
    // pruning during log replay. The same filters will also be pushed down into
    // ParquetSource for row-group-level pruning - the two layers are additive.
    //
    // We combine all supported data filters into a single AND conjunction so
    // kernel receives one predicate tree. BoundReferences carry the column INDEX
    // into scan.output; the native side resolves indices to column names using
    // the columnNames array we pass alongside.
    val predicateBytes: Array[Byte] = {
      val protoFilters = new ListBuffer[Expr]()
      scan.supportedDataFilters.foreach { filter =>
        exprToProto(filter, scan.output) match {
          case Some(proto) => protoFilters += proto
          case _ =>
        }
      }
      if (protoFilters.isEmpty) {
        Array.emptyByteArray
      } else if (protoFilters.size == 1) {
        protoFilters.head.toByteArray
      } else {
        // Combine multiple filters into AND(f1, AND(f2, ...))
        val combined = protoFilters.reduceLeft { (acc, f) =>
          val and = ExprOuterClass.BinaryExpr
            .newBuilder()
            .setLeft(acc)
            .setRight(f)
            .build()
          Expr
            .newBuilder()
            .setAnd(and)
            .build()
        }
        combined.toByteArray
      }
    }

    // Column name list for resolving BoundReference indices to kernel column
    // names. Must match the order of scan.output because exprToProto binds
    // attribute references by position in that schema.
    val columnNames: Array[String] = scan.output.map(_.name).toArray

    // --- 1. Get the active file list. ---
    //
    // Two code paths:
    //   (a) Pre-materialized FileIndex (`TahoeBatchFileIndex`, `CdcAddFileIndex`):
    //       Delta's streaming micro-batch reads AND MERGE / UPDATE / DELETE
    //       post-join rewrites both carry an exact `addFiles: Seq[AddFile]` on
    //       the FileIndex. Kernel log replay against the snapshot would return a
    //       DIFFERENT file set (the whole snapshot, or a version's deltas), which
    //       is a correctness hazard -- empty streaming batches, MERGE rewrites
    //       that see the whole table instead of only touched files. Build the
    //       DeltaScanTaskList proto directly from those AddFiles, skipping kernel.
    //   (b) Regular scan against a snapshot: call kernel for log replay as before.
    val taskListBytes =
      if (DeltaReflection.isBatchFileIndex(relation.location)) {
        DeltaReflection.extractBatchAddFiles(relation.location) match {
          case Some(addFiles) if addFiles.forall(!_.hasDeletionVector) =>
            buildTaskListFromAddFiles(
              tableRoot,
              snapshotVersion,
              addFiles,
              nativeOp = null,
              columnNames).toByteArray
          case Some(_) =>
            // Phase 1 of the pre-materialized-index path: fall back when any
            // AddFile carries a DeletionVectorDescriptor. Phase 2 can apply the
            // DV inline via our DeltaDvFilterExec.
            import org.apache.comet.CometSparkSessionExtensions.withInfo
            withInfo(
              scan,
              "Native Delta scan falls back for pre-materialized FileIndex with " +
                "deletion vectors (streaming/MERGE with DVs).")
            return None
          case None =>
            // Reflection failed; fall back conservatively.
            import org.apache.comet.CometSparkSessionExtensions.withInfo
            withInfo(
              scan,
              s"Native Delta scan could not extract AddFiles from " +
                s"${relation.location.getClass.getName}; falling back.")
            return None
        }
      } else {
        try {
          nativeLib.planDeltaScan(
            tableRoot,
            snapshotVersion,
            storageOptions,
            predicateBytes,
            columnNames)
        } catch {
          case e: Throwable =>
            logWarning(
              s"CometDeltaNativeScan: delta-kernel-rs log replay failed for $tableRoot",
              e)
            return None
        }
      }
    val taskList = DeltaScanTaskList.parseFrom(taskListBytes)

    // Phase 6 reader-feature gate. Kernel reports any Delta reader features that
    // are currently in use in this snapshot and that Comet's native path does NOT
    // correctly handle. Falling back is mandatory for correctness: reading through
    // the native path would silently produce wrong results (e.g. returning rows
    // that a deletion vector should have hidden). The gate becomes obsolete feature
    // by feature as later phases ship:
    //   deletionVectors -> Phase 3
    //   columnMapping   -> Phase 4
    //   typeWidening    -> future phase
    //   rowTracking     -> future phase
    val unsupportedFeatures = taskList.getUnsupportedFeaturesList.asScala.toSeq
    if (unsupportedFeatures.nonEmpty &&
      CometConf.COMET_DELTA_FALLBACK_ON_UNSUPPORTED_FEATURE.get(scan.conf)) {
      logInfo(
        s"CometDeltaNativeScan: falling back for table $tableRoot " +
          s"due to unsupported reader features: ${unsupportedFeatures.mkString(", ")}")
      import org.apache.comet.CometSparkSessionExtensions.withInfo
      withInfo(
        scan,
        s"Native Delta scan does not yet support these features in use on this " +
          s"snapshot: ${unsupportedFeatures.mkString(", ")}. Falling back to Spark's " +
          s"Delta reader. Set ${CometConf.COMET_DELTA_FALLBACK_ON_UNSUPPORTED_FEATURE.key}=false " +
          s"to bypass this check (NOT recommended - may produce incorrect results).")
      return None
    }

    // Apply Spark's partition filters to the task list so that queries like
    // `WHERE partition_col = X` don't drag in files from other partitions. Kernel
    // itself is given the whole snapshot (no predicate yet - that lands in Phase 2),
    // so we do the pruning in Scala by evaluating each task's partition-value map
    // against Spark's `partitionFilters`. This is a single driver-side loop; filtered
    // tasks never go over the wire to executors.
    val filteredTasks =
      prunePartitions(taskList.getTasksList.asScala.toSeq, scan, relation.partitionSchema)

    // --- 2. Build the common block ---
    val commonBuilder = DeltaScanCommon.newBuilder()
    commonBuilder.setSource(scan.simpleStringWithNodeId())
    commonBuilder.setTableRoot(taskList.getTableRoot)
    commonBuilder.setSnapshotVersion(taskList.getSnapshotVersion)
    commonBuilder.setSessionTimezone(scan.conf.getConfString("spark.sql.session.timeZone"))
    commonBuilder.setCaseSensitive(scan.conf.getConf[Boolean](SQLConf.CASE_SENSITIVE))
    commonBuilder.setDataFileConcurrencyLimit(
      CometConf.COMET_DELTA_DATA_FILE_CONCURRENCY_LIMIT.get())

    // Schemas. Delta is different from vanilla Parquet: `relation.dataSchema` on a Delta
    // table INCLUDES partition columns, but the physical parquet files on disk do NOT.
    // So we compute the actual file schema by subtracting the partition columns from
    // `relation.dataSchema`. Mirrors what delta-kernel itself reports as the scan schema.
    val partitionNames =
      relation.partitionSchema.fields.map(_.name.toLowerCase(Locale.ROOT)).toSet
    val fileDataSchemaFields =
      relation.dataSchema.fields.filterNot(f =>
        partitionNames.contains(f.name.toLowerCase(Locale.ROOT)))

    val dataSchema = schema2Proto(fileDataSchemaFields)
    val requiredSchema = schema2Proto(scan.requiredSchema.fields)
    val partitionSchema = schema2Proto(relation.partitionSchema.fields)
    commonBuilder.addAllDataSchema(dataSchema.toIterable.asJava)
    commonBuilder.addAllRequiredSchema(requiredSchema.toIterable.asJava)
    commonBuilder.addAllPartitionSchema(partitionSchema.toIterable.asJava)

    // Projection vector maps output positions to (file_data_schema ++ partition_schema)
    // indices. Spark's `FileSourceScanExec` splits its visible schema into
    // `requiredSchema` (data-only columns that must be read from parquet) and an
    // implicit partition tail that is materialised from `PartitionedFile.partition_values`.
    // The scan's `output` is `requiredSchema ++ partitionSchema` in that order.
    //
    // We mirror that layout: first emit one index per required (data) field pointing
    // into `fileDataSchemaFields`, then append one index per partition field pointing
    // at `fileDataSchemaFields.length + partitionIdx` so the native side resolves those
    // positions against `PartitionedFile.partition_values`.
    //
    // If `scan.requiredSchema` ever contains a partition column (some Delta code paths
    // leak one in), we resolve it through the partition tail without re-reading from
    // parquet.
    val partitionNameToIndex: Map[String, Int] =
      relation.partitionSchema.fields.zipWithIndex.map { case (f, i) =>
        f.name.toLowerCase(Locale.ROOT) -> i
      }.toMap
    val requiredIndexes: Seq[Int] = scan.requiredSchema.fields.map { field =>
      val nameLower = field.name.toLowerCase(Locale.ROOT)
      val dataIdx =
        fileDataSchemaFields.indexWhere(_.name.toLowerCase(Locale.ROOT) == nameLower)
      if (dataIdx >= 0) {
        dataIdx
      } else {
        partitionNameToIndex
          .get(nameLower)
          .map(p => fileDataSchemaFields.length + p)
          .getOrElse(-1)
      }
    }
    val partitionTailIndexes: Seq[Int] =
      relation.partitionSchema.fields.indices.map(i => fileDataSchemaFields.length + i)
    val projectionVector: Seq[Int] = requiredIndexes ++ partitionTailIndexes
    commonBuilder.addAllProjectionVector(
      projectionVector.map(idx => idx.toLong.asInstanceOf[java.lang.Long]).toIterable.asJava)

    // Pushed-down data filters. Gated by Spark's parquet filter pushdown config, same as
    // CometNativeScan, so we behave consistently across scan implementations.
    if (scan.conf.getConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED) &&
      CometConf.COMET_RESPECT_PARQUET_FILTER_PUSHDOWN.get(scan.conf)) {
      val dataFilters = new ListBuffer[Expr]()
      scan.supportedDataFilters.foreach { filter =>
        exprToProto(filter, scan.output) match {
          case Some(proto) => dataFilters += proto
          case _ => logWarning(s"CometDeltaNativeScan: unsupported data filter $filter")
        }
      }
      commonBuilder.addAllDataFilters(dataFilters.asJava)
    }

    storageOptions.asScala.foreach { case (key, value) =>
      commonBuilder.putObjectStoreOptions(key, value)
    }

    // Phase 4: pass column mapping from kernel through to the native planner.
    val columnMappings = taskList.getColumnMappingsList.asScala
    columnMappings.foreach { cm =>
      commonBuilder.addColumnMappings(
        OperatorOuterClass.DeltaColumnMapping
          .newBuilder()
          .setLogicalName(cm.getLogicalName)
          .setPhysicalName(cm.getPhysicalName)
          .build())
    }

    // --- 3. Pack into a DeltaScan with COMMON ONLY (split-mode, Phase 5).
    // Tasks are NOT included in the proto at planning time. They'll be
    // serialized per-partition in CometDeltaNativeScanExec.serializedPartitionData
    // at execution time, and merged via DeltaPlanDataInjector.
    val deltaScanBuilder = DeltaScan.newBuilder()
    deltaScanBuilder.setCommon(commonBuilder.build())
    // No addAllTasks: tasks stay in taskListBytes for the exec's lazy split.

    // Stash the full task-list bytes for createExec to retrieve. The ThreadLocal
    // bridges the convert() -> createExec() gap in CometExecRule.convertToComet.
    // Build a modified taskList with ONLY the filtered tasks (partition-pruned).
    val filteredTaskList = OperatorOuterClass.DeltaScanTaskList
      .newBuilder()
      .setSnapshotVersion(taskList.getSnapshotVersion)
      .setTableRoot(taskList.getTableRoot)
      .addAllTasks(filteredTasks.asJava)
      .addAllColumnMappings(taskList.getColumnMappingsList)
      .addAllUnsupportedFeatures(taskList.getUnsupportedFeaturesList)
      .build()
    lastTaskListBytes.set(filteredTaskList.toByteArray)

    builder.clearChildren()
    Some(builder.setDeltaScan(deltaScanBuilder.build()).build())
  }

  /**
   * Filter `tasks` down to the subset whose partition values satisfy Spark's
   * `scan.partitionFilters`. Returns the original list unchanged when the scan has no partition
   * filters.
   *
   * Delta stores partition values as strings inside add actions, so we parse each value into the
   * correct Catalyst type using `castPartitionString` below before feeding it to an
   * `InterpretedPredicate`. Only values for fields actually referenced by the predicate need
   * parsing, but we do the full row for simplicity.
   */
  private def prunePartitions(
      tasks: Seq[OperatorOuterClass.DeltaScanTask],
      scan: CometScanExec,
      partitionSchema: StructType): Seq[OperatorOuterClass.DeltaScanTask] = {
    if (scan.partitionFilters.isEmpty || partitionSchema.isEmpty) return tasks

    // Phase 5b: filter out DPP expressions (DynamicPruningExpression wrapping
    // InSubqueryExec) because they aren't resolved at planning time. Spark
    // applies them post-scan at runtime. Static partition filters are still
    // evaluated here for file-level pruning.
    val staticFilters = scan.partitionFilters.filterNot(
      _.exists(_.isInstanceOf[org.apache.spark.sql.catalyst.expressions.PlanExpression[_]]))
    if (staticFilters.isEmpty) return tasks

    // Build an `InterpretedPredicate` that expects a row whose schema matches
    // `partitionSchema`. Rewrite attribute references to `BoundReference`s keyed by
    // partition-schema field index, respecting case sensitivity.
    val caseSensitive = scan.conf.getConf[Boolean](SQLConf.CASE_SENSITIVE)
    val combined = staticFilters.reduce(And)
    val bound = combined.transform {
      case a: org.apache.spark.sql.catalyst.expressions.AttributeReference =>
        val idx = if (caseSensitive) {
          partitionSchema.fieldIndex(a.name)
        } else {
          partitionSchema.fields.indexWhere(
            _.name.toLowerCase(Locale.ROOT) == a.name.toLowerCase(Locale.ROOT))
        }
        if (idx < 0) return tasks // Can't resolve; skip pruning
        BoundReference(idx, partitionSchema(idx).dataType, partitionSchema(idx).nullable)
    }
    val predicate = InterpretedPredicate(bound)
    predicate.initialize(0)

    tasks.filter { task =>
      val row = InternalRow.fromSeq(partitionSchema.fields.toSeq.map { field =>
        val proto = task.getPartitionValuesList.asScala.find(_.getName == field.name)
        val strValue =
          if (proto.exists(_.hasValue)) Some(proto.get.getValue) else None
        castPartitionString(strValue, field.dataType)
      })
      predicate.eval(row)
    }
  }

  private def castPartitionString(str: Option[String], dt: DataType): Any =
    DeltaReflection.castPartitionString(str, dt)

  /**
   * Build a kernel-independent `DeltaScanTaskList` from a caller-provided AddFile list. Used when
   * the Delta scan has a pre-materialized FileIndex (streaming micro-batch, MERGE/UPDATE/DELETE
   * post-join) so we can honour its exact file list instead of re-running log replay (which would
   * return a different set).
   *
   * Each AddFile becomes one `DeltaScanTask`. Absolute path resolution mirrors
   * `DeltaFileOperations.absolutePath`: if `AddFile.path` is already absolute (has a URI scheme),
   * keep it verbatim; otherwise join against `tableRoot`.
   */
  private def buildTaskListFromAddFiles(
      tableRoot: String,
      snapshotVersion: Long,
      addFiles: Seq[DeltaReflection.ExtractedAddFile],
      nativeOp: AnyRef,
      columnNames: Array[String]): OperatorOuterClass.DeltaScanTaskList = {
    val tlBuilder = OperatorOuterClass.DeltaScanTaskList.newBuilder()
    tlBuilder.setTableRoot(tableRoot)
    if (snapshotVersion >= 0) tlBuilder.setSnapshotVersion(snapshotVersion)

    addFiles.foreach { af =>
      val absPath =
        if (af.path.contains(":/")) af.path
        else {
          val sep = if (tableRoot.endsWith("/")) "" else "/"
          tableRoot + sep + af.path
        }
      val taskBuilder = OperatorOuterClass.DeltaScanTask.newBuilder()
      taskBuilder.setFilePath(absPath)
      taskBuilder.setFileSize(af.size)
      DeltaReflection.parseNumRecords(af.statsJson).foreach(taskBuilder.setRecordCount)
      af.partitionValues.foreach { case (k, v) =>
        val pvBuilder = OperatorOuterClass.DeltaPartitionValue.newBuilder().setName(k)
        if (v != null) pvBuilder.setValue(v)
        taskBuilder.addPartitionValues(pvBuilder.build())
      }
      af.baseRowId.foreach(taskBuilder.setBaseRowId)
      af.defaultRowCommitVersion.foreach(taskBuilder.setDefaultRowCommitVersion)
      tlBuilder.addTasks(taskBuilder.build())
    }
    tlBuilder.build()
  }

  override def createExec(nativeOp: Operator, op: CometScanExec): CometNativeExec = {
    val tableRoot = DeltaReflection.extractTableRoot(op.relation).getOrElse("unknown")
    val tlBytes =
      try {
        Option(lastTaskListBytes.get()).getOrElse(Array.emptyByteArray)
      } finally {
        lastTaskListBytes.remove()
      }

    val dppFilters = op.partitionFilters.filter(
      _.exists(_.isInstanceOf[org.apache.spark.sql.catalyst.expressions.PlanExpression[_]]))
    val partitionSchema = op.relation.partitionSchema

    CometDeltaNativeScanExec(
      nativeOp,
      op.output,
      org.apache.spark.sql.comet.SerializedPlan(None),
      op.wrapped,
      tableRoot,
      tlBytes,
      dppFilters,
      partitionSchema)
  }
}
