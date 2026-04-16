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
import org.apache.spark.sql.execution.{FileSourceScanExec, InSubqueryExec}
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
    partitionSchema: StructType = new StructType())
    extends CometLeafExec {

  override val supportsColumnar: Boolean = true

  override val nodeName: String = s"CometDeltaNativeScan $tableRoot"

  override protected def doPrepare(): Unit = {
    dppFilters.foreach {
      case DynamicPruningExpression(e: InSubqueryExec) =>
        e.plan.prepare()
      case _ =>
    }
    super.doPrepare()
  }

  @transient private lazy val commonBytes: Array[Byte] =
    nativeOp.getDeltaScan.getCommon.toByteArray

  @transient private lazy val allTasks: Seq[OperatorOuterClass.DeltaScanTask] =
    OperatorOuterClass.DeltaScanTaskList.parseFrom(taskListBytes).getTasksList.asScala.toSeq

  /**
   * Build per-partition bytes from the current DPP-pruned task list. DPP filters that are still
   * `SubqueryAdaptiveBroadcastExec` placeholders at planning time materialise lazily once AQE
   * runs the broadcast; by recomputing this at `doExecuteColumnar` (rather than memoising the
   * result in a lazy val) we pick up the resolved values and actually skip partitions, instead of
   * reading the full table every time AQE is in the loop.
   */
  private def buildPerPartitionBytes(): Array[Array[Byte]] = {
    val tasks =
      if (dppFilters.nonEmpty && partitionSchema.nonEmpty) applyDppFilters(allTasks)
      else allTasks
    if (tasks.isEmpty) {
      Array.empty[Array[Byte]]
    } else {
      tasks.map { task =>
        OperatorOuterClass.DeltaScan
          .newBuilder()
          .addTasks(task)
          .build()
          .toByteArray
      }.toArray
    }
  }

  // Planning-time snapshot used by metrics, sourceKey derivations, and `numPartitions`.
  // Execution-time recomputation happens inside `doExecuteColumnar`.
  @transient private lazy val planningPerPartitionBytes: Array[Array[Byte]] =
    buildPerPartitionBytes()

  private def applyDppFilters(
      tasks: Seq[OperatorOuterClass.DeltaScanTask]): Seq[OperatorOuterClass.DeltaScanTask] = {
    // If any DPP subquery is still a `SubqueryAdaptiveBroadcastExec` placeholder,
    // AQE hasn't yet replaced it with the real broadcast plan. We can't execute
    // it ourselves (that plan's `doExecute` throws), so skip pruning for this
    // batch — the scan just reads all tasks, which is correct but slower.
    val hasUnresolvedAdaptive = dppFilters.exists {
      case DynamicPruningExpression(inSub: InSubqueryExec) =>
        inSub.plan.isInstanceOf[org.apache.spark.sql.execution.SubqueryAdaptiveBroadcastExec]
      case _ => false
    }
    if (hasUnresolvedAdaptive) return tasks
    dppFilters.foreach {
      case DynamicPruningExpression(inSub: InSubqueryExec) if inSub.values().isEmpty =>
        inSub.updateResult()
      case _ =>
    }

    val resolvedFilters = dppFilters.map {
      case DynamicPruningExpression(e) => e
      case other => other
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
    org.apache.comet.delta.DeltaReflection.castPartitionString(str, dt)

  def commonData: Array[Byte] = commonBytes
  def perPartitionData: Array[Array[Byte]] = planningPerPartitionBytes

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

  def numPartitions: Int = perPartitionData.length

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

    val baseMetrics = Map(
      "output_rows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "num_splits" -> SQLMetrics.createMetric(sparkContext, "number of file splits processed"))

    val planningMetrics = if (taskList != null) {
      val totalFiles = new ImmutableSQLMetric("sum")
      totalFiles.set(taskList.getTasksCount.toLong)
      sparkContext.register(totalFiles, "total files")

      val dvFiles = new ImmutableSQLMetric("sum")
      dvFiles.set(taskList.getTasksList.asScala.count(!_.getDeletedRowIndexesList.isEmpty).toLong)
      sparkContext.register(dvFiles, "files with deletion vectors")

      Map("total_files" -> totalFiles, "dv_files" -> dvFiles)
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
    val baseRDD = CometExecRDD(
      sparkContext,
      inputRDDs = Seq.empty,
      commonByKey = Map(sourceKey -> commonData),
      perPartitionByKey = Map(sourceKey -> execPerPartitionBytes),
      serializedPlan = serializedPlan,
      numPartitions = execPerPartitionBytes.length,
      numOutputCols = output.length,
      nativeMetrics = nativeMetrics,
      subqueries = Seq.empty)

    // InputFileBlockHolder for downstream `input_file_name()` is populated in
    // `CometExecRDD.setInputFileForDeltaScan` so it also fires when this scan
    // is embedded inside a larger Comet native tree (where this exec's own
    // `doExecuteColumnar` is bypassed in favour of the parent's).
    baseRDD
  }

  override def convertBlock(): CometDeltaNativeScanExec = {
    val newSerializedPlan = if (serializedPlanOpt.isEmpty) {
      val bytes = CometExec.serializeNativePlan(nativeOp)
      SerializedPlan(Some(bytes))
    } else {
      serializedPlanOpt
    }
    CometDeltaNativeScanExec(
      nativeOp,
      output,
      newSerializedPlan,
      originalPlan,
      tableRoot,
      taskListBytes,
      dppFilters,
      partitionSchema)
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
