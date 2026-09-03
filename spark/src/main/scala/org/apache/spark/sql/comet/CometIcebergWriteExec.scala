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

import scala.jdk.CollectionConverters._

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.comet.execution.arrow.CometArrowStream
import org.apache.spark.sql.comet.util.{Utils => CometUtils}
import org.apache.spark.sql.connector.write.{BatchWrite, WriterCommitMessage}
import org.apache.spark.sql.execution.{ColumnarToRowTransition, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.TaskFailureListener

import com.google.protobuf.CodedOutputStream

import org.apache.comet.CometExecIterator
import org.apache.comet.iceberg.IcebergReflection
import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * Native variant of [[IcebergWriteExec]]. Drives the iceberg-rust writer stack via Comet's native
 * execution pipeline; the JVM side decodes the per-task Avro-encoded `DataFile` blob the native
 * operator emits and packages it as a [[WriterCommitMessage]] so the outer [[IcebergCommitExec]]
 * consumes it unchanged.
 *
 * Selected by [[org.apache.comet.serde.operator.CometIcebergNativeWrite]] when the table's
 * properties allow it (parquet, V2, no encryption, ...) and the child plan is fully Comet-native;
 * otherwise the JVM path's [[IcebergWriteExec]] runs instead.
 *
 * @param nativeOp
 *   Template operator carrying the `IcebergWrite` proto. Per-task `partition_id` /
 *   `task_attempt_id` get stamped on a copy at execution time.
 * @param child
 *   Comet native child (must be a [[CometNativeExec]] so columnar batches flow through FFI).
 * @param batchWrite
 *   Shared with the outer [[IcebergCommitExec]] -- the same instance the strategy materialised
 *   via `write.toBatch`. Used here only to provide the `dataLocation` / partition spec needed by
 *   the native side; never invoked for commit.
 * @param partitionSpecId
 *   Output partition spec id (from `SparkWrite.outputSpecId`). Decoded `DataFile`s are stamped
 *   with this spec id; required because iceberg-rust's `DataFile` is spec-agnostic at the wire.
 */
case class CometIcebergWriteExec(
    nativeOp: Operator,
    child: SparkPlan,
    @transient batchWrite: BatchWrite,
    @transient table: AnyRef,
    partitionSpecId: Int)
    extends CometNativeExec
    with UnaryExecNode
    // We consume Arrow batches (via FFI) and emit row-shaped commit messages, so we are a
    // columnar-to-row transition. Without this trait Spark's
    // `ApplyColumnarRulesAndInsertTransitions` wedges a `CometNativeColumnarToRowExec` between
    // us and the Comet-native child, which would then fail `child.executeColumnar()` in
    // `doExecuteColumnar`.
    with ColumnarToRowTransition {

  override def originalPlan: SparkPlan = child

  // Same output schema as IcebergWriteExec so the outer IcebergCommitExec consumes the
  // commit messages identically regardless of which inner exec emitted them.
  override def output: Seq[Attribute] = Seq(
    AttributeReference(IcebergWriteExec.CommitMessageColumn, BinaryType, nullable = false)())

  // Native exec emits a single Binary column; the surrounding command framework expects rows, so
  // the outer commit exec calls executeCollect on us. supportsColumnar = false keeps Spark from
  // inserting a ColumnarToRow that would clash with our (Nil-output-like) row contract.
  override def supportsColumnar: Boolean = false

  override def executeCollect(): Array[InternalRow] = {
    val rdd = doExecute()
    // SparkPlan.executeCollect defaults to byteArrayRdd which goes through UnsafeRow encoding;
    // doExecute already projects each row through UnsafeProjection (see the per-task closure
    // below) so a plain `collect()` is safe.
    rdd.collect()
  }

  override def serializedPlanOpt: SerializedPlan = {
    val size = nativeOp.getSerializedSize
    val bytes = new Array[Byte](size)
    val codedOutput = CodedOutputStream.newInstance(bytes)
    nativeOp.writeTo(codedOutput)
    codedOutput.checkNoSpaceLeft()
    SerializedPlan(Some(bytes))
  }

  override def withNewChildInternal(newChild: SparkPlan): SparkPlan = copy(child = newChild)

  override def nodeName: String = "CometIcebergWrite"

  // Never let TreeNode.argString fall through to the protobuf's toString: the TextFormat dump
  // carries catalog_properties -- including fs.s3a.* credentials translated from the Hadoop
  // configuration -- and Spark's argString redaction only applies to Scala Maps, so the secrets
  // would land verbatim in explain(), the SQL UI node description, and the event log. Mirror
  // CometIcebergNativeScanExec: emit output plus a short human-readable descriptor instead.
  override def stringArgs: Iterator[Any] = {
    val common = nativeOp.getIcebergWrite.getCommon
    Iterator(output, s"${common.getDataLocation}, ${common.getWriterMode}")
  }

  // Names mirror Spark's stock `BatchWriteHelper` metrics (`numFiles` / `numOutputRows` /
  // `numOutputBytes`) so the Spark SQL UI shows the same row as it would for a non-Comet
  // Iceberg write. `write_time` is pushed from the native operator by name (see
  // `iceberg_write.rs`).
  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numFiles" -> SQLMetrics.createMetric(sparkContext, "number of files written"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "written output"),
    "write_time" -> SQLMetrics.createNanoTimingMetric(
      sparkContext,
      "time in native Iceberg writer"))

  override def doExecute(): RDD[InternalRow] = {
    val columnarRdd = doExecuteColumnar()
    val specId = partitionSpecId
    val sortOrderId = nativeOp.getIcebergWrite.getCommon.getSortOrderId
    val schemaTypes = output.map(_.dataType).toArray
    val filesMetric = longMetric("numFiles")
    val rowsMetric = longMetric("numOutputRows")
    val bytesMetric = longMetric("numOutputBytes")
    // Resolved on the driver (they need the non-serializable `Table`) and shipped to the task
    // closure: Iceberg requires `FileIO`, `MetricsConfig`, `PartitionSpec`, and `Schema` to be
    // Serializable. They feed the executor-side manifest-metrics rebuild below.
    val tableIO = IcebergReflection
      .getTableIO(table)
      .getOrElse(throw new IllegalStateException("Native Iceberg write: Table.io() unavailable"))
    val metricsConfig = IcebergReflection
      .metricsConfigForTable(table)
      .getOrElse(
        throw new IllegalStateException("Native Iceberg write: MetricsConfig.forTable failed"))
    val outputSpec = IcebergReflection
      .getPartitionSpecById(table, specId)
      .getOrElse(
        throw new IllegalStateException(s"Native Iceberg write: no partition spec id=$specId"))
      .asInstanceOf[AnyRef]
    // Prefer the schema the write was planned against over the table's current schema, so a
    // schema change between planning and execution cannot misclassify float/double columns.
    val writeSchema = IcebergReflection
      .getOuterSparkWrite(batchWrite)
      .flatMap(IcebergReflection.getWriteSchemaFromSparkWrite)
      .orElse(IcebergReflection.getSchema(table))
      .getOrElse(throw new IllegalStateException("Native Iceberg write: no write schema"))
      .asInstanceOf[AnyRef]
    val sortOrder = IcebergReflection
      .getSortOrderById(table, sortOrderId)
      .getOrElse(
        throw new IllegalStateException(s"Native Iceberg write: no sort order id=$sortOrderId"))
    columnarRdd.mapPartitionsInternal { batches =>
      // Per-task: drain the native output (single-row Binary column carrying one V2 data
      // manifest), decode it via Iceberg's `ManifestFiles.read` to recover `DataFile`s, rebuild
      // each file's metrics into the ones iceberg-java's own writer would have committed
      // (re-derived from the written parquet footer through the version-matched
      // `ParquetUtil.footerMetrics` + `MetricsConfig`, with float/double NaN counts and bounds
      // carried over from the rust writer) while re-applying the write's `SortOrder` (which
      // iceberg-rust doesn't stamp), wrap them in `SparkWrite$TaskCommit` via reflection
      // (the constructor is package-private), and Java-serialize the message so the outer
      // `IcebergCommitExec` consumes it identically to the JVM-path output. Empty manifests
      // (zero data files written) are still committed; the surrounding
      // `BatchWrite.commit(messages)` handles the no-op case correctly.
      val manifestBytes = drainAvroPayload(batches)
      val proj = UnsafeProjection.create(schemaTypes)
      val decoded =
        if (manifestBytes.isEmpty) new java.util.ArrayList[AnyRef]()
        else IcebergReflection.decodeManifestToDataFiles(manifestBytes, specId)
      // From here on the JVM knows which files iceberg-rust wrote. If this task fails before its
      // commit message reaches the committer (metrics rebuild, `TaskCommit` construction,
      // serialization), delete them the way iceberg-java's `DataWriter.abort()` would; failures
      // inside the native writer itself are cleaned up on the native side.
      Option(TaskContext.get()).foreach { tc =>
        tc.addTaskFailureListener(new TaskFailureListener {
          override def onTaskFailure(context: TaskContext, error: Throwable): Unit = {
            val locations =
              decoded.asScala.flatMap(f => IcebergReflection.extractFileLocation(f)).toSeq
            IcebergReflection.deleteFilesQuietly(
              tableIO,
              locations,
              s"failed Iceberg write task ${context.partitionId()} " +
                s"(attempt ${context.attemptNumber()})")
          }
        })
      }
      val dataFiles = IcebergReflection.rebuildDataFilesWithJavaMetrics(
        decoded,
        tableIO,
        metricsConfig,
        outputSpec,
        writeSchema,
        sortOrder)
      val (rowSum, byteSum) = IcebergReflection.sumDataFileMetrics(dataFiles)
      filesMetric.add(dataFiles.size().toLong)
      rowsMetric.add(rowSum)
      bytesMetric.add(byteSum)
      // Mirror iceberg-java's `SparkWrite$TaskCommit.reportOutputMetrics()`: surface the same
      // bytes/records counters via Spark's per-task `OutputMetrics` so the Spark UI "Output"
      // column and `SparkListenerTaskEnd` payloads match the JVM-path writer.
      Option(TaskContext.get()).foreach { tc =>
        val outputMetrics = tc.taskMetrics().outputMetrics
        outputMetrics.setBytesWritten(byteSum)
        outputMetrics.setRecordsWritten(rowSum)
      }
      val taskCommit = IcebergReflection.buildTaskCommit(dataFiles)
      val serialized =
        IcebergWriteExec.serializeMessage(taskCommit.asInstanceOf[WriterCommitMessage])
      Iterator.single(proj(InternalRow(serialized)).copy())
    }
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val childRDD = if (child.supportsColumnar) {
      child.executeColumnar()
    } else {
      throw new UnsupportedOperationException(
        "CometIcebergWriteExec requires a columnar (Comet native) child; got " +
          child.getClass.getName)
    }

    val numPartitions = childRDD.getNumPartitions
    // Native side emits a single Binary column carrying the per-task Avro `DataFile` blob.
    val numOutputCols = 1
    val capturedNativeOp = nativeOp

    childRDD.mapPartitionsInternal { iter =>
      val partitionId = TaskContext.getPartitionId()
      val taskAttemptId = TaskContext.get().taskAttemptId()

      // No child metric nodes: the write's native tree (IcebergWrite -> Projection -> Scan) does
      // not line up with the JVM child operators, which own their own native blocks; zipping
      // them by index would overwrite the child operators' SQLMetrics with the write block's.
      // The root node still receives the native operator's own metrics by name (`write_time`);
      // the file/row/byte SQL metrics and the task OutputMetrics are populated in doExecute from
      // the decoded manifest instead.
      val nativeMetrics = CometMetricNode(metrics, Nil)

      val taskNativeOp = {
        val icebergWrite = capturedNativeOp.getIcebergWrite.toBuilder
          .setPartitionId(partitionId)
          .setTaskAttemptId(taskAttemptId)
          .build()
        capturedNativeOp.toBuilder.setIcebergWrite(icebergWrite).build()
      }

      val size = taskNativeOp.getSerializedSize
      val planBytes = new Array[Byte](size)
      val codedOutput = CodedOutputStream.newInstance(planBytes)
      taskNativeOp.writeTo(codedOutput)
      codedOutput.checkNoSpaceLeft()

      val execIterator = new CometExecIterator(
        CometExec.newIterId,
        CometArrowStream.inputObjects(
          iter,
          CometUtils.fromAttributes(child.output),
          "CometIcebergWriteExec"),
        numOutputCols,
        planBytes,
        nativeMetrics,
        numPartitions,
        partitionId,
        None,
        Seq.empty)

      execIterator
    }
  }

  /**
   * Drain a partition's columnar output. `iceberg_write.rs` always emits exactly one batch with
   * one `BINARY` row carrying the per-task V2 data manifest (possibly empty bytes when no data
   * files were written). The asserts guard against a future native-side change.
   */
  private def drainAvroPayload(batches: Iterator[ColumnarBatch]): Array[Byte] = {
    require(batches.hasNext, "iceberg_write produced no output batch for this task")
    val batch = batches.next()
    val payload =
      try {
        require(
          batch.numRows() == 1,
          s"iceberg_write expected exactly 1 row per task, got ${batch.numRows()}")
        batch.column(0).getBinary(0)
      } finally {
        batch.close()
      }
    require(!batches.hasNext, "iceberg_write produced more than one batch for this task")
    payload
  }
}
