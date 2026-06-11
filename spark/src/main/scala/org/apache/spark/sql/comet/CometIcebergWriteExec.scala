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

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.connector.write.{BatchWrite, WriterCommitMessage}
import org.apache.spark.sql.execution.{ColumnarToRowTransition, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.vectorized.ColumnarBatch

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

  // Names mirror Spark's stock `BatchWriteHelper` metrics (`numFiles` / `numOutputRows` /
  // `numOutputBytes`) so the Spark SQL UI shows the same row as it would for a non-Comet
  // Iceberg write.
  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numFiles" -> SQLMetrics.createMetric(sparkContext, "number of files written"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "written output"))

  override def doExecute(): RDD[InternalRow] = {
    val columnarRdd = doExecuteColumnar()
    val specId = partitionSpecId
    val sortOrderId = nativeOp.getIcebergWrite.getCommon.getSortOrderId
    val schemaTypes = output.map(_.dataType).toArray
    val filesMetric = longMetric("numFiles")
    val rowsMetric = longMetric("numOutputRows")
    val bytesMetric = longMetric("numOutputBytes")
    columnarRdd.mapPartitionsInternal { batches =>
      // Per-task: drain the native output (single-row Binary column carrying one V2 data
      // manifest), decode it via Iceberg's `ManifestFiles.read` to recover `DataFile`s, stamp
      // the table's `sort_order_id` on each (iceberg-rust's writer doesn't expose the field, so
      // we re-apply it here to match what `SparkWrite` would have stamped on the stock path),
      // wrap them in `SparkWrite$TaskCommit` via reflection (the constructor is package-
      // private), Java-serialize the message so the outer `IcebergCommitExec` consumes it
      // identically to the JVM-path output. Empty manifests (zero data files written) are
      // still committed; the surrounding `BatchWrite.commit(messages)` handles the no-op case
      // correctly.
      val manifestBytes = drainAvroPayload(batches)
      val proj = UnsafeProjection.create(schemaTypes)
      val dataFiles =
        if (manifestBytes.isEmpty) new java.util.ArrayList[AnyRef]()
        else IcebergReflection.decodeManifestToDataFiles(manifestBytes, specId)
      IcebergReflection.stampSortOrderId(dataFiles, sortOrderId)
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

      val nativeMetrics = CometMetricNode.fromCometPlan(this)
      Option(TaskContext.get()).foreach(nativeMetrics.reportNativeWriteOutputMetrics)

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
        Seq(iter),
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
