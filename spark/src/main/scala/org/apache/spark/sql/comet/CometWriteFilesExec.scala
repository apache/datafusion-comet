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

import java.util.Date

import scala.jdk.CollectionConverters._

import org.apache.hadoop.mapreduce.{TaskAttemptContext, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.TaskContext
import org.apache.spark.internal.io.{FileCommitProtocol, FileNameSpec, SparkHadoopWriterUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.comet.execution.arrow.CometArrowStream
import org.apache.spark.sql.comet.util.{Utils => CometUtils}
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{BasicWriteTaskStatsTracker, ExecutedWriteSummary, WriteFilesSpec, WriteJobDescription, WriteTaskResult, WriteTaskStatsTracker}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

import com.google.protobuf.CodedOutputStream

import org.apache.comet.CometExecIterator
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.shims.ShimCometWriteFilesExec

/**
 * Comet's replacement for Spark's `WriteFilesExec`: writes Parquet files natively for one task,
 * and nothing else.
 *
 * Everything around the per-task write stays on Spark's side. Because this node extends
 * `WriteFilesExecBase` (see [[ShimCometWriteFilesExec]]), `V1WritesUtils.getWriteFilesOpt` finds
 * it, so `InsertIntoHadoopFsRelationCommand.run` and `FileFormatWriter` continue to own:
 *
 *   - SaveMode semantics and the delete-before-overwrite of the target directory
 *   - committer instantiation (including `spark.sql.sources.commitProtocolClass`), `setupJob`,
 *     `commitJob`/`abortJob`, `onTaskCommit`, and the `_SUCCESS` marker
 *   - dynamic partition overwrite and custom partition locations
 *   - `WriteJobStatsTracker` aggregation, SQL metrics, and catalog statistics/cache refresh
 *
 * This node mirrors `FileFormatWriter.executeTask` for the parts Comet must do itself: build the
 * `TaskAttemptContext`, ask the commit protocol where to write, run the native writer, drive the
 * stats trackers, and commit or abort the task. Notably the path handed back by
 * [[FileCommitProtocol.newTaskTempFile]] is used verbatim, so staging directories, task-attempt
 * isolation under speculation, and committers that track individual files all behave as they do
 * for Spark's own writer.
 *
 * @param nativeOp
 *   Template for the native write plan. `output_path` is a placeholder here and is replaced per
 *   task with the path the commit protocol chose.
 * @param child
 *   The Comet native operator producing the batches to write.
 */
case class CometWriteFilesExec(nativeOp: Operator, child: SparkPlan)
    extends CometNativeExec
    with ShimCometWriteFilesExec {

  override def originalPlan: SparkPlan = child

  override def nodeName: String = "CometWriteFiles"

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "files_written" -> SQLMetrics.createMetric(sparkContext, "number of written data files"),
    "bytes_written" -> SQLMetrics.createSizeMetric(sparkContext, "written data"),
    "rows_written" -> SQLMetrics.createMetric(sparkContext, "number of written rows"))

  override def serializedPlanOpt: SerializedPlan = SerializedPlan(Some(serialize(nativeOp)))

  override def withNewChildInternal(newChild: SparkPlan): SparkPlan = copy(child = newChild)

  /**
   * Spark drives this node through `executeWrite`, never `execute`. `WriteFilesExecBase` already
   * throws for `doExecute`, but `CometExec` widens it to a public member that returns a
   * `ColumnarToRowExec` result, so the conflict has to be resolved explicitly here.
   */
  override def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException(s"$nodeName does not support doExecute")

  override protected def doExecuteWrite(
      writeFilesSpec: WriteFilesSpec): RDD[WriterCommitMessage] = {
    val description = writeFilesSpec.description
    val committer = writeFilesSpec.committer
    // Same identifier scheme as FileFormatWriter, so committers that parse the job ID agree.
    val jobTrackerID = SparkHadoopWriterUtils.createJobTrackerID(new Date())

    val childRDD = child.executeColumnar()
    val numPartitions = childRDD.getNumPartitions
    val numOutputCols = child.output.length
    val childSchema = CometUtils.fromAttributes(child.output)
    val nativeOpTemplate = nativeOp

    // Column names come from the write job description, not from the query output: for
    // `INSERT INTO t SELECT ...` the query may name columns after the expressions that produced
    // them, while the file must carry the target table's column names.
    val dataColumnNames = description.dataColumns.map(_.name)
    assert(
      dataColumnNames.length == numOutputCols,
      s"Expected ${dataColumnNames.length} data columns to write but the child produces " +
        s"$numOutputCols")

    childRDD.mapPartitionsInternal { batches =>
      val taskCtx = TaskContext.get()
      val sparkPartitionId = taskCtx.partitionId()
      val taskAttemptContext = createTaskAttemptContext(
        description,
        jobTrackerID,
        taskCtx.stageId(),
        sparkPartitionId,
        // Truncation to Int matches FileFormatWriter: the masked low bits are what the Hadoop
        // TaskAttemptID accepts, and uniqueness within a job is preserved by the task ID.
        taskCtx.taskAttemptId().toInt & Integer.MAX_VALUE)

      committer.setupTask(taskAttemptContext)
      val statsTrackers = description.statsTrackers.map(_.newTaskInstance())

      try {
        // Mirrors FileFormatWriter's EmptyDirectoryDataWriter case: an empty input still writes
        // one file from partition 0 so that the output carries the schema, but every other empty
        // partition produces no file at all.
        val writeFile = sparkPartitionId == 0 || batches.hasNext

        val writtenFile = if (writeFile) {
          val ext = description.outputWriterFactory.getFileExtension(taskAttemptContext)
          // FileNameSpec's "-c000" suffix reproduces Spark's part-<id>-<uuid>-c000.<codec>.parquet
          // naming. The file counter is always 0 until file rolling is supported.
          val filePath =
            committer.newTaskTempFile(taskAttemptContext, None, FileNameSpec("", "-c000" + ext))

          statsTrackers.foreach(_.newFile(filePath))
          val rowsWritten = writeNatively(
            nativeOpTemplate,
            filePath,
            dataColumnNames,
            batches,
            childSchema,
            numOutputCols,
            numPartitions,
            sparkPartitionId)
          recordRows(statsTrackers, filePath, rowsWritten)
          statsTrackers.foreach(_.closeFile(filePath))
          Some(filePath)
        } else {
          // Drain so the child's native execution completes and releases its resources.
          batches.foreach(_.close())
          None
        }

        val (taskCommitMessage, taskCommitTime) = Utils.timeTakenMs {
          committer.commitTask(taskAttemptContext)
        }
        logDebug(
          s"Task ${taskAttemptContext.getTaskAttemptID} committed " +
            writtenFile.getOrElse("no file"))

        Iterator(
          WriteTaskResult(
            taskCommitMessage,
            ExecutedWriteSummary(
              // Only non-partitioned writes are supported so far, so no partition paths were
              // added. Populating this is part of adding partitioned write support.
              updatedPartitions = Set.empty,
              stats = statsTrackers.map(_.getFinalStats(taskCommitTime)))))
      } catch {
        case t: Throwable =>
          Utils.tryLogNonFatalError(committer.abortTask(taskAttemptContext))
          logError(s"Task ${taskAttemptContext.getTaskAttemptID} aborted: ${t.getMessage}", t)
          throw t
      }
    }
  }

  /**
   * Run the native write plan for one task, returning the number of rows written.
   *
   * The row count is taken on the JVM side as batches are pulled into native code. That is exact
   * because the native writer consumes its whole input before completing.
   */
  private def writeNatively(
      template: Operator,
      filePath: String,
      dataColumnNames: Seq[String],
      batches: Iterator[ColumnarBatch],
      childSchema: StructType,
      numOutputCols: Int,
      numPartitions: Int,
      partitionId: Int): Long = {
    val parquetWriter = template.getParquetWriter.toBuilder
      .setOutputPath(filePath)
      .clearColumnNames()
      .addAllColumnNames(dataColumnNames.asJava)
      .build()
    val taskOp = template.toBuilder.setParquetWriter(parquetWriter).build()

    var rowsWritten = 0L
    val countingBatches = batches.map { batch =>
      rowsWritten += batch.numRows()
      batch
    }

    val execIterator = new CometExecIterator(
      CometExec.newIterId,
      CometArrowStream.inputObjects(countingBatches, childSchema, nodeName),
      numOutputCols,
      serialize(taskOp),
      CometMetricNode.fromCometPlan(this),
      numPartitions,
      partitionId,
      None,
      Seq.empty)

    try {
      // The native writer emits no batches; draining performs the write.
      while (execIterator.hasNext) {
        execIterator.next().close()
      }
    } finally {
      execIterator.close()
    }

    rowsWritten
  }

  /**
   * Report `count` rows to each stats tracker.
   *
   * `WriteTaskStatsTracker.newRow` is a per-row callback, but the only implementation Spark
   * ships, [[BasicWriteTaskStatsTracker]], ignores the row and just counts. Comet has columnar
   * batches rather than `InternalRow`s here, so it passes an empty row instead of materializing
   * every row just to hand it straight back. A tracker that actually inspects row contents would
   * therefore see empty rows, so warn rather than silently report wrong statistics.
   */
  private def recordRows(
      statsTrackers: Seq[WriteTaskStatsTracker],
      filePath: String,
      count: Long): Unit = {
    statsTrackers.filterNot(_.isInstanceOf[BasicWriteTaskStatsTracker]).foreach { tracker =>
      logWarning(
        s"${tracker.getClass.getName} receives row counts but not row contents from Comet's " +
          "native Parquet writer. Set spark.comet.parquet.write.enabled=false if this tracker " +
          "needs to inspect written rows.")
    }
    var i = 0L
    while (i < count) {
      statsTrackers.foreach(_.newRow(filePath, InternalRow.empty))
      i += 1
    }
  }

  /** Build the `TaskAttemptContext` exactly as `FileFormatWriter.executeTask` does. */
  private def createTaskAttemptContext(
      description: WriteJobDescription,
      jobTrackerID: String,
      sparkStageId: Int,
      sparkPartitionId: Int,
      sparkAttemptNumber: Int): TaskAttemptContext = {
    val jobId = SparkHadoopWriterUtils.createJobID(jobTrackerID, sparkStageId)
    val taskId = new TaskID(jobId, TaskType.MAP, sparkPartitionId)
    val taskAttemptId = new TaskAttemptID(taskId, sparkAttemptNumber)

    val hadoopConf = description.serializableHadoopConf.value
    hadoopConf.set("mapreduce.job.id", jobId.toString)
    hadoopConf.set("mapreduce.task.id", taskAttemptId.getTaskID.toString)
    hadoopConf.set("mapreduce.task.attempt.id", taskAttemptId.toString)
    hadoopConf.setBoolean("mapreduce.task.ismap", true)
    hadoopConf.setInt("mapreduce.task.partition", 0)

    new TaskAttemptContextImpl(hadoopConf, taskAttemptId)
  }

  private def serialize(op: Operator): Array[Byte] = {
    val bytes = new Array[Byte](op.getSerializedSize)
    val codedOutput = CodedOutputStream.newInstance(bytes)
    op.writeTo(codedOutput)
    codedOutput.checkNoSpaceLeft()
    bytes
  }
}
