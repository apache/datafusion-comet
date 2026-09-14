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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.TaskContext
import org.apache.spark.internal.io.{FileCommitProtocol, FileNameSpec, SparkHadoopWriterUtils}
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.comet.execution.arrow.CometArrowStream
import org.apache.spark.sql.comet.util.{Utils => CometUtils}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.datasources.OutputWriterFactory
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{SerializableConfiguration, Utils}

import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.operator.NativeWriteUtils

/**
 * Native Parquet writes on Spark 3.x, where Comet replaces the entire write command.
 *
 * Both execution entry points run Spark's job and task commit lifecycle. Native code writes the
 * exact filename supplied by the configured commit protocol, using the prepared Hadoop job. Spark
 * 4.0+ uses CometWriteFilesExec and leaves the surrounding command with Spark instead.
 */
case class CometNativeWriteExec(
    nativeOp: Operator,
    child: SparkPlan,
    outputPath: String,
    mode: SaveMode,
    committer: FileCommitProtocol,
    serializableHadoopConf: SerializableConfiguration,
    outputWriterFactory: OutputWriterFactory,
    jobTrackerID: String = SparkHadoopWriterUtils.createJobTrackerID(new Date()))
    extends CometNativeExec
    with UnaryExecNode {

  override def originalPlan: SparkPlan = child

  override def serializedPlanOpt: SerializedPlan =
    SerializedPlan(Some(CometExec.serializeNativePlan(nativeOp)))

  override def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  override def nodeName: String = "CometNativeWrite"

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "files_written" -> SQLMetrics.createMetric(sparkContext, "number of written data files"),
    "bytes_written" -> SQLMetrics.createSizeMetric(sparkContext, "written data"),
    "rows_written" -> SQLMetrics.createMetric(sparkContext, "number of written rows"))

  override def doExecute(): RDD[InternalRow] = {
    executeWriteAndCommit()
    sparkContext.emptyRDD[InternalRow]
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    executeWriteAndCommit()
    sparkContext.emptyRDD[ColumnarBatch]
  }

  private def executeWriteAndCommit(): Unit = {
    if (!prepareOutputPathForMode()) {
      logInfo(s"Skipping insertion into $outputPath - already exists (SaveMode.$mode)")
      return
    }

    val job = Job.getInstance(new Configuration(serializableHadoopConf.value))
    // Like FileFormatWriter, only abort after setupJob has succeeded.
    committer.setupJob(job)
    Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
      // Include configuration changes made by setupJob in the task contexts.
      val commitMessages = runNativeWriteJob(new SerializableConfiguration(job.getConfiguration))
      committer.commitJob(job, commitMessages.toSeq)
      logInfo(
        s"Successfully committed native write job to $outputPath: " +
          s"${metrics("files_written").value} files, " +
          s"${metrics("bytes_written").value} bytes, ${metrics("rows_written").value} rows")
    })(catchBlock = committer.abortJob(job))
  }

  private def runNativeWriteJob(
      hadoopConf: SerializableConfiguration): Array[TaskCommitMessage] = {
    val childRDD = if (child.supportsColumnar) {
      child.executeColumnar()
    } else {
      child.execute().mapPartitionsInternal { _ =>
        throw new UnsupportedOperationException(
          "Row-based child operators not yet supported for native write")
      }
    }

    val numPartitions = childRDD.getNumPartitions
    val childSchema = CometUtils.fromAttributes(child.output)
    val capturedNativeOp = nativeOp
    val capturedCommitter = committer
    val writerFactory = outputWriterFactory
    val nativeMetrics = CometMetricNode.fromCometPlan(this)
    val commitMessages = new Array[TaskCommitMessage](numPartitions)

    sparkContext.runJob(
      childRDD,
      (context: TaskContext, batches: Iterator[ColumnarBatch]) => {
        val taskContext = createTaskContext(hadoopConf.value, context)
        capturedCommitter.setupTask(taskContext)
        // Guard filename allocation, native iterator construction, execution, cleanup and commit.
        // Spark's helper preserves the original error if abortTask also fails.
        Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
          val extension = writerFactory.getFileExtension(taskContext)
          val filePath = capturedCommitter.newTaskTempFile(
            taskContext,
            None,
            FileNameSpec("", "-c000" + extension))
          NativeWriteUtils.checkNativeWriteDestination(filePath)
          val writer = capturedNativeOp.getParquetWriter.toBuilder
            .setOutputPath(filePath)
            .clearWorkDir()
            .build()
          val taskOp = capturedNativeOp.toBuilder.setParquetWriter(writer).build()

          // Register before the iterator so this listener runs after its cleanup.
          nativeMetrics.reportNativeWriteOutputMetrics(context)
          val execIterator = CometExec.getCometIterator(
            CometArrowStream.inputObjects(batches, childSchema, "CometNativeWriteExec"),
            childSchema.length,
            taskOp,
            nativeMetrics,
            numPartitions,
            context.partitionId(),
            None,
            Seq.empty)

          // Close before committing. A failed write must remain the primary error even if
          // native teardown (including the final metrics update) also throws.
          Utils.tryWithSafeFinally {
            while (execIterator.hasNext) {
              execIterator.next().close()
            }
          } {
            execIterator.close()
          }
          capturedCommitter.commitTask(taskContext)
        })(catchBlock = capturedCommitter.abortTask(taskContext))
      },
      childRDD.partitions.indices,
      (index, message: TaskCommitMessage) => {
        committer.onTaskCommit(message)
        commitMessages(index) = message
      })
    commitMessages
  }

  /**
   * Applies SaveMode semantics to the output path before the write starts. Returns `true` when
   * the write should proceed and `false` when it should be skipped (Ignore + existing target).
   * For ErrorIfExists throws when the target already exists. For Overwrite deletes the target so
   * the writer can produce a clean directory. Ported from Spark's
   * InsertIntoHadoopFsRelationCommand.run() doInsertion logic, minus the partition/catalog paths
   * that Comet does not support.
   */
  private def prepareOutputPathForMode(): Boolean = {
    val path = new Path(outputPath)
    val hadoopConf = serializableHadoopConf.value
    val fs = path.getFileSystem(hadoopConf)
    val qualifiedOutputPath = path.makeQualified(fs.getUri, fs.getWorkingDirectory)

    mode match {
      case SaveMode.Append =>
        true
      case SaveMode.ErrorIfExists =>
        if (fs.exists(qualifiedOutputPath)) {
          throw QueryCompilationErrors.outputPathAlreadyExistsError(qualifiedOutputPath)
        }
        true
      case SaveMode.Overwrite =>
        if (fs.exists(qualifiedOutputPath)) {
          if (!committer.deleteWithJob(fs, qualifiedOutputPath, true)) {
            throw QueryExecutionErrors.cannotClearOutputDirectoryError(qualifiedOutputPath)
          }
        }
        true
      case SaveMode.Ignore =>
        !fs.exists(qualifiedOutputPath)
    }
  }

  /** Match FileFormatWriter's Hadoop task identifiers and configuration. */
  private def createTaskContext(conf: Configuration, context: TaskContext): TaskAttemptContext = {
    val hadoopConf = new Configuration(conf)
    val jobId = SparkHadoopWriterUtils.createJobID(jobTrackerID, context.stageId())
    val taskId = new TaskID(jobId, TaskType.MAP, context.partitionId())
    val attemptId = new TaskAttemptID(taskId, context.taskAttemptId().toInt & Integer.MAX_VALUE)
    hadoopConf.set("mapreduce.job.id", jobId.toString)
    hadoopConf.set("mapreduce.task.id", taskId.toString)
    hadoopConf.set("mapreduce.task.attempt.id", attemptId.toString)
    hadoopConf.setBoolean("mapreduce.task.ismap", true)
    hadoopConf.setInt("mapreduce.task.partition", 0)
    new TaskAttemptContextImpl(hadoopConf, attemptId)
  }
}
