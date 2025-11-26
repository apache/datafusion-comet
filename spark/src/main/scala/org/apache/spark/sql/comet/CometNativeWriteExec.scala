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

import java.io.ByteArrayOutputStream

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

import org.apache.comet.CometExecIterator
import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * Comet physical operator for native Parquet write operations with FileCommitProtocol support.
 *
 * This operator writes data to Parquet files using the native Comet engine. It integrates with
 * Spark's FileCommitProtocol to provide atomic writes with proper staging and commit semantics.
 *
 * The implementation includes support for Spark's file commit protocol through work_dir, job_id,
 * and task_attempt_id parameters that can be set in the operator. When work_dir is set, files are
 * written to a temporary location that can be atomically committed later.
 *
 * @param nativeOp
 *   The native operator representing the write operation (template, will be modified per task)
 * @param child
 *   The child operator providing the data to write
 * @param outputPath
 *   The path where the Parquet file will be written
 * @param committer
 *   FileCommitProtocol for atomic writes. If None, files are written directly.
 * @param jobTrackerID
 *   Unique identifier for this write job
 */
case class CometNativeWriteExec(
    nativeOp: Operator,
    child: SparkPlan,
    outputPath: String,
    committer: Option[FileCommitProtocol] = None,
    jobTrackerID: String = Utils.createTempDir().getName)
    extends CometNativeExec
    with UnaryExecNode {

  override def originalPlan: SparkPlan = child

  override def serializedPlanOpt: SerializedPlan = {
    val outputStream = new ByteArrayOutputStream()
    nativeOp.writeTo(outputStream)
    outputStream.close()
    SerializedPlan(Some(outputStream.toByteArray))
  }

  override def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  override def nodeName: String = "CometNativeWrite"

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "files_written" -> SQLMetrics.createMetric(sparkContext, "number of written data files"),
    "bytes_written" -> SQLMetrics.createSizeMetric(sparkContext, "written data"),
    "rows_written" -> SQLMetrics.createMetric(sparkContext, "number of written rows"))

  override def doExecute(): RDD[InternalRow] = {
    // Setup job if committer is present
    committer.foreach { c =>
      val jobContext = createJobContext()
      c.setupJob(jobContext)
    }

    // Execute the native write with commit protocol
    val resultRDD = doExecuteColumnar()

    // Consume all batches (they're empty, just forcing execution)
    resultRDD
      .mapPartitions { iter =>
        iter.foreach(_.close())
        Iterator.empty
      }
      .count() // Force execution

    // Extract write statistics from metrics
    val filesWritten = metrics("files_written").value
    val bytesWritten = metrics("bytes_written").value
    val rowsWritten = metrics("rows_written").value

    // Commit job
    committer.foreach { c =>
      val jobContext = createJobContext()
      try {
        // For now, just commit the job without task commit messages
        // In a full implementation, we'd collect TaskCommitMessage from each task
        c.commitJob(jobContext, Seq.empty)
        logInfo(
          s"Successfully committed write job to $outputPath: " +
            s"$filesWritten files, $bytesWritten bytes, $rowsWritten rows")
      } catch {
        case e: Exception =>
          logError("Failed to commit job, aborting", e)
          c.abortJob(jobContext)
          throw e
      }
    }

    // Return empty RDD as write operations don't return data
    sparkContext.emptyRDD[InternalRow]
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    // Get the input data from the child operator
    val childRDD = if (child.supportsColumnar) {
      child.executeColumnar()
    } else {
      // If child doesn't support columnar, convert to columnar
      child.execute().mapPartitionsInternal { _ =>
        // TODO this could delegate to CometRowToColumnar, but maybe Comet
        // does not need to support this case?
        throw new UnsupportedOperationException(
          "Row-based child operators not yet supported for native write")
      }
    }

    // Capture metadata before the transformation
    val numPartitions = childRDD.getNumPartitions
    val numOutputCols = child.output.length
    val broadcastedCommitter = committer.map(c => sparkContext.broadcast(c))
    val capturedJobTrackerID = jobTrackerID
    val capturedNativeOp = nativeOp

    // Execute native write operation with task-level commit protocol
    childRDD.mapPartitionsInternal { iter =>
      val partitionId = org.apache.spark.TaskContext.getPartitionId()
      val taskAttemptId = org.apache.spark.TaskContext.get().taskAttemptId()

      // Setup task-level commit protocol if provided
      val (workDir, taskContext, commitMsg) = broadcastedCommitter
        .map { bc =>
          val committer = bc.value
          val taskContext =
            createTaskContext(capturedJobTrackerID, partitionId, taskAttemptId.toInt)

          // Setup task - this creates the temporary working directory
          committer.setupTask(taskContext)

          // Get the work directory for temp files
          val workPath = committer.newTaskTempFile(taskContext, None, "")
          val workDir = new Path(workPath).getParent.toString

          (Some(workDir), Some((committer, taskContext)), null)
        }
        .getOrElse((None, None, null))

      // Modify the native operator to include task-specific parameters
      val modifiedNativeOp = if (workDir.isDefined) {
        val parquetWriter = capturedNativeOp.getParquetWriter.toBuilder
          .setWorkDir(workDir.get)
          .setJobId(capturedJobTrackerID)
          .setTaskAttemptId(taskAttemptId.toInt)
          .build()

        capturedNativeOp.toBuilder.setParquetWriter(parquetWriter).build()
      } else {
        capturedNativeOp
      }

      val nativeMetrics = CometMetricNode.fromCometPlan(this)

      val outputStream = new ByteArrayOutputStream()
      modifiedNativeOp.writeTo(outputStream)
      outputStream.close()
      val planBytes = outputStream.toByteArray

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

      // Wrap the iterator to handle task commit/abort
      new Iterator[ColumnarBatch] {
        private var completed = false
        private var thrownException: Option[Throwable] = None

        override def hasNext: Boolean = {
          val result =
            try {
              execIterator.hasNext
            } catch {
              case e: Throwable =>
                thrownException = Some(e)
                handleTaskEnd()
                throw e
            }

          if (!result && !completed) {
            handleTaskEnd()
          }

          result
        }

        override def next(): ColumnarBatch = {
          try {
            execIterator.next()
          } catch {
            case e: Throwable =>
              thrownException = Some(e)
              handleTaskEnd()
              throw e
          }
        }

        private def handleTaskEnd(): Unit = {
          if (!completed) {
            completed = true

            // Handle commit or abort based on whether an exception was thrown
            taskContext.foreach { case (committer, ctx) =>
              try {
                if (thrownException.isEmpty) {
                  // Commit the task
                  committer.commitTask(ctx)
                  logInfo(s"Task ${ctx.getTaskAttemptID} committed successfully")
                } else {
                  // Abort the task
                  committer.abortTask(ctx)
                  val exMsg = thrownException.get.getMessage
                  logWarning(s"Task ${ctx.getTaskAttemptID} aborted due to exception: $exMsg")
                }
              } catch {
                case e: Exception =>
                  // Log the commit/abort exception but don't mask the original exception
                  logError(s"Error during task commit/abort: ${e.getMessage}", e)
                  if (thrownException.isEmpty) {
                    // If no original exception, propagate the commit/abort exception
                    throw e
                  }
              }
            }
          }
        }
      }
    }
  }

  /** Create a JobContext for the write job */
  private def createJobContext(): Job = {
    val job = Job.getInstance()
    job.setJobID(new org.apache.hadoop.mapreduce.JobID(jobTrackerID, 0))
    job
  }

  /** Create a TaskAttemptContext for a specific task */
  private def createTaskContext(
      jobId: String,
      partitionId: Int,
      attemptNumber: Int): TaskAttemptContext = {
    val job = Job.getInstance()
    val taskAttemptID = new TaskAttemptID(
      new TaskID(new org.apache.hadoop.mapreduce.JobID(jobId, 0), TaskType.REDUCE, partitionId),
      attemptNumber)
    new TaskAttemptContextImpl(job.getConfiguration, taskAttemptID)
  }
}
