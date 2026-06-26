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
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.TaskContext
import org.apache.spark.internal.io.{FileCommitProtocol, FileNameSpec, SparkHadoopWriterUtils}
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.comet.CometNativeWriteExec.CommitProtocolConfig
import org.apache.spark.sql.comet.execution.arrow.CometArrowStream
import org.apache.spark.sql.comet.util.{Utils => CometUtils}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.datasources.OutputWriterFactory
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

import com.google.protobuf.CodedOutputStream

import org.apache.comet.CometExecIterator
import org.apache.comet.serde.OperatorOuterClass.Operator

object CometNativeWriteExec {

  def newJobTrackerID(): String = SparkHadoopWriterUtils.createJobTrackerID(new Date())

  /**
   * Driver-created objects required to use Spark's FileCommitProtocol from native write tasks.
   * The committer instance is serializable by contract and is sent to executors, while the same
   * driver-side instance receives task commit messages and commits or aborts the job.
   */
  case class CommitProtocolConfig(
      committer: FileCommitProtocol,
      serializableHadoopConf: SerializableConfiguration,
      outputWriterFactory: OutputWriterFactory,
      commitProtocolJobId: String,
      jobTrackerID: String)
      extends Serializable
}

/**
 * Comet physical operator for native Parquet write operations.
 *
 * When [[commitProtocol]] is present, this operator follows Spark's FileCommitProtocol lifecycle:
 * driver setupJob, executor setupTask/newTaskTempFile/commitTask or abortTask, and driver
 * commitJob or abortJob. Native code writes exactly to the task temp file returned by Spark's
 * commit protocol; it does not generate its own final part-file names.
 *
 * @param nativeOp
 *   The native operator representing the write operation (template, modified per task)
 * @param child
 *   The child operator providing the data to write
 * @param outputPath
 *   The final output directory for the write
 * @param commitProtocol
 *   Spark file commit protocol state. If absent, files are written directly under outputPath.
 */
case class CometNativeWriteExec(
    nativeOp: Operator,
    child: SparkPlan,
    outputPath: String,
    commitProtocol: Option[CommitProtocolConfig] = None)
    extends CometNativeExec
    with UnaryExecNode {

  override def originalPlan: SparkPlan = child

  override def serializedPlanOpt: SerializedPlan = SerializedPlan(
    Some(serializeNativeOp(nativeOp)))

  override def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  override def nodeName: String = "CometNativeWrite"

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "files_written" -> SQLMetrics.createMetric(sparkContext, "number of written data files"),
    "bytes_written" -> SQLMetrics.createSizeMetric(sparkContext, "written data"),
    "rows_written" -> SQLMetrics.createMetric(sparkContext, "number of written rows"))

  override def doExecute(): RDD[InternalRow] = {
    executeWriteAndCommit()
    // Write operations do not return rows.
    sparkContext.emptyRDD[InternalRow]
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    executeWriteAndCommit()
    // Write operations do not return columnar batches. Spark may still ask for columnar output
    // while this operator is nested under write planning nodes, so run the terminal write here too.
    sparkContext.emptyRDD[ColumnarBatch]
  }

  private def executeWriteAndCommit(): Unit = {
    commitProtocol match {
      case Some(protocol) =>
        val jobContext = createJobContext(protocol)

        // Match Spark's FileFormatWriter lifecycle: setupJob is outside the try block because it
        // only initializes the job; failures after this point should abort the job.
        protocol.committer.setupJob(jobContext)

        try {
          val commitMessages = runNativeWriteJob(protocol)
          protocol.committer.commitJob(jobContext, commitMessages.toSeq)

          val filesWritten = metrics("files_written").value
          val bytesWritten = metrics("bytes_written").value
          val rowsWritten = metrics("rows_written").value
          logInfo(
            s"Successfully committed native write job to $outputPath: " +
              s"$filesWritten files, $bytesWritten bytes, $rowsWritten rows")
        } catch {
          case t: Throwable =>
            abortJob(protocol, jobContext, t)
            throw t
        }

      case None =>
        // Direct-write fallback for tests or callers that do not provide a commit protocol.
        nativeWriteTasks(None).count()
    }
  }

  private def runNativeWriteJob(protocol: CommitProtocolConfig): Array[TaskCommitMessage] = {
    val writeRDD = nativeWriteTasks(Some(protocol))
    val ret = new Array[TaskCommitMessage](writeRDD.partitions.length)

    sparkContext.runJob(
      writeRDD,
      (_: TaskContext, iter: Iterator[TaskCommitMessage]) => {
        assert(iter.hasNext, "Native write task did not return a commit message")
        val commitMessage = iter.next()
        assert(!iter.hasNext, "Native write task returned more than one commit message")
        commitMessage
      },
      writeRDD.partitions.indices,
      (index, commitMessage: TaskCommitMessage) => {
        protocol.committer.onTaskCommit(commitMessage)
        ret(index) = commitMessage
      })

    ret
  }

  private def nativeWriteTasks(
      capturedCommitProtocol: Option[CommitProtocolConfig]): RDD[TaskCommitMessage] = {
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

    val numPartitions = childRDD.getNumPartitions
    val numOutputCols = child.output.length
    val capturedNativeOp = nativeOp
    val writeExec = this

    childRDD.mapPartitionsInternal { iter =>
      val sparkTaskContext = TaskContext.get()
      val partitionId = sparkTaskContext.partitionId()
      val sparkStageId = sparkTaskContext.stageId()
      val taskAttemptId = sparkTaskContext.taskAttemptId()
      val sparkAttemptNumber = taskAttemptId.toInt & Integer.MAX_VALUE

      new Iterator[TaskCommitMessage] {
        private var emitted = false

        override def hasNext: Boolean = !emitted

        override def next(): TaskCommitMessage = {
          if (emitted) {
            throw new NoSuchElementException("Native write task already completed")
          }
          emitted = true

          var taskCommitter: Option[(FileCommitProtocol, TaskAttemptContext)] = None
          var execIterator: CometExecIterator = null

          try {
            val taskOutputPath = capturedCommitProtocol match {
              case Some(protocol) =>
                val taskAttemptContext =
                  createTaskContext(protocol, sparkStageId, partitionId, sparkAttemptNumber)
                protocol.committer.setupTask(taskAttemptContext)
                taskCommitter = Some(protocol.committer -> taskAttemptContext)

                val fileExtension =
                  protocol.outputWriterFactory.getFileExtension(taskAttemptContext)
                protocol.committer.newTaskTempFile(
                  taskAttemptContext,
                  None,
                  FileNameSpec("", "-c000" + fileExtension))

              case None =>
                directTaskOutputPath(partitionId)
            }

            val parquetWriter = capturedNativeOp.getParquetWriter.toBuilder
              .setTaskOutputPath(taskOutputPath)
              .setTaskAttemptId(sparkAttemptNumber)

            capturedCommitProtocol.foreach { protocol =>
              parquetWriter.setJobId(protocol.commitProtocolJobId)
            }

            val modifiedNativeOp = capturedNativeOp.toBuilder
              .setParquetWriter(parquetWriter.build())
              .build()

            val nativeMetrics = CometMetricNode.fromCometPlan(writeExec)
            // Register before CometExecIterator so completion listeners run after iterator close
            // (Spark runs task completion callbacks in reverse registration order).
            Option(TaskContext.get()).foreach(nativeMetrics.reportNativeWriteOutputMetrics)

            execIterator = new CometExecIterator(
              CometExec.newIterId,
              CometArrowStream.inputObjects(
                iter,
                CometUtils.fromAttributes(child.output),
                "CometNativeWriteExec"),
              numOutputCols,
              serializeNativeOp(modifiedNativeOp),
              nativeMetrics,
              numPartitions,
              partitionId,
              None,
              Seq.empty)

            while (execIterator.hasNext) {
              execIterator.next().close()
            }

            taskCommitter match {
              case Some((committer, taskAttemptContext)) =>
                val message = committer.commitTask(taskAttemptContext)
                logInfo(s"Task ${taskAttemptContext.getTaskAttemptID} committed successfully")
                message
              case None =>
                FileCommitProtocol.EmptyTaskCommitMessage
            }
          } catch {
            case t: Throwable =>
              taskCommitter.foreach { case (committer, taskAttemptContext) =>
                try {
                  committer.abortTask(taskAttemptContext)
                  logWarning(
                    s"Task ${taskAttemptContext.getTaskAttemptID} aborted due to exception: " +
                      Option(t.getMessage).getOrElse(t.getClass.getName))
                } catch {
                  case abortError: Throwable =>
                    logWarning(
                      s"Error aborting task ${taskAttemptContext.getTaskAttemptID}",
                      abortError)
                    t.addSuppressed(abortError)
                }
              }
              throw t
          } finally {
            if (execIterator != null) {
              execIterator.close()
            }
          }
        }
      }
    }
  }

  private def serializeNativeOp(op: Operator): Array[Byte] = {
    val size = op.getSerializedSize
    val bytes = new Array[Byte](size)
    val codedOutput = CodedOutputStream.newInstance(bytes)
    op.writeTo(codedOutput)
    codedOutput.checkNoSpaceLeft()
    bytes
  }

  private def directTaskOutputPath(partitionId: Int): String = {
    val separator = if (outputPath.endsWith("/")) "" else "/"
    f"${outputPath}${separator}part-$partitionId%05d.parquet"
  }

  private def abortJob(
      protocol: CommitProtocolConfig,
      jobContext: Job,
      cause: Throwable): Unit = {
    logError("Native write failed, aborting job", cause)
    try {
      protocol.committer.abortJob(jobContext)
    } catch {
      case abortError: Throwable =>
        logWarning("Error aborting native write job", abortError)
        cause.addSuppressed(abortError)
    }
  }

  /** Create a JobContext for the write job using the prepared Hadoop write configuration. */
  private def createJobContext(protocol: CommitProtocolConfig): Job = {
    Job.getInstance(new Configuration(protocol.serializableHadoopConf.value))
  }

  /** Create a TaskAttemptContext matching Spark's FileFormatWriter task ID setup. */
  private def createTaskContext(
      protocol: CommitProtocolConfig,
      sparkStageId: Int,
      sparkPartitionId: Int,
      sparkAttemptNumber: Int): TaskAttemptContext = {
    val hadoopConf = new Configuration(protocol.serializableHadoopConf.value)
    val jobId = SparkHadoopWriterUtils.createJobID(protocol.jobTrackerID, sparkStageId)
    val taskId = new TaskID(jobId, TaskType.MAP, sparkPartitionId)
    val taskAttemptId = new TaskAttemptID(taskId, sparkAttemptNumber)

    hadoopConf.set("mapreduce.job.id", jobId.toString)
    hadoopConf.set("mapreduce.task.id", taskAttemptId.getTaskID.toString)
    hadoopConf.set("mapreduce.task.attempt.id", taskAttemptId.toString)
    hadoopConf.setBoolean("mapreduce.task.ismap", true)
    hadoopConf.setInt("mapreduce.task.partition", 0)

    new TaskAttemptContextImpl(hadoopConf, taskAttemptId)
  }
}
