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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.write.{BatchWrite, Write, WriterCommitMessage}
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution, UnaryExecNode}
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

import org.apache.comet.iceberg.{IcebergDriverMetricsShim, IcebergReflection}

/**
 * Driver-side committer for Comet's split-operator Iceberg V2 write.
 */
case class IcebergCommitExec(
    // None of these fields are serialized, this is all run on the driver.
    @transient batchWrite: BatchWrite,
    @transient write: Write,
    @transient refreshCache: IcebergCommitExec.RefreshCache,
    child: SparkPlan)
    extends V2CommandExec
    with UnaryExecNode
    with Logging {

  override def output: Seq[Attribute] = Nil

  override lazy val metrics: Map[String, SQLMetric] =
    Map(
      "numCommittedMessages" -> SQLMetrics
        .createMetric(sparkContext, "number of task commit messages")) ++
      write
        .supportedCustomMetrics()
        .map(m => m.name -> SQLMetrics.createV2CustomMetric(sparkContext, m))

  // Exactly-once relies on V2CommandExec memoizing run() via its `result` lazy val and on
  // the writer executing once inside the AQE bubble anchored by IcebergWriteLogical; pinned
  // by the AQE re-plan test in CometIcebergWriteActionSuite.
  override protected def run(): Seq[InternalRow] = {
    try {
      collectAndCommit()
    } finally {
      postDriverMetrics()
    }
  }

  private def collectAndCommit(): Seq[InternalRow] = {
    // Collect each task's commit message as its task finishes, the way Spark's own V2 write
    // does, so that a job failure still knows which tasks completed. `executeCollect` would
    // only hand back the messages once every task succeeded.
    val rdd = child.execute()
    val messages = new Array[WriterCommitMessage](rdd.partitions.length)
    try {
      sparkContext.runJob(
        rdd,
        (iter: Iterator[InternalRow]) => iter.map(_.getBinary(0)).toArray,
        (index: Int, payloads: Array[Array[Byte]]) => {
          payloads.foreach { payload =>
            messages(index) = IcebergWriteExec.deserializeMessage(payload)
          }
        })
    } catch {
      case cause: Throwable =>
        val completed = messages.filter(_ != null)
        logError(
          s"Iceberg write job failed; aborting with ${completed.length} completed task " +
            "message(s)",
          cause)
        // The BatchWrite contract expects a job-level abort. Iceberg's own `SparkWrite.abort`
        // only deletes files after a cleanable *commit* failure and skips cleanup otherwise, so
        // the data files of tasks that completed before the job failed would stay behind even
        // though nothing can reference them (no commit was attempted). Delete them here; the
        // failed task's own files are cleaned up by the task itself.
        try batchWrite.abort(completed)
        catch {
          case abortFailure: Throwable =>
            cause.addSuppressed(abortFailure)
        }
        try deleteCompletedTaskFiles(completed)
        catch {
          case cleanupFailure: Throwable =>
            cause.addSuppressed(cleanupFailure)
        }
        throw cause
    }
    longMetric("numCommittedMessages").add(messages.length)

    try {
      messages.foreach(batchWrite.onDataWriterCommit)
      IcebergWriteSummaryShim.commit(batchWrite, messages, child)
      logInfo(s"Iceberg commit succeeded with ${messages.length} task message(s)")
    } catch {
      case cause: Throwable =>
        logError(s"Iceberg commit failed; aborting ${messages.length} task message(s)", cause)
        try batchWrite.abort(messages)
        catch {
          case abortFailure: Throwable =>
            cause.addSuppressed(abortFailure)
        }
        throw cause
    }

    refreshCache()
    Nil
  }

  private def deleteCompletedTaskFiles(completed: Array[WriterCommitMessage]): Unit = {
    val locations = completed.toSeq.flatMap(m => IcebergReflection.taskCommitFileLocations(m))
    if (locations.nonEmpty) {
      val io = IcebergReflection
        .getOuterSparkWrite(batchWrite)
        .flatMap(IcebergReflection.getTableFromSparkWrite)
        .flatMap(IcebergReflection.getTableIO)
      io match {
        case Some(fileIO) =>
          IcebergReflection.deleteFilesQuietly(
            fileIO,
            locations,
            s"job abort, ${completed.length} completed task(s)")
        case None =>
          logWarning(
            s"Could not resolve the table FileIO; leaving ${locations.size} data file(s) from " +
              "completed tasks of a failed write job for remove_orphan_files")
      }
    }
  }

  private def postDriverMetrics(): Unit = {
    val driverMetrics = IcebergDriverMetricsShim.reportDriverMetrics(write)
    if (driverMetrics.nonEmpty) {
      val updated = driverMetrics.flatMap { taskMetric =>
        metrics.get(taskMetric.name).map { sqlMetric =>
          sqlMetric.set(taskMetric.value)
          sqlMetric
        }
      }
      val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
      SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, updated.toIndexedSeq)
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): IcebergCommitExec =
    copy(child = newChild)

  override def nodeName: String = "IcebergCommit"
}

object IcebergCommitExec {
  type RefreshCache = () => Unit
}
