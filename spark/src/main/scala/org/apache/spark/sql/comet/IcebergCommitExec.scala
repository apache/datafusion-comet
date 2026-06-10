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

import org.apache.comet.iceberg.IcebergDriverMetricsShim

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
    val messages: Array[WriterCommitMessage] =
      try {
        child.executeCollect().map { row =>
          IcebergWriteExec.deserializeMessage(row.getBinary(0))
        }
      } catch {
        case cause: Throwable =>
          // The write job failed; the BatchWrite contract still expects a job-level abort.
          try batchWrite.abort(Array.empty[WriterCommitMessage])
          catch {
            case abortFailure: Throwable =>
              cause.addSuppressed(abortFailure)
          }
          throw cause
      }
    longMetric("numCommittedMessages").add(messages.length)

    try {
      messages.foreach(batchWrite.onDataWriterCommit)
      batchWrite.commit(messages)
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
