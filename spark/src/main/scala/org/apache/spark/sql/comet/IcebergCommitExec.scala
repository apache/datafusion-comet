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
import org.apache.spark.sql.connector.write.{BatchWrite, WriterCommitMessage}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

/**
 * Driver-side committer for Comet's split-operator Iceberg V2 write -- the "committer" half
 * described in `IcebergWriteStrategy`. Collects per-task [[WriterCommitMessage]]s from the paired
 * [[IcebergWriteExec]] and hands them to `BatchWrite.commit`, then refreshes Spark's cache for
 * the table.
 */
case class IcebergCommitExec(
    // `run()` is invoked driver-side (V2CommandExec contract), so neither field needs to cross
    // the JVM boundary.
    @transient batchWrite: BatchWrite,
    @transient refreshCache: () => Unit,
    child: SparkPlan)
    extends V2CommandExec
    with UnaryExecNode
    with Logging {

  override def output: Seq[Attribute] = Nil

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numCommittedMessages" -> SQLMetrics
      .createMetric(sparkContext, "number of task commit messages"))

  override protected def run(): Seq[InternalRow] = {
    val messages: Array[WriterCommitMessage] = child.executeCollect().map { row =>
      IcebergWriteExec.deserializeMessage(row.getBinary(0))
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

  override protected def withNewChildInternal(newChild: SparkPlan): IcebergCommitExec =
    copy(child = newChild)

  override def nodeName: String = "IcebergCommit"
}
