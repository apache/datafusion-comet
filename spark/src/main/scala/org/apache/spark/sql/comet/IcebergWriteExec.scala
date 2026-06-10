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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, PhysicalWriteInfoImpl, WriterCommitMessage}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}
import org.apache.spark.util.Utils

/**
 * Executor-side file writer for Comet's split-operator Iceberg V2 write.
 */
case class IcebergWriteExec(
    // `batchWrite` only stored driver side, only the writer factory is shipped to executors.
    @transient batchWrite: BatchWrite,
    child: SparkPlan)
    extends UnaryExecNode {

  override def output: Seq[Attribute] = Seq(
    AttributeReference(IcebergWriteExec.CommitMessageColumn, BinaryType, nullable = false)())

  // Spark already adds a distribution for the V2 write; adding another here is redundant.
  override def requiredChildDistribution: Seq[Distribution] = Seq(UnspecifiedDistribution)

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override protected def doExecute(): RDD[InternalRow] = {
    val rdd = child.execute()
    val factory = batchWrite.createBatchWriterFactory(PhysicalWriteInfoImpl(rdd.getNumPartitions))
    require(
      !batchWrite.useCommitCoordinator(),
      "Comet's Iceberg write path does not currently support BatchWrite implementations that " +
        "require Spark's commit coordinator; received: " + batchWrite.getClass.getName)

    val rowsMetric = longMetric("numOutputRows")
    val schemaTypes = output.map(_.dataType).toArray
    rdd.mapPartitionsInternal { iter =>
      val partId = TaskContext.getPartitionId()
      val taskId = TaskContext.get().taskAttemptId()
      val writer = factory.createWriter(partId, taskId)
      val projection = UnsafeProjection.create(schemaTypes)
      IcebergWriteExec.runWriter(writer, iter, rowsMetric, projection)
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): IcebergWriteExec =
    copy(child = newChild)

  override def nodeName: String = "IcebergWrite"
}

object IcebergWriteExec {

  val CommitMessageColumn: String = "iceberg_commit_message"

  val OutputSchema: StructType = StructType(
    Seq(StructField(CommitMessageColumn, BinaryType, nullable = false)))

  /** Writes data files and returns the serialised Iceberg commit message. */
  def runWriter(
      writer: DataWriter[InternalRow],
      iter: Iterator[InternalRow],
      rowsMetric: SQLMetric,
      projection: UnsafeProjection): Iterator[InternalRow] = {
    val message = Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
      while (iter.hasNext) {
        writer.write(iter.next())
        rowsMetric.add(1L)
      }
      writer.commit()
    })(
      catchBlock = {
        writer.abort()
      },
      finallyBlock = {
        writer.close()
      })

    Iterator.single(projection(InternalRow(serializeMessage(message))).copy())
  }

  def serializeMessage(message: WriterCommitMessage): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    try oos.writeObject(message)
    finally oos.close()
    bos.toByteArray
  }

  def deserializeMessage(bytes: Array[Byte]): WriterCommitMessage = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    try ois.readObject().asInstanceOf[WriterCommitMessage]
    finally ois.close()
  }
}
