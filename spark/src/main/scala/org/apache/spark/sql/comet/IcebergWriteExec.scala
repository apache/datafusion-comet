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
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, PhysicalWriteInfoImpl, Write, WriterCommitMessage}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{CustomMetrics, SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}
import org.apache.spark.util.Utils

/**
 * Executor-side file writer for Comet's split-operator Iceberg V2 write.
 */
case class IcebergWriteExec(
    // `batchWrite` only stored driver side, only the writer factory is shipped to executors.
    @transient batchWrite: BatchWrite,
    // Driver-side only; used to declare the write's custom task metrics.
    @transient write: Write,
    override val output: Seq[Attribute],
    child: SparkPlan)
    extends UnaryExecNode {

  // Spark already adds a distribution for the V2 write; adding another here is redundant.
  override def requiredChildDistribution: Seq[Distribution] = Seq(UnspecifiedDistribution)

  override lazy val metrics: Map[String, SQLMetric] =
    Map("numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows")) ++
      write
        .supportedCustomMetrics()
        .map(m => m.name -> SQLMetrics.createV2CustomMetric(sparkContext, m))

  override protected def doExecute(): RDD[InternalRow] = {
    val rdd = {
      val tempRdd = child.execute()
      // SPARK-23271: run one write task even when the query RDD has zero partitions.
      if (tempRdd.getNumPartitions == 0) {
        sparkContext.parallelize(Seq.empty[InternalRow], 1)
      } else {
        tempRdd
      }
    }
    val factory = batchWrite.createBatchWriterFactory(PhysicalWriteInfoImpl(rdd.getNumPartitions))
    // Backstop only; IcebergWriteStrategy already falls back at planning time.
    require(
      !batchWrite.useCommitCoordinator(),
      "Comet's Iceberg write path does not currently support BatchWrite implementations that " +
        "require Spark's commit coordinator; received: " + batchWrite.getClass.getName)

    val rowsMetric = longMetric("numOutputRows")
    val customMetrics = metrics.filter { case (name, _) => name != "numOutputRows" }
    val schemaTypes = output.map(_.dataType).toArray
    rdd.mapPartitionsInternal { iter =>
      val partId = TaskContext.getPartitionId()
      val taskId = TaskContext.get().taskAttemptId()
      val writer = factory.createWriter(partId, taskId)
      val projection = UnsafeProjection.create(schemaTypes)
      IcebergWriteExec.runWriter(writer, iter, rowsMetric, customMetrics, projection)
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
      customMetrics: Map[String, SQLMetric],
      projection: UnsafeProjection): Iterator[InternalRow] = {
    val iterWithMetrics = new IteratorWithMetrics(iter, writer, customMetrics, rowsMetric)
    val message = Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
      while (iterWithMetrics.hasNext) {
        writer.write(iterWithMetrics.next())
      }
      CustomMetrics.updateMetrics(writer.currentMetricsValues.toSeq, customMetrics)
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

  private class IteratorWithMetrics(
      iter: Iterator[InternalRow],
      dataWriter: DataWriter[InternalRow],
      customMetrics: Map[String, SQLMetric],
      rowsMetric: SQLMetric)
      extends Iterator[InternalRow] {
    private var count = 0L

    override def hasNext: Boolean = iter.hasNext

    override def next(): InternalRow = {
      if (count % CustomMetrics.NUM_ROWS_PER_UPDATE == 0) {
        CustomMetrics.updateMetrics(dataWriter.currentMetricsValues.toSeq, customMetrics)
      }
      count += 1
      rowsMetric.add(1L)
      iter.next()
    }
  }

  def serializeMessage(message: WriterCommitMessage): Array[Byte] =
    Utils.serialize(message)

  def deserializeMessage(bytes: Array[Byte]): WriterCommitMessage =
    Utils.deserialize[WriterCommitMessage](bytes, Utils.getContextOrSparkClassLoader)
}
