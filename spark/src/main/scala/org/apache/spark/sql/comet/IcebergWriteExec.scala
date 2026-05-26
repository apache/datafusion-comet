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

import org.apache.comet.iceberg.ReplaceDataDispatchInfo

/**
 * Executor-side file writer for Comet's split-operator Iceberg V2 write -- the "file writer exec"
 * half described in `IcebergWriteStrategy`. Per task, opens a [[DataWriter]] from the connector's
 * factory, drains the upstream RDD, and emits a single row containing the Java-serialised
 * [[WriterCommitMessage]] for the paired [[IcebergCommitExec]] to consume.
 */
case class IcebergWriteExec(
    // `doExecute()` runs driver-side and turns `batchWrite` into a serialisable
    // `DataWriterFactory`; only the factory ships to executors via closure capture, so
    // `batchWrite` itself never crosses.
    @transient batchWrite: BatchWrite,
    child: SparkPlan,
    replaceDataDispatch: Option[ReplaceDataDispatchInfo] = None)
    extends UnaryExecNode {

  override def output: Seq[Attribute] = Seq(
    AttributeReference(IcebergWriteExec.CommitMessageColumn, BinaryType, nullable = false)())

  // `DistributionAndOrderingUtils` already injected the shuffles/sorts the write needed above the
  // original logical-write node; re-requesting them here would force a second shuffle.
  override def requiredChildDistribution: Seq[Distribution] = Seq(UnspecifiedDistribution)

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override protected def doExecute(): RDD[InternalRow] = {
    val rdd = child.execute()
    val factory = batchWrite.createBatchWriterFactory(PhysicalWriteInfoImpl(rdd.getNumPartitions))
    // None of Iceberg's `BatchWrite` impls return true here -- guard catches a future change
    // rather than silently producing duplicate files.
    require(
      !batchWrite.useCommitCoordinator(),
      "Comet's Iceberg write path does not currently support BatchWrite implementations that " +
        "require Spark's commit coordinator; received: " + batchWrite.getClass.getName)

    val rowsMetric = longMetric("numOutputRows")
    val schemaTypes = output.map(_.dataType).toArray
    val capturedReplaceDataDispatch = replaceDataDispatch
    rdd.mapPartitionsInternal { iter =>
      val partId = TaskContext.getPartitionId()
      val taskId = TaskContext.get().taskAttemptId()
      val writer = factory.createWriter(partId, taskId)
      // Driver-side `executeCollect` casts each emitted row to `UnsafeRow`; project through
      // `UnsafeProjection` to satisfy that contract.
      val projection = UnsafeProjection.create(schemaTypes)
      IcebergWriteExec.runWriter(
        writer,
        iter,
        rowsMetric,
        projection,
        capturedReplaceDataDispatch)
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): IcebergWriteExec =
    copy(child = newChild)

  override def nodeName: String = "IcebergWrite"
}

object IcebergWriteExec {

  /** Name of the single Binary column carrying serialized commit messages. */
  val CommitMessageColumn: String = "iceberg_commit_message"

  /** Schema of [[IcebergWriteExec]] output. */
  val OutputSchema: StructType = StructType(
    Seq(StructField(CommitMessageColumn, BinaryType, nullable = false)))

  /**
   * Per-task write loop. Drains `iter` into `writer`, commits locally, and returns a single-row
   * iterator carrying the serialised [[WriterCommitMessage]]. On any exception, calls
   * `writer.abort()` to release task-local resources -- staged files become orphans, swept up by
   * `RemoveOrphanFiles`.
   */
  def runWriter(
      writer: DataWriter[InternalRow],
      iter: Iterator[InternalRow],
      rowsMetric: SQLMetric,
      projection: UnsafeProjection,
      replaceDataDispatch: Option[ReplaceDataDispatchInfo]): Iterator[InternalRow] = {
    val message = Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
      if (replaceDataDispatch.isDefined) {
        runReplaceDataWriter(writer, iter, replaceDataDispatch.get, rowsMetric)
      } else {
        while (iter.hasNext) {
          writer.write(iter.next())
          rowsMetric.add(1L)
        }
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

  // Mirrors org.apache.spark.sql.catalyst.util.RowDeltaUtils. Inlined because that object is
  // `private[spark]` -- we can't import it. The values 5 / 6 are verified against the shipped
  // spark-catalyst 4.0.0, 4.1.1, and 4.2.0-preview4 jars (`RowDeltaUtils$` static initializer:
  // WRITE_OPERATION=5, WRITE_WITH_METADATA_OPERATION=6), and the shipped `ReplaceData` writing
  // tasks dispatch on exactly these codes.
  //
  // TODO(spark-trunk-rename): Spark `main` (post-4.2.0-preview4) renames these to
  // COPY_OPERATION(5)/UPDATE_OPERATION(2)/INSERT_OPERATION(3) and re-dispatches the writing tasks,
  // dropping WRITE_WITH_METADATA. When Comet starts targeting a Spark release carrying that rename,
  // these constants and `runReplaceDataWriter`'s dispatch must be revisited. The CoW UPDATE / MERGE
  // cases in `CometIcebergWriteActionSuite` are the regression that will fail first -- start there.
  //
  // WRITE_OPERATION (5) and WRITE_WITH_METADATA_OPERATION (6) are Spark 4.x-only -- emitted by
  // `ReplaceData`'s rewritten row stream alongside `ReplaceDataProjections`.
  private val WRITE_OPERATION = 5
  private val WRITE_WITH_METADATA_OPERATION = 6

  // Spark 4.x added a two-arg `DataWriter#write(T metadata, T row)`. 3.4 / 3.5's `DataWriter`
  // only has the single-arg `write(T row)`. Reflected once per JVM and reused by the
  // WRITE_WITH_METADATA dispatch.
  @transient private lazy val dataWriterWriteWithMetadataMethod
      : Option[java.lang.reflect.Method] =
    try Some(classOf[DataWriter[_]].getMethod("write", classOf[Object], classOf[Object]))
    catch { case _: NoSuchMethodException => None }

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

  /**
   * Spark 4.x copy-on-write row dispatch. The reflective `write(metadata, row)` lookup falls back
   * to a clear error on 3.x, since `WRITE_WITH_METADATA_OPERATION` is 4.x-only.
   */
  private def runReplaceDataWriter(
      writer: DataWriter[InternalRow],
      iter: Iterator[InternalRow],
      dispatch: ReplaceDataDispatchInfo,
      rowsMetric: SQLMetric): Unit = {
    val rowProjection = dispatch.rowProjection
    val metadataProjection = dispatch.metadataProjection.orNull
    while (iter.hasNext) {
      val row = iter.next()
      rowsMetric.add(1L)
      row.getInt(0) match {
        case WRITE_OPERATION =>
          rowProjection.project(row)
          writer.write(rowProjection)
        case WRITE_WITH_METADATA_OPERATION =>
          rowProjection.project(row)
          if (metadataProjection != null) metadataProjection.project(row)
          val writeWithMetadata = dataWriterWriteWithMetadataMethod.getOrElse(
            throw new UnsupportedOperationException(
              "DataWriter.write(metadata, row) is not available in this Spark version but the " +
                s"analyzer emitted operation code $WRITE_WITH_METADATA_OPERATION"))
          writeWithMetadata.invoke(writer, metadataProjection, rowProjection)
        case other =>
          throw new IllegalArgumentException(
            s"Unexpected ReplaceData operation code $other; supported: " +
              s"$WRITE_OPERATION (WRITE), $WRITE_WITH_METADATA_OPERATION (WRITE_WITH_METADATA)")
      }
    }
  }
}
