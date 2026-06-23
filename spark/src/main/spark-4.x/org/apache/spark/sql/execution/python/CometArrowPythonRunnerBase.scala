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

package org.apache.spark.sql.execution.python

import java.io.{DataInputStream, DataOutputStream}
import java.nio.channels.Channels
import java.util.concurrent.atomic.AtomicBoolean

import scala.jdk.CollectionConverters._

import org.apache.arrow.vector.{BaseFixedWidthVector, BaseLargeVariableWidthVector, BaseVariableWidthVector, FieldVector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.{LargeListVector, ListVector, StructVector}
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType}
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python.{BasePythonRunner, PythonRDD, PythonWorker, SpecialLengths}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.unsafe.Platform

import org.apache.comet.CometArrowAllocator
import org.apache.comet.vector.{CometDecodedVector, CometVector}

/**
 * Shared base for Comet's Arrow Python runners (Spark 4.0 / 4.1 / 4.2).
 *
 * Unlike a stock `ArrowPythonRunner`, this does not extend Spark's `PythonArrowInput` /
 * `BasicPythonArrowOutput` traits. Those traits expose Spark's Arrow types (`VectorSchemaRoot`,
 * `Schema`) in their members, and the packaged `comet-spark` jar relocates `org.apache.arrow` to
 * `org.apache.comet.shaded.arrow`, so mixing them in produces a class whose synthetic Arrow
 * members no longer match Spark's unshaded trait contract (an `AbstractMethodError` at runtime).
 *
 * Instead it extends only the Arrow-agnostic `BasePythonRunner` and performs the Arrow IPC
 * exchange itself using Comet's (shaded) Arrow. The Python worker only ever sees a standard Arrow
 * IPC byte stream, which is version-neutral, so nothing crosses the shaded/unshaded boundary:
 *   - Input: each Comet `ColumnarBatch` is copied into a shaded struct root and written to the
 *     worker with a shaded `ArrowStreamWriter`.
 *   - Output: the worker's Arrow IPC is read with a shaded `ArrowStreamReader` straight into
 *     `CometVector`s, which is exactly what `CometMapInBatchExec` and downstream native operators
 *     consume.
 *
 * `BasePythonRunner` has the same shape across Spark 4.0/4.1/4.2; only the subclass constructor
 * arguments and `writeUDF` differ, so those stay in the per-version subclasses.
 */
private[python] trait CometArrowPythonRunnerBase
    extends BasePythonRunner[Iterator[ColumnarBatch], ColumnarBatch] {

  /** Worker configuration written to the Python worker before execution. */
  protected def workerConf: Map[String, String]

  /** Comet's Python SQL metrics (data sent/received, rows). */
  protected def pythonMetrics: Map[String, SQLMetric]

  /** Version-specific UDF command serialization. */
  protected def writeUDF(dataOut: DataOutputStream): Unit

  override val pythonExec: String =
    SQLConf.get.pysparkWorkerPythonExecutable.getOrElse(funcs.head.funcs.head.pythonExec)

  override val faultHandlerEnabled: Boolean = SQLConf.get.pythonUDFWorkerFaulthandlerEnabled
  override val idleTimeoutSeconds: Long = SQLConf.get.pythonUDFWorkerIdleTimeoutSeconds
  override val hideTraceback: Boolean = SQLConf.get.pysparkHideTraceback
  override val simplifiedTraceback: Boolean = SQLConf.get.pysparkSimplifiedTraceback

  override val bufferSize: Int = SQLConf.get.pandasUDFBufferSize
  require(
    bufferSize >= 4,
    "Pandas execution requires more than 4 bytes. Please set higher buffer. " +
      s"Please change '${SQLConf.PANDAS_UDF_BUFFER_SIZE.key}'.")

  override protected def newWriter(
      env: SparkEnv,
      worker: PythonWorker,
      inputIterator: Iterator[Iterator[ColumnarBatch]],
      partitionIndex: Int,
      context: TaskContext): Writer = {
    new Writer(env, worker, inputIterator, partitionIndex, context) {

      private val allocator =
        CometArrowAllocator.newChildAllocator(s"stdout writer for $pythonExec", 0, Long.MaxValue)
      private var currentGroup: Iterator[ColumnarBatch] = _
      private var arrowWriter: ArrowStreamWriter = _
      private var writeRoot: VectorSchemaRoot = _
      private var structVec: StructVector = _

      context.addTaskCompletionListener[Unit] { _ =>
        if (writeRoot != null) {
          writeRoot.close()
        }
        allocator.close()
      }

      protected override def writeCommand(dataOut: DataOutputStream): Unit = {
        // handleMetadataBeforeExec: write the worker config as key/value string pairs.
        dataOut.writeInt(workerConf.size)
        for ((k, v) <- workerConf) {
          PythonRDD.writeUTF(k, dataOut)
          PythonRDD.writeUTF(v, dataOut)
        }
        writeUDF(dataOut)
      }

      override def writeNextInputToStream(dataOut: DataOutputStream): Boolean = {
        while (currentGroup == null || !currentGroup.hasNext) {
          if (!inputIterator.hasNext) {
            if (arrowWriter != null) {
              arrowWriter.end()
            }
            return false
          }
          currentGroup = inputIterator.next()
        }

        val cometBatch = currentGroup.next()
        val startData = dataOut.size()

        if (arrowWriter == null) {
          // Build the destination struct root once, sized to the first batch's child fields.
          // mapInArrow/mapInPandas exchange the columns under a single non-nullable struct.
          val childFields = (0 until cometBatch.numCols()).map { i =>
            cometBatch.column(i).asInstanceOf[CometDecodedVector].getValueVector.getField
          }
          val structField =
            new Field(
              "struct",
              new FieldType(false, ArrowType.Struct.INSTANCE, null),
              childFields.asJava)
          structVec = structField.createVector(allocator).asInstanceOf[StructVector]
          writeRoot = new VectorSchemaRoot(Seq[FieldVector](structVec).asJava)
          arrowWriter = new ArrowStreamWriter(writeRoot, null, Channels.newChannel(dataOut))
          arrowWriter.start()
        }

        var i = 0
        while (i < cometBatch.numCols()) {
          val src = cometBatch
            .column(i)
            .asInstanceOf[CometDecodedVector]
            .getValueVector
            .asInstanceOf[FieldVector]
          val dst = structVec.getChildByOrdinal(i).asInstanceOf[FieldVector]
          copyVector(src, dst)
          i += 1
        }
        val numRows = cometBatch.numRows()
        structVec.setValueCount(numRows)
        // Mark every row of the struct non-null (all-1 validity). The validity buffer is freshly
        // allocated and zero-initialised, so without this Python would see an all-null struct.
        val validityBytes = (numRows + 7) / 8
        Platform.setMemory(
          structVec.getValidityBuffer.memoryAddress(),
          0xff.toByte,
          validityBytes)
        writeRoot.setRowCount(numRows)
        arrowWriter.writeBatch()

        pythonMetrics("pythonDataSent") += dataOut.size() - startData
        true
      }
    }
  }

  override protected def newReaderIterator(
      stream: DataInputStream,
      writer: Writer,
      startTime: Long,
      env: SparkEnv,
      worker: PythonWorker,
      pid: Option[Int],
      releasedOrClosed: AtomicBoolean,
      context: TaskContext): Iterator[ColumnarBatch] = {
    new ReaderIterator(stream, writer, startTime, env, worker, pid, releasedOrClosed, context) {

      private val allocator =
        CometArrowAllocator.newChildAllocator(s"stdin reader for $pythonExec", 0, Long.MaxValue)
      private var reader: ArrowStreamReader = _
      private var root: VectorSchemaRoot = _
      private var batchLoaded = true

      context.addTaskCompletionListener[Unit] { _ =>
        if (reader != null) {
          reader.close(false)
        }
        allocator.close()
      }

      protected override def read(): ColumnarBatch = {
        if (writer.exception.isDefined) {
          throw writer.exception.get
        }
        try {
          if (reader != null && batchLoaded) {
            batchLoaded = reader.loadNextBatch()
            if (batchLoaded) {
              // Re-wrap the (reloaded) field vectors fresh each batch, mirroring Comet's
              // StreamReader, so each ColumnarBatch reflects the current buffers.
              val vectors: Array[ColumnVector] = root.getFieldVectors.asScala.map { vector =>
                CometVector.getVector(vector, null).asInstanceOf[ColumnVector]
              }.toArray
              val batch = new ColumnarBatch(vectors)
              batch.setNumRows(root.getRowCount)
              pythonMetrics("pythonNumRowsReceived") += root.getRowCount
              batch
            } else {
              reader.close(false)
              allocator.close()
              read()
            }
          } else {
            stream.readInt() match {
              case SpecialLengths.START_ARROW_STREAM =>
                reader = new ArrowStreamReader(stream, allocator)
                root = reader.getVectorSchemaRoot()
                read()
              case SpecialLengths.TIMING_DATA =>
                handleTimingData()
                read()
              case SpecialLengths.PYTHON_EXCEPTION_THROWN =>
                throw handlePythonException()
              case SpecialLengths.END_OF_DATA_SECTION =>
                handleEndOfDataSection()
                null
            }
          }
        } catch handleException
      }
    }
  }

  /**
   * Copy a Comet column into the destination FieldVector. Walks both trees in lockstep: sizes
   * each destination node from the source, copies every buffer with `ArrowBuf.setBytes`, then
   * sets value counts bottom-up so `setValueCount` does not rewrite the offset bytes we just
   * copied. Both source and destination are Comet's (shaded) Arrow vectors, so no shaded /
   * unshaded type crosses.
   */
  private def copyVector(src: FieldVector, dst: FieldVector): Unit = {
    val valueCount = src.getValueCount

    dst match {
      case bfwv: BaseFixedWidthVector =>
        bfwv.allocateNew(valueCount)
      case bvwv: BaseVariableWidthVector =>
        bvwv.allocateNew(src.getDataBuffer.readableBytes, valueCount)
      case blvwv: BaseLargeVariableWidthVector =>
        blvwv.allocateNew(src.getDataBuffer.readableBytes, valueCount)
      case _ =>
        dst.setInitialCapacity(valueCount)
        dst.allocateNew()
    }

    val srcBufs = src.getFieldBuffers
    val dstBufs = dst.getFieldBuffers
    require(
      srcBufs.size == dstBufs.size,
      s"buffer count mismatch for ${dst.getField}: src=${srcBufs.size}, dst=${dstBufs.size}")
    var b = 0
    while (b < srcBufs.size) {
      val s = srcBufs.get(b)
      dstBufs.get(b).setBytes(0, s, 0, s.readableBytes)
      b += 1
    }

    val srcChildren = src.getChildrenFromFields
    val dstChildren = dst.getChildrenFromFields
    require(
      srcChildren.size == dstChildren.size,
      s"child count mismatch for ${dst.getField}: src=${srcChildren.size}, dst=${dstChildren.size}")
    srcChildren.asScala.zip(dstChildren.asScala).foreach { case (sc, dc) =>
      copyVector(sc.asInstanceOf[FieldVector], dc.asInstanceOf[FieldVector])
    }

    // For vectors that fill offset-buffer "holes" in setValueCount (variable-width and list
    // types), set lastSet = vc - 1 first so fillHoles is a no-op and the already-copied offset
    // bytes are preserved.
    dst match {
      case v: BaseVariableWidthVector => v.setLastSet(valueCount - 1)
      case v: BaseLargeVariableWidthVector => v.setLastSet(valueCount - 1)
      case v: ListVector => v.setLastSet(valueCount - 1)
      case v: LargeListVector => v.setLastSet(valueCount - 1)
      case _ =>
    }
    dst.setValueCount(valueCount)
  }
}
