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
import org.apache.arrow.vector.util.VectorSchemaRootAppender
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python.{BasePythonRunner, PythonRDD, PythonWorker, SpecialLengths}
import org.apache.spark.sql.comet.shims.CometArrowPythonInputConfig
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.unsafe.Platform

import org.apache.comet.CometArrowAllocator
import org.apache.comet.vector.CometVector

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

  protected def inputConfig: CometArrowPythonInputConfig

  protected def writeWorkerConf: Boolean = true

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
      private var continuousState: RootState = _

      private case class RootState(
          root: VectorSchemaRoot,
          vectors: IndexedSeq[FieldVector],
          structVector: Option[StructVector],
          writer: Option[ArrowStreamWriter])

      context.addTaskCompletionListener[Unit] { _ =>
        if (continuousState != null) {
          continuousState.root.close()
          continuousState = null
        }
        allocator.close()
      }

      protected override def writeCommand(dataOut: DataOutputStream): Unit = {
        if (writeWorkerConf) {
          dataOut.writeInt(workerConf.size)
          for ((k, v) <- workerConf) {
            PythonRDD.writeUTF(k, dataOut)
            PythonRDD.writeUTF(v, dataOut)
          }
        }
        writeUDF(dataOut)
      }

      private def startRoot(
          sample: Option[ColumnarBatch],
          dataOut: DataOutputStream,
          withWriter: Boolean): RootState = {
        val inputSchema = inputConfig.schema
        val fields = sample match {
          case Some(batch) =>
            require(
              batch.numCols() == inputSchema.length,
              s"Input column count ${batch.numCols()} does not match ${inputSchema.length}")
            (0 until batch.numCols()).map { i =>
              val field = batch.column(i).asInstanceOf[CometVector].getValueVector.getField
              renamed(field, inputSchema.fields(i).name, forceNullable = true)
            }
          case None =>
            inputSchema.fields.toSeq.map(f =>
              Utils.toArrowField(f.name, f.dataType, nullable = true, "UTC"))
        }

        val (root, vectors, structVector) =
          if (inputConfig.structInput) {
            val structField =
              new Field(
                "struct",
                new FieldType(false, ArrowType.Struct.INSTANCE, null),
                fields.asJava)
            val structVec = structField.createVector(allocator).asInstanceOf[StructVector]
            (
              new VectorSchemaRoot(Seq[FieldVector](structVec).asJava),
              fields.indices
                .map(i => structVec.getChildByOrdinal(i).asInstanceOf[FieldVector])
                .toIndexedSeq,
              Some(structVec))
          } else {
            val rootVectors = fields.map(_.createVector(allocator).asInstanceOf[FieldVector])
            (new VectorSchemaRoot(rootVectors.asJava), rootVectors.toIndexedSeq, None)
          }
        val writer =
          if (withWriter) {
            val streamWriter =
              new ArrowStreamWriter(root, null, Channels.newChannel(dataOut))
            streamWriter.start()
            Some(streamWriter)
          } else {
            None
          }
        RootState(root, vectors, structVector, writer)
      }

      private def copyBatch(batch: ColumnarBatch, state: RootState): Unit = {
        var i = 0
        while (i < batch.numCols()) {
          val src = batch
            .column(i)
            .asInstanceOf[CometVector]
            .getValueVector
            .asInstanceOf[FieldVector]
          copyVector(src, state.vectors(i))
          i += 1
        }
        val numRows = batch.numRows()
        state.structVector.foreach { structVec =>
          structVec.setValueCount(numRows)
          val validityBytes = (numRows + 7) / 8
          Platform.setMemory(
            structVec.getValidityBuffer.memoryAddress(),
            0xff.toByte,
            validityBytes)
        }
        state.root.setRowCount(numRows)
      }

      private def closeInput(batch: ColumnarBatch): Unit = {
        if (inputConfig.grouped) {
          batch.close()
        }
      }

      private def withInputBatch[T](batch: ColumnarBatch)(f: => T): T =
        try {
          f
        } finally {
          closeInput(batch)
        }

      private def writeContinuous(dataOut: DataOutputStream): Boolean = {
        while (currentGroup == null || !currentGroup.hasNext) {
          if (!inputIterator.hasNext) {
            if (continuousState == null) {
              continuousState = startRoot(None, dataOut, withWriter = true)
            }
            continuousState.writer.get.end()
            return false
          }
          currentGroup = inputIterator.next()
        }

        val cometBatch = currentGroup.next()
        val startData = dataOut.size()
        withInputBatch(cometBatch) {
          if (continuousState == null) {
            continuousState = startRoot(Some(cometBatch), dataOut, withWriter = true)
          }
          copyBatch(cometBatch, continuousState)
          continuousState.writer.get.writeBatch()
        }
        pythonMetrics("pythonDataSent") += dataOut.size() - startData
        true
      }

      private def writeFramedGroup(dataOut: DataOutputStream): Boolean = {
        if (!inputIterator.hasNext) {
          dataOut.writeInt(0)
          return false
        }

        val startData = dataOut.size()
        dataOut.writeInt(1)
        val batches = inputIterator.next()
        val first = if (batches.hasNext) Some(batches.next()) else None
        var state: RootState = null
        try {
          first match {
            case Some(batch) =>
              withInputBatch(batch) {
                state = startRoot(Some(batch), dataOut, withWriter = true)
                copyBatch(batch, state)
                state.writer.get.writeBatch()
              }
            case None =>
              state = startRoot(None, dataOut, withWriter = true)
          }
          batches.foreach { batch =>
            withInputBatch(batch) {
              copyBatch(batch, state)
              state.writer.get.writeBatch()
            }
          }
          state.writer.get.end()
        } finally {
          if (state != null) {
            state.root.close()
          }
        }
        pythonMetrics("pythonDataSent") += dataOut.size() - startData
        true
      }

      private def writeSingleBatchGroup(dataOut: DataOutputStream): Boolean = {
        if (!inputIterator.hasNext) {
          if (continuousState == null) {
            continuousState = startRoot(None, dataOut, withWriter = true)
          }
          continuousState.writer.get.end()
          return false
        }

        val startData = dataOut.size()
        val batches = inputIterator.next()
        val first = if (batches.hasNext) Some(batches.next()) else None
        if (continuousState == null) {
          first match {
            case Some(batch) =>
              withInputBatch(batch) {
                continuousState = startRoot(Some(batch), dataOut, withWriter = true)
                copyBatch(batch, continuousState)
              }
            case None =>
              continuousState = startRoot(None, dataOut, withWriter = true)
          }
        } else {
          first.foreach { batch =>
            withInputBatch(batch) {
              copyBatch(batch, continuousState)
            }
          }
        }

        batches.foreach { batch =>
          withInputBatch(batch) {
            val scratch = startRoot(Some(batch), dataOut, withWriter = false)
            try {
              copyBatch(batch, scratch)
              VectorSchemaRootAppender.append(continuousState.root, scratch.root)
            } finally {
              scratch.root.close()
            }
          }
        }
        continuousState.writer.get.writeBatch()
        continuousState.root.clear()
        continuousState.root.setRowCount(0)
        pythonMetrics("pythonDataSent") += dataOut.size() - startData
        true
      }

      override def writeNextInputToStream(dataOut: DataOutputStream): Boolean = {
        if (!inputConfig.grouped) {
          writeContinuous(dataOut)
        } else if (inputConfig.framedGroups) {
          writeFramedGroup(dataOut)
        } else {
          writeSingleBatchGroup(dataOut)
        }
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
            val bytesReadStart = reader.bytesRead()
            batchLoaded = reader.loadNextBatch()
            if (batchLoaded) {
              // Re-wrap the (reloaded) field vectors fresh each batch, mirroring Comet's
              // StreamReader, so each ColumnarBatch reflects the current buffers.
              val vectors: Array[ColumnVector] = root.getFieldVectors.asScala.map { vector =>
                CometVector.getVector(vector, null).asInstanceOf[ColumnVector]
              }.toArray
              val batch = new ColumnarBatch(vectors)
              batch.setNumRows(root.getRowCount)
              // Track bytes read so `pythonDataReceived` matches the vanilla fallback path
              // (`BasicPythonArrowOutput`), which meters the same delta around `loadNextBatch`.
              pythonMetrics("pythonDataReceived") += reader.bytesRead() - bytesReadStart
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
   * Rebuild `field` with `name`, preserving its Arrow type and child structure. Any nested child
   * whose name Comet's FFI import left null is given a positional placeholder so shaded Arrow can
   * materialize the struct. Keeping the type and structure intact means the destination tree
   * still mirrors the Comet source tree for [[copyVector]].
   */
  private def renamed(field: Field, name: String, forceNullable: Boolean): Field = {
    // A Map's descendants must keep their original nullability: Arrow requires the entries struct
    // (and its key) to be non-nullable, and `MapVector.createVector` rejects a nullable entries
    // struct. Stop forcing nullable once we enter a Map subtree.
    val childrenForceNullable = forceNullable && !field.getType.isInstanceOf[ArrowType.Map]
    val children = field.getChildren
    val newChildren =
      if (children.isEmpty) children
      else
        children.asScala.zipWithIndex.map { case (child, idx) =>
          // Only null-named FFI children get the positional `_$idx` placeholder. This assumes no
          // real sibling is literally named `_0`, `_1`, ... (which would collide); struct fields
          // reaching here carry their real names, so a null name means Comet's FFI import dropped
          // it and a synthetic positional name is safe.
          renamed(
            child,
            if (child.getName == null) s"_$idx" else child.getName,
            childrenForceNullable)
        }.asJava
    // Force the field nullable where allowed. Comet's FFI-imported vectors may carry a
    // non-nullable Arrow `Field` even for columns that contain nulls (Comet uses positional schema
    // and does not round-trip Spark's nullability), and the worker rejects a null value under a
    // non-nullable field (`from_pandas(pdf, schema=batch.schema)` raises). Marking the field
    // nullable is a safe superset; `copyVector` fills an all-valid validity buffer when the source
    // has no nulls.
    val ft = field.getFieldType
    val nullable = forceNullable || ft.isNullable
    val newFt = new FieldType(nullable, ft.getType, ft.getDictionary, ft.getMetadata)
    new Field(name, newFt, newChildren)
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
    srcBufs.asScala.zip(dstBufs.asScala).foreach { case (s, d) =>
      d.setBytes(0, s, 0, s.readableBytes)
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

    // Every destination field is nullable (see `renamed`), so the worker reads the validity
    // buffer. When the source has no nulls its validity buffer may be empty (Comet omits it),
    // which would otherwise leave the freshly-allocated destination validity all-zero and make
    // the worker see every value as null. Set all-valid in that case. Done after setValueCount,
    // which can rewrite validity, mirroring the struct-level all-valid fill in writeNextInput.
    if (valueCount > 0 && dst.getField.isNullable && src.getNullCount == 0) {
      val validityBytes = (valueCount + 7) / 8
      Platform.setMemory(dst.getValidityBuffer.memoryAddress(), 0xff.toByte, validityBytes)
    }
  }
}
