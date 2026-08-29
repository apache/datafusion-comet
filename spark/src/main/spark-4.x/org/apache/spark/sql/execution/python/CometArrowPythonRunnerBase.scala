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
import java.util.ArrayList
import java.util.concurrent.atomic.AtomicBoolean

import scala.jdk.CollectionConverters._

import org.apache.arrow.memory.{ArrowBuf, BufferAllocator}
import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter, WriteChannel}
import org.apache.arrow.vector.ipc.message.{ArrowFieldNode, ArrowRecordBatch, MessageSerializer}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType}
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python.{BasePythonRunner, PythonRDD, PythonWorker, SpecialLengths}
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

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
 *   - Input: each Comet `ColumnarBatch` is written directly from its shaded Arrow vectors to the
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

  /**
   * Write the worker configuration where Spark 4.0 and 4.1 workers expect it. Spark 4.2 moved
   * this map into [[BasePythonRunner.runnerConf]], so its subclass overrides this hook with a
   * no-op.
   */
  protected def writeWorkerConf(dataOut: DataOutputStream): Unit = {
    dataOut.writeInt(workerConf.size)
    for ((key, value) <- workerConf) {
      PythonRDD.writeUTF(key, dataOut)
      PythonRDD.writeUTF(value, dataOut)
    }
  }

  /**
   * Input schema as Comet hands it to the runner: a single non-nullable struct named "struct"
   * whose children are the user's input columns. Comet's FFI-imported vectors carry Arrow
   * `Field`s with null names (Comet uses positional schema), so these names are the source of
   * truth for the field names written into the IPC stream that the Python worker reads by name.
   */
  protected def schema: StructType

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
      private var streamFields: Seq[Field] = _

      // The runner's input schema is a single struct column ("struct") whose children are the
      // user's input columns (see `schema` above). Cast once here rather than at each use site.
      private lazy val inputStructType = schema.head.dataType.asInstanceOf[StructType]

      context.addTaskCompletionListener[Unit] { _ =>
        if (writeRoot != null) {
          writeRoot.close()
        }
        allocator.close()
      }

      protected override def writeCommand(dataOut: DataOutputStream): Unit = {
        // handleMetadataBeforeExec: write the worker config as key/value string pairs.
        writeWorkerConf(dataOut)
        writeUDF(dataOut)
      }

      /** Build the schema-only struct root and start the writer from the given child fields. */
      private def startWriter(childFields: Seq[Field], dataOut: DataOutputStream): Unit = {
        val structField =
          new Field(
            "struct",
            new FieldType(false, ArrowType.Struct.INSTANCE, null),
            childFields.asJava)
        val structVec = structField.createVector(allocator).asInstanceOf[StructVector]
        // Declare the root's schema from `structField`, not from `structVec.getField`: Arrow's
        // `MinorType.NULL` factory builds a NullType map key as a nullable `NullVector`, so the
        // live vector reports an invalid key field even when `structField` is valid.
        // `Utils.newArrowStreamWriter` repairs the declared schema if needed and returns the root
        // the writer is bound to, which is the one to close.
        val declaredRoot =
          new VectorSchemaRoot(Seq(structField).asJava, Seq[FieldVector](structVec).asJava, 0)
        val (boundRoot, writer) =
          Utils.newArrowStreamWriter(declaredRoot, null, Channels.newChannel(dataOut))
        writeRoot = boundRoot
        arrowWriter = writer
        arrowWriter.start()
      }

      override def writeNextInputToStream(dataOut: DataOutputStream): Boolean = {
        while (currentGroup == null || !currentGroup.hasNext) {
          if (!inputIterator.hasNext) {
            if (arrowWriter == null) {
              // No input batch was ever produced (e.g. an upstream filter removed every row).
              // Still emit a valid, empty Arrow IPC stream so the Python worker's
              // ArrowStreamReader reads a schema and then sees zero batches, instead of failing
              // on an absent stream ("Invalid IPC stream: negative continuation token"). There is
              // no sample batch, so derive the schema from the Spark input schema. The timezone is
              // irrelevant here because no rows are exchanged.
              val childFields = inputStructType.fields.toSeq.map(f =>
                Utils.toArrowField(f.name, f.dataType, nullable = true, "UTC"))
              startWriter(childFields, dataOut)
            }
            arrowWriter.end()
            return false
          }
          currentGroup = inputIterator.next()
        }

        val cometBatch = currentGroup.next()
        val startData = dataOut.size()
        val sourceVectors = (0 until cometBatch.numCols()).map { i =>
          cometBatch
            .column(i)
            .asInstanceOf[CometDecodedVector]
            .getValueVector
            .asInstanceOf[FieldVector]
        }
        val batchFields = sourceVectors.map(_.getField)

        if (arrowWriter == null) {
          // Build the schema-only struct root once from the first batch's child fields.
          // mapInArrow/mapInPandas exchange the columns under a single non-nullable struct.
          // Comet's FFI-imported vectors leave the Arrow Field name null, so restore the real
          // column names from the input schema (the worker reads columns by name, and shaded
          // Arrow rejects a null field name). Keep the field types and child structure as-is so
          // the advertised schema matches the source buffers. Keeping the type as-is also means
          // a TimestampType reaches the worker with Comet's UTC time zone
          // rather than the session zone vanilla Spark would label it with; this is a documented
          // limitation (see pyarrow-udfs.md), not a value difference, since the stored instant is
          // identical.
          val childNames = inputStructType.fieldNames
          streamFields = batchFields.zipWithIndex.map { case (field, i) =>
            // A NullType map key comes back from Arrow as a nullable `NullVector`, which
            // `MapVector.initializeChildrenFromFields` rejects when `createVector` rebuilds the
            // struct in `startWriter`. Repair the key nullability the same way
            // `Utils.serializeBatches` does.
            Utils.withNonNullableMapKeys(renamed(field, childNames(i), forceNullable = true))
          }
          startWriter(streamFields, dataOut)
        }

        // Union branches may differ in names, nullability, or descriptive metadata. Only
        // differences that change how the advertised schema interprets the buffers are invalid.
        require(
          CometArrowPythonRunnerBase.hasCompatibleSchema(streamFields, batchFields),
          s"Arrow input schema changed between batches: expected $streamFields, got $batchFields")

        CometArrowPythonRunnerBase.serializeBatch(
          new WriteChannel(Channels.newChannel(dataOut)),
          sourceVectors,
          cometBatch.numRows(),
          allocator)

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
   * materialize the struct. Keeping the type and structure intact means the advertised schema
   * still mirrors the Comet source vectors serialized directly into each record batch.
   */
  private def renamed(
      field: Field,
      name: String,
      forceNullable: Boolean,
      isMapEntry: Boolean = false): Field = {
    // Map entries and keys must stay non-nullable, but values and fields nested inside a
    // complex key may be nullable. Do not propagate the restriction through the whole subtree.
    val isMap = field.getType.isInstanceOf[ArrowType.Map]
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
            forceNullable = !isMap && !(isMapEntry && idx == 0),
            isMapEntry = isMap)
        }.asJava
    // Force the field nullable where allowed. Comet's FFI-imported vectors may carry a
    // non-nullable Arrow `Field` even for columns that contain nulls (Comet uses positional schema
    // and does not round-trip Spark's nullability), and the worker rejects a null value under a
    // non-nullable field (`from_pandas(pdf, schema=batch.schema)` raises). Marking the field
    // nullable is a safe superset; Arrow IPC permits an empty validity buffer when its field node
    // reports no null values.
    val ft = field.getFieldType
    val nullable = forceNullable || ft.isNullable
    val newFt = new FieldType(nullable, ft.getType, ft.getDictionary, ft.getMetadata)
    new Field(name, newFt, newChildren)
  }
}

private[python] object CometArrowPythonRunnerBase {

  // Extensions can change interpretation even when their underlying storage types match.
  private val extensionMetadataKeys = Seq(
    ArrowType.ExtensionType.EXTENSION_METADATA_KEY_NAME,
    ArrowType.ExtensionType.EXTENSION_METADATA_KEY_METADATA)

  private def areArrowTypesCompatible(expected: ArrowType, actual: ArrowType): Boolean = {
    if (expected == actual) {
      true
    } else {
      (expected, actual) match {
        case (left: ArrowType.Timestamp, right: ArrowType.Timestamp) =>
          // Native scans use UTC, while date_trunc can retain the equivalent Etc/UTC session
          // zone. Both interpret the same instants, so preserve the stream schema and buffers.
          left.getUnit == right.getUnit &&
          ((left.getTimezone == "UTC" && right.getTimezone == "Etc/UTC") ||
            (left.getTimezone == "Etc/UTC" && right.getTimezone == "UTC"))
        case _ => false
      }
    }
  }

  /** Names, nullability and ordinary field metadata do not change the IPC buffer layout. */
  private[python] def hasCompatibleSchema(expected: Seq[Field], actual: Seq[Field]): Boolean = {
    expected.size == actual.size && expected.zip(actual).forall { case (left, right) =>
      areArrowTypesCompatible(left.getType, right.getType) &&
      left.getDictionary == right.getDictionary &&
      extensionMetadataKeys.forall(key =>
        left.getMetadata.get(key) == right.getMetadata.get(key)) &&
      hasCompatibleSchema(left.getChildren.asScala.toSeq, right.getChildren.asScala.toSeq)
    }
  }

  /**
   * Serialize source vectors directly beneath the non-null struct advertised in the IPC stream.
   *
   * VectorUnloader recursively retains the source buffers without moving them between allocators.
   * The wrapping record batch takes its own references, so closing both temporary batches
   * restores the original reference counts after the synchronous pipe write. The borrowed
   * VectorSchemaRoot must never be closed because its vectors are owned by the input
   * ColumnarBatch.
   */
  private[python] def serializeBatch(
      writeChannel: WriteChannel,
      sourceVectors: Seq[FieldVector],
      numRows: Int,
      allocator: BufferAllocator): Unit = {
    val sourceRoot =
      new VectorSchemaRoot(sourceVectors.map(_.getField).asJava, sourceVectors.asJava, numRows)
    val sourceBatch = new VectorUnloader(sourceRoot).getRecordBatch
    try {
      val validityBytes = (numRows.toLong + 7L) / 8L
      val structValidity = allocator.buffer(validityBytes)
      try {
        if (validityBytes > 0) {
          structValidity.setOne(0L, validityBytes)
        }
        structValidity.writerIndex(validityBytes)

        val nodes = new ArrayList[ArrowFieldNode](sourceBatch.getNodes.size() + 1)
        nodes.add(new ArrowFieldNode(numRows, 0))
        nodes.addAll(sourceBatch.getNodes)

        val buffers = new ArrayList[ArrowBuf](sourceBatch.getBuffers.size() + 1)
        buffers.add(structValidity)
        buffers.addAll(sourceBatch.getBuffers)

        val wrappedBatch = new ArrowRecordBatch(
          numRows,
          nodes,
          buffers,
          sourceBatch.getBodyCompression,
          sourceBatch.getVariadicBufferCounts,
          true)
        try {
          MessageSerializer.serialize(writeChannel, wrappedBatch)
        } finally {
          wrappedBatch.close()
        }
      } finally {
        structValidity.close()
      }
    } finally {
      sourceBatch.close()
    }
  }
}
