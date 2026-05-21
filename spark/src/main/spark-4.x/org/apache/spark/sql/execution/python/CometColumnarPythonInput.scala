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

import java.io.DataOutputStream
import java.nio.channels.Channels

import scala.jdk.CollectionConverters._

import org.apache.arrow.vector.{BaseFixedWidthVector, BaseLargeVariableWidthVector, BaseVariableWidthVector, FieldVector, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.complex.{LargeListVector, ListVector, StructVector}
import org.apache.arrow.vector.compression.{CompressionCodec, CompressionUtil, NoCompressionCodec}
import org.apache.arrow.vector.ipc.{ArrowStreamWriter, WriteChannel}
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.spark.SparkException
import org.apache.spark.api.python.BasePythonRunner
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.Platform

import org.apache.comet.vector.CometDecodedVector

/**
 * `PythonArrowInput` implementation that streams Comet `ColumnarBatch` values to the Python
 * worker as Arrow IPC.
 *
 * Per batch: walk the destination struct's children, allocate each child sized to match the
 * corresponding Comet column, and copy each buffer with `ArrowBuf.setBytes`. The current path
 * does two copies per batch: this one (Comet vector buffers → destination IPC root), and a
 * second one inside `VectorUnloader` / `MessageSerializer.serialize` (root → pipe). The pipe
 * write is structural — Spark's transport to Python is fork + pipe + Arrow IPC, so the buffer
 * bytes must reach the pipe at least once. Dropping the first copy by serialising directly
 * from Comet's vectors is tracked in #4294; once done, the path is at the single-copy floor.
 *
 * The cross-allocator constraint on `TransferPair` is independent of the copy count: even after
 * #4294, true zero-copy at the JVM boundary is blocked because Comet's source `FieldVector`s
 * are imported from native via Arrow C Data Interface (their buffers route `release` through
 * FFI), while Spark's destination IPC root is a child of `ArrowUtils.rootAllocator`. The two
 * reference managers cannot share buffers.
 */
private[python] trait CometColumnarPythonInput extends PythonArrowInput[Iterator[ColumnarBatch]] {
  self: BasePythonRunner[Iterator[ColumnarBatch], _] =>

  private var currentGroup: Iterator[ColumnarBatch] = _

  // Constructed once per task: `root` (the trait's persistent destination IPC root) and
  // `cometCodec` are both stable across the partition. `getRecordBatch` reads the current
  // contents of `root.getFieldVectors` on every call, so re-using the unloader is safe.
  private lazy val batchUnloader: VectorUnloader =
    new VectorUnloader(root, /* includeNullCount */ true, cometCodec, /* alignBuffers */ true)

  // Read the codec name via raw config key. Spark 4.0.x has no `SQLConf.arrowCompressionCodec`
  // accessor at all (it was added after the 4.0 line was cut), so a typed `ShimSQLConf`
  // forwarder would still need a stringly-typed fallback for the 4.0 build. The codec instances
  // are obtained through `CompressionCodec.Factory` (arrow-vector) rather than importing the
  // concrete `Lz4CompressionCodec` / `ZstdCompressionCodec` from the separate
  // arrow-compression artifact, which Comet does not depend on.
  private lazy val cometCodec: CompressionCodec = {
    val factory = CompressionCodec.Factory.INSTANCE
    SQLConf.get.getConfString("spark.sql.execution.arrow.compression.codec", "none") match {
      case "none" => NoCompressionCodec.INSTANCE
      case "lz4" =>
        factory.createCodec(CompressionUtil.CodecType.LZ4_FRAME)
      case "zstd" =>
        val level =
          SQLConf.get.getConfString("spark.sql.execution.arrow.compression.zstd.level", "3").toInt
        factory.createCodec(CompressionUtil.CodecType.ZSTD, level)
      case other =>
        throw SparkException.internalError(
          s"Unsupported Arrow compression codec: $other. Supported values: none, lz4, zstd")
    }
  }

  override protected def writeNextBatchToArrowStream(
      root: VectorSchemaRoot,
      writer: ArrowStreamWriter,
      dataOut: DataOutputStream,
      inputIterator: Iterator[Iterator[ColumnarBatch]]): Boolean = {

    while (currentGroup == null || !currentGroup.hasNext) {
      if (!inputIterator.hasNext) {
        super[PythonArrowInput].close()
        return false
      }
      currentGroup = inputIterator.next()
    }

    val cometBatch = currentGroup.next()
    val startData = dataOut.size()
    val structVec = root.getVector(0).asInstanceOf[StructVector]

    var i = 0
    while (i < cometBatch.numCols()) {
      val src =
        cometBatch
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
    // Mark every row in the struct as non-null (all-1 validity bits). The struct validity
    // buffer is freshly allocated (or cleared) and zero-initialised, so without this step
    // Python would see an all-null struct column and return null for every output row.
    val validityBytes = (numRows + 7) / 8
    Platform.setMemory(structVec.getValidityBuffer.memoryAddress(), 0xff.toByte, validityBytes)
    root.setRowCount(numRows)

    val recordBatch = batchUnloader.getRecordBatch
    try {
      val writeChannel = new WriteChannel(Channels.newChannel(dataOut))
      MessageSerializer.serialize(writeChannel, recordBatch)
    } finally {
      recordBatch.close()
    }

    pythonMetrics("pythonDataSent") += dataOut.size() - startData
    true
  }

  /**
   * Copy a Comet column into the destination FieldVector. Walks both trees in lockstep: sizes
   * each destination node from the source, copies every buffer with `ArrowBuf.setBytes`, then
   * sets value counts bottom-up so `setValueCount` does not rewrite the offset bytes we just
   * copied.
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
    // types), set lastSet = vc - 1 first so fillHoles is a no-op and the already-copied
    // offset bytes are preserved.
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
