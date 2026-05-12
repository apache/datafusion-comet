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

import org.apache.arrow.vector.{BaseFixedWidthVector, BaseVariableWidthVector, FieldVector, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.compression.{CompressionCodec, CompressionUtil, NoCompressionCodec}
import org.apache.arrow.vector.ipc.{ArrowStreamWriter, WriteChannel}
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.spark.SparkException
import org.apache.spark.api.python.BasePythonRunner
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.vector.CometDecodedVector

/**
 * `PythonArrowInput` implementation that streams Comet `ColumnarBatch` values to the Python
 * worker as Arrow IPC, bypassing the row materialization that `BasicPythonArrowInput` performs.
 * The persistent root supplied by `PythonArrowInput` carries the wrapped-struct schema
 * (`StructType(Array(StructField("struct", childSchema)))`) so the Python worker contract is
 * preserved.
 *
 * Each call writes one Comet batch. The runner contract repeats `writeNextBatchToArrowStream`
 * until it returns `false`. Per-batch the input trait allocates a destination vector in the
 * persistent root and copies each source buffer via `ArrowBuf.setBytes` -- this is bulk per
 * buffer, not per row, but it is NOT zero-copy: Comet's Parquet reader allocators are independent
 * roots from `ArrowUtils.rootAllocator`.
 */
private[python] trait CometColumnarPythonInput extends PythonArrowInput[Iterator[ColumnarBatch]] {
  self: BasePythonRunner[Iterator[ColumnarBatch], _] =>

  private var currentGroup: Iterator[ColumnarBatch] = _

  // Read the codec name via raw config key so this compiles against Spark 4.0 (which lacks
  // SQLConf.arrowCompressionCodec) as well as 4.1/4.2. The codec instances are obtained
  // through CompressionCodec.Factory (arrow-vector) rather than importing the concrete
  // Lz4CompressionCodec / ZstdCompressionCodec from the separate arrow-compression artifact.
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
      val src = cometBatch
        .column(i)
        .asInstanceOf[CometDecodedVector]
        .getValueVector
        .asInstanceOf[FieldVector]
      val dst = structVec.getChildByOrdinal(i).asInstanceOf[FieldVector]
      copyVector(src, dst)
      i += 1
    }
    structVec.setValueCount(cometBatch.numRows())
    root.setRowCount(cometBatch.numRows())

    // VectorUnloader is lightweight (wraps root); create per-batch to stay compatible
    // across Spark 4.0/4.1/4.2 which differ in how the unloader field is managed.
    val batchUnloader =
      new VectorUnloader(root, /* includeNullCount */ true, cometCodec, /* alignBuffers */ true)
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
   * Copy `src` into `dst` via per-buffer memcpy. Allocates `dst` sized to match `src`, then
   * `ArrowBuf.setBytes` copies each field buffer (validity, offsets, data) wholesale. Recurses
   * into struct / list children.
   *
   * This does NOT transfer buffer ownership and does NOT change refcounts: `src` retains its
   * buffers, `dst` allocates new ones in the runner's allocator. Required because Comet's Parquet
   * reader allocators are independent roots from `ArrowUtils.rootAllocator`.
   */
  private def copyVector(src: FieldVector, dst: FieldVector): Unit = {
    val numRows = src.getValueCount

    dst match {
      case bfwv: BaseFixedWidthVector =>
        bfwv.allocateNew(numRows)
      case bvwv: BaseVariableWidthVector =>
        // Variable-width data buffer size depends on actual byte content, not just numRows.
        // Match the source data buffer's readable bytes.
        val srcFieldBufs = src.getFieldBuffers
        val dataBufIdx = srcFieldBufs.size - 1
        val srcDataSize = srcFieldBufs.get(dataBufIdx).readableBytes
        bvwv.allocateNew(srcDataSize, numRows)
      case _ =>
        dst.setInitialCapacity(numRows)
        dst.allocateNew()
    }

    val srcBufs = src.getFieldBuffers
    val dstBufs = dst.getFieldBuffers
    require(
      srcBufs.size == dstBufs.size,
      s"buffer count mismatch for ${src.getField}: src=${srcBufs.size} dst=${dstBufs.size}")
    var bi = 0
    while (bi < srcBufs.size) {
      val sBuf = srcBufs.get(bi)
      val dBuf = dstBufs.get(bi)
      dBuf.setBytes(0L, sBuf, 0L, sBuf.readableBytes)
      bi += 1
    }

    val srcChildren = src.getChildrenFromFields
    val dstChildren = dst.getChildrenFromFields
    require(
      srcChildren.size == dstChildren.size,
      s"child count mismatch for ${src.getField}: " +
        s"src=${srcChildren.size} dst=${dstChildren.size}")
    var ci = 0
    while (ci < srcChildren.size) {
      copyVector(srcChildren.get(ci), dstChildren.get(ci))
      ci += 1
    }

    dst.setValueCount(numRows)
  }
}
