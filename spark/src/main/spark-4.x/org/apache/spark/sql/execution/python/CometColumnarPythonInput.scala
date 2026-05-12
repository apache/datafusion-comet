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

import scala.collection.mutable.ArrayBuffer
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

import org.apache.comet.vector.{CometDecodedVector, CometVectorIpcCopier}

/**
 * `PythonArrowInput` implementation that streams Comet `ColumnarBatch` values to the Python
 * worker as Arrow IPC.
 *
 * Comet's vectors live in the shaded `org.apache.comet.shaded.arrow.*` package at runtime
 * (relocated by comet-common's maven-shade-plugin). This trait must not reference shaded Arrow
 * types directly; buffer copying is delegated to `CometVectorIpcCopier` in comet-common, which
 * crosses the module boundary using only `long` primitives.
 *
 * Per-batch: walk the destination struct's children (unshaded, allocated from the runner's
 * persistent root), allocate each child sized to match the corresponding Comet column, collect
 * dst buffer addresses into a `long[]`, and call the helper for a single bulk memcpy across all
 * buffers.
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
      val src = cometBatch.column(i).asInstanceOf[CometDecodedVector]
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
   * Copy a Comet column (whose Arrow buffers are in the shaded class tree) into the destination
   * FieldVector (allocated from the runner's persistent root, in the unshaded class tree). The
   * actual byte copy happens inside `CometVectorIpcCopier` in comet-common, which references only
   * shaded Arrow types internally and exposes the buffer addresses as `long` primitives.
   */
  private def copyVector(src: CometDecodedVector, dst: FieldVector): Unit = {
    val srcBufSizes = CometVectorIpcCopier.bufferReadableBytes(src)
    val srcValueCounts = CometVectorIpcCopier.valueCounts(src)

    val dstNodes = collectFieldVectors(dst)
    require(
      dstNodes.size == srcValueCounts.length,
      s"tree node count mismatch for ${dst.getField}: " +
        s"dst=${dstNodes.size}, src=${srcValueCounts.length}")

    var bufIdx = 0
    var nodeIdx = 0
    while (nodeIdx < dstNodes.size) {
      val node = dstNodes(nodeIdx)
      val valueCount = srcValueCounts(nodeIdx)
      node match {
        case bfwv: BaseFixedWidthVector =>
          bfwv.allocateNew(valueCount)
        case bvwv: BaseVariableWidthVector =>
          val ownBufCount = node.getFieldBuffers.size
          val dataSize = srcBufSizes(bufIdx + ownBufCount - 1)
          bvwv.allocateNew(dataSize, valueCount)
        case blvwv: BaseLargeVariableWidthVector =>
          val ownBufCount = node.getFieldBuffers.size
          val dataSize = srcBufSizes(bufIdx + ownBufCount - 1)
          blvwv.allocateNew(dataSize, valueCount)
        case _ =>
          node.setInitialCapacity(valueCount)
          node.allocateNew()
      }
      bufIdx += node.getFieldBuffers.size
      nodeIdx += 1
    }
    require(
      bufIdx == srcBufSizes.length,
      s"buffer count mismatch for ${dst.getField}: dst=$bufIdx, src=${srcBufSizes.length}")

    val dstAddrs = collectBufferAddresses(dstNodes, srcBufSizes.length)
    CometVectorIpcCopier.copyBuffersToAddresses(src, dstAddrs)

    // Process nodes bottom-up (leaves first) so that when a composite vector (struct, list)
    // calls setValueCount on its children recursively, those children have already had their
    // lastSet field updated and fillHoles becomes a no-op.
    var fi = dstNodes.size - 1
    while (fi >= 0) {
      val node = dstNodes(fi)
      val vc = srcValueCounts(fi)
      // For vectors that fill offset-buffer "holes" in setValueCount (variable-width and list
      // types), set lastSet = vc - 1 first so fillHoles is a no-op and the already-copied
      // offset bytes are preserved.
      node match {
        case v: BaseVariableWidthVector => v.setLastSet(vc - 1)
        case v: BaseLargeVariableWidthVector => v.setLastSet(vc - 1)
        case v: ListVector => v.setLastSet(vc - 1)
        case v: LargeListVector => v.setLastSet(vc - 1)
        case _ =>
      }
      node.setValueCount(vc)
      fi -= 1
    }
  }

  private def collectFieldVectors(vec: FieldVector): IndexedSeq[FieldVector] = {
    val buf = ArrayBuffer.empty[FieldVector]
    walkFieldVectors(vec, buf)
    buf.toIndexedSeq
  }

  private def walkFieldVectors(vec: FieldVector, buf: ArrayBuffer[FieldVector]): Unit = {
    buf += vec
    vec.getChildrenFromFields.asScala.foreach { child =>
      walkFieldVectors(child.asInstanceOf[FieldVector], buf)
    }
  }

  private def collectBufferAddresses(
      nodes: IndexedSeq[FieldVector],
      expected: Int): Array[Long] = {
    val addrs = new Array[Long](expected)
    var idx = 0
    var ni = 0
    while (ni < nodes.size) {
      val bufs = nodes(ni).getFieldBuffers
      var bi = 0
      while (bi < bufs.size) {
        addrs(idx) = bufs.get(bi).memoryAddress()
        idx += 1
        bi += 1
      }
      ni += 1
    }
    require(idx == expected, s"collected $idx addresses, expected $expected")
    addrs
  }
}
