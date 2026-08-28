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

package org.apache.spark.sql.comet.execution.arrow

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.channels.Channels

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.arrow.compression.{CommonsCompressionFactory, ZstdCompressionCodec}
import org.apache.arrow.flatbuf.{RecordBatch => FlatBufRecordBatch}
import org.apache.arrow.memory.{ArrowBuf, BufferAllocator}
import org.apache.arrow.vector.{FieldVector, TypeLayout, ValueVector, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.compression.{CompressionCodec, CompressionUtil, NoCompressionCodec}
import org.apache.arrow.vector.dictionary.DictionaryEncoder
import org.apache.arrow.vector.ipc.{ReadChannel, WriteChannel}
import org.apache.arrow.vector.ipc.message.{ArrowBodyCompression, ArrowFieldNode, ArrowRecordBatch, MessageSerializer}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}
import org.apache.spark.SparkException
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * The on-disk shape of a `CometCachedBatch` payload, and the two operations over it.
 *
 * A cached batch is one encapsulated Arrow IPC RecordBatch message followed by its body, with no
 * Schema message and no end-of-stream marker. The schema is not stored because the reader already
 * has it: `InMemoryRelation` knows the cached relation's attributes, and `Utils.toArrowSchema`
 * maps them to exactly the fields the writer unloaded. Leaving it out saves a schema message per
 * cached batch, which for a wide relation cached in many batches is a large share of the payload
 * that is not data.
 *
 * Compression is applied by Arrow per buffer rather than by wrapping the whole payload in a Spark
 * `CompressionCodec`. That is what makes projection cheap: the message metadata records every
 * buffer's offset and length within the body, so [[readProjected]] can copy out only the buffers
 * of the columns a scan selected and let `VectorLoader` decompress just those. A whole-payload
 * codec would have to inflate everything before any column could be read.
 */
private[comet] object CachedBatchIpc {

  /**
   * The Arrow compression codec named by `spark.comet.exec.inMemoryCache.compression.codec`.
   *
   * Only the write path consults the config. A batch records which codec compressed it, so the
   * read path looks the codec up from the batch itself and keeps reading data cached before the
   * config changed.
   */
  def compressionCodec(codecName: String, zstdLevel: Int): CompressionCodec = codecName match {
    case "none" => NoCompressionCodec.INSTANCE
    // Constructed directly rather than through CompressionCodec.Factory, which ignores the level
    // and always builds a codec at zstd's default.
    case "zstd" => new ZstdCompressionCodec(zstdLevel)
    // Arrow's other codec, LZ4_FRAME, is not offered. It is commons-compress's pure-Java LZ4 --
    // no relation to the JNI-accelerated lz4-java behind spark.io.compression.codec -- and
    // measures three orders of magnitude slower to write than zstd while also producing larger
    // output, so nothing prefers it. Reads still accept it, since the factory the read path uses
    // handles whatever codec a batch records.
    case other =>
      throw new SparkException(
        s"Unsupported Arrow compression codec for Comet's cache: $other. " +
          "Supported values: none, zstd")
  }

  /**
   * Serialize `batch` into one encapsulated IPC RecordBatch message.
   *
   * Returns the message bytes and the on-body compressed size of each top-level column, which the
   * caller records in the statistics row. The sizes come from the message's own buffer layout, so
   * they are the real stored sizes rather than an estimate.
   *
   * Dictionary-encoded columns are decoded to their plain form first. A payload with no Schema
   * message cannot describe a dictionary encoding, and the schema the reader rebuilds from Spark
   * attributes never carries one, so a dictionary-encoded column has nowhere to record either its
   * index type or the dictionary itself. Comet's native scans do produce such columns, so this is
   * a real path, not a defensive one.
   *
   * As in `Utils.serializeBatches`, `batch`'s vectors are cleared once written, so callers gather
   * anything they need from the batch (statistics, for instance) before calling this.
   */
  def serialize(
      batch: ColumnarBatch,
      codec: CompressionCodec,
      allocator: BufferAllocator): (Array[Byte], Array[Long]) = {
    val (vectors, hydrated) = hydrateDictionaries(batch, allocator)
    try {
      val root = new VectorSchemaRoot(vectors.asJava)
      // A batch of zero columns carries only a row count, which a VectorSchemaRoot cannot infer
      // without vectors to measure.
      if (vectors.isEmpty) {
        root.setRowCount(batch.numRows())
      }

      // alignBuffers=true matches the 8-byte buffer alignment readProjected reproduces when it
      // repacks the selected buffers.
      val unloader = new VectorUnloader(root, true, codec, true)
      val recordBatch = unloader.getRecordBatch
      try {
        val fields = vectors.map(_.getField)
        // Serializing consumes the batch, as it does in Utils.serializeBatches. The record batch
        // holds its own buffers by now -- compressed copies, or retained references when the codec
        // is none -- so releasing the vectors here does not touch it. getField still answers
        // afterwards: clearing releases buffers, not the schema.
        //
        // Not load bearing for memory: the plan that produced the batch releases its vectors
        // either way, and dropping this line leaks nothing. It is here because serializeBatches
        // does the same, so both writers leave a batch they were handed in the same state.
        root.clear()

        val out = new ByteArrayOutputStream()
        val channel = new WriteChannel(Channels.newChannel(out))
        MessageSerializer.serialize(channel, recordBatch)
        (out.toByteArray, columnSizes(fields, recordBatch))
      } finally {
        recordBatch.close()
      }
    } finally {
      // Only the vectors this method allocated. The rest belong to the input batch.
      hydrated.foreach(v =>
        try v.close()
        catch { case NonFatal(_) => () })
    }
  }

  /**
   * Read an encapsulated IPC RecordBatch message, materializing off-heap only the buffers of the
   * requested top-level columns.
   *
   * The body is a flat, depth-first sequence of buffers in schema order, so each top-level column
   * owns a contiguous run of buffers whose length is [[fieldBufferCount]]; field nodes and
   * variadic buffer counts run in the same order. The selected columns' bytes are copied into a
   * single off-heap allocation, each buffer 8-byte aligned exactly as Arrow's IPC body lays them
   * out, and the returned batch's buffers are windows into it -- one allocation, no per-buffer
   * bookkeeping.
   *
   * Only the selected buffers are ever decompressed. A buffer's recorded (offset, length) covers
   * its on-body bytes including the uncompressed-length prefix, so a copied window is exactly
   * what the writer emitted; the columns that were not selected are never read, let alone
   * inflated. The copied windows are then decompressed in one pass -- see [[decompressed]] for
   * why that is not left to `VectorLoader` -- so what comes back is an uncompressed batch.
   *
   * The returned batch owns its buffers; the caller closes it.
   */
  def readProjected(
      data: Array[Byte],
      schemaFields: Seq[Field],
      selectedIndices: Array[Int],
      allocator: BufferAllocator): ArrowRecordBatch = {
    val readChannel = new ReadChannel(Channels.newChannel(new ByteArrayInputStream(data)))
    // Reads the message metadata only. The body stays in `data` and is copied selectively below.
    val metadata = MessageSerializer.readMessage(readChannel)
    if (metadata == null) {
      throw new SparkException("Unexpected end of input reading a Comet cached batch")
    }
    val batch =
      metadata.getMessage.header(new FlatBufRecordBatch()).asInstanceOf[FlatBufRecordBatch]
    // serialize writes exactly [encapsulated message][body] and nothing after it, so the body is
    // the tail of `data`.
    val bodyStart = data.length - metadata.getMessageBodyLength.toInt

    val compression =
      if (batch.compression() == null) NoCompressionCodec.DEFAULT_BODY_COMPRESSION
      else new ArrowBodyCompression(batch.compression().codec(), batch.compression().method())

    val nodeStarts = schemaFields.scanLeft(0)(_ + fieldNodeCount(_)).toArray
    val bufferStarts = schemaFields.scanLeft(0)(_ + fieldBufferCount(_)).toArray
    val variadicStarts = schemaFields.scanLeft(0)(_ + fieldVariadicCount(_)).toArray
    val hasVariadic = batch.variadicBufferCountsLength() > 0

    // The selected columns' field nodes, buffer indices and variadic counts, in output order.
    val nodes = new java.util.ArrayList[ArrowFieldNode]()
    val bufferIndices = mutable.ArrayBuffer.empty[Int]
    val variadicCounts = new java.util.ArrayList[java.lang.Long]()
    selectedIndices.foreach { i =>
      val field = schemaFields(i)
      val nodeStart = nodeStarts(i)
      (nodeStart until nodeStart + fieldNodeCount(field)).foreach { j =>
        val node = batch.nodes(j)
        nodes.add(new ArrowFieldNode(node.length(), node.nullCount()))
      }
      val bufferStart = bufferStarts(i)
      (bufferStart until bufferStart + fieldBufferCount(field)).foreach(bufferIndices += _)
      if (hasVariadic) {
        val variadicStart = variadicStarts(i)
        (variadicStart until variadicStart + fieldVariadicCount(field))
          .foreach(j => variadicCounts.add(batch.variadicBufferCounts(j)))
      }
    }

    val layout = bufferIndices.map { j =>
      val buffer = batch.buffers(j)
      (buffer.offset(), buffer.length())
    }
    val alignedSizes = layout.map { case (_, length) => ((length + 7) / 8) * 8 }
    // allocator.buffer(0) is legal but yields a buffer no window can be sliced from, and an
    // all-empty projection (every selected column a NullVector, say) would ask for exactly that.
    val body = allocator.buffer(math.max(alignedSizes.sum, 1L))
    val compressedBatch =
      try {
        val buffers = new java.util.ArrayList[ArrowBuf]()
        var position = 0L
        layout.indices.foreach { k =>
          val (sourceOffset, length) = layout(k)
          if (length > 0) {
            body.setBytes(position, data, bodyStart + sourceOffset.toInt, length.toInt)
          }
          val window = body.slice(position, length)
          window.writerIndex(length)
          buffers.add(window)
          position += alignedSizes(k)
        }
        new ArrowRecordBatch(
          batch.length().toInt,
          nodes,
          buffers,
          compression,
          variadicCounts,
          false)
      } catch {
        case NonFatal(e) =>
          body.close()
          throw e
      }

    // The constructor retained each window; slice() alone does not. Dropping `body`'s own
    // reference leaves the batch as sole owner of the one allocation, so closing the batch below
    // is what frees it -- and closing `body` again here would drive its reference count negative.
    body.close()
    try decompressed(compressedBatch, allocator)
    finally compressedBatch.close()
  }

  /**
   * The same record batch with every buffer decompressed, as a new batch the caller owns.
   *
   * `VectorLoader` would do this itself, but arrow-java 18.3.0 leaks on the failure path:
   * `VectorLoader.loadBuffers` decompresses a field's buffers into a local list and only releases
   * them after the whole field has loaded, so if one buffer of a field fails to decompress, every
   * buffer of that field decompressed before it is unreachable and never freed. A string column
   * is enough to reach it -- its offsets buffer decompresses, then its data buffer throws -- so a
   * single corrupt cached batch leaks off-heap for the life of the executor. Doing the
   * decompression here keeps every allocation reachable from this method's own error path.
   *
   * Buffers are retained before decompressing rather than after, which is the other half of the
   * difference. `decompress` consumes a reference to its input on the paths where it allocates,
   * so retaining afterwards leaves the reference stranded if it throws -- and, when a batch has a
   * single buffer, drops the shared body to zero references and frees it before the retain that
   * was meant to protect it.
   */
  private def decompressed(
      batch: ArrowRecordBatch,
      allocator: BufferAllocator): ArrowRecordBatch = {
    // getCodec is the raw IPC byte; the factory keys off the enum. Both sides of the comparison
    // below have to be CodecType: NoCompressionCodec.COMPRESSION_TYPE is the byte -1, and Scala
    // compares a CodecType against it by universal equality, which is quietly always unequal.
    val codecType =
      CompressionUtil.CodecType.fromCompressionType(batch.getBodyCompression.getCodec)
    val compressed = codecType != CompressionUtil.CodecType.NO_COMPRESSION
    val codec: CompressionCodec =
      if (compressed) CommonsCompressionFactory.INSTANCE.createCodec(codecType)
      else NoCompressionCodec.INSTANCE

    val buffers = new java.util.ArrayList[ArrowBuf]()
    try {
      batch.getBuffers.asScala.foreach { buffer =>
        buffer.getReferenceManager.retain()
        val plain =
          try {
            // An empty buffer carries no compressed length prefix to read.
            if (compressed && buffer.writerIndex() > 0) codec.decompress(allocator, buffer)
            else buffer
          } catch {
            case NonFatal(e) =>
              buffer.getReferenceManager.release()
              throw e
          }
        buffers.add(plain)
      }

      val result = new ArrowRecordBatch(
        batch.getLength,
        batch.getNodes,
        buffers,
        NoCompressionCodec.DEFAULT_BODY_COMPRESSION,
        batch.getVariadicBufferCounts,
        false)
      // The constructor retained each buffer, so drop the references held here.
      buffers.asScala.foreach(_.close())
      result
    } catch {
      case NonFatal(e) =>
        buffers.asScala.foreach { buffer =>
          try buffer.close()
          catch { case NonFatal(closeError) => e.addSuppressed(closeError) }
        }
        throw e
    }
  }

  /**
   * A `VectorLoader` for what [[readProjected]] returns.
   *
   * No compression factory: [[readProjected]] has already decompressed every buffer, so the
   * loader only ever sees a batch marked uncompressed.
   */
  def loaderFor(root: VectorSchemaRoot): org.apache.arrow.vector.VectorLoader =
    new org.apache.arrow.vector.VectorLoader(root)

  /**
   * The on-body compressed size of each top-level column.
   *
   * Each column owns the run of buffers its subtree occupies, so its stored size is the sum of
   * those buffers' recorded lengths. With one payload per batch these are the only per-column
   * sizes available -- there is no separate stream to measure -- and they are exact.
   */
  private def columnSizes(fields: Seq[Field], recordBatch: ArrowRecordBatch): Array[Long] = {
    val buffers = recordBatch.getBuffersLayout
    val starts = fields.scanLeft(0)(_ + fieldBufferCount(_)).toArray
    fields.indices.map { i =>
      (starts(i) until starts(i) + fieldBufferCount(fields(i)))
        .map(j => buffers.get(j).getSize)
        .sum
    }.toArray
  }

  /**
   * Replace every dictionary-encoded column of `batch` with its decoded form.
   *
   * Returns the vectors to write and, separately, the ones allocated here so the caller can close
   * exactly those. Columns that needed no decoding are returned as they are and stay owned by
   * `batch`.
   */
  private def hydrateDictionaries(
      batch: ColumnarBatch,
      allocator: BufferAllocator): (Seq[FieldVector], Seq[ValueVector]) = {
    val hydrated = mutable.ArrayBuffer.empty[ValueVector]
    try {
      val vectors =
        Utils.getBatchFieldVectorsWithProviders(batch).map { case (vector, providerOpt) =>
          val encoding = vector.getField.getDictionary
          if (encoding == null) {
            vector
          } else {
            val dictionary = providerOpt.map(_.lookup(encoding.getId)).orNull
            if (dictionary == null) {
              throw new SparkException(
                s"Column ${vector.getField.getName} is dictionary encoded with ID " +
                  s"${encoding.getId}, but no dictionary with that ID was provided")
            }
            val decoded = DictionaryEncoder.decode(vector, dictionary, allocator)
            hydrated += decoded
            decoded.asInstanceOf[FieldVector]
          }
        }
      (vectors, hydrated.toSeq)
    } catch {
      case NonFatal(e) =>
        hydrated.foreach(v =>
          try v.close()
          catch { case NonFatal(closeError) => e.addSuppressed(closeError) })
        throw e
    }
  }

  /**
   * Number of Arrow buffers a field occupies in a RecordBatch body, including every descendant,
   * in the depth-first order `VectorLoader` consumes them. The type's own count covers its
   * validity and offset/data buffers; each child contributes its whole subtree.
   */
  private def fieldBufferCount(field: Field): Int =
    TypeLayout.getTypeBufferCount(field.getType) +
      field.getChildren.asScala.map(fieldBufferCount).sum

  /** Number of field nodes a field occupies: itself plus every descendant. */
  private def fieldNodeCount(field: Field): Int =
    1 + field.getChildren.asScala.map(fieldNodeCount).sum

  /**
   * Number of variadic buffer counts a field contributes, one per view-type buffer, recursively.
   *
   * Only Utf8View and BinaryView carry one. Comet's cache never writes view vectors today, but
   * the span arithmetic above has to stay correct if that changes.
   */
  private def fieldVariadicCount(field: Field): Int = {
    val own = field.getType match {
      case _: ArrowType.Utf8View | _: ArrowType.BinaryView => 1
      case _ => 0
    }
    own + field.getChildren.asScala.map(fieldVariadicCount).sum
  }
}
