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
import org.apache.arrow.vector.{FieldVector, TypeLayout, ValueVector, VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.compression.{CompressionCodec, CompressionUtil, NoCompressionCodec}
import org.apache.arrow.vector.dictionary.DictionaryEncoder
import org.apache.arrow.vector.ipc.{ReadChannel, WriteChannel}
import org.apache.arrow.vector.ipc.message.{ArrowBodyCompression, ArrowFieldNode, ArrowRecordBatch, MessageSerializer}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}
import org.apache.arrow.vector.util.DataSizeRoundingUtil
import org.apache.spark.SparkException
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.vector.CometVector

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
 * buffer's offset and length within the body, so [[Projection.load]] can copy out only the
 * buffers of the columns a scan selected and let `VectorLoader` decompress just those. A
 * whole-payload codec would have to inflate everything before any column could be read.
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

  // Room for the encapsulated metadata message that precedes the body. The message is a small
  // flatbuffer whose size grows with the field count, not the data, so this is a starting size for
  // the output buffer rather than a bound -- it grows if a very wide schema needs more.
  private val METADATA_SIZE_HINT = 8 * 1024

  // Decompressors are stateless and shared. Resolving one per cached batch would allocate a codec
  // per batch on every scan, and the enum lookup walks the CodecType values each time.
  private val readCodecs: Map[CompressionUtil.CodecType, CompressionCodec] =
    CompressionUtil.CodecType
      .values()
      .filter(_ != CompressionUtil.CodecType.NO_COMPRESSION)
      .map(t => t -> CommonsCompressionFactory.INSTANCE.createCodec(t))
      .toMap

  /**
   * The decompressor for a body-compression byte, or None when the batch is stored plain.
   *
   * A byte this build does not recognize is rejected rather than read as plain bytes.
   * `CodecType.fromCompressionType` answers `NO_COMPRESSION` for anything outside its enum, so
   * taking its word for it would turn a corrupt payload into garbage values instead of an error.
   */
  private def readCodec(compressionType: Byte): Option[CompressionCodec] =
    if (compressionType == NoCompressionCodec.COMPRESSION_TYPE) {
      None
    } else {
      val codecType = CompressionUtil.CodecType.fromCompressionType(compressionType)
      if (codecType == CompressionUtil.CodecType.NO_COMPRESSION) {
        throw new SparkException(
          s"Comet cached batch records an unknown Arrow compression codec: $compressionType")
      }
      Some(readCodecs(codecType))
    }

  /**
   * Whether `batch`'s vectors can be unloaded as they stand, or have to be converted first.
   *
   * The payload records no schema, so [[Projection]] rebuilds the fields from the cached
   * relation's Spark attributes and reads the body against them. The direct write path unloads
   * whatever vectors the cached plan produced, and one Spark type can arrive as more than one
   * Arrow type: `BinaryType` is a `VarBinaryVector` from Comet's own scans but a
   * `FixedSizeBinaryVector` from an accelerated `mapInArrow` or an Iceberg `fixed[N]` read, and
   * those occupy three buffers and two. Writing one and reading the other shifts every buffer
   * from that column on, which is wrong values rather than an error, so a batch that does not
   * already carry the reader's types is converted instead.
   *
   * The same holds inside a nested column, which `Utils.isArrowBacked` does not look at: it
   * answers for the top-level vector only, so a struct of large strings passes it while its child
   * is stored with 64-bit offsets and read with 32-bit ones.
   *
   * Names, nullability and a timestamp's timezone are not compared. None of them changes how the
   * reader interprets the body, and the writer's legitimately differ -- a Comet scan labels
   * timestamps with the session's zone where the reader rebuilds them as UTC, which is a label
   * only: Spark's representation is micros since the epoch either way.
   */
  def matchesReaderLayout(batch: ColumnarBatch, readerFields: Seq[Field]): Boolean =
    batch.numCols() == readerFields.length &&
      (0 until batch.numCols()).forall { i =>
        batch.column(i) match {
          case v: CometVector => sameLayout(writtenField(v), readerFields(i))
          case _ => false
        }
      }

  /**
   * The field a column reaches the body as.
   *
   * A dictionary-encoded vector's own field carries the index type, not the values', because
   * [[decodeDictionaries]] replaces it with the decoded form before anything is unloaded.
   * Resolved through the same `lookupDictionary` the write path uses, so a batch missing its
   * dictionary fails here exactly as it would there.
   */
  private def writtenField(column: CometVector): Field = {
    val vector = column.getValueVector
    if (vector.getField.getDictionary == null) {
      vector.getField
    } else {
      Utils
        .lookupDictionary(vector.asInstanceOf[FieldVector], Option(column.getDictionaryProvider))
        .getVector
        .getField
    }
  }

  private def sameLayout(written: Field, read: Field): Boolean =
    layoutType(written.getType) == layoutType(read.getType) && {
      val writtenChildren = written.getChildren
      val readChildren = read.getChildren
      writtenChildren.size == readChildren.size &&
      (0 until writtenChildren.size).forall(i =>
        sameLayout(writtenChildren.get(i), readChildren.get(i)))
    }

  private def layoutType(t: ArrowType): ArrowType = t match {
    case ts: ArrowType.Timestamp if ts.getTimezone != null =>
      new ArrowType.Timestamp(ts.getUnit, "UTC")
    case other => other
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
    val (vectors, decoded) = decodeDictionaries(batch, allocator)
    try {
      val root = new VectorSchemaRoot(vectors.asJava)
      // A batch of zero columns carries only a row count, which a VectorSchemaRoot cannot infer
      // without vectors to measure.
      if (vectors.isEmpty) {
        root.setRowCount(batch.numRows())
      }

      // Unloaded plain and compressed afterwards rather than by handing the codec to the unloader;
      // see compressed for why.
      val unloader = new VectorUnloader(root, true, NoCompressionCodec.INSTANCE, true)
      val plainBatch = unloader.getRecordBatch
      val recordBatch =
        try compressed(plainBatch, codec, allocator)
        finally plainBatch.close()
      try {
        val fields = vectors.map(_.getField)
        // Leaves the batch in the state serializeBatches leaves one. The record batch holds its
        // own buffers by now, so this does not touch it, and getField still answers afterwards:
        // clearing releases buffers, not the schema.
        root.clear()

        // Sized up front from the body length the record batch already knows, plus room for the
        // metadata message. An unsized ByteArrayOutputStream starts at 32 bytes and doubles, so a
        // multi-MiB payload would be reallocated and recopied a dozen-odd times per batch.
        val sizeHint = recordBatch.computeBodyLength() + METADATA_SIZE_HINT
        val out = new ByteArrayOutputStream(
          math.min(math.max(sizeHint, METADATA_SIZE_HINT), Int.MaxValue.toLong).toInt)
        val channel = new WriteChannel(Channels.newChannel(out))
        MessageSerializer.serialize(channel, recordBatch)
        (out.toByteArray, columnSizes(fields, recordBatch))
      } finally {
        recordBatch.close()
      }
    } finally {
      // Only the vectors this method allocated. The rest belong to the input batch.
      decoded.foreach(v =>
        try v.close()
        catch { case NonFatal(_) => () })
    }
  }

  /**
   * Everything about reading one projection of this format that does not change between batches.
   * A scan builds one of these per partition.
   *
   * The index arithmetic walks every field of the cached relation rather than just the projected
   * ones, so recomputing it per batch would make the bookkeeping O(total columns) against
   * O(selected columns) of useful work -- worst in exactly the wide-relation, narrow-projection
   * case this format exists for.
   *
   * Holding the projected `Schema` here too is what keeps it consistent with the buffers:
   * [[Projection.load]] packs field nodes and buffers by walking `selectedIndices` in order, and
   * the schema is built from the same walk, so the two cannot drift apart.
   */
  final class Projection(arrowFields: IndexedSeq[Field], selectedIndices: Array[Int]) {

    private val schema = new Schema(selectedIndices.map(arrowFields).toSeq.asJava)

    // A record batch body is a flat, depth-first sequence of buffers in schema order, so each
    // top-level column owns a contiguous run of it; field nodes run in the same order. The totals
    // are what a payload is checked against in load.
    private val (nodeIndices, totalNodes) =
      selectedRange(arrowFields, selectedIndices, fieldNodeCount)
    private val (bufferIndices, totalBuffers) =
      selectedRange(arrowFields, selectedIndices, fieldBufferCount)

    /**
     * Decode the projected columns of one cached payload into a fresh root the caller owns.
     *
     * A buffer's recorded (offset, length) covers its on-body bytes including the
     * uncompressed-length prefix, so a window copied out of the payload is exactly what the
     * writer emitted, 8-byte aligned as Arrow's IPC body lays it out. The columns that were not
     * selected are never read, let alone inflated. The windows are then decompressed in one pass;
     * see [[decompressed]] for why that is not left to `VectorLoader`.
     */
    def load(data: Array[Byte], allocator: BufferAllocator): VectorSchemaRoot = {
      val readChannel = new ReadChannel(Channels.newChannel(new ByteArrayInputStream(data)))
      // Reads the message metadata only. The body stays in `data` and is copied selectively.
      val metadata = MessageSerializer.readMessage(readChannel)
      if (metadata == null) {
        throw new SparkException("Unexpected end of input reading a Comet cached batch")
      }
      val batch =
        metadata.getMessage.header(new FlatBufRecordBatch()).asInstanceOf[FlatBufRecordBatch]

      // The payload carries no schema, so nothing in it says the writer laid the body out the way
      // these windows read it. batch.buffers(j) is an unchecked flatbuffer accessor, so a
      // disagreement would otherwise surface as wrong values, or as an out-of-range read from
      // inside the copy below, rather than as an error naming the cause. See matchesReaderLayout
      // for how the write path avoids producing one.
      if (batch.nodesLength() != totalNodes || batch.buffersLength() != totalBuffers) {
        throw new SparkException(
          "Comet cached batch does not match the cached schema: the payload holds " +
            s"${batch.nodesLength()} field nodes and ${batch.buffersLength()} buffers, but the " +
            s"schema describes $totalNodes and $totalBuffers")
      }

      // serialize writes exactly [encapsulated message][body] and nothing after it, so the body is
      // the tail of `data`.
      val bodyStart = data.length - metadata.getMessageBodyLength.toInt

      val compression =
        if (batch.compression() == null) NoCompressionCodec.DEFAULT_BODY_COMPRESSION
        else new ArrowBodyCompression(batch.compression().codec(), batch.compression().method())

      val nodes = new java.util.ArrayList[ArrowFieldNode](nodeIndices.length)
      nodeIndices.foreach { j =>
        val node = batch.nodes(j)
        nodes.add(new ArrowFieldNode(node.length(), node.nullCount()))
      }

      val offsets = new Array[Long](bufferIndices.length)
      val lengths = new Array[Long](bufferIndices.length)
      var total = 0L
      var k = 0
      while (k < bufferIndices.length) {
        val buffer = batch.buffers(bufferIndices(k))
        offsets(k) = buffer.offset()
        lengths(k) = buffer.length()
        total += DataSizeRoundingUtil.roundUpTo8Multiple(lengths(k))
        k += 1
      }

      // allocator.buffer(0) is legal but yields a buffer no window can be sliced from, and an
      // all-empty projection (every selected column a NullVector, say) would ask for exactly that.
      val body = allocator.buffer(math.max(total, 1L))
      val compressedBatch =
        try {
          val buffers = new java.util.ArrayList[ArrowBuf](bufferIndices.length)
          var position = 0L
          var i = 0
          while (i < bufferIndices.length) {
            val length = lengths(i)
            if (length > 0) {
              body.setBytes(position, data, bodyStart + offsets(i).toInt, length.toInt)
            }
            val window = body.slice(position, length)
            window.writerIndex(length)
            buffers.add(window)
            position += DataSizeRoundingUtil.roundUpTo8Multiple(length)
            i += 1
          }
          new ArrowRecordBatch(batch.length().toInt, nodes, buffers, compression, false)
        } catch {
          case NonFatal(e) =>
            body.close()
            throw e
        }

      // The constructor retained each window; slice() alone does not. Dropping `body`'s own
      // reference leaves the batch as sole owner of the one allocation, so closing the batch is
      // what frees it -- and closing `body` again would drive its reference count negative.
      body.close()
      val plainBatch =
        try decompressed(compressedBatch, allocator)
        finally compressedBatch.close()

      // The loader needs no compression factory: every buffer is decompressed by this point.
      val root = VectorSchemaRoot.create(schema, allocator)
      try {
        new VectorLoader(root).load(plainBatch)
        root
      } catch {
        case NonFatal(e) =>
          try root.close()
          catch { case NonFatal(closeError) => e.addSuppressed(closeError) }
          throw e
      } finally {
        plainBatch.close()
      }
    }
  }

  /**
   * The indices, within a record batch's flat depth-first sequence, that the selected columns
   * own, paired with the length of the whole sequence.
   *
   * `count` gives how many entries of the sequence a field occupies including its descendants, so
   * a running total over every field turns a column index into its run within the sequence. The
   * final total is what [[Projection.load]] checks a payload against.
   */
  private def selectedRange(
      arrowFields: IndexedSeq[Field],
      selectedIndices: Array[Int],
      count: Field => Int): (Array[Int], Int) = {
    val starts = arrowFields.scanLeft(0)(_ + count(_)).toArray
    (selectedIndices.flatMap(i => starts(i) until starts(i + 1)), starts.last)
  }

  /**
   * The same record batch with every buffer compressed, as a new batch the caller owns.
   *
   * `VectorUnloader` would do this itself if handed the codec, but it leaks on the failure path:
   * `appendNodes` retains each input buffer and accumulates the compressed ones into a list local
   * to `getRecordBatch`, so a buffer that fails to compress -- zstd unable to allocate its
   * workspace, say -- strands that retain and leaves every buffer compressed before it reachable
   * from nothing. Closing the input batch afterwards undoes neither, so one failed cache
   * materialization leaks a batch's worth of off-heap for the life of the executor. Compressing
   * here keeps every allocation reachable from this method's own error path, as [[decompressed]]
   * does on the read side.
   *
   * The retain before each `compress` is where the reference on the buffer that comes back is
   * from. A codec that allocates consumes it and hands back a buffer of its own;
   * `NoCompressionCodec` hands back the input itself, and the retain is then the reference
   * `result` ends up owning. Releasing it again is what a throw owes.
   */
  private def compressed(
      batch: ArrowRecordBatch,
      codec: CompressionCodec,
      allocator: BufferAllocator): ArrowRecordBatch = {
    val buffers = new java.util.ArrayList[ArrowBuf](batch.getBuffers.size)
    try {
      batch.getBuffers.asScala.foreach { buffer =>
        buffer.getReferenceManager.retain()
        val packed =
          try codec.compress(allocator, buffer)
          catch {
            case NonFatal(e) =>
              buffer.getReferenceManager.release()
              throw e
          }
        buffers.add(packed)
      }

      val result = new ArrowRecordBatch(
        batch.getLength,
        batch.getNodes,
        buffers,
        CompressionUtil.createBodyCompression(codec),
        batch.getVariadicBufferCounts,
        // alignBuffers=true matches the 8-byte buffer alignment Projection.load reproduces when it
        // repacks the selected buffers. This is the layout that gets written, so the unloader's is
        // not the one that matters.
        true)
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
    // getBodyCompression().getCodec() is the raw IPC byte, which readCodec turns into a codec or
    // rejects.
    val codec = readCodec(batch.getBodyCompression.getCodec)

    val buffers = new java.util.ArrayList[ArrowBuf]()
    try {
      batch.getBuffers.asScala.foreach { buffer =>
        buffer.getReferenceManager.retain()
        val plain =
          try {
            // An empty buffer carries no compressed length prefix to read.
            codec match {
              case Some(c) if buffer.writerIndex() > 0 => c.decompress(allocator, buffer)
              case _ => buffer
            }
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
   * The on-body compressed size of each top-level column.
   *
   * Each column owns the run of buffers its subtree occupies, so its stored size is the sum of
   * those buffers' recorded lengths. With one payload per batch these are the only per-column
   * sizes available -- there is no separate stream to measure -- and they are exact.
   */
  private def columnSizes(fields: Seq[Field], recordBatch: ArrowRecordBatch): Array[Long] = {
    val buffers = recordBatch.getBuffersLayout
    val starts = fields.scanLeft(0)(_ + fieldBufferCount(_)).toArray
    val sizes = new Array[Long](fields.length)
    var i = 0
    while (i < sizes.length) {
      var size = 0L
      var j = starts(i)
      while (j < starts(i + 1)) {
        size += buffers.get(j).getSize
        j += 1
      }
      sizes(i) = size
      i += 1
    }
    sizes
  }

  /**
   * Replace every dictionary-encoded column of `batch` with its decoded form.
   *
   * Returns the vectors to write and, separately, the ones allocated here so the caller can close
   * exactly those. Columns that needed no decoding are returned as they are and stay owned by
   * `batch`.
   */
  private def decodeDictionaries(
      batch: ColumnarBatch,
      allocator: BufferAllocator): (Seq[FieldVector], Seq[ValueVector]) = {
    val decoded = mutable.ArrayBuffer.empty[ValueVector]
    try {
      val vectors =
        Utils.getBatchFieldVectorsWithProviders(batch).map { case (vector, providerOpt) =>
          if (vector.getField.getDictionary == null) {
            vector
          } else {
            val dictionary = Utils.lookupDictionary(vector, providerOpt)
            val plain = DictionaryEncoder.decode(vector, dictionary, allocator)
            decoded += plain
            plain.asInstanceOf[FieldVector]
          }
        }
      (vectors, decoded.toSeq)
    } catch {
      case NonFatal(e) =>
        decoded.foreach(v =>
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
}
