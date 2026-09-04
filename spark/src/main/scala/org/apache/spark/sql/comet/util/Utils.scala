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

package org.apache.spark.sql.comet.util

import java.io.{DataInputStream, DataOutputStream, File}
import java.nio.ByteBuffer
import java.nio.channels.{Channels, WritableByteChannel}

import scala.jdk.CollectionConverters._

import org.apache.arrow.c.CDataDictionaryProvider
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.dictionary.{Dictionary, DictionaryProvider}
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.arrow.vector.types._
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.util.VectorSchemaRootAppender
import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.comet.execution.arrow.{ArrowReaderIterator, ConstantColumnVectors}
import org.apache.spark.sql.execution.vectorized.ConstantColumnVector
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

import org.apache.comet.Constants.COMET_CONF_DIR_ENV
import org.apache.comet.shims.CometTypeShim
import org.apache.comet.vector.CometVector

object Utils extends CometTypeShim with Logging {
  def getConfPath(confFileName: String): String = {
    sys.env
      .get(COMET_CONF_DIR_ENV)
      .map { t => new File(s"$t${File.separator}$confFileName") }
      .filter(_.isFile)
      .map(_.getAbsolutePath)
      .orNull
  }

  def stringToSeq(str: String): Seq[String] = {
    str.split(",").iterator.map(_.trim()).filter(_.nonEmpty).toList
  }

  /** bridges the function call to Spark's Util */
  def getSimpleName(cls: Class[_]): String = {
    org.apache.spark.util.Utils.getSimpleName(cls)
  }

  def fromArrowField(field: Field): DataType = {
    field.getType match {
      case _: ArrowType.Map =>
        val elementField = field.getChildren.get(0)
        val keyType = fromArrowField(elementField.getChildren.get(0))
        val valueType = fromArrowField(elementField.getChildren.get(1))
        MapType(keyType, valueType, elementField.getChildren.get(1).isNullable)
      case ArrowType.List.INSTANCE =>
        val elementField = field.getChildren().get(0)
        val elementType = fromArrowField(elementField)
        ArrayType(elementType, containsNull = elementField.isNullable)
      case ArrowType.Struct.INSTANCE =>
        val fields = field.getChildren().asScala.map { child =>
          val dt = fromArrowField(child)
          StructField(child.getName, dt, child.isNullable)
        }
        StructType(fields.toSeq)
      case arrowType => fromArrowType(arrowType)
    }
  }

  def fromArrowType(dt: ArrowType): DataType = dt match {
    case ArrowType.Bool.INSTANCE => BooleanType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 => ByteType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 2 => ShortType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 4 => IntegerType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 8 => LongType
    case float: ArrowType.FloatingPoint if float.getPrecision == FloatingPointPrecision.SINGLE =>
      FloatType
    case float: ArrowType.FloatingPoint if float.getPrecision == FloatingPointPrecision.DOUBLE =>
      DoubleType
    case ArrowType.Utf8.INSTANCE => StringType
    // Large (64-bit offset) variants: a PyArrow UDF's Python output may use large_string /
    // large_binary (e.g. pandas 3 backs string columns with Arrow large types), and mapInArrow
    // passes those types straight through to the JVM. CometPlainVector reads both offset widths.
    case ArrowType.LargeUtf8.INSTANCE => StringType
    case ArrowType.Binary.INSTANCE => BinaryType
    case ArrowType.LargeBinary.INSTANCE => BinaryType
    case _: ArrowType.FixedSizeBinary => BinaryType
    case d: ArrowType.Decimal => DecimalType(d.getPrecision, d.getScale)
    case date: ArrowType.Date if date.getUnit == DateUnit.DAY => DateType
    case ts: ArrowType.Timestamp
        if ts.getUnit == TimeUnit.MICROSECOND && ts.getTimezone == null =>
      TimestampNTZType
    case ts: ArrowType.Timestamp if ts.getUnit == TimeUnit.MICROSECOND => TimestampType
    case ArrowType.Null.INSTANCE => NullType
    case yi: ArrowType.Interval if yi.getUnit == IntervalUnit.YEAR_MONTH =>
      YearMonthIntervalType()
    case di: ArrowType.Interval if di.getUnit == IntervalUnit.DAY_TIME => DayTimeIntervalType()
    case ci: ArrowType.Interval if ci.getUnit == IntervalUnit.MONTH_DAY_NANO =>
      CalendarIntervalType
    case d: ArrowType.Duration if d.getUnit == TimeUnit.MICROSECOND => DayTimeIntervalType()
    case t: ArrowType.Time if t.getUnit == TimeUnit.NANOSECOND && t.getBitWidth == 64 =>
      // scalastyle:off classforname
      val clazz = Class.forName("org.apache.spark.sql.types.TimeType$")
      // scalastyle:on classforname
      val module = clazz.getField("MODULE$").get(null)
      clazz.getMethod("apply").invoke(module).asInstanceOf[DataType]
    case _ => throw new UnsupportedOperationException(s"Unsupported data type: ${dt.toString}")
  }

  def fromArrowSchema(schema: Schema): StructType = {
    StructType(schema.getFields.asScala.map { field =>
      val dt = fromArrowField(field)
      StructField(field.getName, dt, field.isNullable)
    }.toArray)
  }

  /** Maps data type from Spark to Arrow. NOTE: timeZoneId required for TimestampTypes */
  def toArrowType(dt: DataType, timeZoneId: String): ArrowType =
    dt match {
      case BooleanType => ArrowType.Bool.INSTANCE
      case ByteType => new ArrowType.Int(8, true)
      case ShortType => new ArrowType.Int(8 * 2, true)
      case IntegerType => new ArrowType.Int(8 * 4, true)
      case LongType => new ArrowType.Int(8 * 8, true)
      case FloatType => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
      case DoubleType => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
      case _: StringType => ArrowType.Utf8.INSTANCE
      case dt if isStringCollationType(dt) => ArrowType.Utf8.INSTANCE
      case BinaryType => ArrowType.Binary.INSTANCE
      case DecimalType.Fixed(precision, scale) => new ArrowType.Decimal(precision, scale, 128)
      case DateType => new ArrowType.Date(DateUnit.DAY)
      case TimestampType =>
        if (timeZoneId == null) {
          throw new UnsupportedOperationException(
            s"${TimestampType.catalogString} must supply timeZoneId parameter")
        } else {
          new ArrowType.Timestamp(TimeUnit.MICROSECOND, timeZoneId)
        }
      case TimestampNTZType =>
        new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)
      case NullType => ArrowType.Null.INSTANCE
      case dt if isTimeType(dt) =>
        new ArrowType.Time(TimeUnit.NANOSECOND, 64)
      case _: YearMonthIntervalType => new ArrowType.Interval(IntervalUnit.YEAR_MONTH)
      // Spark stores DayTimeIntervalType as microseconds in an int64, matching Arrow
      // Duration(Microsecond) rather than the lossy Interval(DayTime) {days, millis} layout.
      case _: DayTimeIntervalType => new ArrowType.Duration(TimeUnit.MICROSECOND)
      case CalendarIntervalType => new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO)
      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported data type: [${dt.getClass.getName}] ${dt.catalogString}")
    }

  /**
   * Nullability to declare for a nested child (array element, struct field, map value) of type
   * `dataType`. A `NullType` child is always declared nullable, whatever Spark's flag says: every
   * value is null, so a non-nullable flag is a contradiction, and native kernels that rebuild a
   * list around the input's actual child compare that child's nullability with the one they
   * assume and fail on the mismatch. Applied here and in `QueryPlanSerde.serializeDataType` so
   * the JVM-exported field and the type declared to native agree. Map keys are not children in
   * this sense: Arrow requires them non-nullable.
   */
  def declaredChildNullability(dataType: DataType, nullable: Boolean): Boolean =
    nullable || dataType == NullType

  /** Maps field from Spark to Arrow. NOTE: timeZoneId required for TimestampType */
  def toArrowField(name: String, dt: DataType, nullable: Boolean, timeZoneId: String): Field = {
    dt match {
      case ArrayType(elementType, containsNull) =>
        val fieldType = new FieldType(nullable, ArrowType.List.INSTANCE, null)
        new Field(
          name,
          fieldType,
          Seq(
            toArrowField(
              "element",
              elementType,
              declaredChildNullability(elementType, containsNull),
              timeZoneId)).asJava)
      case StructType(fields) =>
        val fieldType = new FieldType(nullable, ArrowType.Struct.INSTANCE, null)
        new Field(
          name,
          fieldType,
          fields
            .map { field =>
              toArrowField(
                field.name,
                field.dataType,
                declaredChildNullability(field.dataType, field.nullable),
                timeZoneId)
            }
            .toSeq
            .asJava)
      case MapType(keyType, valueType, valueContainsNull) =>
        val mapType = new FieldType(nullable, new ArrowType.Map(false), null)
        // Note: Map Type struct can not be null, Struct Type key field can not be null (so the
        // key is built here rather than through the StructType case and
        // `declaredChildNullability`)
        val entries = new Field(
          MapVector.DATA_VECTOR_NAME,
          new FieldType(false, ArrowType.Struct.INSTANCE, null),
          Seq(
            toArrowField(MapVector.KEY_NAME, keyType, nullable = false, timeZoneId),
            toArrowField(
              MapVector.VALUE_NAME,
              valueType,
              declaredChildNullability(valueType, valueContainsNull),
              timeZoneId)).asJava)
        new Field(name, mapType, Seq(entries).asJava)
      case dataType =>
        val fieldType = new FieldType(nullable, toArrowType(dataType, timeZoneId), null)
        new Field(name, fieldType, Seq.empty[Field].asJava)
    }
  }

  /**
   * Maps schema from Spark to Arrow. NOTE: timeZoneId required for TimestampType in StructType
   */
  def toArrowSchema(schema: StructType, timeZoneId: String): Schema = {
    new Schema(schema.map { field =>
      toArrowField(field.name, field.dataType, field.nullable, timeZoneId)
    }.asJava)
  }

  /**
   * Returns `field` with every map key field, at any nesting depth, marked non-nullable.
   *
   * Arrow requires map keys to be non-nullable and `MapVector.initializeChildrenFromFields`
   * enforces it, so an IPC reader rejects a schema whose key field is nullable. Comet always
   * declares keys non-nullable (`toArrowField`), but Arrow Java's `MinorType.NULL` factory builds
   * a `NullVector` from the field name alone, so a `NullType` key (e.g. `map()`) reports a
   * nullable field once a vector for it has been created by `Field.createVector`, i.e. after a
   * native import, codegen dispatch output, or row-to-Arrow conversion. Apply this to the fields
   * written to IPC.
   */
  def withNonNullableMapKeys(field: Field): Field = {
    val children = field.getChildren.asScala.toSeq
    if (children.isEmpty) {
      return field
    }
    val newChildren = field.getType match {
      case _: ArrowType.Map =>
        children.map { entries =>
          entries.getChildren.asScala.toSeq match {
            case Seq(key, value) =>
              val nonNullKey = withNonNullableMapKeys(key)
              val repairedKey = if (nonNullKey.isNullable) {
                new Field(
                  nonNullKey.getName,
                  new FieldType(
                    false,
                    nonNullKey.getType,
                    nonNullKey.getDictionary,
                    nonNullKey.getMetadata),
                  nonNullKey.getChildren)
              } else {
                nonNullKey
              }
              new Field(
                entries.getName,
                entries.getFieldType,
                Seq(repairedKey, withNonNullableMapKeys(value)).asJava)
            case _ => withNonNullableMapKeys(entries)
          }
        }
      case _ => children.map(withNonNullableMapKeys)
    }
    if (newChildren == children) {
      field
    } else {
      new Field(field.getName, field.getFieldType, newChildren.asJava)
    }
  }

  /**
   * Returns `root` unchanged when its declared schema already satisfies Arrow's non-nullable
   * map-key invariant, otherwise a new root that shares the same vectors but advertises repaired
   * fields.
   *
   * The check is against `root.getSchema`, not the live vectors' fields: a caller that built the
   * root with already-repaired fields keeps its own root object. That matters when the row count
   * is set after the writer is created (see `CometArrowPythonRunnerBase.startWriter`), because a
   * replacement root copies the row count at construction and does not track the original.
   */
  private def withNonNullableMapKeys(root: VectorSchemaRoot): VectorSchemaRoot = {
    val declared = root.getSchema.getFields.asScala.toSeq
    val repaired = declared.map(withNonNullableMapKeys)
    if (repaired == declared) {
      root
    } else {
      new VectorSchemaRoot(repaired.asJava, root.getFieldVectors, root.getRowCount)
    }
  }

  /**
   * The only supported way to build an `ArrowStreamWriter` in Comet; enforced by the scalastyle
   * `arrowstreamwriter` rule. Repairs the declared schema with [[withNonNullableMapKeys]], so a
   * new IPC writer cannot reintroduce the nullable `NullType` map key by forgetting to ask.
   *
   * Returns the writer together with the root it is bound to, which is `root` itself unless the
   * declared schema needed repairing. Callers must use the returned root: a writer serializes the
   * root it was constructed with, so a row count set on a superseded root is not seen (it
   * surfaces in the Python worker as "Array length did not match record batch length").
   */
  def newArrowStreamWriter(
      root: VectorSchemaRoot,
      provider: DictionaryProvider,
      channel: WritableByteChannel): (VectorSchemaRoot, ArrowStreamWriter) = {
    val bound = withNonNullableMapKeys(root)
    // scalastyle:off arrowstreamwriter
    (bound, new ArrowStreamWriter(bound, provider, channel))
    // scalastyle:on arrowstreamwriter
  }

  /**
   * Whether an Arrow `Null` field is a direct child of a struct (map entries included) anywhere
   * in `schema`. Arrow's `VectorAppender` cannot grow such a column: a struct's capacity is the
   * minimum over its *direct* children, a `NullVector`'s capacity equals its value count and its
   * `reAlloc()` is a no-op, so the struct's capacity loop never terminates.
   *
   * A list breaks that chain from both sides, because `ListVector` overrides
   * `BaseRepeatedValueVector.getValueCapacity` with one that only looks at its own offset and
   * validity buffers. So a `Null` under a list appends fine (`array(NULL)`), and so does a list
   * under a struct even when the list holds nulls (`map(k, array(NULL))`) - the struct only sees
   * the list's own, growable capacity. Top-level `NullVector`s are fine too.
   */
  private def hasNullDirectlyUnderStruct(schema: Schema): Boolean = {
    def check(field: Field): Boolean = {
      val isStruct = field.getType.isInstanceOf[ArrowType.Struct]
      field.getChildren.asScala.exists { child =>
        (isStruct && child.getType.isInstanceOf[ArrowType.Null]) || check(child)
      }
    }
    schema.getFields.asScala.exists(check)
  }

  /**
   * Build a `StructType` from a sequence of Spark `Attribute`s. Avoids
   * `StructType.fromAttributes` (removed in Spark 4) and `DataTypeUtils.fromAttributes` (only on
   * 4) so the same call works across supported Spark versions.
   */
  def fromAttributes(attributes: Seq[Attribute]): StructType =
    StructType(attributes.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))

  /**
   * Serializes a list of `ColumnarBatch` into an output stream. This method must be in `spark`
   * package because `ChunkedByteBufferOutputStream` is spark private class. As it uses Arrow
   * classes, it must be in `common` module.
   *
   * @param batches
   *   the output batches, each batch is a list of Arrow vectors wrapped in `CometVector`
   * @param out
   *   the output stream
   */
  def serializeBatches(batches: Iterator[ColumnarBatch]): Iterator[(Long, ChunkedByteBuffer)] = {
    batches.map { batch =>
      val dictionaryProvider: CDataDictionaryProvider = new CDataDictionaryProvider

      val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
      val cbbos = new ChunkedByteBufferOutputStream(1024 * 1024, ByteBuffer.allocate)
      val out = new DataOutputStream(codec.compressedOutputStream(cbbos))

      val (fieldVectors, batchProviderOpt) = getBatchFieldVectors(batch)
      val root = new VectorSchemaRoot(fieldVectors.asJava)
      if (fieldVectors.isEmpty) {
        // VSR cannot infer rowCount without field vectors
        root.setRowCount(batch.numRows())
      }
      val provider = batchProviderOpt.getOrElse(dictionaryProvider)

      val (writeRoot, writer) = newArrowStreamWriter(root, provider, Channels.newChannel(out))
      writer.start()
      writer.writeBatch()
      writeRoot.clear()
      writer.close()

      if (out.size() > 0) {
        (batch.numRows().toLong, cbbos.toChunkedByteBuffer)
      } else {
        (batch.numRows().toLong, new ChunkedByteBuffer(Array.empty[ByteBuffer]))
      }
    }
  }

  /**
   * Serializes each column of `batch` into its own compressed Arrow IPC stream, in column order.
   *
   * [[serializeBatches]] writes one stream covering every column, so a reader has to inflate all
   * of them before it can project. Comet's in-memory cache stores columns separately instead, so
   * a scan decodes only the ones it selected. Each stream is self-contained, including its schema
   * and any dictionaries the column needs.
   *
   * The row count is not recoverable from the result when `batch` has no columns, so callers keep
   * it alongside. As with [[serializeBatches]], the batch's vectors are cleared once written.
   */
  def serializeBatchColumns(batch: ColumnarBatch): Array[ChunkedByteBuffer] = {
    val codec = CompressionCodec.createCodec(SparkEnv.get.conf)

    // Each column is written with the provider it was decoded with, not the batch's first one:
    // columns decoded from separate streams have independent dictionary ID namespaces.
    getBatchFieldVectorsWithProviders(batch).map { case (fieldVector, providerOpt) =>
      val provider = providerOpt.getOrElse(new CDataDictionaryProvider)
      val cbbos = new ChunkedByteBufferOutputStream(1024 * 1024, ByteBuffer.allocate)
      val out = new DataOutputStream(codec.compressedOutputStream(cbbos))

      val root = new VectorSchemaRoot(Seq(fieldVector).asJava)
      val (writeRoot, writer) = newArrowStreamWriter(root, provider, Channels.newChannel(out))
      writer.start()
      writer.writeBatch()
      writeRoot.clear()
      writer.close()

      cbbos.toChunkedByteBuffer
    }.toArray
  }

  /**
   * The classes that carry the output of [[serializeBatches]] and [[serializeBatchColumns]] out
   * of Comet, for Kryo registration by [[org.apache.comet.CometKryoRegistrator]].
   *
   * Spark registers `ChunkedByteBuffer` itself but not an array of them, and
   * `CometBroadcastExchangeExec` broadcasts exactly that array, so a native broadcast fails under
   * `spark.kryo.registrationRequired=true` whichever Comet features are enabled. Comet's cache
   * format stores one buffer per column and so needs the same registrations.
   */
  def arrowBytesKryoClasses: Seq[Class[_]] = Seq(
    classOf[ChunkedByteBuffer],
    classOf[Array[ChunkedByteBuffer]],
    // A ChunkedByteBuffer's own chunks. ChunkedByteBufferOutputStream allocates them on heap.
    classOf[Array[ByteBuffer]],
    ByteBuffer.allocate(1).getClass)

  /**
   * Decodes the byte arrays back to ColumnarBatchs and put them into buffer.
   *
   * @param bytes
   *   the serialized batches
   * @param source
   *   the class that calls this method
   * @return
   *   an iterator of ColumnarBatch
   */
  def decodeBatches(bytes: ChunkedByteBuffer, source: String): Iterator[ColumnarBatch] = {
    if (bytes.size == 0) {
      return Iterator.empty
    }

    // use Spark's compression codec (LZ4 by default) and not Comet's compression
    val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
    val cbbis = bytes.toInputStream()
    val ins = new DataInputStream(codec.compressedInputStream(cbbis))
    // batches are in Arrow IPC format
    new ArrowReaderIterator(Channels.newChannel(ins), source)
  }

  /**
   * Coalesces many small Arrow IPC batches into a single batch for broadcasting.
   *
   * Why this is necessary: The broadcast exchange collects shuffle output by calling
   * getByteArrayRdd, which serializes each ColumnarBatch independently into its own
   * ChunkedByteBuffer. The shuffle reader (CometBlockStoreShuffleReader) produces one
   * ColumnarBatch per shuffle block, and there is one block per writer task per output partition.
   * So with W writer tasks and P output partitions, the broadcast collects up to W * P tiny
   * batches. For example, with 400 writer tasks and 500 partitions, 1M rows would arrive as ~200K
   * batches of ~5 rows each.
   *
   * Without coalescing, every consumer task in the broadcast join would independently deserialize
   * all of these tiny Arrow IPC streams, paying per-stream overhead (schema parsing, buffer
   * allocation) for each one. With coalescing, we decode and append all batches into one
   * VectorSchemaRoot on the driver, then re-serialize once. Each consumer task then deserializes
   * a single Arrow IPC stream.
   */
  def coalesceBroadcastBatches(
      input: Iterator[ChunkedByteBuffer]): (Array[ChunkedByteBuffer], Long, Long) = {
    val buffers = input.filterNot(_.size == 0).toArray
    if (buffers.isEmpty) {
      return (Array.empty, 0L, 0L)
    }

    val allocator = org.apache.comet.CometArrowAllocator
      .newChildAllocator("broadcast-coalesce", 0, Long.MaxValue)
    try {
      var targetRoot: VectorSchemaRoot = null
      var totalRows = 0L
      var batchCount = 0

      val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
      try {
        for (bytes <- buffers) {
          val compressedInputStream =
            new DataInputStream(codec.compressedInputStream(bytes.toInputStream()))
          val reader =
            new ArrowStreamReader(Channels.newChannel(compressedInputStream), allocator)
          try {
            // Schemas that cannot be appended fall back to the original uncoalesced buffers:
            // - Comet decodes dictionaries during execution, so a dictionary-encoded column
            //   shouldn't happen. If it does, each partition can have a different dictionary,
            //   and appending index vectors would silently mix incompatible dictionaries.
            // - `VectorSchemaRootAppender` cannot grow a `NullVector` directly under a struct
            //   (see `hasNullDirectlyUnderStruct`).
            val skipReason =
              if (!reader.getDictionaryVectors.isEmpty) {
                Some("unexpected dictionary-encoded column")
              } else if (hasNullDirectlyUnderStruct(reader.getVectorSchemaRoot.getSchema)) {
                Some("NullType directly under a struct or map entry")
              } else {
                None
              }
            if (skipReason.isDefined) {
              logWarning(
                s"${skipReason.get} during BroadcastExchange coalescing; skipping coalesce")
              return (buffers, 0L, 0L)
            }
            while (reader.loadNextBatch()) {
              val sourceRoot = reader.getVectorSchemaRoot
              if (targetRoot == null) {
                targetRoot = VectorSchemaRoot.create(sourceRoot.getSchema, allocator)
                targetRoot.allocateNew()
              }
              try {
                VectorSchemaRootAppender.append(targetRoot, sourceRoot)
              } catch {
                case e: IllegalArgumentException =>
                  logWarning(
                    "Arrow batches cannot be appended during BroadcastExchange coalescing; " +
                      "skipping coalesce",
                    e)
                  targetRoot.close()
                  targetRoot = null
                  return (buffers, 0L, 0L)
              }
              totalRows += sourceRoot.getRowCount
              batchCount += 1
            }
          } finally {
            reader.close()
          }
        }

        if (targetRoot == null) {
          return (Array.empty, 0L, 0L)
        }

        if (targetRoot.getSchema.getFields.isEmpty) {
          // VSRAppender does not update rowCount with no columns
          targetRoot.setRowCount(totalRows.toInt)
        }

        assert(
          targetRoot.getRowCount.toLong == totalRows,
          s"Row count mismatch after coalesce: ${targetRoot.getRowCount} != $totalRows")

        logInfo(s"Coalesced $batchCount broadcast batches into 1 ($totalRows rows)")

        val outputStream = new ChunkedByteBufferOutputStream(1024 * 1024, ByteBuffer.allocate)
        val compressedOutputStream =
          new DataOutputStream(codec.compressedOutputStream(outputStream))
        val (_, writer) =
          newArrowStreamWriter(targetRoot, null, Channels.newChannel(compressedOutputStream))
        try {
          writer.start()
          writer.writeBatch()
        } finally {
          writer.close()
        }

        (Array(outputStream.toChunkedByteBuffer), batchCount.toLong, totalRows)
      } finally {
        if (targetRoot != null) {
          targetRoot.close()
        }
      }
    } finally {
      allocator.close()
    }
  }

  /**
   * Whether every column in `batch` is an Arrow-backed `CometVector`, so [[getBatchFieldVectors]]
   * can hand out its vectors directly. Callers that may receive batches from a plan they did not
   * build (e.g. Comet's cache serializer, which Spark hands the cached plan's columnar output)
   * use this to convert foreign vectors to Arrow instead of tripping the exception below.
   *
   * Stricter than what [[getBatchFieldVectors]] accepts: a `ConstantColumnVector` is rejected
   * here even though that method materializes one, so such a batch takes the conversion path
   * rather than being materialized column by column.
   */
  def isArrowBacked(batch: ColumnarBatch): Boolean =
    (0 until batch.numCols()).forall { i =>
      batch.column(i) match {
        // Not every CometVector can be handed to getFieldVector: a CometPlainVector can wrap a
        // LargeVarCharVector or LargeVarBinaryVector (an accelerated mapInArrow returning
        // pa.large_string(), for instance), which it rejects. Answering true for those would
        // send a batch down the direct write path that then fails, so check the vector itself
        // and let the caller convert instead.
        case v: CometVector => isSupportedFieldVector(v.getValueVector)
        case _ => false
      }
    }

  def getBatchFieldVectors(
      batch: ColumnarBatch): (Seq[FieldVector], Option[DictionaryProvider]) = {
    val columns = getBatchFieldVectorsWithProviders(batch)
    (columns.map(_._1), combineDictionaryProviders(columns))
  }

  /**
   * The dictionaries every dictionary-encoded column of `columns` refers to, as one provider.
   *
   * Columns of a batch need not share a provider. Comet's cache decodes each column from its own
   * Arrow stream, so a dictionary-backed column arrives carrying the provider its reader built,
   * and a batch that reaches [[serializeBatches]] -- a native broadcast of a cache scan, say --
   * can hold several. Writing the whole batch emits one schema covering every column and resolves
   * each column's dictionary ID against the single provider the writer was given, so handing it
   * any one column's provider fails with "Could not find dictionary with ID n" for the others.
   */
  private def combineDictionaryProviders(
      columns: Seq[(FieldVector, Option[DictionaryProvider])]): Option[DictionaryProvider] = {
    val dictionaries = scala.collection.mutable.LinkedHashMap.empty[Long, Dictionary]

    columns.foreach { case (vector, providerOpt) =>
      val encoding = vector.getField.getDictionary
      if (encoding != null) {
        val id = encoding.getId
        val dictionary = providerOpt.map(_.lookup(id)).orNull
        if (dictionary == null) {
          throw new SparkException(
            s"Column ${vector.getField.getName} is dictionary encoded with ID $id, but no " +
              "dictionary with that ID was provided")
        }
        dictionaries.get(id) match {
          // Every provider seen here descends from one upstream reader, which numbers the
          // dictionaries it hands out, so two columns sharing an ID share the dictionary itself.
          // A genuine clash would need renumbering, which means rewriting each vector's field,
          // so refuse rather than silently decode one column against another's dictionary.
          case Some(existing) if existing.getVector ne dictionary.getVector =>
            throw new SparkException(
              s"Columns of the same batch carry different dictionaries under ID $id")
          case _ => dictionaries.put(id, dictionary)
        }
      }
    }

    if (dictionaries.isEmpty) None
    else Some(new MapDictionaryProvider(dictionaries.values.toSeq: _*))
  }

  /**
   * Field vectors of `batch` paired with the dictionary provider each column was decoded with.
   *
   * [[getBatchFieldVectors]] folds these into one provider covering the whole batch, which is
   * what a single stream over every column needs. Comet's cache decodes each column from its own
   * stream and writes it back the same way, so it keeps the pairing instead: each column is
   * written with the provider it was decoded with.
   */
  def getBatchFieldVectorsWithProviders(
      batch: ColumnarBatch): Seq[(FieldVector, Option[DictionaryProvider])] = {
    val rows = batch.numRows()
    (0 until batch.numCols()).map { index =>
      batch.column(index) match {
        case a: CometVector =>
          val valueVector = a.getValueVector
          val provider =
            if (valueVector.getField.getDictionary != null) Some(a.getDictionaryProvider)
            else None

          (getFieldVector(valueVector, "serialize"), provider)

        case cv: ConstantColumnVector =>
          // Spark wraps file-source partition columns and other per-batch constants in
          // `ConstantColumnVector`. Materialise to an Arrow vector so the serialisation path
          // doesn't reject the batch. "UTC" is intentional -- see `ConstantColumnVectors`.
          val materialized = ConstantColumnVectors.materialize(
            cv,
            cv.dataType(),
            rows,
            s"_const_$index",
            org.apache.comet.CometArrowAllocator,
            "UTC")
          (materialized, None)

        case c =>
          throw new SparkException(
            s"Comet execution only takes Arrow Arrays, but got ${c.getClass}. " +
              "This typically happens when a Comet scan falls back to Spark due to unsupported " +
              "data types (e.g., complex types like structs, arrays, or maps). " +
              "To resolve this, you can: " +
              "(1) enable spark.comet.scan.allowIncompatible=true to use a compatible native " +
              "scan variant, or " +
              "(2) enable spark.comet.convert.parquet.enabled=true to convert Spark Parquet " +
              "data to Arrow format automatically.")
      }
    }
  }

  /** Whether [[getFieldVector]] accepts this vector, without throwing to find out. */
  def isSupportedFieldVector(valueVector: ValueVector): Boolean = valueVector match {
    case _: BitVector | _: TinyIntVector | _: SmallIntVector | _: IntVector | _: BigIntVector |
        _: Float4Vector | _: Float8Vector | _: VarCharVector | _: DecimalVector |
        _: DateDayVector | _: TimeStampMicroTZVector | _: VarBinaryVector |
        _: FixedSizeBinaryVector | _: TimeStampMicroVector | _: StructVector | _: ListVector |
        _: MapVector | _: NullVector | _: TimeNanoVector =>
      true
    case _ => false
  }

  def getFieldVector(valueVector: ValueVector, reason: String): FieldVector = {
    if (isSupportedFieldVector(valueVector)) {
      valueVector.asInstanceOf[FieldVector]
    } else {
      throw new SparkException(s"Unsupported Arrow Vector for $reason: ${valueVector.getClass}")
    }
  }
}
