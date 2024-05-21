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

import java.io.{DataOutputStream, File}
import java.nio.ByteBuffer
import java.nio.channels.Channels

import scala.collection.JavaConverters._

import org.apache.arrow.c.{ArrowArray, ArrowSchema, CDataDictionaryProvider, CometSchemaImporter, Data}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{BigIntVector, BitVector, DateDayVector, DecimalVector, FieldVector, FixedSizeBinaryVector, Float4Vector, Float8Vector, IntVector, SmallIntVector, TimeStampMicroTZVector, TimeStampMicroVector, TinyIntVector, ValueVector, VarBinaryVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.MapVector
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.types._
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

import org.apache.comet.vector._

object Utils {
  def getConfPath(confFileName: String): String = {
    sys.env
      .get("COMET_CONF_DIR")
      .map { t => new File(s"$t${File.separator}$confFileName") }
      .filter(_.isFile)
      .map(_.getAbsolutePath)
      .orNull
  }

  def stringToSeq(str: String): Seq[String] = {
    str.split(",").map(_.trim()).filter(_.nonEmpty)
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
    case ArrowType.Binary.INSTANCE => BinaryType
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
      case StringType => ArrowType.Utf8.INSTANCE
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
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported data type: ${dt.catalogString}")
    }

  /** Maps field from Spark to Arrow. NOTE: timeZoneId required for TimestampType */
  def toArrowField(name: String, dt: DataType, nullable: Boolean, timeZoneId: String): Field = {
    dt match {
      case ArrayType(elementType, containsNull) =>
        val fieldType = new FieldType(nullable, ArrowType.List.INSTANCE, null)
        new Field(
          name,
          fieldType,
          Seq(toArrowField("element", elementType, containsNull, timeZoneId)).asJava)
      case StructType(fields) =>
        val fieldType = new FieldType(nullable, ArrowType.Struct.INSTANCE, null)
        new Field(
          name,
          fieldType,
          fields
            .map { field =>
              toArrowField(field.name, field.dataType, field.nullable, timeZoneId)
            }
            .toSeq
            .asJava)
      case MapType(keyType, valueType, valueContainsNull) =>
        val mapType = new FieldType(nullable, new ArrowType.Map(false), null)
        // Note: Map Type struct can not be null, Struct Type key field can not be null
        new Field(
          name,
          mapType,
          Seq(
            toArrowField(
              MapVector.DATA_VECTOR_NAME,
              new StructType()
                .add(MapVector.KEY_NAME, keyType, nullable = false)
                .add(MapVector.VALUE_NAME, valueType, nullable = valueContainsNull),
              nullable = false,
              timeZoneId)).asJava)
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
      val provider = batchProviderOpt.getOrElse(dictionaryProvider)

      val writer = new ArrowStreamWriter(root, provider, Channels.newChannel(out))
      writer.start()
      writer.writeBatch()

      root.clear()
      writer.end()

      out.flush()
      out.close()

      if (out.size() > 0) {
        (batch.numRows(), cbbos.toChunkedByteBuffer)
      } else {
        (batch.numRows(), new ChunkedByteBuffer(Array.empty[ByteBuffer]))
      }
    }
  }

  def getBatchFieldVectors(
      batch: ColumnarBatch): (Seq[FieldVector], Option[DictionaryProvider]) = {
    var provider: Option[DictionaryProvider] = None
    val fieldVectors = (0 until batch.numCols()).map { index =>
      batch.column(index) match {
        case a: CometVector =>
          val valueVector = a.getValueVector
          if (valueVector.getField.getDictionary != null) {
            if (provider.isEmpty) {
              provider = Some(a.getDictionaryProvider)
            }
          }

          getFieldVector(valueVector, "serialize")

        case c =>
          throw new SparkException(
            "Comet execution only takes Arrow Arrays, but got " +
              s"${c.getClass}")
      }
    }
    (fieldVectors, provider)
  }

  def getFieldVector(valueVector: ValueVector, reason: String): FieldVector = {
    valueVector match {
      case v @ (_: BitVector | _: TinyIntVector | _: SmallIntVector | _: IntVector |
          _: BigIntVector | _: Float4Vector | _: Float8Vector | _: VarCharVector |
          _: DecimalVector | _: DateDayVector | _: TimeStampMicroTZVector | _: VarBinaryVector |
          _: FixedSizeBinaryVector | _: TimeStampMicroVector) =>
        v.asInstanceOf[FieldVector]
      case _ =>
        throw new SparkException(s"Unsupported Arrow Vector for $reason: ${valueVector.getClass}")
    }
  }
}
