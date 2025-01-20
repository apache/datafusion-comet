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

package org.apache.comet

import java.math.{BigDecimal, RoundingMode}
import java.nio.charset.Charset
import java.sql.Timestamp

import scala.collection.mutable.ListBuffer
import scala.util.Random

import org.apache.spark.sql.{RandomDataGenerator, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, DecimalType, MapType, StringType, StructField, StructType}

object DataGenerator {
  // note that we use `def` rather than `val` intentionally here so that
  // each test suite starts with a fresh data generator to help ensure
  // that tests are deterministic
  def DEFAULT = new DataGenerator(new Random(42))
  // matches the probability of nulls in Spark's RandomDataGenerator
  private val PROBABILITY_OF_NULL: Float = 0.1f
}

class DataGenerator(r: Random) {
  import DataGenerator._

  /** Pick a random item from a sequence */
  def pickRandom[T](items: Seq[T]): T = {
    items(r.nextInt(items.length))
  }

  /** Generate a random string using the specified characters */
  def generateString(chars: String, maxLen: Int): String = {
    val len = r.nextInt(maxLen)
    Range(0, len).map(_ => chars.charAt(r.nextInt(chars.length))).mkString
  }

  /** Generate random strings */
  def generateStrings(n: Int, maxLen: Int): Seq[String] = {
    Range(0, n).map(_ => r.nextString(maxLen))
  }

  /** Generate random strings using the specified characters */
  def generateStrings(n: Int, chars: String, maxLen: Int): Seq[String] = {
    Range(0, n).map(_ => generateString(chars, maxLen))
  }

  def generateFloats(n: Int): Seq[Float] = {
    Seq(
      Float.MaxValue,
      Float.MinPositiveValue,
      Float.MinValue,
      Float.NaN,
      Float.PositiveInfinity,
      Float.NegativeInfinity,
      1.0f,
      -1.0f,
      Short.MinValue.toFloat,
      Short.MaxValue.toFloat,
      0.0f) ++
      Range(0, n).map(_ => r.nextFloat())
  }

  def generateDoubles(n: Int): Seq[Double] = {
    Seq(
      Double.MaxValue,
      Double.MinPositiveValue,
      Double.MinValue,
      Double.NaN,
      Double.PositiveInfinity,
      Double.NegativeInfinity,
      0.0d) ++
      Range(0, n).map(_ => r.nextDouble())
  }

  def generateBytes(n: Int): Seq[Byte] = {
    Seq(Byte.MinValue, Byte.MaxValue) ++
      Range(0, n).map(_ => r.nextInt().toByte)
  }

  def generateShorts(n: Int): Seq[Short] = {
    val r = new Random(0)
    Seq(Short.MinValue, Short.MaxValue) ++
      Range(0, n).map(_ => r.nextInt().toShort)
  }

  def generateInts(n: Int): Seq[Int] = {
    Seq(Int.MinValue, Int.MaxValue) ++
      Range(0, n).map(_ => r.nextInt())
  }

  def generateLongs(n: Int): Seq[Long] = {
    Seq(Long.MinValue, Long.MaxValue) ++
      Range(0, n).map(_ => r.nextLong())
  }

  // Generate a random row according to the schema, the string filed in the struct could be
  // configured to generate strings by passing a stringGen function. Other types are delegated
  // to Spark's RandomDataGenerator.
  def generateRow(schema: StructType, stringGen: Option[() => String] = None): Row = {
    val fields = schema.fields.map { f =>
      f.dataType match {
        case StructType(children) =>
          generateRow(StructType(children), stringGen)
        case StringType if stringGen.isDefined =>
          val gen = stringGen.get
          val data = if (f.nullable && r.nextFloat() <= PROBABILITY_OF_NULL) {
            null
          } else {
            gen()
          }
          data
        case _ =>
          val gen = RandomDataGenerator.forType(f.dataType, f.nullable, r) match {
            case Some(g) => g
            case None =>
              throw new IllegalStateException(s"No RandomDataGenerator for type ${f.dataType}")
          }
          gen()
      }
    }.toSeq
    Row.fromSeq(fields)
  }

  def generateRows(
      num: Int,
      schema: StructType,
      stringGen: Option[() => String] = None): Seq[Row] = {
    Range(0, num).map(_ => generateRow(schema, stringGen))
  }

  private val primitiveTypes = Seq(
    DataTypes.BooleanType,
    DataTypes.ByteType,
    DataTypes.ShortType,
    DataTypes.IntegerType,
    DataTypes.LongType,
    DataTypes.FloatType,
    DataTypes.DoubleType,
    DataTypes.createDecimalType(10, 2),
    DataTypes.createDecimalType(36, 18),
    DataTypes.DateType,
    DataTypes.TimestampType,
    // TimestampNTZType only in Spark 3.4+
    // DataTypes.TimestampNTZType,
    DataTypes.StringType,
    DataTypes.BinaryType)

  def makeParquetFile(
      r: Random,
      spark: SparkSession,
      filename: String,
      numRows: Int,
      options: DataGenOptions): Unit = {

    val dataTypes = ListBuffer[DataType]()
    dataTypes.appendAll(primitiveTypes)

    if (options.generateStruct) {
      dataTypes += StructType(
        primitiveTypes.zipWithIndex.map(x => StructField(s"c${x._2}", x._1, true)))
    }

    if (options.generateMap) {
      dataTypes += MapType(DataTypes.IntegerType, DataTypes.StringType)
    }

    if (options.generateArray) {
      dataTypes.appendAll(primitiveTypes.map(DataTypes.createArrayType))

      if (options.generateStruct) {
        dataTypes += DataTypes.createArrayType(
          StructType(primitiveTypes.zipWithIndex.map(x => StructField(s"c${x._2}", x._1, true))))
      }

      if (options.generateMap) {
        dataTypes += DataTypes.createArrayType(
          MapType(DataTypes.IntegerType, DataTypes.StringType))
      }
    }

    // generate schema using random data types
    val fields = dataTypes.zipWithIndex
      .map(i => StructField(s"c${i._2}", i._1, nullable = true))
    val schema = StructType(fields)

    // generate columnar data
    val cols: Seq[Seq[Any]] = fields.map(f => generateColumn(r, f.dataType, numRows, options))

    // convert to rows
    val rows = Range(0, numRows).map(rowIndex => {
      Row.fromSeq(cols.map(_(rowIndex)))
    })

    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    df.write.mode(SaveMode.Overwrite).parquet(filename)
  }

  def generateColumn(
      r: Random,
      dataType: DataType,
      numRows: Int,
      options: DataGenOptions): Seq[Any] = {
    dataType match {
      case ArrayType(elementType, _) =>
        val values = generateColumn(r, elementType, numRows, options)
        val list = ListBuffer[Any]()
        for (i <- 0 until numRows) {
          if (i % 10 == 0 && options.allowNull) {
            list += null
          } else {
            list += Range(0, r.nextInt(5)).map(j => values((i + j) % values.length)).toArray
          }
        }
        list.toSeq
      case StructType(fields) =>
        val values = fields.map(f => generateColumn(r, f.dataType, numRows, options))
        Range(0, numRows).map(i => Row(values.indices.map(j => values(j)(i)): _*))
      case MapType(keyType, valueType, _) =>
        val mapOptions = options.copy(allowNull = false)
        val k = generateColumn(r, keyType, numRows, mapOptions)
        val v = generateColumn(r, valueType, numRows, mapOptions)
        k.zip(v).map(x => Map(x._1 -> x._2))
      case DataTypes.BooleanType =>
        generateColumn(r, DataTypes.LongType, numRows, options)
          .map(_.asInstanceOf[Long].toShort)
          .map(s => s % 2 == 0)
      case DataTypes.ByteType =>
        generateColumn(r, DataTypes.LongType, numRows, options)
          .map(_.asInstanceOf[Long].toByte)
      case DataTypes.ShortType =>
        generateColumn(r, DataTypes.LongType, numRows, options)
          .map(_.asInstanceOf[Long].toShort)
      case DataTypes.IntegerType =>
        generateColumn(r, DataTypes.LongType, numRows, options)
          .map(_.asInstanceOf[Long].toInt)
      case DataTypes.LongType =>
        Range(0, numRows).map(_ => {
          r.nextInt(50) match {
            case 0 if options.allowNull => null
            case 1 => 0L
            case 2 => Byte.MinValue.toLong
            case 3 => Byte.MaxValue.toLong
            case 4 => Short.MinValue.toLong
            case 5 => Short.MaxValue.toLong
            case 6 => Int.MinValue.toLong
            case 7 => Int.MaxValue.toLong
            case 8 => Long.MinValue
            case 9 => Long.MaxValue
            case _ => r.nextLong()
          }
        })
      case DataTypes.FloatType =>
        Range(0, numRows).map(_ => {
          r.nextInt(20) match {
            case 0 if options.allowNull => null
            case 1 => Float.NegativeInfinity
            case 2 => Float.PositiveInfinity
            case 3 => Float.MinValue
            case 4 => Float.MaxValue
            case 5 => 0.0f
            case 6 if options.generateNegativeZero => -0.0f
            case _ => r.nextFloat()
          }
        })
      case DataTypes.DoubleType =>
        Range(0, numRows).map(_ => {
          r.nextInt(20) match {
            case 0 if options.allowNull => null
            case 1 => Double.NegativeInfinity
            case 2 => Double.PositiveInfinity
            case 3 => Double.MinValue
            case 4 => Double.MaxValue
            case 5 => 0.0
            case 6 if options.generateNegativeZero => -0.0
            case _ => r.nextDouble()
          }
        })
      case dt: DecimalType =>
        Range(0, numRows).map(_ =>
          new BigDecimal(r.nextDouble()).setScale(dt.scale, RoundingMode.HALF_UP))
      case DataTypes.StringType =>
        Range(0, numRows).map(_ => {
          r.nextInt(10) match {
            case 0 if options.allowNull => null
            case 1 => r.nextInt().toByte.toString
            case 2 => r.nextLong().toString
            case 3 => r.nextDouble().toString
            case _ => r.nextString(8)
          }
        })
      case DataTypes.BinaryType =>
        generateColumn(r, DataTypes.StringType, numRows, options)
          .map {
            case x: String =>
              x.getBytes(Charset.defaultCharset())
            case _ =>
              null
          }
      case DataTypes.DateType =>
        Range(0, numRows).map(_ => new java.sql.Date(1716645600011L + r.nextInt()))
      case DataTypes.TimestampType =>
        Range(0, numRows).map(_ => new Timestamp(1716645600011L + r.nextInt()))
      case _ => throw new IllegalStateException(s"Cannot generate data for $dataType yet")
    }
  }

}

case class DataGenOptions(
    allowNull: Boolean,
    generateNegativeZero: Boolean,
    generateArray: Boolean,
    generateStruct: Boolean,
    generateMap: Boolean)
