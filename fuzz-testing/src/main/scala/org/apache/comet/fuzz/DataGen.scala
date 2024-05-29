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

package org.apache.comet.fuzz

import java.nio.charset.Charset
import java.sql.Timestamp

import scala.util.Random

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

object DataGen {

  def generateRandomFiles(
      r: Random,
      spark: SparkSession,
      numFiles: Int,
      numRows: Int,
      numColumns: Int): Unit = {
    for (i <- 0 until numFiles) {
      generateRandomParquetFile(r, spark, s"test$i.parquet", numRows, numColumns)
    }
  }

  def generateRandomParquetFile(
      r: Random,
      spark: SparkSession,
      filename: String,
      numRows: Int,
      numColumns: Int): Unit = {

    // generate schema using random data types
    val fields = Range(0, numColumns)
      .map(i => StructField(s"c$i", Utils.randomWeightedChoice(Meta.dataTypes), nullable = true))
    val schema = StructType(fields)

    // generate columnar data
    val cols: Seq[Seq[Any]] = fields.map(f => generateColumn(r, f.dataType, numRows))

    // convert to rows
    val rows = Range(0, numRows).map(rowIndex => {
      Row.fromSeq(cols.map(_(rowIndex)))
    })

    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    df.write.mode(SaveMode.Overwrite).parquet(filename)
  }

  def generateColumn(r: Random, dataType: DataType, numRows: Int): Seq[Any] = {
    dataType match {
      case DataTypes.BooleanType =>
        generateColumn(r, DataTypes.LongType, numRows)
          .map(_.asInstanceOf[Long].toShort)
          .map(s => s % 2 == 0)
      case DataTypes.ByteType =>
        generateColumn(r, DataTypes.LongType, numRows).map(_.asInstanceOf[Long].toByte)
      case DataTypes.ShortType =>
        generateColumn(r, DataTypes.LongType, numRows).map(_.asInstanceOf[Long].toShort)
      case DataTypes.IntegerType =>
        generateColumn(r, DataTypes.LongType, numRows).map(_.asInstanceOf[Long].toInt)
      case DataTypes.LongType =>
        Range(0, numRows).map(_ => {
          r.nextInt(50) match {
            case 0 => null
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
            case 0 => null
            case 1 => Float.NegativeInfinity
            case 2 => Float.PositiveInfinity
            case 3 => Float.MinValue
            case 4 => Float.MaxValue
            case 5 => 0.0f
            case 6 => -0.0f
            case _ => r.nextFloat()
          }
        })
      case DataTypes.DoubleType =>
        Range(0, numRows).map(_ => {
          r.nextInt(20) match {
            case 0 => null
            case 1 => Double.NegativeInfinity
            case 2 => Double.PositiveInfinity
            case 3 => Double.MinValue
            case 4 => Double.MaxValue
            case 5 => 0.0
            case 6 => -0.0
            case _ => r.nextDouble()
          }
        })
      case DataTypes.StringType =>
        Range(0, numRows).map(_ => {
          r.nextInt(10) match {
            case 0 => null
            case 1 => r.nextInt().toByte.toString
            case 2 => r.nextLong().toString
            case 3 => r.nextDouble().toString
            case _ => r.nextString(8)
          }
        })
      case DataTypes.BinaryType =>
        generateColumn(r, DataTypes.StringType, numRows)
          .map(_.asInstanceOf[String].getBytes(Charset.defaultCharset()))
      case DataTypes.DateType =>
        Range(0, numRows).map(_ => new java.sql.Date(1716645600011L + r.nextInt()))
      case DataTypes.TimestampType | DataTypes.TimestampNTZType =>
        Range(0, numRows).map(_ => new Timestamp(1716645600011L + r.nextInt()))
      case _ => throw new IllegalStateException(s"Cannot generate data for $dataType yet")
    }
  }

}
