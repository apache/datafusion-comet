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

package org.apache.comet.testing

import scala.collection.mutable.ListBuffer
import scala.util.Random

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DataType, DataTypes, MapType, StructField, StructType}

object ParquetGenerator {

  def makeParquetSchema(options: ParquetDataGenOptions): StructType = {
    val primitiveTypes = options.primitiveTypes
    val dataTypes = ListBuffer[DataType]()
    dataTypes.appendAll(primitiveTypes)

    val arraysOfPrimitives = primitiveTypes.map(DataTypes.createArrayType)

    if (options.generateStruct) {
      dataTypes += StructType(
        primitiveTypes.zipWithIndex.map(x => StructField(s"c${x._2}", x._1, nullable = true)))

      if (options.generateArray) {
        dataTypes += StructType(arraysOfPrimitives.zipWithIndex.map(x =>
          StructField(s"c${x._2}", x._1, nullable = true)))
      }
    }

    if (options.generateMap) {
      dataTypes += MapType(DataTypes.IntegerType, DataTypes.StringType)
    }

    if (options.generateArray) {
      dataTypes.appendAll(arraysOfPrimitives)

      if (options.generateStruct) {
        dataTypes += DataTypes.createArrayType(StructType(primitiveTypes.zipWithIndex.map(x =>
          StructField(s"c${x._2}", x._1, nullable = true))))
      }

      if (options.generateMap) {
        dataTypes += DataTypes.createArrayType(
          MapType(DataTypes.IntegerType, DataTypes.StringType))
      }
    }

    // generate schema using random data types
    val fields = dataTypes.zipWithIndex
      .map(i => StructField(s"c${i._2}", i._1, nullable = true))
    StructType(fields.toSeq)
  }

  def makeParquetFile(
      r: Random,
      spark: SparkSession,
      filename: String,
      numRows: Int,
      options: ParquetDataGenOptions): Unit = {

    val schema = makeParquetSchema(options)

    val x = DataGenOptions2(
      allowNull = options.allowNull,
      generateNegativeZero = options.generateNegativeZero,
      baseDate = options.baseDate)

    val df = FuzzDataGenerator.generateDataFrame(r, spark, schema, numRows, x)

    df.write.mode(SaveMode.Overwrite).parquet(filename)
  }
}

case class ParquetDataGenOptions(
    allowNull: Boolean = true,
    generateNegativeZero: Boolean = true,
    baseDate: Long = FuzzDataGenerator.defaultBaseDate,
    generateArray: Boolean = false,
    generateStruct: Boolean = false,
    generateMap: Boolean = false,
    primitiveTypes: Seq[DataType] = Seq(
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
      DataTypes.TimestampNTZType,
      DataTypes.StringType,
      DataTypes.BinaryType))
