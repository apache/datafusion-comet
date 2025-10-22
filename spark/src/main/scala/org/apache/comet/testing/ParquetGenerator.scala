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

import scala.util.Random

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DataType, StructType}

object ParquetGenerator {

  /** Generate a Parquet file using a generated schema */
  def makeParquetFile(
      r: Random,
      spark: SparkSession,
      filename: String,
      numRows: Int,
      options: ParquetGeneratorOptions): Unit = {

    val schemaGenOptions = SchemaGenOptions(
      generateArray = options.generateArray,
      generateStruct = options.generateStruct,
      generateMap = options.generateMap,
      primitiveTypes = options.primitiveTypes)
    val schema = FuzzDataGenerator.generateSchema(schemaGenOptions)

    val dataGenOptions = DataGenOptions(
      allowNull = options.allowNull,
      generateNegativeZero = options.generateNegativeZero,
      baseDate = options.baseDate)

    makeParquetFile(r, spark, filename, schema, numRows, dataGenOptions)
  }

  /** Generate a Parquet file using the provided schema */
  def makeParquetFile(
      r: Random,
      spark: SparkSession,
      filename: String,
      schema: StructType,
      numRows: Int,
      options: DataGenOptions): Unit = {
    val df = FuzzDataGenerator.generateDataFrame(r, spark, schema, numRows, options)
    df.write.mode(SaveMode.Overwrite).parquet(filename)
  }

}

/** Schema and Data generation options */
case class ParquetGeneratorOptions(
    allowNull: Boolean = true,
    generateNegativeZero: Boolean = true,
    baseDate: Long = FuzzDataGenerator.defaultBaseDate,
    generateArray: Boolean = false,
    generateStruct: Boolean = false,
    generateMap: Boolean = false,
    primitiveTypes: Seq[DataType] = SchemaGenOptions.defaultPrimitiveTypes)
