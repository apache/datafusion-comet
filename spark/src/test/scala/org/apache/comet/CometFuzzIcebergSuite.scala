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

import scala.util.Random

import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.ParquetOutputTimestampType
import org.apache.spark.sql.types._

import org.apache.comet.DataTypeSupport.isComplexType
import org.apache.comet.testing.{DataGenOptions, FuzzDataGenerator, ParquetGenerator, SchemaGenOptions}

class CometFuzzIcebergSuite extends CometFuzzIcebergBase {

  test("select *") {
    val sql = s"SELECT * FROM $icebergTableName"
    val (_, cometPlan) = checkSparkAnswer(sql)
    assert(1 == collectIcebergNativeScans(cometPlan).length)
  }

  test("select * with limit") {
    val sql = s"SELECT * FROM $icebergTableName LIMIT 500"
    val (_, cometPlan) = checkSparkAnswer(sql)
    assert(1 == collectIcebergNativeScans(cometPlan).length)
  }

  test("order by single column") {
    val df = spark.table(icebergTableName)
    for (col <- df.columns) {
      val sql = s"SELECT $col FROM $icebergTableName ORDER BY $col"
      // cannot run fully natively due to range partitioning and sort
      val (_, cometPlan) = checkSparkAnswer(sql)
      assert(1 == collectIcebergNativeScans(cometPlan).length)
    }
  }

  test("order by multiple columns") {
    val df = spark.table(icebergTableName)
    val allCols = df.columns.mkString(",")
    val sql = s"SELECT $allCols FROM $icebergTableName ORDER BY $allCols"
    // cannot run fully natively due to range partitioning and sort
    val (_, cometPlan) = checkSparkAnswer(sql)
    assert(1 == collectIcebergNativeScans(cometPlan).length)
  }

  test("order by random columns") {
    val df = spark.table(icebergTableName)

    for (_ <- 1 to 10) {
      // We only do order by permutations of primitive types to exercise native shuffle's
      // RangePartitioning which only supports those types.
      val primitiveColumns =
        df.schema.fields.filterNot(f => isComplexType(f.dataType)).map(_.name)
      val shuffledPrimitiveCols = Random.shuffle(primitiveColumns.toList)
      val randomSize = Random.nextInt(shuffledPrimitiveCols.length) + 1
      val randomColsSubset = shuffledPrimitiveCols.take(randomSize).toArray.mkString(",")
      val sql = s"SELECT $randomColsSubset FROM $icebergTableName ORDER BY $randomColsSubset"
      checkSparkAnswerAndOperator(sql)
    }
  }

  test("distribute by single column (complex types)") {
    val df = spark.table(icebergTableName)
    val columns = df.schema.fields.filter(f => isComplexType(f.dataType)).map(_.name)
    for (col <- columns) {
      // DISTRIBUTE BY is equivalent to df.repartition($col) and uses
      val sql = s"SELECT $col FROM $icebergTableName DISTRIBUTE BY $col"
      val resultDf = spark.sql(sql)
      resultDf.collect()
      // check for Comet shuffle
      val plan =
        resultDf.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec].executedPlan
      val cometShuffleExchanges = collectCometShuffleExchanges(plan)
      // Iceberg native scan supports complex types
      assert(cometShuffleExchanges.length == 1)
    }
  }

  test("shuffle supports all types") {
    val df = spark.table(icebergTableName)
    val df2 = df.repartition(8, df.col("c0")).sort("c1")
    df2.collect()
    val cometShuffles = collectCometShuffleExchanges(df2.queryExecution.executedPlan)
    // Iceberg native scan supports complex types
    assert(cometShuffles.length == 2)
  }

  test("join") {
    val df = spark.table(icebergTableName)
    df.createOrReplaceTempView("t1")
    df.createOrReplaceTempView("t2")
    // Filter out complex types - iceberg-rust can't create predicates for struct/array/map equality
    val primitiveColumns = df.schema.fields.filterNot(f => isComplexType(f.dataType)).map(_.name)
    for (col <- primitiveColumns) {
      // cannot run fully native due to HashAggregate
      val sql = s"SELECT count(*) FROM t1 JOIN t2 ON t1.$col = t2.$col"
      val (_, cometPlan) = checkSparkAnswer(sql)
      assert(2 == collectIcebergNativeScans(cometPlan).length)
    }
  }

  test("decode") {
    val df = spark.table(icebergTableName)
    // We want to make sure that the schema generator wasn't modified to accidentally omit
    // BinaryType, since then this test would not run any queries and silently pass.
    var testedBinary = false
    for (field <- df.schema.fields if field.dataType == BinaryType) {
      testedBinary = true
      // Intentionally use odd capitalization of 'utf-8' to test normalization.
      val sql = s"SELECT decode(${field.name}, 'utF-8') FROM $icebergTableName"
      checkSparkAnswerAndOperator(sql)
    }
    assert(testedBinary)
  }

  test("regexp_replace") {
    withSQLConf(CometConf.COMET_REGEXP_ALLOW_INCOMPATIBLE.key -> "true") {
      val df = spark.table(icebergTableName)
      // We want to make sure that the schema generator wasn't modified to accidentally omit
      // StringType, since then this test would not run any queries and silently pass.
      var testedString = false
      for (field <- df.schema.fields if field.dataType == StringType) {
        testedString = true
        val sql = s"SELECT regexp_replace(${field.name}, 'a', 'b') FROM $icebergTableName"
        checkSparkAnswerAndOperator(sql)
      }
      assert(testedString)
    }
  }

  test("Iceberg temporal types written as INT96") {
    testIcebergTemporalTypes(ParquetOutputTimestampType.INT96)
  }

  test("Iceberg temporal types written as TIMESTAMP_MICROS") {
    testIcebergTemporalTypes(ParquetOutputTimestampType.TIMESTAMP_MICROS)
  }

  test("Iceberg temporal types written as TIMESTAMP_MILLIS") {
    testIcebergTemporalTypes(ParquetOutputTimestampType.TIMESTAMP_MILLIS)
  }

  private def testIcebergTemporalTypes(
      outputTimestampType: ParquetOutputTimestampType.Value,
      generateArray: Boolean = true,
      generateStruct: Boolean = true): Unit = {

    val schema = FuzzDataGenerator.generateSchema(
      SchemaGenOptions(
        generateArray = generateArray,
        generateStruct = generateStruct,
        primitiveTypes = SchemaGenOptions.defaultPrimitiveTypes.filterNot { dataType =>
          // Disable decimals - iceberg-rust doesn't support FIXED_LEN_BYTE_ARRAY in page index yet
          dataType.isInstanceOf[DecimalType]
        }))

    val options =
      DataGenOptions(generateNegativeZero = false)

    withTempPath { filename =>
      val random = new Random(42)
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "false",
        SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> outputTimestampType.toString,
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> defaultTimezone) {
        ParquetGenerator.makeParquetFile(random, spark, filename.toString, schema, 100, options)
      }

      Seq(defaultTimezone, "UTC", "America/Denver").foreach { tz =>
        Seq(true, false).foreach { inferTimestampNtzEnabled =>
          Seq(true, false).foreach { int96TimestampConversion =>
            Seq(true, false).foreach { int96AsTimestamp =>
              withSQLConf(
                CometConf.COMET_ENABLED.key -> "true",
                SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz,
                SQLConf.PARQUET_INT96_AS_TIMESTAMP.key -> int96AsTimestamp.toString,
                SQLConf.PARQUET_INT96_TIMESTAMP_CONVERSION.key -> int96TimestampConversion.toString,
                SQLConf.PARQUET_INFER_TIMESTAMP_NTZ_ENABLED.key -> inferTimestampNtzEnabled.toString) {

                val df = spark.table(icebergTableName)

                Seq(defaultTimezone, "UTC", "America/Denver").foreach { tz =>
                  withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz) {
                    def hasTemporalType(t: DataType): Boolean = t match {
                      case DataTypes.DateType | DataTypes.TimestampType |
                          DataTypes.TimestampNTZType =>
                        true
                      case t: StructType => t.exists(f => hasTemporalType(f.dataType))
                      case t: ArrayType => hasTemporalType(t.elementType)
                      case _ => false
                    }

                    val columns =
                      df.schema.fields.filter(f => hasTemporalType(f.dataType)).map(_.name)

                    for (col <- columns) {
                      checkSparkAnswer(s"SELECT $col FROM $icebergTableName ORDER BY $col")
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  def collectCometShuffleExchanges(plan: org.apache.spark.sql.execution.SparkPlan)
      : Seq[org.apache.spark.sql.execution.SparkPlan] = {
    collect(plan) {
      case exchange: org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec =>
        exchange
    }
  }
}
