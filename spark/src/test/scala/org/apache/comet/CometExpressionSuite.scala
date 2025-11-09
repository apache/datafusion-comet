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

import java.time.{Duration, Period}

import scala.util.Random

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{CometTestBase, DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast, Literal, TruncDate, TruncTimestamp}
import org.apache.spark.sql.catalyst.optimizer.SimplifyExtractValueOps
import org.apache.spark.sql.comet.{CometColumnarToRowExec, CometProjectExec}
import org.apache.spark.sql.execution.{InputAdapter, ProjectExec, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.SESSION_LOCAL_TIMEZONE
import org.apache.spark.sql.types._

import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus
import org.apache.comet.serde.CometConcat
import org.apache.comet.testing.{DataGenOptions, FuzzDataGenerator}

class CometExpressionSuite extends CometTestBase with AdaptiveSparkPlanHelper {
  import testImplicits._

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      withSQLConf(CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_AUTO) {
        testFun
      }
    }
  }

  val ARITHMETIC_OVERFLOW_EXCEPTION_MSG =
    """org.apache.comet.CometNativeException: [ARITHMETIC_OVERFLOW] integer overflow. If necessary set "spark.sql.ansi.enabled" to "false" to bypass this error"""
  val DIVIDE_BY_ZERO_EXCEPTION_MSG =
    """Division by zero. Use `try_divide` to tolerate divisor being 0 and return NULL instead"""

  test("sort floating point with negative zero") {
    val schema = StructType(
      Seq(
        StructField("c0", DataTypes.FloatType, true),
        StructField("c1", DataTypes.DoubleType, true)))
    val df = FuzzDataGenerator.generateDataFrame(
      new Random(42),
      spark,
      schema,
      1000,
      DataGenOptions(generateNegativeZero = true))
    df.createOrReplaceTempView("tbl")

    withSQLConf(CometConf.getExprAllowIncompatConfigKey("SortOrder") -> "false") {
      checkSparkAnswerAndFallbackReasons(
        "select * from tbl order by 1, 2",
        Set(
          "unsupported range partitioning sort order",
          "Sorting on floating-point is not 100% compatible with Spark"))
    }
  }

  test("sort array of floating point with negative zero") {
    val schema = StructType(
      Seq(
        StructField("c0", DataTypes.createArrayType(DataTypes.FloatType), true),
        StructField("c1", DataTypes.createArrayType(DataTypes.DoubleType), true)))
    val df = FuzzDataGenerator.generateDataFrame(
      new Random(42),
      spark,
      schema,
      1000,
      DataGenOptions(generateNegativeZero = true))
    df.createOrReplaceTempView("tbl")

    withSQLConf(CometConf.getExprAllowIncompatConfigKey("SortOrder") -> "false") {
      checkSparkAnswerAndFallbackReason(
        "select * from tbl order by 1, 2",
        "unsupported range partitioning sort order")
    }
  }

  test("sort struct containing floating point with negative zero") {
    val schema = StructType(
      Seq(
        StructField(
          "float_struct",
          StructType(Seq(StructField("c0", DataTypes.FloatType, true)))),
        StructField(
          "float_double",
          StructType(Seq(StructField("c0", DataTypes.DoubleType, true))))))
    val df = FuzzDataGenerator.generateDataFrame(
      new Random(42),
      spark,
      schema,
      1000,
      DataGenOptions(generateNegativeZero = true))
    df.createOrReplaceTempView("tbl")

    withSQLConf(CometConf.getExprAllowIncompatConfigKey("SortOrder") -> "false") {
      checkSparkAnswerAndFallbackReason(
        "select * from tbl order by 1, 2",
        "unsupported range partitioning sort order")
    }
  }

  test("compare true/false to negative zero") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
        val table = "test"
        withTable(table) {
          sql(s"create table $table(col1 boolean, col2 float) using parquet")
          sql(s"insert into $table values(true, -0.0)")
          sql(s"insert into $table values(false, -0.0)")

          checkSparkAnswerAndOperator(
            s"SELECT col1, negative(col2), cast(col1 as float), col1 = negative(col2) FROM $table")
        }
      }
    }
  }

  test("parquet default values") {
    withTable("t1") {
      sql("create table t1(col1 boolean) using parquet")
      sql("insert into t1 values(true)")
      sql("alter table t1 add column col2 string default 'hello'")
      checkSparkAnswerAndOperator("select * from t1")
    }
  }

  test("coalesce should return correct datatype") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = dictionaryEnabled, 10000)
        withParquetTable(path.toString, "tbl") {
          checkSparkAnswerAndOperator(
            "SELECT coalesce(cast(_18 as date), cast(_19 as date), _20) FROM tbl")
        }
      }
    }
  }

  test("decimals divide by zero") {
    Seq(true, false).foreach { dictionary =>
      withSQLConf(
        SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key -> "false",
        "parquet.enable.dictionary" -> dictionary.toString) {
        withTempPath { dir =>
          val data = makeDecimalRDD(10, DecimalType(18, 10), dictionary)
          data.write.parquet(dir.getCanonicalPath)
          readParquetFile(dir.getCanonicalPath) { df =>
            {
              val decimalLiteral = Decimal(0.00)
              val cometDf = df.select($"dec" / decimalLiteral, $"dec" % decimalLiteral)
              checkSparkAnswerAndOperator(cometDf)
            }
          }
        }
      }
    }
  }

  test("Integral Division Overflow Handling Matches Spark Behavior") {
    withTable("t1") {
      withSQLConf(CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key -> "true") {
        val value = Long.MinValue
        sql("create table t1(c1 long, c2 short) using parquet")
        sql(s"insert into t1 values($value, -1)")
        val res = sql("select c1 div c2 from t1 order by c1")
        checkSparkAnswerAndOperator(res)
      }
    }
  }

  test("basic data type support") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = dictionaryEnabled, 10000)
        withSQLConf(CometConf.COMET_SCAN_ALLOW_INCOMPATIBLE.key -> "false") {
          withParquetTable(path.toString, "tbl") {
            checkSparkAnswerAndOperator("select * FROM tbl WHERE _2 > 100")
          }
        }
      }
    }
  }

  test("uint data type support") {
    Seq(true, false).foreach { dictionaryEnabled =>
      // TODO: Once the question of what to get back from uint_8, uint_16 types is resolved,
      // we can also update this test to check for COMET_SCAN_ALLOW_INCOMPATIBLE=true
      Seq(false).foreach { allowIncompatible =>
        {
          withSQLConf(CometConf.COMET_SCAN_ALLOW_INCOMPATIBLE.key -> allowIncompatible.toString) {
            withTempDir { dir =>
              val path = new Path(dir.toURI.toString, "testuint.parquet")
              makeParquetFileAllPrimitiveTypes(
                path,
                dictionaryEnabled = dictionaryEnabled,
                Byte.MinValue,
                Byte.MaxValue)
              withParquetTable(path.toString, "tbl") {
                val qry = "select _9 from tbl order by _11"
                if (usingDataSourceExec(conf)) {
                  if (!allowIncompatible) {
                    checkSparkAnswerAndOperator(qry)
                  } else {
                    // need to convert the values to unsigned values
                    val expected = (Byte.MinValue to Byte.MaxValue)
                      .map(v => {
                        if (v < 0) Byte.MaxValue.toShort - v else v
                      })
                      .toDF("a")
                    checkAnswer(sql(qry), expected)
                  }
                } else {
                  checkSparkAnswerAndOperator(qry)
                }
              }
            }
          }
        }
      }
    }
  }

  test("null literals") {
    val batchSize = 1000
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = dictionaryEnabled, batchSize)
        withParquetTable(path.toString, "tbl") {
          val sqlString =
            """SELECT
              |_4 + null,
              |_15 - null,
              |_16 * null,
              |cast(null as struct<_1:int>),
              |cast(null as map<int, int>),
              |cast(null as array<int>)
              |FROM tbl""".stripMargin
          val df2 = sql(sqlString)
          val rows = df2.collect()
          assert(rows.length == batchSize)
          assert(rows.forall(_ == Row(null, null, null, null, null, null)))

          checkSparkAnswerAndOperator(sqlString)
        }
      }

    }
  }

  test("date and timestamp type literals") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = dictionaryEnabled, 10000)
        withParquetTable(path.toString, "tbl") {
          checkSparkAnswerAndOperator(
            "SELECT _4 FROM tbl WHERE " +
              "_20 > CAST('2020-01-01' AS DATE) AND _18 < CAST('2020-01-01' AS TIMESTAMP)")
        }
      }
    }
  }

  test("date_add with int scalars") {
    Seq(true, false).foreach { dictionaryEnabled =>
      Seq("TINYINT", "SHORT", "INT").foreach { intType =>
        withTempDir { dir =>
          val path = new Path(dir.toURI.toString, "test.parquet")
          makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = dictionaryEnabled, 10000)
          withParquetTable(path.toString, "tbl") {
            checkSparkAnswerAndOperator(f"SELECT _20 + CAST(2 as $intType) from tbl")
          }
        }
      }
    }
  }

  // TODO: https://github.com/apache/datafusion-comet/issues/2539
  ignore("date_add with scalar overflow") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = dictionaryEnabled, 10000)
        withParquetTable(path.toString, "tbl") {
          val (sparkErr, cometErr) =
            checkSparkMaybeThrows(sql(s"SELECT _20 + ${Int.MaxValue} FROM tbl"))
          if (isSpark40Plus) {
            assert(sparkErr.get.getMessage.contains("EXPRESSION_DECODING_FAILED"))
          } else {
            assert(sparkErr.get.getMessage.contains("integer overflow"))
          }
          assert(cometErr.get.getMessage.contains("attempt to add with overflow"))
        }
      }
    }
  }

  test("date_add with int arrays") {
    Seq(true, false).foreach { dictionaryEnabled =>
      Seq("_2", "_3", "_4").foreach { intColumn => // tinyint, short, int columns
        withTempDir { dir =>
          val path = new Path(dir.toURI.toString, "test.parquet")
          makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = dictionaryEnabled, 10000)
          withParquetTable(path.toString, "tbl") {
            checkSparkAnswerAndOperator(f"SELECT _20 + $intColumn FROM tbl")
          }
        }
      }
    }
  }

  test("date_sub with int scalars") {
    Seq(true, false).foreach { dictionaryEnabled =>
      Seq("TINYINT", "SHORT", "INT").foreach { intType =>
        withTempDir { dir =>
          val path = new Path(dir.toURI.toString, "test.parquet")
          makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = dictionaryEnabled, 10000)
          withParquetTable(path.toString, "tbl") {
            checkSparkAnswerAndOperator(f"SELECT _20 - CAST(2 as $intType) from tbl")
          }
        }
      }
    }
  }

  test("date_sub with scalar overflow") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = dictionaryEnabled, 10000)
        withParquetTable(path.toString, "tbl") {
          val (sparkErr, cometErr) =
            checkSparkMaybeThrows(sql(s"SELECT _20 - ${Int.MaxValue} FROM tbl"))
          if (isSpark40Plus) {
            assert(sparkErr.get.getMessage.contains("EXPRESSION_DECODING_FAILED"))
            assert(cometErr.get.getMessage.contains("EXPRESSION_DECODING_FAILED"))
          } else {
            assert(sparkErr.get.getMessage.contains("integer overflow"))
            assert(cometErr.get.getMessage.contains("integer overflow"))
          }
        }
      }
    }
  }

  test("date_sub with int arrays") {
    Seq(true, false).foreach { dictionaryEnabled =>
      Seq("_2", "_3", "_4").foreach { intColumn => // tinyint, short, int columns
        withTempDir { dir =>
          val path = new Path(dir.toURI.toString, "test.parquet")
          makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = dictionaryEnabled, 10000)
          withParquetTable(path.toString, "tbl") {
            checkSparkAnswerAndOperator(f"SELECT _20 - $intColumn FROM tbl")
          }
        }
      }
    }
  }

  test("try_add") {
    val data = Seq((1, 1))
    withParquetTable(data, "tbl") {
      checkSparkAnswerAndOperator(spark.sql("""
          |SELECT
          |  try_add(2147483647, 1),
          |  try_add(-2147483648, -1),
          |  try_add(NULL, 5),
          |  try_add(5, NULL),
          |  try_add(9223372036854775807, 1),
          |  try_add(-9223372036854775808, -1)
          |  from tbl
          |  """.stripMargin))
    }
  }

  test("try_subtract") {
    val data = Seq((1, 1))
    withParquetTable(data, "tbl") {
      checkSparkAnswerAndOperator(spark.sql("""
          |SELECT
          |  try_subtract(2147483647, -1),
          |  try_subtract(-2147483648, 1),
          |  try_subtract(NULL, 5),
          |  try_subtract(5, NULL),
          |  try_subtract(9223372036854775807, -1),
          |  try_subtract(-9223372036854775808, 1)
          |  FROM tbl
           """.stripMargin))
    }
  }

  test("try_multiply") {
    val data = Seq((1, 1))
    withParquetTable(data, "tbl") {
      checkSparkAnswerAndOperator(spark.sql("""
          |SELECT
          |  try_multiply(1073741824, 4),
          |  try_multiply(-1073741824, 4),
          |  try_multiply(NULL, 5),
          |  try_multiply(5, NULL),
          |  try_multiply(3037000499, 3037000500),
          |  try_multiply(-3037000499, 3037000500)
          |FROM tbl
           """.stripMargin))
    }
  }

  test("try_divide") {
    val data = Seq((15121991, 0))
    withParquetTable(data, "tbl") {
      checkSparkAnswerAndOperator("SELECT try_divide(_1, _2) FROM tbl")
      checkSparkAnswerAndOperator("""
            |SELECT
            |  try_divide(10, 0),
            |  try_divide(NULL, 5),
            |  try_divide(5, NULL),
            |  try_divide(-2147483648, -1),
            |  try_divide(-9223372036854775808, -1),
            |  try_divide(DECIMAL('9999999999999999999999999999'), 0.1)
            |  from tbl
            |""".stripMargin)
    }
  }

  test("try_integral_divide overflow cases") {
    val data = Seq((15121991, 0))
    withParquetTable(data, "tbl") {
      checkSparkAnswerAndOperator("SELECT try_divide(_1, _2) FROM tbl")
      checkSparkAnswerAndOperator("""
                                    |SELECT try_divide(-128, -1),
                                    |try_divide(-32768, -1),
                                    |try_divide(-2147483648, -1),
                                    |try_divide(-9223372036854775808, -1),
                                    |try_divide(CAST(99999 AS DECIMAL(5,0)), CAST(0.0001 AS DECIMAL(5,4)))
                                    |from tbl
                                    |""".stripMargin)
    }
  }

  test("test coalesce lazy eval") {
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key -> "true") {
      val data = Seq((9999999999999L, 0))
      withParquetTable(data, "t1") {
        val res = spark.sql("""
            |SELECT coalesce(_1, CAST(_1 AS TINYINT)) from t1;
            |  """.stripMargin)
        checkSparkAnswerAndOperator(res)
      }
    }
  }

  test("dictionary arithmetic") {
    // TODO: test ANSI mode
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false", "parquet.enable.dictionary" -> "true") {
      withParquetTable((0 until 10).map(i => (i % 5, i % 3)), "tbl") {
        checkSparkAnswerAndOperator("SELECT _1 + _2, _1 - _2, _1 * _2, _1 / _2, _1 % _2 FROM tbl")
      }
    }
  }

  test("dictionary arithmetic with scalar") {
    withSQLConf("parquet.enable.dictionary" -> "true") {
      withParquetTable((0 until 10).map(i => (i % 5, i % 3)), "tbl") {
        checkSparkAnswerAndOperator("SELECT _1 + 1, _1 - 1, _1 * 2, _1 / 2, _1 % 2 FROM tbl")
      }
    }
  }

  test("string type and substring") {
    withParquetTable((0 until 5).map(i => (i.toString, (i + 100).toString)), "tbl") {
      checkSparkAnswerAndOperator("SELECT _1, substring(_2, 2, 2) FROM tbl")
      checkSparkAnswerAndOperator("SELECT _1, substring(_2, 2, -2) FROM tbl")
      checkSparkAnswerAndOperator("SELECT _1, substring(_2, -2, 2) FROM tbl")
      checkSparkAnswerAndOperator("SELECT _1, substring(_2, -2, -2) FROM tbl")
      checkSparkAnswerAndOperator("SELECT _1, substring(_2, -2, 10) FROM tbl")
      checkSparkAnswerAndOperator("SELECT _1, substring(_2, 0, 0) FROM tbl")
      checkSparkAnswerAndOperator("SELECT _1, substring(_2, 1, 0) FROM tbl")
    }
  }

  test("substring with start < 1") {
    withTempPath { _ =>
      withTable("t") {
        sql("create table t (col string) using parquet")
        sql("insert into t values('123456')")
        checkSparkAnswerAndOperator(sql("select substring(col, 0) from t"))
        checkSparkAnswerAndOperator(sql("select substring(col, -1) from t"))
      }
    }
  }

  test("string with coalesce") {
    withParquetTable(
      (0 until 10).map(i => (i.toString, if (i > 5) None else Some((i + 100).toString))),
      "tbl") {
      checkSparkAnswerAndOperator(
        "SELECT coalesce(_1), coalesce(_1, 1), coalesce(null, _1), coalesce(null, 1), coalesce(_2, _1), coalesce(null) FROM tbl")
    }
  }

  test("substring with dictionary") {
    val data = (0 until 1000)
      .map(_ % 5) // reduce value space to trigger dictionary encoding
      .map(i => (i.toString, (i + 100).toString))
    withParquetTable(data, "tbl") {
      checkSparkAnswerAndOperator("SELECT _1, substring(_2, 2, 2) FROM tbl")
    }
  }

  test("hour, minute, second") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "part-r-0.parquet")
        val expected = makeRawTimeParquetFile(path, dictionaryEnabled = dictionaryEnabled, 10000)
        readParquetFile(path.toString) { df =>
          val query = df.select(expr("hour(_1)"), expr("minute(_1)"), expr("second(_1)"))

          checkAnswer(
            query,
            expected.map {
              case None =>
                Row(null, null, null)
              case Some(i) =>
                val timestamp = new java.sql.Timestamp(i).toLocalDateTime
                val hour = timestamp.getHour
                val minute = timestamp.getMinute
                val second = timestamp.getSecond

                Row(hour, minute, second)
            })
        }
      }
    }
  }

  test("time expressions folded on jvm") {
    val ts = "1969-12-31 16:23:45"

    val functions = Map("hour" -> 16, "minute" -> 23, "second" -> 45)

    functions.foreach { case (func, expectedValue) =>
      val query = s"SELECT $func('$ts') AS result"
      val df = spark.sql(query)
      val optimizedPlan = df.queryExecution.optimizedPlan

      val isFolded = optimizedPlan.expressions.exists {
        case alias: Alias =>
          alias.child match {
            case Literal(value, _) => value == expectedValue
            case _ => false
          }
        case _ => false
      }

      assert(isFolded, s"Expected '$func(...)' to be constant-folded to Literal($expectedValue)")
    }
  }

  test("hour on int96 timestamp column") {
    import testImplicits._

    val N = 100
    val ts = "2020-01-01 01:02:03.123456"
    Seq(true, false).foreach { dictionaryEnabled =>
      Seq(false, true).foreach { conversionEnabled =>
        withSQLConf(
          SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> "INT96",
          SQLConf.PARQUET_INT96_TIMESTAMP_CONVERSION.key -> conversionEnabled.toString) {
          withTempPath { path =>
            Seq
              .tabulate(N)(_ => ts)
              .toDF("ts1")
              .select($"ts1".cast("timestamp").as("ts"))
              .repartition(1)
              .write
              .option("parquet.enable.dictionary", dictionaryEnabled)
              .parquet(path.getCanonicalPath)

            checkAnswer(
              spark.read.parquet(path.getCanonicalPath).select(expr("hour(ts)")),
              Seq.tabulate(N)(_ => Row(1)))
          }
        }
      }
    }
  }

  test("cast timestamp and timestamp_ntz") {
    withSQLConf(
      SESSION_LOCAL_TIMEZONE.key -> "Asia/Kathmandu",
      CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key -> "true") {
      Seq(true, false).foreach { dictionaryEnabled =>
        withTempDir { dir =>
          val path = new Path(dir.toURI.toString, "timestamp_trunc.parquet")
          makeRawTimeParquetFile(path, dictionaryEnabled = dictionaryEnabled, 10000)
          withParquetTable(path.toString, "timetbl") {
            checkSparkAnswerAndOperator(
              "SELECT " +
                "cast(_2 as timestamp) tz_millis, " +
                "cast(_3 as timestamp) ntz_millis, " +
                "cast(_4 as timestamp) tz_micros, " +
                "cast(_5 as timestamp) ntz_micros " +
                " from timetbl")
          }
        }
      }
    }
  }

  test("cast timestamp and timestamp_ntz to string") {
    withSQLConf(
      SESSION_LOCAL_TIMEZONE.key -> "Asia/Kathmandu",
      CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key -> "true") {
      Seq(true, false).foreach { dictionaryEnabled =>
        withTempDir { dir =>
          val path = new Path(dir.toURI.toString, "timestamp_trunc.parquet")
          makeRawTimeParquetFile(path, dictionaryEnabled = dictionaryEnabled, 2001)
          withParquetTable(path.toString, "timetbl") {
            checkSparkAnswerAndOperator(
              "SELECT " +
                "cast(_2 as string) tz_millis, " +
                "cast(_3 as string) ntz_millis, " +
                "cast(_4 as string) tz_micros, " +
                "cast(_5 as string) ntz_micros " +
                " from timetbl")
          }
        }
      }
    }
  }

  test("cast timestamp and timestamp_ntz to long, date") {
    withSQLConf(
      SESSION_LOCAL_TIMEZONE.key -> "Asia/Kathmandu",
      CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key -> "true") {
      Seq(true, false).foreach { dictionaryEnabled =>
        withTempDir { dir =>
          val path = new Path(dir.toURI.toString, "timestamp_trunc.parquet")
          makeRawTimeParquetFile(path, dictionaryEnabled = dictionaryEnabled, 10000)
          withParquetTable(path.toString, "timetbl") {
            checkSparkAnswerAndOperator(
              "SELECT " +
                "cast(_2 as long) tz_millis, " +
                "cast(_4 as long) tz_micros, " +
                "cast(_2 as date) tz_millis_to_date, " +
                "cast(_3 as date) ntz_millis_to_date, " +
                "cast(_4 as date) tz_micros_to_date, " +
                "cast(_5 as date) ntz_micros_to_date " +
                " from timetbl")
          }
        }
      }
    }
  }

  test("trunc") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "date_trunc.parquet")
        makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = dictionaryEnabled, 10000)
        withParquetTable(path.toString, "tbl") {
          Seq("YEAR", "YYYY", "YY", "QUARTER", "MON", "MONTH", "MM", "WEEK").foreach { format =>
            checkSparkAnswerAndOperator(s"SELECT trunc(_20, '$format') from tbl")
          }
        }
      }
    }
  }

  test("trunc with format array") {
    val numRows = 1000
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "date_trunc_with_format.parquet")
        makeDateTimeWithFormatTable(path, dictionaryEnabled = dictionaryEnabled, numRows)
        withParquetTable(path.toString, "dateformattbl") {
          withSQLConf(CometConf.getExprAllowIncompatConfigKey(classOf[TruncDate]) -> "true") {
            checkSparkAnswerAndOperator(
              "SELECT " +
                "dateformat, _7, " +
                "trunc(_7, dateformat) " +
                " from dateformattbl ")
          }
        }
      }
    }
  }

  test("date_trunc") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "timestamp_trunc.parquet")
        makeRawTimeParquetFile(path, dictionaryEnabled = dictionaryEnabled, 10000)
        withParquetTable(path.toString, "timetbl") {
          Seq(
            "YEAR",
            "YYYY",
            "YY",
            "MON",
            "MONTH",
            "MM",
            "QUARTER",
            "WEEK",
            "DAY",
            "DD",
            "HOUR",
            "MINUTE",
            "SECOND",
            "MILLISECOND",
            "MICROSECOND").foreach { format =>
            checkSparkAnswerAndOperator(
              "SELECT " +
                s"date_trunc('$format', _0), " +
                s"date_trunc('$format', _1), " +
                s"date_trunc('$format', _2), " +
                s"date_trunc('$format', _4) " +
                " from timetbl")
          }
        }
      }
    }
  }

  test("date_trunc with timestamp_ntz") {
    withSQLConf(CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key -> "true") {
      Seq(true, false).foreach { dictionaryEnabled =>
        withTempDir { dir =>
          val path = new Path(dir.toURI.toString, "timestamp_trunc.parquet")
          makeRawTimeParquetFile(path, dictionaryEnabled = dictionaryEnabled, 10000)
          withParquetTable(path.toString, "timetbl") {
            Seq(
              "YEAR",
              "YYYY",
              "YY",
              "MON",
              "MONTH",
              "MM",
              "QUARTER",
              "WEEK",
              "DAY",
              "DD",
              "HOUR",
              "MINUTE",
              "SECOND",
              "MILLISECOND",
              "MICROSECOND").foreach { format =>
              checkSparkAnswerAndOperator(
                "SELECT " +
                  s"date_trunc('$format', _3), " +
                  s"date_trunc('$format', _5)  " +
                  " from timetbl")
            }
          }
        }
      }
    }
  }

  test("date_trunc with format array") {
    val numRows = 1000
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "timestamp_trunc_with_format.parquet")
        makeDateTimeWithFormatTable(path, dictionaryEnabled = dictionaryEnabled, numRows)
        withParquetTable(path.toString, "timeformattbl") {
          withSQLConf(
            CometConf.getExprAllowIncompatConfigKey(classOf[Cast]) -> "true",
            CometConf.getExprAllowIncompatConfigKey(classOf[TruncTimestamp]) -> "true") {
            checkSparkAnswerAndOperator(
              "SELECT " +
                "format, _0, _1, _2, _3, _4, _5, " +
                "date_trunc(format, _0), " +
                "date_trunc(format, _1), " +
                "date_trunc(format, _2), " +
                "date_trunc(format, _3), " +
                "date_trunc(format, _4), " +
                "date_trunc(format, _5) " +
                " from timeformattbl ")
          }
        }
      }
    }
  }

  test("date_trunc on int96 timestamp column") {
    import testImplicits._

    val N = 100
    val ts = "2020-01-01 01:02:03.123456"
    Seq(true, false).foreach { dictionaryEnabled =>
      Seq(false, true).foreach { conversionEnabled =>
        withSQLConf(
          SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> "INT96",
          SQLConf.PARQUET_INT96_TIMESTAMP_CONVERSION.key -> conversionEnabled.toString) {
          withTempPath { path =>
            Seq
              .tabulate(N)(_ => ts)
              .toDF("ts1")
              .select($"ts1".cast("timestamp").as("ts"))
              .repartition(1)
              .write
              .option("parquet.enable.dictionary", dictionaryEnabled)
              .parquet(path.getCanonicalPath)

            withParquetTable(path.toString, "int96timetbl") {
              Seq(
                "YEAR",
                "YYYY",
                "YY",
                "MON",
                "MONTH",
                "MM",
                "QUARTER",
                "WEEK",
                "DAY",
                "DD",
                "HOUR",
                "MINUTE",
                "SECOND",
                "MILLISECOND",
                "MICROSECOND").foreach { format =>
                val sql = "SELECT " +
                  s"date_trunc('$format', ts )" +
                  " from int96timetbl"

                if (conversionEnabled) {
                  // plugin is disabled if PARQUET_INT96_TIMESTAMP_CONVERSION is true
                  checkSparkAnswer(sql)
                } else {
                  checkSparkAnswerAndOperator(sql)
                }
              }
            }
          }
        }
      }
    }
  }

  test("charvarchar") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
        val table = "char_tbl4"
        withTable(table) {
          val view = "str_view"
          withView(view) {
            sql(s"""create temporary view $view as select c, v from values
                   | (null, null), (null, null),
                   | (null, 'S'), (null, 'S'),
                   | ('N', 'N '), ('N', 'N '),
                   | ('Ne', 'Sp'), ('Ne', 'Sp'),
                   | ('Net  ', 'Spa  '), ('Net  ', 'Spa  '),
                   | ('NetE', 'Spar'), ('NetE', 'Spar'),
                   | ('NetEa ', 'Spark '), ('NetEa ', 'Spark '),
                   | ('NetEas ', 'Spark'), ('NetEas ', 'Spark'),
                   | ('NetEase', 'Spark-'), ('NetEase', 'Spark-') t(c, v);""".stripMargin)
            sql(
              s"create table $table(c7 char(7), c8 char(8), v varchar(6), s string) using parquet;")
            sql(s"insert into $table select c, c, v, c from $view;")
            val df = sql(s"""select substring(c7, 2), substring(c8, 2),
                            | substring(v, 3), substring(s, 2) from $table;""".stripMargin)

            val expected = Row("      ", "       ", "", "") ::
              Row(null, null, "", null) :: Row(null, null, null, null) ::
              Row("e     ", "e      ", "", "e") :: Row("et    ", "et     ", "a  ", "et  ") ::
              Row("etE   ", "etE    ", "ar", "etE") ::
              Row("etEa  ", "etEa   ", "ark ", "etEa ") ::
              Row("etEas ", "etEas  ", "ark", "etEas ") ::
              Row("etEase", "etEase ", "ark-", "etEase") :: Nil
            checkAnswer(df, expected ::: expected)
          }
        }
      }
    }
  }

  test("char varchar over length values") {
    Seq("char", "varchar").foreach { typ =>
      withTempPath { dir =>
        withTable("t") {
          sql("select '123456' as col").write.format("parquet").save(dir.toString)
          sql(s"create table t (col $typ(2)) using parquet location '$dir'")
          sql("insert into t values('1')")
          checkSparkAnswerAndOperator(sql("select substring(col, 1) from t"))
          checkSparkAnswerAndOperator(sql("select substring(col, 0) from t"))
          checkSparkAnswerAndOperator(sql("select substring(col, -1) from t"))
        }
      }
    }
  }

  test("like (LikeSimplification enabled)") {
    val table = "names"
    withTable(table) {
      sql(s"create table $table(id int, name varchar(20)) using parquet")
      sql(s"insert into $table values(1,'James Smith')")
      sql(s"insert into $table values(2,'Michael Rose')")
      sql(s"insert into $table values(3,'Robert Williams')")
      sql(s"insert into $table values(4,'Rames Rose')")
      sql(s"insert into $table values(5,'Rames rose')")

      // Filter column having values 'Rames _ose', where any character matches for '_'
      val query = sql(s"select id from $table where name like 'Rames _ose'")
      checkAnswer(query, Row(4) :: Row(5) :: Nil)

      // Filter rows that contains 'rose' in 'name' column
      val queryContains = sql(s"select id from $table where name like '%rose%'")
      checkAnswer(queryContains, Row(5) :: Nil)

      // Filter rows that starts with 'R' following by any characters
      val queryStartsWith = sql(s"select id from $table where name like 'R%'")
      checkAnswer(queryStartsWith, Row(3) :: Row(4) :: Row(5) :: Nil)

      // Filter rows that ends with 's' following by any characters
      val queryEndsWith = sql(s"select id from $table where name like '%s'")
      checkAnswer(queryEndsWith, Row(3) :: Nil)
    }
  }

  test("like with custom escape") {
    val table = "names"
    withTable(table) {
      sql(s"create table $table(id int, name varchar(20)) using parquet")
      sql(s"insert into $table values(1,'James Smith')")
      sql(s"insert into $table values(2,'Michael_Rose')")
      sql(s"insert into $table values(3,'Robert_R_Williams')")

      // Filter column having values that include underscores
      val queryDefaultEscape = sql("select id from names where name like '%\\_%'")
      checkSparkAnswerAndOperator(queryDefaultEscape)

      val queryCustomEscape = sql("select id from names where name like '%$_%' escape '$'")
      checkAnswer(queryCustomEscape, Row(2) :: Row(3) :: Nil)

    }
  }

  test("rlike simple case") {
    val table = "rlike_names"
    Seq(false, true).foreach { withDictionary =>
      val data = Seq("James Smith", "Michael Rose", "Rames Rose", "Rames rose") ++
        // add repetitive data to trigger dictionary encoding
        Range(0, 100).map(_ => "John Smith")
      withParquetFile(data.zipWithIndex, withDictionary) { file =>
        withSQLConf(CometConf.COMET_REGEXP_ALLOW_INCOMPATIBLE.key -> "true") {
          spark.read.parquet(file).createOrReplaceTempView(table)
          val query = sql(s"select _2 as id, _1 rlike 'R[a-z]+s [Rr]ose' from $table")
          checkSparkAnswerAndOperator(query)
        }
      }
    }
  }

  test("withInfo") {
    val table = "with_info"
    withTable(table) {
      sql(s"create table $table(id int, name varchar(20)) using parquet")
      sql(s"insert into $table values(1,'James Smith')")
      val query = sql(s"select cast(id as string) from $table")
      val (_, cometPlan) = checkSparkAnswerAndOperator(query)
      val project = cometPlan
        .asInstanceOf[WholeStageCodegenExec]
        .child
        .asInstanceOf[CometColumnarToRowExec]
        .child
        .asInstanceOf[InputAdapter]
        .child
        .asInstanceOf[CometProjectExec]
      val id = project.expressions.head
      CometSparkSessionExtensions.withInfo(id, "reason 1")
      CometSparkSessionExtensions.withInfo(project, "reason 2")
      CometSparkSessionExtensions.withInfo(project, "reason 3", id)
      CometSparkSessionExtensions.withInfo(project, id)
      CometSparkSessionExtensions.withInfo(project, "reason 4")
      CometSparkSessionExtensions.withInfo(project, "reason 5", id)
      CometSparkSessionExtensions.withInfo(project, id)
      CometSparkSessionExtensions.withInfo(project, "reason 6")
      val explain = new ExtendedExplainInfo().generateExtendedInfo(project)
      for (i <- 1 until 7) {
        assert(explain.contains(s"reason $i"))
      }
    }
  }

  test("rlike fallback for non scalar pattern") {
    val table = "rlike_fallback"
    withTable(table) {
      sql(s"create table $table(id int, name varchar(20)) using parquet")
      sql(s"insert into $table values(1,'James Smith')")
      withSQLConf(CometConf.COMET_REGEXP_ALLOW_INCOMPATIBLE.key -> "true") {
        val query2 = sql(s"select id from $table where name rlike name")
        val (_, cometPlan) = checkSparkAnswer(query2)
        val explain = new ExtendedExplainInfo().generateExtendedInfo(cometPlan)
        assert(explain.contains("Only scalar regexp patterns are supported"))
      }
    }
  }

  test("rlike whitespace") {
    val table = "rlike_whitespace"
    withTable(table) {
      sql(s"create table $table(id int, name varchar(20)) using parquet")
      val values =
        Seq("James Smith", "\rJames\rSmith\r", "\nJames\nSmith\n", "\r\nJames\r\nSmith\r\n")
      values.zipWithIndex.foreach { x =>
        sql(s"insert into $table values (${x._2}, '${x._1}')")
      }
      val patterns = Seq(
        "James",
        "J[a-z]mes",
        "^James",
        "\\AJames",
        "Smith",
        "James$",
        "James\\Z",
        "James\\z",
        "^Smith",
        "\\ASmith",
        // $ produces different results - we could potentially transpile this to a different
        // expression or just fall back to Spark for this case
        // "Smith$",
        "Smith\\Z",
        "Smith\\z")
      withSQLConf(CometConf.COMET_REGEXP_ALLOW_INCOMPATIBLE.key -> "true") {
        patterns.foreach { pattern =>
          val query2 = sql(s"select name, '$pattern', name rlike '$pattern' from $table")
          checkSparkAnswerAndOperator(query2)
        }
      }
    }
  }

  test("rlike") {
    val table = "rlike_fuzz"
    val gen = new DataGenerator(new Random(42))
    withTable(table) {
      // generate some data
      // newline characters are intentionally omitted for now
      val dataChars = "\t abc123"
      sql(s"create table $table(id int, name varchar(20)) using parquet")
      gen.generateStrings(100, dataChars, 6).zipWithIndex.foreach { x =>
        sql(s"insert into $table values(${x._2}, '${x._1}')")
      }

      // test some common cases - this is far from comprehensive
      // see https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html
      // for all valid patterns in Java's regexp engine
      //
      // patterns not currently covered:
      // - octal values
      // - hex values
      // - specific character matches
      // - specific whitespace/newline matches
      // - complex character classes (union, intersection, subtraction)
      // - POSIX character classes
      // - java.lang.Character classes
      // - Classes for Unicode scripts, blocks, categories and binary properties
      // - reluctant quantifiers
      // - possessive quantifiers
      // - logical operators
      // - back-references
      // - quotations
      // - special constructs (name capturing and non-capturing)
      val startPatterns = Seq("", "^", "\\A")
      val endPatterns = Seq("", "$", "\\Z", "\\z")
      val patternParts = Seq(
        "[0-9]",
        "[a-z]",
        "[^a-z]",
        "\\d",
        "\\D",
        "\\w",
        "\\W",
        "\\b",
        "\\B",
        "\\h",
        "\\H",
        "\\s",
        "\\S",
        "\\v",
        "\\V")
      val qualifiers = Seq("", "+", "*", "?", "{1,}")

      withSQLConf(CometConf.COMET_REGEXP_ALLOW_INCOMPATIBLE.key -> "true") {
        // testing every possible combination takes too long, so we pick some
        // random combinations
        for (_ <- 0 until 100) {
          val pattern = gen.pickRandom(startPatterns) +
            gen.pickRandom(patternParts) +
            gen.pickRandom(qualifiers) +
            gen.pickRandom(endPatterns)
          val query = sql(s"select id, name, name rlike '$pattern' from $table")
          checkSparkAnswerAndOperator(query)
        }
      }
    }
  }

  test("contains") {
    val table = "names"
    withTable(table) {
      sql(s"create table $table(id int, name varchar(20)) using parquet")
      sql(s"insert into $table values(1,'James Smith')")
      sql(s"insert into $table values(2,'Michael Rose')")
      sql(s"insert into $table values(3,'Robert Williams')")
      sql(s"insert into $table values(4,'Rames Rose')")
      sql(s"insert into $table values(5,'Rames rose')")

      // Filter rows that contains 'rose' in 'name' column
      val queryContains = sql(s"select id from $table where contains (name, 'rose')")
      checkAnswer(queryContains, Row(5) :: Nil)
    }
  }

  test("startswith") {
    val table = "names"
    withTable(table) {
      sql(s"create table $table(id int, name varchar(20)) using parquet")
      sql(s"insert into $table values(1,'James Smith')")
      sql(s"insert into $table values(2,'Michael Rose')")
      sql(s"insert into $table values(3,'Robert Williams')")
      sql(s"insert into $table values(4,'Rames Rose')")
      sql(s"insert into $table values(5,'Rames rose')")

      // Filter rows that starts with 'R' following by any characters
      val queryStartsWith = sql(s"select id from $table where startswith (name, 'R')")
      checkAnswer(queryStartsWith, Row(3) :: Row(4) :: Row(5) :: Nil)
    }
  }

  test("endswith") {
    val table = "names"
    withTable(table) {
      sql(s"create table $table(id int, name varchar(20)) using parquet")
      sql(s"insert into $table values(1,'James Smith')")
      sql(s"insert into $table values(2,'Michael Rose')")
      sql(s"insert into $table values(3,'Robert Williams')")
      sql(s"insert into $table values(4,'Rames Rose')")
      sql(s"insert into $table values(5,'Rames rose')")

      // Filter rows that ends with 's' following by any characters
      val queryEndsWith = sql(s"select id from $table where endswith (name, 's')")
      checkAnswer(queryEndsWith, Row(3) :: Nil)
    }
  }

  test("add overflow (ANSI disable)") {
    // Enabling ANSI will cause native engine failure, but as we cannot catch
    // native error now, we cannot test it here.
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      withParquetTable(Seq((Int.MaxValue, 1)), "tbl") {
        checkSparkAnswerAndOperator("SELECT _1 + _2 FROM tbl")
      }
    }
  }

  test("divide by zero (ANSI disable)") {
    // Enabling ANSI will cause native engine failure, but as we cannot catch
    // native error now, we cannot test it here.
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      withParquetTable(Seq((1, 0, 1.0, 0.0, -0.0)), "tbl") {
        checkSparkAnswerAndOperator("SELECT _1 / _2, _3 / _4, _3 / _5 FROM tbl")
        checkSparkAnswerAndOperator("SELECT _1 % _2, _3 % _4, _3 % _5 FROM tbl")
        checkSparkAnswerAndOperator("SELECT _1 / 0, _3 / 0.0, _3 / -0.0 FROM tbl")
        checkSparkAnswerAndOperator("SELECT _1 % 0, _3 % 0.0, _3 % -0.0 FROM tbl")
      }
    }
  }

  test("decimals arithmetic and comparison") {
    def makeDecimalRDD(num: Int, decimal: DecimalType, useDictionary: Boolean): DataFrame = {
      val div = if (useDictionary) 5 else num // narrow the space to make it dictionary encoded
      spark
        .range(num)
        .map(_ % div)
        // Parquet doesn't allow column names with spaces, have to add an alias here.
        // Minus 500 here so that negative decimals are also tested.
        .select(
          (($"value" - 500) / 100.0) cast decimal as Symbol("dec1"),
          (($"value" - 600) / 100.0) cast decimal as Symbol("dec2"))
        .coalesce(1)
    }

    Seq(true, false).foreach { dictionary =>
      Seq(16, 1024).foreach { batchSize =>
        withSQLConf(
          CometConf.COMET_BATCH_SIZE.key -> batchSize.toString,
          SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key -> "false",
          "parquet.enable.dictionary" -> dictionary.toString) {
          var combinations = Seq((5, 2), (1, 0), (18, 10), (18, 17), (19, 0), (38, 37))
          // If ANSI mode is on, the combination (1, 1) will cause a runtime error. Otherwise, the
          // decimal RDD contains all null values and should be able to read back from Parquet.

          if (!SQLConf.get.ansiEnabled) {
            combinations = combinations ++ Seq((1, 1))
          }

          for ((precision, scale) <- combinations) {
            withTempPath { dir =>
              val data = makeDecimalRDD(10, DecimalType(precision, scale), dictionary)
              data.write.parquet(dir.getCanonicalPath)
              readParquetFile(dir.getCanonicalPath) { df =>
                {
                  val decimalLiteral1 = Decimal(1.00)
                  val decimalLiteral2 = Decimal(123.456789)
                  val cometDf = df.select(
                    $"dec1" + $"dec2",
                    $"dec1" - $"dec2",
                    $"dec1" % $"dec2",
                    $"dec1" >= $"dec1",
                    $"dec1" === "1.0",
                    $"dec1" + decimalLiteral1,
                    $"dec1" - decimalLiteral1,
                    $"dec1" + decimalLiteral2,
                    $"dec1" - decimalLiteral2)

                  checkAnswer(
                    cometDf,
                    data
                      .select(
                        $"dec1" + $"dec2",
                        $"dec1" - $"dec2",
                        $"dec1" % $"dec2",
                        $"dec1" >= $"dec1",
                        $"dec1" === "1.0",
                        $"dec1" + decimalLiteral1,
                        $"dec1" - decimalLiteral1,
                        $"dec1" + decimalLiteral2,
                        $"dec1" - decimalLiteral2)
                      .collect()
                      .toSeq)
                }
              }
            }
          }
        }
      }
    }
  }

  test("scalar decimal arithmetic operations") {
    withTable("tbl") {
      withSQLConf(CometConf.COMET_ENABLED.key -> "true") {
        sql("CREATE TABLE tbl (a INT) USING PARQUET")
        sql("INSERT INTO tbl VALUES (0)")

        val combinations = Seq((7, 3), (18, 10), (38, 4))
        for ((precision, scale) <- combinations) {
          for (op <- Seq("+", "-", "*", "/", "%")) {
            val left = s"CAST(1.00 AS DECIMAL($precision, $scale))"
            val right = s"CAST(123.45 AS DECIMAL($precision, $scale))"

            withSQLConf(
              "spark.sql.optimizer.excludedRules" ->
                "org.apache.spark.sql.catalyst.optimizer.ConstantFolding") {

              checkSparkAnswerAndOperator(s"SELECT $left $op $right FROM tbl")
            }
          }
        }
      }
    }
  }

  test("cast decimals to int") {
    Seq(16, 1024).foreach { batchSize =>
      withSQLConf(
        CometConf.COMET_BATCH_SIZE.key -> batchSize.toString,
        SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key -> "false") {
        var combinations = Seq((5, 2), (1, 0), (18, 10), (18, 17), (19, 0), (38, 37))
        // If ANSI mode is on, the combination (1, 1) will cause a runtime error. Otherwise, the
        // decimal RDD contains all null values and should be able to read back from Parquet.

        if (!SQLConf.get.ansiEnabled) {
          combinations = combinations ++ Seq((1, 1))
        }

        for ((precision, scale) <- combinations; useDictionary <- Seq(false)) {
          withTempPath { dir =>
            val data = makeDecimalRDD(10, DecimalType(precision, scale), useDictionary)
            data.write.parquet(dir.getCanonicalPath)
            readParquetFile(dir.getCanonicalPath) { df =>
              {
                val cometDf = df.select($"dec".cast("int"))

                // `data` is not read from Parquet, so it doesn't go Comet exec.
                checkAnswer(cometDf, data.select($"dec".cast("int")).collect().toSeq)
              }
            }
          }
        }
      }
    }
  }

  private val doubleValues: Seq[Double] = Seq(
    -1.0,
    // TODO we should eventually enable negative zero but there are known issues still
    // -0.0,
    0.0,
    +1.0,
    Double.MinValue,
    Double.MaxValue,
    Double.NaN,
    Double.MinPositiveValue,
    Double.PositiveInfinity,
    Double.NegativeInfinity)

  test("various math scalar functions") {
    val data = doubleValues.map(n => (n, n))
    withParquetTable(data, "tbl") {
      // expressions with single arg
      for (expr <- Seq(
          "acos",
          "asin",
          "atan",
          "cos",
          "exp",
          "ln",
          "log10",
          "log2",
          "sin",
          "sqrt",
          "tan")) {
        val (_, cometPlan) =
          checkSparkAnswerAndOperatorWithTol(sql(s"SELECT $expr(_1), $expr(_2) FROM tbl"))
        val cometProjectExecs = collect(cometPlan) { case op: CometProjectExec =>
          op
        }
        assert(cometProjectExecs.length == 1, expr)
      }
      // expressions with two args
      for (expr <- Seq("atan2", "pow")) {
        val (_, cometPlan) =
          checkSparkAnswerAndOperatorWithTol(sql(s"SELECT $expr(_1, _2) FROM tbl"))
        val cometProjectExecs = collect(cometPlan) { case op: CometProjectExec =>
          op
        }
        assert(cometProjectExecs.length == 1, expr)
      }
    }
  }

  private def testDoubleScalarExpr(expr: String): Unit = {
    val testValuesRepeated = doubleValues.flatMap(v => Seq.fill(1000)(v))
    for (withDictionary <- Seq(true, false)) {
      withParquetTable(testValuesRepeated.map(n => (n, n)), "tbl", withDictionary) {
        val (_, cometPlan) = checkSparkAnswerAndOperatorWithTol(sql(s"SELECT $expr(_1) FROM tbl"))
        val projections = collect(cometPlan) { case p: CometProjectExec =>
          p
        }
        assert(projections.length == 1)
      }
    }
  }

  test("disable expression using dynamic config") {
    def countSparkProjectExec(plan: SparkPlan) = {
      plan.collect { case _: ProjectExec =>
        true
      }.length
    }
    withParquetTable(Seq(0, 1, 2).map(n => (n, n)), "tbl") {
      val sql = "select _1+_2 from tbl"
      val (_, cometPlan) = checkSparkAnswerAndOperator(sql)
      assert(0 == countSparkProjectExec(cometPlan))
      withSQLConf(CometConf.getExprEnabledConfigKey("Add") -> "false") {
        val (_, cometPlan) = checkSparkAnswerAndFallbackReason(
          sql,
          "Expression support is disabled. Set spark.comet.expression.Add.enabled=true to enable it.")
        assert(1 == countSparkProjectExec(cometPlan))
      }
    }
  }

  test("enable incompat expression using dynamic config") {
    def countSparkProjectExec(plan: SparkPlan) = {
      plan.collect { case _: ProjectExec =>
        true
      }.length
    }
    withParquetTable(Seq(0, 1, 2).map(n => (n.toString, n.toString)), "tbl") {
      val sql = "select initcap(_1) from tbl"
      val (_, cometPlan) = checkSparkAnswer(sql)
      assert(1 == countSparkProjectExec(cometPlan))
      withSQLConf(CometConf.getExprAllowIncompatConfigKey("InitCap") -> "true") {
        val (_, cometPlan) = checkSparkAnswerAndOperator(sql)
        assert(0 == countSparkProjectExec(cometPlan))
      }
    }
  }

  test("signum") {
    testDoubleScalarExpr("signum")
  }

  test("expm1") {
    testDoubleScalarExpr("expm1")
  }

  test("ceil and floor") {
    Seq("true", "false").foreach { dictionary =>
      withSQLConf(
        "parquet.enable.dictionary" -> dictionary,
        CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key -> "true") {
        withParquetTable(
          (-5 until 5).map(i => (i.toDouble + 0.3, i.toDouble + 0.8)),
          "tbl",
          withDictionary = dictionary.toBoolean) {
          checkSparkAnswerAndOperator("SELECT ceil(_1), ceil(_2), floor(_1), floor(_2) FROM tbl")
          checkSparkAnswerAndOperator(
            "SELECT ceil(0.0), ceil(-0.0), ceil(-0.5), ceil(0.5), ceil(-1.2), ceil(1.2) FROM tbl")
          checkSparkAnswerAndOperator(
            "SELECT floor(0.0), floor(-0.0), floor(-0.5), floor(0.5), " +
              "floor(-1.2), floor(1.2) FROM tbl")
        }
        withParquetTable(
          (-5 until 5).map(i => (i.toLong, i.toLong)),
          "tbl",
          withDictionary = dictionary.toBoolean) {
          checkSparkAnswerAndOperator("SELECT ceil(_1), ceil(_2), floor(_1), floor(_2) FROM tbl")
          checkSparkAnswerAndOperator(
            "SELECT ceil(0), ceil(-0), ceil(-5), ceil(5), ceil(-1), ceil(1) FROM tbl")
          checkSparkAnswerAndOperator(
            "SELECT floor(0), floor(-0), floor(-5), floor(5), " +
              "floor(-1), floor(1) FROM tbl")
        }
        withParquetTable(
          (-33L to 33L by 3L).map(i => Tuple1(Decimal(i, 21, 1))), // -3.3 ~ +3.3
          "tbl",
          withDictionary = dictionary.toBoolean) {
          checkSparkAnswerAndOperator("SELECT ceil(_1), floor(_1) FROM tbl")
          checkSparkAnswerAndOperator("SELECT ceil(cast(_1 as decimal(20, 0))) FROM tbl")
          checkSparkAnswerAndOperator("SELECT floor(cast(_1 as decimal(20, 0))) FROM tbl")
          withSQLConf(
            // Exclude the constant folding optimizer in order to actually execute the native ceil
            // and floor operations for scalar (literal) values.
            "spark.sql.optimizer.excludedRules" -> "org.apache.spark.sql.catalyst.optimizer.ConstantFolding") {
            for (n <- Seq("0.0", "-0.0", "0.5", "-0.5", "1.2", "-1.2")) {
              checkSparkAnswerAndOperator(s"SELECT ceil(cast(${n} as decimal(38, 18))) FROM tbl")
              checkSparkAnswerAndOperator(s"SELECT ceil(cast(${n} as decimal(20, 0))) FROM tbl")
              checkSparkAnswerAndOperator(s"SELECT floor(cast(${n} as decimal(38, 18))) FROM tbl")
              checkSparkAnswerAndOperator(s"SELECT floor(cast(${n} as decimal(20, 0))) FROM tbl")
            }
          }
        }
      }
    }
  }

  test("round") {
    // https://github.com/apache/datafusion-comet/issues/1441
    assume(!usingDataSourceExec)
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllPrimitiveTypes(
          path,
          dictionaryEnabled = dictionaryEnabled,
          -128,
          128,
          randomSize = 100)
        // this test requires native_comet scan due to unsigned u8/u16 issue
        withSQLConf(CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_NATIVE_COMET) {
          withParquetTable(path.toString, "tbl") {
            for (s <- Seq(-5, -1, 0, 1, 5, -1000, 1000, -323, -308, 308, -15, 15, -16, 16,
                null)) {
              // array tests
              // TODO: enable test for floats (_6, _7, _8, _13)
              for (c <- Seq(2, 3, 4, 5, 9, 10, 11, 12, 15, 16, 17)) {
                checkSparkAnswerAndOperator(s"select _${c}, round(_${c}, ${s}) FROM tbl")
              }
              // scalar tests
              // Exclude the constant folding optimizer in order to actually execute the native round
              // operations for scalar (literal) values.
              // TODO: comment in the tests for float once supported
              withSQLConf(
                "spark.sql.optimizer.excludedRules" -> "org.apache.spark.sql.catalyst.optimizer.ConstantFolding") {
                for (n <- Seq("0.0", "-0.0", "0.5", "-0.5", "1.2", "-1.2")) {
                  checkSparkAnswerAndOperator(
                    s"select round(cast(${n} as tinyint), ${s}) FROM tbl")
                  // checkSparkAnswerAndCometOperators(s"select round(cast(${n} as float), ${s}) FROM tbl")
                  checkSparkAnswerAndOperator(
                    s"select round(cast(${n} as decimal(38, 18)), ${s}) FROM tbl")
                  checkSparkAnswerAndOperator(
                    s"select round(cast(${n} as decimal(20, 0)), ${s}) FROM tbl")
                }
                // checkSparkAnswer(s"select round(double('infinity'), ${s}) FROM tbl")
                // checkSparkAnswer(s"select round(double('-infinity'), ${s}) FROM tbl")
                // checkSparkAnswer(s"select round(double('NaN'), ${s}) FROM tbl")
                // checkSparkAnswer(
                //   s"select round(double('0.000000000000000000000000000000000001'), ${s}) FROM tbl")
              }
            }
          }
        }
      }
    }
  }

  test("md5") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
        val table = "test"
        withTable(table) {
          sql(s"create table $table(col String) using parquet")
          sql(
            s"insert into $table values ('test1'), ('test1'), ('test2'), ('test2'), (NULL), ('')")
          checkSparkAnswerAndOperator(s"select md5(col) FROM $table")
        }
      }
    }
  }

  test("hex") {
    // https://github.com/apache/datafusion-comet/issues/1441
    assume(!usingDataSourceExec)
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "hex.parquet")
        // this test requires native_comet scan due to unsigned u8/u16 issue
        withSQLConf(CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_NATIVE_COMET) {
          makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = dictionaryEnabled, 10000)
          withParquetTable(path.toString, "tbl") {
            checkSparkAnswerAndOperator(
              "SELECT hex(_1), hex(_2), hex(_3), hex(_4), hex(_5), hex(_6), hex(_7), hex(_8), hex(_9), hex(_10), hex(_11), hex(_12), hex(_13), hex(_14), hex(_15), hex(_16), hex(_17), hex(_18), hex(_19), hex(_20) FROM tbl")
          }
        }
      }
    }
  }

  test("unhex") {
    val table = "unhex_table"
    withTable(table) {
      sql(s"create table $table(col string) using parquet")

      sql(s"""INSERT INTO $table VALUES
        |('537061726B2053514C'),
        |('737472696E67'),
        |('\\0'),
        |(''),
        |('###'),
        |('G123'),
        |('hello'),
        |('A1B'),
        |('0A1B')""".stripMargin)

      checkSparkAnswerAndOperator(s"SELECT unhex(col) FROM $table")
    }
  }

  test("EqualNullSafe should preserve comet filter") {
    Seq("true", "false").foreach(b =>
      withParquetTable(
        data = (0 until 8).map(i => (i, if (i > 5) None else Some(i % 2 == 0))),
        tableName = "tbl",
        withDictionary = b.toBoolean) {
        // IS TRUE
        Seq("SELECT * FROM tbl where _2 is true", "SELECT * FROM tbl where _2 <=> true")
          .foreach(s => checkSparkAnswerAndOperator(s))

        // IS FALSE
        Seq("SELECT * FROM tbl where _2 is false", "SELECT * FROM tbl where _2 <=> false")
          .foreach(s => checkSparkAnswerAndOperator(s))

        // IS NOT TRUE
        Seq("SELECT * FROM tbl where _2 is not true", "SELECT * FROM tbl where not _2 <=> true")
          .foreach(s => checkSparkAnswerAndOperator(s))

        // IS NOT FALSE
        Seq("SELECT * FROM tbl where _2 is not false", "SELECT * FROM tbl where not _2 <=> false")
          .foreach(s => checkSparkAnswerAndOperator(s))
      })
  }

  test("test in(set)/not in(set)") {
    Seq("100", "0").foreach { inSetThreshold =>
      Seq(false, true).foreach { dictionary =>
        withSQLConf(
          SQLConf.OPTIMIZER_INSET_CONVERSION_THRESHOLD.key -> inSetThreshold,
          "parquet.enable.dictionary" -> dictionary.toString) {
          val table = "names"
          withTable(table) {
            sql(s"create table $table(id int, name varchar(20)) using parquet")
            sql(
              s"insert into $table values(1, 'James'), (1, 'Jones'), (2, 'Smith'), (3, 'Smith')," +
                "(NULL, 'Jones'), (4, NULL)")

            checkSparkAnswerAndOperator(s"SELECT * FROM $table WHERE id in (1, 2, 4, NULL)")
            checkSparkAnswerAndOperator(
              s"SELECT * FROM $table WHERE name in ('Smith', 'Brown', NULL)")

            // TODO: why with not in, the plan is only `LocalTableScan`?
            checkSparkAnswerAndOperator(s"SELECT * FROM $table WHERE id not in (1)")
            checkSparkAnswer(s"SELECT * FROM $table WHERE name not in ('Smith', 'Brown', NULL)")
          }
        }
      }
    }
  }

  test("case_when") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
        val table = "test"
        withTable(table) {
          sql(s"create table $table(id int) using parquet")
          sql(s"insert into $table values(1), (NULL), (2), (2), (3), (3), (4), (5), (NULL)")
          checkSparkAnswerAndOperator(
            s"SELECT CASE WHEN id > 2 THEN 3333 WHEN id > 1 THEN 2222 ELSE 1111 END FROM $table")
          checkSparkAnswerAndOperator(
            s"SELECT CASE WHEN id > 2 THEN NULL WHEN id > 1 THEN 2222 ELSE 1111 END FROM $table")
          checkSparkAnswerAndOperator(
            s"SELECT CASE id WHEN 1 THEN 1111 WHEN 2 THEN 2222 ELSE 3333 END FROM $table")
          checkSparkAnswerAndOperator(
            s"SELECT CASE id WHEN 1 THEN 1111 WHEN 2 THEN 2222 ELSE NULL END FROM $table")
          checkSparkAnswerAndOperator(
            s"SELECT CASE id WHEN 1 THEN 1111 WHEN 2 THEN 2222 WHEN 3 THEN 3333 WHEN 4 THEN 4444 END FROM $table")
          checkSparkAnswerAndOperator(
            s"SELECT CASE id WHEN NULL THEN 0 WHEN 1 THEN 1111 WHEN 2 THEN 2222 ELSE 3333 END FROM $table")
        }
      }
    }
  }

  test("not") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
        val table = "test"
        withTable(table) {
          sql(s"create table $table(col1 int, col2 boolean) using parquet")
          sql(s"insert into $table values(1, false), (2, true), (3, true), (3, false)")
          checkSparkAnswerAndOperator(s"SELECT col1, col2, NOT(col2), !(col2) FROM $table")
        }
      }
    }
  }

  test("negative") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
        val table = "test"
        withTable(table) {
          sql(s"create table $table(col1 int) using parquet")
          sql(s"insert into $table values(1), (2), (3), (3)")
          checkSparkAnswerAndOperator(s"SELECT negative(col1), -(col1) FROM $table")
        }
      }
    }
  }

  test("conditional expressions") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
        val table = "test1"
        withTable(table) {
          sql(s"create table $table(c1 int, c2 string, c3 int) using parquet")
          sql(
            s"insert into $table values(1, 'comet', 1), (2, 'comet', 3), (null, 'spark', 4)," +
              " (null, null, 4), (2, 'spark', 3), (2, 'comet', 3)")
          checkSparkAnswerAndOperator(s"SELECT if (c1 < 2, 1111, 2222) FROM $table")
          checkSparkAnswerAndOperator(s"SELECT if (c1 < c3, 1111, 2222) FROM $table")
          checkSparkAnswerAndOperator(
            s"SELECT if (c2 == 'comet', 'native execution', 'non-native execution') FROM $table")
        }
      }
    }
  }

  test("basic arithmetic") {
    withSQLConf("parquet.enable.dictionary" -> "false") {
      withParquetTable((1 until 10).map(i => (i, i + 1)), "tbl", false) {
        checkSparkAnswerAndOperator("SELECT _1 + _2, _1 - _2, _1 * _2, _1 / _2, _1 % _2 FROM tbl")
      }
    }

    withSQLConf("parquet.enable.dictionary" -> "false") {
      withParquetTable((1 until 10).map(i => (i.toFloat, i.toFloat + 0.5)), "tbl", false) {
        checkSparkAnswerAndOperator("SELECT _1 + _2, _1 - _2, _1 * _2, _1 / _2, _1 % _2 FROM tbl")
      }
    }

    withSQLConf("parquet.enable.dictionary" -> "false") {
      withParquetTable((1 until 10).map(i => (i.toDouble, i.toDouble + 0.5d)), "tbl", false) {
        checkSparkAnswerAndOperator("SELECT _1 + _2, _1 - _2, _1 * _2, _1 / _2, _1 % _2 FROM tbl")
      }
    }
  }

  test("date partition column does not forget date type") {
    withTable("t1") {
      sql("CREATE TABLE t1(flag LONG, cal_dt DATE) USING PARQUET PARTITIONED BY (cal_dt)")
      sql("""
            |INSERT INTO t1 VALUES
            |(2, date'2021-06-27'),
            |(2, date'2021-06-28'),
            |(2, date'2021-06-29'),
            |(2, date'2021-06-30')""".stripMargin)
      checkSparkAnswerAndOperator(sql("SELECT CAST(cal_dt as STRING) FROM t1"))
      checkSparkAnswer("SHOW PARTITIONS t1")
    }
  }

  test("DatePart functions: Year/Month/DayOfMonth/DayOfWeek/DayOfYear/WeekOfYear/Quarter") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
        val table = "test"
        withTable(table) {
          sql(s"create table $table(col timestamp) using parquet")
          sql(s"insert into $table values (now()), (timestamp('1900-01-01')), (null)")
          checkSparkAnswerAndOperator(
            "SELECT col, year(col), month(col), day(col), weekday(col), " +
              s" dayofweek(col), dayofyear(col), weekofyear(col), quarter(col) FROM $table")
        }
      }
    }
  }

  test("from_unixtime") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf(
        "parquet.enable.dictionary" -> dictionary.toString,
        CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key -> "true") {
        val table = "test"
        withTempDir { dir =>
          val path = new Path(dir.toURI.toString, "test.parquet")
          makeParquetFileAllPrimitiveTypes(
            path,
            dictionaryEnabled = dictionary,
            -128,
            128,
            randomSize = 100)
          withParquetTable(path.toString, table) {
            // TODO: DataFusion supports only -8334601211038 <= sec <= 8210266876799
            // https://github.com/apache/datafusion/issues/16594
            // After fixing this issue, remove the where clause below
            val where = "where _5 BETWEEN -8334601211038 AND 8210266876799"
            checkSparkAnswerAndOperator(s"SELECT from_unixtime(_5) FROM $table $where")
            checkSparkAnswerAndOperator(s"SELECT from_unixtime(_8) FROM $table $where")
            // TODO: DataFusion toChar does not support Spark datetime pattern format
            // https://github.com/apache/datafusion/issues/16577
            // https://github.com/apache/datafusion/issues/14536
            // After fixing these issues, change checkSparkAnswer to checkSparkAnswerAndOperator
            checkSparkAnswer(s"SELECT from_unixtime(_5, 'yyyy') FROM $table $where")
            checkSparkAnswer(s"SELECT from_unixtime(_8, 'yyyy') FROM $table $where")
            withSQLConf(SESSION_LOCAL_TIMEZONE.key -> "Asia/Kathmandu") {
              checkSparkAnswerAndOperator(s"SELECT from_unixtime(_5) FROM $table $where")
              checkSparkAnswerAndOperator(s"SELECT from_unixtime(_8) FROM $table $where")
              checkSparkAnswer(s"SELECT from_unixtime(_5, 'yyyy') FROM $table $where")
              checkSparkAnswer(s"SELECT from_unixtime(_8, 'yyyy') FROM $table $where")
            }
          }
        }
      }
    }
  }

  test("Decimal binary ops multiply is aligned to Spark") {
    Seq(true, false).foreach { allowPrecisionLoss =>
      withSQLConf(
        "spark.sql.decimalOperations.allowPrecisionLoss" -> allowPrecisionLoss.toString) {

        testSingleLineQuery(
          "select cast(1.23456 as decimal(10,9)) c1, cast(2.345678 as decimal(10,9)) c2",
          "select a, b, typeof(a), typeof(b) from (select c1 * 2.345678 a, c2 * c1 b from tbl)",
          s"basic_positive_numbers (allowPrecisionLoss = ${allowPrecisionLoss})")

        testSingleLineQuery(
          "select cast(1.23456 as decimal(10,9)) c1, cast(-2.345678 as decimal(10,9)) c2",
          "select a, b, typeof(a), typeof(b) from (select c1 * -2.345678 a, c2 * c1 b from tbl)",
          s"basic_neg_numbers (allowPrecisionLoss = ${allowPrecisionLoss})")

        testSingleLineQuery(
          "select cast(1.23456 as decimal(10,9)) c1, cast(0 as decimal(10,9)) c2",
          "select a, b, typeof(a), typeof(b) from (select c1 * 0.0 a, c2 * c1 b from tbl)",
          s"zero (allowPrecisionLoss = ${allowPrecisionLoss})")

        testSingleLineQuery(
          "select cast(1.23456 as decimal(10,9)) c1, cast(1 as decimal(10,9)) c2",
          "select a, b, typeof(a), typeof(b) from (select c1 * 1.0 a, c2 * c1 b from tbl)",
          s"identity (allowPrecisionLoss = ${allowPrecisionLoss})")

        testSingleLineQuery(
          "select cast(123456789.1234567890 as decimal(20,10)) c1, cast(987654321.9876543210 as decimal(20,10)) c2",
          "select a, b, typeof(a), typeof(b) from (select c1 * cast(987654321.9876543210 as decimal(20,10)) a, c2 * c1 b from tbl)",
          s"large_numbers (allowPrecisionLoss = ${allowPrecisionLoss})")

        testSingleLineQuery(
          "select cast(0.00000000123456789 as decimal(20,19)) c1, cast(0.00000000987654321 as decimal(20,19)) c2",
          "select a, b, typeof(a), typeof(b) from (select c1 * cast(0.00000000987654321 as decimal(20,19)) a, c2 * c1 b from tbl)",
          s"small_numbers (allowPrecisionLoss = ${allowPrecisionLoss})")

        testSingleLineQuery(
          "select cast(64053151420411946063694043751862251568 as decimal(38,0)) c1, cast(12345 as decimal(10,0)) c2",
          "select a, b, typeof(a), typeof(b) from (select c1 * cast(12345 as decimal(10,0)) a, c2 * c1 b from tbl)",
          s"overflow_precision (allowPrecisionLoss = ${allowPrecisionLoss})")

        testSingleLineQuery(
          "select cast(6.4053151420411946063694043751862251568 as decimal(38,37)) c1, cast(1.2345 as decimal(10,9)) c2",
          "select a, b, typeof(a), typeof(b) from (select c1 * cast(1.2345 as decimal(10,9)) a, c2 * c1 b from tbl)",
          s"overflow_scale (allowPrecisionLoss = ${allowPrecisionLoss})")

        testSingleLineQuery(
          """
            |select cast(6.4053151420411946063694043751862251568 as decimal(38,37)) c1, cast(1.2345 as decimal(10,9)) c2
            |union all
            |select cast(1.23456 as decimal(10,9)) c1, cast(1 as decimal(10,9)) c2
            |""".stripMargin,
          "select a, typeof(a) from (select c1 * c2 a from tbl)",
          s"mixed_errs_and_results (allowPrecisionLoss = ${allowPrecisionLoss})")
      }
    }
  }

  test("Decimal random number tests") {
    val rand = scala.util.Random
    def makeNum(p: Int, s: Int): String = {
      val int1 = rand.nextLong()
      val int2 = rand.nextLong().abs
      val frac1 = rand.nextLong().abs
      val frac2 = rand.nextLong().abs
      s"$int1$int2".take(p - s + (int1 >>> 63).toInt) + "." + s"$frac1$frac2".take(s)
    }

    val table = "test"
    (0 until 10).foreach { _ =>
      val p1 = rand.nextInt(38) + 1 // 1 <= p1 <= 38
      val s1 = rand.nextInt(p1 + 1) // 0 <= s1 <= p1
      val p2 = rand.nextInt(38) + 1
      val s2 = rand.nextInt(p2 + 1)

      withTable(table) {
        sql(s"create table $table(a decimal($p1, $s1), b decimal($p2, $s2)) using parquet")
        val values =
          (0 until 10).map(_ => s"(${makeNum(p1, s1)}, ${makeNum(p2, s2)})").mkString(",")
        sql(s"insert into $table values $values")
        Seq(true, false).foreach { allowPrecisionLoss =>
          withSQLConf(
            "spark.sql.decimalOperations.allowPrecisionLoss" -> allowPrecisionLoss.toString) {
            val a = makeNum(p1, s1)
            val b = makeNum(p2, s2)
            val ops = Seq("+", "-", "*", "/", "%", "div")
            for (op <- ops) {
              checkSparkAnswerAndOperator(s"select a, b, a $op b from $table")
              checkSparkAnswerAndOperator(s"select $a, b, $a $op b from $table")
              checkSparkAnswerAndOperator(s"select a, $b, a $op $b from $table")
              checkSparkAnswerAndOperator(
                s"select $a, $b, decimal($a) $op decimal($b) from $table")
            }
          }
        }
      }
    }
  }

  test("test cast utf8 to boolean as compatible with Spark") {
    def testCastedColumn(inputValues: Seq[String]): Unit = {
      val table = "test_table"
      withTable(table) {
        val values = inputValues.map(x => s"('$x')").mkString(",")
        sql(s"create table $table(base_column char(20)) using parquet")
        sql(s"insert into $table values $values")
        checkSparkAnswerAndOperator(
          s"select base_column, cast(base_column as boolean) as casted_column from $table")
      }
    }

    // Supported boolean values as true by both Arrow and Spark
    testCastedColumn(inputValues = Seq("t", "true", "y", "yes", "1", "T", "TrUe", "Y", "YES"))
    // Supported boolean values as false by both Arrow and Spark
    testCastedColumn(inputValues = Seq("f", "false", "n", "no", "0", "F", "FaLSe", "N", "No"))
    // Supported boolean values by Arrow but not Spark
    testCastedColumn(inputValues =
      Seq("TR", "FA", "tr", "tru", "ye", "on", "fa", "fal", "fals", "of", "off"))
    // Invalid boolean casting values for Arrow and Spark
    testCastedColumn(inputValues = Seq("car", "Truck"))
  }

  test("explain comet") {
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "false",
      SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true",
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "false",
      EXTENDED_EXPLAIN_PROVIDERS_KEY -> "org.apache.comet.ExtendedExplainInfo") {
      val table = "test"
      withTable(table) {
        sql(s"create table $table(c0 int, c1 int , c2 float) using parquet")
        sql(s"insert into $table values(0, 1, 100.000001)")

        Seq(
          (
            s"SELECT cast(make_interval(c0, c1, c0, c1, c0, c0, c2) as string) as C from $table",
            Set("Cast from CalendarIntervalType to StringType is not supported")),
          (
            "SELECT "
              + "date_part('YEAR', make_interval(c0, c1, c0, c1, c0, c0, c2))"
              + " + "
              + "date_part('MONTH', make_interval(c0, c1, c0, c1, c0, c0, c2))"
              + s" as yrs_and_mths from $table",
            Set(
              "extractintervalyears is not supported",
              "extractintervalmonths is not supported")),
          (
            s"SELECT sum(c0), sum(c2) from $table group by c1",
            Set("Comet shuffle is not enabled: spark.comet.exec.shuffle.enabled is not enabled")),
          (
            "SELECT A.c1, A.sum_c0, A.sum_c2, B.casted from "
              + s"(SELECT c1, sum(c0) as sum_c0, sum(c2) as sum_c2 from $table group by c1) as A, "
              + s"(SELECT c1, cast(make_interval(c0, c1, c0, c1, c0, c0, c2) as string) as casted from $table) as B "
              + "where A.c1 = B.c1 ",
            Set(
              "Cast from CalendarIntervalType to StringType is not supported",
              "Comet shuffle is not enabled: spark.comet.exec.shuffle.enabled is not enabled")),
          (s"select * from $table LIMIT 10 OFFSET 3", Set("Comet shuffle is not enabled")))
          .foreach(test => {
            val qry = test._1
            val expected = test._2
            val df = sql(qry)
            df.collect() // force an execution
            checkSparkAnswerAndFallbackReasons(df, expected)
          })
      }
    }
  }

  test("explain: CollectLimit disabled") {
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_EXEC_COLLECT_LIMIT_ENABLED.key -> "false",
      EXTENDED_EXPLAIN_PROVIDERS_KEY -> "org.apache.comet.ExtendedExplainInfo") {
      val table = "test"
      withTable(table) {
        sql(s"create table $table(c0 int, c1 int , c2 float) using parquet")
        sql(s"insert into $table values(0, 1, 100.000001)")
        Seq(
          (
            s"select * from $table LIMIT 10",
            Set("spark.comet.exec.collectLimit.enabled is false")))
          .foreach(test => {
            val qry = test._1
            val expected = test._2
            val df = sql(qry)
            df.collect() // force an execution
            checkSparkAnswerAndFallbackReasons(df, expected)
          })
      }
    }
  }

  test("hash functions") {
    Seq(true, false).foreach { dictionary =>
      withSQLConf(
        "parquet.enable.dictionary" -> dictionary.toString,
        CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key -> "true") {
        val table = "test"
        withTable(table) {
          sql(s"create table $table(col string, a int, b float) using parquet")
          sql(s"""
              |insert into $table values
              |('Spark SQL  ', 10, 1.2), (NULL, NULL, NULL), ('', 0, 0.0), ('苹果手机', NULL, 3.999999)
              |, ('Spark SQL  ', 10, 1.2), (NULL, NULL, NULL), ('', 0, 0.0), ('苹果手机', NULL, 3.999999)
              |""".stripMargin)
          checkSparkAnswerAndOperator("""
              |select
              |md5(col), md5(cast(a as string)), md5(cast(b as string)),
              |hash(col), hash(col, 1), hash(col, 0), hash(col, a, b), hash(b, a, col),
              |xxhash64(col), xxhash64(col, 1), xxhash64(col, 0), xxhash64(col, a, b), xxhash64(b, a, col),
              |sha2(col, 0), sha2(col, 256), sha2(col, 224), sha2(col, 384), sha2(col, 512), sha2(col, 128), sha2(col, -1),
              |sha1(col), sha1(cast(a as string)), sha1(cast(b as string))
              |from test
              |""".stripMargin)
        }
      }
    }
  }

  test("remainder function") {
    def withAnsiMode(enabled: Boolean)(f: => Unit): Unit = {
      withSQLConf(
        SQLConf.ANSI_ENABLED.key -> enabled.toString,
        CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key -> enabled.toString,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true")(f)
    }

    def verifyResult(query: String): Unit = {
      val expectedDivideByZeroError =
        "[DIVIDE_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor being 0 and return NULL instead."

      checkSparkMaybeThrows(sql(query)) match {
        case (Some(sparkException), Some(cometException)) =>
          assert(sparkException.getMessage.contains(expectedDivideByZeroError))
          assert(cometException.getMessage.contains(expectedDivideByZeroError))
        case (None, None) => checkSparkAnswerAndOperator(sql(query))
        case (None, Some(ex)) =>
          fail("Comet threw an exception but Spark did not. Comet exception: " + ex.getMessage)
        case (Some(sparkException), None) =>
          fail(
            "Spark threw an exception but Comet did not. Spark exception: " +
              sparkException.getMessage)
      }
    }

    val tableName = "remainder_table"
    withTable(tableName) {
      sql(s"""
          |create table $tableName (
          |a_int int,
          |b_int int,
          |a_float float,
          |b_float float,
          |a_decimal decimal(18,4),
          |b_decimal decimal(18,4)
          |) using parquet
          """.stripMargin)

      val minInt = Int.MinValue

      sql(s"""
          |insert into $tableName values
          |(3, 0, 3.0, 0.0, cast(3 as decimal(18,4)), cast(0 as decimal(18,4))),
          |(10, 3, 10.5, 3.0, cast(10 as decimal(18,4)), cast(3 as decimal(18,4))),
          |(-10, 3, -10.5, 3.0, cast(-10 as decimal(18,4)), cast(3 as decimal(18,4))),
          |($minInt, -1, $minInt, -1.0, cast($minInt as decimal(18,4)), cast(-1 as decimal(18,4))),
          |(null, 3, null, 3.0, null, cast(3 as decimal(18,4))),
          |(3, null, 3.0, null, cast(3 as decimal(18,4)), null)
          """.stripMargin)

      Seq(true, false).foreach(enabled => {
        withAnsiMode(enabled = enabled) {
          verifyResult(s"""
          |select
          |a_int, b_int, a_float, b_float, a_decimal, b_decimal,
          |a_int % b_int as int_int_modulo,
          |a_int % b_float as int_float_modulo,
          |mod(a_int, b_int) as int_int_mod,
          |mod(a_int, b_float) as int_float_mod,
          |a_float % b_float as float_float_modulo,
          |a_float % b_decimal as float_decimal_modulo,
          |mod(a_float, b_float) as float_float_mod,
          |mod(a_float, b_decimal) as float_decimal_mod,
          |a_decimal % b_decimal as decimal_decimal_modulo,
          |mod(a_decimal, b_decimal) as decimal_decimal_mod
          |from $tableName
          """.stripMargin)
        }
      })
    }
  }

  test("hash functions with random input") {
    val dataGen = DataGenerator.DEFAULT
    // sufficient number of rows to create dictionary encoded ArrowArray.
    val randomNumRows = 1000

    val whitespaceChars = " \t\r\n"
    val timestampPattern = "0123456789/:T" + whitespaceChars
    Seq(true, false).foreach { dictionary =>
      withSQLConf(
        "parquet.enable.dictionary" -> dictionary.toString,
        CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key -> "true") {
        val table = "test"
        withTable(table) {
          sql(s"create table $table(col string, a int, b float) using parquet")
          val tableSchema = spark.table(table).schema
          val rows = dataGen.generateRows(
            randomNumRows,
            tableSchema,
            Some(() => dataGen.generateString(timestampPattern, 6)))
          val data = spark.createDataFrame(spark.sparkContext.parallelize(rows), tableSchema)
          data.write
            .mode("append")
            .insertInto(table)
          // with random generated data
          // disable cast(b as string) for now, as the cast from float to string may produce incompatible result
          checkSparkAnswerAndOperator("""
              |select
              |md5(col), md5(cast(a as string)), --md5(cast(b as string)),
              |hash(col), hash(col, 1), hash(col, 0), hash(col, a, b), hash(b, a, col),
              |xxhash64(col), xxhash64(col, 1), xxhash64(col, 0), xxhash64(col, a, b), xxhash64(b, a, col),
              |sha2(col, 0), sha2(col, 256), sha2(col, 224), sha2(col, 384), sha2(col, 512), sha2(col, 128), sha2(col, -1),
              |sha1(col), sha1(cast(a as string))
              |from test
              |""".stripMargin)
        }
      }
    }
  }

  test("hash function with decimal input") {
    val testPrecisionScales: Seq[(Int, Int)] = Seq(
      (1, 0),
      (17, 2),
      (18, 2),
      (19, 2),
      (DecimalType.MAX_PRECISION, DecimalType.MAX_SCALE - 1))
    for ((p, s) <- testPrecisionScales) {
      withTable("t1") {
        sql(s"create table t1(c1 decimal($p, $s)) using parquet")
        sql("insert into t1 values(1.23), (-1.23), (0.0), (null)")
        if (p <= 18) {
          checkSparkAnswerAndOperator("select c1, hash(c1) from t1 order by c1")
        } else {
          // not supported natively yet
          checkSparkAnswer("select c1, hash(c1) from t1 order by c1")
        }
      }
    }
  }

  test("xxhash64 function with decimal input") {
    val testPrecisionScales: Seq[(Int, Int)] = Seq(
      (1, 0),
      (17, 2),
      (18, 2),
      (19, 2),
      (DecimalType.MAX_PRECISION, DecimalType.MAX_SCALE - 1))
    for ((p, s) <- testPrecisionScales) {
      withTable("t1") {
        sql(s"create table t1(c1 decimal($p, $s)) using parquet")
        sql("insert into t1 values(1.23), (-1.23), (0.0), (null)")
        if (p <= 18) {
          checkSparkAnswerAndOperator("select c1, xxhash64(c1) from t1 order by c1")
        } else {
          // not supported natively yet
          checkSparkAnswer("select c1, xxhash64(c1) from t1 order by c1")
        }
      }
    }
  }

  test("unary negative integer overflow test") {
    def withAnsiMode(enabled: Boolean)(f: => Unit): Unit = {
      withSQLConf(
        SQLConf.ANSI_ENABLED.key -> enabled.toString,
        CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key -> enabled.toString,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true")(f)
    }

    def checkOverflow(query: String, dtype: String): Unit = {
      checkSparkMaybeThrows(sql(query)) match {
        case (Some(sparkException), Some(cometException)) =>
          assert(sparkException.getMessage.contains(dtype + " overflow"))
          assert(cometException.getMessage.contains(dtype + " overflow"))
        case (None, None) => checkSparkAnswerAndOperator(sql(query))
        case (None, Some(ex)) =>
          fail("Comet threw an exception but Spark did not " + ex.getMessage)
        case (Some(_), None) =>
          fail("Spark threw an exception but Comet did not")
      }
    }

    def runArrayTest(query: String, dtype: String, path: String): Unit = {
      withParquetTable(path, "t") {
        withAnsiMode(enabled = false) {
          checkSparkAnswerAndOperator(sql(query))
        }
        withAnsiMode(enabled = true) {
          checkOverflow(query, dtype)
        }
      }
    }

    withTempDir { dir =>
      // Array values test
      val dataTypes = Seq(
        ("array_test.parquet", Seq(Int.MaxValue, Int.MinValue).toDF("a"), "integer"),
        ("long_array_test.parquet", Seq(Long.MaxValue, Long.MinValue).toDF("a"), "long"),
        ("short_array_test.parquet", Seq(Short.MaxValue, Short.MinValue).toDF("a"), ""),
        ("byte_array_test.parquet", Seq(Byte.MaxValue, Byte.MinValue).toDF("a"), ""))

      dataTypes.foreach { case (fileName, df, dtype) =>
        val path = new Path(dir.toURI.toString, fileName).toString
        df.write.mode("overwrite").parquet(path)
        val query = "select a, -a from t"
        runArrayTest(query, dtype, path)
      }

      withParquetTable((0 until 5).map(i => (i % 5, i % 3)), "tbl") {
        withAnsiMode(enabled = true) {
          // interval test without cast
          val longDf = Seq(Long.MaxValue, Long.MaxValue, 2)
          val yearMonthDf = Seq(Int.MaxValue, Int.MaxValue, 2)
            .map(Period.ofMonths)
          val dayTimeDf = Seq(106751991L, 106751991L, 2L)
            .map(Duration.ofDays)
          Seq(longDf, yearMonthDf, dayTimeDf).foreach { _ =>
            checkOverflow("select -(_1) FROM tbl", "")
          }
        }
      }

      // scalar tests
      withParquetTable((0 until 5).map(i => (i % 5, i % 3)), "tbl") {
        withSQLConf(
          "spark.sql.optimizer.excludedRules" -> "org.apache.spark.sql.catalyst.optimizer.ConstantFolding",
          SQLConf.ANSI_ENABLED.key -> "true",
          CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key -> "true",
          CometConf.COMET_ENABLED.key -> "true",
          CometConf.COMET_EXEC_ENABLED.key -> "true") {
          for (n <- Seq("2147483647", "-2147483648")) {
            checkOverflow(s"select -(cast(${n} as int)) FROM tbl", "integer")
          }
          for (n <- Seq("32767", "-32768")) {
            checkOverflow(s"select -(cast(${n} as short)) FROM tbl", "")
          }
          for (n <- Seq("127", "-128")) {
            checkOverflow(s"select -(cast(${n} as byte)) FROM tbl", "")
          }
          for (n <- Seq("9223372036854775807", "-9223372036854775808")) {
            checkOverflow(s"select -(cast(${n} as long)) FROM tbl", "long")
          }
          for (n <- Seq("3.4028235E38", "-3.4028235E38")) {
            checkOverflow(s"select -(cast(${n} as float)) FROM tbl", "float")
          }
        }
      }
    }
  }

  test("readSidePadding") {
    // https://stackoverflow.com/a/46290728
    val table = "test"
    withTable(table) {
      sql(s"create table $table(col1 CHAR(2)) using parquet")
      sql(s"insert into $table values('é')") // unicode 'e\\u{301}'
      sql(s"insert into $table values('é')") // unicode '\\u{e9}'
      sql(s"insert into $table values('')")
      sql(s"insert into $table values('ab')")

      checkSparkAnswerAndOperator(s"SELECT * FROM $table")
    }
  }

  test("isnan") {
    Seq("true", "false").foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary) {
        withParquetTable(
          Seq(Some(1.0), Some(Double.NaN), None).map(i => Tuple1(i)),
          "tbl",
          withDictionary = dictionary.toBoolean) {
          checkSparkAnswerAndOperator("SELECT isnan(_1), isnan(cast(_1 as float)) FROM tbl")
          // Use inside a nullable statement to make sure isnan has correct behavior for null input
          checkSparkAnswerAndOperator(
            "SELECT CASE WHEN (_1 > 0) THEN NULL ELSE isnan(_1) END FROM tbl")
        }
      }
    }
  }

  test("named_struct") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = dictionaryEnabled, 10000)
        withParquetTable(path.toString, "tbl") {
          checkSparkAnswerAndOperator("SELECT named_struct('a', _1, 'b', _2) FROM tbl")
          checkSparkAnswerAndOperator("SELECT named_struct('a', _1, 'b', 2) FROM tbl")
          checkSparkAnswerAndOperator(
            "SELECT named_struct('a', named_struct('b', _1, 'c', _2)) FROM tbl")
        }
      }
    }
  }

  test("named_struct with duplicate field names") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = dictionaryEnabled, 10000)
        withParquetTable(path.toString, "tbl") {
          checkSparkAnswerAndOperator(
            "SELECT named_struct('a', _1, 'a', _2) FROM tbl",
            classOf[ProjectExec])
          checkSparkAnswerAndOperator(
            "SELECT named_struct('a', _1, 'a', 2) FROM tbl",
            classOf[ProjectExec])
          checkSparkAnswerAndOperator(
            "SELECT named_struct('a', named_struct('b', _1, 'b', _2)) FROM tbl",
            classOf[ProjectExec])
        }
      }
    }
  }

  test("to_json") {
    // TODO fix for Spark 4.0.0
    assume(!isSpark40Plus)
    Seq(true, false).foreach { dictionaryEnabled =>
      withParquetTable(
        (0 until 100).map(i => {
          val str = if (i % 2 == 0) {
            "even"
          } else {
            "odd"
          }
          (i.toByte, i.toShort, i, i.toLong, i * 1.2f, -i * 1.2d, str, i.toString)
        }),
        "tbl",
        withDictionary = dictionaryEnabled) {

        val fields = Range(1, 8).map(n => s"'col$n', _$n").mkString(", ")

        checkSparkAnswerAndOperator(s"SELECT to_json(named_struct($fields)) FROM tbl")
        checkSparkAnswerAndOperator(
          s"SELECT to_json(named_struct('nested', named_struct($fields))) FROM tbl")
      }
    }
  }

  test("to_json escaping of field names and string values") {
    // TODO fix for Spark 4.0.0
    assume(!isSpark40Plus)
    val gen = new DataGenerator(new Random(42))
    val chars = "\\'\"abc\t\r\n\f\b"
    Seq(true, false).foreach { dictionaryEnabled =>
      withParquetTable(
        (0 until 100).map(i => {
          val str1 = gen.generateString(chars, 8)
          val str2 = gen.generateString(chars, 8)
          (i.toString, str1, str2)
        }),
        "tbl",
        withDictionary = dictionaryEnabled) {

        val fields = Range(1, 3)
          .map(n => {
            val columnName = s"""column "$n""""
            s"'$columnName', _$n"
          })
          .mkString(", ")

        checkSparkAnswerAndOperator(
          """SELECT 'column "1"' x, """ +
            s"to_json(named_struct($fields)) FROM tbl ORDER BY x")
      }
    }
  }

  test("to_json unicode") {
    // TODO fix for Spark 4.0.0
    assume(!isSpark40Plus)
    Seq(true, false).foreach { dictionaryEnabled =>
      withParquetTable(
        (0 until 100).map(i => {
          (i.toString, "\uD83E\uDD11", "\u018F")
        }),
        "tbl",
        withDictionary = dictionaryEnabled) {

        val fields = Range(1, 3)
          .map(n => {
            val columnName = s"""column "$n""""
            s"'$columnName', _$n"
          })
          .mkString(", ")

        checkSparkAnswerAndOperator(
          """SELECT 'column "1"' x, """ +
            s"to_json(named_struct($fields)) FROM tbl ORDER BY x")
      }
    }
  }

  test("struct and named_struct with dictionary") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withParquetTable(
        (0 until 100).map(i =>
          (
            i,
            if (i % 2 == 0) { "even" }
            else { "odd" })),
        "tbl",
        withDictionary = dictionaryEnabled) {
        checkSparkAnswerAndOperator("SELECT struct(_1, _2) FROM tbl")
        checkSparkAnswerAndOperator("SELECT named_struct('a', _1, 'b', _2) FROM tbl")
      }
    }
  }

  test("get_struct_field") {
    Seq("", "parquet").foreach { v1List =>
      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> v1List,
        CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "false",
        CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.key -> "true") {
        withTempPath { dir =>
          var df = spark
            .range(5)
            // Add both a null struct and null inner value
            .select(
              when(
                col("id") > 1,
                struct(
                  when(col("id") > 2, col("id")).alias("id"),
                  when(col("id") > 2, struct(when(col("id") > 3, col("id")).alias("id")))
                    .as("nested2")))
                .alias("nested1"))

          df.write.parquet(dir.toString())

          df = spark.read.parquet(dir.toString())
          checkSparkAnswerAndOperator(df.select("nested1.id"))
          checkSparkAnswerAndOperator(df.select("nested1.nested2.id"))
        }
      }
    }
  }

  test("get_struct_field - select primitive fields") {
    val scanImpl = CometConf.COMET_NATIVE_SCAN_IMPL.get()
    assume(!(scanImpl == CometConf.SCAN_AUTO && CometSparkSessionExtensions.isSpark40Plus))
    withTempPath { dir =>
      // create input file with Comet disabled
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        val df = spark
          .range(5)
          // Add both a null struct and null inner value
          .select(when(col("id") > 1, struct(when(col("id") > 2, col("id")).alias("id")))
            .alias("nested1"))

        df.write.parquet(dir.toString())
      }
      val df = spark.read.parquet(dir.toString()).select("nested1.id")
      // Comet's original scan does not support structs.
      // The plan will have a Comet Scan only if scan impl is native_full or native_recordbatch
      if (!scanImpl.equals(CometConf.SCAN_NATIVE_COMET)) {
        checkSparkAnswerAndOperator(df)
      } else {
        checkSparkAnswer(df)
      }
    }
  }

  test("get_struct_field - select subset of struct") {
    val scanImpl = CometConf.COMET_NATIVE_SCAN_IMPL.get()
    assume(!(scanImpl == CometConf.SCAN_AUTO && CometSparkSessionExtensions.isSpark40Plus))
    withTempPath { dir =>
      // create input file with Comet disabled
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        val df = spark
          .range(5)
          // Add both a null struct and null inner value
          .select(
            when(
              col("id") > 1,
              struct(
                when(col("id") > 2, col("id")).alias("id"),
                when(col("id") > 2, struct(when(col("id") > 3, col("id")).alias("id")))
                  .as("nested2")))
              .alias("nested1"))

        df.write.parquet(dir.toString())
      }

      val df = spark.read.parquet(dir.toString())
      // Comet's original scan does not support structs.
      // The plan will have a Comet Scan only if scan impl is native_full or native_recordbatch
      if (scanImpl != CometConf.SCAN_NATIVE_COMET) {
        checkSparkAnswerAndOperator(df.select("nested1.id"))
        checkSparkAnswerAndOperator(df.select("nested1.nested2"))
        checkSparkAnswerAndOperator(df.select("nested1.nested2.id"))
        checkSparkAnswerAndOperator(df.select("nested1.id", "nested1.nested2.id"))
      } else {
        checkSparkAnswer(df.select("nested1.id"))
        checkSparkAnswer(df.select("nested1.nested2"))
        checkSparkAnswer(df.select("nested1.nested2.id"))
        checkSparkAnswer(df.select("nested1.id", "nested1.nested2.id"))
      }
    }
  }

  test("get_struct_field - read entire struct") {
    val scanImpl = CometConf.COMET_NATIVE_SCAN_IMPL.get()
    assume(!(scanImpl == CometConf.SCAN_AUTO && CometSparkSessionExtensions.isSpark40Plus))
    withTempPath { dir =>
      // create input file with Comet disabled
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        val df = spark
          .range(5)
          // Add both a null struct and null inner value
          .select(
            when(
              col("id") > 1,
              struct(
                when(col("id") > 2, col("id")).alias("id"),
                when(col("id") > 2, struct(when(col("id") > 3, col("id")).alias("id")))
                  .as("nested2")))
              .alias("nested1"))

        df.write.parquet(dir.toString())
      }

      val df = spark.read.parquet(dir.toString()).select("nested1.id")
      // Comet's original scan does not support structs.
      // The plan will have a Comet Scan only if scan impl is native_full or native_recordbatch
      if (scanImpl != CometConf.SCAN_NATIVE_COMET) {
        checkSparkAnswerAndOperator(df)
      } else {
        checkSparkAnswer(df)
      }
    }
  }

  private def testV1AndV2(testName: String)(f: => Unit): Unit = {
    test(s"$testName - V1") {
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") { f }
    }

    // The test will fail because it will  produce a different plan and the operator check will fail
    // We could get the test to pass anyway by skipping the operator check, but when V2 does get supported,
    // we want to make sure we enable the operator check and marking the test as ignore will make it
    // more obvious
    //
    ignore(s"$testName - V2") {
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") { f }
    }
  }

  testV1AndV2("get_struct_field with DataFusion ParquetExec - simple case") {
    withTempPath { dir =>
      // create input file with Comet disabled
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        val df = spark
          .range(5)
          // Add both a null struct and null inner value
          .select(when(col("id") > 1, struct(when(col("id") > 2, col("id")).alias("id")))
            .alias("nested1"))

        df.write.parquet(dir.toString())
      }

      withSQLConf(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_NATIVE_DATAFUSION,
        CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.key -> "true") {

        val df = spark.read.parquet(dir.toString())
        checkSparkAnswerAndOperator(df.select("nested1.id"))
      }
    }
  }

  testV1AndV2("get_struct_field with DataFusion ParquetExec - select subset of struct") {
    withTempPath { dir =>
      // create input file with Comet disabled
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        val df = spark
          .range(5)
          // Add both a null struct and null inner value
          .select(
            when(
              col("id") > 1,
              struct(
                when(col("id") > 2, col("id")).alias("id"),
                when(col("id") > 2, struct(when(col("id") > 3, col("id")).alias("id")))
                  .as("nested2")))
              .alias("nested1"))

        df.write.parquet(dir.toString())
      }

      withSQLConf(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_NATIVE_DATAFUSION,
        CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.key -> "true") {

        val df = spark.read.parquet(dir.toString())

        checkSparkAnswerAndOperator(df.select("nested1.id"))
        checkSparkAnswerAndOperator(df.select("nested1.id", "nested1.nested2.id"))
        checkSparkAnswerAndOperator(df.select("nested1.nested2.id"))
      }
    }
  }

  test("get_struct_field with DataFusion ParquetExec - read entire struct") {
    assume(usingDataSourceExec(conf))
    withTempPath { dir =>
      // create input file with Comet disabled
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        val df = spark
          .range(5)
          // Add both a null struct and null inner value
          .select(
            when(
              col("id") > 1,
              struct(
                when(col("id") > 2, col("id")).alias("id"),
                when(col("id") > 2, struct(when(col("id") > 3, col("id")).alias("id")))
                  .as("nested2")))
              .alias("nested1"))

        df.write.parquet(dir.toString())
      }

      Seq("", "parquet").foreach { v1List =>
        withSQLConf(
          SQLConf.USE_V1_SOURCE_LIST.key -> v1List,
          CometConf.COMET_ENABLED.key -> "true",
          CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.key -> "true") {

          val df = spark.read.parquet(dir.toString())
          if (v1List.isEmpty) {
            checkSparkAnswer(df.select("nested1"))
          } else {
            checkSparkAnswerAndOperator(df.select("nested1"))
          }
        }
      }
    }
  }

  test("read array[int] from parquet") {
    assume(usingDataSourceExec(conf))

    withTempPath { dir =>
// create input file with Comet disabled
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        val df = spark
          .range(5)
// Spark does not allow null as a key but does allow null as a
// value, and the entire map be null
          .select(when(col("id") > 1, sequence(lit(0), col("id") * 2)).alias("array1"))
        df.write.parquet(dir.toString())
      }

      Seq("", "parquet").foreach { v1List =>
        withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> v1List) {
          val df = spark.read.parquet(dir.toString())
          if (v1List.isEmpty) {
            checkSparkAnswer(df.select("array1"))
            checkSparkAnswer(df.select(element_at(col("array1"), lit(1))))
          } else {
            checkSparkAnswerAndOperator(df.select("array1"))
            checkSparkAnswerAndOperator(df.select(element_at(col("array1"), lit(1))))
          }
        }
      }
    }
  }

  test("CreateArray") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = dictionaryEnabled, 10000)
        val df = spark.read.parquet(path.toString)
        checkSparkAnswerAndOperator(df.select(array(col("_2"), col("_3"), col("_4"))))
        checkSparkAnswerAndOperator(df.select(array(col("_4"), col("_11"), lit(null))))
        checkSparkAnswerAndOperator(
          df.select(array(array(col("_4")), array(col("_4"), lit(null)))))
        checkSparkAnswerAndOperator(df.select(array(col("_8"), col("_13"))))
        // This ends up returning empty strings instead of nulls for the last element
        checkSparkAnswerAndOperator(df.select(array(col("_8"), col("_13"), lit(null))))
        checkSparkAnswerAndOperator(df.select(array(array(col("_8")), array(col("_13")))))
        checkSparkAnswerAndOperator(df.select(array(col("_8"), col("_8"), lit(null))))
        checkSparkAnswerAndOperator(df.select(array(struct("_4"), struct("_4"))))
        checkSparkAnswerAndOperator(
          df.select(array(struct(col("_8").alias("a")), struct(col("_13").alias("a")))))
      }
    }
  }

  test("ListExtract") {
    def assertBothThrow(df: DataFrame): Unit = {
      checkSparkMaybeThrows(df) match {
        case (Some(_), Some(_)) => ()
        case (spark, comet) =>
          fail(
            s"Expected Spark and Comet to throw exception, but got\nSpark: $spark\nComet: $comet")
      }
    }

    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = dictionaryEnabled, 100)

        Seq(true, false).foreach { ansiEnabled =>
          withSQLConf(
            CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key -> "true",
            SQLConf.ANSI_ENABLED.key -> ansiEnabled.toString(),
            // Prevent the optimizer from collapsing an extract value of a create array
            SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> SimplifyExtractValueOps.ruleName) {
            val df = spark.read.parquet(path.toString)

            val stringArray = df.select(array(col("_8"), col("_8"), lit(null)).alias("arr"))
            checkSparkAnswerAndOperator(
              stringArray
                .select(col("arr").getItem(0), col("arr").getItem(1), col("arr").getItem(2)))

            checkSparkAnswerAndOperator(
              stringArray.select(
                element_at(col("arr"), -3),
                element_at(col("arr"), -2),
                element_at(col("arr"), -1),
                element_at(col("arr"), 1),
                element_at(col("arr"), 2),
                element_at(col("arr"), 3)))

            // 0 is an invalid index for element_at
            assertBothThrow(stringArray.select(element_at(col("arr"), 0)))

            if (ansiEnabled) {
              assertBothThrow(stringArray.select(col("arr").getItem(-1)))
              assertBothThrow(stringArray.select(col("arr").getItem(3)))
              assertBothThrow(stringArray.select(element_at(col("arr"), -4)))
              assertBothThrow(stringArray.select(element_at(col("arr"), 4)))
            } else {
              checkSparkAnswerAndOperator(stringArray.select(col("arr").getItem(-1)))
              checkSparkAnswerAndOperator(stringArray.select(col("arr").getItem(3)))
              checkSparkAnswerAndOperator(stringArray.select(element_at(col("arr"), -4)))
              checkSparkAnswerAndOperator(stringArray.select(element_at(col("arr"), 4)))
            }

            val intArray =
              df.select(when(col("_4").isNotNull, array(col("_4"), col("_4"))).alias("arr"))
            checkSparkAnswerAndOperator(
              intArray
                .select(col("arr").getItem(0), col("arr").getItem(1)))

            checkSparkAnswerAndOperator(
              intArray.select(
                element_at(col("arr"), 1),
                element_at(col("arr"), 2),
                element_at(col("arr"), -1),
                element_at(col("arr"), -2)))
          }
        }
      }
    }
  }

  test("GetArrayStructFields") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> SimplifyExtractValueOps.ruleName) {
        withTempDir { dir =>
          val path = new Path(dir.toURI.toString, "test.parquet")
          makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = dictionaryEnabled, 10000)
          val df = spark.read
            .parquet(path.toString)
            .select(
              array(struct(col("_2"), col("_3"), col("_4"), col("_8")), lit(null)).alias("arr"))
          checkSparkAnswerAndOperator(df.select("arr._2", "arr._3", "arr._4"))

          val complex = spark.read
            .parquet(path.toString)
            .select(array(struct(struct(col("_4"), col("_8")).alias("nested"))).alias("arr"))

          checkSparkAnswerAndOperator(complex.select(col("arr.nested._4")))
        }
      }
    }
  }

  test("test integral divide") {
    // this test requires native_comet scan due to unsigned u8/u16 issue
    withSQLConf(
      CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_NATIVE_COMET,
      CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key -> "true") {
      Seq(true, false).foreach { dictionaryEnabled =>
        withTempDir { dir =>
          val path1 = new Path(dir.toURI.toString, "test1.parquet")
          val path2 = new Path(dir.toURI.toString, "test2.parquet")
          makeParquetFileAllPrimitiveTypes(
            path1,
            dictionaryEnabled = dictionaryEnabled,
            0,
            0,
            randomSize = 10000)
          makeParquetFileAllPrimitiveTypes(
            path2,
            dictionaryEnabled = dictionaryEnabled,
            0,
            0,
            randomSize = 10000)
          withParquetTable(path1.toString, "tbl1") {
            withParquetTable(path2.toString, "tbl2") {
              checkSparkAnswerAndOperator("""
                  |select
                  | t1._2 div t2._2, div(t1._2, t2._2),
                  | t1._3 div t2._3, div(t1._3, t2._3),
                  | t1._4 div t2._4, div(t1._4, t2._4),
                  | t1._5 div t2._5, div(t1._5, t2._5),
                  | t1._9 div t2._9, div(t1._9, t2._9),
                  | t1._10 div t2._10, div(t1._10, t2._10),
                  | t1._11 div t2._11, div(t1._11, t2._11)
                  | from tbl1 t1 join tbl2 t2 on t1._id = t2._id
                  | order by t1._id""".stripMargin)

              checkSparkAnswerAndOperator("""
                  |select
                  | t1._12 div t2._12, div(t1._12, t2._12),
                  | t1._15 div t2._15, div(t1._15, t2._15),
                  | t1._16 div t2._16, div(t1._16, t2._16),
                  | t1._17 div t2._17, div(t1._17, t2._17)
                  | from tbl1 t1 join tbl2 t2 on t1._id = t2._id
                  | order by t1._id""".stripMargin)
            }
          }
        }
      }
    }
  }

  test("ANSI support for add") {
    val data = Seq((Integer.MAX_VALUE, 1), (Integer.MIN_VALUE, -1))
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      withParquetTable(data, "tbl") {
        val res = spark.sql("""
                              |SELECT
                              |  _1 + _2
                              |  from tbl
                              |  """.stripMargin)

        checkSparkMaybeThrows(res) match {
          case (Some(sparkExc), Some(cometExc)) =>
            assert(cometExc.getMessage.contains(ARITHMETIC_OVERFLOW_EXCEPTION_MSG))
            assert(sparkExc.getMessage.contains("overflow"))
          case _ => fail("Exception should be thrown")
        }
      }
    }
  }

  test("ANSI support for subtract") {
    val data = Seq((Integer.MIN_VALUE, 1))
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      withParquetTable(data, "tbl") {
        val res = spark.sql("""
                              |SELECT
                              |  _1 - _2
                              |  from tbl
                              |  """.stripMargin)
        checkSparkMaybeThrows(res) match {
          case (Some(sparkExc), Some(cometExc)) =>
            assert(cometExc.getMessage.contains(ARITHMETIC_OVERFLOW_EXCEPTION_MSG))
            assert(sparkExc.getMessage.contains("overflow"))
          case _ => fail("Exception should be thrown")
        }
      }
    }
  }

  test("ANSI support for multiply") {
    val data = Seq((Integer.MAX_VALUE, 10))
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      withParquetTable(data, "tbl") {
        val res = spark.sql("""
                              |SELECT
                              |  _1 * _2
                              |  from tbl
                              |  """.stripMargin)

        checkSparkMaybeThrows(res) match {
          case (Some(sparkExc), Some(cometExc)) =>
            assert(cometExc.getMessage.contains(ARITHMETIC_OVERFLOW_EXCEPTION_MSG))
            assert(sparkExc.getMessage.contains("overflow"))
          case _ => fail("Exception should be thrown")
        }
      }
    }
  }

  test("ANSI support for divide (division by zero)") {
    val data = Seq((Integer.MIN_VALUE, 0))
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      withParquetTable(data, "tbl") {
        val res = spark.sql("""
                              |SELECT
                              |  _1 / _2
                              |  from tbl
                              |  """.stripMargin)

        checkSparkMaybeThrows(res) match {
          case (Some(sparkExc), Some(cometExc)) =>
            assert(cometExc.getMessage.contains(DIVIDE_BY_ZERO_EXCEPTION_MSG))
            assert(sparkExc.getMessage.contains("Division by zero"))
          case _ => fail("Exception should be thrown")
        }
      }
    }
  }

  test("ANSI support for divide (division by zero) float division") {
    val data = Seq((Float.MinPositiveValue, 0.0))
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      withParquetTable(data, "tbl") {
        val res = spark.sql("""
                              |SELECT
                              |  _1 / _2
                              |  from tbl
                              |  """.stripMargin)

        checkSparkMaybeThrows(res) match {
          case (Some(sparkExc), Some(cometExc)) =>
            assert(cometExc.getMessage.contains(DIVIDE_BY_ZERO_EXCEPTION_MSG))
            assert(sparkExc.getMessage.contains("Division by zero"))
          case _ => fail("Exception should be thrown")
        }
      }
    }
  }

  test("ANSI support for integral divide (division by zero)") {
    val data = Seq((Integer.MAX_VALUE, 0))
    Seq("true", "false").foreach { p =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> p) {
        withParquetTable(data, "tbl") {
          val res = spark.sql("""
                |SELECT
                |  _1 div _2
                |  from tbl
                |  """.stripMargin)

          checkSparkMaybeThrows(res) match {
            case (Some(sparkException), Some(cometException)) =>
              assert(sparkException.getMessage.contains(DIVIDE_BY_ZERO_EXCEPTION_MSG))
              assert(cometException.getMessage.contains(DIVIDE_BY_ZERO_EXCEPTION_MSG))
            case (None, None) => checkSparkAnswerAndOperator(res)
            case (None, Some(ex)) =>
              fail(
                "Comet threw an exception but Spark did not. Comet exception: " + ex.getMessage)
            case (Some(sparkException), None) =>
              fail(
                "Spark threw an exception but Comet did not. Spark exception: " +
                  sparkException.getMessage)
          }
        }
      }
    }
  }

  test("ANSI support for round function") {
    Seq(
      (
        Int.MaxValue,
        Int.MinValue,
        Long.MinValue,
        Long.MaxValue,
        Byte.MinValue,
        Byte.MaxValue,
        Short.MinValue,
        Short.MaxValue)).foreach { value =>
      val data = Seq(value)
      withParquetTable(data, "tbl") {
        Seq(-1000, -100, -10, -1, 0, 1, 10, 100, 1000).foreach { scale =>
          Seq(true, false).foreach { ansi =>
            withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi.toString) {
              val res = spark.sql(s"SELECT round(_1, $scale) from tbl")
              checkSparkMaybeThrows(res) match {
                case (Some(sparkException), Some(cometException)) =>
                  assert(sparkException.getMessage.contains("ARITHMETIC_OVERFLOW"))
                  assert(cometException.getMessage.contains("ARITHMETIC_OVERFLOW"))
                case (None, None) => checkSparkAnswerAndOperator(res)
                case (None, Some(ex)) =>
                  fail("Comet threw an exception but Spark did not. Comet exception: " + ex.getMessage)
                case (Some(sparkException), None) =>
                  fail("Spark threw an exception but Comet did not. Spark exception: " +
                    sparkException.getMessage)
              }
            }
          }
        }
      }
    }
  }

  test("test integral divide overflow for decimal") {
    if (isSpark40Plus) {
      Seq(true, false)
    } else
      {
        // ansi mode only supported in Spark 4.0+
        Seq(false)
      }.foreach { ansiMode =>
        withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiMode.toString) {
          withTable("t1") {
            sql("create table t1(a decimal(38,0), b decimal(2,2)) using parquet")
            sql(
              "insert into t1 values(-62672277069777110394022909049981876593,-0.40)," +
                " (-68299431870253176399167726913574455270,-0.22), (-77532633078952291817347741106477071062,0.36)," +
                " (-79918484954351746825313746420585672848,0.44), (54400354300704342908577384819323710194,0.18)," +
                " (78585488402645143056239590008272527352,-0.51)")
            checkSparkAnswerAndOperator("select a div b from t1")
          }
        }
      }
  }

  private def testOnShuffledRangeWithRandomParameters(testLogic: DataFrame => Unit): Unit = {
    val partitionsNumber = Random.nextInt(10) + 1
    val rowsNumber = Random.nextInt(500)
    // use this value to have both single-batch and multi-batch partitions
    val cometBatchSize = math.max(1, math.floor(rowsNumber.toDouble / partitionsNumber).toInt)
    withSQLConf("spark.comet.batchSize" -> cometBatchSize.toString) {
      withParquetDataFrame((0 until rowsNumber).map(Tuple1.apply)) { df =>
        testLogic(df.repartition(partitionsNumber))
      }
    }
  }

  test("rand expression with random parameters") {
    testOnShuffledRangeWithRandomParameters { df =>
      val seed = Random.nextLong()
      val dfWithRandParameters = df.withColumn("rnd", rand(seed))
      checkSparkAnswerAndOperator(dfWithRandParameters)
      val dfWithOverflowSeed = df.withColumn("rnd", rand(Long.MaxValue))
      checkSparkAnswerAndOperator(dfWithOverflowSeed)
      val dfWithNullSeed = df.selectExpr("_1", "rand(null) as rnd")
      checkSparkAnswerAndOperator(dfWithNullSeed)
    }
  }

  test("randn expression with random parameters") {
    testOnShuffledRangeWithRandomParameters { df =>
      val seed = Random.nextLong()
      val dfWithRandParameters = df.withColumn("randn", randn(seed))
      checkSparkAnswerAndOperatorWithTol(dfWithRandParameters)
      val dfWithOverflowSeed = df.withColumn("randn", randn(Long.MaxValue))
      checkSparkAnswerAndOperatorWithTol(dfWithOverflowSeed)
      val dfWithNullSeed = df.selectExpr("_1", "randn(null) as randn")
      checkSparkAnswerAndOperatorWithTol(dfWithNullSeed)
    }
  }

  test("spark_partition_id expression on random dataset") {
    testOnShuffledRangeWithRandomParameters { df =>
      val dfWithRandParameters =
        df.withColumn("pid1", spark_partition_id())
          .repartition(3)
          .withColumn("pid2", spark_partition_id())
      checkSparkAnswerAndOperator(dfWithRandParameters)
    }
  }

  test("monotonically_increasing_id expression on random dataset") {
    testOnShuffledRangeWithRandomParameters { df =>
      val dfWithRandParameters =
        df.withColumn("mid1", monotonically_increasing_id())
          .repartition(3)
          .withColumn("mid2", monotonically_increasing_id())
      checkSparkAnswerAndOperator(dfWithRandParameters)
    }
  }

  test("multiple nondetermenistic expressions with shuffle") {
    testOnShuffledRangeWithRandomParameters { df =>
      val seed1 = Random.nextLong()
      val seed2 = Random.nextLong()
      val complexRandDf = df
        .withColumn("rand1", rand(seed1))
        .withColumn("randn1", randn(seed1))
        .repartition(2, col("_1"))
        .sortWithinPartitions("_1")
        .withColumn("rand2", rand(seed2))
        .withColumn("randn2", randn(seed2))
      checkSparkAnswerAndOperatorWithTol(complexRandDf)
    }
  }

  test("vectorized reader: missing all struct fields") {
    Seq(true, false).foreach { offheapEnabled =>
      withSQLConf(
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.key -> "false",
        CometConf.COMET_NATIVE_SCAN_IMPL.key -> "native_datafusion",
        SQLConf.PARQUET_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key -> "true",
        SQLConf.COLUMN_VECTOR_OFFHEAP_ENABLED.key -> offheapEnabled.toString) {
        val data = Seq(Tuple1((1, "a")), Tuple1((2, null)), Tuple1(null))

        val readSchema = new StructType().add(
          "_1",
          new StructType()
            .add("_3", IntegerType, nullable = false)
            .add("_4", StringType, nullable = false),
          nullable = false)

        withParquetFile(data) { file =>
          checkAnswer(
            spark.read.schema(readSchema).parquet(file),
            Row(null) :: Row(null) :: Row(null) :: Nil)
        }
      }
    }
  }

  test("test length function") {
    withTable("t1") {
      sql(
        "create table t1 using parquet as select cast(id as string) as c1, cast(id as binary) as c2 from range(10)")
      // FIXME: Change checkSparkAnswer to checkSparkAnswerAndOperator after resolving
      //  https://github.com/apache/datafusion-comet/issues/2348
      checkSparkAnswer("select length(c1), length(c2) AS x FROM t1 ORDER BY c1")
    }
  }

  test("test concat function - strings") {
    withTable("t1") {
      sql(
        "create table t1 using parquet as select uuid() c1, uuid() c2, uuid() c3, uuid() c4, cast(null as string) c5 from range(10)")
      checkSparkAnswerAndOperator("select concat(c1, c2) AS x FROM t1")
      checkSparkAnswerAndOperator("select concat(c1, c1) AS x FROM t1")
      checkSparkAnswerAndOperator("select concat(c1, c2, c3) AS x FROM t1")
      checkSparkAnswerAndOperator("select concat(c1, c2, c3, c5) AS x FROM t1")
      checkSparkAnswerAndOperator(
        "select concat(concat(c1, c2, c3), concat(c1, c3)) AS x FROM t1")
    }
  }

  // https://github.com/apache/datafusion-comet/issues/2647
  test("test concat function - arrays") {
    withTable("t1") {
      sql(
        "create table t1 using parquet as select array(id, id+1) c1, array(id+2, id+3) c2, CAST(array() AS array<int>) c3, CAST(array(null) as array<int>) c4, cast(null as array<int>) c5 from range(10)")
      checkSparkAnswerAndFallbackReason(
        "select concat(c1, c2) AS x FROM t1",
        CometConcat.unsupportedReason)
      checkSparkAnswerAndFallbackReason(
        "select concat(c1, c1) AS x FROM t1",
        CometConcat.unsupportedReason)
      checkSparkAnswerAndFallbackReason(
        "select concat(c1, c2, c3) AS x FROM t1",
        CometConcat.unsupportedReason)
      checkSparkAnswerAndFallbackReason(
        "select concat(c1, c2, c3, c5) AS x FROM t1",
        CometConcat.unsupportedReason)
      checkSparkAnswerAndFallbackReason(
        "select concat(concat(c1, c2, c3), concat(c1, c3)) AS x FROM t1",
        CometConcat.unsupportedReason)
    }
  }

  // https://github.com/apache/datafusion-comet/issues/2647
  test("test concat function - binary") {
    withTable("t1") {
      sql(
        "create table t1 using parquet as select cast(uuid() as binary) c1, cast(uuid() as binary) c2, cast(uuid() as binary) c3, cast(uuid() as binary) c4, cast(null as binary) c5 from range(10)")
      checkSparkAnswerAndFallbackReason(
        "select concat(c1, c2) AS x FROM t1",
        CometConcat.unsupportedReason)
      checkSparkAnswerAndFallbackReason(
        "select concat(c1, c1) AS x FROM t1",
        CometConcat.unsupportedReason)
      checkSparkAnswerAndFallbackReason(
        "select concat(c1, c2, c3) AS x FROM t1",
        CometConcat.unsupportedReason)
      checkSparkAnswerAndFallbackReason(
        "select concat(c1, c2, c3, c5) AS x FROM t1",
        CometConcat.unsupportedReason)
      checkSparkAnswerAndFallbackReason(
        "select concat(concat(c1, c2, c3), concat(c1, c3)) AS x FROM t1",
        CometConcat.unsupportedReason)
    }
  }
}
