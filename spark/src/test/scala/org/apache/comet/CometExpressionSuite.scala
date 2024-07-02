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

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{CometTestBase, DataFrame, Row}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.SESSION_LOCAL_TIMEZONE
import org.apache.spark.sql.types.{Decimal, DecimalType}

import org.apache.comet.CometSparkSessionExtensions.{isSpark33Plus, isSpark34Plus}

class CometExpressionSuite extends CometTestBase with AdaptiveSparkPlanHelper {
  import testImplicits._

  test("coalesce should return correct datatype") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllTypes(path, dictionaryEnabled = dictionaryEnabled, 10000)
        withParquetTable(path.toString, "tbl") {
          checkSparkAnswerAndOperator(
            "SELECT coalesce(cast(_18 as date), cast(_19 as date), _20) FROM tbl")
        }
      }
    }
  }

  test("decimals divide by zero") {
    // TODO: enable Spark 3.3 tests after supporting decimal divide operation
    assume(isSpark34Plus)

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

  test("bitwise shift with different left/right types") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
        val table = "test"
        withTable(table) {
          sql(s"create table $table(col1 long, col2 int) using parquet")
          sql(s"insert into $table values(1111, 2)")
          sql(s"insert into $table values(1111, 2)")
          sql(s"insert into $table values(3333, 4)")
          sql(s"insert into $table values(5555, 6)")

          checkSparkAnswerAndOperator(
            s"SELECT shiftright(col1, 2), shiftright(col1, col2) FROM $table")
          checkSparkAnswerAndOperator(
            s"SELECT shiftleft(col1, 2), shiftleft(col1, col2) FROM $table")
        }
      }
    }
  }

  test("basic data type support") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllTypes(path, dictionaryEnabled = dictionaryEnabled, 10000)
        withParquetTable(path.toString, "tbl") {
          // TODO: enable test for unsigned ints
          checkSparkAnswerAndOperator(
            "select _1, _2, _3, _4, _5, _6, _7, _8, _13, _14, _15, _16, _17, " +
              "_18, _19, _20 FROM tbl WHERE _2 > 100")
        }
      }
    }
  }

  test("null literals") {
    val batchSize = 1000
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllTypes(path, dictionaryEnabled = dictionaryEnabled, batchSize)
        withParquetTable(path.toString, "tbl") {
          val sqlString = "SELECT _4 + null, _15 - null, _16 * null  FROM tbl"
          val df2 = sql(sqlString)
          val rows = df2.collect()
          assert(rows.length == batchSize)
          assert(rows.forall(_ == Row(null, null, null)))

          checkSparkAnswerAndOperator(sqlString)
        }
      }
    }
  }

  test("date and timestamp type literals") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllTypes(path, dictionaryEnabled = dictionaryEnabled, 10000)
        withParquetTable(path.toString, "tbl") {
          checkSparkAnswerAndOperator(
            "SELECT _4 FROM tbl WHERE " +
              "_20 > CAST('2020-01-01' AS DATE) AND _18 < CAST('2020-01-01' AS TIMESTAMP)")
        }
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

  test("string_space") {
    withParquetTable((0 until 5).map(i => (i, i + 1)), "tbl") {
      checkSparkAnswerAndOperator("SELECT space(_1), space(_2) FROM tbl")
    }
  }

  test("string_space with dictionary") {
    val data = (0 until 1000).map(i => Tuple1(i % 5))

    withSQLConf("parquet.enable.dictionary" -> "true") {
      withParquetTable(data, "tbl") {
        checkSparkAnswerAndOperator("SELECT space(_1) FROM tbl")
      }
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
      CometConf.COMET_CAST_ALLOW_INCOMPATIBLE.key -> "true") {
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
    // TODO: make the test pass for Spark 3.3
    assume(isSpark34Plus)

    withSQLConf(
      SESSION_LOCAL_TIMEZONE.key -> "Asia/Kathmandu",
      CometConf.COMET_CAST_ALLOW_INCOMPATIBLE.key -> "true") {
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
    // TODO: make the test pass for Spark 3.3
    assume(isSpark34Plus)

    withSQLConf(
      SESSION_LOCAL_TIMEZONE.key -> "Asia/Kathmandu",
      CometConf.COMET_CAST_ALLOW_INCOMPATIBLE.key -> "true") {
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
        makeParquetFileAllTypes(path, dictionaryEnabled = dictionaryEnabled, 10000)
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
          checkSparkAnswerAndOperator(
            "SELECT " +
              "dateformat, _7, " +
              "trunc(_7, dateformat) " +
              " from dateformattbl ")
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
    withSQLConf(CometConf.COMET_CAST_ALLOW_INCOMPATIBLE.key -> "true") {
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
    assume(isSpark33Plus, "TimestampNTZ is supported in Spark 3.3+, See SPARK-36182")
    withSQLConf(CometConf.COMET_CAST_ALLOW_INCOMPATIBLE.key -> "true") {
      val numRows = 1000
      Seq(true, false).foreach { dictionaryEnabled =>
        withTempDir { dir =>
          val path = new Path(dir.toURI.toString, "timestamp_trunc_with_format.parquet")
          makeDateTimeWithFormatTable(path, dictionaryEnabled = dictionaryEnabled, numRows)
          withParquetTable(path.toString, "timeformattbl") {
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
                checkSparkAnswer(
                  "SELECT " +
                    s"date_trunc('$format', ts )" +
                    " from int96timetbl")
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
      withParquetTable(Seq((1, 0, 1.0, 0.0)), "tbl") {
        checkSparkAnswerAndOperator("SELECT _1 / _2, _3 / _4 FROM tbl")
      }
    }
  }

  test("decimals arithmetic and comparison") {
    // TODO: enable Spark 3.3 tests after supporting decimal reminder operation
    assume(isSpark34Plus)

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
    assume(isSpark34Plus)
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

  test("various math scalar functions") {
    Seq("true", "false").foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary) {
        withParquetTable(
          (0 until 5).map(i => (i.toDouble + 0.3, i.toDouble + 0.8)),
          "tbl",
          withDictionary = dictionary.toBoolean) {
          checkSparkAnswerWithTol(
            "SELECT abs(_1), acos(_2), asin(_1), atan(_2), atan2(_1, _2), cos(_1) FROM tbl")
          checkSparkAnswerWithTol(
            "SELECT exp(_1), ln(_2), log10(_1), log2(_1), pow(_1, _2) FROM tbl")
          // TODO: comment in the round tests once supported
          // checkSparkAnswerWithTol("SELECT round(_1), round(_2) FROM tbl")
          checkSparkAnswerWithTol("SELECT signum(_1), sin(_1), sqrt(_1) FROM tbl")
          checkSparkAnswerWithTol("SELECT tan(_1) FROM tbl")
        }
      }
    }
  }

  test("abs") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllTypes(path, dictionaryEnabled = dictionaryEnabled, 100)
        withParquetTable(path.toString, "tbl") {
          Seq(2, 3, 4, 5, 6, 7, 15, 16, 17).foreach { col =>
            checkSparkAnswerAndOperator(s"SELECT abs(_${col}) FROM tbl")
          }
        }
      }
    }
  }

  test("abs Overflow ansi mode") {

    def testAbsAnsiOverflow[T <: Product: ClassTag: TypeTag](data: Seq[T]): Unit = {
      withParquetTable(data, "tbl") {
        checkSparkMaybeThrows(sql("select abs(_1), abs(_2) from tbl")) match {
          case (Some(sparkExc), Some(cometExc)) =>
            val cometErrorPattern =
              """.+[ARITHMETIC_OVERFLOW].+overflow. If necessary set "spark.sql.ansi.enabled" to "false" to bypass this error.""".r
            assert(cometErrorPattern.findFirstIn(cometExc.getMessage).isDefined)
            assert(sparkExc.getMessage.contains("overflow"))
          case _ => fail("Exception should be thrown")
        }
      }
    }

    def testAbsAnsi[T <: Product: ClassTag: TypeTag](data: Seq[T]): Unit = {
      withParquetTable(data, "tbl") {
        checkSparkAnswerAndOperator("select abs(_1), abs(_2) from tbl")
      }
    }

    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      CometConf.COMET_ANSI_MODE_ENABLED.key -> "true") {
      testAbsAnsiOverflow(Seq((Byte.MaxValue, Byte.MinValue)))
      testAbsAnsiOverflow(Seq((Short.MaxValue, Short.MinValue)))
      testAbsAnsiOverflow(Seq((Int.MaxValue, Int.MinValue)))
      testAbsAnsiOverflow(Seq((Long.MaxValue, Long.MinValue)))
      testAbsAnsi(Seq((Float.MaxValue, Float.MinValue)))
      testAbsAnsi(Seq((Double.MaxValue, Double.MinValue)))
    }
  }

  test("abs Overflow legacy mode") {

    def testAbsLegacyOverflow[T <: Product: ClassTag: TypeTag](data: Seq[T]): Unit = {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        withParquetTable(data, "tbl") {
          checkSparkAnswerAndOperator("select abs(_1), abs(_2) from tbl")
        }
      }
    }

    testAbsLegacyOverflow(Seq((Byte.MaxValue, Byte.MinValue)))
    testAbsLegacyOverflow(Seq((Short.MaxValue, Short.MinValue)))
    testAbsLegacyOverflow(Seq((Int.MaxValue, Int.MinValue)))
    testAbsLegacyOverflow(Seq((Long.MaxValue, Long.MinValue)))
    testAbsLegacyOverflow(Seq((Float.MaxValue, Float.MinValue)))
    testAbsLegacyOverflow(Seq((Double.MaxValue, Double.MinValue)))
  }

  test("ceil and floor") {
    Seq("true", "false").foreach { dictionary =>
      withSQLConf(
        "parquet.enable.dictionary" -> dictionary,
        CometConf.COMET_CAST_ALLOW_INCOMPATIBLE.key -> "true") {
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
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllTypes(
          path,
          dictionaryEnabled = dictionaryEnabled,
          -128,
          128,
          randomSize = 100)
        withParquetTable(path.toString, "tbl") {
          for (s <- Seq(-5, -1, 0, 1, 5, -1000, 1000, -323, -308, 308, -15, 15, -16, 16, null)) {
            // array tests
            // TODO: enable test for unsigned ints (_9, _10, _11, _12)
            // TODO: enable test for floats (_6, _7, _8, _13)
            for (c <- Seq(2, 3, 4, 5, 15, 16, 17)) {
              checkSparkAnswerAndOperator(s"select _${c}, round(_${c}, ${s}) FROM tbl")
            }
            // scalar tests
            // Exclude the constant folding optimizer in order to actually execute the native round
            // operations for scalar (literal) values.
            // TODO: comment in the tests for float once supported
            withSQLConf(
              "spark.sql.optimizer.excludedRules" -> "org.apache.spark.sql.catalyst.optimizer.ConstantFolding") {
              for (n <- Seq("0.0", "-0.0", "0.5", "-0.5", "1.2", "-1.2")) {
                checkSparkAnswerAndOperator(s"select round(cast(${n} as tinyint), ${s}) FROM tbl")
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

  test("Various String scalar functions") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
        val table = "names"
        withTable(table) {
          sql(s"create table $table(id int, name varchar(20)) using parquet")
          sql(
            s"insert into $table values(1, 'James Smith'), (2, 'Michael Rose')," +
              " (3, 'Robert Williams'), (4, 'Rames Rose'), (5, 'James Smith')")
          checkSparkAnswerAndOperator(
            s"SELECT ascii(name), bit_length(name), octet_length(name), upper(name), lower(name) FROM $table")
        }
      }
    }
  }

  test("Chr") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf(
        "parquet.enable.dictionary" -> dictionary.toString,
        CometConf.COMET_CAST_ALLOW_INCOMPATIBLE.key -> "true") {
        val table = "test"
        withTable(table) {
          sql(s"create table $table(col varchar(20)) using parquet")
          sql(
            s"insert into $table values('65'), ('66'), ('67'), ('68'), ('65'), ('66'), ('67'), ('68')")
          checkSparkAnswerAndOperator(s"SELECT chr(col) FROM $table")
        }
      }
    }
  }

  test("Chr with null character") {
    // test compatibility with Spark, spark supports chr(0)
    Seq(false, true).foreach { dictionary =>
      withSQLConf(
        "parquet.enable.dictionary" -> dictionary.toString,
        CometConf.COMET_CAST_ALLOW_INCOMPATIBLE.key -> "true") {
        val table = "test0"
        withTable(table) {
          sql(s"create table $table(c9 int, c4 int) using parquet")
          sql(s"insert into $table values(0, 0), (66, null), (null, 70), (null, null)")
          val query = s"SELECT chr(c9), chr(c4) FROM $table"
          checkSparkAnswerAndOperator(query)
        }
      }
    }
  }

  test("Chr with negative and large value") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
        val table = "test0"
        withTable(table) {
          sql(s"create table $table(c9 int, c4 int) using parquet")
          sql(
            s"insert into $table values(0, 0), (61231, -61231), (-1700, 1700), (0, -4000), (-40, 40), (256, 512)")
          val query = s"SELECT chr(c9), chr(c4) FROM $table"
          checkSparkAnswerAndOperator(query)
        }
      }
    }

    withParquetTable((0 until 5).map(i => (i % 5, i % 3)), "tbl") {
      withSQLConf(
        "spark.sql.optimizer.excludedRules" -> "org.apache.spark.sql.catalyst.optimizer.ConstantFolding") {
        for (n <- Seq("0", "-0", "0.5", "-0.5", "555", "-555", "null")) {
          checkSparkAnswerAndOperator(s"select chr(cast(${n} as int)) FROM tbl")
        }
      }
    }
  }

  test("InitCap") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
        val table = "names"
        withTable(table) {
          sql(s"create table $table(id int, name varchar(20)) using parquet")
          sql(
            s"insert into $table values(1, 'james smith'), (2, 'michael rose'), " +
              "(3, 'robert williams'), (4, 'rames rose'), (5, 'james smith')")
          checkSparkAnswerAndOperator(s"SELECT initcap(name) FROM $table")
        }
      }
    }
  }

  test("trim") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
        val table = "test"
        withTable(table) {
          sql(s"create table $table(col varchar(20)) using parquet")
          sql(s"insert into $table values('    SparkSQL   '), ('SSparkSQLS')")

          checkSparkAnswerAndOperator(s"SELECT upper(trim(col)) FROM $table")
          checkSparkAnswerAndOperator(s"SELECT trim('SL', col) FROM $table")

          checkSparkAnswerAndOperator(s"SELECT upper(btrim(col)) FROM $table")
          checkSparkAnswerAndOperator(s"SELECT btrim('SL', col) FROM $table")

          checkSparkAnswerAndOperator(s"SELECT upper(ltrim(col)) FROM $table")
          checkSparkAnswerAndOperator(s"SELECT ltrim('SL', col) FROM $table")

          checkSparkAnswerAndOperator(s"SELECT upper(rtrim(col)) FROM $table")
          checkSparkAnswerAndOperator(s"SELECT rtrim('SL', col) FROM $table")
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

  test("string concat_ws") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
        val table = "names"
        withTable(table) {
          sql(
            s"create table $table(id int, first_name varchar(20), middle_initial char(1), last_name varchar(20)) using parquet")
          sql(
            s"insert into $table values(1, 'James', 'B', 'Taylor'), (2, 'Smith', 'C', 'Davis')," +
              " (3, NULL, NULL, NULL), (4, 'Smith', 'C', 'Davis')")
          checkSparkAnswerAndOperator(
            s"SELECT concat_ws(' ', first_name, middle_initial, last_name) FROM $table")
        }
      }
    }
  }

  test("string repeat") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
        val table = "names"
        withTable(table) {
          sql(s"create table $table(id int, name varchar(20)) using parquet")
          sql(s"insert into $table values(1, 'James'), (2, 'Smith'), (3, 'Smith')")
          checkSparkAnswerAndOperator(s"SELECT repeat(name, 3) FROM $table")
        }
      }
    }
  }

  test("hex") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "hex.parquet")
        makeParquetFileAllTypes(path, dictionaryEnabled = dictionaryEnabled, 10000)

        withParquetTable(path.toString, "tbl") {
          // _9 and _10 (uint8 and uint16) not supported
          checkSparkAnswerAndOperator(
            "SELECT hex(_1), hex(_2), hex(_3), hex(_4), hex(_5), hex(_6), hex(_7), hex(_8), hex(_11), hex(_12), hex(_13), hex(_14), hex(_15), hex(_16), hex(_17), hex(_18), hex(_19), hex(_20) FROM tbl")
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
  test("length, reverse, instr, replace, translate") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
        val table = "test"
        withTable(table) {
          sql(s"create table $table(col string) using parquet")
          sql(
            s"insert into $table values('Spark SQL  '), (NULL), (''), ('苹果手机'), ('Spark SQL  '), (NULL), (''), ('苹果手机')")
          checkSparkAnswerAndOperator("select length(col), reverse(col), instr(col, 'SQL'), instr(col, '手机'), replace(col, 'SQL', '123')," +
            s" replace(col, 'SQL'), replace(col, '手机', '平板'), translate(col, 'SL苹', '123') from $table")
        }
      }
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

  test("bitwise expressions") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
        val table = "test"
        withTable(table) {
          sql(s"create table $table(col1 int, col2 int) using parquet")
          sql(s"insert into $table values(1111, 2)")
          sql(s"insert into $table values(1111, 2)")
          sql(s"insert into $table values(3333, 4)")
          sql(s"insert into $table values(5555, 6)")

          checkSparkAnswerAndOperator(
            s"SELECT col1 & col2,  col1 | col2, col1 ^ col2 FROM $table")
          checkSparkAnswerAndOperator(
            s"SELECT col1 & 1234,  col1 | 1234, col1 ^ 1234 FROM $table")
          checkSparkAnswerAndOperator(
            s"SELECT shiftright(col1, 2), shiftright(col1, col2) FROM $table")
          checkSparkAnswerAndOperator(
            s"SELECT shiftleft(col1, 2), shiftleft(col1, col2) FROM $table")
          checkSparkAnswerAndOperator(s"SELECT ~(11), ~col1, ~col2 FROM $table")
        }
      }
    }
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
            checkSparkAnswer(s"SELECT * FROM $table WHERE id not in (1)")
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

  test("Year") {
    Seq(false, true).foreach { dictionary =>
      withSQLConf("parquet.enable.dictionary" -> dictionary.toString) {
        val table = "test"
        withTable(table) {
          sql(s"create table $table(col timestamp) using parquet")
          sql(s"insert into $table values (now()), (null)")
          checkSparkAnswerAndOperator(s"SELECT year(col) FROM $table")
        }
      }
    }
  }

  test("Decimal binary ops multiply is aligned to Spark") {
    assume(isSpark34Plus)
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
    assume(isSpark34Plus) // Only Spark 3.4+ has the fix for SPARK-45786
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
            var ops = Seq("+", "-", "*", "/", "%")
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
    assume(isSpark34Plus)
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "false",
      SQLConf.COALESCE_PARTITIONS_ENABLED.key -> "true",
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_ENFORCE_MODE_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ALL_OPERATOR_ENABLED.key -> "true",
      EXTENDED_EXPLAIN_PROVIDERS_KEY -> "org.apache.comet.ExtendedExplainInfo") {
      val table = "test"
      withTable(table) {
        sql(s"create table $table(c0 int, c1 int , c2 float) using parquet")
        sql(s"insert into $table values(0, 1, 100.000001)")

        Seq(
          (
            s"SELECT cast(make_interval(c0, c1, c0, c1, c0, c0, c2) as string) as C from $table",
            Set("make_interval is not supported")),
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
            Set(
              "Comet shuffle is not enabled: spark.comet.exec.shuffle.enabled is not enabled",
              "AQEShuffleRead is not supported")),
          (
            "SELECT A.c1, A.sum_c0, A.sum_c2, B.casted from "
              + s"(SELECT c1, sum(c0) as sum_c0, sum(c2) as sum_c2 from $table group by c1) as A, "
              + s"(SELECT c1, cast(make_interval(c0, c1, c0, c1, c0, c0, c2) as string) as casted from $table) as B "
              + "where A.c1 = B.c1 ",
            Set(
              "Comet shuffle is not enabled: spark.comet.exec.shuffle.enabled is not enabled",
              "AQEShuffleRead is not supported",
              "make_interval is not supported",
              "BroadcastExchange is not supported",
              "BroadcastHashJoin disabled because not all child plans are native")))
          .foreach(test => {
            val qry = test._1
            val expected = test._2
            val df = sql(qry)
            df.collect() // force an execution
            checkSparkAnswerAndCompareExplainPlan(df, expected)
          })
      }
    }
  }

  test("hash functions") {
    Seq(true, false).foreach { dictionary =>
      withSQLConf(
        "parquet.enable.dictionary" -> dictionary.toString,
        CometConf.COMET_CAST_ALLOW_INCOMPATIBLE.key -> "true") {
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
              |sha2(col, 0), sha2(col, 256), sha2(col, 224), sha2(col, 384), sha2(col, 512), sha2(col, 128)
              |from test
              |""".stripMargin)
        }
      }
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
        CometConf.COMET_CAST_ALLOW_INCOMPATIBLE.key -> "true") {
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
              |sha2(col, 0), sha2(col, 256), sha2(col, 224), sha2(col, 384), sha2(col, 512), sha2(col, 128)
              |from test
              |""".stripMargin)
        }
      }
    }
  }
  test("unary negative integer overflow test") {
    def withAnsiMode(enabled: Boolean)(f: => Unit): Unit = {
      withSQLConf(
        SQLConf.ANSI_ENABLED.key -> enabled.toString,
        CometConf.COMET_ANSI_MODE_ENABLED.key -> enabled.toString,
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
          CometConf.COMET_ANSI_MODE_ENABLED.key -> "true",
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
  test("named_struct") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllTypes(path, dictionaryEnabled = dictionaryEnabled, 10000)
        withParquetTable(path.toString, "tbl") {
          checkSparkAnswerAndOperator("SELECT named_struct('a', _1, 'b', _2) FROM tbl")
          checkSparkAnswerAndOperator("SELECT named_struct('a', _1, 'b', 2) FROM tbl")
          checkSparkAnswerAndOperator(
            "SELECT named_struct('a', named_struct('b', _1, 'c', _2)) FROM tbl")
        }
      }
    }
  }
}
