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

package org.apache.comet.parquet

import java.io.{File, FileFilter}
import java.math.BigDecimal
import java.time.{ZoneId, ZoneOffset}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.schema.MessageTypeParser
import org.apache.spark.SparkException
import org.apache.spark.sql.{CometTestBase, DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.comet.CometBatchScanExec
import org.apache.spark.sql.comet.CometScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.google.common.primitives.UnsignedLong

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.{isSpark34Plus, isSpark40Plus}

abstract class ParquetReadSuite extends CometTestBase {
  import testImplicits._

  testStandardAndLegacyModes("decimals") {
    Seq(true, false).foreach { useDecimal128 =>
      Seq(16, 1024).foreach { batchSize =>
        withSQLConf(
          CometConf.COMET_EXEC_ENABLED.key -> false.toString,
          CometConf.COMET_USE_DECIMAL_128.key -> useDecimal128.toString,
          CometConf.COMET_BATCH_SIZE.key -> batchSize.toString) {
          var combinations = Seq((5, 2), (1, 0), (18, 10), (18, 17), (19, 0), (38, 37))
          // If ANSI mode is on, the combination (1, 1) will cause a runtime error. Otherwise, the
          // decimal RDD contains all null values and should be able to read back from Parquet.

          if (!SQLConf.get.ansiEnabled) {
            combinations = combinations ++ Seq((1, 1))
          }
          for ((precision, scale) <- combinations; useDictionary <- Seq(false, true)) {
            withTempPath { dir =>
              val data = makeDecimalRDD(1000, DecimalType(precision, scale), useDictionary)
              data.write.parquet(dir.getCanonicalPath)
              readParquetFile(dir.getCanonicalPath) { df =>
                {
                  checkAnswer(df, data.collect().toSeq)
                }
              }
            }
          }
        }
      }
    }
  }

  test("simple count") {
    withParquetTable((0 until 10).map(i => (i, i.toString)), "tbl") {
      assert(sql("SELECT * FROM tbl WHERE _1 % 2 == 0").count() == 5)
    }
  }

  test("basic data types") {
    Seq(7, 1024).foreach { batchSize =>
      withSQLConf(CometConf.COMET_BATCH_SIZE.key -> batchSize.toString) {
        val data = (-100 to 100).map { i =>
          (
            i % 2 == 0,
            i,
            i.toByte,
            i.toShort,
            i.toLong,
            i.toFloat,
            i.toDouble,
            DateTimeUtils.toJavaDate(i))
        }
        checkParquetScan(data)
        checkParquetFile(data)
      }
    }
  }

  test("basic data types with dictionary") {
    Seq(7, 1024).foreach { batchSize =>
      withSQLConf(CometConf.COMET_BATCH_SIZE.key -> batchSize.toString) {
        val data = (-100 to 100).map(_ % 4).map { i =>
          (
            i % 2 == 0,
            i,
            i.toByte,
            i.toShort,
            i.toLong,
            i.toFloat,
            i.toDouble,
            DateTimeUtils.toJavaDate(i))
        }
        checkParquetScan(data)
        checkParquetFile(data)
      }
    }
  }

  test("basic filters") {
    val data = (-100 to 100).map { i =>
      (
        i % 2 == 0,
        i,
        i.toByte,
        i.toShort,
        i.toLong,
        i.toFloat,
        i.toDouble,
        DateTimeUtils.toJavaDate(i))
    }
    val filter = (row: Row) => row.getBoolean(0)
    checkParquetScan(data, filter)
    checkParquetFile(data, filter)
  }

  test("raw binary test") {
    val data = (1 to 4).map(i => Tuple1(Array.fill(3)(i.toByte)))
    withParquetDataFrame(data) { df =>
      assertResult(data.map(_._1.mkString(",")).sorted) {
        df.collect().map(_.getAs[Array[Byte]](0).mkString(",")).sorted
      }
    }
  }

  test("string") {
    val data = (1 to 4).map(i => Tuple1(i.toString))
    // Property spark.sql.parquet.binaryAsString shouldn't affect Parquet files written by Spark SQL
    // as we store Spark SQL schema in the extra metadata.
    withSQLConf(SQLConf.PARQUET_BINARY_AS_STRING.key -> "false")(checkParquetFile(data))
    withSQLConf(SQLConf.PARQUET_BINARY_AS_STRING.key -> "true")(checkParquetFile(data))
  }

  test("string with dictionary") {
    Seq((100, 5), (1000, 10)).foreach { case (total, divisor) =>
      val data = (1 to total).map(i => Tuple1((i % divisor).toString))
      // Property spark.sql.parquet.binaryAsString shouldn't affect Parquet files written by Spark SQL
      // as we store Spark SQL schema in the extra metadata.
      withSQLConf(SQLConf.PARQUET_BINARY_AS_STRING.key -> "false")(checkParquetFile(data))
      withSQLConf(SQLConf.PARQUET_BINARY_AS_STRING.key -> "true")(checkParquetFile(data))
    }
  }

  test("long string + reserve additional space for value buffer") {
    withSQLConf(CometConf.COMET_BATCH_SIZE.key -> 16.toString) {
      val data = (1 to 100).map(i => (i, i.toString * 10))
      checkParquetFile(data)
    }
  }

  test("timestamp") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "part-r-0.parquet")
        val expected = makeRawTimeParquetFile(path, dictionaryEnabled = dictionaryEnabled, 10000)
        readParquetFile(path.toString) { df =>
          checkAnswer(
            df.select($"_0", $"_1", $"_2", $"_3", $"_4", $"_5"),
            expected.map {
              case None =>
                Row(null, null, null, null, null, null)
              case Some(i) =>
                val ts = new java.sql.Timestamp(i)
                val ldt = ts.toLocalDateTime
                  .atZone(ZoneId.systemDefault())
                  .withZoneSameInstant(ZoneOffset.UTC)
                  .toLocalDateTime
                Row(ts, ts, ts, ldt, ts, ldt)
            })
        }
      }
    }
  }

  test("timestamp as int96") {
    import testImplicits._

    val N = 100
    val ts = "2020-01-01 01:02:03.123456"
    Seq(false, true).foreach { dictionaryEnabled =>
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
              spark.read.parquet(path.getCanonicalPath).select($"ts".cast("string")),
              Seq.tabulate(N)(_ => Row(ts)))
          }
        }
      }
    }
  }

  test("batch paging on basic types") {
    Seq(1, 2, 4, 9).foreach { batchSize =>
      withSQLConf(CometConf.COMET_BATCH_SIZE.key -> batchSize.toString) {
        val data = (1 to 10).map(i => (i, i.toByte, i.toShort, i.toFloat, i.toDouble, i.toString))
        checkParquetFile(data)
      }
    }
  }

  test("nulls") {
    val allNulls = (
      null.asInstanceOf[java.lang.Boolean],
      null.asInstanceOf[Integer],
      null.asInstanceOf[java.lang.Long],
      null.asInstanceOf[java.lang.Float],
      null.asInstanceOf[java.lang.Double],
      null.asInstanceOf[java.lang.String])

    withParquetDataFrame(allNulls :: Nil) { df =>
      val rows = df.collect()
      assert(rows.length === 1)
      assert(rows.head === Row(Seq.fill(6)(null): _*))
      assert(df.where("_1 is null").count() == 1)
    }
  }

  test("mixed nulls and non-nulls") {
    val rand = scala.util.Random
    val data = (0 to 100).map { i =>
      val row: (Boolean, Integer, java.lang.Long, java.lang.Float, java.lang.Double, String) = {
        if (rand.nextBoolean()) {
          (i % 2 == 0, i, i.toLong, i.toFloat, i.toDouble, i.toString)
        } else {
          (
            null.asInstanceOf[java.lang.Boolean],
            null.asInstanceOf[Integer],
            null.asInstanceOf[java.lang.Long],
            null.asInstanceOf[java.lang.Float],
            null.asInstanceOf[java.lang.Double],
            null.asInstanceOf[String])
        }
      }
      row
    }
    checkParquetFile(data)
  }

  test("test multiple pages with different sizes and nulls") {
    def makeRawParquetFile(
        path: Path,
        dictionaryEnabled: Boolean,
        n: Int,
        pageSize: Int): Seq[Option[Int]] = {
      val schemaStr = {
        if (isSpark34Plus) {
          """
            |message root {
            |  optional boolean                 _1;
            |  optional int32                   _2(INT_8);
            |  optional int32                   _3(INT_16);
            |  optional int32                   _4;
            |  optional int64                   _5;
            |  optional float                   _6;
            |  optional double                  _7;
            |  optional binary                  _8(UTF8);
            |  optional int32                   _9(UINT_8);
            |  optional int32                   _10(UINT_16);
            |  optional int32                   _11(UINT_32);
            |  optional int64                   _12(UINT_64);
            |  optional binary                  _13(ENUM);
            |  optional FIXED_LEN_BYTE_ARRAY(3) _14;
            |}
        """.stripMargin
        } else {
          """
            |message root {
            |  optional boolean                 _1;
            |  optional int32                   _2(INT_8);
            |  optional int32                   _3(INT_16);
            |  optional int32                   _4;
            |  optional int64                   _5;
            |  optional float                   _6;
            |  optional double                  _7;
            |  optional binary                  _8(UTF8);
            |  optional int32                   _9(UINT_8);
            |  optional int32                   _10(UINT_16);
            |  optional int32                   _11(UINT_32);
            |  optional int64                   _12(UINT_64);
            |  optional binary                  _13(ENUM);
            |  optional binary                  _14(UTF8);
            |}
        """.stripMargin
        }
      }

      val schema = MessageTypeParser.parseMessageType(schemaStr)
      val writer = createParquetWriter(
        schema,
        path,
        dictionaryEnabled = dictionaryEnabled,
        pageSize = pageSize,
        dictionaryPageSize = pageSize)

      val rand = scala.util.Random
      val expected = (0 until n).map { i =>
        if (rand.nextBoolean()) {
          None
        } else {
          Some(i)
        }
      }
      expected.foreach { opt =>
        val record = new SimpleGroup(schema)
        opt match {
          case Some(i) =>
            record.add(0, i % 2 == 0)
            record.add(1, i.toByte)
            record.add(2, i.toShort)
            record.add(3, i)
            record.add(4, i.toLong)
            record.add(5, i.toFloat)
            record.add(6, i.toDouble)
            record.add(7, i.toString * 48)
            record.add(8, (-i).toByte)
            record.add(9, (-i).toShort)
            record.add(10, -i)
            record.add(11, (-i).toLong)
            record.add(12, i.toString)
            record.add(13, (i % 10).toString * 3)
          case _ =>
        }
        writer.write(record)
      }

      writer.close()
      expected
    }

    Seq(64, 128, 256, 512, 1024, 4096, 5000).foreach { pageSize =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "part-r-0.parquet")
        val expected = makeRawParquetFile(path, dictionaryEnabled = false, 10000, pageSize)
        readParquetFile(path.toString) { df =>
          checkAnswer(
            df,
            expected.map {
              case None =>
                Row(null, null, null, null, null, null, null, null, null, null, null, null, null,
                  null)
              case Some(i) =>
                val flba_field = if (isSpark34Plus) {
                  Array.fill(3)(i % 10 + 48) // char '0' is 48 in ascii
                } else {
                  (i % 10).toString * 3
                }
                Row(
                  i % 2 == 0,
                  i.toByte,
                  i.toShort,
                  i,
                  i.toLong,
                  i.toFloat,
                  i.toDouble,
                  i.toString * 48,
                  java.lang.Byte.toUnsignedInt((-i).toByte),
                  java.lang.Short.toUnsignedInt((-i).toShort),
                  java.lang.Integer.toUnsignedLong(-i),
                  new BigDecimal(UnsignedLong.fromLongBits((-i).toLong).bigIntegerValue()),
                  i.toString,
                  flba_field)
            })
        }
        readParquetFile(path.toString) { df =>
          assert(
            df.filter("_8 IS NOT NULL AND _4 % 256 == 255").count() ==
              expected.flatten.count(_ % 256 == 255))
        }
      }
    }
  }

  test("vector reloading with all non-null values") {
    def makeRawParquetFile(
        path: Path,
        dictionaryEnabled: Boolean,
        n: Int,
        numNonNulls: Int): Seq[Option[Int]] = {
      val schemaStr =
        """
          |message root {
          | optional int32 _1;
          |}
        """.stripMargin

      val schema = MessageTypeParser.parseMessageType(schemaStr)
      val writer = createParquetWriter(schema, path, dictionaryEnabled = dictionaryEnabled)

      val expected = (0 until n).map { i =>
        if (i >= numNonNulls) {
          None
        } else {
          Some(i)
        }
      }
      expected.foreach { opt =>
        val record = new SimpleGroup(schema)
        opt match {
          case Some(i) =>
            record.add(0, i)
          case _ =>
        }
        writer.write(record)
      }

      writer.close()
      expected
    }

    Seq(2, 99, 1024).foreach { numNonNulls =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "part-r-0.parquet")
        val expected = makeRawParquetFile(path, dictionaryEnabled = false, 1024, numNonNulls)
        withSQLConf(CometConf.COMET_BATCH_SIZE.key -> "2") {
          readParquetFile(path.toString) { df =>
            checkAnswer(
              df,
              expected.map {
                case None =>
                  Row(null)
                case Some(i) =>
                  Row(i)
              })
          }
        }
      }
    }
  }

  test("test lazy materialization skipping") {
    def makeRawParquetFile(
        path: Path,
        dictionaryEnabled: Boolean,
        pageSize: Int,
        pageRowCountLimit: Int,
        expected: Seq[Row]): Unit = {
      val schemaStr =
        """
          |message root {
          |  optional int32   _1;
          |  optional binary  _2(UTF8);
          |}
        """.stripMargin

      val schema = MessageTypeParser.parseMessageType(schemaStr)
      val writer = createParquetWriter(
        schema,
        path,
        dictionaryEnabled = dictionaryEnabled,
        pageSize = pageSize,
        dictionaryPageSize = pageSize,
        pageRowCountLimit = pageRowCountLimit)

      expected.foreach { row =>
        val record = new SimpleGroup(schema)
        record.add(0, row.getInt(0))
        record.add(1, row.getString(1))
        writer.write(record)
      }

      writer.close()
    }

    val skip = Row(0, "a") // row to skip by lazy materialization
    val read = Row(1, "b") // row not to skip
    // The initial page row count is always 100 in ParquetWriter, even with pageRowCountLimit config
    // Thus, use this header to fill in the first 100
    val header = Seq.fill(100)(skip)

    val expected = Seq( // spotless:off
      read, read, read, read, // read all rows in the page
      skip, skip, skip, skip, // skip all rows in the page
      skip, skip, skip, skip, // consecutively skip all rows in the page
      read, skip, skip, read, // skip middle rows in the page
      skip, read, read, skip, // read middle rows in the page
      skip, read, skip, read, // skip and read in turns
      read, skip, read, skip // skip and read in turns
    ) // spotless:on

    withTempDir { dir =>
      val path = new Path(dir.toURI.toString, "part-r-0.parquet")
      withSQLConf(
        CometConf.COMET_BATCH_SIZE.key -> "4",
        CometConf.COMET_EXEC_ENABLED.key -> "false") {
        makeRawParquetFile(path, dictionaryEnabled = false, 1024, 4, header ++ expected)
        readParquetFile(path.toString) { df =>
          checkAnswer(df.filter("_1 != 0"), expected.filter(_.getInt(0) != 0))
        }
      }
    }
  }

  test("test multiple pages with mixed PLAIN_DICTIONARY and PLAIN encoding") {
    // TODO: consider merging this with the same method above
    def makeRawParquetFile(path: Path, n: Int): Seq[Option[Int]] = {
      val dictionaryPageSize = 1024
      val pageRowCount = 500
      val schemaStr =
        """
          |message root {
          |  optional boolean _1;
          |  optional int32   _2(INT_8);
          |  optional int32   _3(INT_16);
          |  optional int32   _4;
          |  optional int64   _5;
          |  optional float   _6;
          |  optional double  _7;
          |  optional binary  _8(UTF8);
          |}
        """.stripMargin

      val schema = MessageTypeParser.parseMessageType(schemaStr)
      val writer = createParquetWriter(
        schema,
        path,
        dictionaryEnabled = true,
        dictionaryPageSize = dictionaryPageSize,
        pageRowCountLimit = pageRowCount)

      val rand = scala.util.Random
      val expected = (0 until n).map { i =>
        // use a single value for the first page, to make sure dictionary encoding kicks in
        val value = if (i < pageRowCount) i % 8 else i
        if (rand.nextBoolean()) None
        else Some(value)
      }

      expected.foreach { opt =>
        val record = new SimpleGroup(schema)
        opt match {
          case Some(i) =>
            record.add(0, i % 2 == 0)
            record.add(1, i.toByte)
            record.add(2, i.toShort)
            record.add(3, i)
            record.add(4, i.toLong)
            record.add(5, i.toFloat)
            record.add(6, i.toDouble)
            record.add(7, i.toString * 100)
          case _ =>
        }
        writer.write(record)
      }

      writer.close()
      expected
    }

    Seq(16, 128).foreach { batchSize =>
      withSQLConf(CometConf.COMET_BATCH_SIZE.key -> batchSize.toString) {
        withTempDir { dir =>
          val path = new Path(dir.toURI.toString, "part-r-0.parquet")
          val expected = makeRawParquetFile(path, 10000)
          readParquetFile(path.toString) { df =>
            checkAnswer(
              df,
              expected.map {
                case None =>
                  Row(null, null, null, null, null, null, null, null)
                case Some(i) =>
                  Row(
                    i % 2 == 0,
                    i.toByte,
                    i.toShort,
                    i,
                    i.toLong,
                    i.toFloat,
                    i.toDouble,
                    i.toString * 100)
              })
          }
        }
      }
    }
  }

  test("skip vector re-loading") {
    Seq(false, true).foreach { enableDictionary =>
      withSQLConf(
        CometConf.COMET_BATCH_SIZE.key -> 7.toString,
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ALL_OPERATOR_ENABLED.key -> "true") {
        // Make sure this works with Comet native execution too
        val data = (1 to 100)
          .map(_ % 5) // trigger dictionary encoding
          .map(i => (i, i.toByte, i.toShort, i.toFloat, i.toDouble, i.toString))
        withParquetTable(data, "tbl", withDictionary = enableDictionary) {
          val df = sql("SELECT count(*) FROM tbl WHERE _1 >= 0")
          checkAnswer(df, Row(100) :: Nil)
        }
      }
    }
  }

  test("partition column types") {
    withTempPath { dir =>
      Seq(1).toDF().repartition(1).write.parquet(dir.getCanonicalPath)

      val dataTypes =
        Seq(
          StringType,
          BooleanType,
          ByteType,
          BinaryType,
          ShortType,
          IntegerType,
          LongType,
          FloatType,
          DoubleType,
          DecimalType(25, 5),
          DateType,
          TimestampType)

      // TODO: support `NullType` here, after we add the support in `ColumnarBatchRow`
      val constantValues =
        Seq(
          UTF8String.fromString("a string"),
          true,
          1.toByte,
          "Spark SQL".getBytes,
          2.toShort,
          3,
          Long.MaxValue,
          0.25.toFloat,
          0.75d,
          Decimal("1234.23456"),
          DateTimeUtils.fromJavaDate(java.sql.Date.valueOf("2015-01-01")),
          DateTimeUtils.fromJavaTimestamp(java.sql.Timestamp.valueOf("2015-01-01 23:50:59.123")))

      dataTypes.zip(constantValues).foreach { case (dt, v) =>
        val schema = StructType(StructField("pcol", dt) :: Nil)
        val conf = SQLConf.get
        val partitionValues = new GenericInternalRow(Array(v))
        val file = dir
          .listFiles(new FileFilter {
            override def accept(pathname: File): Boolean =
              pathname.isFile && pathname.toString.endsWith("parquet")
          })
          .head
        val reader = new BatchReader(
          file.toString,
          CometConf.COMET_BATCH_SIZE.get(conf),
          schema,
          partitionValues)
        reader.init()

        try {
          reader.nextBatch()
          val batch = reader.currentBatch()
          val actual = batch.getRow(0).get(1, dt)
          val expected = v
          if (dt.isInstanceOf[BinaryType]) {
            assert(
              actual.asInstanceOf[Array[Byte]] sameElements expected.asInstanceOf[Array[Byte]])
          } else {
            assert(actual == expected)
          }
        } finally {
          reader.close()
        }
      }
    }
  }

  test("partition columns - multiple batch") {
    withSQLConf(
      CometConf.COMET_BATCH_SIZE.key -> 7.toString,
      CometConf.COMET_EXEC_ENABLED.key -> "false",
      CometConf.COMET_EXEC_ALL_OPERATOR_ENABLED.key -> "true",
      CometConf.COMET_ENABLED.key -> "true") {
      Seq("a", null).foreach { partValue =>
        withTempPath { dir =>
          (1 to 100)
            .map(v => (partValue.asInstanceOf[String], v))
            .toDF("pcol", "col")
            .repartition(1)
            .write
            .format("parquet")
            .partitionBy("pcol")
            .save(dir.getCanonicalPath)
          val df = spark.read.format("parquet").load(dir.getCanonicalPath)
          assert(df.filter("col > 90").count() == 10)
        }
      }
    }
  }

  test("fix: string partition column with incorrect offset buffer") {
    def makeRawParquetFile(
        path: Path,
        dictionaryEnabled: Boolean,
        n: Int,
        pageSize: Int): Seq[Option[Int]] = {
      val schemaStr =
        """
          |message root {
          |  optional binary                  _1(UTF8);
          |}
    """.stripMargin

      val schema = MessageTypeParser.parseMessageType(schemaStr)
      val writer = createParquetWriter(
        schema,
        path,
        dictionaryEnabled = dictionaryEnabled,
        pageSize = pageSize,
        dictionaryPageSize = pageSize,
        rowGroupSize = 1024 * 128)

      val rand = scala.util.Random
      val expected = (0 until n).map { i =>
        if (rand.nextBoolean()) {
          None
        } else {
          Some(i)
        }
      }
      expected.foreach { opt =>
        val record = new SimpleGroup(schema)
        opt match {
          case Some(i) =>
            record.add(0, i.toString * 48)
          case _ =>
        }
        writer.write(record)
      }

      writer.close()
      expected
    }

    withTable("tbl") {
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "part-r-0.parquet")
        makeRawParquetFile(path, false, 10000, 128)

        sql("CREATE TABLE tbl (value STRING, p STRING) USING PARQUET PARTITIONED BY (p) ")
        sql(s"ALTER TABLE tbl ADD PARTITION (p='a') LOCATION '$dir'")
        assert(sql("SELECT DISTINCT p FROM tbl").count() == 1)
      }
    }

  }

  test("missing columns") {
    withTempPath { dir =>
      Seq("a", "b").toDF("col1").write.parquet(dir.getCanonicalPath)

      // Create a schema where `col2` doesn't exist in the file schema
      var schema =
        StructType(Seq(StructField("col1", StringType), StructField("col2", IntegerType)))
      var df = spark.read.schema(schema).parquet(dir.getCanonicalPath)
      checkAnswer(df, Row("a", null) :: Row("b", null) :: Nil)

      // Should be the same when the missing column is at the beginning of the schema

      schema = StructType(Seq(StructField("col0", BooleanType), StructField("col1", StringType)))
      df = spark.read.schema(schema).parquet(dir.getCanonicalPath)
      checkAnswer(df, Row(null, "a") :: Row(null, "b") :: Nil)
    }
  }

  test("unsigned int supported") {
    Seq(true, false).foreach { dictionaryEnabled =>
      def makeRawParquetFile(path: Path): Unit = {
        val schemaStr =
          """message root {
            |  required INT32 a(UINT_8);
            |  required INT32 b(UINT_16);
            |  required INT32 c(UINT_32);
            |}
        """.stripMargin
        val schema = MessageTypeParser.parseMessageType(schemaStr)

        val writer = createParquetWriter(schema, path, dictionaryEnabled)

        (0 until 10).foreach { n =>
          val record = new SimpleGroup(schema)
          record.add(0, n.toByte + Byte.MaxValue)
          record.add(1, n.toShort + Short.MaxValue)
          record.add(2, n + Int.MaxValue)
          writer.write(record)
        }
        writer.close()
      }

      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "part-r-0.parquet")
        makeRawParquetFile(path)
        readParquetFile(path.toString) { df =>
          checkAnswer(
            df,
            (0 until 10).map(n =>
              Row(n.toByte + Byte.MaxValue, n.toShort + Short.MaxValue, n + Int.MaxValue.toLong)))
        }
      }
    }
  }

  test("unsigned long supported") {
    Seq(true, false).foreach { dictionaryEnabled =>
      def makeRawParquetFile(path: Path): Unit = {
        val schemaStr =
          """message root {
            |  required INT64 a(UINT_64);
            |}
        """.stripMargin
        val schema = MessageTypeParser.parseMessageType(schemaStr)

        val writer = createParquetWriter(schema, path, dictionaryEnabled)

        (0 until 10).map(_.toLong).foreach { n =>
          val record = new SimpleGroup(schema)
          record.add(0, n + Long.MaxValue)
          writer.write(record)
        }
        writer.close()
      }

      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "part-r-0.parquet")
        makeRawParquetFile(path)
        readParquetFile(path.toString) { df =>
          checkAnswer(
            df,
            (0 until 10).map(n =>
              Row(
                new BigDecimal(UnsignedLong.fromLongBits(n + Long.MaxValue).bigIntegerValue()))))
        }
      }
    }
  }

  test("enum support") {
    // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
    // "enum type should interpret ENUM annotated field as a UTF-8"
    Seq(true, false).foreach { dictionaryEnabled =>
      def makeRawParquetFile(path: Path): Unit = {
        val schemaStr =
          """message root {
            |  required BINARY a(ENUM);
            |}
        """.stripMargin
        val schema = MessageTypeParser.parseMessageType(schemaStr)

        val writer = createParquetWriter(schema, path, dictionaryEnabled)

        (0 until 10).map(_.toLong).foreach { n =>
          val record = new SimpleGroup(schema)
          record.add(0, n.toString)
          writer.write(record)
        }
        writer.close()
      }

      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "part-r-0.parquet")
        makeRawParquetFile(path)
        readParquetFile(path.toString) { df =>
          checkAnswer(df, (0 until 10).map(n => Row(n.toString)))
        }
      }
    }
  }

  test("FIXED_LEN_BYTE_ARRAY support") {
    assume(isSpark34Plus)
    Seq(true, false).foreach { dictionaryEnabled =>
      def makeRawParquetFile(path: Path): Unit = {
        val schemaStr =
          """message root {
            |  required FIXED_LEN_BYTE_ARRAY(1) a;
            |  required FIXED_LEN_BYTE_ARRAY(3) b;
            |}
        """.stripMargin
        val schema = MessageTypeParser.parseMessageType(schemaStr)

        val writer = createParquetWriter(schema, path, dictionaryEnabled)

        (0 until 10).map(_.toString).foreach { n =>
          val record = new SimpleGroup(schema)
          record.add(0, n)
          record.add(1, n + n + n)
          writer.write(record)
        }
        writer.close()
      }

      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "part-r-0.parquet")
        makeRawParquetFile(path)
        readParquetFile(path.toString) { df =>
          checkAnswer(
            df,
            (48 until 58).map(n => // char '0' is 48 in ascii
              Row(Array(n), Array(n, n, n))))
        }
      }
    }
  }

  test("schema evolution") {
    Seq(true, false).foreach { enableSchemaEvolution =>
      Seq(true, false).foreach { useDictionary =>
        {
          withSQLConf(
            CometConf.COMET_SCHEMA_EVOLUTION_ENABLED.key -> enableSchemaEvolution.toString) {
            val data = (0 until 100).map(i => {
              val v = if (useDictionary) i % 5 else i
              (v, v.toFloat)
            })
            val readSchema =
              StructType(
                Seq(StructField("_1", LongType, false), StructField("_2", DoubleType, false)))

            withParquetDataFrame(data, schema = Some(readSchema)) { df =>
              if (enableSchemaEvolution) {
                checkAnswer(df, data.map(Row.fromTuple))
              } else {
                assertThrows[SparkException](df.collect())
              }
            }
          }
        }
      }
    }
  }

  test("scan metrics") {
    val metricNames = Seq(
      "ParquetRowGroups",
      "ParquetNativeDecodeTime",
      "ParquetNativeLoadTime",
      "ParquetLoadRowGroupTime",
      "ParquetInputFileReadTime",
      "ParquetInputFileReadSize",
      "ParquetInputFileReadThroughput")

    withParquetTable((0 until 10000).map(i => (i, i.toDouble)), "tbl") {
      val df = sql("SELECT * FROM tbl WHERE _1 > 0")
      val scans = df.queryExecution.executedPlan collect {
        case s: CometScanExec => s
        case s: CometBatchScanExec => s
      }
      assert(scans.size == 1, s"Expect one scan node but found ${scans.size}")
      val metrics = scans.head.metrics
      metricNames.foreach { metricName =>
        assert(metrics.contains(metricName), s"metric $metricName was not found")
      }

      df.collect()

      metricNames.foreach { metricName =>
        assert(
          metrics(metricName).value > 0,
          s"Expect metric value for $metricName to be positive")
      }
    }
  }

  test("read dictionary encoded decimals written as FIXED_LEN_BYTE_ARRAY") {
    // In this test, data is encoded using Parquet page v2 format, but with PLAIN encoding
    checkAnswer(
      // Decimal column in this file is encoded using plain dictionary
      readResourceParquetFile("test-data/dec-in-fixed-len.parquet"),
      spark.range(1 << 4).select('id % 10 cast DecimalType(10, 2) as 'fixed_len_dec))
  }

  test("read long decimals with precision <= 9") {
    // decimal32-written-as-64-bit.snappy.parquet was generated using a 3rd-party library. It has
    // 10 rows of Decimal(9, 1) written as LongDecimal instead of an IntDecimal
    var df = readResourceParquetFile("test-data/decimal32-written-as-64-bit.snappy.parquet")
    assert(10 == df.collect().length)
    var first10Df = df.head(10)
    assert(
      Seq(792059492, 986842987, 540247998, null, 357991078, 494131059, 92536396, 426847157,
        -999999999, 204486094)
        .zip(first10Df)
        .forall(d =>
          d._2.isNullAt(0) && d._1 == null ||
            d._1 == d._2.getDecimal(0).unscaledValue().intValue()))

    // decimal32-written-as-64-bit-dict.snappy.parquet was generated using a 3rd-party library. It
    // has 2048 rows of Decimal(3, 1) written as LongDecimal instead of an IntDecimal
    df = readResourceParquetFile("test-data/decimal32-written-as-64-bit-dict.snappy.parquet")
    assert(2048 == df.collect().length)
    first10Df = df.head(10)
    assert(
      Seq(751, 937, 511, null, 337, 467, 84, 403, -999, 190)
        .zip(first10Df)
        .forall(d =>
          d._2.isNullAt(0) && d._1 == null ||
            d._1 == d._2.getDecimal(0).unscaledValue().intValue()))

    val last10Df = df.tail(10)
    assert(
      Seq(866, 20, 492, 76, 824, 604, 343, 820, 864, 243)
        .zip(last10Df)
        .forall(d => d._1 == d._2.getDecimal(0).unscaledValue().intValue()))
  }

  private val actions: Seq[DataFrame => DataFrame] = Seq(
    "_1 = 500",
    "_1 = 500 or _1 = 1500",
    "_1 = 500 or _1 = 501 or _1 = 1500",
    "_1 = 500 or _1 = 501 or _1 = 1000 or _1 = 1500",
    "_1 >= 500 and _1 < 1000",
    "(_1 >= 500 and _1 < 1000) or (_1 >= 1500 and _1 < 1600)").map(f =>
    (df: DataFrame) => df.filter(f))

  test("test lazy materialization when batch size is small") {
    val df = spark.range(0, 2000).selectExpr("id as _1", "cast(id as string) as _11")
    checkParquetDataFrame(df)(actions: _*)
  }

  test("test lazy materialization when batch size is small (dict encode)") {
    val df = spark.range(0, 2000).selectExpr("id as _1", "cast(id % 10 as string) as _11")
    checkParquetDataFrame(df)(actions: _*)
  }

  private def testStandardAndLegacyModes(testName: String)(f: => Unit): Unit = {
    test(s"Standard mode - $testName") {
      withSQLConf(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key -> "false") { f }
    }

    test(s"Legacy mode - $testName") {
      withSQLConf(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key -> "true") { f }
    }
  }

  private def checkParquetFile[T <: Product: ClassTag: TypeTag](
      data: Seq[T],
      f: Row => Boolean = _ => true): Unit = {
    withParquetDataFrame(data)(r => checkAnswer(r.filter(f), data.map(Row.fromTuple).filter(f)))
  }

  protected def checkParquetScan[T <: Product: ClassTag: TypeTag](
      data: Seq[T],
      f: Row => Boolean = _ => true): Unit

  /**
   * create parquet file with various page sizes and batch sizes
   */
  private def checkParquetDataFrame(df: DataFrame)(actions: (DataFrame => DataFrame)*): Unit = {
    Seq(true, false).foreach { enableDictionary =>
      Seq(64, 127, 4049).foreach { pageSize =>
        withTempPath(file => {
          df.coalesce(1)
            .write
            .option("parquet.page.size", pageSize.toString)
            .option("parquet.enable.dictionary", enableDictionary.toString)
            .parquet(file.getCanonicalPath)

          Seq(true, false).foreach { useLazyMaterialization =>
            Seq(true, false).foreach { enableCometExec =>
              Seq(4, 13, 4049).foreach { batchSize =>
                withSQLConf(
                  CometConf.COMET_BATCH_SIZE.key -> batchSize.toString,
                  CometConf.COMET_EXEC_ENABLED.key -> enableCometExec.toString,
                  CometConf.COMET_USE_LAZY_MATERIALIZATION.key -> useLazyMaterialization.toString) {
                  readParquetFile(file.getCanonicalPath) { parquetDf =>
                    actions.foreach { action =>
                      checkAnswer(action(parquetDf), action(df))
                    }
                  }
                }
              }
            }
          }
        })
      }
    }
  }

  test("row group skipping doesn't overflow when reading into larger type") {
    // Spark 4.0 no longer fails for widening types SPARK-40876
    // https://github.com/apache/spark/commit/3361f25dc0ff6e5233903c26ee105711b79ba967
    assume(isSpark34Plus && !isSpark40Plus)

    withTempPath { path =>
      Seq(0).toDF("a").write.parquet(path.toString)
      // Reading integer 'a' as a long isn't supported. Check that an exception is raised instead
      // of incorrectly skipping the single row group and producing incorrect results.
      val exception = intercept[SparkException] {
        spark.read
          .schema("a LONG")
          .parquet(path.toString)
          .where(s"a < ${Long.MaxValue}")
          .collect()
      }
      assert(exception.getMessage.contains("Column: [a], Expected: bigint, Found: INT32"))
    }
  }

  test("test pre-fetching multiple files") {
    def makeRawParquetFile(
        path: Path,
        dictionaryEnabled: Boolean,
        n: Int,
        pageSize: Int): Seq[Option[Int]] = {
      val schemaStr =
        """
          |message root {
          |  optional boolean _1;
          |  optional int32   _2(INT_8);
          |  optional int32   _3(INT_16);
          |  optional int32   _4;
          |  optional int64   _5;
          |  optional float   _6;
          |  optional double  _7;
          |  optional binary  _8(UTF8);
          |  optional int32   _9(UINT_8);
          |  optional int32   _10(UINT_16);
          |  optional int32   _11(UINT_32);
          |  optional int64   _12(UINT_64);
          |  optional binary  _13(ENUM);
          |}
        """.stripMargin

      val schema = MessageTypeParser.parseMessageType(schemaStr)
      val writer = createParquetWriter(
        schema,
        path,
        dictionaryEnabled = dictionaryEnabled,
        pageSize = pageSize,
        dictionaryPageSize = pageSize)

      val rand = scala.util.Random
      val expected = (0 until n).map { i =>
        if (rand.nextBoolean()) {
          None
        } else {
          Some(i)
        }
      }
      expected.foreach { opt =>
        val record = new SimpleGroup(schema)
        opt match {
          case Some(i) =>
            record.add(0, i % 2 == 0)
            record.add(1, i.toByte)
            record.add(2, i.toShort)
            record.add(3, i)
            record.add(4, i.toLong)
            record.add(5, i.toFloat)
            record.add(6, i.toDouble)
            record.add(7, i.toString * 48)
            record.add(8, (-i).toByte)
            record.add(9, (-i).toShort)
            record.add(10, -i)
            record.add(11, (-i).toLong)
            record.add(12, i.toString)
          case _ =>
        }
        writer.write(record)
      }

      writer.close()
      expected
    }

    val conf = new Configuration()
    conf.set("spark.comet.scan.preFetch.enabled", "true");
    conf.set("spark.comet.scan.preFetch.threadNum", "4");

    withTempDir { dir =>
      val threadPool = CometPrefetchThreadPool.getOrCreateThreadPool(2)

      val readers = (0 to 10).map { idx =>
        val path = new Path(dir.toURI.toString, s"part-r-$idx.parquet")
        makeRawParquetFile(path, dictionaryEnabled = false, 10000, 500)

        val reader = new BatchReader(conf, path.toString, 1000, null, null)
        reader.submitPrefetchTask(threadPool)

        reader
      }

      // Wait for all pre-fetch tasks
      readers.foreach { reader =>
        val task = reader.getPrefetchTask()
        task.get()
      }

      val totolRows = readers.map { reader =>
        val queue = reader.getPrefetchQueue()
        var rowCount = 0L

        while (!queue.isEmpty) {
          val rowGroup = queue.take().getLeft
          rowCount += rowGroup.getRowCount
        }

        reader.close()

        rowCount
      }.sum

      readParquetFile(dir.toString) { df =>
        assert(df.count() == totolRows)
      }
    }
  }
  def testScanner(cometEnabled: String, scanner: String, v1: Option[String] = None): Unit = {
    withSQLConf(
      CometConf.COMET_ENABLED.key -> cometEnabled,
      CometConf.COMET_EXEC_ENABLED.key -> cometEnabled,
      CometConf.COMET_EXEC_ALL_OPERATOR_ENABLED.key -> cometEnabled,
      SQLConf.USE_V1_SOURCE_LIST.key -> v1.getOrElse("")) {
      withParquetTable(Seq((Long.MaxValue, 1), (Long.MaxValue, 2)), "tbl") {
        val df = spark.sql("select * from tbl")
        assert(
          stripAQEPlan(df.queryExecution.executedPlan)
            .collectLeaves()
            .head
            .toString()
            .startsWith(scanner))
      }
    }
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    Seq(true, false).foreach { prefetch =>
      val cometTestName = if (prefetch) {
        testName + " (prefetch enabled)"
      } else {
        testName
      }

      super.test(cometTestName, testTags: _*) {
        withSQLConf(CometConf.COMET_SCAN_PREFETCH_ENABLED.key -> prefetch.toString) {
          testFun
        }
      }
    }
  }
}

class ParquetReadV1Suite extends ParquetReadSuite with AdaptiveSparkPlanHelper {
  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*)(withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
      testFun
    })(pos)
  }

  override def checkParquetScan[T <: Product: ClassTag: TypeTag](
      data: Seq[T],
      f: Row => Boolean = _ => true): Unit = {
    withParquetDataFrame(data) { r =>
      val scans = collect(r.filter(f).queryExecution.executedPlan) { case p: CometScanExec =>
        p
      }
      if (CometConf.COMET_ENABLED.get()) {
        assert(scans.nonEmpty)
      } else {
        assert(scans.isEmpty)
      }
    }
  }

  test("Test V1 parquet scan uses respective scanner") {
    Seq(("false", "FileScan parquet"), ("true", "CometScan parquet")).foreach {
      case (cometEnabled, expectedScanner) =>
        testScanner(cometEnabled, scanner = expectedScanner, v1 = Some("parquet"))
    }
  }
}

class ParquetReadV2Suite extends ParquetReadSuite with AdaptiveSparkPlanHelper {
  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*)(withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      testFun
    })(pos)
  }

  override def checkParquetScan[T <: Product: ClassTag: TypeTag](
      data: Seq[T],
      f: Row => Boolean = _ => true): Unit = {
    withParquetDataFrame(data) { r =>
      val scans = collect(r.filter(f).queryExecution.executedPlan) { case p: CometBatchScanExec =>
        p.scan
      }
      if (CometConf.COMET_ENABLED.get()) {
        assert(scans.nonEmpty && scans.forall(_.isInstanceOf[CometParquetScan]))
      } else {
        assert(!scans.exists(_.isInstanceOf[CometParquetScan]))
      }
    }
  }

  test("Test V2 parquet scan uses respective scanner") {
    Seq(("false", "BatchScan"), ("true", "CometBatchScan")).foreach {
      case (cometEnabled, expectedScanner) =>
        testScanner(cometEnabled, scanner = expectedScanner, v1 = None)
    }
  }
}
