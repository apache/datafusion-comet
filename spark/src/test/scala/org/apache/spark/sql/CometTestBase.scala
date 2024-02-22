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

package org.apache.spark.sql

import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import org.scalactic.source.Position
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Tag

import org.apache.hadoop.fs.Path
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.schema.{MessageType, MessageTypeParser}
import org.apache.spark._
import org.apache.spark.sql.comet.{CometBatchScanExec, CometBroadcastExchangeExec, CometExec, CometScanExec, CometScanWrapper, CometSinkPlaceHolder}
import org.apache.spark.sql.comet.execution.shuffle.{CometColumnarShuffle, CometNativeShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.execution.{ColumnarToRowExec, InputAdapter, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal._
import org.apache.spark.sql.test._
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.StructType

import org.apache.comet._
import org.apache.comet.CometSparkSessionExtensions.isSpark34Plus

/**
 * Base class for testing. This exists in `org.apache.spark.sql` since [[SQLTestUtils]] is
 * package-private.
 */
abstract class CometTestBase
    extends QueryTest
    with SQLTestUtils
    with BeforeAndAfterEach
    with AdaptiveSparkPlanHelper {
  import testImplicits._

  protected val shuffleManager: String =
    "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager"

  protected def sparkConf: SparkConf = {
    val conf = new SparkConf()
    conf.set("spark.hadoop.fs.file.impl", classOf[DebugFilesystem].getName)
    conf.set(SQLConf.SHUFFLE_PARTITIONS, 10) // reduce parallelism in tests
    conf.set("spark.shuffle.manager", shuffleManager)
    conf.set(CometConf.COMET_MEMORY_OVERHEAD.key, "2g")
    conf
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ALL_OPERATOR_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ALL_EXPR_ENABLED.key -> "true",
        CometConf.COMET_COLUMNAR_SHUFFLE_MEMORY_SIZE.key -> "2g",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "1g",
        SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "1g",
        SQLConf.ANSI_ENABLED.key -> "false") {
        testFun
      }
    }
  }

  /**
   * A helper function for comparing Comet DataFrame with Spark result using absolute tolerance.
   */
  protected def checkAnswerWithTol(
      dataFrame: DataFrame,
      expectedAnswer: Seq[Row],
      absTol: Double): Unit = {
    val actualAnswer = dataFrame.collect()
    require(
      actualAnswer.length == expectedAnswer.length,
      s"actual num rows ${actualAnswer.length} != expected num of rows ${expectedAnswer.length}")

    actualAnswer.zip(expectedAnswer).foreach { case (actualRow, expectedRow) =>
      checkAnswerWithTol(actualRow, expectedRow, absTol)
    }
  }

  /**
   * Compares two answers and makes sure the answer is within absTol of the expected result.
   */
  protected def checkAnswerWithTol(
      actualAnswer: Row,
      expectedAnswer: Row,
      absTol: Double): Unit = {
    require(
      actualAnswer.length == expectedAnswer.length,
      s"actual answer length ${actualAnswer.length} != " +
        s"expected answer length ${expectedAnswer.length}")
    require(absTol > 0 && absTol <= 1e-6, s"absTol $absTol is out of range (0, 1e-6]")

    actualAnswer.toSeq.zip(expectedAnswer.toSeq).foreach {
      case (actual: Double, expected: Double) =>
        if (!actual.isNaN && !expected.isNaN) {
          assert(
            math.abs(actual - expected) < absTol,
            s"actual answer $actual not within $absTol of correct answer $expected")
        }
      case (actual, expected) =>
        assert(actual == expected, s"$actualAnswer did not equal $expectedAnswer")
    }
  }

  protected def checkSparkAnswer(query: String): Unit = {
    checkSparkAnswer(sql(query))
  }

  protected def checkSparkAnswer(df: => DataFrame): Unit = {
    var expected: Array[Row] = Array.empty
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      val dfSpark = Dataset.ofRows(spark, df.logicalPlan)
      expected = dfSpark.collect()
    }
    val dfComet = Dataset.ofRows(spark, df.logicalPlan)
    checkAnswer(dfComet, expected)
  }

  protected def checkSparkAnswerAndOperator(query: String, excludedClasses: Class[_]*): Unit = {
    checkSparkAnswerAndOperator(sql(query), excludedClasses: _*)
  }

  protected def checkSparkAnswerAndOperator(
      df: => DataFrame,
      excludedClasses: Class[_]*): Unit = {
    checkCometOperators(stripAQEPlan(df.queryExecution.executedPlan), excludedClasses: _*)
    checkSparkAnswer(df)
  }

  protected def checkCometOperators(plan: SparkPlan, excludedClasses: Class[_]*): Unit = {
    plan.foreach {
      case _: CometScanExec | _: CometBatchScanExec => true
      case _: CometSinkPlaceHolder | _: CometScanWrapper => false
      case _: CometExec | _: CometShuffleExchangeExec => true
      case _: CometBroadcastExchangeExec => true
      case _: WholeStageCodegenExec | _: ColumnarToRowExec | _: InputAdapter => true
      case op =>
        if (excludedClasses.exists(c => c.isAssignableFrom(op.getClass))) {
          true
        } else {
          assert(
            false,
            s"Expected only Comet native operators, but found ${op.nodeName}.\n" +
              s"plan: $plan")
        }
    }
  }

  /**
   * Check the answer of a Comet SQL query with Spark result using absolute tolerance.
   */
  protected def checkSparkAnswerWithTol(query: String, absTol: Double = 1e-6): Unit = {
    checkSparkAnswerWithTol(sql(query), absTol)
  }

  /**
   * Check the answer of a Comet DataFrame with Spark result using absolute tolerance.
   */
  protected def checkSparkAnswerWithTol(df: => DataFrame, absTol: Double): Unit = {
    var expected: Array[Row] = Array.empty
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      val dfSpark = Dataset.ofRows(spark, df.logicalPlan)
      expected = dfSpark.collect()
    }
    val dfComet = Dataset.ofRows(spark, df.logicalPlan)
    checkAnswerWithTol(dfComet, expected, absTol: Double)
  }

  private var _spark: SparkSession = _
  protected implicit def spark: SparkSession = _spark
  protected implicit def sqlContext: SQLContext = _spark.sqlContext

  override protected def sparkContext: SparkContext = {
    SparkContext.clearActiveContext()

    val conf = sparkConf

    if (!conf.contains("spark.master")) {
      conf.setMaster("local[5]")
    }

    if (!conf.contains("spark.app.name")) {
      conf.setAppName(java.util.UUID.randomUUID().toString)
    }

    SparkContext.getOrCreate(conf)
  }

  protected def createSparkSession: SparkSession = {
    SparkSession.cleanupAnyExistingSession()

    SparkSession
      .builder()
      .config(
        sparkContext.getConf
      ) // Don't use `sparkConf` as we can have overridden it in plugin
      .withExtensions(new CometSparkSessionExtensions)
      .getOrCreate()
  }

  protected def initializeSession(): Unit = {
    if (_spark == null) _spark = createSparkSession
  }

  protected override def beforeAll(): Unit = {
    initializeSession()
    super.beforeAll()
  }

  protected override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      if (_spark != null) {
        try {
          _spark.stop()
        } finally {
          _spark = null
          SparkSession.clearActiveSession()
          SparkSession.clearDefaultSession()
        }
      }
    }
  }

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    DebugFilesystem.clearOpenStreams()
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    spark.sharedState.cacheManager.clearCache()
    eventually(timeout(10.seconds), interval(2.seconds)) {
      DebugFilesystem.assertNoOpenStreams()
    }
  }

  protected def readResourceParquetFile(name: String): DataFrame = {
    spark.read.parquet(getResourceParquetFilePath(name))
  }

  protected def getResourceParquetFilePath(name: String): String = {
    Thread.currentThread().getContextClassLoader.getResource(name).toString
  }

  protected def withParquetDataFrame[T <: Product: ClassTag: TypeTag](
      data: Seq[T],
      withDictionary: Boolean = true,
      schema: Option[StructType] = None)(f: DataFrame => Unit): Unit = {
    withParquetFile(data, withDictionary)(path => readParquetFile(path, schema)(f))
  }

  protected def withParquetTable[T <: Product: ClassTag: TypeTag](
      data: Seq[T],
      tableName: String,
      withDictionary: Boolean = true)(f: => Unit): Unit = {
    withParquetDataFrame(data, withDictionary) { df =>
      df.createOrReplaceTempView(tableName)
      withTempView(tableName)(f)
    }
  }

  protected def withParquetTable(df: DataFrame, tableName: String)(f: => Unit): Unit = {
    df.createOrReplaceTempView(tableName)
    withTempView(tableName)(f)
  }

  protected def withParquetTable(path: String, tableName: String)(f: => Unit): Unit = {
    val df = spark.read.format("parquet").load(path)
    withParquetTable(df, tableName)(f)
  }

  protected def withParquetFile[T <: Product: ClassTag: TypeTag](
      data: Seq[T],
      withDictionary: Boolean = true)(f: String => Unit): Unit = {
    withTempPath { file =>
      spark
        .createDataFrame(data)
        .write
        .option("parquet.enable.dictionary", withDictionary.toString)
        .parquet(file.getCanonicalPath)
      f(file.getCanonicalPath)
    }
  }

  protected def readParquetFile(path: String, schema: Option[StructType] = None)(
      f: DataFrame => Unit): Unit = schema match {
    case Some(s) => f(spark.read.format("parquet").schema(s).load(path))
    case None => f(spark.read.format("parquet").load(path))
  }

  protected def createParquetWriter(
      schema: MessageType,
      path: Path,
      dictionaryEnabled: Boolean = false,
      pageSize: Int = 1024,
      dictionaryPageSize: Int = 1024,
      pageRowCountLimit: Int = ParquetProperties.DEFAULT_PAGE_ROW_COUNT_LIMIT,
      rowGroupSize: Long = 1024 * 1024L): ParquetWriter[Group] = {
    val hadoopConf = spark.sessionState.newHadoopConf()

    ExampleParquetWriter
      .builder(path)
      .withDictionaryEncoding(dictionaryEnabled)
      .withType(schema)
      .withRowGroupSize(rowGroupSize.toInt)
      .withPageSize(pageSize)
      .withDictionaryPageSize(dictionaryPageSize)
      .withPageRowCountLimit(pageRowCountLimit)
      .withConf(hadoopConf)
      .build()
  }

  // Maps `i` to both positive and negative to test timestamp after and before the Unix epoch
  protected def getValue(i: Long, div: Long): Long = {
    val value = if (i % 2 == 0) i else -i
    value % div
  }

  def makeParquetFileAllTypes(path: Path, dictionaryEnabled: Boolean, n: Int): Unit = {
    makeParquetFileAllTypes(path, dictionaryEnabled, 0, n)
  }

  def makeParquetFileAllTypes(
      path: Path,
      dictionaryEnabled: Boolean,
      begin: Int,
      end: Int,
      pageSize: Int = 128,
      randomSize: Int = 0): Unit = {
    val schemaStr =
      if (isSpark34Plus) {
        """
          |message root {
          |  optional boolean                  _1;
          |  optional int32                    _2(INT_8);
          |  optional int32                    _3(INT_16);
          |  optional int32                    _4;
          |  optional int64                    _5;
          |  optional float                    _6;
          |  optional double                   _7;
          |  optional binary                   _8(UTF8);
          |  optional int32                    _9(UINT_8);
          |  optional int32                    _10(UINT_16);
          |  optional int32                    _11(UINT_32);
          |  optional int64                    _12(UINT_64);
          |  optional binary                   _13(ENUM);
          |  optional FIXED_LEN_BYTE_ARRAY(3)  _14;
          |  optional int32                    _15(DECIMAL(5, 2));
          |  optional int64                    _16(DECIMAL(18, 10));
          |  optional FIXED_LEN_BYTE_ARRAY(16) _17(DECIMAL(38, 37));
          |  optional INT64                    _18(TIMESTAMP(MILLIS,true));
          |  optional INT64                    _19(TIMESTAMP(MICROS,true));
          |  optional INT32                    _20(DATE);
          |}
        """.stripMargin
      } else {
        """
          |message root {
          |  optional boolean                  _1;
          |  optional int32                    _2(INT_8);
          |  optional int32                    _3(INT_16);
          |  optional int32                    _4;
          |  optional int64                    _5;
          |  optional float                    _6;
          |  optional double                   _7;
          |  optional binary                   _8(UTF8);
          |  optional int32                    _9(UINT_8);
          |  optional int32                    _10(UINT_16);
          |  optional int32                    _11(UINT_32);
          |  optional int64                    _12(UINT_64);
          |  optional binary                   _13(ENUM);
          |  optional binary                   _14(UTF8);
          |  optional int32                    _15(DECIMAL(5, 2));
          |  optional int64                    _16(DECIMAL(18, 10));
          |  optional FIXED_LEN_BYTE_ARRAY(16) _17(DECIMAL(38, 37));
          |  optional INT64                    _18(TIMESTAMP(MILLIS,true));
          |  optional INT64                    _19(TIMESTAMP(MICROS,true));
          |  optional INT32                    _20(DATE);
          |}
        """.stripMargin
      }

    val schema = MessageTypeParser.parseMessageType(schemaStr)
    val writer = createParquetWriter(
      schema,
      path,
      dictionaryEnabled = dictionaryEnabled,
      pageSize = pageSize,
      dictionaryPageSize = pageSize)

    val rand = scala.util.Random
    val data = (begin until end).map { i =>
      if (rand.nextBoolean()) {
        None
      } else {
        if (dictionaryEnabled) Some(i % 4) else Some(i)
      }
    }
    data.foreach { opt =>
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
          record.add(13, ((i % 10).toString * 3).take(3))
          record.add(14, i)
          record.add(15, i.toLong)
          record.add(16, ((i % 10).toString * 16).take(16))
          record.add(17, i.toLong)
          record.add(18, i.toLong)
          record.add(19, i)
        case _ =>
      }
      writer.write(record)
    }
    (0 until randomSize).foreach { _ =>
      val i = rand.nextLong()
      val record = new SimpleGroup(schema)
      record.add(0, i % 2 == 0)
      record.add(1, i.toByte)
      record.add(2, i.toShort)
      record.add(3, i.toInt)
      record.add(4, i)
      record.add(5, java.lang.Float.intBitsToFloat(i.toInt))
      record.add(6, java.lang.Double.longBitsToDouble(i))
      record.add(7, i.toString * 24)
      record.add(8, (-i).toByte)
      record.add(9, (-i).toShort)
      record.add(10, (-i).toInt)
      record.add(11, -i)
      record.add(12, i.toString)
      record.add(13, i.toString.take(3).padTo(3, '0'))
      record.add(14, i.toInt % 100000)
      record.add(15, i % 1000000000000000000L)
      record.add(16, i.toString.take(16).padTo(16, '0'))
      record.add(17, i)
      record.add(18, i)
      record.add(19, i.toInt)
      writer.write(record)
    }

    writer.close()
  }

  protected def makeRawTimeParquetFileColumns(
      path: Path,
      dictionaryEnabled: Boolean,
      n: Int,
      rowGroupSize: Long = 1024 * 1024L): Seq[Option[Long]] = {
    val schemaStr =
      """
        |message root {
        |  optional int64 _0(INT_64);
        |  optional int64 _1(INT_64);
        |  optional int64 _2(INT_64);
        |  optional int64 _3(INT_64);
        |  optional int64 _4(INT_64);
        |  optional int64 _5(INT_64);
        |}
        """.stripMargin

    val schema = MessageTypeParser.parseMessageType(schemaStr)
    val writer = createParquetWriter(
      schema,
      path,
      dictionaryEnabled = dictionaryEnabled,
      rowGroupSize = rowGroupSize)
    val div = if (dictionaryEnabled) 10 else n // maps value to a small range for dict to kick in

    val rand = scala.util.Random
    val expected = (0 until n).map { i =>
      if (rand.nextBoolean()) {
        None
      } else {
        Some(getValue(i, div))
      }
    }
    expected.foreach { opt =>
      val record = new SimpleGroup(schema)
      opt match {
        case Some(i) =>
          record.add(0, i)
          record.add(1, i * 1000) // convert millis to micros, same below
          record.add(2, i)
          record.add(3, i)
          record.add(4, i * 1000)
          record.add(5, i * 1000)
        case _ =>
      }
      writer.write(record)
    }

    writer.close()
    expected
  }

  // Creates Parquet file of timestamp values
  protected def makeRawTimeParquetFile(
      path: Path,
      dictionaryEnabled: Boolean,
      n: Int,
      rowGroupSize: Long = 1024 * 1024L): Seq[Option[Long]] = {
    val schemaStr =
      """
        |message root {
        |  optional int64 _0(TIMESTAMP_MILLIS);
        |  optional int64 _1(TIMESTAMP_MICROS);
        |  optional int64 _2(TIMESTAMP(MILLIS,true));
        |  optional int64 _3(TIMESTAMP(MILLIS,false));
        |  optional int64 _4(TIMESTAMP(MICROS,true));
        |  optional int64 _5(TIMESTAMP(MICROS,false));
        |  optional int64 _6(INT_64);
        |}
        """.stripMargin

    val schema = MessageTypeParser.parseMessageType(schemaStr)
    val writer = createParquetWriter(
      schema,
      path,
      dictionaryEnabled = dictionaryEnabled,
      rowGroupSize = rowGroupSize)
    val div = if (dictionaryEnabled) 10 else n // maps value to a small range for dict to kick in

    val rand = scala.util.Random
    val expected = (0 until n).map { i =>
      if (rand.nextBoolean()) {
        None
      } else {
        Some(getValue(i, div))
      }
    }
    expected.foreach { opt =>
      val record = new SimpleGroup(schema)
      opt match {
        case Some(i) =>
          record.add(0, i)
          record.add(1, i * 1000) // convert millis to micros, same below
          record.add(2, i)
          record.add(3, i)
          record.add(4, i * 1000)
          record.add(5, i * 1000)
          record.add(6, i * 1000)
        case _ =>
      }
      writer.write(record)
    }

    writer.close()
    expected
  }

  protected def makeDateTimeWithFormatTable(
      path: Path,
      dictionaryEnabled: Boolean,
      n: Int,
      rowGroupSize: Long = 1024 * 1024L): Seq[Option[Long]] = {
    val schemaStr =
      """
        |message root {
        |  optional int64 _0(TIMESTAMP_MILLIS);
        |  optional int64 _1(TIMESTAMP_MICROS);
        |  optional int64 _2(TIMESTAMP(MILLIS,true));
        |  optional int64 _3(TIMESTAMP(MILLIS,false));
        |  optional int64 _4(TIMESTAMP(MICROS,true));
        |  optional int64 _5(TIMESTAMP(MICROS,false));
        |  optional int64 _6(INT_64);
        |  optional int32 _7(DATE);
        |  optional binary format(UTF8);
        |  optional binary dateFormat(UTF8);
        |  }
      """.stripMargin

    val schema = MessageTypeParser.parseMessageType(schemaStr)
    val writer = createParquetWriter(
      schema,
      path,
      dictionaryEnabled = dictionaryEnabled,
      rowGroupSize = rowGroupSize)
    val div = if (dictionaryEnabled) 10 else n // maps value to a small range for dict to kick in

    val expected = (0 until n).map { i =>
      Some(getValue(i, div))
    }
    expected.foreach { opt =>
      val timestampFormats = List(
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
        "MICROSECOND")
      val dateFormats = List("YEAR", "YYYY", "YY", "MON", "MONTH", "MM", "QUARTER", "WEEK")
      val formats = timestampFormats.zipAll(dateFormats, "NONE", "YEAR")

      formats.foreach { format =>
        val record = new SimpleGroup(schema)
        opt match {
          case Some(i) =>
            record.add(0, i)
            record.add(1, i * 1000) // convert millis to micros, same below
            record.add(2, i)
            record.add(3, i)
            record.add(4, i * 1000)
            record.add(5, i * 1000)
            record.add(6, i * 1000)
            record.add(7, i.toInt)
            record.add(8, format._1)
            record.add(9, format._2)
          case _ =>
        }
        writer.write(record)
      }
    }
    writer.close()
    expected
  }

  def makeDecimalRDD(num: Int, decimal: DecimalType, useDictionary: Boolean): DataFrame = {
    val div = if (useDictionary) 5 else num // narrow the space to make it dictionary encoded
    spark
      .range(num)
      .map(_ % div)
      // Parquet doesn't allow column names with spaces, have to add an alias here.
      // Minus 500 here so that negative decimals are also tested.
      .select((($"value" - 500) / 100.0) cast decimal as Symbol("dec"))
      .coalesce(1)
  }

  def stripRandomPlanParts(plan: String): String = {
    plan.replaceFirst("file:.*,", "").replaceAll(raw"#\d+", "")
  }

  protected def checkCometExchange(
      df: DataFrame,
      cometExchangeNum: Int,
      native: Boolean): Seq[CometShuffleExchangeExec] = {
    if (CometConf.COMET_EXEC_SHUFFLE_ENABLED.get()) {
      val sparkPlan = stripAQEPlan(df.queryExecution.executedPlan)

      val cometShuffleExecs = sparkPlan.collect { case b: CometShuffleExchangeExec => b }
      assert(
        cometShuffleExecs.length == cometExchangeNum,
        s"$sparkPlan has ${cometShuffleExecs.length} " +
          s" CometShuffleExchangeExec node which doesn't match the expected: $cometExchangeNum")

      if (native) {
        cometShuffleExecs.foreach { b =>
          assert(b.shuffleType == CometNativeShuffle)
        }
      } else {
        cometShuffleExecs.foreach { b =>
          assert(b.shuffleType == CometColumnarShuffle)
        }
      }

      cometShuffleExecs
    } else {
      Seq.empty
    }
  }
}
