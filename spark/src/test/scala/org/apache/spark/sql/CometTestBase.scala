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

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.util.{Success, Try}

import org.scalatest.BeforeAndAfterEach

import org.apache.hadoop.fs.Path
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.{SimpleGroup, SimpleGroupFactory}
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.example.{ExampleParquetWriter, GroupWriteSupport}
import org.apache.parquet.schema.{MessageType, MessageTypeParser}
import org.apache.spark._
import org.apache.spark.internal.config.{MEMORY_OFFHEAP_ENABLED, MEMORY_OFFHEAP_SIZE, SHUFFLE_MANAGER}
import org.apache.spark.sql.comet._
import org.apache.spark.sql.comet.execution.shuffle.{CometColumnarShuffle, CometNativeShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal._
import org.apache.spark.sql.test._
import org.apache.spark.sql.types.{DecimalType, StructType}

import org.apache.comet._
import org.apache.comet.shims.ShimCometSparkSessionExtensions

/**
 * Base class for testing. This exists in `org.apache.spark.sql` since [[SQLTestUtils]] is
 * package-private.
 */
abstract class CometTestBase
    extends QueryTest
    with SQLTestUtils
    with BeforeAndAfterEach
    with AdaptiveSparkPlanHelper
    with ShimCometSparkSessionExtensions
    with ShimCometTestBase {
  import testImplicits._

  protected val shuffleManager: String =
    "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager"

  protected def sparkConf: SparkConf = {
    val conf = new SparkConf()
    conf.set("spark.hadoop.fs.file.impl", classOf[DebugFilesystem].getName)
    conf.set("spark.ui.enabled", "false")
    conf.set(SQLConf.SHUFFLE_PARTITIONS, 10) // reduce parallelism in tests
    conf.set(SQLConf.ANSI_ENABLED.key, "false")
    conf.set(SHUFFLE_MANAGER, shuffleManager)
    conf.set(MEMORY_OFFHEAP_ENABLED.key, "true")
    conf.set(MEMORY_OFFHEAP_SIZE.key, "2g")
    conf.set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "1g")
    conf.set(SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key, "1g")
    conf.set(CometConf.COMET_ENABLED.key, "true")
    conf.set(CometConf.COMET_ONHEAP_ENABLED.key, "true")
    conf.set(CometConf.COMET_EXEC_ENABLED.key, "true")
    conf.set(CometConf.COMET_EXEC_SHUFFLE_ENABLED.key, "true")
    conf.set(CometConf.COMET_RESPECT_PARQUET_FILTER_PUSHDOWN.key, "true")
    conf.set(CometConf.COMET_SPARK_TO_ARROW_ENABLED.key, "true")
    conf.set(CometConf.COMET_NATIVE_SCAN_ENABLED.key, "true")
    conf.set(CometConf.COMET_SCAN_ALLOW_INCOMPATIBLE.key, "true")
    conf.set(CometConf.COMET_ONHEAP_MEMORY_OVERHEAD.key, "2g")
    conf.set(CometConf.COMET_EXEC_SORT_MERGE_JOIN_WITH_JOIN_FILTER_ENABLED.key, "true")
    // SortOrder is incompatible for mixed zero and negative zero floating point values, but
    // this is an edge case, and we expect most users to allow sorts on floating point, so we
    // enable this for the tests
    conf.set(CometConf.getExprAllowIncompatConfigKey("SortOrder"), "true")
    conf
  }

  protected def isFeatureEnabled(feature: String): Boolean = {
    try {
      NativeBase.isFeatureEnabled(feature)
    } catch {
      case _: Throwable =>
        false
    }
  }

  protected def internalCheckSparkAnswer(
      df: => DataFrame,
      assertCometNative: Boolean,
      includeClasses: Seq[Class[_]] = Seq.empty,
      excludedClasses: Seq[Class[_]] = Seq.empty,
      withTol: Option[Double] = None): (SparkPlan, SparkPlan) = {

    var expected: Array[Row] = Array.empty
    var sparkPlan = null.asInstanceOf[SparkPlan]
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      val dfSpark = datasetOfRows(spark, df.logicalPlan)
      expected = dfSpark.collect()
      sparkPlan = dfSpark.queryExecution.executedPlan
    }
    val dfComet = datasetOfRows(spark, df.logicalPlan)

    if (withTol.isDefined) {
      checkAnswerWithTolerance(dfComet, expected, withTol.get)
    } else {
      checkAnswer(dfComet, expected)
    }

    if (assertCometNative) {
      checkCometOperators(stripAQEPlan(df.queryExecution.executedPlan), excludedClasses: _*)
    } else {
      if (CometConf.COMET_STRICT_TESTING.get()) {
        if (findFirstNonCometOperator(
            stripAQEPlan(df.queryExecution.executedPlan),
            excludedClasses: _*).isEmpty) {
          fail("Plan was fully native in Comet. Call checkSparkAnswerAndOperator instead.")
        }
      }
    }

    if (includeClasses.nonEmpty) {
      checkPlanContains(stripAQEPlan(df.queryExecution.executedPlan), includeClasses: _*)
    }

    (sparkPlan, dfComet.queryExecution.executedPlan)
  }

  /**
   * Check that the query returns the correct results when Comet is enabled, but do not check if
   * Comet accelerated any operators
   */
  protected def checkSparkAnswer(query: String): (SparkPlan, SparkPlan) = {
    internalCheckSparkAnswer(sql(query), assertCometNative = false)
  }

  /**
   * Check that the query returns the correct results when Comet is enabled, but do not check if
   * Comet accelerated any operators
   */
  protected def checkSparkAnswer(df: => DataFrame): (SparkPlan, SparkPlan) = {
    internalCheckSparkAnswer(df, assertCometNative = false)
  }

  /**
   * Check that the query returns the correct results when Comet is enabled, but do not check if
   * Comet accelerated any operators
   *
   * Use the provided `tol` when comparing floating-point results.
   */
  protected def checkSparkAnswerWithTolerance(
      query: String,
      absTol: Double = 1e-6): (SparkPlan, SparkPlan) = {
    checkSparkAnswerWithTolerance(sql(query), absTol)
  }

  /**
   * Check that the query returns the correct results when Comet is enabled, but do not check if
   * Comet accelerated any operators
   *
   * Use the provided `tol` when comparing floating-point results.
   */
  protected def checkSparkAnswerWithTolerance(
      df: => DataFrame,
      absTol: Double): (SparkPlan, SparkPlan) = {
    internalCheckSparkAnswer(df, assertCometNative = false, withTol = Some(absTol))
  }

  /**
   * Check that the query returns the correct results when Comet is enabled and that Comet
   * replaced all possible operators except for those specified in the excluded list.
   */
  protected def checkSparkAnswerAndOperator(
      query: String,
      excludedClasses: Class[_]*): (SparkPlan, SparkPlan) = {
    checkSparkAnswerAndOperator(sql(query), excludedClasses: _*)
  }

  /**
   * Check that the query returns the correct results when Comet is enabled and that Comet
   * replaced all possible operators except for those specified in the excluded list.
   */
  protected def checkSparkAnswerAndOperator(
      df: => DataFrame,
      excludedClasses: Class[_]*): (SparkPlan, SparkPlan) = {
    internalCheckSparkAnswer(
      df,
      assertCometNative = true,
      excludedClasses = Seq(excludedClasses: _*))
  }

  /**
   * Check that the query returns the correct results when Comet is enabled and that Comet
   * replaced all possible operators except for those specified in the excluded list.
   *
   * Also check that the plan included all operators specified in `includeClasses`.
   */
  protected def checkSparkAnswerAndOperator(
      df: => DataFrame,
      includeClasses: Seq[Class[_]],
      excludedClasses: Class[_]*): (SparkPlan, SparkPlan) = {
    internalCheckSparkAnswer(
      df,
      assertCometNative = true,
      includeClasses,
      excludedClasses = Seq(excludedClasses: _*))
  }

  /**
   * Check that the query returns the correct results when Comet is enabled and that Comet
   * replaced all possible operators except for those specified in the excluded list.
   *
   * Also check that the plan included all operators specified in `includeClasses`.
   *
   * Use the provided `tol` when comparing floating-point results.
   */
  protected def checkSparkAnswerAndOperatorWithTol(
      df: => DataFrame,
      tol: Double = 1e-6): (SparkPlan, SparkPlan) = {
    checkSparkAnswerAndOperatorWithTol(df, tol, Seq.empty)
  }

  /**
   * Check that the query returns the correct results when Comet is enabled and that Comet
   * replaced all possible operators except for those specified in the excluded list.
   *
   * Also check that the plan included all operators specified in `includeClasses`.
   *
   * Use the provided `tol` when comparing floating-point results.
   */
  protected def checkSparkAnswerAndOperatorWithTol(
      df: => DataFrame,
      tol: Double,
      includeClasses: Seq[Class[_]],
      excludedClasses: Class[_]*): (SparkPlan, SparkPlan) = {
    internalCheckSparkAnswer(
      df,
      assertCometNative = true,
      includeClasses = Seq(includeClasses: _*),
      excludedClasses = Seq(excludedClasses: _*),
      withTol = Some(tol))
  }

  /** Check for the correct results as well as the expected fallback reason */
  protected def checkSparkAnswerAndFallbackReason(
      query: String,
      fallbackReason: String): (SparkPlan, SparkPlan) = {
    checkSparkAnswerAndFallbackReasons(sql(query), Set(fallbackReason))
  }

  /** Check for the correct results as well as the expected fallback reason */
  protected def checkSparkAnswerAndFallbackReason(
      df: => DataFrame,
      fallbackReason: String): (SparkPlan, SparkPlan) = {
    checkSparkAnswerAndFallbackReasons(df, Set(fallbackReason))
  }

  /** Check for the correct results as well as the expected fallback reasons */
  protected def checkSparkAnswerAndFallbackReasons(
      query: String,
      fallbackReasons: Set[String]): (SparkPlan, SparkPlan) = {
    checkSparkAnswerAndFallbackReasons(sql(query), fallbackReasons)
  }

  /** Check for the correct results as well as the expected fallback reasons */
  protected def checkSparkAnswerAndFallbackReasons(
      df: => DataFrame,
      fallbackReasons: Set[String]): (SparkPlan, SparkPlan) = {
    val (sparkPlan, cometPlan) = internalCheckSparkAnswer(df, assertCometNative = false)
    val explainInfo = new ExtendedExplainInfo()
    val actualFallbacks = explainInfo.getFallbackReasons(cometPlan)
    for (reason <- fallbackReasons) {
      if (!actualFallbacks.exists(_.contains(reason))) {
        if (actualFallbacks.isEmpty) {
          fail(
            s"Expected fallback reason '$reason' but no fallback reasons were found. Explain: ${explainInfo
                .generateExtendedInfo(cometPlan)}")
        } else {
          fail(
            s"Expected fallback reason '$reason' not found in [${actualFallbacks.mkString(", ")}]")
        }
      }
    }
    (sparkPlan, cometPlan)
  }

  /**
   * Try executing the query against Spark and Comet and return the results or the exception.
   *
   * This method does not check that Comet replaced any operators or that the results match in the
   * case where the query is successful against both Spark and Comet.
   */
  protected def checkSparkMaybeThrows(
      df: => DataFrame): (Option[Throwable], Option[Throwable]) = {
    var expected: Try[Array[Row]] = null
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      expected = Try(datasetOfRows(spark, df.logicalPlan).collect())
    }
    val actual = Try(datasetOfRows(spark, df.logicalPlan).collect())

    (expected, actual) match {
      case (Success(_), Success(_)) =>
        // TODO compare results and confirm that they match
        // https://github.com/apache/datafusion-comet/issues/2657
        (None, None)
      case _ =>
        (expected.failed.toOption, actual.failed.toOption)
    }
  }

  /**
   * A helper function for comparing Comet DataFrame with Spark result using absolute tolerance.
   */
  private def checkAnswerWithTolerance(
      dataFrame: DataFrame,
      expectedAnswer: Seq[Row],
      absTol: Double): Unit = {
    val actualAnswer = dataFrame.collect()
    require(
      actualAnswer.length == expectedAnswer.length,
      s"actual num rows ${actualAnswer.length} != expected num of rows ${expectedAnswer.length}")

    actualAnswer.zip(expectedAnswer).foreach { case (actualRow, expectedRow) =>
      checkAnswerWithTolerance(actualRow, expectedRow, absTol)
    }
  }

  /**
   * Compares two answers and makes sure the answer is within absTol of the expected result.
   */
  private def checkAnswerWithTolerance(
      actualAnswer: Row,
      expectedAnswer: Row,
      absTol: Double): Unit = {
    require(
      actualAnswer.length == expectedAnswer.length,
      s"actual answer length ${actualAnswer.length} != " +
        s"expected answer length ${expectedAnswer.length}")
    require(absTol > 0 && absTol <= 1e-6, s"absTol $absTol is out of range (0, 1e-6]")

    actualAnswer.toSeq.zip(expectedAnswer.toSeq).foreach {
      case (actual: Float, expected: Float) =>
        if (actual.isInfinity || expected.isInfinity) {
          assert(actual.isInfinity == expected.isInfinity, s"actual answer $actual != $expected")
        } else if (!actual.isNaN && !expected.isNaN) {
          assert(
            math.abs(actual - expected) < absTol,
            s"actual answer $actual not within $absTol of correct answer $expected")
        }
      case (actual: Double, expected: Double) =>
        if (actual.isInfinity || expected.isInfinity) {
          assert(actual.isInfinity == expected.isInfinity, s"actual answer $actual != $expected")
        } else if (!actual.isNaN && !expected.isNaN) {
          assert(
            math.abs(actual - expected) < absTol,
            s"actual answer $actual not within $absTol of correct answer $expected")
        }
      case (actual, expected) =>
        assert(actual == expected, s"$actualAnswer did not equal $expectedAnswer")
    }
  }

  protected def checkCometOperators(plan: SparkPlan, excludedClasses: Class[_]*): Unit = {
    findFirstNonCometOperator(plan, excludedClasses: _*) match {
      case Some(op) =>
        assert(
          false,
          s"Expected only Comet native operators, but found ${op.nodeName}.\n" +
            s"plan: ${new ExtendedExplainInfo().generateExtendedInfo(plan)}")
      case _ =>
    }
  }

  protected def findFirstNonCometOperator(
      plan: SparkPlan,
      excludedClasses: Class[_]*): Option[SparkPlan] = {
    val wrapped = wrapCometSparkToColumnar(plan)
    wrapped.foreach {
      case _: CometNativeScanExec | _: CometScanExec | _: CometBatchScanExec =>
      case _: CometSinkPlaceHolder | _: CometScanWrapper =>
      case _: CometColumnarToRowExec =>
      case _: CometSparkToColumnarExec =>
      case _: CometExec | _: CometShuffleExchangeExec =>
      case _: CometBroadcastExchangeExec =>
      case _: WholeStageCodegenExec | _: ColumnarToRowExec | _: InputAdapter =>
      case op if !excludedClasses.exists(c => c.isAssignableFrom(op.getClass)) =>
        return Some(op)
      case _ =>
    }
    None
  }

  private def checkPlanContains(plan: SparkPlan, includePlans: Class[_]*): Unit = {
    includePlans.foreach { case planClass =>
      if (plan.find(op => planClass.isAssignableFrom(op.getClass)).isEmpty) {
        assert(
          false,
          s"Expected plan to contain ${planClass.getSimpleName}, but not.\n" +
            s"plan: $plan")
      }
    }
  }

  /** Wraps the CometRowToColumn as ScanWrapper, so the child operators will not be checked */
  private def wrapCometSparkToColumnar(plan: SparkPlan): SparkPlan = {
    plan.transformDown {
      // don't care the native operators
      case p: CometSparkToColumnarExec => CometScanWrapper(null, p)
    }
  }

  private var _spark: SparkSessionType = _
  override protected implicit def spark: SparkSessionType = _spark
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

  protected def createSparkSession: SparkSessionType = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()

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
      // TODO we need to shim this and use withRowGroupSize(Long) with later parquet-hadoop versions to remove
      // the deprecated warning here
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

  def makeParquetFileAllPrimitiveTypes(path: Path, dictionaryEnabled: Boolean, n: Int): Unit = {
    makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled, 0, n)
  }

  def getPrimitiveTypesParquetSchema: String = {
    if (usingDataSourceExecWithIncompatTypes(conf)) {
      // Comet complex type reader has different behavior for uint_8, uint_16 types.
      // The issue stems from undefined behavior in the parquet spec and is tracked
      // here: https://github.com/apache/parquet-java/issues/3142
      // here: https://github.com/apache/arrow-rs/issues/7040
      // and here: https://github.com/apache/datafusion-comet/issues/1348
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
       |  optional int32                    _9(UINT_32);
       |  optional int32                    _10(UINT_32);
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
       |  optional binary                   _21;
       |  optional INT32                    _id;
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
       |  optional FIXED_LEN_BYTE_ARRAY(3)  _14;
       |  optional int32                    _15(DECIMAL(5, 2));
       |  optional int64                    _16(DECIMAL(18, 10));
       |  optional FIXED_LEN_BYTE_ARRAY(16) _17(DECIMAL(38, 37));
       |  optional INT64                    _18(TIMESTAMP(MILLIS,true));
       |  optional INT64                    _19(TIMESTAMP(MICROS,true));
       |  optional INT32                    _20(DATE);
       |  optional binary                   _21;
       |  optional INT32                    _id;
       |}
      """.stripMargin
    }
  }

  def makeParquetFileAllPrimitiveTypes(
      path: Path,
      dictionaryEnabled: Boolean,
      begin: Int,
      end: Int,
      nullEnabled: Boolean = true,
      pageSize: Int = 128,
      randomSize: Int = 0): Unit = {
    // alwaysIncludeUnsignedIntTypes means we include unsignedIntTypes in the test even if the
    // reader does not support them
    val schemaStr = getPrimitiveTypesParquetSchema

    val schema = MessageTypeParser.parseMessageType(schemaStr)
    val writer = createParquetWriter(
      schema,
      path,
      dictionaryEnabled = dictionaryEnabled,
      pageSize = pageSize,
      dictionaryPageSize = pageSize)

    val idGenerator = new AtomicInteger(0)

    val rand = scala.util.Random
    val data = (begin until end).map { i =>
      if (nullEnabled && rand.nextBoolean()) {
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
          record.add(20, i.toString)
          record.add(21, idGenerator.getAndIncrement())
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
      record.add(20, i.toString)
      record.add(21, idGenerator.getAndIncrement())
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

  // Generate a file based on a complex schema. Schema derived from https://arrow.apache.org/blog/2022/10/17/arrow-parquet-encoding-part-3/
  def makeParquetFileComplexTypes(
      path: Path,
      dictionaryEnabled: Boolean,
      numRows: Integer = 10000): Unit = {
    val schemaString =
      """
      message ComplexDataSchema {
        optional group optional_array (LIST) {
          repeated group list {
            optional int32 element;
          }
        }
        required group array_of_struct (LIST) {
          repeated group list {
            optional group struct_element {
              required int32 field1;
              optional group optional_nested_array (LIST) {
                repeated group list {
                  required int32 element;
                }
              }
            }
          }
        }
        optional group optional_map (MAP) {
          repeated group key_value {
            required int32 key;
            optional int32 value;
          }
        }
        required group complex_map (MAP) {
          repeated group key_value {
            required group map_key {
              required int32 key_field1;
              optional int32 key_field2;
            }
            required group map_value {
              required int32 value_field1;
              repeated int32 value_field2;
            }
          }
        }
      }
    """

    val schema: MessageType = MessageTypeParser.parseMessageType(schemaString)
    GroupWriteSupport.setSchema(schema, spark.sparkContext.hadoopConfiguration)

    val writer = createParquetWriter(schema, path, dictionaryEnabled)

    val groupFactory = new SimpleGroupFactory(schema)

    for (i <- 0 until numRows) {
      val record = groupFactory.newGroup()

      // Optional array of optional integers
      if (i % 2 == 0) { // optional_array for every other row
        val optionalArray = record.addGroup("optional_array")
        for (j <- 0 until (i % 5)) {
          val elementGroup = optionalArray.addGroup("list")
          if (j % 2 == 0) { // some elements are optional
            elementGroup.append("element", j)
          }
        }
      }

      // Required array of structs
      val arrayOfStruct = record.addGroup("array_of_struct")
      for (j <- 0 until (i % 3) + 1) { // Add one to three elements
        val structElementGroup = arrayOfStruct.addGroup("list").addGroup("struct_element")
        structElementGroup.append("field1", i * 10 + j)

        // Optional nested array
        if (j % 2 != 0) { // optional nested array for every other struct
          val nestedArray = structElementGroup.addGroup("optional_nested_array")
          for (k <- 0 until (i % 4)) { // Add zero to three elements.
            val nestedElementGroup = nestedArray.addGroup("list")
            nestedElementGroup.append("element", i + j + k)
          }
        }
      }

      // Optional map
      if (i % 3 == 0) { // optional_map every third row
        val optionalMap = record.addGroup("optional_map")
        optionalMap
          .addGroup("key_value")
          .append("key", i)
          .append("value", i)
        if (i % 5 == 0) { // another optional entry
          optionalMap
            .addGroup("key_value")
            .append("key", i)
          // Value is optional
          if (i % 10 == 0) {
            optionalMap
              .addGroup("key_value")
              .append("key", i)
              .append("value", i)
          }
        }
      }

      // Required map with complex key and value types
      val complexMap = record.addGroup("complex_map")
      val complexMapKeyVal = complexMap.addGroup("key_value")

      complexMapKeyVal
        .addGroup("map_key")
        .append("key_field1", i)

      complexMapKeyVal
        .addGroup("map_value")
        .append("value_field1", i)
        .append("value_field2", i * 100)
        .append("value_field2", i * 101)
        .append("value_field2", i * 102)

      writer.write(record)
    }

    writer.close()
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

  /**
   * The test encapsulates integration Comet test and does following:
   *   - prepares data using SELECT query and saves it to the Parquet file in temp folder
   *   - creates a temporary table with name `tableName` on top of temporary parquet file
   *   - runs the query `testQuery` reading data from `tableName`
   *
   * Asserts the `testQuery` data with Comet is the same is with Apache Spark and also asserts
   * only Comet operator are in the physical plan
   *
   * Example:
   *
   * {{{
   *  test("native reader - read simple ARRAY fields with SHORT field") {
   *     testSingleLineQuery(
   *       """
   *         |select array(cast(1 as short)) arr
   *         |""".stripMargin,
   *       "select arr from tbl",
   *       sqlConf = Seq(
   *         CometConf.COMET_SCAN_ALLOW_INCOMPATIBLE.key -> "false",
   *         "spark.comet.explainFallback.enabled" -> "false"
   *       ),
   *       debugCometDF = df => {
   *         df.printSchema()
   *         df.explain("extended")
   *         df.show()
   *       })
   *   }
   * }}}
   *
   * @param prepareQuery
   *   prepare sample data with Comet disabled
   * @param testQuery
   *   the query to test. Typically with Comet enabled + other SQL config applied
   * @param testName
   *   test name
   * @param tableName
   *   table name where sample data stored
   * @param sqlConf
   *   additional spark sql configuration
   * @param debugCometDF
   *   optional debug access to DataFrame for `testQuery`
   */
  def testSingleLineQuery(
      prepareQuery: String,
      testQuery: String,
      testName: String = "test",
      tableName: String = "tbl",
      sqlConf: Seq[(String, String)] = Seq.empty,
      readSchema: Option[StructType] = None,
      debugCometDF: DataFrame => Unit = _ => (),
      checkCometOperator: Boolean = true): Unit = {

    withTempDir { dir =>
      val path = new Path(dir.toURI.toString, testName).toUri.toString
      var data: java.util.List[Row] = new java.util.ArrayList()
      var schema: StructType = null

      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        val df = spark.sql(prepareQuery)
        data = df.collectAsList()
        schema = df.schema
      }

      spark.createDataFrame(data, schema).repartition(1).write.parquet(path)
      readParquetFile(path, readSchema.orElse(Some(schema))) { df =>
        df.createOrReplaceTempView(tableName)
      }

      withSQLConf(sqlConf: _*) {
        val cometDF = sql(testQuery)
        debugCometDF(cometDF)
        if (checkCometOperator) checkSparkAnswerAndOperator(cometDF)
        else checkSparkAnswer(cometDF)
      }
    }
  }

  def showString[T](
      df: Dataset[T],
      _numRows: Int,
      truncate: Int = 20,
      vertical: Boolean = false): String = {
    df.showString(_numRows, truncate, vertical)
  }

  def makeParquetFile(
      path: Path,
      total: Int,
      numGroups: Int,
      dictionaryEnabled: Boolean): Unit = {
    val schemaStr =
      """
        |message root {
        |  optional INT32                    _1(INT_8);
        |  optional INT32                    _2(INT_16);
        |  optional INT32                    _3;
        |  optional INT64                    _4;
        |  optional FLOAT                    _5;
        |  optional DOUBLE                   _6;
        |  optional INT32                    _7(DECIMAL(5, 2));
        |  optional INT64                    _8(DECIMAL(18, 10));
        |  optional FIXED_LEN_BYTE_ARRAY(16) _9(DECIMAL(38, 37));
        |  optional INT64                    _10(TIMESTAMP(MILLIS,true));
        |  optional INT64                    _11(TIMESTAMP(MICROS,true));
        |  optional INT32                    _12(DATE);
        |  optional INT32                    _g1(INT_8);
        |  optional INT32                    _g2(INT_16);
        |  optional INT32                    _g3;
        |  optional INT64                    _g4;
        |  optional FLOAT                    _g5;
        |  optional DOUBLE                   _g6;
        |  optional INT32                    _g7(DECIMAL(5, 2));
        |  optional INT64                    _g8(DECIMAL(18, 10));
        |  optional FIXED_LEN_BYTE_ARRAY(16) _g9(DECIMAL(38, 37));
        |  optional INT64                    _g10(TIMESTAMP(MILLIS,true));
        |  optional INT64                    _g11(TIMESTAMP(MICROS,true));
        |  optional INT32                    _g12(DATE);
        |  optional BINARY                   _g13(UTF8);
        |  optional BINARY                   _g14;
        |}
      """.stripMargin

    val schema = MessageTypeParser.parseMessageType(schemaStr)
    val writer = createParquetWriter(schema, path, dictionaryEnabled = true)

    val rand = scala.util.Random
    val expected = (0 until total).map { i =>
      // use a single value for the first page, to make sure dictionary encoding kicks in
      if (rand.nextBoolean()) None
      else {
        if (dictionaryEnabled) Some(i % 10) else Some(i)
      }
    }

    expected.foreach { opt =>
      val record = new SimpleGroup(schema)
      opt match {
        case Some(i) =>
          record.add(0, i.toByte)
          record.add(1, i.toShort)
          record.add(2, i)
          record.add(3, i.toLong)
          record.add(4, rand.nextFloat())
          record.add(5, rand.nextDouble())
          record.add(6, i)
          record.add(7, i.toLong)
          record.add(8, (i % 10).toString * 16)
          record.add(9, i.toLong)
          record.add(10, i.toLong)
          record.add(11, i)
          record.add(12, i.toByte % numGroups)
          record.add(13, i.toShort % numGroups)
          record.add(14, i % numGroups)
          record.add(15, i.toLong % numGroups)
          record.add(16, rand.nextFloat())
          record.add(17, rand.nextDouble())
          record.add(18, i)
          record.add(19, i.toLong)
          record.add(20, (i % 10).toString * 16)
          record.add(21, i.toLong)
          record.add(22, i.toLong)
          record.add(23, i)
          record.add(24, (i % 10).toString * 24)
          record.add(25, (i % 10).toString * 36)
        case _ =>
      }
      writer.write(record)
    }

    writer.close()
  }

  def usingDataSourceExec: Boolean = usingDataSourceExec(SQLConf.get)

  def usingDataSourceExec(conf: SQLConf): Boolean =
    Seq(CometConf.SCAN_NATIVE_ICEBERG_COMPAT, CometConf.SCAN_NATIVE_DATAFUSION).contains(
      CometConf.COMET_NATIVE_SCAN_IMPL.get(conf))

  def usingDataSourceExecWithIncompatTypes(conf: SQLConf): Boolean = {
    usingDataSourceExec(conf) &&
    !CometConf.COMET_SCAN_ALLOW_INCOMPATIBLE.get(conf)
  }
}
