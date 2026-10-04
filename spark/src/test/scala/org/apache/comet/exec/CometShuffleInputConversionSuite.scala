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

package org.apache.comet.exec

import org.apache.spark.sql.{CometTestBase, DataFrame, Row}
import org.apache.spark.sql.comet.CometSparkToColumnarExec
import org.apache.spark.sql.comet.execution.shuffle.{CometColumnarShuffle, CometNativeShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.execution.{ColumnarToRowExec, ColumnarToRowTransition, SparkPlan}
import org.apache.spark.sql.functions.{array, avg, col, count, length, max, min, size, sum}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, BinaryType, CalendarIntervalType, DataTypes, DecimalType, IntegerType, LongType, StringType, StructType}
import org.apache.spark.unsafe.types.CalendarInterval

import org.apache.comet.{CometConf, ExtendedExplainInfo}

// Top-level, so the encoder needs no outer pointer.
case class ShuffleInputRec(a: Int, b: String)

/** Tests for [[CometConf.COMET_CONVERT_FROM_SHUFFLE_INPUT_ENABLED]]. */
class CometShuffleInputConversionSuite extends CometTestBase {

  import testImplicits._

  /**
   * `CometTestBase` turns on the conversion of leaf operators, which would convert the RDD scans
   * below the shuffles here before this conversion sees them. It is off by default.
   */
  private def withoutLeafConversion(f: => Unit): Unit =
    withSQLConf(CometConf.COMET_SPARK_TO_ARROW_ENABLED.key -> "false")(f)

  /** Defines a test that runs with the shuffle input conversion enabled. */
  private def convertTest(name: String)(f: => Unit): Unit =
    test(name) {
      withoutLeafConversion {
        withSQLConf(CometConf.COMET_CONVERT_FROM_SHUFFLE_INPUT_ENABLED.key -> "true")(f)
      }
    }

  private val rowSchema = new StructType()
    .add("k", IntegerType)
    .add("l", LongType)
    .add("s", DataTypes.StringType)
    .add("d", DataTypes.DoubleType)
    .add("m", DecimalType(18, 2))

  /**
   * `rows` rows from an RDD, so the shuffle above them reads a Spark operator: the conversion of
   * leaf operators, `spark.comet.sparkToColumnar.enabled`, is off by default.
   */
  private def rowsDf(rows: Int = 1000): DataFrame = {
    val data = (0 until rows).map { i =>
      Row(
        i % 23,
        i.toLong,
        if (i % 11 == 0) null else s"value-$i",
        i + 0.5d,
        java.math.BigDecimal.valueOf(i * 7919L % 1000000L, 2))
    }
    spark.createDataFrame(spark.sparkContext.parallelize(data, 4), rowSchema)
  }

  /** Rows with an `array<int>` column, which `CometSparkToColumnarExec` does not convert. */
  private def arraysDf(rows: Int = 100): DataFrame = {
    val schema = new StructType().add("k", IntegerType).add("xs", ArrayType(IntegerType))
    val data = (0 until rows).map(i => Row(i % 23, Seq(i, i + 1)))
    spark.createDataFrame(spark.sparkContext.parallelize(data, 4), schema)
  }

  private def conversions(plan: SparkPlan): Seq[CometSparkToColumnarExec] =
    collectWithSubqueries(plan) { case c: CometSparkToColumnarExec => c }

  private def cometShuffles(plan: SparkPlan): Seq[CometShuffleExchangeExec] =
    collectWithSubqueries(plan) { case s: CometShuffleExchangeExec => s }

  /** The native shuffles that read rows `CometSparkToColumnarExec` converted. */
  private def convertedShuffles(plan: SparkPlan): Seq[CometShuffleExchangeExec] =
    cometShuffles(plan).filter { s =>
      s.shuffleType == CometNativeShuffle && s.child.isInstanceOf[CometSparkToColumnarExec]
    }

  /**
   * Spark inserts no columnar transitions below a `CometSparkToColumnarExec`, so the rule adds
   * them for the operators under the conversion. Without one, an operator reads its Comet child
   * through `CometExec.doExecute`, Spark's interpreted columnar-to-row path, which gives the
   * right answer slowly, so only the plan shows it.
   */
  private def assertRowOperatorsReadThroughTransitions(plan: SparkPlan): Unit = {
    val converted = conversions(plan)
    assert(converted.nonEmpty, plan)
    converted.foreach { conversion =>
      // `InputAdapter` and `WholeStageCodegenExec` report their child's `supportsColumnar`.
      val bare = conversion.child.collect {
        case p
            if !p.supportsColumnar && !p.isInstanceOf[ColumnarToRowTransition] &&
              p.children.exists(_.supportsColumnar) =>
          p
      }
      assert(bare.isEmpty, s"row operators read a columnar child without a transition:\n$plan")
      assert(
        conversion.child.collect { case c: ColumnarToRowExec => c }.isEmpty,
        s"expected Comet's columnar-to-row transitions, not Spark's:\n$plan")
    }
    // The transitions are added on the first of the rule's passes under AQE. A later pass must
    // not report them as operators Comet failed to convert.
    val reasons = new ExtendedExplainInfo().getFallbackReasons(plan)
    assert(!reasons.exists(_.contains("ColumnarToRow")), reasons)
  }

  convertTest("the shuffle of a Spark operator's rows runs as native shuffle") {
    Seq("true", "false").foreach { aqe =>
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe) {
        val (_, plan) = checkSparkAnswer(
          rowsDf()
            .repartition(10, col("k"))
            .groupBy("k")
            .agg(sum("l"), sum(length(col("s"))), sum("d"), sum("m")))
        assert(convertedShuffles(plan).length == 1, s"AQE $aqe:\n$plan")
        assert(
          !cometShuffles(plan).exists(_.shuffleType == CometColumnarShuffle),
          s"AQE $aqe:\n$plan")
      }
    }
  }

  convertTest("Spark operators over Comet operators read them through transitions") {
    withParquetTable((0 until 200).map(i => (i, (i % 13).toString)), "tbl") {
      Seq("true", "false").foreach { aqe =>
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe) {
          val ds = spark.sql("SELECT _1 AS a, _2 AS b FROM tbl").as[ShuffleInputRec]
          // The shuffle reads the typed operation's SerializeFromObject.
          val (_, repartitioned) = checkSparkAnswer(
            ds.map(r => ShuffleInputRec(r.a % 13, r.b))
              .repartition(7, col("a"))
              .groupBy("a")
              .agg(count("b")))
          assert(convertedShuffles(repartitioned).length == 1, s"AQE $aqe:\n$repartitioned")
          assertRowOperatorsReadThroughTransitions(repartitioned)
          // The shuffle reads a Spark partial aggregate over the typed operation. The final
          // aggregate stays on Spark too, so the shuffle would go back to Spark's own.
          withSQLConf(CometConf.COMET_SHUFFLE_REVERT_REDUNDANT_COLUMNAR_ENABLED.key -> "false") {
            val (_, aggregated) =
              checkSparkAnswer(ds.map(r => ShuffleInputRec(r.a % 7, r.b)).groupBy("a").count())
            assert(convertedShuffles(aggregated).length == 1, s"AQE $aqe:\n$aggregated")
            assertRowOperatorsReadThroughTransitions(aggregated)
          }
        }
      }
    }
  }

  convertTest("aggregates whose partial aggregate runs on Spark") {
    checkSparkAnswer(
      rowsDf()
        .groupBy((col("k") % 7).as("g"))
        .agg(sum("l"), count("s"), avg("d"), sum("m"), max("l"), min("s")))
  }

  convertTest("a shuffle between two Spark aggregates goes back to Spark's shuffle") {
    withSQLConf(CometConf.COMET_EXEC_AGGREGATE_ENABLED.key -> "false") {
      val (_, plan) = checkSparkAnswer(rowsDf().groupBy("k").agg(sum("l")))
      assert(cometShuffles(plan).isEmpty, plan)
      withSQLConf(CometConf.COMET_SHUFFLE_REVERT_REDUNDANT_COLUMNAR_ENABLED.key -> "false") {
        val (_, kept) = checkSparkAnswer(rowsDf().groupBy("k").agg(sum("l")))
        assert(convertedShuffles(kept).length == 1, kept)
      }
    }
  }

  convertTest("a join with an input that stays on the JVM columnar shuffle") {
    // The left input converts and the right one, with its array<int> column, does not. Native
    // shuffle hashes an int key as Spark does, so matching keys still meet.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "10") {
      val df = rowsDf(100)
        .select(col("k").as("lk"), col("l"))
        .join(arraysDf(), col("lk") === col("k"))
      val (_, plan) = checkSparkAnswer(df)
      assert(convertedShuffles(plan).length == 1, plan)
      assert(cometShuffles(plan).count(_.shuffleType == CometColumnarShuffle) == 1, plan)
    }
  }

  convertTest("a shuffle that hashes a wide decimal stays on the JVM columnar shuffle") {
    // Native shuffle hashes a decimal wider than 18 digits differently from Spark's partitioner
    // (#5994). The right input, with its array<int> column, stays on the JVM columnar shuffle,
    // so the left one has to as well, or matching keys land in different partitions.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "10") {
      val left = rowsDf(100).select(col("k").cast(DecimalType(38, 0)).as("lk"), col("l"))
      val right = arraysDf().select(col("k").cast(DecimalType(38, 0)).as("rk"), col("xs"))
      val df = left.join(right, col("lk") === col("rk"))
      val (_, plan) = checkSparkAnswer(df)
      assert(conversions(plan).isEmpty, plan)
      checkCometExchange(df, 2, native = false)
    }
  }

  convertTest("a shuffle that hashes a string stays on the JVM columnar shuffle") {
    // Spark's partitioner hashes a string's bytes as they are, but native shuffle hashes them
    // after the import into native has replaced invalid UTF-8. The right input, with its
    // array<int> column, stays on the JVM columnar shuffle, so the left one has to as well, or
    // matching keys with invalid UTF-8 land in different partitions. Both shuffles write the rows
    // with invalid UTF-8 replaced, so the four keys stay apart only because they hash to four
    // different partitions.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "10") {
      val schema = new StructType().add("b", BinaryType).add("v", LongType)
      val data = (0 until 40).map(i => Row(Array((0x80 + i % 4).toByte), i.toLong))
      val keys = spark.createDataFrame(spark.sparkContext.parallelize(data, 4), schema)
      val left = keys.select(col("b").cast(StringType).as("lk"), col("v"))
      val right = keys.select(
        col("b").cast(StringType).as("rk"),
        array(col("v").cast(IntegerType)).as("xs"))
      val df = left.join(right, col("lk") === col("rk"))
      val (_, plan) = checkSparkAnswer(df)
      assert(conversions(plan).isEmpty, plan)
      checkCometExchange(df, 2, native = false)
    }
  }

  convertTest("struct columns over several batches") {
    // The rows of each input partition fill several batches. With the conversion of leaf
    // operators on, native shuffle reads the converted scan the same way.
    Seq("false", "true").foreach { leafConversion =>
      withSQLConf(
        CometConf.COMET_BATCH_SIZE.key -> "7",
        CometConf.COMET_SPARK_TO_ARROW_ENABLED.key -> leafConversion) {
        val schema = new StructType()
          .add("k", IntegerType)
          .add("payload", new StructType().add("v", LongType).add("s", StringType))
        val data = (0 until 24).map(i => Row(i, Row(i.toLong, s"s$i")))
        val df = spark.createDataFrame(spark.sparkContext.parallelize(data, 1), schema)
        Seq(df.repartition(3, col("k")), df.repartitionByRange(3, col("k"))).foreach { shuffled =>
          val (_, plan) = checkSparkAnswer(shuffled)
          assert(convertedShuffles(plan).length == 1, s"leaf conversion $leafConversion:\n$plan")
        }
      }
    }
  }

  convertTest("calendar intervals keep Spark's shuffle") {
    // Arrow holds an interval's time part in nanoseconds, which can't hold every number of
    // microseconds Spark can. The JVM columnar shuffle doesn't take calendar intervals either.
    val schema = new StructType().add("k", IntegerType).add("i", CalendarIntervalType)
    val data = Seq(Row(1, new CalendarInterval(0, 0, 10800000000000000L)))
    val df = spark
      .createDataFrame(spark.sparkContext.parallelize(data, 1), schema)
      .repartition(2, col("k"))
    val (_, plan) = checkSparkAnswer(df)
    assert(cometShuffles(plan).isEmpty, plan)
  }

  convertTest("columns the conversion does not support keep the JVM columnar shuffle") {
    val df = arraysDf().repartition(5, col("k")).groupBy("k").agg(sum(size(col("xs"))))
    val (_, plan) = checkSparkAnswer(df)
    assert(conversions(plan).isEmpty, plan)
    assert(cometShuffles(plan).forall(_.shuffleType == CometColumnarShuffle), plan)
  }

  convertTest("range partitioning") {
    val (_, plan) = checkSparkAnswer(rowsDf().orderBy(col("l").desc))
    assert(convertedShuffles(plan).length == 1, plan)
  }

  convertTest("the JVM shuffle mode keeps the JVM columnar shuffle") {
    withSQLConf(CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
      val (_, plan) = checkSparkAnswer(rowsDf().repartition(10, col("k")).groupBy("k").count())
      assert(conversions(plan).isEmpty, plan)
      assert(cometShuffles(plan).forall(_.shuffleType == CometColumnarShuffle), plan)
    }
  }

  test("off by default") {
    withoutLeafConversion {
      val (_, plan) = checkSparkAnswer(rowsDf().repartition(10, col("k")).groupBy("k").count())
      assert(conversions(plan).isEmpty, plan)
      assert(cometShuffles(plan).exists(_.shuffleType == CometColumnarShuffle), plan)
    }
  }
}
