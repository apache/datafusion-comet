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

import java.sql.{Date, Timestamp}
import java.time.LocalDateTime

import org.apache.spark.sql.{CometTestBase, DataFrame, Row}
import org.apache.spark.sql.comet.{CometHashAggregateExec, CometPlan, CometSparkToColumnarExec}
import org.apache.spark.sql.comet.execution.shuffle.{CometColumnarShuffle, CometNativeShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.execution.{ColumnarToRowExec, ColumnarToRowTransition, SparkPlan}
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.functions.{array, avg, col, count, hash, length, max, min, size, spark_partition_id, sum}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, CalendarIntervalType, DataType, DataTypes, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructType, TimestampNTZType, TimestampType}
import org.apache.spark.unsafe.types.CalendarInterval

import org.apache.comet.{CometConf, ExtendedExplainInfo}

// Top-level, so the encoder needs no outer pointer.
case class ShuffleInputRec(a: Int, b: String)

/** Tests for [[CometConf.COMET_CONVERT_FROM_SHUFFLE_INPUT_ENABLED]]. */
class CometShuffleInputConversionSuite extends CometTestBase {

  import testImplicits._

  /**
   * `CometTestBase` turns on the conversion of leaf operators such as RDD scans, which would
   * convert the scans below the shuffles here before this conversion sees them. It is off by
   * default.
   */
  private def withoutLeafConversion(f: => Unit): Unit =
    withSQLConf(sparkToArrowConversionConfs(enabled = false): _*)(f)

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
   * RDD scans, `spark.comet.convert.rdd.enabled`, is off by default.
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

  /** Runs `f` with each join planned as a sort-merge join of two shuffles into 10 partitions. */
  private def withShuffledJoins(f: => Unit): Unit =
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "10")(f)

  /**
   * Values of each type the conversion admits as a hash key, with boundary values. The floating
   * point values include NaN and both zeros, which a join normalizes.
   */
  private val hashKeys: Seq[(DataType, Seq[Any])] = Seq(
    BooleanType -> Seq(true, false),
    ByteType -> Seq(0.toByte, -1.toByte, Byte.MinValue, Byte.MaxValue),
    ShortType -> Seq(0.toShort, -1.toShort, Short.MinValue, Short.MaxValue),
    IntegerType -> Seq(0, -1, Int.MinValue, Int.MaxValue),
    LongType -> Seq(0L, -1L, Long.MinValue, Long.MaxValue),
    FloatType -> Seq(
      0.0f,
      -0.0f,
      1.5f,
      Float.NaN,
      Float.MinPositiveValue,
      Float.MaxValue,
      Float.NegativeInfinity),
    DoubleType -> Seq(
      0.0d,
      -0.0d,
      1.5d,
      Double.NaN,
      Double.MinPositiveValue,
      Double.MaxValue,
      Double.NegativeInfinity),
    DecimalType(18, 2) -> Seq("0.00", "-0.01", "9999999999999999.99", "-9999999999999999.99")
      .map(new java.math.BigDecimal(_)),
    DateType -> Seq("1970-01-01", "1969-12-31", "0001-01-01", "9999-12-31").map(Date.valueOf),
    TimestampType -> Seq(
      new Timestamp(0L),
      new Timestamp(-1L),
      Timestamp.valueOf("0001-01-01 00:00:00"),
      Timestamp.valueOf("9999-12-31 23:59:59.999999")),
    TimestampNTZType -> Seq(
      LocalDateTime.of(1970, 1, 1, 0, 0),
      LocalDateTime.of(1969, 12, 31, 23, 59, 59, 999999000),
      LocalDateTime.of(1, 1, 1, 0, 0),
      LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999999000)),
    BinaryType -> Seq(Array.emptyByteArray, Array[Byte](0), Array[Byte](-1, -128, 127)))

  /** Four rows of each value and of a NULL, as column `k`, over four input partitions. */
  private def keysDf(keyType: DataType, values: Seq[Any]): DataFrame = {
    val keys = values :+ null
    val data = (0 until keys.length * 4).map(i => Row(keys(i % keys.length), i.toLong))
    val schema = new StructType().add("k", keyType).add("v", LongType)
    spark.createDataFrame(spark.sparkContext.parallelize(data, 4), schema)
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
    // The partial aggregate reads Spark rows, so it runs on Spark, and native shuffle carries its
    // buffers to the final aggregate. With these aggregates the final one runs natively.
    val (_, split) = checkSparkAnswer(
      rowsDf()
        .groupBy((col("k") % 7).as("g"))
        .agg(sum("l"), avg("d"), max("l"), min("l")))
    assert(convertedShuffles(split).length == 1, split)
    assert(collect(split) { case a: HashAggregateExec => a }.length == 1, split)
    assert(collect(split) { case a: CometHashAggregateExec => a }.length == 1, split)
    // The min of a string makes Spark plan sort aggregates, which stay on Spark, so the final
    // aggregate reads the buffers back as rows. The shuffle stays native: the final aggregate
    // reads it through a sort, and the revert in the next test only takes a shuffle that an
    // aggregate reads directly.
    val (_, sorted) = checkSparkAnswer(
      rowsDf()
        .groupBy((col("k") % 7).as("g"))
        .agg(sum("l"), count("s"), avg("d"), sum("m"), max("l"), min("s")))
    assert(convertedShuffles(sorted).length == 1, sorted)
    assert(collect(sorted) { case a: SortAggregateExec => a }.length == 2, sorted)
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

  convertTest("a stage that the transition revert puts back on Spark keeps the JVM shuffle") {
    // The typed operation reads the Comet scan through a transition. With no transitions allowed,
    // the revert puts the stage below the shuffle back on Spark, the conversion included, and the
    // shuffle goes back to the JVM columnar shuffle, which reads the stage's rows.
    withParquetTable((0 until 200).map(i => (i, (i % 13).toString)), "tbl") {
      Seq("true", "false").foreach { aqe =>
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe,
          CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
          CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "0") {
          val ds = spark.sql("SELECT _1 AS a, _2 AS b FROM tbl").as[ShuffleInputRec]
          val (_, plan) = checkSparkAnswer(
            ds.map(r => ShuffleInputRec(r.a % 13, r.b))
              .repartition(7, col("a"))
              .groupBy("a")
              .agg(max("b")))
          assert(conversions(plan).isEmpty, s"AQE $aqe:\n$plan")
          val reverted = cometShuffles(plan).filter(_.shuffleType == CometColumnarShuffle)
          assert(reverted.length == 1, s"AQE $aqe:\n$plan")
          assert(!reverted.head.child.exists(_.isInstanceOf[CometPlan]), s"AQE $aqe:\n$plan")
        }
      }
    }
  }

  hashKeys.foreach { case (keyType, values) =>
    convertTest(s"native shuffle puts ${keyType.simpleString} keys where Spark does") {
      withShuffledJoins {
        val keys = keysDf(keyType, values)
        // The partition of each row, against Spark's partitioner, NULL keys included.
        val (_, placed) = checkSparkAnswer(
          keys.repartition(10, col("k")).select(col("k"), col("v"), spark_partition_id()))
        assert(convertedShuffles(placed).length == 1, placed)
        // The left input converts and the right one, with its array<bigint> column, does not, so
        // matching keys meet only if both shuffles put them in the same partition.
        val left = keys.select(col("k").as("lk"), col("v"))
        val right = keys.select(col("k").as("rk"), array(col("v")).as("xs"))
        val (_, joined) = checkSparkAnswer(left.join(right, col("lk") === col("rk")))
        assert(convertedShuffles(joined).length == 1, joined)
        assert(cometShuffles(joined).count(_.shuffleType == CometColumnarShuffle) == 1, joined)
      }
    }
  }

  convertTest("a shuffle that hashes a wide decimal stays on the JVM columnar shuffle") {
    // Native shuffle hashes a decimal wider than 18 digits differently from Spark's partitioner
    // (#5994). The right input, with its array<int> column, stays on the JVM columnar shuffle,
    // so the left one has to as well, or matching keys land in different partitions.
    withShuffledJoins {
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
    // matching keys with invalid UTF-8 land in different partitions. A key computed from a
    // string, such as its hash, is no different: native shuffle would compute it from the
    // replaced string. Both shuffles write the rows with invalid UTF-8 replaced, and the join
    // compares what they wrote, so the two keys stay apart only because each key expression puts
    // them in different partitions.
    withShuffledJoins {
      val schema = new StructType().add("b", BinaryType).add("v", LongType)
      val data = (0 until 40).map { i =>
        Row(Array((if (i % 2 == 0) 0x80 else 0x83).toByte), i.toLong)
      }
      val keys = spark.createDataFrame(spark.sparkContext.parallelize(data, 4), schema)
      val left = keys.select(col("b").cast(StringType).as("lk"), col("v"))
      val right = keys.select(
        col("b").cast(StringType).as("rk"),
        array(col("v").cast(IntegerType)).as("xs"))
      Seq(col("lk") === col("rk"), hash(col("lk")) === hash(col("rk"))).foreach { condition =>
        val df = left.join(right, condition)
        val (_, plan) = checkSparkAnswer(df)
        assert(conversions(plan).isEmpty, s"$condition:\n$plan")
        checkCometExchange(df, 2, native = false)
      }
    }
  }

  convertTest("nested columns over several batches") {
    // The rows of each input partition fill several batches, and the conversion reuses its
    // vectors from one batch to the next. With the conversion of RDD scans on, native shuffle
    // reads the converted scan the same way.
    val schema = new StructType()
      .add("k", IntegerType)
      .add("payload", new StructType().add("v", LongType).add("s", StringType))
      .add("xs", ArrayType(StringType))
      .add("m", MapType(StringType, StringType))
    val data = (0 until 24).map { i =>
      Row(
        i,
        if (i % 5 == 4) null else Row(i.toLong, if (i % 3 == 0) null else s"s$i"),
        if (i % 6 == 5) null else Seq.tabulate(i % 4)(j => if (j == 1) null else s"x$i-$j"),
        if (i % 7 == 6) null
        else Seq.tabulate(i % 3)(j => s"k$j" -> (if (j == 1) null else s"v$i-$j")).toMap)
    }
    Seq("false", "true").foreach { leafConversion =>
      withSQLConf(
        CometConf.COMET_BATCH_SIZE.key -> "7",
        CometConf.COMET_CONVERT_FROM_RDD_ENABLED.key -> leafConversion) {
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
    // The range partitioner samples the rows that the conversion reads, without converting them,
    // so the conversion only counts the rows that the shuffle writes.
    assert(conversions(plan).head.metrics("numInputRows").value == 1000, plan)
  }

  convertTest("the conversion only takes over from the JVM columnar shuffle") {
    // In `jvm` mode the shuffle keeps the JVM columnar shuffle. In `native` mode, where Comet has
    // no shuffle for a Spark operator's rows, it keeps Spark's shuffle.
    Seq("jvm" -> Seq(CometColumnarShuffle), "native" -> Seq.empty).foreach {
      case (mode, shuffleTypes) =>
        withSQLConf(CometConf.COMET_SHUFFLE_MODE.key -> mode) {
          val (_, plan) =
            checkSparkAnswer(rowsDf().repartition(10, col("k")).groupBy("k").count())
          assert(conversions(plan).isEmpty, s"$mode:\n$plan")
          assert(
            cometShuffles(plan).map(_.shuffleType).distinct == shuffleTypes,
            s"$mode:\n$plan")
        }
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
