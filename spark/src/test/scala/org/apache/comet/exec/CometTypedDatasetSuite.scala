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

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.sql.{CometTestBase, DataFrame, Dataset}
import org.apache.spark.sql.catalyst.expressions.aggregate.Partial
import org.apache.spark.sql.comet.{CometBroadcastHashJoinExec, CometHashAggregateExec, CometSparkToColumnarExec}
import org.apache.spark.sql.execution.{ColumnarToRowExec, ColumnarToRowTransition, SerializeFromObjectExec, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.functions.{broadcast, col, size, sum}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.{CometConf, ExtendedExplainInfo}

// Top-level, so the encoders need no outer pointer, which is the ordinary user shape.
case class TypedDsRec(a: Int, b: String)

case class TypedDsWide(i: Int, s: String, d: java.math.BigDecimal, opt: Option[Long])

case class TypedDsNested(id: Int, inner: TypedDsRec, tags: Seq[String])

case class TypedDsInts(id: Int, xs: Seq[Int])

case class TypedDsDecimal(k: java.math.BigDecimal, v: Long)

case class TypedDsDecimalInts(k: java.math.BigDecimal, xs: Seq[Int])

/** Counts calls to a user function. Comet tests run in local mode, so tasks see this object. */
object TypedDsCounter {
  val calls = new AtomicLong(0)
}

/** Tests for [[CometConf.COMET_CONVERT_FROM_TYPED_DATASET_ENABLED]]. */
class CometTypedDatasetSuite extends CometTestBase {

  import testImplicits._

  /** Defines a test that runs with the typed Dataset output conversion enabled. */
  private def convertTest(name: String)(f: => Unit): Unit =
    test(name) {
      withSQLConf(CometConf.COMET_CONVERT_FROM_TYPED_DATASET_ENABLED.key -> "true")(f)
    }

  /** `rows` rows of `TypedDsRec` in a Parquet table, read back as a typed Dataset. */
  private def withRecs(rows: Int = 200)(f: Dataset[TypedDsRec] => Unit): Unit =
    withParquetTable((0 until rows).map(i => (i, (i % 13).toString)), "tbl") {
      f(spark.sql("SELECT _1 AS a, _2 AS b FROM tbl").as[TypedDsRec])
    }

  private def conversions(plan: SparkPlan): Seq[CometSparkToColumnarExec] =
    collectWithSubqueries(plan) { case c: CometSparkToColumnarExec => c }

  /**
   * Spark inserts no columnar transitions below a `CometSparkToColumnarExec`, so the rule adds
   * them for the typed operation's own operators. Without one, an operator reads its Comet child
   * through `CometExec.doExecute`, Spark's interpreted columnar-to-row path, which gives the
   * right answer slowly, so only the plan shows it.
   */
  private def assertRowOperatorsReadThroughTransitions(plan: SparkPlan): Unit = {
    // `InputAdapter` and `WholeStageCodegenExec` report their child's `supportsColumnar`.
    val bare = collectWithSubqueries(plan) {
      case p
          if !p.supportsColumnar && !p.isInstanceOf[ColumnarToRowTransition] &&
            p.children.exists(_.supportsColumnar) =>
        p
    }
    assert(bare.isEmpty, s"row operators read a columnar child without a transition:\n$plan")
    assert(
      collectWithSubqueries(plan) { case c: ColumnarToRowExec => c }.isEmpty,
      s"expected Comet's columnar-to-row transitions, not Spark's:\n$plan")
  }

  /**
   * Checks the answer, that everything above the typed operation runs natively, and that the
   * operation's output is converted right above its `SerializeFromObjectExec`.
   */
  private def checkConverted(df: DataFrame): SparkPlan = {
    val (_, plan) =
      checkSparkAnswerAndOperator(df, includeClasses = Seq(classOf[CometSparkToColumnarExec]))
    conversions(plan).foreach { c =>
      val converted = c.child match {
        case w: WholeStageCodegenExec => w.child
        case other => other
      }
      assert(
        converted.isInstanceOf[SerializeFromObjectExec],
        s"expected the conversion directly above SerializeFromObject:\n$plan")
    }
    assertRowOperatorsReadThroughTransitions(plan)
    // The transitions are added on the first of the rule's passes under AQE. A later pass must
    // not report them as operators Comet failed to convert.
    val reasons = new ExtendedExplainInfo().getFallbackReasons(plan)
    assert(!reasons.exists(_.contains("ColumnarToRow")), reasons)
    plan
  }

  private def nativePartialAggregates(plan: SparkPlan): Seq[CometHashAggregateExec] =
    collectWithSubqueries(plan) {
      case a: CometHashAggregateExec if a.modes.contains(Partial) => a
    }

  convertTest("the aggregate and the shuffle above a map run natively") {
    withRecs() { ds =>
      Seq("true", "false").foreach { aqe =>
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe) {
          val plan = checkConverted(ds.map(r => TypedDsRec(r.a + 1, r.b)).groupBy("b").count())
          assert(nativePartialAggregates(plan).nonEmpty, s"AQE $aqe:\n$plan")
        }
      }
    }
  }

  convertTest("flatMap, mapPartitions, mapGroups and cogroup") {
    withRecs() { ds =>
      checkConverted(ds.flatMap(r => Seq(r, TypedDsRec(-r.a, r.b))).groupBy("b").count())
      checkConverted(ds.mapPartitions(_.map(r => TypedDsRec(r.a + 1, r.b))).groupBy("b").count())
      checkConverted(
        ds.groupByKey(_.b)
          .mapGroups((k, rows) => TypedDsRec(rows.map(_.a).sum, k))
          .groupBy("a")
          .count())
      checkConverted(
        ds.groupByKey(_.b)
          .cogroup(ds.groupByKey(_.b))((k, left, right) =>
            Iterator(TypedDsRec(left.size + right.size, k)))
          .groupBy("a")
          .count())
    }
  }

  convertTest("a Dataset built from an RDD of objects") {
    val ds = spark.sparkContext
      .parallelize((0 until 200).map(i => TypedDsRec(i, (i % 13).toString)), 4)
      .toDS()
    checkConverted(ds.groupBy("b").agg(sum("a")))
  }

  convertTest("a broadcast join above the typed operation runs natively") {
    withRecs() { ds =>
      withParquetTable((0 until 13).map(i => (i.toString, s"name$i")), "dim") {
        val dim = spark.table("dim").toDF("b", "name")
        val plan = checkConverted(ds.map(r => TypedDsRec(r.a * 2, r.b)).join(broadcast(dim), "b"))
        assert(
          collectWithSubqueries(plan) { case j: CometBroadcastHashJoinExec => j }.nonEmpty,
          plan)
      }
    }
  }

  convertTest("decimal, string, Option, nested struct and string array fields") {
    val wide = (1 to 40).map(i =>
      (i, s"s$i", new java.math.BigDecimal(s"$i.25"), if (i % 3 == 0) None else Some(i * 10L)))
    withParquetTable(wide, "wide") {
      val ds = spark.sql("SELECT _1 AS i, _2 AS s, _3 AS d, _4 AS opt FROM wide").as[TypedDsWide]
      checkConverted(
        ds.map(r => TypedDsWide(r.i % 4, r.s, r.d, r.opt.map(_ + 1)))
          .groupBy("i")
          .agg(sum("d"), sum("opt")))
    }
    val nested = (1 to 30).map(i => (i, (i, s"n$i"), Seq(s"t$i", null)))
    withParquetTable(nested, "nested") {
      val ds = spark
        .sql(
          "SELECT _1 AS id, named_struct('a', _2._1, 'b', _2._2) AS inner, _3 AS tags " +
            "FROM nested")
        .as[TypedDsNested]
      checkConverted(
        ds.map(r => TypedDsNested(r.id % 3, TypedDsRec(r.inner.a, r.inner.b), r.tags.reverse))
          .groupBy("id")
          .count())
    }
  }

  convertTest("nothing above the typed operation consumes Arrow, so nothing is converted") {
    withRecs() { ds =>
      val (_, plan) = checkSparkAnswer(ds.map(r => TypedDsRec(r.a + 1, r.b)).toDF())
      assert(conversions(plan).isEmpty, plan)
      assertRowOperatorsReadThroughTransitions(plan)
    }
  }

  convertTest("columns the conversion does not support keep the operators above on Spark") {
    withParquetTable((0 until 20).map(i => (i, Seq(i, i + 1))), "ints") {
      val ds = spark.sql("SELECT _1 AS id, _2 AS xs FROM ints").as[TypedDsInts]
      val (_, plan) = checkSparkAnswerAndFallbackReason(
        // The aggregate reads `xs`, or Spark would prune it from the serializer.
        ds.map(r => TypedDsInts(r.id % 3, r.xs.reverse)).groupBy("id").agg(sum(size(col("xs")))),
        "Comet cannot convert the output of a typed Dataset operation to Arrow because it " +
          "does not support the type of these columns: xs: array<int>")
      assert(conversions(plan).isEmpty, plan)
    }
  }

  convertTest("a join on wide decimal keys with an input that is not converted") {
    // The left input converts and the right one, with its array<int> column, does not. Native
    // shuffle hashes a decimal wider than 18 digits differently from Spark's partitioner (#5994),
    // so the shuffle above the conversion has to stay on Comet's columnar shuffle like the right
    // input's, or matching keys land in different partitions and the join loses rows.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "10") {
      val left = spark
        .range(0, 100, 1, 2)
        .map(i => TypedDsDecimal(new java.math.BigDecimal(i.longValue), i.longValue))
        .alias("l")
      val right = spark
        .range(0, 100, 1, 2)
        .map(i => TypedDsDecimalInts(new java.math.BigDecimal(i.longValue), Seq(i.intValue)))
        .alias("r")
      val df = left.join(right, col("l.k") === col("r.k")).select(col("l.v"), col("r.xs"))
      val (_, plan) = checkSparkAnswer(df)
      assert(conversions(plan).nonEmpty, plan)
      // One columnar shuffle for each input of the join.
      checkCometExchange(df, 2, native = false)
    }
  }

  convertTest("the user function runs once per row") {
    withRecs(500) { ds =>
      TypedDsCounter.calls.set(0)
      val df = ds
        .map { r =>
          TypedDsCounter.calls.incrementAndGet()
          TypedDsRec(r.a, r.b)
        }
        .groupBy("b")
        .count()
      assert(df.collect().map(_.getLong(1)).sum == 500)
      assert(conversions(df.queryExecution.executedPlan).nonEmpty, df.queryExecution.executedPlan)
      assert(TypedDsCounter.calls.get() == 500)
    }
  }

  convertTest("an exception from the user function fails the query") {
    // One partition, so the failing row is past the first batch, where an error from a JVM input
    // reaches the native plan through the Arrow stream rather than on the JVM.
    withTempPath { path =>
      spark
        .range(20000)
        .selectExpr("CAST(id AS INT) AS a", "CAST(id % 13 AS STRING) AS b")
        .coalesce(1)
        .write
        .parquet(path.toString)
      val df = spark.read
        .parquet(path.toString)
        .as[TypedDsRec]
        .map { r =>
          if (r.a == 15000) throw new IllegalArgumentException("typed map failed at 15000")
          r
        }
        .groupBy("b")
        .count()
      assert(conversions(df.queryExecution.executedPlan).nonEmpty, df.queryExecution.executedPlan)
      val e = intercept[Exception](df.collect())
      assert(
        causeChain(e).exists(t => Option(t.getMessage).exists(_.contains("failed at 15000"))),
        e)
    }
  }

  /**
   * One partition whose user function throws on row 30. Spark computes typed rows as they are
   * read, so a reader that stops before row 30 never reaches it, but an Arrow batch would.
   */
  private def failsOnRow30: Dataset[Long] =
    spark
      .range(0, 100, 1, 1)
      .map { i =>
        if (i == 30L) {
          throw new IllegalArgumentException("unexpected evaluation of row 30")
        }
        i + 1L
      }

  convertTest("a limit does not evaluate typed Dataset rows beyond the result") {
    Seq("true", "false").foreach { aqe =>
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe) {
        val (_, plan) = checkSparkAnswerAndFallbackReason(
          failsOnRow30.toDF().limit(1),
          "Comet does not convert the output of a typed Dataset operation when a limit can " +
            "stop reading it early")
        assert(conversions(plan).isEmpty, s"AQE $aqe:\n$plan")
      }
    }
  }

  convertTest("a mapPartitions function does not evaluate typed Dataset rows it never reads") {
    Seq("true", "false").foreach { aqe =>
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe) {
        // The filter between the two typed operations is what the conversion would run natively.
        val (_, plan) = checkSparkAnswerAndFallbackReason(
          failsOnRow30.filter(col("value") > 0L).mapPartitions(_.take(1)).toDF(),
          "Comet does not convert the output of a typed Dataset operation when a mapPartitions " +
            "function can stop reading it early")
        assert(conversions(plan).isEmpty, s"AQE $aqe:\n$plan")
      }
    }
  }

  convertTest("code reading Dataset.rdd does not evaluate typed Dataset rows it never reads") {
    Seq("true", "false").foreach { aqe =>
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe) {
        val filtered = failsOnRow30.filter(col("value") > 0L)
        assert(filtered.rdd.take(1).toSeq == Seq(1L))
        // When the Dataset ends in a typed operation, Spark drops the deserializer that `rdd`
        // adds, so the root of the plan is that operation, or a typed filter over it.
        assert(filtered.map(_ + 1L).rdd.take(1).toSeq == Seq(2L))
        assert(filtered.map(_ + 1L).filter((v: Long) => v > 0L).rdd.take(1).toSeq == Seq(2L))
      }
    }
  }

  convertTest("a limit above an aggregate keeps the conversion the aggregate reads all of") {
    withRecs() { ds =>
      Seq("true", "false").foreach { aqe =>
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe) {
          // There are 13 groups, so the limit keeps all of them in whatever order they come.
          checkConverted(ds.map(r => TypedDsRec(r.a + 1, r.b)).groupBy("b").count().limit(20))
        }
      }
    }
  }

  // https://github.com/apache/datafusion-comet/issues/6573
  convertTest("input_file_name above a map keeps the map's output on Spark") {
    withTempPath { dir =>
      spark.range(9000).repartition(3).write.parquet(dir.toString)
      // The filter would run in Comet above the conversion, which would read ahead of the files
      // Spark's reader is on when Spark evaluates input_file_name in the project.
      val df = spark.read
        .parquet(dir.toString)
        .as[Long]
        .map(_ + 1)
        .toDF("id")
        .where("id >= 0")
        .selectExpr("input_file_name()", "input_file_block_start()", "input_file_block_length()")
      val (_, plan) = checkSparkAnswerAndFallbackReason(
        df,
        "Spark to Arrow conversion is not compatible with input_file_name")
      assert(conversions(plan).isEmpty, plan)
    }
  }

  test("off by default") {
    withRecs() { ds =>
      val (_, plan) = checkSparkAnswer(ds.map(r => TypedDsRec(r.a + 1, r.b)).groupBy("b").count())
      assert(conversions(plan).isEmpty, plan)
      assert(nativePartialAggregates(plan).isEmpty, plan)
    }
  }
}
