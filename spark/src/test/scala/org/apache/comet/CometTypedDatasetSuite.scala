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

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.{CometTestBase, DataFrame, Dataset, Encoders}
import org.apache.spark.sql.comet.CometProjectExec
import org.apache.spark.sql.execution.{MapPartitionsExec, ObjectConsumerExec, ObjectProducerExec, SparkPlan}
import org.apache.spark.sql.internal.SQLConf

/** Top-level so `NewInstance` needs no outer pointer, which is the ordinary user shape. */
case class TypedRec(a: Int, b: String)

case class TypedWide(i: Int, s: String, d: java.math.BigDecimal, opt: Option[Long])

case class TypedNested(id: Int, inner: TypedRec, tags: Seq[String])

case class TypedDec(id: Int, d: java.math.BigDecimal)

/** Twelve columns, to pin the `spark.sql.codegen.maxFields` accounting. */
case class TypedWide12(
    c1: Int,
    c2: Int,
    c3: Int,
    c4: Int,
    c5: Int,
    c6: Int,
    c7: Int,
    c8: Int,
    c9: Int,
    c10: Int,
    c11: Int,
    c12: Int)

/** JVM-static counter. Comet tests run in local mode, so driver and executor share this. */
object TypedMapCounter {
  val calls = new AtomicLong(0)

  def reset(): Unit = calls.set(0)
}

/**
 * Tests for [[org.apache.comet.rules.RewriteTypedDatasetMap]], which fuses the
 * `SerializeFromObject` / `MapElements` / `DeserializeToObject` sandwich a typed `Dataset.map`
 * produces into a Comet projection. See https://github.com/apache/datafusion-comet/issues/5710.
 */
class CometTypedDatasetSuite extends CometTestBase {

  import testImplicits._

  private def withFusion(pairs: (String, String)*)(f: => Unit): Unit =
    withSQLConf(
      (Seq(
        CometConf.COMET_EXEC_TYPED_DATASET_MAP_ENABLED.key -> "true",
        CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "true") ++ pairs): _*)(f)

  /** The common fixture: `rows` rows of `(a: Int, b: String)` via Parquet, read back typed. */
  private def withTypedRecs(rows: Int = 100)(f: Dataset[TypedRec] => Unit): Unit =
    withParquetTable((0 until rows).map(i => (i, i.toString)), "tbl") {
      f(spark.sql("select _1 as a, _2 as b from tbl").as[TypedRec])
    }

  /**
   * Round-trips `df` through Parquet and registers it as `tbl`. Needed rather than
   * `withParquetTable(df, name)` because that registers the DataFrame directly, leaving a
   * `LocalTableScanExec` that the native-plan assertions do not tolerate.
   */
  private def withParquetRoundTrip(df: => DataFrame)(f: => Unit): Unit =
    withTempPath { path =>
      df.write.parquet(path.toString)
      withParquetTable(path.toString, "tbl")(f)
    }

  /**
   * Every operator in the typed-Dataset family, via the two traits Spark uses to mark them. Wider
   * than the `ds.map` sandwich on purpose: it also covers `FlatMapGroupsExec` /
   * `AppendColumnsExec`, which this rule does not fuse, so a future change that starts fusing
   * them is visible here.
   */
  private def objectOperators(plan: SparkPlan): Seq[SparkPlan] =
    collectWithSubqueries(plan) { case p @ (_: ObjectConsumerExec | _: ObjectProducerExec) => p }

  private def assertNoObjectOperators(plan: SparkPlan): Unit = {
    val remaining = objectOperators(plan)
    if (remaining.nonEmpty) {
      fail(
        "expected the typed sandwich to be fused away, but plan still has " +
          s"${remaining.map(_.nodeName).mkString(", ")}:\n$plan")
    }
  }

  test("ds.map produces a fully native plan - single output column") {
    withFusion() {
      withTypedRecs() { ds =>
        val (_, cometPlan) = checkSparkAnswerAndOperator(ds.map(_.a + 1).toDF())
        assertNoObjectOperators(cometPlan)
      }
    }
  }

  test("ds.map produces a fully native plan - multiple output columns") {
    withFusion() {
      withTypedRecs() { ds =>
        val (_, cometPlan) =
          checkSparkAnswerAndOperator(ds.map(r => TypedRec(r.a + 1, r.b + "!")).toDF())
        assertNoObjectOperators(cometPlan)
        // Inner projection builds the struct, outer one unpacks it.
        assert(
          collectWithSubqueries(cometPlan) { case p: CometProjectExec => p }.size >= 2,
          s"expected stacked projections for the struct fuse:\n$cometPlan")
      }
    }
  }

  test("executed-plan output attributes are unchanged by the rewrite") {
    // `Dataset.schema` comes from the analyzed plan, which a physical rule cannot touch, so
    // comparing it would be vacuous. The property that matters is that the rewritten projections
    // reproduce `SerializeFromObjectExec.output` exactly -- names, types and nullability -- since
    // parent operators reference those attributes.
    withTypedRecs(20) { _ =>
      def executedOutput(fused: Boolean): String = {
        var out: String = null
        withSQLConf(CometConf.COMET_EXEC_TYPED_DATASET_MAP_ENABLED.key -> fused.toString) {
          out = spark
            .sql("select _1 as a, _2 as b from tbl")
            .as[TypedRec]
            .map(r => TypedRec(r.a + 1, r.b))
            .toDF()
            .queryExecution
            .executedPlan
            .output
            .map(a => s"${a.name}:${a.dataType.simpleString}:${a.nullable}")
            .mkString(",")
        }
        out
      }
      assert(executedOutput(fused = true) === executedOutput(fused = false))
    }
  }

  test("closure runs exactly once per row with multiple output columns") {
    withFusion() {
      withTypedRecs(50) { ds =>
        TypedMapCounter.reset()
        val fused = ds
          .map { r =>
            TypedMapCounter.calls.incrementAndGet()
            TypedRec(r.a * 2, r.b)
          }
          .toDF()
        assertNoObjectOperators(fused.queryExecution.executedPlan)
        assert(fused.collect().length === 50)
        // The whole point of the struct wrapper: N output columns must not mean N closure calls.
        assert(
          TypedMapCounter.calls.get() === 50,
          s"expected 50 closure calls for 50 rows, got ${TypedMapCounter.calls.get()}")
      }
    }
  }

  test("a 12-column record still fuses (maxFields counts each input ordinal once)") {
    // Every struct field carries the whole deserializer, so counting BoundReference occurrences
    // rather than distinct ordinals made this 12 + 12*12 = 156 fields against a default
    // `spark.sql.codegen.maxFields` of 100, and the rule silently declined.
    withFusion() {
      val cols = (1 to 12).map(i => s"_1 + $i as c$i").mkString(", ")
      withParquetTable((0 until 20).map(i => (i, i.toString)), "tbl") {
        val ds = spark.sql(s"select $cols from tbl").as[TypedWide12]
        val (_, cometPlan) =
          checkSparkAnswerAndOperator(ds.map(r => r.copy(c1 = r.c1 + 1)).toDF())
        assertNoObjectOperators(cometPlan)
      }
    }
  }

  test("fusion unblocks the aggregate and shuffle above it") {
    withFusion() {
      withParquetTable((0 until 100).map(i => (i, (i % 7).toString)), "tbl") {
        val ds = spark.sql("select _1 as a, _2 as b from tbl").as[TypedRec]
        val (_, cometPlan) =
          checkSparkAnswerAndOperator(ds.map(r => TypedRec(r.a + 1, r.b)).groupBy("b").count())
        assertNoObjectOperators(cometPlan)
      }
    }
  }

  test("wide record with decimal, string and Option fields") {
    withFusion() {
      val rows = (1 to 40).map(i =>
        (
          i,
          s"s$i",
          new java.math.BigDecimal(s"$i.25"),
          if (i % 3 == 0) null else Long.box(i * 10L)))
      withParquetRoundTrip(rows.toDF("i", "s", "d", "opt")) {
        val ds = spark.table("tbl").as[TypedWide]
        val (_, cometPlan) = checkSparkAnswerAndOperator(
          ds.map(r => TypedWide(r.i + 1, r.s, r.d, r.opt.map(_ + 1))).toDF())
        assertNoObjectOperators(cometPlan)
      }
    }
  }

  test("nested struct and array fields round-trip") {
    withFusion() {
      val nested = (1 to 30)
        .map(i => (i, (i, s"n$i"), Seq(s"t$i", s"u$i")))
        .toDF("id", "inner", "tags")
        .selectExpr("id", "named_struct('a', inner._1, 'b', inner._2) as inner", "tags")
      withParquetRoundTrip(nested) {
        val ds = spark.table("tbl").as[TypedNested]
        val (_, cometPlan) = checkSparkAnswerAndOperator(
          ds.map(r => TypedNested(r.id + 1, TypedRec(r.inner.a, r.inner.b), r.tags.reverse))
            .toDF())
        assertNoObjectOperators(cometPlan)
      }
    }
  }

  test("null field values in the input and the output") {
    withFusion() {
      val withNulls = (1 to 30).map(i => (i, if (i % 4 == 0) null else s"s$i")).toDF("a", "b")
      withParquetRoundTrip(withNulls) {
        val ds = spark.table("tbl").as[TypedRec]
        val (_, cometPlan) = checkSparkAnswerAndOperator(
          ds.map(r => TypedRec(r.a, if (r.a % 5 == 0) null else r.b)).toDF())
        assertNoObjectOperators(cometPlan)
      }
    }
  }

  test("AssertNotNull inside the fused kernel still raises like Spark") {
    // Returning null for a non-nullable top-level product is an error in Spark. The serializer's
    // `assertnotnull` has to survive the fuse, or Comet would silently emit a null row instead.
    withFusion() {
      withTypedRecs(20) { ds =>
        val df = ds.map(r => if (r.a == 7) null else TypedRec(r.a, r.b)).toDF()
        assertNoObjectOperators(df.queryExecution.executedPlan)
        val err = intercept[Exception](df.collect())
        // Spark 3.x raises NullPointerException("Null value appeared ..."), Spark 4.x raises
        // SparkRuntimeException("[NOT_NULL_ASSERT_VIOLATION] NULL value appeared ..."). Match the
        // part they share.
        assert(
          Option(err.getMessage).exists(
            _.toLowerCase.contains("value appeared in non-nullable field")),
          s"expected a not-null assertion failure, got: ${err.getMessage}")
      }
    }
  }

  test("decimal overflow in the serializer is caught before the Arrow write") {
    // The concern on #5710 was that an encoder-declared decimal(38,18) could receive a wider
    // value that Spark nulls at row materialization but the kernel's Arrow DecimalVector write
    // would not -- the shape of the Iceberg truncate(w, decimal) bug from #5575. It does not
    // apply: the encoder's serializer already wraps the value in `CheckOverflow`, so the fused
    // tree raises (ANSI) or nulls (non-ANSI) exactly where Spark does, ahead of the write.
    withFusion() {
      withParquetTable((1 to 5).map(i => (i, new java.math.BigDecimal(s"$i.5"))), "tbl") {
        val ds = spark.sql("select _1 as id, _2 as d from tbl").as[TypedDec]
        def overflowed =
          ds.map(r => TypedDec(r.id, r.d.multiply(new java.math.BigDecimal("1" + "0" * 30))))
            .toDF()

        assertNoObjectOperators(overflowed.queryExecution.executedPlan)

        withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
          val err = intercept[Exception](overflowed.collect())
          assert(
            err.getMessage.contains("cannot be represented as Decimal(38, 18)"),
            s"expected an ANSI overflow error, got: ${err.getMessage}")
        }
        withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
          // Non-ANSI nulls the overflowing value; Comet must produce the same nulls as Spark.
          val (_, cometPlan) = checkSparkAnswerAndOperator(overflowed)
          assertNoObjectOperators(cometPlan)
        }
      }
    }
  }

  test("Java MapFunction takes the same path as a Scala closure") {
    withFusion() {
      withTypedRecs(40) { ds =>
        val fn = new MapFunction[TypedRec, TypedRec] {
          override def call(r: TypedRec): TypedRec = TypedRec(r.a + 100, r.b)
        }
        val (_, cometPlan) =
          checkSparkAnswerAndOperator(ds.map(fn, Encoders.product[TypedRec]).toDF())
        assertNoObjectOperators(cometPlan)
      }
    }
  }

  test("chained maps fuse into one native pipeline") {
    withFusion() {
      withTypedRecs(40) { ds =>
        val (_, cometPlan) = checkSparkAnswerAndOperator(
          ds.map(r => TypedRec(r.a + 1, r.b)).map(r => TypedRec(r.a * 2, r.b + "x")).toDF())
        assertNoObjectOperators(cometPlan)
      }
    }
  }

  test("off by default") {
    withTypedRecs(20) { ds =>
      val df = ds.map(r => TypedRec(r.a + 1, r.b)).toDF()
      df.collect()
      assert(
        objectOperators(df.queryExecution.executedPlan).nonEmpty,
        "rewrite must not apply unless it is explicitly enabled")
    }
  }

  test("declines when the codegen dispatcher is disabled") {
    withSQLConf(
      CometConf.COMET_EXEC_TYPED_DATASET_MAP_ENABLED.key -> "true",
      CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "false") {
      withTypedRecs(20) { ds =>
        checkSparkAnswerAndFallbackReason(
          ds.map(r => TypedRec(r.a + 1, r.b)).toDF(),
          s"Cannot fuse typed Dataset map: " +
            s"${CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key}=false, so there is no dispatcher " +
            "to fuse into")
      }
    }
  }

  test("declines multi-column fusion when subexpression elimination is off") {
    // Without CSE the single kernel would evaluate the shared closure Invoke once per struct
    // field, so the rule must decline rather than change how many times user code runs.
    withFusion(SQLConf.SUBEXPRESSION_ELIMINATION_ENABLED.key -> "false") {
      withTypedRecs(20) { ds =>
        checkSparkAnswerAndFallbackReason(
          ds.map(r => TypedRec(r.a + 1, r.b)).toDF(),
          "Cannot fuse typed Dataset map: 2 output columns require subexpression elimination " +
            "to keep the closure to one call per row, but " +
            s"${SQLConf.SUBEXPRESSION_ELIMINATION_ENABLED.key}=false")
      }
    }
  }

  test("mapPartitions is not fused") {
    withFusion() {
      withTypedRecs(20) { ds =>
        val df = ds.mapPartitions(it => it.map(r => TypedRec(r.a + 1, r.b))).toDF()
        checkSparkAnswer(df)
        assert(
          collectWithSubqueries(df.queryExecution.executedPlan) { case p: MapPartitionsExec =>
            p
          }.nonEmpty,
          "mapPartitions is iterator-shaped and must be left alone")
      }
    }
  }
}
