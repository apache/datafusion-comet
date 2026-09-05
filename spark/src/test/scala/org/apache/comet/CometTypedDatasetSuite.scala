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
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.comet.CometProjectExec
import org.apache.spark.sql.execution.{DeserializeToObjectExec, MapElementsExec, MapPartitionsExec, SerializeFromObjectExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf

/** Top-level so `NewInstance` needs no outer pointer, which is the ordinary user shape. */
case class TypedRec(a: Int, b: String)

case class TypedWide(i: Int, s: String, d: java.math.BigDecimal, opt: Option[Long])

case class TypedNested(id: Int, inner: TypedRec, tags: Seq[String])

case class TypedDec(id: Int, d: java.math.BigDecimal)

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
class CometTypedDatasetSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  import testImplicits._

  private def withFusion(pairs: (String, String)*)(f: => Unit): Unit =
    withSQLConf(
      (Seq(
        CometConf.COMET_EXEC_TYPED_DATASET_MAP_ENABLED.key -> "true",
        CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "true") ++ pairs): _*)(f)

  private def objectOperators(plan: SparkPlan): Seq[SparkPlan] =
    collectWithSubqueries(plan) {
      case p: SerializeFromObjectExec => p
      case p: DeserializeToObjectExec => p
      case p: MapElementsExec => p
      case p: MapPartitionsExec => p
    }

  private def assertNoObjectOperators(plan: SparkPlan): Unit =
    assert(
      objectOperators(plan).isEmpty,
      s"expected the typed sandwich to be fused away, but plan still has " +
        s"${objectOperators(plan).map(_.nodeName).mkString(", ")}:\n$plan")

  test("ds.map produces a fully native plan - single output column") {
    withFusion() {
      withParquetTable((0 until 100).map(i => (i, i.toString)), "tbl") {
        val ds = spark.sql("select _1 as a, _2 as b from tbl").as[TypedRec]
        val (_, cometPlan) = checkSparkAnswerAndOperator(ds.map(_.a + 1).toDF())
        assertNoObjectOperators(cometPlan)
      }
    }
  }

  test("ds.map produces a fully native plan - multiple output columns") {
    withFusion() {
      withParquetTable((0 until 100).map(i => (i, i.toString)), "tbl") {
        val ds = spark.sql("select _1 as a, _2 as b from tbl").as[TypedRec]
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

  test("output schema is unchanged by the rewrite") {
    withParquetTable((0 until 20).map(i => (i, i.toString)), "tbl") {
      // `withSQLConf` returns Unit on Spark 3.x, so capture rather than return from the block.
      def schemaOf(fused: Boolean): String = {
        var schema: String = null
        withSQLConf(CometConf.COMET_EXEC_TYPED_DATASET_MAP_ENABLED.key -> fused.toString) {
          schema = spark
            .sql("select _1 as a, _2 as b from tbl")
            .as[TypedRec]
            .map(r => TypedRec(r.a + 1, r.b))
            .toDF()
            .schema
            .treeString
        }
        schema
      }
      assert(schemaOf(fused = true) === schemaOf(fused = false))
    }
  }

  test("closure runs exactly once per row with multiple output columns") {
    withFusion() {
      withParquetTable((0 until 50).map(i => (i, i.toString)), "tbl") {
        val ds = spark.sql("select _1 as a, _2 as b from tbl").as[TypedRec]
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
      withTempPath { path =>
        rows.toDF("i", "s", "d", "opt").write.parquet(path.toString)
        withParquetTable(path.toString, "tbl") {
          val ds = spark.table("tbl").as[TypedWide]
          val (_, cometPlan) = checkSparkAnswerAndOperator(
            ds.map(r => TypedWide(r.i + 1, r.s, r.d, r.opt.map(_ + 1))).toDF())
          assertNoObjectOperators(cometPlan)
        }
      }
    }
  }

  test("nested struct and array fields round-trip") {
    withFusion() {
      withTempPath { path =>
        (1 to 30)
          .map(i => (i, (i, s"n$i"), Seq(s"t$i", s"u$i")))
          .toDF("id", "inner", "tags")
          .selectExpr("id", "named_struct('a', inner._1, 'b', inner._2) as inner", "tags")
          .write
          .parquet(path.toString)
        withParquetTable(path.toString, "tbl") {
          val ds = spark.table("tbl").as[TypedNested]
          val (_, cometPlan) = checkSparkAnswerAndOperator(
            ds.map(r => TypedNested(r.id + 1, TypedRec(r.inner.a, r.inner.b), r.tags.reverse))
              .toDF())
          assertNoObjectOperators(cometPlan)
        }
      }
    }
  }

  test("null field values in the input and the output") {
    withFusion() {
      withTempPath { path =>
        (1 to 30)
          .map(i => (i, if (i % 4 == 0) null else s"s$i"))
          .toDF("a", "b")
          .write
          .parquet(path.toString)
        withParquetTable(path.toString, "tbl") {
          val ds = spark.table("tbl").as[TypedRec]
          val (_, cometPlan) = checkSparkAnswerAndOperator(
            ds.map(r => TypedRec(r.a, if (r.a % 5 == 0) null else r.b)).toDF())
          assertNoObjectOperators(cometPlan)
        }
      }
    }
  }

  test("AssertNotNull inside the fused kernel still raises like Spark") {
    // Returning null for a non-nullable top-level product is an error in Spark. The serializer's
    // `assertnotnull` has to survive the fuse, or Comet would silently emit a null row instead.
    withFusion() {
      withParquetTable((0 until 20).map(i => (i, i.toString)), "tbl") {
        val ds = spark.sql("select _1 as a, _2 as b from tbl").as[TypedRec]
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
      withParquetTable((0 until 40).map(i => (i, i.toString)), "tbl") {
        val ds = spark.sql("select _1 as a, _2 as b from tbl").as[TypedRec]
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
      withParquetTable((0 until 40).map(i => (i, i.toString)), "tbl") {
        val ds = spark.sql("select _1 as a, _2 as b from tbl").as[TypedRec]
        val (_, cometPlan) = checkSparkAnswerAndOperator(
          ds.map(r => TypedRec(r.a + 1, r.b)).map(r => TypedRec(r.a * 2, r.b + "x")).toDF())
        assertNoObjectOperators(cometPlan)
      }
    }
  }

  test("off by default") {
    withParquetTable((0 until 20).map(i => (i, i.toString)), "tbl") {
      val ds = spark.sql("select _1 as a, _2 as b from tbl").as[TypedRec]
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
      withParquetTable((0 until 20).map(i => (i, i.toString)), "tbl") {
        val ds = spark.sql("select _1 as a, _2 as b from tbl").as[TypedRec]
        val df = ds.map(r => TypedRec(r.a + 1, r.b)).toDF()
        checkSparkAnswer(df)
        assert(
          objectOperators(df.queryExecution.executedPlan).nonEmpty,
          "without the dispatcher there is nothing to fuse into")
      }
    }
  }

  test("declines multi-column fusion when subexpression elimination is off") {
    // Without CSE the single kernel would evaluate the shared closure Invoke once per struct
    // field, so the rule must decline rather than change how many times user code runs.
    withFusion(SQLConf.SUBEXPRESSION_ELIMINATION_ENABLED.key -> "false") {
      withParquetTable((0 until 20).map(i => (i, i.toString)), "tbl") {
        val ds = spark.sql("select _1 as a, _2 as b from tbl").as[TypedRec]
        val df = ds.map(r => TypedRec(r.a + 1, r.b)).toDF()
        checkSparkAnswer(df)
        assert(
          objectOperators(df.queryExecution.executedPlan).nonEmpty,
          "multi-column fusion needs CSE to keep the closure to one call per row")
      }
    }
  }

  test("mapPartitions is not fused") {
    withFusion() {
      withParquetTable((0 until 20).map(i => (i, i.toString)), "tbl") {
        val ds = spark.sql("select _1 as a, _2 as b from tbl").as[TypedRec]
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
