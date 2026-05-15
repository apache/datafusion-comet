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

import org.apache.arrow.vector._
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.types._

import org.apache.comet.udf.codegen.CometScalaUDFCodegen

/**
 * Smoke tests for the Arrow-direct codegen dispatcher. Runs ScalaUDF queries across the scalar
 * and complex type surface, composed UDF trees, subquery reuse, `TaskContext` propagation, and
 * per-task cache isolation, asserting results match Spark.
 */
class CometCodegenDispatchSmokeSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key, "true")

  private def withSubjects(values: String*)(f: => Unit): Unit = {
    withTable("t") {
      sql("CREATE TABLE t (s STRING) USING parquet")
      val rows = values
        .map(v => if (v == null) "(NULL)" else s"('${v.replace("'", "''")}')")
        .mkString(", ")
      sql(s"INSERT INTO t VALUES $rows")
      f
    }
  }

  /**
   * Composition smoke tests. Demonstrate that the codegen dispatcher handles nested expression
   * trees in one compile per (tree, schema) pair, not one JNI hop per sub-expression. Each test
   * wraps the query in `assertCodegenDidWork` to prove the codegen path ran rather than silently
   * falling back to Spark.
   */
  private def assertCodegenDidWork(f: => Unit): Unit = {
    CometScalaUDFCodegen.resetStats()
    f
    val after = CometScalaUDFCodegen.stats()
    assert(
      after.compileCount + after.cacheHitCount >= 1,
      s"expected codegen dispatcher activity, got $after")
  }

  /**
   * Stronger form of [[assertCodegenDidWork]]: asserts the full expression subtree compiled into
   * at most one kernel. A "one JNI crossing per nesting level" implementation would produce one
   * cache entry per sub-expression and `compileCount` of N. `<=` rather than `==` because the
   * cache is JVM-wide; a prior test may have produced a hit (compileCount==0). The activity check
   * guards against silent Spark fallback where the first two asserts pass vacuously.
   */
  private def assertOneKernelForSubtree(f: => Unit): Unit = {
    CometScalaUDFCodegen.resetStats()
    val sizeBefore = CometScalaUDFCodegen.stats().cacheSize
    f
    val after = CometScalaUDFCodegen.stats()
    assert(after.compileCount <= 1, s"expected <= 1 compile for the composed subtree, got $after")
    val grew = after.cacheSize - sizeBefore
    assert(grew <= 1, s"expected cache to grow by <= 1 entry, grew by $grew; stats=$after")
    assert(
      after.compileCount + after.cacheHitCount >= 1,
      s"expected codegen dispatcher activity, got $after")
  }

  /**
   * Assert the compile cache contains a kernel matching the given input Arrow vector classes (in
   * ordinal order) and output `DataType`. A specialization check: if a future change loses
   * vector-class discrimination on the cache key, `checkSparkAnswerAndOperator` still passes
   * (Spark answers correctly) but this assertion fails. Cache is JVM-wide so a prior test's
   * compile counts; pair with `assertCodegenDidWork` to also prove this test ran the dispatcher.
   *
   * Compares by simple name because `common` shades `org.apache.arrow`; a direct
   * `classOf[VarCharVector]` here (unshaded) wouldn't match the shaded class the dispatcher
   * actually stores.
   */
  private def assertKernelSignaturePresent(
      inputs: Seq[Class[_ <: ValueVector]],
      output: DataType): Unit = {
    val sigs = CometScalaUDFCodegen.snapshotCompiledSignatures()
    val expectedNames = inputs.map(_.getSimpleName).toIndexedSeq
    val present = sigs.exists { case (cached, dt) =>
      dt == output && cached.map(_.getSimpleName) == expectedNames
    }
    assert(
      present,
      s"expected kernel signature $expectedNames -> $output; " +
        s"cache had ${sigs.map { case (c, d) => (c.map(_.getSimpleName), d) }}")
  }

  /**
   * Multi-column smoke tests. The dispatcher compiles the whole bound expression tree, including
   * composed sub-expressions that reference multiple columns. Verify end-to-end correctness
   * against Spark for a handful of representative shapes.
   */
  private def withTwoStringCols(rows: (String, String)*)(f: => Unit): Unit = {
    withTable("t") {
      sql("CREATE TABLE t (c1 STRING, c2 STRING) USING parquet")
      if (rows.nonEmpty) {
        val tuples = rows.map { case (a, b) =>
          val av = if (a == null) "NULL" else s"'${a.replace("'", "''")}'"
          val bv = if (b == null) "NULL" else s"'${b.replace("'", "''")}'"
          s"($av, $bv)"
        }
        sql(s"INSERT INTO t VALUES ${tuples.mkString(", ")}")
      }
      f
    }
  }

  test("ScalaUDF over concat(c1, c2) suppresses the null short-circuit") {
    // Concat is not NullIntolerant. The dispatcher's short-circuit guard inspects every node in
    // the bound tree and must skip the whole-tree null short-circuit because one child is
    // non-NullIntolerant. The kernel therefore delegates null handling to Spark's generated
    // code (which handles Concat(null, x) = x correctly) rather than returning null for any
    // null input. Without the guard, null inputs would produce null outputs even where Spark
    // produces a non-null concatenation.
    spark.udf.register("tag", (s: String) => if (s == null) "N" else s"[${s}]")
    withTwoStringCols(("abc", "123"), ("abc", null), (null, "123"), (null, null), ("zz", "zz")) {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT tag(concat(c1, c2)) FROM t"))
      }
    }
  }

  test("disabled mode bypasses the dispatcher") {
    // When the per-feature config is off, `CometScalaUDF.convert` returns None and the enclosing
    // operator falls back to Spark. The dispatcher's counters must not move. We do not assert
    // `checkSparkAnswerAndOperator` here because ScalaUDF has no Comet-native path, so the
    // project runs on the JVM Spark path under this configuration.
    spark.udf.register("noopStr", (s: String) => s)
    CometScalaUDFCodegen.resetStats()
    withSQLConf(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "false") {
      withSubjects("disabled_1", null) {
        checkSparkAnswer(sql("SELECT noopStr(s) FROM t"))
      }
    }
    val after = CometScalaUDFCodegen.stats()
    assert(
      after.compileCount == 0 && after.cacheHitCount == 0,
      s"expected no dispatcher activity under disabled config, got $after")
  }

  test("per-batch nullability produces distinct compiles for null-present vs null-absent") {
    // Same ScalaUDF + same Arrow vector class + different observed nullability should hit
    // different cache keys, because `ArrowColumnSpec.nullable` flips when the batch has no
    // nulls. We don't assert on per-run deltas because Spark's partitioning can split the
    // subject table so the first query alone sees both nullability variants across different
    // partitions. Instead, assert the total invariant: across both queries we see at least two
    // compiles, proving the cache key discriminated on nullability.
    spark.udf.register("nullabilityMarker", (s: String) => if (s == null) null else s + "!")
    CometScalaUDFCodegen.resetStats()

    withSubjects("nullability_marker_1", null, "nullability_marker_2") {
      checkSparkAnswerAndOperator(sql("SELECT nullabilityMarker(s) FROM t"))
    }
    withSubjects("nullability_marker_3", "nullability_marker_4") {
      checkSparkAnswerAndOperator(sql("SELECT nullabilityMarker(s) FROM t"))
    }
    val after = CometScalaUDFCodegen.stats()

    assert(
      after.compileCount >= 2,
      "expected at least two compiles across both nullability distributions (one per " +
        s"nullable=true/false variant); got $after")
  }

  test("dispatcher caches the compiled kernel across batches of one query") {
    // Within a single query, the dispatcher compiles a kernel for the (expression, schema) pair
    // once and reuses it across every subsequent batch of the same shape. Force multiple batches
    // by lowering the Comet batch size with a row count well above it, then assert at least one
    // cache hit happened during the query.
    //
    // We deliberately do not assert cross-query cache reuse: Spark's analyzer produces a fresh
    // `ScalaUDF` instance per query resolution, and the encoders embedded in that instance
    // contain `AttributeReference`s with fresh `ExprId`s that our `BindReferences.bindReference`
    // does not recurse into. The closure-serialized cache key bytes therefore drift across
    // queries even when the registered function and schema are identical, so each new query of a
    // ScalaUDF pays one compile up front and amortizes within itself. This is an acceptable
    // amortization story (a few tens of milliseconds per query), not a behavior we can or do
    // promise across queries.
    spark.udf.register("kernelCacheMarker", (s: String) => if (s == null) null else s + "_kc")
    val rows = (0 until 256).map(i => s"row_$i")
    CometScalaUDFCodegen.resetStats()
    withSQLConf(CometConf.COMET_BATCH_SIZE.key -> "32") {
      withSubjects(rows: _*) {
        checkSparkAnswerAndOperator(sql("SELECT kernelCacheMarker(s) FROM t"))
      }
    }
    val stats = CometScalaUDFCodegen.stats()
    assert(stats.compileCount >= 1, s"expected at least one compile during the query, got $stats")
    assert(
      stats.cacheHitCount >= 1,
      s"expected at least one cache hit across batches of the same query, got $stats")
  }

  test("per-partition kernel preserves Nondeterministic state across batches") {
    // Wrap `monotonically_increasing_id()` as the argument of a ScalaUDF so the whole tree
    // (including the stateful MonotonicallyIncreasingID child) routes through the dispatcher.
    // Per-partition kernel caching means the id counter advances across batches within a
    // partition; without it, every batch would restart at 0 and the UDF output would disagree
    // with Spark's. The UDF body is a trivial identity; we're testing state correctness of the
    // Nondeterministic child across batches, not the UDF logic.
    spark.udf.register("idPassthrough", (id: Long) => id)
    val rows = (0 until 4096).map(i => s"row_$i")
    withSubjects(rows: _*) {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(
          sql("SELECT s, idPassthrough(monotonically_increasing_id()) FROM t"))
      }
    }
  }

  test("per-task cache isolates UDF state across sequential task runs in one session") {
    // Regression guard for the cache-scoping invariant on CometUdfBridge: instances live for
    // exactly one Spark task and are dropped on task completion, so a stateful kernel sees a
    // fresh instance per task. Running the same `monotonically_increasing_id()`-carrying query
    // twice in one session must produce identical results each run. Under a cache that outlived
    // a task and got reused by the next one, the counter would continue from the previous run's
    // final value and the second run's IDs would diverge. Under a cache that was keyed by Tokio
    // worker thread rather than task attempt ID, worker reuse across tasks would cause the same
    // leak whenever the second task happened to be polled by the same worker.
    val rows = (0 until 2048).map(i => s"row_$i")
    withSubjects(rows: _*) {
      val q = "SELECT s, monotonically_increasing_id() AS mid FROM t"
      val first = sql(q).collect().map(r => (r.getString(0), r.getLong(1))).toSeq
      val second = sql(q).collect().map(r => (r.getString(0), r.getLong(1))).toSeq
      assert(
        first == second,
        s"per-task cache leaked state across runs: first=${first.take(5)} second=${second.take(5)}")
    }
  }

  /**
   * Scalar ScalaUDF smoke tests. These prove that user-registered UDFs route through the codegen
   * dispatcher rather than forcing a whole-plan Spark fallback. Spark's `ScalaUDF.doGenCode`
   * already emits compilable Java that calls the user function via `ctx.addReferenceObj`, so the
   * dispatcher's compile path picks it up for free. Tests that user-registered UDFs route through
   * the dispatcher rather than forcing whole-plan Spark fallback.
   */

  test("registered string ScalaUDF routes through dispatcher") {
    spark.udf.register("shout", (s: String) => if (s == null) null else s.toUpperCase + "!")
    withSubjects("Abc", "xyz", null, "mixed") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT shout(s) FROM t"))
      }
    }
  }

  test("registered Java UDF1 routes through dispatcher") {
    // Java API path: `spark.udf.register(name, UDF1<...>, returnType)`. Spark wraps the Java
    // functional interface in a Scala function and produces a `ScalaUDF` expression at plan
    // time, so the dispatcher handles it the same as a Scala-registered UDF. Sanity check that
    // both registration paths land on the same routing code.
    spark.udf.register(
      "javaLen",
      new UDF1[String, Integer] {
        override def call(s: String): Integer = if (s == null) -1 else s.length
      },
      IntegerType)
    withSubjects("abc", "hello", null, "x") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT javaLen(s) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[VarCharVector]), IntegerType)
    }
  }

  test("multi-arg ScalaUDF over string + literal routes through dispatcher") {
    spark.udf.register(
      "prepend",
      (prefix: String, s: String) => if (s == null) null else prefix + s)
    withSubjects("one", "two", null) {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT prepend('[', s) FROM t"))
      }
    }
  }

  test("ScalaUDF as a child of a native Spark expression") {
    // The ScalaUDF routes through the dispatcher as a sub-expression; the surrounding `length`
    // runs through Comet's native scalar function path. This exercises the cross-boundary
    // composition where a dispatcher-compiled kernel returns a UTF8String that a native Comet
    // expression then consumes.
    spark.udf.register("wrap", (s: String) => if (s == null) null else s"|$s|")
    withSubjects("abc", "def", null) {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT length(wrap(s)) FROM t"))
      }
    }
  }

  test("composed ScalaUDFs outer(inner(s)) fuse into one kernel") {
    // Two user UDFs stacked, both operating on String. The dispatcher binds the whole tree and
    // Spark's codegen emits two `ctx.addReferenceObj` calls inside one generated method. Races
    // on the `ExpressionEncoder` serializers in `references` would show up here since each UDF
    // contributes its own stateful serializer; the `freshReferences` closure in `CompiledKernel`
    // is what keeps this correct across partitions.
    spark.udf.register("inner", (s: String) => if (s == null) null else s.toUpperCase)
    spark.udf.register("outer", (s: String) => if (s == null) null else s"<$s>")
    withSubjects("abc", null, "xyz", "MiXeD") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT outer(inner(s)) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[VarCharVector]), StringType)
    }
  }

  test("ScalaUDFs of different types compose: isShort(len(s))") {
    // Exercises an input type transition: String -> Int -> Boolean. Two user UDFs with
    // different I/O type shapes in one tree, one Janino compile.
    spark.udf.register("len", (s: String) => if (s == null) -1 else s.length)
    spark.udf.register("isShort", (i: Int) => i < 5)
    withSubjects("ab", "abcdef", null, "hi") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT isShort(len(s)) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[VarCharVector]), BooleanType)
    }
  }

  test("three-deep ScalaUDF composition lvl3(lvl2(lvl1(s)))") {
    // Three user UDFs stacked in one tree: String -> String -> String -> Int. The fused kernel
    // carries three `ctx.addReferenceObj` calls. `assertOneKernelForSubtree` asserts that the
    // whole chain collapses into a single compile rather than one per nesting level.
    // Input rows intentionally exclude nulls: per-batch nullability is a cache-key dimension
    // (`nullable()` reads `getNullCount != 0`), so a null-present batch compiles a second kernel
    // specialized for `nullable=true`. Null handling through composed UDFs is covered by the
    // other composition tests above.
    spark.udf.register("lvl1", (s: String) => if (s == null) null else s.toUpperCase)
    spark.udf.register("lvl2", (s: String) => if (s == null) null else s.reverse)
    spark.udf.register("lvl3", (s: String) => if (s == null) -1 else s.length)
    withSubjects("abc", "hello world", "x") {
      assertOneKernelForSubtree {
        checkSparkAnswerAndOperator(sql("SELECT lvl3(lvl2(lvl1(s))) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[VarCharVector]), IntegerType)
    }
  }

  test("multi-column ScalaUDF composition join(upperU(c1), lowerU(c2))") {
    // One multi-arg user UDF consuming two other user UDFs, each on a different input column.
    // The bound tree has two BoundReferences, and the kernel is specialized on two VarCharVector
    // columns. `assertOneKernelForSubtree` asserts that the two-branch composition fuses into a
    // single kernel rather than one per branch or one per UDF.
    // Input rows intentionally exclude nulls (see note on the three-deep test above).
    spark.udf.register("upperU", (s: String) => if (s == null) null else s.toUpperCase)
    spark.udf.register("lowerU", (s: String) => if (s == null) null else s.toLowerCase)
    spark.udf.register(
      "joinU",
      (a: String, b: String) => if (a == null || b == null) null else s"$a-$b")
    withTwoStringCols(("Abc", "XYZ"), ("Foo", "bar"), ("baz", "Bar"), ("Hi", "Lo")) {
      assertOneKernelForSubtree {
        checkSparkAnswerAndOperator(sql("SELECT joinU(upperU(c1), lowerU(c2)) FROM t"))
      }
      assertKernelSignaturePresent(
        Seq(classOf[VarCharVector], classOf[VarCharVector]),
        StringType)
    }
  }

  /**
   * Type-surface ScalaUDF tests. Each exercises a distinct Arrow input vector class plus the
   * matching output writer end to end.
   *
   * Backed by parquet tables with declared column types rather than `spark.range` projections:
   * derived `cast(id as int)` columns get folded into the plan and leave the `BoundReference` on
   * the underlying long, not the projected int. A declared parquet column keeps the Arrow vector
   * the dispatcher sees aligned with the UDF's signature.
   */
  private def withTypedCol(sqlType: String, valueLiterals: String*)(f: => Unit): Unit = {
    withTable("t") {
      sql(s"CREATE TABLE t (c $sqlType) USING parquet")
      if (valueLiterals.nonEmpty) {
        val rows = valueLiterals.map(v => s"($v)").mkString(", ")
        sql(s"INSERT INTO t VALUES $rows")
      }
      f
    }
  }

  test("ScalaUDF on IntegerType (IntVector, getInt)") {
    spark.udf.register("doubleIt", (i: Int) => i * 2)
    withTypedCol("INT", "1", "2", "100") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT doubleIt(c) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[IntVector]), IntegerType)
    }
  }

  test("ScalaUDF on LongType (BigIntVector, getLong)") {
    spark.udf.register("inc", (l: Long) => l + 1L)
    withTypedCol("BIGINT", "1", "2", "100") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT inc(c) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[BigIntVector]), LongType)
    }
  }

  test("ScalaUDF on DoubleType (Float8Vector, getDouble)") {
    spark.udf.register("halve", (d: Double) => d / 2.0)
    withTypedCol("DOUBLE", "1.5", "2.5", "100.0") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT halve(c) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[Float8Vector]), DoubleType)
    }
  }

  test("ScalaUDF on FloatType (Float4Vector, getFloat)") {
    spark.udf.register("scaleF", (f: Float) => f * 1.5f)
    withTypedCol("FLOAT", "CAST(1.5 AS FLOAT)", "CAST(2.5 AS FLOAT)") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT scaleF(c) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[Float4Vector]), FloatType)
    }
  }

  test("ScalaUDF on BooleanType (BitVector, getBoolean)") {
    spark.udf.register("neg", (b: Boolean) => !b)
    withTypedCol("BOOLEAN", "TRUE", "FALSE", "TRUE") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT neg(c) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[BitVector]), BooleanType)
    }
  }

  test("ScalaUDF on ShortType (SmallIntVector, getShort)") {
    spark.udf.register("incS", (s: Short) => (s + 1).toShort)
    withTypedCol(
      "SMALLINT",
      "CAST(1 AS SMALLINT)",
      "CAST(2 AS SMALLINT)",
      "CAST(30000 AS SMALLINT)") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT incS(c) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[SmallIntVector]), ShortType)
    }
  }

  test("ScalaUDF on ByteType (TinyIntVector, getByte)") {
    spark.udf.register("incB", (b: Byte) => (b + 1).toByte)
    withTypedCol("TINYINT", "CAST(1 AS TINYINT)", "CAST(2 AS TINYINT)", "CAST(100 AS TINYINT)") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT incB(c) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[TinyIntVector]), ByteType)
    }
  }

  test("ScalaUDF on DateType (DateDayVector, getInt)") {
    // Date input flows through the Int getter because DateType is physically int. The UDF takes
    // java.sql.Date and Spark's encoder handles the int -> Date materialization.
    spark.udf.register(
      "nextDay",
      (d: java.sql.Date) => if (d == null) null else new java.sql.Date(d.getTime + 86400000L))
    withTypedCol("DATE", "DATE'2024-01-01'", "DATE'2024-06-15'", "DATE'1970-01-01'") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT nextDay(c) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[DateDayVector]), DateType)
    }
  }

  test("ScalaUDF on TimestampType (TimeStampMicroTZVector, getLong)") {
    spark.udf.register(
      "plusSecond",
      (t: java.sql.Timestamp) =>
        if (t == null) null else new java.sql.Timestamp(t.getTime + 1000L))
    withTypedCol(
      "TIMESTAMP",
      "TIMESTAMP'2024-01-01 12:00:00'",
      "TIMESTAMP'2024-06-15 23:59:59'") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT plusSecond(c) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[TimeStampMicroTZVector]), TimestampType)
    }
  }

  test("ScalaUDF on TimestampNTZType (TimeStampMicroVector, getLong)") {
    spark.udf.register(
      "plusDayNtz",
      (ldt: java.time.LocalDateTime) => if (ldt == null) null else ldt.plusDays(1))
    withTypedCol(
      "TIMESTAMP_NTZ",
      "TIMESTAMP_NTZ'2024-01-01 12:00:00'",
      "TIMESTAMP_NTZ'2024-06-15 23:59:59'") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT plusDayNtz(c) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[TimeStampMicroVector]), TimestampNTZType)
    }
  }

  test("ScalaUDF returning DateType") {
    spark.udf.register("epochDay", (_: Int) => java.sql.Date.valueOf("1970-01-01"))
    withTypedCol("INT", "1", "2", "3") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT epochDay(c) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[IntVector]), DateType)
    }
  }

  test("ScalaUDF returning TimestampType") {
    spark.udf.register("mkTs", (s: Long) => new java.sql.Timestamp(s * 1000L))
    withTypedCol("BIGINT", "0", "1700000000", "1750000000") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT mkTs(c) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[BigIntVector]), TimestampType)
    }
  }

  test("ScalaUDF returning TimestampNTZType") {
    spark.udf.register(
      "mkTsNtz",
      (s: Long) => java.time.LocalDateTime.ofEpochSecond(s, 0, java.time.ZoneOffset.UTC))
    withTypedCol("BIGINT", "0", "1700000000", "1750000000") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT mkTsNtz(c) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[BigIntVector]), TimestampNTZType)
    }
  }

  test("ScalaUDF returning a different type than its input") {
    // String -> Int transition forces the output writer to switch from VarChar to Int. Exercises
    // the `IntegerType` output path end to end from a user UDF.
    spark.udf.register("codePoint", (s: String) => if (s == null) 0 else s.codePointAt(0))
    withSubjects("abc", "A", null, "!") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT codePoint(s) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[VarCharVector]), IntegerType)
    }
  }

  test("ScalaUDF returning BinaryType (VarBinaryVector output writer)") {
    // Binary output writer path, exercised here by a user UDF for the first time. Before this
    // the writer only had direct-compile unit tests.
    spark.udf.register("bytes", (s: String) => if (s == null) null else s.getBytes("UTF-8"))
    withSubjects("abc", null, "hello") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT bytes(s) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[VarCharVector]), BinaryType)
    }
  }

  test("ScalaUDF on BinaryType (VarBinaryVector, getBinary)") {
    // Binary input getter path: VarBinaryVector with byte[] reads via Spark's `getBinary` getter.
    spark.udf.register("blen", (b: Array[Byte]) => if (b == null) -1 else b.length)
    withTable("t") {
      sql("CREATE TABLE t (b BINARY) USING parquet")
      sql("INSERT INTO t VALUES (CAST('abc' AS BINARY)), (CAST('hello' AS BINARY)), (NULL)")
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT blen(b) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[VarBinaryVector]), IntegerType)
    }
  }

  test("ScalaUDF returning ArrayType(StringType) (ListVector output writer)") {
    // First use of the ArrayType output path end-to-end. The UDF returns a `Seq[String]`,
    // which Spark encodes as `ArrayType(StringType, containsNull = true)`. The dispatcher's
    // canHandle accepts it (ArrayType is supported when its element type is supported),
    // allocateOutput builds a ListVector with an inner VarCharVector, and emitWrite recurses
    // into the StringType case for the per-element UTF8 on-heap shortcut. End-to-end answer
    // matches Spark.
    spark.udf.register(
      "splitComma",
      (s: String) => if (s == null) null else s.split(",", -1).toSeq)
    withSubjects("a,b,c", "x", null, "", "one,,three") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT splitComma(s) FROM t"))
      }
    }
  }

  test("ScalaUDF returning ArrayType(IntegerType)") {
    // Exercises ArrayType output with a primitive element. emitWrite's ArrayType case
    // recurses into the IntegerType case for the inner write; no byte[] allocation involved.
    spark.udf.register(
      "asLengths",
      (s: String) => if (s == null) null else s.split(",").map(_.length).toSeq)
    withSubjects("a,bb,ccc", null, "xyzzy") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT asLengths(s) FROM t"))
      }
    }
  }

  test("zero-column ScalaUDF produces one row per input row") {
    // Non-deterministic (so Spark doesn't constant-fold) with a deterministic body (so
    // Spark-vs-Comet comparison stays honest). The expression has no `AttributeReference`,
    // so the serde produces an empty data-arg list and the dispatcher has no data column to
    // read the batch size from. Guards the `numRows` path through the JNI bridge.
    import org.apache.spark.sql.functions.udf
    val alwaysHello = udf(() => "hello").asNondeterministic()
    spark.udf.register("helloU", alwaysHello)
    withSubjects("a", "b", null, "c") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT helloU() FROM t"))
      }
    }
  }

  /**
   * Decimal tests. The dispatcher's `getDecimal` getter specializes on the `BoundReference`'s
   * `DecimalType.precision` at source-generation time: precision <= 18 emits an unscaled-long
   * fast path via `Decimal.createUnsafe`, precision > 18 emits a `BigDecimal + Decimal.apply`
   * slow path. These smoke tests exercise both sides of the split end to end and verify Spark and
   * Comet agree on correctness across typical decimal workloads.
   */
  private def withDecimalTable(decimalType: String, values: Seq[String])(f: => Unit): Unit = {
    withTable("t") {
      sql(s"CREATE TABLE t (d $decimalType) USING parquet")
      val rows = values.map(v => if (v == null) "(NULL)" else s"($v)").mkString(", ")
      if (values.nonEmpty) sql(s"INSERT INTO t VALUES $rows")
      f
    }
  }

  test("ScalaUDF over Decimal(9, 2) (short precision, fast path)") {
    // Short-precision identity UDF. The column's DecimalType has precision 9, so the generated
    // getter for ordinal 0 emits only the unscaled-long fast path. The UDF's Scala-side signature
    // uses `java.math.BigDecimal`, which Spark's encoder pins at DecimalType(38, 18); the implicit
    // Cast from DECIMAL(9, 2) -> DECIMAL(38, 18) runs inside Spark's generated code, not via our
    // kernel's getter, so the fast path still fires on the column read.
    spark.udf.register("decId9_2", (d: java.math.BigDecimal) => d)
    withDecimalTable("DECIMAL(9, 2)", Seq("0.00", "1.50", "-1.50", "9999.99", "-9999.99", null)) {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT decId9_2(d) FROM t"))
      }
    }
  }

  test("ScalaUDF over Decimal(18, 0) (max short precision, fast path)") {
    // Boundary precision: 18 is the last value for which the unscaled representation fits in a
    // signed 64-bit long. The fast path must still be selected.
    spark.udf.register("decId18_0", (d: java.math.BigDecimal) => d)
    withDecimalTable(
      "DECIMAL(18, 0)",
      Seq("0", "1", "-1", "999999999999999999", "-999999999999999999", null)) {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT decId18_0(d) FROM t"))
      }
    }
  }

  test("ScalaUDF over Decimal(18, 9) (max short precision with scale, fast path)") {
    // Same precision as above but with scale 9 to exercise the fractional side of the long
    // decimal. Spark `Decimal` stores both as the same unscaled long; only the `scale` parameter
    // differs.
    spark.udf.register("decId18_9", (d: java.math.BigDecimal) => d)
    withDecimalTable(
      "DECIMAL(18, 9)",
      Seq("0.000000000", "1.123456789", "-1.123456789", "999999999.999999999", null)) {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT decId18_9(d) FROM t"))
      }
    }
  }

  test("ScalaUDF over Decimal(19, 0) (just past short precision, slow path)") {
    // First precision where the unscaled value can exceed `Long.MAX_VALUE`. The generated getter
    // must emit only the slow path; the fast-path marker must be absent in the compiled kernel.
    spark.udf.register("decId19_0", (d: java.math.BigDecimal) => d)
    withDecimalTable(
      "DECIMAL(19, 0)",
      Seq("0", "1", "-1", "9999999999999999999", "-9999999999999999999", null)) {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT decId19_0(d) FROM t"))
      }
    }
  }

  test("ScalaUDF over Decimal(38, 10) (max precision, slow path)") {
    // Max decimal128 precision. Exercises the `getObject + Decimal.apply` branch and the
    // end-to-end BigDecimal conversion path with a non-trivial scale.
    spark.udf.register("decId38_10", (d: java.math.BigDecimal) => d)
    withDecimalTable(
      "DECIMAL(38, 10)",
      Seq(
        "0.0000000000",
        "1.1234567890",
        "-1.1234567890",
        "9999999999999999999999999999.0000000000",
        null)) {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT decId38_10(d) FROM t"))
      }
    }
  }

  test("ScalaUDF sees TaskContext.partitionId() per partition") {
    // Direct probe: register a ScalaUDF that reads TaskContext.partitionId() and returns it.
    // Spark's own task thread has TaskContext set, so each partition's rows carry that
    // partition's index. For the dispatcher to match Spark, the invocation thread must see a
    // live TaskContext. With the `createPlan`-time TaskContext capture + bridge-side
    // `TaskContext.setTaskContext` install (see `CometUdfBridge.evaluate` and
    // `CometTaskContextShim`), Tokio workers see the propagated TaskContext and the UDF
    // returns the real partitionId. Without that propagation, `TaskContext.get()` returns null
    // on the Tokio thread and the sentinel (-1) leaks through, diverging from Spark.
    spark.udf.register(
      "pid",
      (_: Long) => {
        val tc = TaskContext.get()
        if (tc != null) tc.partitionId() else -1
      })
    val df = spark
      .range(0, 1024, 1, numPartitions = 4)
      .selectExpr("id", "pid(id) as p")
    checkSparkAnswerAndOperator(df)
  }

  test("ScalaUDF sees TaskContext from fully-native parquet plan") {
    // The `spark.range`-based test above runs through `CometSparkRowToColumnar`, which executes
    // on a Spark task thread where TaskContext is live even without explicit propagation. The
    // fully-native path through `CometNativeScan` runs the JVM UDF bridge on a Tokio worker
    // thread where TaskContext.get() would otherwise be null. This test forces that path by
    // sourcing from a Parquet table written as multiple files (so the native read produces
    // multiple partitions) and asserting the UDF still sees the per-partition TaskContext via
    // the `createPlan`-time capture + bridge-side install.
    spark.udf.register(
      "pidP",
      (_: Int) => {
        val tc = TaskContext.get()
        if (tc != null) tc.partitionId() else -1
      })
    withTable("t") {
      sql("CREATE TABLE t (x INT) USING parquet")
      // Multiple INSERT statements -> multiple parquet files -> multiple read splits ->
      // multiple partitions.
      sql("INSERT INTO t VALUES (1), (2), (3), (4)")
      sql("INSERT INTO t VALUES (5), (6), (7), (8)")
      sql("INSERT INTO t VALUES (9), (10), (11), (12)")
      sql("INSERT INTO t VALUES (13), (14), (15), (16)")
      checkSparkAnswerAndOperator(sql("SELECT x, pidP(x) AS p FROM t"))
    }
  }

  test("Rand seeded per partition across a multi-partition table") {
    // Rand.doGenCode registers an XORShiftRandom via ctx.addMutableState and seeds it via
    // ctx.addPartitionInitializationStatement. That init statement runs inside our kernel's
    // `init(int partitionIndex)`, called once per kernel allocation. Spark seeds
    // `XORShiftRandom(seed + partitionIndex)` per partition, so different partitions produce
    // different sequences for the same seed. Matching Spark across partitions requires the
    // kernel to see the real partition index, which the dispatcher derives from
    // `TaskContext.get().partitionId()` — live on this path thanks to the bridge-level
    // TaskContext propagation. Composing with a ScalaUDF (identity on Double here) forces the
    // tree through codegen dispatch so the Rand evaluation runs inside our kernel's init
    // rather than via Spark's normal codegen.
    spark.udf.register("dblId", (d: Double) => d)
    val df = spark
      .range(0, 1024, 1, numPartitions = 4)
      .selectExpr("id", "dblId(rand(42)) as r")
    checkSparkAnswerAndOperator(df)
  }

  test("ScalaUDF composed with reused scalar subquery across projection and filter") {
    // The same scalar subquery appears in two sites: the projection (which the dispatcher
    // compiles into a fused kernel) and the filter (a separate operator). Each site holds its
    // own `ScalarSubquery` expression instance with its own `@volatile result` field. Each
    // surrounding operator's inherited `SparkPlan.waitForSubqueries` populates its instance's
    // `result` before the dispatcher's bridge serializes the expression. The populated value
    // travels through closure serialization into the cache key's bytes, so different subquery
    // values compile distinct kernels. Exercises the full subquery-correctness invariant
    // documented on `CometBatchKernelCodegen.canHandle`.
    spark.udf.register("addOne", (i: Int) => i + 1)
    withTable("t", "t2") {
      sql("CREATE TABLE t (x INT) USING parquet")
      sql("INSERT INTO t VALUES (1), (2), (3), (4), (5)")
      sql("CREATE TABLE t2 (v INT) USING parquet")
      sql("INSERT INTO t2 VALUES (2), (4)")
      checkSparkAnswerAndOperator(
        sql("SELECT addOne(x) + (SELECT max(v) FROM t2) AS r " +
          "FROM t WHERE addOne(x) < (SELECT max(v) FROM t2) * 2"))
    }
  }

  /**
   * ArrayType input. The dispatcher emits a nested `InputArray_col0` final class per array-typed
   * input column; Spark's generated `getArray(ord)` resolves to our kernel's switch which returns
   * the pre-allocated instance after resetting its start/length against the list's offsets.
   * Element reads go through the typed child-vector field with no `ArrayData` copy or boxing.
   *
   * Each smoke test exercises the same serde/transport path at a different element type so the
   * nested getter emitter's scalar-element cases are each covered: `StringType` (zero-copy
   * `UTF8String.fromAddress`), `IntegerType` (primitive direct), and `DecimalType(p <= 18)`
   * (decimal128 fast path).
   */
  private def withArrayTable(colType: String, insertRows: String)(f: => Unit): Unit = {
    withTable("t") {
      sql(s"CREATE TABLE t (a $colType) USING parquet")
      sql(s"INSERT INTO t VALUES $insertRows")
      f
    }
  }

  test("ScalaUDF taking Seq[String] reads through nested ArrayData class") {
    spark.udf.register(
      "headOrNull",
      (arr: Seq[String]) => if (arr == null || arr.isEmpty) null else arr.head)
    withArrayTable(
      "ARRAY<STRING>",
      "(array('a', 'b', 'c')), (array('x')), (null), (array()), (array('alone'))") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT headOrNull(a) FROM t"))
      }
    }
  }

  test("ScalaUDF taking Seq[String] iterating all elements") {
    spark.udf.register(
      "concatArr",
      (arr: Seq[String]) => if (arr == null) null else arr.mkString("|"))
    withArrayTable(
      "ARRAY<STRING>",
      "(array('one', 'two', 'three')), (array('solo')), (null), (array())") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT concatArr(a) FROM t"))
      }
    }
  }

  test("ScalaUDF taking Seq[Int] hits primitive element getter") {
    spark.udf.register("sumArr", (arr: Seq[Int]) => if (arr == null) -1 else arr.sum)
    withArrayTable(
      "ARRAY<INT>",
      "(array(1, 2, 3)), (array(-5, 5)), (array()), (null), (array(42))") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT sumArr(a) FROM t"))
      }
    }
  }

  test("ScalaUDF taking Seq[BigDecimal] hits short-precision decimal fast path") {
    // DecimalType(10, 2) is well inside p <= 18, so the nested-array `getDecimal` emits the
    // unscaled-long fast path (see `emitNestedArrayElementGetter`). A `BigDecimal` UDF argument
    // forces Spark's encoder to call `getDecimal(i, 10, 2)` on our nested ArrayData for each
    // element, which exercises that code path end to end.
    spark.udf.register(
      "sumDecArr",
      (arr: Seq[java.math.BigDecimal]) =>
        if (arr == null) null
        else {
          var acc = java.math.BigDecimal.ZERO
          arr.foreach(v => if (v != null) acc = acc.add(v))
          acc
        })
    withArrayTable(
      "ARRAY<DECIMAL(10, 2)>",
      "(array(1.23, 4.56)), (array(-9.99)), (null), (array())") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT sumDecArr(a) FROM t"))
      }
    }
  }

  // =============================================================================================
  // StructType + MapType + nested-composition smoke tests. Source tests prove the emitted Java
  // is well-shaped; these tests prove Janino compiles it and the runtime roundtrip matches
  // Spark.
  // =============================================================================================

  test("ScalaUDF composes with struct-field access reading Struct<String, Int>.age") {
    // Keeps the UDF arg scalar (Int) but puts a `GetStructField` under it so the codegen
    // dispatcher compiles the struct-input read path (`row.getStruct(0, 2).getInt(1)`).
    spark.udf.register("doubleInt", (i: Int) => i * 2)
    withTable("t") {
      sql("CREATE TABLE t (s STRUCT<name: STRING, age: INT>) USING parquet")
      sql(
        "INSERT INTO t VALUES " +
          "(named_struct('name', 'alice', 'age', 30)), " +
          "(named_struct('name', 'bob', 'age', 42)), " +
          "(null)")
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT doubleInt(s.age) FROM t"))
      }
    }
  }

  test("ScalaUDF taking full Struct<String, Int> value (case class arg)") {
    // Case-class UDF arguments: test data must not include null top-level rows.
    // `ScalaUDF.scalaConverter` applies Spark's `ExpressionEncoder.Deserializer` on every row
    // to materialize the case-class instance. The generated deserializer has a
    // `newInstance(NameAgePair)` step that throws `EXPRESSION_DECODING_FAILED` on a null input,
    // independent of the dispatcher. Case-class UDF tests omit null top-level rows; other
    // tests with plain `Seq` / `Map` args can include nulls because the deserializer hands null
    // to the UDF body which handles it.
    spark.udf.register("fmtPair", (r: NameAgePair) => s"${r.name}:${r.age}")
    withTable("t") {
      sql("CREATE TABLE t (s STRUCT<name: STRING, age: INT>) USING parquet")
      sql(
        "INSERT INTO t VALUES " +
          "(named_struct('name', 'alice', 'age', 30)), " +
          "(named_struct('name', 'bob', 'age', 42))")
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT fmtPair(s) FROM t"))
      }
    }
  }

  test("ScalaUDF returning Struct<String, Int> (case class output)") {
    spark.udf.register("makePair", (i: Int) => NameAgePair(s"n$i", i))
    withTypedCol("INT", "1", "2", "3") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT makePair(c) FROM t"))
      }
    }
  }

  test("ScalaUDF taking Map<String, Int>") {
    spark.udf.register("sumMap", (m: Map[String, Int]) => if (m == null) -1 else m.values.sum)
    withTable("t") {
      sql("CREATE TABLE t (m MAP<STRING, INT>) USING parquet")
      sql("INSERT INTO t VALUES (map('a', 1, 'b', 2)), (map()), (null)")
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT sumMap(m) FROM t"))
      }
    }
  }

  test("ScalaUDF round-trips Map<Int, Int> (primitive key and value)") {
    // Map with non-string keys: exercises the primitive-key element getter on the input side
    // and the corresponding writer on the output side. Spark's encoder for `Map[Int, Int]` calls
    // `getInt(0)` / `getInt(1)` on the entries struct, hitting the kernel's typed scalar getter
    // for each side rather than the UTF8 path.
    spark.udf.register(
      "incValues",
      (m: Map[Int, Int]) => if (m == null) null else m.map { case (k, v) => k -> (v + 1) })
    withTable("t") {
      sql("CREATE TABLE t (m MAP<INT, INT>) USING parquet")
      sql("INSERT INTO t VALUES (map(1, 10, 2, 20)), (map()), (null)")
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT incValues(m) FROM t"))
      }
    }
  }

  test("ScalaUDF returning Map<String, Int>") {
    spark.udf.register(
      "singletonMap",
      (s: String, i: Int) => if (s == null) null else Map(s -> i))
    withTable("t") {
      sql("CREATE TABLE t (s STRING, i INT) USING parquet")
      sql("INSERT INTO t VALUES ('a', 1), ('b', 2), (null, 3)")
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT singletonMap(s, i) FROM t"))
      }
    }
  }

  test("ScalaUDF taking Map<String, Seq<Int>> exercises nested composition") {
    spark.udf.register(
      "totalLens",
      (m: Map[String, Seq[Int]]) => if (m == null) -1 else m.values.flatten.sum)
    withTable("t") {
      sql("CREATE TABLE t (m MAP<STRING, ARRAY<INT>>) USING parquet")
      sql(
        "INSERT INTO t VALUES " +
          "(map('a', array(1, 2, 3), 'b', array(10))), " +
          "(map()), " +
          "(null)")
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT totalLens(m) FROM t"))
      }
    }
  }

  test("ScalaUDF round-trips Array<Array<Int>> (nested array input + output)") {
    // Exercises nested-array input reads and nested-list output writes in one call: the inner
    // `InputArray_col0_e` class on the input side and the recursive emitWrite on the output.
    spark.udf.register(
      "reverseRows",
      (arr: Seq[Seq[Int]]) => if (arr == null) null else arr.map(_.reverse))
    withTable("t") {
      sql("CREATE TABLE t (a ARRAY<ARRAY<INT>>) USING parquet")
      sql(
        "INSERT INTO t VALUES " +
          "(array(array(1, 2, 3), array(4, 5))), " +
          "(array(array())), " +
          "(null)")
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT reverseRows(a) FROM t"))
      }
    }
  }

  test("ScalaUDF round-trips Struct<name, items: Array<Int>>") {
    // Struct with a complex field on both sides: input reads go through InputStruct_col0 +
    // InputArray_col0_f1, output writes through StructVector + ListVector.
    // Null top-level rows omitted - case-class arg; see the note on `fmtPair` above.
    spark.udf.register(
      "growItems",
      (r: NameItems) =>
        if (r == null) null else NameItems(r.name, if (r.items == null) null else r.items :+ 0))
    withTable("t") {
      sql("CREATE TABLE t (s STRUCT<name: STRING, items: ARRAY<INT>>) USING parquet")
      sql(
        "INSERT INTO t VALUES " +
          "(named_struct('name', 'a', 'items', array(1, 2))), " +
          "(named_struct('name', 'b', 'items', array()))")
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT growItems(s) FROM t"))
      }
    }
  }

  test("ScalaUDF round-trips Map<String, Array<Int>> (nested value both sides)") {
    // Map input read goes through InputMap_col0 + InputArray_col0_v (the complex-value side);
    // output write emits MapVector + entries Struct + per-value ListVector inside the map's
    // entries struct.
    spark.udf.register(
      "sortValues",
      (m: Map[String, Seq[Int]]) =>
        if (m == null) null
        else m.map { case (k, v) => k -> (if (v == null) null else v.sorted) })
    withTable("t") {
      sql("CREATE TABLE t (m MAP<STRING, ARRAY<INT>>) USING parquet")
      sql(
        "INSERT INTO t VALUES " +
          "(map('a', array(3, 1, 2), 'b', array(10))), " +
          "(map()), " +
          "(null)")
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT sortValues(m) FROM t"))
      }
    }
  }

  test("ScalaUDF round-trips Map<String, Struct<x: Int, y: String>>") {
    // Struct value inside a map, both sides. Null top-level rows omitted - the map value is a
    // case class; see the note on `fmtPair` above.
    spark.udf.register(
      "tagValues",
      (m: Map[String, XyPair]) =>
        if (m == null) null
        else
          m.map { case (k, v) => k -> (if (v == null) null else XyPair(v.x + 1, s"<${v.y}>")) })
    withTable("t") {
      sql("CREATE TABLE t (m MAP<STRING, STRUCT<x: INT, y: STRING>>) USING parquet")
      sql(
        "INSERT INTO t VALUES " +
          "(map('a', named_struct('x', 1, 'y', 'one'))), " +
          "(map())")
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT tagValues(m) FROM t"))
      }
    }
  }

  // =============================================================================================
  // Regression tests pinning specific kernel bugs first surfaced in CometCodegenDispatchFuzzSuite.
  // Each is the smallest deterministic input that triggered the bug; kept post-fix as a guard
  // against future regression.
  // =============================================================================================

  test("array_distinct on Array<Struct<Int, String>> retains element identity across hash set") {
    // Fuzz signal: cardinality(array_distinct(arr_of_struct)) returns 1 where Spark returns 2.
    // Hypothesis: the kernel's InputStruct wrapper backing array_distinct's element reads is
    // reused without resetting per-element state, so every hashed element looks identical and
    // distinct collapses the array to a single entry.
    spark.udf.register("idIntDistinct", (i: Int) => i)
    withTable("t") {
      sql("CREATE TABLE t (s ARRAY<STRUCT<a: INT, b: STRING>>) USING parquet")
      sql(
        "INSERT INTO t VALUES " +
          "(array(named_struct('a', 1, 'b', 'x'), named_struct('a', 1, 'b', 'x'))), " +
          "(array(named_struct('a', 1, 'b', 'x'), named_struct('a', 2, 'b', 'y'))), " +
          "(array(named_struct('a', 1, 'b', 'x'), named_struct('a', 2, 'b', 'y'), " +
          "named_struct('a', 1, 'b', 'x')))")
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(
          sql("SELECT idIntDistinct(cardinality(array_distinct(s))) FROM t"))
      }
    }
  }

  test("array_max(flatten(arr)) on Array<Array<Binary>> with mixed null inner arrays") {
    // Fuzz signal: array_max(flatten(arr)) returns empty byte arrays where Spark returns the
    // actual max binary, with the empties sorting to the front of the output. Pattern points at
    // cross-batch state pollution. Generate 100 rows of varied outer/inner shape, longer
    // binaries, mixed nulls; force multiple batches with a small batch size.
    spark.udf.register("idBinFlat", (b: Array[Byte]) => b)
    withSQLConf(CometConf.COMET_BATCH_SIZE.key -> "16") {
      withTable("t") {
        sql("CREATE TABLE t (a ARRAY<ARRAY<BINARY>>) USING parquet")
        val rows = (0 until 100).map { i =>
          if (i % 11 == 0) {
            "(NULL)"
          } else {
            val outerSize = (i % 5) + 1
            val inners = (0 until outerSize).map { j =>
              val pick = (i * 7 + j) % 13
              if (pick == 0) "array()"
              else if (pick == 1) "NULL"
              else {
                val innerSize = ((i + j) % 4) + 1
                val bytes = (0 until innerSize).map { k =>
                  val len = ((i + j + k) % 8) + 1
                  val hex = (0 until len)
                    .map(b => f"${(i * 13 + j * 17 + k * 5 + b) & 0xff}%02x")
                    .mkString
                  s"X'$hex'"
                }
                "array(" + bytes.mkString(", ") + ")"
              }
            }
            s"(array(${inners.mkString(", ")}))"
          }
        }
        sql(s"INSERT INTO t VALUES ${rows.mkString(", ")}")
        assertCodegenDidWork {
          checkSparkAnswerAndOperator(sql("SELECT idBinFlat(array_max(flatten(a))) FROM t"))
        }
      }
    }
  }

  // =============================================================================================
  // Regression tests for nested reference-type getter null-handling. Spark's
  // `CodeGenerator.setArrayElement` (called from e.g. `Flatten.doGenCode`) only emits an
  // `isNullAt` check before `array.update(i, getX(j))` when the element is a Java primitive
  // (`int`/`long`/etc.). For reference-typed elements (Binary, String, Decimal, Struct, Array,
  // Map) it emits `array.update(i, getX(j))` unconditionally, relying on the source's getter to
  // return `null` for null positions itself (Spark's own `ColumnarArray.getBinary` does
  // `if (isNullAt(...)) return null;`). Our nested `InputArray_*.getX` getters do not honor that
  // contract, so any inner null at a reference-typed position becomes an empty-bytes / empty-
  // string / garbage-decimal / non-null-shell value in the flattened output. Each test below
  // pins one reference-type variant so the fix can be verified per type.
  // =============================================================================================

  test("array_max(flatten(arr)) on Array<Array<Binary>> with null inner Binary returns null") {
    spark.udf.register("idBin", (b: Array[Byte]) => b)
    withArrayTable(
      "ARRAY<ARRAY<BINARY>>",
      "(array(array(NULL))), " +
        "(array(array(NULL, NULL))), " +
        "(array(array(), array(NULL)))") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT idBin(array_max(flatten(a))) FROM t"))
      }
    }
  }

  test("array_max(flatten(arr)) on Array<Array<String>> with null inner String returns null") {
    spark.udf.register("idStr", (s: String) => s)
    withArrayTable(
      "ARRAY<ARRAY<STRING>>",
      "(array(array(NULL))), " +
        "(array(array(NULL, NULL))), " +
        "(array(array(), array(NULL)))") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT idStr(array_max(flatten(a))) FROM t"))
      }
    }
  }

  test(
    "array_max(flatten(arr)) on Array<Array<DECIMAL(10,2)>> with null inner Decimal " +
      "(short-precision fast path)") {
    spark.udf.register("idDec10", (d: java.math.BigDecimal) => d)
    withArrayTable(
      "ARRAY<ARRAY<DECIMAL(10, 2)>>",
      "(array(array(CAST(NULL AS DECIMAL(10, 2))))), " +
        "(array(array(" +
        "CAST(NULL AS DECIMAL(10, 2)), CAST(NULL AS DECIMAL(10, 2))))), " +
        "(array(array(), array(CAST(NULL AS DECIMAL(10, 2)))))") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT idDec10(array_max(flatten(a))) FROM t"))
      }
    }
  }

  test(
    "array_max(flatten(arr)) on Array<Array<DECIMAL(30,2)>> with null inner Decimal " +
      "(long-precision slow path)") {
    spark.udf.register("idDec30", (d: java.math.BigDecimal) => d)
    withArrayTable(
      "ARRAY<ARRAY<DECIMAL(30, 2)>>",
      "(array(array(CAST(NULL AS DECIMAL(30, 2))))), " +
        "(array(array(" +
        "CAST(NULL AS DECIMAL(30, 2)), CAST(NULL AS DECIMAL(30, 2))))), " +
        "(array(array(), array(CAST(NULL AS DECIMAL(30, 2)))))") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT idDec30(array_max(flatten(a))) FROM t"))
      }
    }
  }

  // Note: a runtime regression test for nullable nested `getStruct` / `getArray` / `getMap` would
  // need a
  // non-HOF expression that reads null elements after `flatten`. Spark's optimizer rules
  // (`SimplifyExtractValueOps` and friends) tend to rewrite the obvious candidates
  // (`element_at(flatten(arr), 1).x`, `flatten(arr)[i].x`) into shapes our dispatcher rejects
  // without a clean reason, and the only iteration paths over complex elements without
  // simplification go through HOFs (`array_filter`, `transform`) which our `canHandle` rejects
  // (TODO(hof-lambdas) on `CometBatchKernelCodegen`). Static coverage of the emitter for these
  // three getters lives in `CometCodegenSourceSuite` instead.
}

/**
 * Case class used by the struct-input / struct-output smoke tests. Must be declared at file scope
 * (not inside the test class) so Spark's TypeTag-based UDF encoder can resolve the Spark
 * `StructType` schema from the Scala class.
 */
private case class NameAgePair(name: String, age: Int)

private case class NameItems(name: String, items: Seq[Int])

private case class XyPair(x: Int, y: String)
