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

import scala.util.Random

import org.apache.arrow.vector._
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.catalyst.expressions.{CreateArray, CreateMap, CreateNamedStruct, Expression, Literal, MapConcat}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.udf.codegen.CometScalaUDFCodegen

/**
 * End-to-end correctness for the Arrow-direct codegen dispatcher. Covers the scalar and complex
 * type surface, composed UDF trees, subquery reuse, `TaskContext` propagation, per-task cache
 * isolation, the `maxFields` plan-time gate, and regressions pinned from fuzz.
 *
 * Tests exercising fallback paths (config disabled, `maxFields` exceeded) use `checkSparkAnswer`
 * rather than `checkSparkAnswerAndOperator` because ScalaUDF has no Comet-native path. Under
 * fallback the project runs on the JVM Spark path.
 */
class CometCodegenSuite
    extends CometTestBase
    with AdaptiveSparkPlanHelper
    with CometCodegenAssertions {

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
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT tag(concat(c1, c2)) FROM t"))
      }
    }
  }

  test("disabled mode bypasses the dispatcher") {
    // When the per-feature config is off, `CometScalaUDF.convert` returns None and the enclosing
    // operator falls back to Spark. The dispatcher's counters must not move.
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

  test("schema exceeding spark.sql.codegen.maxFields falls back to Spark") {
    // `CometBatchKernelCodegen.canHandle` mirrors WSCG's `spark.sql.codegen.maxFields` gate by
    // counting nested input fields plus the output field and refusing once the total exceeds the
    // configured cap. Comet has no mid-execution fallback, so the gate must fire at plan time
    // (in the serde) rather than letting an oversized kernel reach Janino. With 5 input
    // BoundReferences and a 1-field output we have 6 fields total. Setting `maxFields=3` ensures
    // the gate fires here regardless of test ordering or future schema additions.
    spark.udf.register(
      "sumFiveInts",
      (a: Int, b: Int, c: Int, d: Int, e: Int) => a + b + c + d + e)
    withTable("t") {
      sql("CREATE TABLE t (a INT, b INT, c INT, d INT, e INT) USING parquet")
      sql("INSERT INTO t VALUES (1, 2, 3, 4, 5), (10, 20, 30, 40, 50)")
      CometScalaUDFCodegen.resetStats()
      withSQLConf("spark.sql.codegen.maxFields" -> "3") {
        checkSparkAnswer(sql("SELECT sumFiveInts(a, b, c, d, e) FROM t"))
      }
      val after = CometScalaUDFCodegen.stats()
      assert(
        after.compileCount == 0 && after.cacheHitCount == 0,
        s"expected dispatcher fallback under maxFields=3, got $after")
    }
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
    // partition. Without it, every batch would restart at 0 and the UDF output would disagree
    // with Spark's. The UDF body is a trivial identity. We're testing state correctness of the
    // Nondeterministic child across batches, not the UDF logic.
    spark.udf.register("idPassthrough", (id: Long) => id)
    val rows = (0 until 4096).map(i => s"row_$i")
    withSubjects(rows: _*) {
      assertCodegenRan {
        checkSparkAnswerAndOperator(
          sql("SELECT s, idPassthrough(monotonically_increasing_id()) FROM t"))
      }
    }
  }

  test(
    "same UDF over nullable and non-nullable columns gets distinct kernels with independent state") {
    // Two columns, same type, different schema-declared nullability. Same UDF applied to each
    // alongside a per-projection MonotonicallyIncreasingID. Each projection has its own MII
    // child (different bytesKey), so each kernel must have its own counter advancing 0..N-1.
    // If the dispatcher collapses them onto one kernel or shares state somehow, the counters
    // would interleave and the output would diverge from Spark.
    spark.udf.register("withId", (s: String, id: Long) => s"${s}_${id}")
    withTempPath { dir =>
      import org.apache.spark.sql.Row
      import org.apache.spark.sql.types.{StringType, StructField, StructType}
      val schema = StructType(
        Seq(
          StructField("a", StringType, nullable = true),
          StructField("b", StringType, nullable = false)))
      val rows = (0 until 64).map(i => Row(s"a_$i", s"b_$i"))
      val rdd = spark.sparkContext.parallelize(rows, numSlices = 1)
      spark.createDataFrame(rdd, schema).write.parquet(dir.getCanonicalPath)
      withTable("t") {
        sql(s"CREATE TABLE t USING parquet LOCATION '${dir.getCanonicalPath}'")
        withSQLConf(CometConf.COMET_BATCH_SIZE.key -> "8") {
          assertCodegenRan {
            checkSparkAnswerAndOperator(
              sql("SELECT withId(a, monotonically_increasing_id()), " +
                "withId(b, monotonically_increasing_id()) FROM t"))
          }
        }
      }
    }
  }

  test("Nondeterministic state persists across nullability flips within a partition") {
    // Regression guard against re-introducing per-batch nullability into the cache key. Force a
    // single parquet file with `spark.range(numPartitions=1)`, large enough that batch size 8
    // produces many batches in one scan partition. Null density varies by row range. If the
    // dispatcher ever started deriving spec nullability from runtime data again, the cache key
    // would flip mid-partition, the kernel would be re-allocated, and MII's counter would reset
    // across the flip.
    spark.udf.register("idPair", (id: Long, s: String) => (id, s))
    withTempPath { dir =>
      spark
        .range(0, 200, 1, numPartitions = 1)
        .selectExpr("CASE WHEN id >= 16 AND id < 32 THEN NULL ELSE concat('row_', id) END AS s")
        .write
        .parquet(dir.getCanonicalPath)
      withTable("t") {
        sql(s"CREATE TABLE t USING parquet LOCATION '${dir.getCanonicalPath}'")
        withSQLConf(CometConf.COMET_BATCH_SIZE.key -> "8") {
          assertCodegenRan {
            checkSparkAnswerAndOperator(
              sql("SELECT idPair(monotonically_increasing_id(), s) FROM t"))
          }
        }
      }
    }
  }

  test("Nondeterministic state persists across two ScalaUDFs in one task") {
    // The dispatcher is one instance per task (keyed by `(taskAttemptId, udfClassName)` in
    // CometUdfBridge), so a plan with two distinct ScalaUDFs shares one CometScalaUDFCodegen.
    // Two distinct closure-serialized expressions hit two cache entries. Per batch the
    // dispatcher is invoked once for each. Each cache entry must stash its own kernel instance,
    // otherwise the two expressions would fight for a shared kernel slot and stateful state
    // (MII counter) would reset on every flip.
    //
    // Small batch size forces multiple batches over a small table so the per-key flip happens
    // several times within one task.
    spark.udf.register("idA", (id: Long) => id)
    spark.udf.register("idB", (id: Long) => -id)
    val rows = (0 until 64).map(i => s"row_$i")
    withSubjects(rows: _*) {
      withSQLConf(CometConf.COMET_BATCH_SIZE.key -> "8") {
        assertCodegenRan {
          checkSparkAnswerAndOperator(
            sql(
              "SELECT s, " +
                "idA(monotonically_increasing_id()) AS a, " +
                "idB(monotonically_increasing_id()) AS b FROM t"))
        }
      }
    }
  }

  test("per-task cache isolates UDF state across sequential task runs in one session") {
    // Regression guard for the cache-scoping invariant on CometUdfBridge: instances live for
    // exactly one Spark task and are dropped on task completion, so a stateful kernel sees a
    // fresh instance per task. The query has to actually route through the dispatcher for this
    // to test anything, so wrap `monotonically_increasing_id()` in a ScalaUDF identity. Running
    // it twice in one session must produce results matching Spark each time. Under a cache that
    // outlived a task and got reused by the next one, the counter would continue from the
    // previous run's final value and the second run's IDs would diverge from Spark. Under a
    // cache that was keyed by Tokio worker thread rather than task attempt ID, worker reuse
    // across tasks would cause the same leak whenever the second task happened to be polled by
    // the same worker. Two `checkSparkAnswerAndOperator` calls are stronger than asserting
    // first == second: equality alone could pass if both runs are wrong-but-consistent (e.g.
    // `init(partitionIndex)` never fires); matching Spark on both runs rules that out and
    // implies cross-run equality because Spark is deterministic on the same query.
    spark.udf.register("idPassthrough", (id: Long) => id)
    val rows = (0 until 2048).map(i => s"row_$i")
    withSubjects(rows: _*) {
      val q = "SELECT s, idPassthrough(monotonically_increasing_id()) AS mid FROM t"
      checkSparkAnswerAndOperator(sql(q))
      checkSparkAnswerAndOperator(sql(q))
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
      assertCodegenRan {
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
      assertCodegenRan {
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
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT prepend('[', s) FROM t"))
      }
    }
  }

  test("ScalaUDF as a child of a native Spark expression") {
    // The ScalaUDF routes through the dispatcher as a sub-expression. The surrounding `length`
    // runs through Comet's native scalar function path. This exercises the cross-boundary
    // composition where a dispatcher-compiled kernel returns a UTF8String that a native Comet
    // expression then consumes.
    spark.udf.register("wrap", (s: String) => if (s == null) null else s"|$s|")
    withSubjects("abc", "def", null) {
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT length(wrap(s)) FROM t"))
      }
    }
  }

  test("composed ScalaUDFs outer(inner(s)) fuse into one kernel") {
    // Two user UDFs stacked, both operating on String. The dispatcher binds the whole tree and
    // Spark's codegen emits two `ctx.addReferenceObj` calls inside one generated method. Races
    // on the `ExpressionEncoder` serializers in `references` would show up here since each UDF
    // contributes its own stateful serializer. The `freshReferences` closure in `CompiledKernel`
    // is what keeps this correct across partitions.
    spark.udf.register("inner", (s: String) => if (s == null) null else s.toUpperCase)
    spark.udf.register("outer", (s: String) => if (s == null) null else s"<$s>")
    withSubjects("abc", null, "xyz", "MiXeD") {
      assertCodegenRan {
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
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT isShort(len(s)) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[VarCharVector]), BooleanType)
    }
  }

  test("three-deep ScalaUDF composition lvl3(lvl2(lvl1(s)))") {
    // Three user UDFs stacked in one tree: String -> String -> String -> Int. The fused kernel
    // carries three `ctx.addReferenceObj` calls. `assertOneKernelForSubtree` asserts that the
    // whole chain collapses into a single compile rather than one per nesting level.
    // Null handling through composed UDFs is covered by the other composition tests above.
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
   * Per-primitive identity-UDF coverage. Each entry registers a `T => T` UDF over a parquet
   * column declared at `sqlType` and asserts the dispatcher compiled a kernel for the matching
   * `(vector class, output type)` pair. Parquet-backed (rather than `spark.range`-cast) tables
   * keep the column's Arrow vector class aligned with the UDF signature.
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

  private case class IdentityUdfCase(
      label: String,
      sqlType: String,
      values: Seq[String],
      vec: Class[_ <: ValueVector],
      output: DataType,
      udfName: String,
      register: () => Unit)

  private val identityScalarCases: Seq[IdentityUdfCase] = Seq(
    IdentityUdfCase(
      "Boolean",
      "BOOLEAN",
      Seq("TRUE", "FALSE", "TRUE"),
      classOf[BitVector],
      BooleanType,
      "u_bool",
      () => spark.udf.register("u_bool", (b: Boolean) => !b)),
    IdentityUdfCase(
      "Byte",
      "TINYINT",
      Seq("CAST(1 AS TINYINT)", "CAST(2 AS TINYINT)", "CAST(100 AS TINYINT)"),
      classOf[TinyIntVector],
      ByteType,
      "u_byte",
      () => spark.udf.register("u_byte", (b: Byte) => (b + 1).toByte)),
    IdentityUdfCase(
      "Short",
      "SMALLINT",
      Seq("CAST(1 AS SMALLINT)", "CAST(2 AS SMALLINT)", "CAST(30000 AS SMALLINT)"),
      classOf[SmallIntVector],
      ShortType,
      "u_short",
      () => spark.udf.register("u_short", (s: Short) => (s + 1).toShort)),
    IdentityUdfCase(
      "Int",
      "INT",
      Seq("1", "2", "100"),
      classOf[IntVector],
      IntegerType,
      "u_int",
      () => spark.udf.register("u_int", (i: Int) => i * 2)),
    IdentityUdfCase(
      "Long",
      "BIGINT",
      Seq("1", "2", "100"),
      classOf[BigIntVector],
      LongType,
      "u_long",
      () => spark.udf.register("u_long", (l: Long) => l + 1L)),
    IdentityUdfCase(
      "Float",
      "FLOAT",
      Seq("CAST(1.5 AS FLOAT)", "CAST(2.5 AS FLOAT)"),
      classOf[Float4Vector],
      FloatType,
      "u_float",
      () => spark.udf.register("u_float", (f: Float) => f * 1.5f)),
    IdentityUdfCase(
      "Double",
      "DOUBLE",
      Seq("1.5", "2.5", "100.0"),
      classOf[Float8Vector],
      DoubleType,
      "u_double",
      () => spark.udf.register("u_double", (d: Double) => d / 2.0)),
    IdentityUdfCase(
      "Date",
      "DATE",
      Seq("DATE'2024-01-01'", "DATE'2024-06-15'", "DATE'1970-01-01'"),
      classOf[DateDayVector],
      DateType,
      "u_date",
      () =>
        spark.udf.register(
          "u_date",
          (d: java.sql.Date) =>
            if (d == null) null else new java.sql.Date(d.getTime + 86400000L))),
    IdentityUdfCase(
      "Timestamp",
      "TIMESTAMP",
      Seq("TIMESTAMP'2024-01-01 12:00:00'", "TIMESTAMP'2024-06-15 23:59:59'"),
      classOf[TimeStampMicroTZVector],
      TimestampType,
      "u_ts",
      () =>
        spark.udf.register(
          "u_ts",
          (t: java.sql.Timestamp) =>
            if (t == null) null else new java.sql.Timestamp(t.getTime + 1000L))),
    IdentityUdfCase(
      "TimestampNTZ",
      "TIMESTAMP_NTZ",
      Seq("TIMESTAMP_NTZ'2024-01-01 12:00:00'", "TIMESTAMP_NTZ'2024-06-15 23:59:59'"),
      classOf[TimeStampMicroVector],
      TimestampNTZType,
      "u_tsntz",
      () =>
        spark.udf.register(
          "u_tsntz",
          (ldt: java.time.LocalDateTime) => if (ldt == null) null else ldt.plusDays(1))))

  identityScalarCases.foreach { c =>
    test(s"identity ScalaUDF on ${c.label} routes through dispatcher") {
      c.register()
      withTypedCol(c.sqlType, c.values: _*) {
        assertCodegenRan {
          checkSparkAnswerAndOperator(sql(s"SELECT ${c.udfName}(c) FROM t"))
        }
        assertKernelSignaturePresent(Seq(c.vec), c.output)
      }
    }
  }

  test("ScalaUDF returning a different type than its input") {
    // String -> Int output transition. Identity-loop above keeps input == output. This asserts
    // the writer can switch types per the UDF's declared return.
    spark.udf.register("codePoint", (s: String) => if (s == null) 0 else s.codePointAt(0))
    withSubjects("abc", "A", null, "!") {
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT codePoint(s) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[VarCharVector]), IntegerType)
    }
  }

  test("ScalaUDF returning BinaryType") {
    // Binary output writer path, exercised here by a user UDF for the first time. Before this
    // the writer only had direct-compile unit tests.
    spark.udf.register("bytes", (s: String) => if (s == null) null else s.getBytes("UTF-8"))
    withSubjects("abc", null, "hello") {
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT bytes(s) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[VarCharVector]), BinaryType)
    }
  }

  test("ScalaUDF on BinaryType") {
    // Binary input getter path: VarBinaryVector with byte[] reads via Spark's `getBinary` getter.
    spark.udf.register("blen", (b: Array[Byte]) => if (b == null) -1 else b.length)
    withTable("t") {
      sql("CREATE TABLE t (b BINARY) USING parquet")
      sql("INSERT INTO t VALUES (CAST('abc' AS BINARY)), (CAST('hello' AS BINARY)), (NULL)")
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT blen(b) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[VarBinaryVector]), IntegerType)
    }
  }

  test("ScalaUDF returning ArrayType(StringType)") {
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
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT splitComma(s) FROM t"))
      }
    }
  }

  test("ScalaUDF returning ArrayType(IntegerType)") {
    // Exercises ArrayType output with a primitive element. emitWrite's ArrayType case
    // recurses into the IntegerType case for the inner write. No byte[] allocation involved.
    spark.udf.register(
      "asLengths",
      (s: String) => if (s == null) null else s.split(",").map(_.length).toSeq)
    withSubjects("a,bb,ccc", null, "xyzzy") {
      assertCodegenRan {
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
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT helloU() FROM t"))
      }
    }
  }

  /**
   * Decimal end-to-end: the dispatcher's `getDecimal` specializes per `DecimalType.precision` at
   * source-generation time. Two representative cases here; `CometCodegenFuzzSuite` sweeps every
   * shape across the boundary at varying null densities.
   */
  private def withDecimalTable(decimalType: String, values: Seq[String])(f: => Unit): Unit = {
    withTable("t") {
      sql(s"CREATE TABLE t (d $decimalType) USING parquet")
      val rows = values.map(v => if (v == null) "(NULL)" else s"($v)").mkString(", ")
      if (values.nonEmpty) sql(s"INSERT INTO t VALUES $rows")
      f
    }
  }

  test("ScalaUDF over Decimal(18, 9) routes through the unscaled-long fast path") {
    // Boundary precision (18 == `MAX_LONG_DIGITS`) with a non-zero scale exercises the fractional
    // branch of the fast-path encoding.
    spark.udf.register("decIdShort", (d: java.math.BigDecimal) => d)
    withDecimalTable(
      "DECIMAL(18, 9)",
      Seq("0.000000000", "1.123456789", "-1.123456789", "999999999.999999999", null)) {
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT decIdShort(d) FROM t"))
      }
    }
  }

  test("ScalaUDF over Decimal(38, 10) routes through the BigDecimal slow path") {
    // Pin the return type to Decimal(38, 10). TypeTag inference for `BigDecimal` would default to
    // Decimal(38, 18), and under Spark 4 ANSI the encoder's CheckOverflow throws on the 28-digit
    // boundary value below when rescaling 10 -> 18.
    spark.udf.register(
      "decIdLong",
      new UDF1[java.math.BigDecimal, java.math.BigDecimal] {
        override def call(d: java.math.BigDecimal): java.math.BigDecimal = d
      },
      DecimalType(38, 10))
    withDecimalTable(
      "DECIMAL(38, 10)",
      Seq(
        "0.0000000000",
        "1.1234567890",
        "-1.1234567890",
        "9999999999999999999999999999.0000000000",
        null)) {
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT decIdLong(d) FROM t"))
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
    // `TaskContext.get().partitionId()`, live on this path thanks to the bridge-level
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

  test("ScalaUDF taking Seq[String] reads element by element") {
    spark.udf.register(
      "headOrNull",
      (arr: Seq[String]) => if (arr == null || arr.isEmpty) null else arr.head)
    withArrayTable(
      "ARRAY<STRING>",
      "(array('a', 'b', 'c')), (array('x')), (null), (array()), (array('alone'))") {
      assertCodegenRan {
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
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT concatArr(a) FROM t"))
      }
    }
  }

  test("ScalaUDF taking Seq[Int] reads primitive elements") {
    spark.udf.register("sumArr", (arr: Seq[Int]) => if (arr == null) -1 else arr.sum)
    withArrayTable(
      "ARRAY<INT>",
      "(array(1, 2, 3)), (array(-5, 5)), (array()), (null), (array(42))") {
      assertCodegenRan {
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
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT sumDecArr(a) FROM t"))
      }
    }
  }

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
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT doubleInt(s.age) FROM t"))
      }
    }
  }

  test("ScalaUDF taking full Struct<String, Int> value (case class arg)") {
    // Case-class UDF arguments: test data must not include null top-level rows.
    // `ScalaUDF.scalaConverter` applies Spark's `ExpressionEncoder.Deserializer` on every row
    // to materialize the case-class instance. The generated deserializer has a
    // `newInstance(NameAgePair)` step that throws `EXPRESSION_DECODING_FAILED` on a null input,
    // independent of the dispatcher. Case-class UDF tests omit null top-level rows. Other
    // tests with plain `Seq` / `Map` args can include nulls because the deserializer hands null
    // to the UDF body which handles it.
    spark.udf.register("fmtPair", (r: NameAgePair) => s"${r.name}:${r.age}")
    withTable("t") {
      sql("CREATE TABLE t (s STRUCT<name: STRING, age: INT>) USING parquet")
      sql(
        "INSERT INTO t VALUES " +
          "(named_struct('name', 'alice', 'age', 30)), " +
          "(named_struct('name', 'bob', 'age', 42))")
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT fmtPair(s) FROM t"))
      }
    }
  }

  test("ScalaUDF returning Struct<String, Int> (case class output)") {
    spark.udf.register("makePair", (i: Int) => NameAgePair(s"n$i", i))
    withTypedCol("INT", "1", "2", "3") {
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT makePair(c) FROM t"))
      }
    }
  }

  test("ScalaUDF taking Map<String, Int>") {
    spark.udf.register("sumMap", (m: Map[String, Int]) => if (m == null) -1 else m.values.sum)
    withTable("t") {
      sql("CREATE TABLE t (m MAP<STRING, INT>) USING parquet")
      sql("INSERT INTO t VALUES (map('a', 1, 'b', 2)), (map()), (null)")
      assertCodegenRan {
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
      assertCodegenRan {
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
      assertCodegenRan {
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
      assertCodegenRan {
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
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT reverseRows(a) FROM t"))
      }
    }
  }

  test("ScalaUDF round-trips Struct<name, items: Array<Int>>") {
    // Struct with a complex field on both sides: input reads go through InputStruct_col0 +
    // InputArray_col0_f1, output writes through StructVector + ListVector.
    // Null top-level rows omitted - case-class arg. See the note on `fmtPair` above.
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
      assertCodegenRan {
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
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT sortValues(m) FROM t"))
      }
    }
  }

  test("ScalaUDF round-trips Map<String, Struct<x: Int, y: String>>") {
    // Struct value inside a map, both sides. Null top-level rows omitted - the map value is a
    // case class. See the note on `fmtPair` above.
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
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT tagValues(m) FROM t"))
      }
    }
  }

  private def kernelMapIntString(expr: Expression): Map[Int, String] =
    runKernel(expr, 1) { v =>
      val map = v.getMap(0)
      val keys = map.keyArray()
      val values = map.valueArray()
      (0 until map.numElements())
        .map(i => keys.getInt(i) -> values.getUTF8String(i).toString)
        .toMap
    }

  test("constant-folded map_concat output round-trips every key through the kernel (#4539)") {
    // map_concat(map(1,'a',2,'b'), map(3,'c')) is all-literal, so Spark's optimizer constant-folds
    // it to a Literal(MapType) holding an ArrayBasedMapData. The MapType output writer must marshal
    // every entry into the Arrow MapVector; the reported bug corrupts the last key (3 -> 0).
    def s(str: String): Literal = Literal(UTF8String.fromString(str), StringType)
    val map1 =
      CreateMap(Seq(Literal(1), s("a"), Literal(2), s("b")), useStringTypeWhenEmpty = false)
    val map2 = CreateMap(Seq(Literal(3), s("c")), useStringTypeWhenEmpty = false)
    val folded =
      Literal.create(MapConcat(Seq(map1, map2)).eval(null), MapType(IntegerType, StringType))
    assert(kernelMapIntString(folded) === Map(1 -> "a", 2 -> "b", 3 -> "c"))
  }

  test("constant-folded array output writes every element past the pre-sized child (#4539)") {
    // A single-row array with far more elements than the list child's numRows-derived initial
    // capacity. The element child is written at a cumulative index, so a bare `set` overflows the
    // pre-sized buffer once the row's element count exceeds it; `setSafe` grows it. Sibling of the
    // map_concat case for ArrayType.
    val n = 16
    val elems = (0 until n).map(i => Literal(i * 10, IntegerType))
    val folded =
      Literal.create(CreateArray(elems).eval(null), ArrayType(IntegerType, containsNull = false))

    val got = runKernel(folded, 1) { v =>
      val arr = v.getArray(0)
      (0 until arr.numElements()).map(arr.getInt)
    }
    assert(got === (0 until n).map(_ * 10))
  }

  test(
    "constant-folded Array<Struct<Int, String>> writes struct fields past the pre-sized child " +
      "(#4539)") {
    // The struct sits inside an array, so its fields inherit the array's cumulative index. The
    // fixed-width Int field would overflow with a bare `set`; propagating `nested` into the struct
    // branch makes it `setSafe`. Guards the struct-nested-in-collection path.
    val n = 16
    def structAt(i: Int): Expression =
      CreateNamedStruct(
        Seq(
          Literal("a"),
          Literal(i, IntegerType),
          Literal("b"),
          Literal(UTF8String.fromString(s"v$i"), StringType)))
    val structType = new StructType()
      .add("a", IntegerType, nullable = false)
      .add("b", StringType, nullable = false)
    val folded = Literal.create(
      CreateArray((0 until n).map(structAt)).eval(null),
      ArrayType(structType, containsNull = false))

    val got = runKernel(folded, 1) { v =>
      val arr = v.getArray(0)
      (0 until arr.numElements()).map { i =>
        val r = arr.getStruct(i, 2)
        r.getInt(0) -> r.getUTF8String(1).toString
      }
    }
    assert(got === (0 until n).map(i => i -> s"v$i"))
  }

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
      assertCodegenRan {
        checkSparkAnswerAndOperator(
          sql("SELECT idIntDistinct(cardinality(array_distinct(s))) FROM t"))
      }
    }
  }

  test("array_max(flatten(arr)) on Array<Array<Binary>> with mixed null inner arrays") {
    // Fuzz signal: array_max(flatten(arr)) returns empty byte arrays where Spark returns the
    // actual max binary, with the empties sorting to the front of the output. Pattern points at
    // cross-batch state pollution. Generate 100 rows of varied outer/inner shape, longer
    // binaries, mixed nulls. Force multiple batches with a small batch size.
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
        assertCodegenRan {
          checkSparkAnswerAndOperator(sql("SELECT idBinFlat(array_max(flatten(a))) FROM t"))
        }
      }
    }
  }

  /**
   * Regressions for nested reference-typed getter null handling. Spark's
   * `CodeGenerator.setArrayElement` only emits an `isNullAt` check before `array.update(i,
   * getX(j))` for Java primitives. For reference-typed elements (Binary, String, Decimal, Struct,
   * Array, Map) it relies on the source's `getX` to return `null` itself, matching
   * `ColumnarArray.getBinary`. Without that contract, inner nulls become empty bytes / empty
   * strings / garbage decimals / non-null shells in the flattened output.
   */

  test("array_max(flatten(arr)) on Array<Array<Binary>> with null inner Binary returns null") {
    spark.udf.register("idBin", (b: Array[Byte]) => b)
    withArrayTable(
      "ARRAY<ARRAY<BINARY>>",
      "(array(array(NULL))), " +
        "(array(array(NULL, NULL))), " +
        "(array(array(), array(NULL)))") {
      assertCodegenRan {
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
      assertCodegenRan {
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
      assertCodegenRan {
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
      assertCodegenRan {
        checkSparkAnswerAndOperator(sql("SELECT idDec30(array_max(flatten(a))) FROM t"))
      }
    }
  }

  // Runtime coverage for nullable nested `getStruct` / `getArray` / `getMap` element reads is
  // exercised through HOFs in `CometCodegenHOFSuite`. Static emitter assertions live in
  // `CometCodegenSourceSuite`.

  /**
   * Dynamically sized collection output (regression family for #4539). Each UDF takes a scalar
   * seed and returns a collection whose per-row size is a function of the seed, so the output
   * writer fills each collection's child vector at a cumulative index that `numRows` does not
   * bound. Before #4539 the fixed-width child writes used a bare `set`, which ran off the end of
   * the pre-sized buffer; with Comet's unsafe Arrow memory the overflow corrupted neighboring
   * entries (or, under NMT, aborted the JVM). Scalar input keeps the read side off the
   * complex-input deserializer, isolating coverage to the writer.
   *
   * A small batch size makes the child's `numRows`-derived pre-size tiny relative to the per-row
   * element counts, so the larger rows reliably push past it. Randomized type/shape coverage of
   * the same writer lives in `CometCodegenFuzzSuite`.
   */
  private val collectionOutputSeeds: Seq[String] = {
    val rng = new Random(42)
    (0 until 256).map { i =>
      if (i % 17 == 0) "NULL" // null result
      else if (i % 13 == 0) "0" // empty collection
      else (rng.nextInt(80) - 39).toString // mix of small and larger-than-batch sizes
    }
  }

  private def withSeedTable(f: => Unit): Unit = {
    withTable("t") {
      sql("CREATE TABLE t (seed INT) USING parquet")
      collectionOutputSeeds.grouped(64).foreach { batch =>
        sql(s"INSERT INTO t VALUES ${batch.map(s => s"($s)").mkString(", ")}")
      }
      f
    }
  }

  private case class CollectionOutputCase(label: String, register: () => String)

  private val collectionOutputCases: Seq[CollectionOutputCase] = Seq(
    // Fixed-width element with nulls: the exact nested write #4539 corrupted.
    CollectionOutputCase(
      "Array<Int> with null elements",
      () => {
        val n = "arrout_int"
        spark.udf.register(
          n,
          (i: java.lang.Integer) =>
            if (i == null) null
            else
              (0 until (math.abs(i.intValue) % 40)).map(j =>
                if (j % 4 == 0) null else java.lang.Integer.valueOf(i + j)))
        n
      }),
    CollectionOutputCase(
      "Array<Long>",
      () => {
        val n = "arrout_long"
        spark.udf.register(
          n,
          (i: java.lang.Integer) =>
            if (i == null) null
            else (0 until (math.abs(i.intValue) % 40)).map(j => (i.toLong + j) * 1000000000L))
        n
      }),
    CollectionOutputCase(
      "Array<String> with null elements",
      () => {
        val n = "arrout_str"
        spark.udf.register(
          n,
          (i: java.lang.Integer) =>
            if (i == null) null
            else
              (0 until (math.abs(i.intValue) % 40)).map(j =>
                if (j % 3 == 0) null else s"v${i}_$j"))
        n
      }),
    CollectionOutputCase(
      "Array<Decimal>",
      () => {
        val n = "arrout_dec"
        spark.udf.register(
          n,
          (i: java.lang.Integer) =>
            if (i == null) null
            else
              (0 until (math.abs(i.intValue) % 40)).map(j =>
                java.math.BigDecimal.valueOf((i + j).toLong)))
        n
      }),
    CollectionOutputCase(
      "Array<Binary>",
      () => {
        val n = "arrout_bin"
        spark.udf.register(
          n,
          (i: java.lang.Integer) =>
            if (i == null) null
            else
              (0 until (math.abs(i.intValue) % 40)).map(j =>
                if (j % 5 == 0) null else s"b${i}_$j".getBytes("UTF-8")))
        n
      }),
    CollectionOutputCase(
      "Map<Int, Int>",
      () => {
        val n = "mapout_ii"
        spark.udf.register(
          n,
          (i: java.lang.Integer) =>
            if (i == null) null
            else (0 until (math.abs(i.intValue) % 40)).map(j => j -> (i + j)).toMap)
        n
      }),
    CollectionOutputCase(
      "Map<String, Int>",
      () => {
        val n = "mapout_si"
        spark.udf.register(
          n,
          (i: java.lang.Integer) =>
            if (i == null) null
            else (0 until (math.abs(i.intValue) % 40)).map(j => s"k$j" -> (i + j)).toMap)
        n
      }),
    CollectionOutputCase(
      "Array<Array<Int>>",
      () => {
        val n = "arrout_arr"
        spark.udf.register(
          n,
          (i: java.lang.Integer) =>
            if (i == null) null
            else (0 until (math.abs(i.intValue) % 40)).map(j => (0 to j).map(_ + i)))
        n
      }),
    CollectionOutputCase(
      "Map<Int, Array<Int>>",
      () => {
        val n = "mapout_iarr"
        spark.udf.register(
          n,
          (i: java.lang.Integer) =>
            if (i == null) null
            else (0 until (math.abs(i.intValue) % 40)).map(j => j -> (0 to j).map(_ + i)).toMap)
        n
      }),
    CollectionOutputCase(
      "Array<Struct<Int, String>>",
      () => {
        val n = "arrout_struct"
        spark.udf.register(
          n,
          (i: java.lang.Integer) =>
            if (i == null) null
            else (0 until (math.abs(i.intValue) % 40)).map(j => IntStr(i + j, s"v$j")))
        n
      }))

  for (c <- collectionOutputCases) {
    test(s"dynamically-sized ${c.label} output round-trips through codegen dispatch (#4539)") {
      val udf = c.register()
      withSQLConf(CometConf.COMET_BATCH_SIZE.key -> "8") {
        withSeedTable {
          assertCodegenRan {
            checkSparkAnswerAndOperator(sql(s"SELECT $udf(seed) FROM t"))
          }
        }
      }
    }
  }
}

/**
 * Case class used by the struct-input / struct-output smoke tests. Must be declared at file scope
 * (not inside the test class) so Spark's TypeTag-based UDF encoder can resolve the Spark
 * `StructType` schema from the Scala class.
 */
private case class NameAgePair(name: String, age: Int)

private case class NameItems(name: String, items: Seq[Int])

private case class XyPair(x: Int, y: String)

/** Element type for the `Array<Struct<Int, String>>` dynamically-sized output case. */
private case class IntStr(a: Int, b: String)
