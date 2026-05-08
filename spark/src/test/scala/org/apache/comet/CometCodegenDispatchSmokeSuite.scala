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

import org.apache.arrow.vector.{BigIntVector, BitVector, Float4Vector, Float8Vector, IntVector, SmallIntVector, TinyIntVector, ValueVector, VarCharVector}
import org.apache.spark.SparkConf
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType}

import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus
import org.apache.comet.udf.CometCodegenDispatchUDF

/**
 * Smoke tests for the Arrow-direct codegen dispatcher. Runs rlike and regexp_replace queries and
 * asserts results match Spark. Widens to more expression shapes as the productionization plan
 * lands supporting types and plan-time dispatchability.
 */
class CometCodegenDispatchSmokeSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set(CometConf.COMET_REGEXP_ENGINE.key, CometConf.REGEXP_ENGINE_JAVA)
      // `auto` would also route rlike/regexp_replace to codegen when engine=java, but `force`
      // guarantees it and exercises the codegen path regardless of future auto-mode tuning.
      .set(CometConf.COMET_CODEGEN_DISPATCH_MODE.key, CometConf.CODEGEN_DISPATCH_FORCE)

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

  test("codegen: rlike projection with null handling") {
    withSubjects("abc123", "no digits", null, "mixed_42_data") {
      checkSparkAnswerAndOperator(sql("SELECT s, s rlike '\\\\d+' AS m FROM t"))
    }
  }

  test("codegen: rlike predicate") {
    withSubjects("abc123", "no digits", null, "mixed_42_data") {
      checkSparkAnswerAndOperator(sql("SELECT s FROM t WHERE s rlike '\\\\d+'"))
    }
  }

  test("codegen: rlike with backreference (Java-only)") {
    withSubjects("aa", "ab", "xyzzy", null) {
      checkSparkAnswerAndOperator(sql("SELECT s, s rlike '^(\\\\w)\\\\1$' FROM t"))
    }
  }

  test("codegen: rlike on all-null column") {
    withSubjects(null, null, null) {
      checkSparkAnswerAndOperator(sql("SELECT s rlike '\\\\d+' FROM t"))
    }
  }

  test("codegen: rlike empty pattern matches every non-null row") {
    withSubjects("a", "", null, "bc") {
      checkSparkAnswerAndOperator(sql("SELECT s, s rlike '' FROM t"))
    }
  }

  test("codegen: regexp_replace digits with a token") {
    withSubjects("abc123", "no digits", null, "mixed_42_data") {
      checkSparkAnswerAndOperator(sql("SELECT s, regexp_replace(s, '\\\\d+', 'N') FROM t"))
    }
  }

  test("codegen: regexp_replace with empty replacement") {
    withSubjects("abc123def", "no digits", null, "") {
      checkSparkAnswerAndOperator(sql("SELECT s, regexp_replace(s, '\\\\d+', '') FROM t"))
    }
  }

  test("codegen: regexp_replace no-match preserves input") {
    withSubjects("abc", "xyz", null) {
      checkSparkAnswerAndOperator(sql("SELECT s, regexp_replace(s, '\\\\d+', 'N') FROM t"))
    }
  }

  /**
   * Composition smoke tests. Demonstrate that the codegen dispatcher handles nested expression
   * trees in one compile per (tree, schema) pair, not one JNI hop per sub-expression. Each test
   * wraps the query in `assertCodegenDidWork` to prove the codegen path ran rather than silently
   * falling back to Spark.
   */
  private def assertCodegenDidWork(f: => Unit): Unit = {
    CometCodegenDispatchUDF.resetStats()
    f
    val after = CometCodegenDispatchUDF.stats()
    assert(
      after.compileCount + after.cacheHitCount >= 1,
      s"expected codegen dispatcher activity, got $after")
  }

  /**
   * Stronger form of [[assertCodegenDidWork]] for composition tests. Asserts that the full
   * expression subtree compiled into at most one kernel. The "one JNI crossing per nesting level"
   * alternative (the PR description's foil) would produce one `(bytes, specs)` cache entry per
   * nested sub-expression, so `compileCount` would be N and the cache would grow by N after the
   * first batch. Asserting `compileCount <= 1` and `cacheSize` growth `<= 1` directly falsifies
   * that shape.
   *
   * Uses `<=` rather than `==` because the compile cache is JVM-wide and shared across tests; a
   * prior test that already compiled the same `(expression bytes, input schema)` pair will make
   * this run a cache hit (`compileCount == 0`). The dispatcher-activity check guards against a
   * silent fallback where the query runs through Spark and the first two assertions pass
   * vacuously.
   */
  private def assertOneKernelForSubtree(f: => Unit): Unit = {
    CometCodegenDispatchUDF.resetStats()
    val sizeBefore = CometCodegenDispatchUDF.stats().cacheSize
    f
    val after = CometCodegenDispatchUDF.stats()
    assert(after.compileCount <= 1, s"expected <= 1 compile for the composed subtree, got $after")
    val grew = after.cacheSize - sizeBefore
    assert(grew <= 1, s"expected cache to grow by <= 1 entry, grew by $grew; stats=$after")
    assert(
      after.compileCount + after.cacheHitCount >= 1,
      s"expected codegen dispatcher activity, got $after")
  }

  /**
   * Assert that the dispatcher's compile cache contains a kernel compiled for the given input
   * Arrow vector classes (in ordinal order) and output Spark `DataType`. This is a specialization
   * check: the dispatcher is supposed to bake the concrete Arrow vector class into the generated
   * kernel, and the cache key reflects that. If a future change accidentally loses that
   * discrimination, `checkSparkAnswerAndOperator` would still pass (Spark computes the right
   * answer) but this assertion would fail.
   *
   * Asserts presence in the cache, not newness. The cache is JVM-wide and shared across tests; if
   * a prior test already compiled the same signature, that still counts. Combined with
   * `assertCodegenDidWork` (which proves the dispatcher ran in this test), the pair gives both
   * "this test exercised the dispatcher" and "the dispatcher's cache has a kernel of the expected
   * shape".
   *
   * Compares by simple name because the `common` module shades `org.apache.arrow`, so a direct
   * class-identity check against `classOf[VarCharVector]` at this call site (unshaded) misses the
   * shaded classes the dispatcher actually uses internally.
   */
  private def assertKernelSignaturePresent(
      inputs: Seq[Class[_ <: ValueVector]],
      output: DataType): Unit = {
    val sigs = CometCodegenDispatchUDF.snapshotCompiledSignatures()
    val expectedNames = inputs.map(_.getSimpleName).toIndexedSeq
    val present = sigs.exists { case (cached, dt) =>
      dt == output && cached.map(_.getSimpleName) == expectedNames
    }
    assert(
      present,
      s"expected kernel signature $expectedNames -> $output; " +
        s"cache had ${sigs.map { case (c, d) => (c.map(_.getSimpleName), d) }}")
  }

  test("codegen: compose upper(s) rlike pattern") {
    // The serde binds the whole tree, including the Upper, and ships it to the codegen
    // dispatcher. Inside the kernel, Upper.doGenCode emits `this.getUTF8String(0).toUpperCase()`
    // which feeds directly into the Matcher check. No second JNI hop for Upper.
    withSubjects("Abc123", "NO DIGITS", null, "mixed_42") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT s, upper(s) rlike '[A-Z0-9]+' FROM t"))
      }
    }
  }

  test("codegen: compose regexp_replace(upper(s), pattern, replacement)") {
    // Upper as the subject of RegExpReplace defeats the specialized emitter (its fast path
    // requires a direct BoundReference subject). Falls to the default path, which still compiles
    // cleanly as one fused method because Spark's doGenCode for Upper -> RegExpReplace is
    // self-contained.
    withSubjects("Abc123", "no digits", null, "Mix42") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(
          sql("SELECT s, regexp_replace(upper(s), '[0-9]+', '#') FROM t"))
      }
    }
  }

  test("codegen: compose upper(regexp_replace(s, pattern, replacement))") {
    // Flip the nesting: RegExpReplace is inside, Upper is outside. Still one compile per
    // (tree, schema) pair; the outer Upper's doGenCode consumes the RegExpReplace result as a
    // UTF8String in the same generated method. Case conversion is enabled because the inputs
    // are ASCII-only (the conf guards against locale-specific divergence, which does not apply
    // here).
    withSQLConf(CometConf.COMET_CASE_CONVERSION_ENABLED.key -> "true") {
      withSubjects("Abc123", "no digits", null, "Mix42") {
        assertCodegenDidWork {
          checkSparkAnswerAndOperator(
            sql("SELECT s, upper(regexp_replace(s, '[0-9]+', 'n')) FROM t"))
        }
      }
    }
  }

  test("codegen: compose substring(upper(s), 1, 3)") {
    // Three levels: BoundReference, Upper, Substring. Substring takes two literal ints; its
    // subject is the Upper result. Exercises multiple intermediate UTF8String operations in the
    // generated fused method.
    withSQLConf(CometConf.COMET_CASE_CONVERSION_ENABLED.key -> "true") {
      withSubjects("abcdef", null, "X", "hello world") {
        assertCodegenDidWork {
          checkSparkAnswerAndOperator(
            sql("SELECT s, substring(upper(s), 1, 3) rlike '^[A-Z]+$' FROM t"))
        }
      }
    }
  }

  test("codegen: regexp_extract (StringType output) routes through dispatcher") {
    // regexp_extract has no native path in Comet, so the mode knob decides codegen vs
    // hand-coded. Under the suite's `force` default, codegen runs.
    withSubjects("abc123", "no digits", null, "mix42data") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(
          sql("SELECT s, regexp_extract(s, '([a-z]+)([0-9]+)', 2) FROM t"))
      }
    }
  }

  test("codegen: regexp_instr (IntegerType output) routes through dispatcher") {
    // regexp_instr exercises the IntegerType output writer end to end for the first time since
    // Phase 2b added the allocator/writer; no prior end-to-end serde produced int output.
    withSubjects("abc123", "no digits", null, "mix42data") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT s, regexp_instr(s, '[0-9]+', 0) FROM t"))
      }
    }
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

  test("codegen: concat(c1, c2) rlike 'pat' compiles over two columns") {
    // Concat is not NullIntolerant. The dispatcher's short-circuit guard should skip the
    // whole-tree short-circuit and let Spark's Concat codegen handle nulls correctly.
    withTwoStringCols(("abc", "123"), ("abc", null), (null, "123"), (null, null), ("zz", "zz")) {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT concat(c1, c2) rlike '[a-z]+[0-9]+' FROM t"))
      }
    }
  }

  test("codegen: concat(upper(c1), c2) rlike 'pat' nests Upper inside Concat") {
    // Upper is NullIntolerant; Concat is not. The tree still has a non-NullIntolerant node so
    // the short-circuit must not apply. Exercises mixed-trait composition.
    withSQLConf(CometConf.COMET_CASE_CONVERSION_ENABLED.key -> "true") {
      withTwoStringCols(("abc", "123"), ("abc", null), (null, "zz"), (null, null)) {
        assertCodegenDidWork {
          checkSparkAnswerAndOperator(sql("SELECT concat(upper(c1), c2) rlike '[A-Z]+' FROM t"))
        }
      }
    }
  }

  test("codegen: regexp_replace(c1, literal, c2-ignored-literal) two columns in tree") {
    // Verifies that a second column reference outside the subject (here as a literal
    // replacement) still routes through. Note: regexp_replace requires literal regex and
    // replacement, so this is the only realistic two-column shape for that serde.
    withTwoStringCols(("abc123", "Z"), ("xyz", null), (null, "foo")) {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(
          sql("SELECT regexp_replace(concat(c1, c2), '[0-9]+', 'N') FROM t"))
      }
    }
  }

  test("codegen: disabled mode bypasses the dispatcher") {
    // In `disabled`, the rlike serde must skip codegen entirely and route through the hand-coded
    // JVM UDF path. The dispatcher's counters should not move.
    val pattern = "disabled_mode_marker_[0-9]+"
    CometCodegenDispatchUDF.resetStats()
    withSQLConf(
      CometConf.COMET_CODEGEN_DISPATCH_MODE.key -> CometConf.CODEGEN_DISPATCH_DISABLED) {
      withSubjects("disabled_mode_marker_1", null) {
        checkSparkAnswerAndOperator(sql(s"SELECT s rlike '$pattern' FROM t"))
      }
    }
    val after = CometCodegenDispatchUDF.stats()
    assert(
      after.compileCount == 0 && after.cacheHitCount == 0,
      s"expected no dispatcher activity under disabled mode, got $after")
  }

  test("codegen: auto mode prefers dispatcher when regex engine is java") {
    // `auto` with engine=java should resolve to codegen (the serde's documented preference). Use
    // a pattern unique to this test to guarantee a fresh compile.
    val pattern = "auto_mode_marker_[0-9]+"
    CometCodegenDispatchUDF.resetStats()
    withSQLConf(
      CometConf.COMET_CODEGEN_DISPATCH_MODE.key -> CometConf.CODEGEN_DISPATCH_AUTO,
      CometConf.COMET_REGEXP_ENGINE.key -> CometConf.REGEXP_ENGINE_JAVA) {
      withSubjects("auto_mode_marker_7", null) {
        checkSparkAnswerAndOperator(sql(s"SELECT s rlike '$pattern' FROM t"))
      }
    }
    val after = CometCodegenDispatchUDF.stats()
    assert(
      after.compileCount + after.cacheHitCount >= 1,
      s"expected dispatcher activity under auto mode with java engine, got $after")
  }

  test(
    "codegen: per-batch nullability produces distinct compiles for null-present vs null-absent") {
    // Same expression + same Arrow vector class + different observed nullability should hit
    // different cache keys, because `ArrowColumnSpec.nullable` flips when the batch has no
    // nulls. We don't assert on per-run deltas because Spark's partitioning can split the
    // subject table so the first query alone sees both nullability variants across different
    // partitions. Instead, assert the total invariant: across both queries we see at least two
    // compiles, proving the cache key discriminated on nullability.
    val pattern = "nullability_marker_[0-9]+"
    CometCodegenDispatchUDF.resetStats()

    withSubjects("nullability_marker_1", null, "nullability_marker_2") {
      checkSparkAnswerAndOperator(sql(s"SELECT s rlike '$pattern' FROM t"))
    }
    withSubjects("nullability_marker_3", "nullability_marker_4") {
      checkSparkAnswerAndOperator(sql(s"SELECT s rlike '$pattern' FROM t"))
    }
    val after = CometCodegenDispatchUDF.stats()

    assert(
      after.compileCount >= 2,
      "expected at least two compiles across both nullability distributions (one per " +
        s"nullable=true/false variant); got $after")
  }

  test("codegen: dispatcher stats increment on compile and hit") {
    // Use a pattern no other test in this suite compiles, so the first run is guaranteed to be a
    // cache miss regardless of test order.
    val pattern = "stats_only_marker_[0-9]+"
    CometCodegenDispatchUDF.resetStats()
    withSubjects("stats_only_marker_42", "nope", null) {
      checkSparkAnswerAndOperator(sql(s"SELECT s rlike '$pattern' FROM t"))
    }
    val firstRun = CometCodegenDispatchUDF.stats()
    assert(
      firstRun.compileCount >= 1,
      s"expected compile count >= 1 after first query, got $firstRun")
    assert(firstRun.cacheSize >= 1, s"expected cache size >= 1 after first query, got $firstRun")

    // Re-run the same expression against the same schema; should reuse the compiled kernel.
    val compileBefore = firstRun.compileCount
    withSubjects("stats_only_marker_9", null) {
      checkSparkAnswerAndOperator(sql(s"SELECT s rlike '$pattern' FROM t"))
    }
    val secondRun = CometCodegenDispatchUDF.stats()
    assert(
      secondRun.cacheHitCount >= 1,
      s"expected cache hits >= 1 after second query, got $secondRun")
    assert(
      secondRun.compileCount == compileBefore,
      s"expected no additional compile on second query, got $secondRun vs $firstRun")
  }

  /**
   * Collation smoke test. Spark 4.x associates a collation id with each `StringType` instance.
   * The codegen dispatcher's argument for handling collation is "Spark's own `doGenCode` for
   * regex-on-string uses `CollationFactory` / `CollationSupport`, so we inherit the right
   * semantics by reusing it". This test proves that end to end for the most common shape: `rlike`
   * on a UTF8_LCASE-cast subject. The collation lives on the expression (`COLLATE` cast in SQL)
   * rather than the column, so the parquet scan reads a default-collation column and stays
   * native; only the Project carries the collated regex evaluation.
   *
   * Limits worth knowing about (separate work, not codegen-dispatch issues):
   *   - `regexp_replace` with a collated subject: Spark's analyzer wraps the regex literal in
   *     `Collate(Literal, ...)`. Our `RegExpReplace` serde's `getSupportLevel` requires a bare
   *     `Literal` for the pattern, so it rejects before the dispatcher is invoked. Widening the
   *     serde to unwrap `Collate(Literal, ...)` would unblock this; it's a serde-side change, not
   *     a codegen-side gap.
   *   - `rlike` on an ICU collation (UNICODE_CI etc.): Spark itself rejects with a type mismatch
   *     ("requires STRING, got STRING COLLATE UNICODE_CI"). RLike contracts on UTF8_BINARY
   *     semantics; binary collations like UTF8_LCASE work, ICU ones don't.
   */
  test("codegen: rlike on UTF8_LCASE-cast column matches case-insensitively") {
    assume(isSpark40Plus, "non-default collations require Spark 4.0+")
    withSubjects("Abc", "abc", "ABC", "xyz", null) {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT s, (s COLLATE UTF8_LCASE) rlike 'abc' FROM t"))
      }
    }
  }

  test("codegen: per-partition kernel preserves Nondeterministic state across batches") {
    // Compose `monotonically_increasing_id()` with rlike so the dispatcher routes the
    // composed tree (the inner expression by itself wouldn't have a serde). The expression
    // also references `s` so the proto carries at least one data column, giving the bridge a
    // row count signal. Per-partition kernel caching means the id counter advances across
    // batches in one partition; without it, every batch would restart at 0 and we'd disagree
    // with Spark on the right side of the rlike. The rlike pattern is permissive on purpose;
    // we're testing state correctness, not regex matching.
    val rows = (0 until 4096).map(i => s"row_$i")
    withSubjects(rows: _*) {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(
          sql("SELECT concat(s, cast(monotonically_increasing_id() as string)) rlike " +
            "'^row_[0-9]+[0-9]+$' FROM t"))
      }
    }
  }

  /**
   * Scalar ScalaUDF smoke tests. These prove that user-registered UDFs route through the codegen
   * dispatcher rather than forcing a whole-plan Spark fallback. Spark's `ScalaUDF.doGenCode`
   * already emits compilable Java that calls the user function via `ctx.addReferenceObj`, so the
   * dispatcher's compile path picks it up for free. Validates the "biggest single unlock" claim
   * for the dispatcher approach.
   */

  test("codegen: registered string ScalaUDF routes through dispatcher") {
    spark.udf.register("shout", (s: String) => if (s == null) null else s.toUpperCase + "!")
    withSubjects("Abc", "xyz", null, "mixed") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT shout(s) FROM t"))
      }
    }
  }

  test("codegen: multi-arg ScalaUDF over string + literal routes through dispatcher") {
    spark.udf.register(
      "prepend",
      (prefix: String, s: String) => if (s == null) null else prefix + s)
    withSubjects("one", "two", null) {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT prepend('[', s) FROM t"))
      }
    }
  }

  test("codegen: ScalaUDF composed with an rlike subject") {
    // Outer rlike binds the whole tree, including the ScalaUDF inside its subject. One
    // compiled kernel handles rlike + user-code + Arrow reads in a single fused method.
    spark.udf.register("wrap", (s: String) => if (s == null) null else s"|$s|")
    withSubjects("abc", "def", null) {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT wrap(s) rlike '^\\\\|[a-z]+\\\\|$' FROM t"))
      }
    }
  }

  test("codegen: composed ScalaUDFs outer(inner(s)) fuse into one kernel") {
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

  test("codegen: ScalaUDFs of different types compose: isShort(len(s))") {
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

  test("codegen: three-deep ScalaUDF composition lvl3(lvl2(lvl1(s)))") {
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

  test("codegen: multi-column ScalaUDF composition join(upperU(c1), lowerU(c2))") {
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
   * matching output writer through the full SQL -> serde -> dispatcher -> Janino -> kernel
   * pipeline. Before ScalaUDF routing, non-string types were covered only by the direct-compile
   * suite (since the regex serdes all produce string or boolean output).
   *
   * Backed by parquet tables with declared column types rather than derived-from-range views:
   * when the source column is a derived projection (e.g. `cast(id as int)` from `spark.range`),
   * the optimizer folds the cast into the outer plan and the ScalaUDF's `BoundReference` ends up
   * on the underlying long, not the projected int. A declared parquet column type keeps the
   * `AttributeReference` on the expected type and the Arrow vector the dispatcher sees matches
   * the UDF's signature.
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

  test("codegen: ScalaUDF on IntegerType (IntVector, getInt)") {
    spark.udf.register("doubleIt", (i: Int) => i * 2)
    withTypedCol("INT", "1", "2", "100") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT doubleIt(c) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[IntVector]), IntegerType)
    }
  }

  test("codegen: ScalaUDF on LongType (BigIntVector, getLong)") {
    spark.udf.register("inc", (l: Long) => l + 1L)
    withTypedCol("BIGINT", "1", "2", "100") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT inc(c) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[BigIntVector]), LongType)
    }
  }

  test("codegen: ScalaUDF on DoubleType (Float8Vector, getDouble)") {
    spark.udf.register("halve", (d: Double) => d / 2.0)
    withTypedCol("DOUBLE", "1.5", "2.5", "100.0") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT halve(c) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[Float8Vector]), DoubleType)
    }
  }

  test("codegen: ScalaUDF on FloatType (Float4Vector, getFloat)") {
    spark.udf.register("scaleF", (f: Float) => f * 1.5f)
    withTypedCol("FLOAT", "CAST(1.5 AS FLOAT)", "CAST(2.5 AS FLOAT)") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT scaleF(c) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[Float4Vector]), FloatType)
    }
  }

  test("codegen: ScalaUDF on BooleanType (BitVector, getBoolean)") {
    spark.udf.register("neg", (b: Boolean) => !b)
    withTypedCol("BOOLEAN", "TRUE", "FALSE", "TRUE") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT neg(c) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[BitVector]), BooleanType)
    }
  }

  test("codegen: ScalaUDF on ShortType (SmallIntVector, getShort)") {
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

  test("codegen: ScalaUDF on ByteType (TinyIntVector, getByte)") {
    spark.udf.register("incB", (b: Byte) => (b + 1).toByte)
    withTypedCol("TINYINT", "CAST(1 AS TINYINT)", "CAST(2 AS TINYINT)", "CAST(100 AS TINYINT)") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT incB(c) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[TinyIntVector]), ByteType)
    }
  }

  test("codegen: ScalaUDF returning a different type than its input") {
    // String -> Int transition forces the output writer to switch from VarChar to Int. Exercises
    // the `IntegerType` output path end to end from a user UDF (previously only regexp_instr
    // covered it).
    spark.udf.register("codePoint", (s: String) => if (s == null) 0 else s.codePointAt(0))
    withSubjects("abc", "A", null, "!") {
      assertCodegenDidWork {
        checkSparkAnswerAndOperator(sql("SELECT codePoint(s) FROM t"))
      }
      assertKernelSignaturePresent(Seq(classOf[VarCharVector]), IntegerType)
    }
  }

  test("codegen: ScalaUDF returning BinaryType (VarBinaryVector output writer)") {
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

  test("codegen: zero-column ScalaUDF produces one row per input row") {
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
}
