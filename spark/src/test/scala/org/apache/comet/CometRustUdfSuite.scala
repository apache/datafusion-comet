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

import java.io.File
import java.util.Locale

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.types._

import org.apache.comet.udf.CometRustUDF

/**
 * End-to-end integration suite: register a Rust UDF, run a Spark query, verify the result.
 *
 * Requires the `comet-test-udfs` cdylib, which is found automatically under `native/target` and
 * can be overridden with `-Dcomet.test.udfs.lib=<path>`.
 *
 * Note that these tests are self-guarding on native execution: `CometRustUDF.register` installs a
 * catalog stub that throws if Spark ever evaluates the UDF itself, so a silent fallback to Spark
 * fails the test rather than passing.
 *
 * To run locally:
 * {{{
 *   cargo build -p comet-test-udfs --manifest-path native/Cargo.toml
 *   ./mvnw test -Dsuites="org.apache.comet.CometRustUdfSuite" -Dtest=none
 * }}}
 */
class CometRustUdfSuite extends CometTestBase {

  private lazy val libPath: String = {
    val overridden = Option(System.getProperty("comet.test.udfs.lib"))
      // An undefined Maven property reaches the forked JVM as the literal "null", and an
      // unsubstituted one as "${comet.test.udfs.lib}". Neither is a path.
      .map(_.trim)
      .filter(p => p.nonEmpty && p != "null" && !p.startsWith("$"))

    overridden.orElse(CometRustUdfSuite.discoverBuiltLibrary()).getOrElse {
      if (sys.env.contains("CI")) {
        // In CI the cdylib is staged alongside libcomet, so its absence means the native build or
        // the artifact upload changed, not that someone forgot a flag. Fail rather than skip.
        fail(
          s"${CometRustUdfSuite.libraryFileName} was not found under native/target. CI stages it " +
            "next to libcomet, so a missing library means the native build or the artifact " +
            "upload has changed.")
      } else {
        cancel(s"${CometRustUdfSuite.libraryFileName} not built; run " +
          "`cargo build -p comet-test-udfs --manifest-path native/Cargo.toml` to run this suite")
      }
    }
  }

  test("add_one_c returns id + 1 for a range") {
    CometRustUDF.register(spark, "add_one_c", libPath, Seq(LongType), LongType)
    val df = spark.range(0, 5).selectExpr("add_one_c(id) AS y")
    val out = df.collect().map(_.getLong(0)).sorted.toSeq
    assert(out == Seq(1L, 2L, 3L, 4L, 5L))
  }

  test("panic inside UDF invoke surfaces as a query error, not a crash") {
    CometRustUDF.register(spark, "panics_on_invoke", libPath, Seq(LongType), LongType)
    val e = intercept[Exception] {
      spark.range(0, 5).selectExpr("panics_on_invoke(id) AS y").collect()
    }
    assert(
      stackTraceContains(e, "deliberate panic from user UDF code"),
      s"panic message not propagated: $e")
  }

  test("panic inside UDF return_field surfaces as a query error, not a crash") {
    CometRustUDF.register(spark, "panics_on_return_field", libPath, Seq(LongType), LongType)
    val e = intercept[Exception] {
      spark.range(0, 5).selectExpr("panics_on_return_field(id) AS y").collect()
    }
    assert(
      stackTraceContains(e, "deliberate panic from user return_field"),
      s"panic message not propagated: $e")
  }

  /** True if `needle` appears anywhere in the exception's cause chain. */
  private def stackTraceContains(e: Throwable, needle: String): Boolean = {
    Iterator
      .iterate(e)(_.getCause)
      .takeWhile(_ != null)
      .exists(t => Option(t.getMessage).exists(_.contains(needle)))
  }

  // ---------- type coverage ----------

  /**
   * One case per supported Spark type: the type itself, and a SQL expression producing a value of
   * that type from the `id` column of `spark.range`.
   *
   * The declared type is asserted against the frame's real schema before it is registered, so a
   * case whose expression does not produce the type it claims fails loudly rather than testing
   * the wrong thing.
   */
  private val typeCases: Seq[(DataType, String)] = Seq(
    (BooleanType, "id % 2 = 0"),
    (ByteType, "cast(id as byte)"),
    (ShortType, "cast(id as short)"),
    (IntegerType, "cast(id as int)"),
    (LongType, "id"),
    (FloatType, "cast(id as float) + 0.5f"),
    (DoubleType, "cast(id as double) + 0.5"),
    (StringType, "concat('s', cast(id as string))"),
    (BinaryType, "cast(concat('b', cast(id as string)) as binary)"),
    (DateType, "date_add(date'2024-01-01', cast(id as int))"),
    (TimestampType, "cast(date_add(date'2024-01-01', cast(id as int)) as timestamp)"),
    (TimestampNTZType, "cast(timestamp_ntz'2024-01-01 12:00:00' as timestamp_ntz)"),
    // The outer cast pins the result to decimal(10,2): Spark widens the precision of the addition
    // itself to decimal(11,2), which would not match the type registered below.
    (DecimalType(10, 2), "cast(cast(id as decimal(10,2)) + 0.25 as decimal(10,2))"),
    // Complex types. `containsNull` / `valueContainsNull` / field nullability are part of the type
    // and must match what the expression actually produces, hence the explicit constructors.
    (ArrayType(IntegerType, containsNull = false), "array(cast(id as int), cast(id + 1 as int))"),
    (ArrayType(StringType, containsNull = false), "array(concat('s', cast(id as string)))"),
    (
      MapType(StringType, IntegerType, valueContainsNull = false),
      "map('k', cast(id as int), 'j', cast(id + 1 as int))"),
    (
      StructType(
        Seq(
          StructField("a", IntegerType, nullable = false),
          StructField("b", StringType, nullable = false))),
      "named_struct('a', cast(id as int), 'b', concat('s', cast(id as string)))"),
    // One level of nesting, to check the FFI carries child arrays rather than just top-level ones.
    (
      ArrayType(
        StructType(Seq(StructField("a", IntegerType, nullable = false))),
        containsNull = false),
      "array(named_struct('a', cast(id as int)))"),
    (
      StructType(
        Seq(StructField("xs", ArrayType(IntegerType, containsNull = false), nullable = false))),
      "named_struct('xs', array(cast(id as int), cast(id + 1 as int)))"))

  /**
   * A 4-row frame with a single column `c` of the given type, where the last row is null so every
   * case also covers null handling across the FFI boundary.
   */
  private def typedFrame(valueExpr: String) =
    spark.range(0, 4).selectExpr(s"case when id = 3 then null else $valueExpr end as c")

  /** Normalize for comparison: byte arrays do not compare by value as `Any`. */
  private def normalize(v: Any): Any = v match {
    case b: Array[Byte] => b.toSeq
    case other => other
  }

  for ((dataType, valueExpr) <- typeCases) {
    test(s"echo_c round-trips ${dataType.simpleString} including nulls") {
      val df = typedFrame(valueExpr)
      assert(
        df.schema.head.dataType == dataType,
        s"test expression produced ${df.schema.head.dataType}, not $dataType")
      CometRustUDF.register(spark, "echo_c", libPath, Seq(dataType), dataType)
      val expected = df.collect().map(r => normalize(r.get(0))).toSeq
      val actual = df.selectExpr("echo_c(c) AS y").collect().map(r => normalize(r.get(0))).toSeq
      assert(actual == expected, s"round trip changed values for ${dataType.simpleString}")
      assert(expected.last == null, "expected a null in the last row")
    }

    test(s"stringify_c reads ${dataType.simpleString} values") {
      CometRustUDF.register(spark, "stringify_c", libPath, Seq(dataType), StringType)
      val df = typedFrame(valueExpr)
      val inputs = df.collect().map(r => normalize(r.get(0))).toSeq
      val rendered = df.selectExpr("stringify_c(c) AS y").collect().map(r => r.get(0)).toSeq

      assert(rendered.length == inputs.length)
      // The UDF must decode each value, so a non-null input yields a non-empty rendering and a
      // null input stays null. The exact text is arrow's formatting, not Spark's, so it is not
      // asserted here.
      inputs.zip(rendered).foreach { case (in, out) =>
        if (in == null) {
          assert(out == null, s"null input rendered as $out for ${dataType.simpleString}")
        } else {
          assert(out != null, s"non-null input $in rendered as null")
          assert(
            out.asInstanceOf[String].nonEmpty,
            s"non-null input $in rendered empty for ${dataType.simpleString}")
        }
      }
    }
  }

  test("one kernel computes its return type on demand and serves many types") {
    // echo_c has no fixed return type of its own: its return_field derives one from the argument
    // types on every call. The type declared to `register` is what Spark plans against, so it is
    // per-registration rather than per-kernel, and the same kernel serves a different type after
    // re-registering.
    CometRustUDF.register(spark, "echo_c", libPath, Seq(LongType), LongType)
    assert(
      spark.range(0, 3).selectExpr("echo_c(id) AS y").collect().map(_.getLong(0)).toSeq ==
        Seq(0L, 1L, 2L))

    CometRustUDF.register(spark, "echo_c", libPath, Seq(StringType), StringType)
    val strings = spark
      .range(0, 3)
      .selectExpr("echo_c(concat('s', cast(id as string))) AS y")
      .collect()
      .map(_.getString(0))
      .toSeq
    assert(strings == Seq("s0", "s1", "s2"))

    val arrayType = ArrayType(IntegerType, containsNull = false)
    CometRustUDF.register(spark, "echo_c", libPath, Seq(arrayType), arrayType)
    val arrays = spark
      .range(0, 2)
      .selectExpr("echo_c(array(cast(id as int), cast(id + 1 as int))) AS y")
      .collect()
      .map(_.getSeq[Int](0))
      .toSeq
    assert(arrays == Seq(Seq(0, 1), Seq(1, 2)))
  }

  test("a declared return type that disagrees with the UDF names both types") {
    // echo_c returns its argument's type, so declaring a different return type is a mismatch.
    CometRustUDF.register(spark, "echo_c", libPath, Seq(LongType), StringType)
    val e = intercept[Exception] {
      spark.range(0, 4).selectExpr("echo_c(id) AS y").collect()
    }
    assert(stackTraceContains(e, "was registered as returning"), s"unhelpful error: $e")
    assert(stackTraceContains(e, "CometRustUDF.register"), s"error lacks guidance: $e")
  }

  test("echo_c rejects a call whose argument count it does not accept") {
    CometRustUDF.register(spark, "echo_c", libPath, Seq(LongType), LongType)
    // The catalog stub is arity-1, so a 2-arg call is rejected during analysis.
    intercept[Exception] {
      spark.range(0, 2).selectExpr("echo_c(id, id) AS y").collect()
    }
  }

  test("registering a nondeterministic UDF is refused") {
    // Comet plans every Rust UDF as immutable, so accepting this would let the optimizer
    // constant-fold or CSE a call the caller told us was not safe to reuse. Refuse at
    // registration rather than silently ignore the flag.
    val e = intercept[IllegalArgumentException] {
      CometRustUDF.register(
        spark,
        "echo_c",
        libPath,
        Seq(LongType),
        LongType,
        deterministic = false)
    }
    assert(e.getMessage.contains("deterministic = false is not supported"), s"unclear: $e")

    // The check runs before any library work, so it fires on a path that does not exist
    // rather than reporting a load failure first.
    val early = intercept[IllegalArgumentException] {
      CometRustUDF.register(
        spark,
        "echo_c",
        "/no/such/library.so",
        Seq(LongType),
        LongType,
        deterministic = false)
    }
    assert(
      early.getMessage.contains("deterministic = false is not supported"),
      s"unclear: $early")
  }
}

object CometRustUdfSuite {

  /** Platform file name of the test cdylib built by the `comet-test-udfs` crate. */
  val libraryFileName: String =
    if (System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("mac")) {
      "libcomet_test_udfs.dylib"
    } else {
      "libcomet_test_udfs.so"
    }

  /**
   * Locate the test cdylib under `native/target`.
   *
   * The working directory differs between a reactor build and a single-module run, so walk up a
   * few levels looking for the `native/target` tree. `release` is where CI stages the downloaded
   * artifact, `ci` and `debug` cover local builds.
   */
  def discoverBuiltLibrary(): Option[String] = {
    val roots = Iterator
      .iterate(new File(".").getCanonicalFile)(_.getParentFile)
      .takeWhile(_ != null)
      .take(4)
    val candidates = for {
      root <- roots
      profile <- Seq("release", "ci", "debug")
    } yield new File(root, s"native/target/$profile/$libraryFileName")
    candidates.find(_.isFile).map(_.getAbsolutePath)
  }
}
