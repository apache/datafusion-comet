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

import java.net.URLClassLoader
import java.nio.file.Path

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{BigIntVector, IntVector, ValueVector}
import org.apache.arrow.vector.complex.ListVector
import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, CometTestBase, DataFrame}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types._

import org.apache.comet.udf.{CometJvmUDF, CometUDF}

/**
 * End-to-end coverage for vectorized JVM UDFs registered through [[CometJvmUDF]].
 *
 * The tests are self-guarding on Comet execution: Spark cannot evaluate a registered UDF, so a
 * fallback to Spark fails a test rather than passing it. For the same reason they compare with
 * literal expected values rather than with Spark's answer, which Spark cannot produce.
 */
class CometJvmUdfSuite extends CometTestBase {

  import CometJvmUdfSuite._

  override protected def sparkConf: SparkConf =
    super.sparkConf.set("spark.executor.extraClassPath", hiddenClassesDir.toString)

  private def registerAddOne(): Unit =
    CometJvmUDF.register(spark, "jvm_add_one", classOf[AddOneUdf], Seq(LongType), LongType)

  private def registerSubtract(): Unit =
    CometJvmUDF.register(
      spark,
      "jvm_subtract",
      classOf[SubtractUdf],
      Seq(LongType, LongType),
      LongType)

  /** The first column of every row. */
  private def column(df: DataFrame): Seq[Any] = df.collect().map(_.get(0)).toSeq

  private def registerRangeList(containsNull: Boolean): Unit =
    CometJvmUDF.register(
      spark,
      "jvm_range_list",
      classOf[RangeListUdf],
      Seq(LongType),
      ArrayType(LongType, containsNull))

  /** True if `needle` appears in the message of `e` or of any of its causes. */
  private def causeChainContains(e: Throwable, needle: String): Boolean =
    causeChain(e).exists(t => Option(t.getMessage).exists(_.contains(needle)))

  test("a vectorized UDF runs in the Comet pipeline") {
    registerAddOne()
    assert(column(spark.range(0, 5).selectExpr("jvm_add_one(id)")) == Seq(1L, 2L, 3L, 4L, 5L))
  }

  test("nulls pass through in both directions") {
    registerAddOne()
    val df = spark.range(0, 4).selectExpr("jvm_add_one(CASE WHEN id = 2 THEN NULL ELSE id END)")
    assert(column(df) == Seq(1L, 2L, null, 4L))
  }

  test("a two-argument UDF receives its arguments in order") {
    // jvm_subtract computes a - b, so swapped arguments would negate every result.
    registerSubtract()
    val df = spark.range(0, 4).selectExpr("jvm_subtract(id * 10, id)")
    assert(column(df) == Seq(0L, 9L, 18L, 27L))
  }

  test("literal arguments arrive as one-row vectors in any position") {
    registerAddOne()
    registerSubtract()
    val rows = spark
      .range(0, 3)
      .selectExpr("jvm_add_one(41L)", "jvm_subtract(id, 10L)", "jvm_subtract(10L, id)")
      .collect()
    assert(rows.map(_.getLong(0)).toSeq == Seq(42L, 42L, 42L))
    assert(rows.map(_.getLong(1)).toSeq == Seq(-10L, -9L, -8L))
    assert(rows.map(_.getLong(2)).toSeq == Seq(10L, 9L, 8L))
  }

  test("arguments are evaluated natively rather than by the codegen dispatcher") {
    registerAddOne()
    // With the dispatcher off, a projection that needed it for `abs` would fall back to Spark,
    // which cannot evaluate the UDF.
    withSQLConf(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "false") {
      val df = spark.range(0, 4).selectExpr("jvm_add_one(abs(id - 2))")
      assert(column(df) == Seq(3L, 2L, 1L, 2L))
    }
  }

  // Filters, join conditions, grouping keys and window partitioning each reach the serde through
  // a different Comet operator. A regression in any one of them would show only as a fallback to
  // Spark, which fails because Spark cannot evaluate the UDF.

  test("a UDF in a filter predicate runs in the Comet pipeline") {
    registerAddOne()
    val df = spark.range(0, 10).filter("jvm_add_one(id) > 3").select("id")
    assert(column(df).map(_.asInstanceOf[Long]).sorted == (3L to 9L).toSeq)
  }

  test("a UDF in a join condition runs in the Comet pipeline") {
    registerAddOne()
    withTempView("l", "r") {
      spark.range(0, 5).createOrReplaceTempView("l")
      spark.range(0, 5).createOrReplaceTempView("r")
      val rows = spark
        .sql("SELECT l.id, r.id FROM l JOIN r ON jvm_add_one(l.id) = r.id")
        .collect()
        .map(row => (row.getLong(0), row.getLong(1)))
        .sorted
        .toSeq
      assert(rows == Seq((0L, 1L), (1L, 2L), (2L, 3L), (3L, 4L)))
    }
  }

  test("a UDF in a grouping key runs in the Comet pipeline") {
    registerAddOne()
    val rows = spark
      .range(0, 6)
      .selectExpr("id % 3 AS k")
      .groupBy(expr("jvm_add_one(k)").as("g"))
      .count()
      .collect()
      .map(row => (row.getLong(0), row.getLong(1)))
      .sorted
      .toSeq
    assert(rows == Seq((1L, 2L), (2L, 2L), (3L, 2L)))
  }

  test("a UDF in a window PARTITION BY runs in the Comet pipeline") {
    registerAddOne()
    withTempView("w") {
      spark.range(0, 6).selectExpr("id", "id % 3 AS k").createOrReplaceTempView("w")
      val rows = spark
        .sql("SELECT id, row_number() OVER (PARTITION BY jvm_add_one(k) ORDER BY id) FROM w")
        .collect()
        .map(row => (row.getLong(0), row.getInt(1)))
        .sorted
        .toSeq
      assert(rows == Seq((0L, 1), (1L, 1), (2L, 1), (3L, 2), (4L, 2), (5L, 2)))
    }
  }

  /**
   * One case per Spark type: the type, and a SQL expression producing it from the `id` column of
   * `spark.range`. Each case asserts its expression really has the type, so a wrong case fails
   * rather than testing something else.
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
    // The outer cast pins the precision: Spark widens the addition itself to decimal(11,2).
    (DecimalType(10, 2), "cast(cast(id as decimal(10,2)) + 0.25 as decimal(10,2))"),
    (ArrayType(IntegerType, containsNull = false), "array(cast(id as int), cast(id + 1 as int))"),
    (
      MapType(StringType, IntegerType, valueContainsNull = false),
      "map('k', cast(id as int), 'j', cast(id + 1 as int))"),
    (
      StructType(
        Seq(
          StructField("a", IntegerType, nullable = false),
          StructField("b", StringType, nullable = false))),
      "named_struct('a', cast(id as int), 'b', concat('s', cast(id as string)))"),
    // One level of nesting each way.
    (
      ArrayType(
        StructType(Seq(StructField("a", IntegerType, nullable = false))),
        containsNull = false),
      "array(named_struct('a', cast(id as int)))"),
    (
      StructType(
        Seq(StructField("xs", ArrayType(IntegerType, containsNull = false), nullable = false))),
      "named_struct('xs', array(cast(id as int), cast(id + 1 as int)))"))

  /** Byte arrays do not compare by value as `Any`. */
  private def comparable(v: Any): Any = v match {
    case b: Array[Byte] => b.toSeq
    case other => other
  }

  for ((dataType, valueExpr) <- typeCases) {
    test(s"a UDF round-trips ${dataType.simpleString} including nulls") {
      // The last row is null, so every case also covers nulls crossing the boundary both ways.
      val df =
        spark.range(0, 4).selectExpr(s"CASE WHEN id = 3 THEN NULL ELSE $valueExpr END AS c")
      assert(
        df.schema.head.dataType == dataType,
        s"test expression produced ${df.schema.head.dataType}, not $dataType")
      CometJvmUDF.register(spark, "jvm_echo", classOf[EchoUdf], Seq(dataType), dataType)
      val rows = df.selectExpr("c", "jvm_echo(c)").collect()
      assert(rows.last.isNullAt(0))
      rows.foreach(row => assert(comparable(row.get(1)) == comparable(row.get(0)), row))
    }
  }

  for (containsNull <- Seq(true, false)) {
    test(
      s"a list built with Arrow Java's default names is accepted, containsNull=$containsNull") {
      // ListVector names its element `$data$` and marks it nullable, where Comet names it `item`
      // and Spark's type says whether it is nullable. Neither changes the data, so the result is
      // relabelled rather than refused.
      registerRangeList(containsNull)
      val lists = column(spark.range(1, 4).selectExpr("jvm_range_list(id)"))
      assert(lists == Seq(Seq(0L), Seq(0L, 1L), Seq(0L, 1L, 2L)))
    }
  }

  test("a batch of only empty lists built with Arrow Java is accepted") {
    // A ListVector's writer creates the element vector on the first element it writes, so a batch
    // of only empty lists arrives with an element of type Null. It holds no values, so it is
    // relabelled to the declared element type like a name.
    registerRangeList(containsNull = false)
    val lists = column(spark.range(0, 3).selectExpr("jvm_range_list(id * 0)"))
    assert(lists == Seq(Seq(), Seq(), Seq()))
  }

  test("a list built with Arrow Java's default names combines with Comet's own lists") {
    // The plan has to carry the result under Comet's names, not Arrow Java's: `if` puts both
    // branches into one column, which fails if their types differ by a field name. Both branch
    // orders, since the native `if` reports the type of its first branch.
    registerRangeList(containsNull = false)
    for (sql <- Seq(
        "if(id > 0, jvm_range_list(id), array(-1L))",
        "if(id = 0, array(-1L), jvm_range_list(id))")) {
      assert(
        column(spark.range(0, 3).selectExpr(sql)) == Seq(Seq(-1L), Seq(0L), Seq(0L, 1L)),
        sql)
    }
  }

  test("a result of another type than declared fails naming both types") {
    CometJvmUDF.register(spark, "jvm_int_result", classOf[IntResultUdf], Seq(LongType), LongType)
    val e = intercept[Exception] {
      spark.range(0, 4).selectExpr("jvm_int_result(id)").collect()
    }
    assert(
      causeChainContains(e, "returned Int32 but its declared return type is Int64"),
      s"unhelpful error: $e")
    assert(causeChainContains(e, classOf[IntResultUdf].getName), s"error lacks the class: $e")
  }

  test("a result with the wrong number of rows fails") {
    CometJvmUDF.register(
      spark,
      "jvm_short_result",
      classOf[ShortResultUdf],
      Seq(LongType),
      LongType)
    // One partition, so the batch holds all four rows.
    val e = intercept[Exception] {
      spark.range(0, 4, 1, 1).selectExpr("jvm_short_result(id)").collect()
    }
    assert(causeChainContains(e, "returned 3 rows, expected 4"), s"unhelpful error: $e")
  }

  test("a call whose argument types differ from the registered ones fails analysis") {
    registerAddOne()
    val e = intercept[AnalysisException] {
      spark.range(0, 3).selectExpr("jvm_add_one(cast(id as int))")
    }
    assert(e.getMessage.contains("BIGINT"), e.getMessage)

    // Spark inserts no cast, so the query has to.
    val fixed = spark.range(0, 3).selectExpr("jvm_add_one(cast(cast(id as int) as bigint))")
    assert(column(fixed) == Seq(1L, 2L, 3L))
  }

  test("a call with the wrong number of arguments fails analysis") {
    registerAddOne()
    val e = intercept[AnalysisException] {
      spark.range(0, 2).selectExpr("jvm_add_one(id, id)")
    }
    assert(
      e.getMessage.contains("requires 1 parameters but the actual number is 2"),
      e.getMessage)
  }

  test("a UDF can take more than four arguments") {
    CometJvmUDF.register(spark, "jvm_sum", classOf[SumUdf], Seq.fill(5)(LongType), LongType)
    val df = spark.range(0, 3).selectExpr("jvm_sum(id, id, id, id, 1L)")
    assert(column(df) == Seq(1L, 5L, 9L))
  }

  test("an ordinary UDF registered under the same name replaces a vectorized one") {
    registerAddOne()
    spark.udf.register("jvm_add_one", (x: Long) => x * 10)
    assert(column(spark.range(0, 3).selectExpr("jvm_add_one(id)")) == Seq(0L, 10L, 20L))
    // Registering the vectorized UDF again takes the name back.
    registerAddOne()
    assert(column(spark.range(0, 3).selectExpr("jvm_add_one(id)")) == Seq(1L, 2L, 3L))
  }

  test("a registration belongs to the session that made it") {
    registerAddOne()
    val other = spark.newSession()
    val e = intercept[AnalysisException] {
      other.range(0, 1).selectExpr("jvm_add_one(id)")
    }
    assert(e.getMessage.contains("jvm_add_one"), e.getMessage)
    // The other session can give the name a UDF of its own without disturbing this one.
    CometJvmUDF.register(other, "jvm_add_one", classOf[EchoUdf], Seq(LongType), LongType)
    assert(column(other.range(0, 3).selectExpr("jvm_add_one(id)")) == Seq(0L, 1L, 2L))
    assert(column(spark.range(0, 3).selectExpr("jvm_add_one(id)")) == Seq(1L, 2L, 3L))
  }

  test("a UDF nested in an ordinary UDF fails the query") {
    // The codegen dispatcher compiles an ordinary UDF's whole argument tree into one JVM kernel,
    // which cannot call a vectorized UDF, so it declines the tree and Spark gets the operator.
    registerAddOne()
    spark.udf.register("jvm_times_ten", (x: Long) => x * 10)
    val e = intercept[Exception] {
      spark.range(0, 2).selectExpr("jvm_times_ten(jvm_add_one(id))").collect()
    }
    assert(
      causeChainContains(e, "UDF 'jvm_add_one' is registered with Comet"),
      s"unhelpful error: $e")
  }

  test("Spark evaluating the UDF fails the query") {
    registerAddOne()
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      val e = intercept[Exception] {
        spark.range(0, 2).selectExpr("jvm_add_one(id)").collect()
      }
      assert(
        causeChainContains(e, "UDF 'jvm_add_one' is registered with Comet"),
        s"unhelpful error: $e")
    }
  }

  test("a nondeterministic UDF is planned as nondeterministic") {
    CometJvmUDF.register(
      spark,
      "jvm_add_one_nondeterministic",
      classOf[AddOneUdf],
      Seq(LongType),
      LongType,
      deterministic = false)
    val df = spark.range(0, 3).selectExpr("jvm_add_one_nondeterministic(id)")
    assert(df.queryExecution.analyzed.expressions.exists(!_.deterministic))
    assert(column(df) == Seq(1L, 2L, 3L))
  }

  test("a class the bridge could not instantiate is refused at registration") {
    def refusal(udfClass: Class[_ <: CometUDF]): String =
      intercept[IllegalArgumentException] {
        CometJvmUDF.register(spark, "jvm_refused", udfClass, Seq(LongType), LongType)
      }.getMessage

    assert(refusal(classOf[AbstractUdf]).contains("it is abstract"))
    assert(
      refusal(classOf[NoDefaultConstructorUdf])
        .contains("it has no public no-argument constructor"))
    // An inner class's constructor takes the enclosing instance.
    assert(refusal(classOf[InnerUdf]).contains("it has no public no-argument constructor"))
    assert(!spark.catalog.functionExists("jvm_refused"))
  }

  test("a Java UDF from a jar only the executors can see") {
    // The bridge loads the class by name through the context ClassLoader that Comet installs for
    // the call, since native execution calls it from a Tokio worker, which has none of its own.
    // Registered through the Java-facing overload, as a Java application would.
    CometJvmUDF.register(
      spark,
      "jvm_times_two",
      hiddenUdfClass,
      java.util.Arrays.asList[DataType](LongType),
      LongType,
      true)
    withSQLConf(CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "true") {
      withTable("t") {
        sql("CREATE TABLE t (x BIGINT) USING parquet")
        sql("INSERT INTO t VALUES (1), (2), (NULL)")
        assert(column(sql("SELECT jvm_times_two(x) FROM t ORDER BY x")) == Seq(null, 2L, 4L))
      }
    }
    assert(column(spark.range(0, 3).selectExpr("jvm_times_two(id)")) == Seq(0L, 2L, 4L))
  }

  /** Never registered successfully: its constructor needs the enclosing suite. */
  class InnerUdf extends CometUDF {
    override def evaluate(inputs: Array[ValueVector], numRows: Int): ValueVector =
      throw new UnsupportedOperationException
  }
}

object CometJvmUdfSuite {

  /** Row `row` of a vector that is either a column or a literal delivered as one value. */
  private def rowOf(v: ValueVector, row: Int, numRows: Int): Int =
    if (v.getValueCount == numRows) row else 0

  private def newLongVector(name: String, allocator: BufferAllocator, n: Int): BigIntVector = {
    val out = new BigIntVector(name, allocator)
    out.allocateNew(n)
    out
  }

  /** `x + 1`. */
  class AddOneUdf extends CometUDF {
    override def evaluate(inputs: Array[ValueVector], numRows: Int): ValueVector = {
      val in = inputs(0).asInstanceOf[BigIntVector]
      val out = newLongVector("jvm_add_one", in.getAllocator, numRows)
      var i = 0
      while (i < numRows) {
        val r = rowOf(in, i, numRows)
        if (in.isNull(r)) out.setNull(i) else out.set(i, in.get(r) + 1)
        i += 1
      }
      out.setValueCount(numRows)
      out
    }
  }

  /** `a - b`. */
  class SubtractUdf extends CometUDF {
    override def evaluate(inputs: Array[ValueVector], numRows: Int): ValueVector = {
      val a = inputs(0).asInstanceOf[BigIntVector]
      val b = inputs(1).asInstanceOf[BigIntVector]
      val out = newLongVector("jvm_subtract", a.getAllocator, numRows)
      var i = 0
      while (i < numRows) {
        val ra = rowOf(a, i, numRows)
        val rb = rowOf(b, i, numRows)
        if (a.isNull(ra) || b.isNull(rb)) out.setNull(i) else out.set(i, a.get(ra) - b.get(rb))
        i += 1
      }
      out.setValueCount(numRows)
      out
    }
  }

  /** The sum of its arguments, however many it is registered with. */
  class SumUdf extends CometUDF {
    override def evaluate(inputs: Array[ValueVector], numRows: Int): ValueVector = {
      val args = inputs.map(_.asInstanceOf[BigIntVector])
      val out = newLongVector("jvm_sum", args.head.getAllocator, numRows)
      var i = 0
      while (i < numRows) {
        val cells = args.map(a => (a, rowOf(a, i, numRows)))
        if (cells.exists { case (a, r) => a.isNull(r) }) out.setNull(i)
        else out.set(i, cells.map { case (a, r) => a.get(r) }.sum)
        i += 1
      }
      out.setValueCount(numRows)
      out
    }
  }

  /** Copies its argument into a new vector of the same type, whatever that type is. */
  class EchoUdf extends CometUDF {
    override def evaluate(inputs: Array[ValueVector], numRows: Int): ValueVector = {
      val in = inputs(0)
      val out = in.getField.createVector(in.getAllocator)
      out.setInitialCapacity(numRows)
      out.allocateNew()
      var i = 0
      while (i < numRows) {
        out.copyFromSafe(i, i, in)
        i += 1
      }
      out.setValueCount(numRows)
      out
    }
  }

  /**
   * `[0, 1, ..., n - 1]`, built with Arrow Java's own list writer, which names the element field
   * `$data$`.
   */
  class RangeListUdf extends CometUDF {
    override def evaluate(inputs: Array[ValueVector], numRows: Int): ValueVector = {
      val in = inputs(0).asInstanceOf[BigIntVector]
      val out = ListVector.empty("jvm_range_list", in.getAllocator)
      val writer = out.getWriter
      var i = 0
      while (i < numRows) {
        writer.setPosition(i)
        writer.startList()
        var v = 0L
        while (v < in.get(rowOf(in, i, numRows))) {
          writer.bigInt().writeBigInt(v)
          v += 1
        }
        writer.endList()
        i += 1
      }
      out.setValueCount(numRows)
      out
    }
  }

  /** Returns an `IntVector`, whatever it was declared to return. */
  class IntResultUdf extends CometUDF {
    override def evaluate(inputs: Array[ValueVector], numRows: Int): ValueVector = {
      val out = new IntVector("jvm_int_result", inputs(0).getAllocator)
      out.allocateNew(numRows)
      (0 until numRows).foreach(i => out.set(i, i))
      out.setValueCount(numRows)
      out
    }
  }

  /** Returns one row fewer than the batch has. */
  class ShortResultUdf extends CometUDF {
    override def evaluate(inputs: Array[ValueVector], numRows: Int): ValueVector = {
      val out = newLongVector("jvm_short_result", inputs(0).getAllocator, numRows)
      (0 until numRows - 1).foreach(i => out.set(i, i.toLong))
      out.setValueCount(numRows - 1)
      out
    }
  }

  abstract class AbstractUdf extends CometUDF

  class NoDefaultConstructorUdf(k: Long) extends CometUDF {
    override def evaluate(inputs: Array[ValueVector], numRows: Int): ValueVector =
      throw new UnsupportedOperationException(s"never instantiated: $k")
  }

  private val HiddenUdfClassName = "hidden.TimesTwoUdf"

  /**
   * Directory holding a Java `CometUDF` compiled at test time, standing in for a jar passed with
   * `--jars`. It is put on `spark.executor.extraClassPath`, which local mode feeds into the
   * executor's ClassLoader but not into the one that loaded Comet. Compiled once per JVM, before
   * the session starts, because that setting is read at session creation.
   */
  lazy val hiddenClassesDir: Path = TestJavaCompiler.compile(
    "TimesTwoUdf.java",
    """package hidden;
      |
      |import org.apache.arrow.vector.BigIntVector;
      |import org.apache.arrow.vector.ValueVector;
      |import org.apache.comet.udf.CometUDF;
      |
      |public class TimesTwoUdf implements CometUDF {
      |  @Override
      |  public ValueVector evaluate(ValueVector[] inputs, int numRows) {
      |    BigIntVector in = (BigIntVector) inputs[0];
      |    BigIntVector out = new BigIntVector("jvm_times_two", in.getAllocator());
      |    out.allocateNew(numRows);
      |    for (int i = 0; i < numRows; i++) {
      |      if (in.isNull(i)) {
      |        out.setNull(i);
      |      } else {
      |        out.set(i, in.get(i) * 2);
      |      }
      |    }
      |    out.setValueCount(numRows);
      |    return out;
      |  }
      |}
      |""".stripMargin,
    Seq(classOf[CometUDF], classOf[ValueVector], classOf[BufferAllocator]))

  /**
   * The UDF's class on the driver, as an application would hold it. Loaded through a ClassLoader
   * over `hiddenClassesDir`, which executors never see: they resolve the class by name.
   */
  lazy val hiddenUdfClass: Class[_ <: CometUDF] =
    new URLClassLoader(Array(hiddenClassesDir.toUri.toURL), getClass.getClassLoader)
      .loadClass(HiddenUdfClassName)
      .asSubclass(classOf[CometUDF])
}
