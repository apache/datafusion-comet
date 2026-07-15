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
import java.text.SimpleDateFormat

import scala.util.Random

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, Literal}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.DataTypeSupport.isComplexType
import org.apache.comet.codegen.CometBatchKernelCodegen
import org.apache.comet.testing.{DataGenOptions, FuzzDataGenerator, ParquetGenerator, SchemaGenOptions}
import org.apache.comet.vector.CometVector

/**
 * Randomized end-to-end tests for the Arrow-direct codegen dispatcher: schema-driven coverage of
 * every input vector class against random parquet, plus a decimal precision-scale sweep across
 * the `Decimal.MAX_LONG_DIGITS=18` boundary at varying null densities. Extends [[CometTestBase]]
 * (not [[CometFuzzTestBase]]) because the base's `shuffle` x `nativeC2R` cross-product is
 * irrelevant for projection-only queries.
 */
class CometCodegenFuzzSuite
    extends CometTestBase
    with AdaptiveSparkPlanHelper
    with CometCodegenAssertions {

  /** Random schema with primitives plus shallow arrays and structs. No maps, no deep nesting. */
  private var mixedTypesFilename: String = _

  /** Random schema with deeply nested arrays / structs / maps. */
  private var nestedTypesFilename: String = _

  /** Asia/Kathmandu has a non-zero minute offset (UTC+5:45); good for timezone edge cases. */
  private val defaultTimezone = "Asia/Kathmandu"

  override def beforeAll(): Unit = {
    super.beforeAll()
    val tempDir = System.getProperty("java.io.tmpdir")
    val random = new Random(42)
    val dataGenOptions = DataGenOptions(
      generateNegativeZero = false,
      baseDate = new SimpleDateFormat("YYYY-MM-DD hh:mm:ss")
        .parse("2024-05-25 12:34:56")
        .getTime)

    mixedTypesFilename = s"$tempDir/CometCodegenFuzzSuite_${System.currentTimeMillis()}.parquet"
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "false",
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> defaultTimezone) {
      val schemaGenOptions =
        SchemaGenOptions(generateArray = true, generateStruct = true)
      ParquetGenerator.makeParquetFile(
        random,
        spark,
        mixedTypesFilename,
        1000,
        schemaGenOptions,
        dataGenOptions)
    }

    nestedTypesFilename =
      s"$tempDir/CometCodegenFuzzSuite_nested_${System.currentTimeMillis()}.parquet"
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "false",
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> defaultTimezone) {
      val schemaGenOptions =
        SchemaGenOptions(generateArray = true, generateStruct = true, generateMap = true)
      val schema = FuzzDataGenerator.generateNestedSchema(
        random,
        numCols = 10,
        minDepth = 2,
        maxDepth = 4,
        options = schemaGenOptions)
      ParquetGenerator.makeParquetFile(
        random,
        spark,
        nestedTypesFilename,
        schema,
        1000,
        dataGenOptions)
    }

    spark.read.parquet(mixedTypesFilename).createOrReplaceTempView("t1")
    spark.read.parquet(nestedTypesFilename).createOrReplaceTempView("t2")
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    FileUtils.deleteDirectory(new File(mixedTypesFilename))
    FileUtils.deleteDirectory(new File(nestedTypesFilename))
  }

  private val RowCount: Int = 512
  private val nullDensities: Seq[Double] = Seq(0.0, 0.1, 0.5, 1.0)
  // (precision, scale) shapes spanning both sides of `Decimal.MAX_LONG_DIGITS=18`: small short,
  // boundary short with varying scale, just-past-boundary long, and max decimal128.
  private val decimalShapes: Seq[(Int, Int)] = Seq((9, 2), (18, 0), (18, 9), (19, 0), (38, 10))

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key, "true")

  /**
   * Identity ScalaUDF for one of the 14 primitive types in
   * [[org.apache.comet.testing.SchemaGenOptions.defaultPrimitiveTypes]]. Returns the registered
   * name when the type maps to a known Scala arg, or `None` for shapes we choose not to probe.
   * `BigDecimal` UDF args are encoded as `DecimalType(38, 18)`; Spark inserts an implicit cast
   * around the call but the underlying column read still hits our kernel's `getDecimal` at the
   * column's native precision.
   */
  private def registerIdentityUdfFor(dt: DataType, name: String): Option[String] = dt match {
    case _: BooleanType => spark.udf.register(name, (x: Boolean) => x); Some(name)
    case _: ByteType => spark.udf.register(name, (x: Byte) => x); Some(name)
    case _: ShortType => spark.udf.register(name, (x: Short) => x); Some(name)
    case _: IntegerType => spark.udf.register(name, (x: Int) => x); Some(name)
    case _: LongType => spark.udf.register(name, (x: Long) => x); Some(name)
    case _: FloatType => spark.udf.register(name, (x: Float) => x); Some(name)
    case _: DoubleType => spark.udf.register(name, (x: Double) => x); Some(name)
    case _: DecimalType =>
      spark.udf.register(name, (x: java.math.BigDecimal) => x); Some(name)
    case _: DateType => spark.udf.register(name, (x: java.sql.Date) => x); Some(name)
    case _: TimestampType =>
      spark.udf.register(name, (x: java.sql.Timestamp) => x); Some(name)
    case _: TimestampNTZType =>
      spark.udf.register(name, (x: java.time.LocalDateTime) => x); Some(name)
    case _: StringType => spark.udf.register(name, (x: String) => x); Some(name)
    case _: BinaryType => spark.udf.register(name, (x: Array[Byte]) => x); Some(name)
    case _ => None
  }

  /**
   * Identity-Int UDF for the cardinality-based complex probe. One UDF covers every Array and Map
   * column, regardless of element type.
   *
   * Avoids `Seq[T]` / `Map[K, V]` UDF arg materialization: Spark's `MapObjects.doGenCode` reads
   * each element unconditionally and null-checks afterward, so on null positions of a
   * dictionary-encoded primitive Arrow vector the garbage ID buffer feeds
   * `dictionary.decodeToLong/decodeToFloat` and throws `ArrayIndexOutOfBoundsException`. Bug
   * reproduces in pure Spark; `cardinality(col)` exercises `getArray`/`getMap` without entering
   * the element deserializer.
   */
  private lazy val cardinalityProbeUdf: String = {
    val name = "sz_complex"
    spark.udf.register(name, (i: Int) => i)
    name
  }

  test("identity ScalaUDF over every primitive column") {
    val primitiveFields =
      spark.table("t1").schema.fields.filterNot(f => isComplexType(f.dataType))
    assert(primitiveFields.nonEmpty, "expected at least one primitive column in random schema")
    for (field <- primitiveFields) {
      val udfName = s"id_${field.name}"
      registerIdentityUdfFor(field.dataType, udfName) match {
        case Some(_) =>
          assertCodegenRan {
            checkSparkAnswerAndOperator(s"SELECT $udfName(${field.name}) FROM t1")
          }
        case None =>
          fail(
            s"primitive column ${field.name}: ${field.dataType} not in identity UDF catalog; " +
              "extend registerIdentityUdfFor")
      }
    }
  }

  test("complex-probe ScalaUDF on every complex column") {
    val complexFields = spark.table("t1").schema.fields.filter(f => isComplexType(f.dataType))
    assert(complexFields.nonEmpty, "expected at least one complex column in random schema")
    for (field <- complexFields) {
      probeComplexColumn(field, viewName = "t1")
    }
  }

  test("complex-probe ScalaUDF on top-level columns of deeply nested schema") {
    for (field <- spark.table("t2").schema.fields) {
      probeComplexColumn(field, viewName = "t2")
    }
  }

  /**
   * Element-level fuzz for nested array reads: `ArrayMax.doGenCode` walks every element of every
   * row, calling the kernel's nested element getter, the path the unsafe-getter optimization
   * touches and which the cardinality probe deliberately skips.
   */
  test("array_max element fuzz: every Array<primitive> column") {
    val arrayPrimitiveFields = spark.table("t1").schema.fields.filter {
      case StructField(_, ArrayType(elemDt, _), _, _) if !isComplexType(elemDt) => true
      case _ => false
    }
    assert(
      arrayPrimitiveFields.nonEmpty,
      "expected at least one Array<primitive> column in random schema")
    for (field <- arrayPrimitiveFields) {
      val ArrayType(elemDt, _) = field.dataType: @unchecked
      val udfName = s"id_arrmax_${field.name}"
      registerIdentityUdfFor(elemDt, udfName) match {
        case Some(_) =>
          assertCodegenRan {
            checkSparkAnswerAndOperator(s"SELECT $udfName(array_max(${field.name})) FROM t1")
          }
        case None =>
          fail(
            s"array column ${field.name} elem ${elemDt} not in identity UDF catalog; " +
              "extend registerIdentityUdfFor")
      }
    }
  }

  /**
   * Map variant of the array element fuzz: `map_keys` / `map_values` produce arrays the kernel
   * walks via `ArrayMax`, exercising the map's per-row offset chain (MapVector -> entries
   * StructVector -> child) that the array test alone wouldn't catch.
   */
  test("array_max element fuzz: map_keys / map_values on Map<primitive, primitive> columns") {
    val mapPrimitiveFields = spark.table("t2").schema.fields.filter {
      case StructField(_, MapType(kDt, vDt, _), _, _)
          if !isComplexType(kDt) && !isComplexType(vDt) =>
        true
      case _ => false
    }
    for (field <- mapPrimitiveFields) {
      val MapType(kDt, vDt, _) = field.dataType: @unchecked
      registerIdentityUdfFor(kDt, s"id_mapk_${field.name}").foreach { udf =>
        assertCodegenRan {
          checkSparkAnswerAndOperator(s"SELECT $udf(array_max(map_keys(${field.name}))) FROM t2")
        }
      }
      registerIdentityUdfFor(vDt, s"id_mapv_${field.name}").foreach { udf =>
        assertCodegenRan {
          checkSparkAnswerAndOperator(
            s"SELECT $udf(array_max(map_values(${field.name}))) FROM t2")
        }
      }
    }
  }

  /**
   * Doubly-nested array element fuzz: `flatten(arr)` collapses `Array<Array<X>>` into `Array<X>`
   * (exercising the outer-array element getter that returns each inner ArrayData), then
   * `array_max` walks the leaf X primitives. Closes the gap that the singly-nested
   * `array_max(arr)` test alone leaves on doubly-nested primitive arrays.
   */
  test("array_max element fuzz: flatten on Array<Array<primitive>> columns") {
    val nestedArrayPrimitiveFields = spark.table("t2").schema.fields.filter {
      case StructField(_, ArrayType(ArrayType(elemDt, _), _), _, _) if !isComplexType(elemDt) =>
        true
      case _ => false
    }
    for (field <- nestedArrayPrimitiveFields) {
      val ArrayType(ArrayType(elemDt, _), _) = field.dataType: @unchecked
      val udfName = s"id_arrflat_${field.name}"
      registerIdentityUdfFor(elemDt, udfName).foreach { _ =>
        assertCodegenRan {
          checkSparkAnswerAndOperator(
            s"SELECT $udfName(array_max(flatten(${field.name}))) FROM t2")
        }
      }
    }
  }

  /**
   * Element-level fuzz for `Array<Struct<...>>`. `array_distinct` is a non-HOF unary expression
   * that hashes each element to dedupe. Struct hashing is field-wise, so the kernel emits element
   * reads on each struct's fields. `cardinality` consumes the result without materialization.
   * Asserts the optimizer keeps `ArrayDistinct` so the coverage isn't vacuously folded.
   */
  test("array_distinct element fuzz: Array<Struct<primitives>> columns") {
    val arrayStructFields = spark.table("t1").schema.fields.filter {
      case StructField(_, ArrayType(_: StructType, _), _, _) => true
      case _ => false
    }
    spark.udf.register("id_int_arrdistinct", (i: Int) => i)
    for (field <- arrayStructFields) {
      val q = s"SELECT id_int_arrdistinct(cardinality(array_distinct(${field.name}))) FROM t1"
      val df = sql(q)
      val plan = df.queryExecution.optimizedPlan.toString
      val planLower = plan.toLowerCase
      assert(
        planLower.contains("array_distinct") || planLower.contains("arraydistinct"),
        s"optimizer eliminated array_distinct on column ${field.name}; coverage would be " +
          s"vacuous. plan=\n$plan")
      assertCodegenRan {
        checkSparkAnswerAndOperator(df)
      }
    }
  }

  private def probeCardinality(accessor: String, dt: DataType, viewName: String): Unit = {
    // `Size` only supports `ArrayType` in Comet, so for `MapType` we route through `map_keys` to
    // reach a `Size(ArrayType)`. Spark still calls `getMap` on the column vector to extract the
    // keys, which is the accessor path this probe is intended to exercise.
    val sizeExpr = dt match {
      case _: MapType => s"size(map_keys($accessor))"
      case _ => s"cardinality($accessor)"
    }
    assertCodegenRan {
      checkSparkAnswerAndOperator(s"SELECT $cardinalityProbeUdf($sizeExpr) FROM $viewName")
    }
  }

  /**
   * Top-level Array / Map produces a cardinality probe. Struct drills into each scalar child via
   * `GetStructField`. Nested Array / Map sub-fields also get the cardinality probe (depth bound:
   * deeper struct-of-struct nesting is skipped to keep the sweep finite).
   */
  private def probeComplexColumn(field: StructField, viewName: String): Unit = {
    field.dataType match {
      case _: ArrayType | _: MapType =>
        probeCardinality(field.name, field.dataType, viewName)

      case st: StructType =>
        for (subField <- st.fields) {
          val accessor = s"${field.name}.${subField.name}"
          subField.dataType match {
            case _: ArrayType | _: MapType =>
              probeCardinality(accessor, subField.dataType, viewName)
            case dt if !isComplexType(dt) =>
              val udfName = s"id_${field.name}_${subField.name}"
              registerIdentityUdfFor(dt, udfName).foreach { _ =>
                assertCodegenRan {
                  checkSparkAnswerAndOperator(s"SELECT $udfName($accessor) FROM $viewName")
                }
              }
            case _ => // deeper struct nesting skipped
          }
        }

      case _ =>
    }
  }

  /** Random `BigDecimal` values fitting `(precision, scale)`, with `nullDensity` of them null. */
  private def generateDecimals(
      seed: Long,
      precision: Int,
      scale: Int,
      nullDensity: Double): Seq[java.math.BigDecimal] = {
    val rng = new Random(seed)
    val intDigits = precision - scale
    // `BigInt.apply(bits, rng)` samples uniformly on `[0, 2^bits - 1]`; bound to the decimal's
    // integer-part range (10^intDigits - 1) so the result fits the schema. `BigInteger.bitLength`
    // would overshoot slightly. Min with the exact max is cheap insurance.
    val intMax = BigInt(10).pow(intDigits) - 1
    val bits = math.max(intMax.bitLength, 1)
    (0 until RowCount).map { _ =>
      if (rng.nextDouble() < nullDensity) null
      else {
        val mag = BigInt(bits, rng).min(intMax)
        val signed = if (rng.nextBoolean()) -mag else mag
        new java.math.BigDecimal(signed.bigInteger, scale)
      }
    }
  }

  private def withDecimalTable(decimalType: String, values: Seq[java.math.BigDecimal])(
      f: => Unit): Unit = {
    withTable("t") {
      sql(s"CREATE TABLE t (d $decimalType) USING parquet")
      if (values.nonEmpty) {
        val rows = values.map { v =>
          if (v == null) "(NULL)" else s"(${v.toPlainString})"
        }
        rows.grouped(64).foreach { batch =>
          sql(s"INSERT INTO t VALUES ${batch.mkString(", ")}")
        }
      }
      f
    }
  }

  for {
    density <- nullDensities
    (precision, scale) <- decimalShapes
  } {
    test(s"decimal identity precision=$precision scale=$scale nullDensity=$density") {
      spark.udf.register("dec_id_fuzz", (d: java.math.BigDecimal) => d)
      val seed = ((precision * 31L) + scale) * 31L + density.hashCode
      val values = generateDecimals(seed, precision, scale, density)
      withDecimalTable(s"DECIMAL($precision, $scale)", values) {
        assertCodegenRan {
          checkSparkAnswerAndOperator(sql("SELECT dec_id_fuzz(d) FROM t"))
        }
      }
    }
  }

  /**
   * Randomized output-writer coverage (#4539). Generates a random nested output type and a random
   * catalyst value of that type, wraps it in a `Literal`, and drives it through the kernel output
   * writer with [[runKernel]]. Reading the Arrow output back must reproduce the value.
   *
   * Random Array / Map sizes mean each collection's child vector fills at a cumulative index that
   * `numRows` does not bound, so the writer must grow the child with `setSafe` (the #4539 fix). A
   * multi-row batch additionally exercises the cumulative index across rows. The root is always a
   * collection so the nested-write path always runs. The generated value is its own oracle:
   * `CatalystTypeConverters.convertToScala` materializes both the value and the Arrow readback
   * (both expose the catalyst ArrayData / MapData / InternalRow interface) and the two must
   * compare equal.
   */
  private val outputLeafTypes: Seq[DataType] =
    Seq(IntegerType, LongType, DoubleType, BooleanType, StringType, DecimalType(10, 2))

  private def randomLeafType(r: Random): DataType =
    outputLeafTypes(r.nextInt(outputLeafTypes.size))

  /** Random nested type, biased toward leaves as depth runs out. Map keys are always leaves. */
  private def randomOutputType(r: Random, depth: Int): DataType =
    if (depth <= 0 || r.nextDouble() < 0.4) randomLeafType(r)
    else
      r.nextInt(3) match {
        case 0 => ArrayType(randomOutputType(r, depth - 1), containsNull = true)
        case 1 =>
          MapType(randomLeafType(r), randomOutputType(r, depth - 1), valueContainsNull = true)
        case _ =>
          StructType((0 to r.nextInt(2)).map(i =>
            StructField(s"f$i", randomOutputType(r, depth - 1), nullable = true)))
      }

  private def randomLeafValue(r: Random, dt: DataType): Any = dt match {
    case IntegerType => r.nextInt()
    case LongType => r.nextLong()
    case DoubleType => r.nextDouble()
    case BooleanType => r.nextBoolean()
    case StringType => UTF8String.fromString(s"s${r.nextInt(1000000)}")
    case d: DecimalType => Decimal((r.nextInt(2000000) - 1000000).toLong, d.precision, d.scale)
    case other => throw new IllegalArgumentException(s"unexpected leaf type $other")
  }

  /** Random catalyst value of `dt`; `nullable` permits an occasional null element / field. */
  private def randomOutputValue(r: Random, dt: DataType, nullable: Boolean): Any = {
    if (nullable && r.nextDouble() < 0.2) null
    else
      dt match {
        case ArrayType(e, containsNull) =>
          val n = r.nextInt(40)
          new GenericArrayData(
            (0 until n).map(_ => randomOutputValue(r, e, containsNull)).toArray[Any])
        case MapType(k, v, valueContainsNull) =>
          // Dedup by materialized key so the map round-trips 1:1 (Spark map keys are distinct).
          val entries = scala.collection.mutable.LinkedHashMap.empty[Any, Any]
          (0 until r.nextInt(20)).foreach { _ =>
            val key = randomOutputValue(r, k, nullable = false)
            entries.getOrElseUpdate(key, randomOutputValue(r, v, valueContainsNull))
          }
          new ArrayBasedMapData(
            new GenericArrayData(entries.keys.toArray[Any]),
            new GenericArrayData(entries.values.toArray[Any]))
        case st: StructType =>
          new GenericInternalRow(
            st.fields.map(f => randomOutputValue(r, f.dataType, f.nullable)).toArray[Any])
        case leaf => randomLeafValue(r, leaf)
      }
  }

  /** Reads the root collection value of `vec` at `row` as a catalyst ArrayData / MapData. */
  private def readRoot(vec: CometVector, dt: DataType, row: Int): Any = dt match {
    case _: ArrayType => vec.getArray(row)
    case _: MapType => vec.getMap(row)
    case other => throw new IllegalArgumentException(s"unexpected root type $other")
  }

  test("randomized dynamically-sized collection output round-trips through the writer (#4539)") {
    val r = new Random(42)
    val numRows = 4 // > 1 so the child's cumulative index accumulates across rows
    // canHandle may reject a generated type (e.g. the maxFields gate on a wide nesting); count
    // the ones we actually drove through the writer to guard against a vacuous run.
    val exercised = (0 until 300).count { _ =>
      // Root is always a collection so the nested-child write path runs every iteration.
      val dt =
        if (r.nextBoolean()) ArrayType(randomOutputType(r, 2), containsNull = true)
        else MapType(randomLeafType(r), randomOutputType(r, 2), valueContainsNull = true)
      val value = randomOutputValue(r, dt, nullable = false)
      val expr = Literal(value, dt)
      val handled = CometBatchKernelCodegen.canHandle(expr).isEmpty
      if (handled) {
        val expected = CatalystTypeConverters.convertToScala(value, dt)
        runKernel(expr, numRows) { vec =>
          (0 until numRows).foreach { row =>
            val actual = CatalystTypeConverters.convertToScala(readRoot(vec, dt, row), dt)
            assert(
              actual === expected,
              s"row $row mismatch for output type $dt\n expected=$expected\n actual=$actual")
          }
        }
      }
      handled
    }
    assert(exercised > 0, "every generated type was rejected by canHandle (of 300 generated)")
  }
}
