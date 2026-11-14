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

package org.apache.spark.sql.comet.execution.arrow

import java.math.{BigDecimal => JBigDecimal, BigInteger}
import java.nio.charset.StandardCharsets

import scala.util.Random

import org.scalatest.funsuite.AnyFunSuite

import org.apache.arrow.c.CDataDictionaryProvider
import org.apache.arrow.vector.{DecimalVector, FieldVector, LargeVarCharVector, VarCharVector}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.{Decimal, DecimalType, StringType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.CometArrowAllocator
import org.apache.comet.vector.CometVector

/** The bounds and null counts `ArrowCachedBatchSerializer` records for decimal columns. */
class ArrowCachedBatchStatsSuite extends AnyFunSuite {

  private val serializer = new ArrowCachedBatchSerializer

  // Values that sit on either side of the boundaries between the two 64-bit words of a 128-bit
  // unscaled value: the low word's sign bit, a carry into the high word, and both signs of each.
  private def boundaryValues(precision: Int): Seq[BigInteger] = {
    val two63 = BigInteger.ONE.shiftLeft(63)
    val two64 = BigInteger.ONE.shiftLeft(64)
    val limit = BigInteger.TEN.pow(precision).subtract(BigInteger.ONE)
    val candidates = Seq(
      BigInteger.ZERO,
      BigInteger.ONE,
      two63.subtract(BigInteger.ONE),
      two63,
      two63.add(BigInteger.ONE),
      two64.subtract(BigInteger.ONE),
      two64,
      two64.add(BigInteger.ONE),
      two64.multiply(BigInteger.valueOf(3)).add(two63))
    val signed = candidates ++ candidates.map(_.negate())
    (signed :+ limit :+ limit.negate()).filter(_.abs.compareTo(limit) <= 0).distinct
  }

  private def randomValues(precision: Int, count: Int, seed: Long): Seq[BigInteger] = {
    val random = new Random(seed)
    val limit = BigInteger.TEN.pow(precision)
    Seq.fill(count) {
      // A random width, so the values spread over the words instead of all filling the high one.
      val bits = 1 + random.nextInt(limit.bitLength())
      val magnitude = new BigInteger(bits, new java.util.Random(random.nextLong())).mod(limit)
      if (random.nextBoolean()) magnitude else magnitude.negate()
    }
  }

  // Gathers the statistics of one decimal column holding `values`, with a null before and after
  // every `nullEvery`-th value when `nullEvery` is positive.
  private def check(precision: Int, scale: Int, values: Seq[BigInteger], nullEvery: Int): Unit = {
    val rows: Seq[Option[BigInteger]] =
      if (nullEvery > 0) {
        values.zipWithIndex.flatMap { case (v, i) =>
          if (i % nullEvery == 0) Seq(None, Some(v), None) else Seq(Some(v))
        }
      } else {
        values.map(Some(_))
      }
    val allocator = CometArrowAllocator.newChildAllocator("decimal-stats", 0, Long.MaxValue)
    val vector = new DecimalVector("d", allocator, precision, scale)
    try {
      vector.allocateNew(rows.length)
      rows.zipWithIndex.foreach {
        case (None, i) => vector.setNull(i)
        case (Some(v), i) => vector.setSafe(i, new JBigDecimal(v, scale))
      }
      vector.setValueCount(rows.length)

      val column = CometVector.getVector(vector, new CDataDictionaryProvider)
      val batch = new ColumnarBatch(Array[ColumnVector](column), rows.length)
      val attrs = Seq(AttributeReference("d", DecimalType(precision, scale))())
      val (lower, upper, nulls) =
        serializer.gatherColumnStats(batch, attrs, serializer.boundsOrderings(attrs))

      val present = rows.flatten
      assert(nulls(0) == rows.length - present.length)
      if (present.isEmpty) {
        assert(lower(0) == null && upper(0) == null)
      } else {
        val expectedMin = new JBigDecimal(present.min, scale)
        val expectedMax = new JBigDecimal(present.max, scale)
        val actualMin = lower(0).asInstanceOf[Decimal]
        val actualMax = upper(0).asInstanceOf[Decimal]
        val context = s"decimal($precision, $scale), ${rows.length} rows"
        assert(actualMin.toJavaBigDecimal.compareTo(expectedMin) == 0, s"min of $context")
        assert(actualMax.toJavaBigDecimal.compareTo(expectedMax) == 0, s"max of $context")
        assert(
          actualMin.precision == precision && actualMin.scale == scale,
          s"min type, $context")
        assert(
          actualMax.precision == precision && actualMax.scale == scale,
          s"max type, $context")
      }
    } finally {
      vector.close()
      allocator.close()
    }
  }

  // Both a narrow precision, whose unscaled value fits the low word, and a wide one, for which the
  // high word decides the order and the low word is compared as unsigned.
  private val precisions =
    Seq((5, 2), (10, 2), (18, 0), (18, 6), (19, 0), (20, 4), (30, 10), (38, 2))

  precisions.foreach { case (precision, scale) =>
    test(s"decimal($precision, $scale) bounds across the word boundaries") {
      val values = boundaryValues(precision)
      check(precision, scale, values, nullEvery = 0)
      // Every order of the same values must give the same bounds.
      check(precision, scale, values.reverse, nullEvery = 0)
      check(precision, scale, new Random(7).shuffle(values), nullEvery = 3)

      // Without the largest magnitudes, which decide the bounds by their high word alone, the
      // values are all near the word boundaries. Of one sign, the high words are equal for many of
      // them, so the low word decides, and it is unsigned.
      val inner = values.filter(_.abs.bitLength <= 66)
      check(precision, scale, inner.filter(_.signum >= 0), nullEvery = 0)
      check(precision, scale, inner.filter(_.signum <= 0), nullEvery = 0)
      check(precision, scale, inner, nullEvery = 2)
    }

    test(s"decimal($precision, $scale) bounds of random values and nulls") {
      (0 until 5).foreach { seed =>
        check(precision, scale, randomValues(precision, 500, seed.toLong), nullEvery = 0)
        check(precision, scale, randomValues(precision, 500, seed.toLong), nullEvery = 7)
      }
    }
  }

  test("decimal bounds of a column with a single value") {
    check(38, 2, Seq(BigInteger.ONE.shiftLeft(100).negate()), nullEvery = 0)
    check(10, 2, Seq(BigInteger.valueOf(-5)), nullEvery = 1)
  }

  // The bounds of a string column, in the unsigned byte order of the default collation.
  private def checkStrings(
      values: Seq[Option[String]],
      vector: (org.apache.arrow.memory.BufferAllocator, Seq[Option[String]]) => FieldVector)
      : Unit = {
    val allocator = CometArrowAllocator.newChildAllocator("string-stats", 0, Long.MaxValue)
    val arrow = vector(allocator, values)
    try {
      val column = CometVector.getVector(arrow, new CDataDictionaryProvider)
      val batch = new ColumnarBatch(Array[ColumnVector](column), values.length)
      val attrs = Seq(AttributeReference("s", StringType)())
      val (lower, upper, nulls) =
        serializer.gatherColumnStats(batch, attrs, serializer.boundsOrderings(attrs))

      val present = values.flatten.map(_.getBytes(StandardCharsets.UTF_8))
      assert(nulls(0) == values.length - present.length)
      if (present.isEmpty) {
        assert(lower(0) == null && upper(0) == null)
      } else {
        val order = Ordering.comparatorToOrdering((a: Array[Byte], b: Array[Byte]) =>
          java.util.Arrays.compareUnsigned(a, b))
        assert(lower(0).asInstanceOf[UTF8String].getBytes.sameElements(present.min(order)))
        assert(upper(0).asInstanceOf[UTF8String].getBytes.sameElements(present.max(order)))
      }
    } finally {
      arrow.close()
      allocator.close()
    }
  }

  private def varChars(
      allocator: org.apache.arrow.memory.BufferAllocator,
      values: Seq[Option[String]]): FieldVector = {
    val vector = new VarCharVector("s", allocator)
    vector.allocateNew()
    values.zipWithIndex.foreach {
      case (None, i) => vector.setNull(i)
      case (Some(v), i) => vector.setSafe(i, v.getBytes(StandardCharsets.UTF_8))
    }
    vector.setValueCount(values.length)
    vector
  }

  private def largeVarChars(
      allocator: org.apache.arrow.memory.BufferAllocator,
      values: Seq[Option[String]]): FieldVector = {
    val vector = new LargeVarCharVector("s", allocator)
    vector.allocateNew()
    values.zipWithIndex.foreach {
      case (None, i) => vector.setNull(i)
      case (Some(v), i) => vector.setSafe(i, v.getBytes(StandardCharsets.UTF_8))
    }
    vector.setValueCount(values.length)
    vector
  }

  // Lengths on both sides of the 8 byte words the comparison reads, shared prefixes of every
  // length, a value that is a prefix of another, and bytes at or above 0x80, which compare as
  // unsigned.
  private val stringValues: Seq[String] = {
    val prefixes = (0 to 17).map(n => "a" * n)
    val lastByteDiffers =
      (7 to 17).flatMap(n => Seq("x" * n + "a", "x" * n + "b", "x" * n + "\u00e9"))
    val multiByte = Seq("\u00e9", "\u20ac", "\ud83d\ude00", "z", "\u00e9a", "\u00e8", "\u7f51")
    prefixes ++ lastByteDiffers ++ multiByte ++ Seq("", "a", "ab", "b", "B")
  }

  test("string bounds across word and byte boundaries") {
    val values = stringValues.map(Option(_))
    checkStrings(values, varChars)
    checkStrings(values.reverse, varChars)
    checkStrings(new Random(3).shuffle(values), varChars)
    // Duplicates, and nulls at the start, between values and at the end.
    checkStrings(None +: values.flatMap(v => Seq(v, None, v)) :+ None, varChars)
    checkStrings(Seq(Some("only")), varChars)
    checkStrings(Seq(Some("")), varChars)
  }

  test("string bounds of the values of a single sign of order") {
    // Only the values that sort after a short prefix, then only those before it, so that the
    // bound is decided by the byte after the shared prefix rather than by the length.
    val after = stringValues.filter(_ > "xxxxxxxx").map(Option(_))
    val before = stringValues.filter(_ < "xxxxxxxx").map(Option(_))
    checkStrings(after, varChars)
    checkStrings(before, varChars)
  }

  test("string bounds of random strings and nulls") {
    (0 until 5).foreach { seed =>
      val random = new Random(seed.toLong)
      val values = Seq.fill(600) {
        if (random.nextInt(8) == 0) {
          None
        } else {
          // A small alphabet over one and two byte characters, so that many strings share long
          // prefixes and the comparison runs through whole words.
          Some(Seq.fill(random.nextInt(40))("ab\u00e9".charAt(random.nextInt(3))).mkString)
        }
      }
      checkStrings(values, varChars)
    }
  }

  test("string bounds of a large-offset vector come from the by-value loop") {
    checkStrings(stringValues.map(Option(_)), largeVarChars)
  }
}
