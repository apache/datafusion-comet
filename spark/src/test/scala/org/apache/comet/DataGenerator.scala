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

import org.apache.spark.sql.{RandomDataGenerator, Row}
import org.apache.spark.sql.types.{StringType, StructType}

object DataGenerator {
  // note that we use `def` rather than `val` intentionally here so that
  // each test suite starts with a fresh data generator to help ensure
  // that tests are deterministic
  def DEFAULT = new DataGenerator(new Random(42))
  // matches the probability of nulls in Spark's RandomDataGenerator
  private val PROBABILITY_OF_NULL: Float = 0.1f
}

class DataGenerator(r: Random) {
  import DataGenerator._

  /** Pick a random item from a sequence */
  def pickRandom[T](items: Seq[T]): T = {
    items(r.nextInt(items.length))
  }

  /** Generate a random string using the specified characters */
  def generateString(chars: String, maxLen: Int): String = {
    val len = r.nextInt(maxLen)
    Range(0, len).map(_ => chars.charAt(r.nextInt(chars.length))).mkString
  }

  /** Generate random strings */
  def generateStrings(n: Int, maxLen: Int): Seq[String] = {
    Range(0, n).map(_ => r.nextString(maxLen))
  }

  /** Generate random strings using the specified characters */
  def generateStrings(n: Int, chars: String, maxLen: Int): Seq[String] = {
    Range(0, n).map(_ => generateString(chars, maxLen))
  }

  def generateFloats(n: Int): Seq[Float] = {
    Seq(
      Float.MaxValue,
      Float.MinPositiveValue,
      Float.MinValue,
      Float.NaN,
      Float.PositiveInfinity,
      Float.NegativeInfinity,
      1.0f,
      -1.0f,
      Short.MinValue.toFloat,
      Short.MaxValue.toFloat,
      0.0f) ++
      Range(0, n).map(_ => r.nextFloat())
  }

  def generateDoubles(n: Int): Seq[Double] = {
    Seq(
      Double.MaxValue,
      Double.MinPositiveValue,
      Double.MinValue,
      Double.NaN,
      Double.PositiveInfinity,
      Double.NegativeInfinity,
      0.0d) ++
      Range(0, n).map(_ => r.nextDouble())
  }

  def generateBytes(n: Int): Seq[Byte] = {
    Seq(Byte.MinValue, Byte.MaxValue) ++
      Range(0, n).map(_ => r.nextInt().toByte)
  }

  def generateShorts(n: Int): Seq[Short] = {
    val r = new Random(0)
    Seq(Short.MinValue, Short.MaxValue) ++
      Range(0, n).map(_ => r.nextInt().toShort)
  }

  def generateInts(n: Int): Seq[Int] = {
    Seq(Int.MinValue, Int.MaxValue) ++
      Range(0, n).map(_ => r.nextInt())
  }

  def generateLongs(n: Int): Seq[Long] = {
    Seq(Long.MinValue, Long.MaxValue) ++
      Range(0, n).map(_ => r.nextLong())
  }

  // Generate a random row according to the schema, the string filed in the struct could be
  // configured to generate strings by passing a stringGen function. Other types are delegated
  // to Spark's RandomDataGenerator.
  def generateRow(schema: StructType, stringGen: Option[() => String] = None): Row = {
    val fields = schema.fields.map { f =>
      f.dataType match {
        case StructType(children) =>
          generateRow(StructType(children), stringGen)
        case StringType if stringGen.isDefined =>
          val gen = stringGen.get
          val data = if (f.nullable && r.nextFloat() <= PROBABILITY_OF_NULL) {
            null
          } else {
            gen()
          }
          data
        case _ =>
          val gen = RandomDataGenerator.forType(f.dataType, f.nullable, r) match {
            case Some(g) => g
            case None =>
              throw new IllegalStateException(s"No RandomDataGenerator for type ${f.dataType}")
          }
          gen()
      }
    }.toSeq
    Row.fromSeq(fields)
  }

  def generateRows(
      num: Int,
      schema: StructType,
      stringGen: Option[() => String] = None): Seq[Row] = {
    Range(0, num).map(_ => generateRow(schema, stringGen))
  }

}
