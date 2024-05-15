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

object DataGenerator {
  // note that we use `def` rather than `val` intentionally here so that
  // each test suite starts with a fresh data generator to help ensure
  // that tests are deterministic
  def DEFAULT = new DataGenerator(new Random(42))
}

class DataGenerator(r: Random) {

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

}
