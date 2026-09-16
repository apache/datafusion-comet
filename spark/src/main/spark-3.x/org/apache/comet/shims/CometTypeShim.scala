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

package org.apache.comet.shims

import java.nio.ByteBuffer
import java.nio.charset.{CharacterCodingException, CodingErrorAction, StandardCharsets}

import scala.annotation.nowarn

import org.apache.spark.sql.catalyst.expressions.aggregate.Mode
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.unsafe.types.UTF8String

trait CometTypeShim {
  @nowarn // Spark 4 feature; stubbed to false in Spark 3.x for compatibility.
  def isStringCollationType(dt: DataType): Boolean = false

  // `mode() WITHIN GROUP (ORDER BY ...)` and the deterministic-flag form (which set `reverseOpt`)
  // are Spark 4.0 features; Spark 3.x `Mode` is always the plain `mode(col)` form.
  @nowarn
  def modeHasUnsupportedOrdering(expr: Mode): Boolean = false

  @nowarn // Spark 4 feature; stubbed to false in Spark 3.x for compatibility.
  def hasNonDefaultStringCollation(dt: DataType): Boolean = false

  @nowarn // Spark 4 feature; collation does not exist in Spark 3.x.
  def hasCollationSupport: Boolean = false

  @nowarn // Spark 4 feature; Variant shredding doesn't exist in Spark 3.x.
  def isVariantStruct(s: StructType): Boolean = false

  @nowarn // Spark 4 feature; VariantType doesn't exist in Spark 3.x.
  def isVariantType(dt: DataType): Boolean = false

  @nowarn // Spark 4 feature; VariantType doesn't exist in Spark 3.x.
  def containsVariantType(dt: DataType): Boolean = false

  @nowarn // Spark 4 feature; VariantType doesn't exist in Spark 3.x.
  def variantType: Option[DataType] = None

  @nowarn // Spark 4.1 feature; TimeType doesn't exist in Spark 3.x.
  def isTimeType(dt: DataType): Boolean = false

  /**
   * `UTF8String.isValid` (which memoizes on the instance) only exists from Spark 4.0, so decode
   * here instead. Rejects the same sequences as Rust's `str::from_utf8`: overlong forms,
   * surrogates, and code points past U+10FFFF. Unlike `Charset.decode`, a `CharsetDecoder` set to
   * REPORT raises rather than silently substituting U+FFFD.
   */
  def isValidUtf8(s: UTF8String): Boolean = {
    val decoder = StandardCharsets.UTF_8
      .newDecoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)
    try {
      decoder.decode(ByteBuffer.wrap(s.getBytes))
      true
    } catch {
      case _: CharacterCodingException => false
    }
  }
}
