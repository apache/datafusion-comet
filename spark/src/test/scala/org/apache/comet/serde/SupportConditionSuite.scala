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

package org.apache.comet.serde

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.expressions.{Attribute, Literal}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

class SupportConditionSuite extends AnyFunSuite {

  /** Serde with no conditions: default path yields Compatible(None). */
  private object EmptySerde extends CometExpressionSerde[Literal] {
    override def convert(
        expr: Literal,
        inputs: Seq[Attribute],
        binding: Boolean): Option[ExprOuterClass.Expr] = None
  }

  /** Serde with three ordered conditions exercising each level. */
  private object OrderedSerde extends CometExpressionSerde[Literal] {
    override val conditions: Seq[SupportCondition[Literal]] = Seq(
      SupportCondition.unsupported[Literal](
        id = "null-literal",
        description = "Literal value is null",
        fires = _.value == null,
        message = "null literal not supported"),
      SupportCondition.incompatible[Literal](
        id = "long-literal",
        description = "Literal is LongType",
        fires = _.dataType == LongType,
        message = "Long literals are incompatible"),
      SupportCondition.compatibleWithNote[Literal](
        id = "string-with-note",
        description = "Literal is StringType",
        fires = _.dataType == StringType,
        message = "string literal carries a note"))

    override def convert(
        expr: Literal,
        inputs: Seq[Attribute],
        binding: Boolean): Option[ExprOuterClass.Expr] = None
  }

  /** Serde with an expression-dependent message. */
  private object DynamicMessageSerde extends CometExpressionSerde[Literal] {
    override val conditions: Seq[SupportCondition[Literal]] = Seq(
      SupportCondition[Literal](
        id = "dynamic",
        description = "Always fires, message includes data type",
        level = SupportLevelKind.Unsupported,
        fires = _ => true,
        message = (e: Literal) => s"unsupported dtype ${e.dataType.simpleString}"))

    override def convert(
        expr: Literal,
        inputs: Seq[Attribute],
        binding: Boolean): Option[ExprOuterClass.Expr] = None
  }

  test("empty conditions returns Compatible(None)") {
    val result = EmptySerde.getSupportLevel(Literal(1, IntegerType))
    assert(result == Compatible(None))
  }

  test("no matching condition returns Compatible(None)") {
    val result = OrderedSerde.getSupportLevel(Literal(1, IntegerType))
    assert(result == Compatible(None))
  }

  test("first matching condition wins when multiple could fire") {
    // null Long literal matches both "null-literal" (Unsupported) and
    // "long-literal" (Incompatible). Unsupported must win because it is first.
    val nullLong = Literal(null, LongType)
    val result = OrderedSerde.getSupportLevel(nullLong)
    assert(result == Unsupported(Some("null literal not supported")))
  }

  test("non-first matching condition produces its own message") {
    val longLit = Literal(42L, LongType)
    val result = OrderedSerde.getSupportLevel(longLit)
    assert(result == Incompatible(Some("Long literals are incompatible")))
  }

  test("compatible-with-note produces Compatible(Some(msg))") {
    val stringLit = Literal(UTF8String.fromString("hello"), StringType)
    val result = OrderedSerde.getSupportLevel(stringLit)
    assert(result == Compatible(Some("string literal carries a note")))
  }

  test("message can depend on the expression instance") {
    val result =
      DynamicMessageSerde.getSupportLevel(Literal(UTF8String.fromString("x"), StringType))
    assert(result == Unsupported(Some("unsupported dtype string")))
  }
}
