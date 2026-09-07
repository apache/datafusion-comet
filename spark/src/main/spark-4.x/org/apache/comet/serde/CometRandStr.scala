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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Literal, RandStr}
import org.apache.spark.sql.types.{IntegerType, LongType}

/**
 * Serde for Spark 4.0+ `randstr(length[, seed])`. Comet reproduces Spark's output exactly: the
 * resolved seed is combined with the partition index and seeds the same `XORShiftRandom` that
 * drives `ExpressionImplUtils.randStr`, so results match Spark bit for bit. `length` and `seed`
 * are required to be foldable, so after constant folding they are literals; a negative length is
 * left to Spark, which raises an error at runtime.
 */
object CometRandStr extends CometExpressionSerde[RandStr] {

  private val nonLiteralLengthReason = "`length` must be a literal value"
  private val negativeLengthReason = "`length` must be non-negative"
  private val nonLiteralSeedReason = "`seed` must be a literal value"

  override def getUnsupportedReasons(): Seq[String] =
    Seq(nonLiteralLengthReason, negativeLengthReason, nonLiteralSeedReason)

  override def getSupportLevel(expr: RandStr): SupportLevel = {
    val lengthReason = expr.length match {
      case Literal(n: Int, IntegerType) if n >= 0 => None
      case Literal(_: Int, IntegerType) => Some(negativeLengthReason)
      case _ => Some(nonLiteralLengthReason)
    }
    val seedReason = expr.seedExpression match {
      case Literal(_: Int, IntegerType) | Literal(_: Long, LongType) => None
      case _ => Some(nonLiteralSeedReason)
    }
    lengthReason.orElse(seedReason).map(r => Unsupported(Some(r))).getOrElse(Compatible(None))
  }

  override def convert(
      expr: RandStr,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    // getSupportLevel guarantees a non-negative literal length and a literal Int/Long seed.
    val length = expr.length match {
      case Literal(n: Int, IntegerType) if n >= 0 => Some(n)
      case _ => None
    }
    val seed = expr.seedExpression match {
      case Literal(s: Int, IntegerType) => Some(s.toLong)
      case Literal(s: Long, LongType) => Some(s)
      case _ => None
    }
    for {
      len <- length
      sd <- seed
    } yield ExprOuterClass.Expr
      .newBuilder()
      .setRandStr(ExprOuterClass.RandStr.newBuilder().setLength(len).setSeed(sd))
      .build()
  }
}
