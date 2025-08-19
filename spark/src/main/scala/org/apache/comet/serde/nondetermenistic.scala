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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Literal, MonotonicallyIncreasingID, Rand, Randn, SparkPartitionID}

object CometSparkPartitionId extends CometExpressionSerde[SparkPartitionID] {
  override def convert(
      expr: SparkPartitionID,
      _inputs: Seq[Attribute],
      _binding: Boolean): Option[ExprOuterClass.Expr] = {
    Some(
      ExprOuterClass.Expr
        .newBuilder()
        .setSparkPartitionId(ExprOuterClass.EmptyExpr.newBuilder())
        .build())
  }
}

object CometMonotonicallyIncreasingId extends CometExpressionSerde[MonotonicallyIncreasingID] {
  override def convert(
      expr: MonotonicallyIncreasingID,
      _inputs: Seq[Attribute],
      _binding: Boolean): Option[ExprOuterClass.Expr] = {
    Some(
      ExprOuterClass.Expr
        .newBuilder()
        .setMonotonicallyIncreasingId(ExprOuterClass.EmptyExpr.newBuilder())
        .build())
  }
}

sealed abstract class CometRandCommonSerde[T <: Expression] extends CometExpressionSerde[T] {
  protected def extractSeedFromExpr(expr: Expression): Option[Long] = {
    expr match {
      case Literal(seed: Long, _) => Some(seed)
      case Literal(null, _) => Some(0L)
      case _ => None
    }
  }
}

object CometRand extends CometRandCommonSerde[Rand] {
  override def convert(
      expr: Rand,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    extractSeedFromExpr(expr.child).map { seed =>
      ExprOuterClass.Expr
        .newBuilder()
        .setRand(ExprOuterClass.Rand.newBuilder().setSeed(seed))
        .build()
    }
  }
}

object CometRandn extends CometRandCommonSerde[Randn] {
  override def convert(
      expr: Randn,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    extractSeedFromExpr(expr.child).map { seed =>
      ExprOuterClass.Expr
        .newBuilder()
        .setRandn(ExprOuterClass.Rand.newBuilder().setSeed(seed))
        .build()
    }
  }
}
