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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum

import org.apache.comet.expressions.CometEvalMode
import org.apache.comet.serde.{CometExpressionSerde, CometStringDecode, CometToPrettyString, CometWidthBucket, CommonStringExprs}
import org.apache.comet.serde.ExprOuterClass.{BinaryOutputStyle, Expr}

/**
 * `CometExprShim` acts as a shim for parsing expressions from different Spark versions.
 */
trait CometExprShim extends CommonStringExprs {
  protected def evalMode(c: Cast): CometEvalMode.Value =
    CometEvalModeUtil.fromSparkEvalMode(c.evalMode)

  def binaryOutputStyle: BinaryOutputStyle = BinaryOutputStyle.HEX_DISCRETE

  def versionSpecificStringExpressions: Map[Class[_ <: Expression], CometExpressionSerde[_]] =
    Map(classOf[StringDecode] -> CometStringDecode)
  def versionSpecificMathExpressions: Map[Class[_ <: Expression], CometExpressionSerde[_]] =
    Map(classOf[WidthBucket] -> CometWidthBucket)
  def versionSpecificMiscExpressions: Map[Class[_ <: Expression], CometExpressionSerde[_]] =
    Map(classOf[ToPrettyString] -> CometToPrettyString)
  def versionSpecificMapExpressions: Map[Class[_ <: Expression], CometExpressionSerde[_]] =
    Map.empty

  def versionSpecificExprToProtoInternal(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = None
}

object CometEvalModeUtil {
  def fromSparkEvalMode(evalMode: EvalMode.Value): CometEvalMode.Value = evalMode match {
    case EvalMode.LEGACY => CometEvalMode.LEGACY
    case EvalMode.TRY => CometEvalMode.TRY
    case EvalMode.ANSI => CometEvalMode.ANSI
  }

  def sumEvalMode(s: Sum): EvalMode.Value = s.evalMode
}
