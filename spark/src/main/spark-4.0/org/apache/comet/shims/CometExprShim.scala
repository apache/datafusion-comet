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

import org.apache.comet.expressions.CometEvalMode
import org.apache.comet.serde.CommonStringExprs
import org.apache.comet.serde.ExprOuterClass.{BinaryOutputStyle, Expr}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types.{BinaryType, BooleanType, StringType}

/**
 * `CometExprShim` acts as a shim for parsing expressions from different Spark versions.
 */
trait CometExprShim extends CommonStringExprs {
    protected def evalMode(c: Cast): CometEvalMode.Value =
        CometEvalModeUtil.fromSparkEvalMode(c.evalMode)

    protected def binaryOutputStyle: BinaryOutputStyle = {
        SQLConf.get.getConf(SQLConf.BINARY_OUTPUT_STYLE).map(SQLConf.BinaryOutputStyle.withName) match {
        case Some(SQLConf.BinaryOutputStyle.UTF8) => BinaryOutputStyle.UTF8
        case Some(SQLConf.BinaryOutputStyle.BASIC) => BinaryOutputStyle.BASIC
        case Some(SQLConf.BinaryOutputStyle.BASE64) => BinaryOutputStyle.BASE64
        case Some(SQLConf.BinaryOutputStyle.HEX) => BinaryOutputStyle.HEX
        case _ => BinaryOutputStyle.HEX_DISCRETE
      }
    }

    def versionSpecificExprToProtoInternal(
        expr: Expression,
        inputs: Seq[Attribute],
        binding: Boolean): Option[Expr] = {
      expr match {
        case s: StaticInvoke
            if s.staticObject == classOf[StringDecode] &&
              s.dataType.isInstanceOf[StringType] &&
              s.functionName == "decode" &&
              s.arguments.size == 4 &&
              s.inputTypes == Seq(
                  BinaryType,
                  StringTypeWithCollation(supportsTrimCollation = true),
                  BooleanType,
                  BooleanType) =>
          val Seq(bin, charset, _, _) = s.arguments
          stringDecode(expr, charset, bin, inputs, binding)

        case _ => None
      }
    }
}

object CometEvalModeUtil {
    def fromSparkEvalMode(evalMode: EvalMode.Value): CometEvalMode.Value = evalMode match {
        case EvalMode.LEGACY => CometEvalMode.LEGACY
        case EvalMode.TRY => CometEvalMode.TRY
        case EvalMode.ANSI => CometEvalMode.ANSI
    }
}

