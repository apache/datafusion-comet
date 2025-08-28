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
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.CometExpressionSerde
import org.apache.spark.sql.catalyst.expressions._

/**
 * `CometExprShim` acts as a shim for for parsing expressions from different Spark versions.
 */
trait CometExprShim extends CommonStringExprs {
    // Version specific expression serde map.
    protected val versionSpecificExprSerdeMap: Map[Class[_ <: Expression], CometExpressionSerde[_]] = Map.empty

    /**
     * Returns a tuple of expressions for the `unhex` function.
     */
    protected def unhexSerde(unhex: Unhex): (Expression, Expression) = {
        (unhex.child, Literal(unhex.failOnError))
    }

    protected def evalMode(c: Cast): CometEvalMode.Value =
        CometEvalModeUtil.fromSparkEvalMode(c.evalMode)

    def versionSpecificExprToProtoInternal(
        expr: Expression,
        inputs: Seq[Attribute],
        binding: Boolean): Option[Expr] = {
      expr match {
        case s: StringDecode =>
          // Right child is the encoding expression.
          stringDecode(expr, s.charset, s.bin, inputs, binding)

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
