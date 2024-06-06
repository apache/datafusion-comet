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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{Average, Sum}

/**
 * `CometExprShim` acts as a shim for for parsing expressions from different Spark versions.
 */
trait CometExprShim {
    /**
     * Returns a tuple of expressions for the `unhex` function.
     */
    def unhexSerde(unhex: Unhex): (Expression, Expression) = {
        (unhex.child, Literal(unhex.failOnError))
    }

    def evalMode(expr: Add): CometEvalMode.Value =
        CometEvalModeUtil.fromSparkEvalMode(expr.evalMode)

    def evalMode(expr: Subtract): CometEvalMode.Value =
        CometEvalModeUtil.fromSparkEvalMode(expr.evalMode)

    def evalMode(expr: Multiply): CometEvalMode.Value =
        CometEvalModeUtil.fromSparkEvalMode(expr.evalMode)

    def evalMode(expr: Divide): CometEvalMode.Value =
        CometEvalModeUtil.fromSparkEvalMode(expr.evalMode)

    def evalMode(expr: IntegralDivide): CometEvalMode.Value =
        CometEvalModeUtil.fromSparkEvalMode(expr.evalMode)

    def evalMode(expr: Remainder): CometEvalMode.Value =
        CometEvalModeUtil.fromSparkEvalMode(expr.evalMode)

    def evalMode(expr: Pmod): CometEvalMode.Value =
        CometEvalModeUtil.fromSparkEvalMode(expr.evalMode)

    def evalMode(expr: Sum): CometEvalMode.Value =
        CometEvalModeUtil.fromSparkEvalMode(expr.evalMode)

    def evalMode(expr: Average): CometEvalMode.Value =
        CometEvalModeUtil.fromSparkEvalMode(expr.evalMode)

    def evalMode(c: Cast): CometEvalMode.Value =
        CometEvalModeUtil.fromSparkEvalMode(c.evalMode)

    def evalMode(r: Round): CometEvalMode.Value = CometEvalMode.fromBoolean(r.ansiEnabled)

    def evalMode(r: BRound): CometEvalMode.Value = CometEvalMode.fromBoolean(r.ansiEnabled)
}

object CometEvalModeUtil {
    def fromSparkEvalMode(evalMode: EvalMode.Value): CometEvalMode.Value = evalMode match {
        case EvalMode.LEGACY => CometEvalMode.LEGACY
        case EvalMode.TRY => CometEvalMode.TRY
        case EvalMode.ANSI => CometEvalMode.ANSI
    }
}
