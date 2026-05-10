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

import org.apache.spark.sql.catalyst.expressions.{Attribute, ScalaUDF}

import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.ExprOuterClass.Expr

/**
 * Routes scalar `ScalaUDF` expressions (user-registered Scala and Java UDFs) through the
 * Arrow-direct codegen dispatcher. `ScalaUDF.doGenCode` emits compilable Java that invokes the
 * user function via `ctx.addReferenceObj`, so the codegen path picks it up unchanged: we
 * serialize the bound tree, the closure serializer carries the function reference across the
 * wire, and the Janino-compiled kernel loads the function and invokes it in a tight batch loop.
 *
 * Not covered here:
 *   - Aggregate UDFs (`ScalaAggregator`, `TypedImperativeAggregate`, old UDAF API) - different
 *     bridge contract.
 *   - Table UDFs (`UserDefinedTableFunction`) - generator shape; `canHandle` rejects.
 *   - Python / Pandas UDFs - different runtime.
 *   - Hive UDFs (`HiveGenericUDF` / `HiveSimpleUDF`) - separate expression classes; would need
 *     their own serde.
 *
 * Mode knob: `auto` prefers codegen because `ScalaUDF` has no native fallback; `disabled` returns
 * `None` and the plan falls back to Spark.
 */
object CometScalaUDF extends CometExpressionSerde[ScalaUDF] {

  override def convert(expr: ScalaUDF, inputs: Seq[Attribute], binding: Boolean): Option[Expr] = {
    CodegenDispatchSerdeHelpers.pickWithMode(
      viaCodegen =
        () => CodegenDispatchSerdeHelpers.buildJvmUdfExpr(expr, inputs, binding, expr.dataType),
      viaNonCodegen = () => {
        withInfo(
          expr,
          "codegen dispatch disabled; ScalaUDF has no native path so the plan falls back to Spark")
        None
      },
      preferCodegenInAuto = true)
  }
}
