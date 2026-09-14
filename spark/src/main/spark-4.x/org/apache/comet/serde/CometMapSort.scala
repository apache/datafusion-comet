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

import org.apache.spark.sql.catalyst.expressions.{Attribute, MapSort}
import org.apache.spark.sql.types.MapType

import org.apache.comet.CometConf
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, scalarFunctionExprToProtoWithReturnType, supportedScalarSortElementType}

// Key types without a native implementation can still run Spark's generated code in-pipeline.
// Spark rejects collated-string map keys by default, but they reach MapSort when
// spark.sql.collation.allowInMapKeys=true and the map is built from dispatcher-supported inputs;
// those expressions take the same Unsupported -> dispatcher route. A scan carrying a collated
// map schema may still be rejected independently by the scan's schema support checks.
object CometMapSort extends CometExpressionSerde[MapSort] with CodegenDispatchFallback {

  override def getIncompatibleReasons(): Seq[String] =
    Seq(
      "MapSort on floating-point keys is not 100% compatible with Spark when " +
        s"`${CometConf.COMET_EXEC_STRICT_FLOATING_POINT.key}=true`.")

  override def getUnsupportedReasons(): Seq[String] =
    Seq(
      "MapSort with an orderable key type outside native scalar coverage, including array, " +
        "struct, interval, and non-default-collated string keys, has no native implementation.")

  override def getSupportLevel(expr: MapSort): SupportLevel = {
    val keyType = expr.dataType.asInstanceOf[MapType].keyType
    if (!supportedScalarSortElementType(keyType)) {
      Unsupported(Some(s"MapSort with key type $keyType has no native implementation"))
    } else {
      SupportLevel
        .strictFloatingPointReason(keyType, "MapSort on floating-point key")
        .map(reason => Incompatible(Some(reason)))
        .getOrElse(Compatible(None))
    }
  }

  override def convert(
      expr: MapSort,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)
    val mapSortExpr = scalarFunctionExprToProtoWithReturnType(
      "map_sort",
      expr.dataType,
      failOnError = false,
      childExpr)
    mapSortExpr
  }
}
