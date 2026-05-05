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
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, optExprWithInfo, scalarFunctionExprToProtoWithReturnType, supportedScalarSortElementType}

object CometMapSort extends CometExpressionSerde[MapSort] {

  override def getIncompatibleReasons(): Seq[String] =
    Seq(
      "MapSort on floating-point keys is not 100% compatible with Spark when " +
        s"`${CometConf.COMET_EXEC_STRICT_FLOATING_POINT.key}=true`.")

  override def getUnsupportedReasons(): Seq[String] =
    Seq("MapSort is unsupported for non-scalar key types (struct, array, map, etc.).")

  override def getSupportLevel(expr: MapSort): SupportLevel = {
    val keyType = expr.dataType.asInstanceOf[MapType].keyType
    if (!supportedScalarSortElementType(keyType)) {
      Unsupported(Some(s"MapSort on map with key type $keyType is not supported"))
    } else if (CometConf.COMET_EXEC_STRICT_FLOATING_POINT.get() &&
      SupportLevel.containsFloatingPoint(keyType)) {
      Incompatible(
        Some(
          "MapSort on floating-point key is not 100% compatible with Spark, and Comet is " +
            s"running with ${CometConf.COMET_EXEC_STRICT_FLOATING_POINT.key}=true. " +
            s"${CometConf.COMPAT_GUIDE}"))
    } else {
      Compatible(None)
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
    optExprWithInfo(mapSortExpr, expr, expr.child)
  }
}
