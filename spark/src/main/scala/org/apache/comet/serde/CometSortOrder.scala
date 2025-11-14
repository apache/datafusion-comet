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

import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, Descending, NullsFirst, NullsLast, SortOrder}
import org.apache.spark.sql.types._

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.QueryPlanSerde.exprToProtoInternal

object CometSortOrder extends CometExpressionSerde[SortOrder] {

  override def getSupportLevel(expr: SortOrder): SupportLevel = {

    def containsFloatingPoint(dt: DataType): Boolean = {
      dt match {
        case DataTypes.FloatType | DataTypes.DoubleType => true
        case ArrayType(elementType, _) => containsFloatingPoint(elementType)
        case StructType(fields) => fields.exists(f => containsFloatingPoint(f.dataType))
        case MapType(keyType, valueType, _) =>
          containsFloatingPoint(keyType) || containsFloatingPoint(valueType)
        case _ => false
      }
    }

    if (containsFloatingPoint(expr.child.dataType)) {
      Incompatible(Some(
        s"Sorting on floating-point is not 100% compatible with Spark. ${CometConf.COMPAT_GUIDE}"))
    } else {
      Compatible()
    }
  }

  override def convert(
      expr: SortOrder,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)

    if (childExpr.isDefined) {
      val sortOrderBuilder = ExprOuterClass.SortOrder.newBuilder()

      sortOrderBuilder.setChild(childExpr.get)

      expr.direction match {
        case Ascending => sortOrderBuilder.setDirectionValue(0)
        case Descending => sortOrderBuilder.setDirectionValue(1)
      }

      expr.nullOrdering match {
        case NullsFirst => sortOrderBuilder.setNullOrderingValue(0)
        case NullsLast => sortOrderBuilder.setNullOrderingValue(1)
      }

      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setSortOrder(sortOrderBuilder)
          .build())
    } else {
      withInfo(expr, expr.child)
      None
    }
  }
}
