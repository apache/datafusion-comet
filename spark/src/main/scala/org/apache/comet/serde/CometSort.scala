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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, Descending, NullsFirst, NullsLast, SortOrder}
import org.apache.spark.sql.execution.SortExec
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, MapType, StructType}

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.{exprToProto, exprToProtoInternal, supportedSortType}

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

object CometSort extends CometOperatorSerde[SortExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_EXEC_SORT_ENABLED)

  override def convert(
      op: SortExec,
      builder: Operator.Builder,
      childOp: Operator*): Option[OperatorOuterClass.Operator] = {
    if (!supportedSortType(op, op.sortOrder)) {
      withInfo(op, "Unsupported data type in sort expressions")
      return None
    }

    val sortOrders = op.sortOrder.map(exprToProto(_, op.child.output))

    if (sortOrders.forall(_.isDefined) && childOp.nonEmpty) {
      val sortBuilder = OperatorOuterClass.Sort
        .newBuilder()
        .addAllSortOrders(sortOrders.map(_.get).asJava)
      Some(builder.setSort(sortBuilder).build())
    } else {
      withInfo(op, "sort order not supported", op.sortOrder: _*)
      None
    }
  }

}
