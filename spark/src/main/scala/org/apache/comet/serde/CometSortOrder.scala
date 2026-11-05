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

import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, CreateNamedStruct, Descending, IsNull, Literal, NullsFirst, NullsLast, SortOrder}
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}

import org.apache.comet.serde.QueryPlanSerde.exprToProtoInternal

/**
 * The key of a Sort, TopK, Window, WindowGroupLimit or range partitioning.
 *
 * Arrow orders floats by IEEE 754 total order, so the native side normalizes a `FLOAT` or
 * `DOUBLE` key, and an array or struct key with a float at any depth, before comparing: NaN
 * payloads fold together and signed zeros tie, as in Spark's `SQLOrderingUtil`. Only the
 * comparison key is normalized; returned values keep their original NaN representation and zero
 * sign. So floats alone never make a key incompatible, in strict floating-point mode either.
 * Nested nulls require the root null placement to be independent of the comparison direction.
 *
 * Spark orders a null element or field below every other value, whatever the key's null order.
 * The native sort takes its place from the key's null order. For a nondefault root null order,
 * wrap the key as `struct(isnull(key), key)`: the first field controls root placement, while the
 * second uses the direction's default null order for nested values.
 * https://github.com/apache/datafusion-comet/issues/6476
 *
 * A `RANGE` window frame orders such a null above every other value, so `CometWindowExec`
 * declines a `RANGE` frame that has to find a row's peers over such a key.
 * https://github.com/apache/datafusion-comet/issues/6477
 */
object CometSortOrder extends CometExpressionSerde[SortOrder] {

  private val nondeterministicNestedKeyReason =
    "A nested sort key with nondefault root null placement must be deterministic: " +
      "the wrapper can evaluate the key twice, for isnull(key) and its value, " +
      "which could disagree"

  override def getUnsupportedReasons(): Seq[String] = Seq(nondeterministicNestedKeyReason)

  private def needsRootNullWrapper(expr: SortOrder): Boolean =
    canHoldNestedNull(expr.child.dataType) &&
      expr.nullOrdering != expr.direction.defaultNullOrdering

  override def getSupportLevel(expr: SortOrder): SupportLevel = {
    if (needsRootNullWrapper(expr) && !expr.child.deterministic) {
      Unsupported(Some(nondeterministicNestedKeyReason))
    } else {
      Compatible()
    }
  }

  /**
   * Whether a value of `dataType` can hold a null below its top level: an array element or a
   * struct field, at any depth. The key being null is not one: both engines place that by the
   * key's null order.
   */
  def canHoldNestedNull(dataType: DataType): Boolean = dataType match {
    case ArrayType(elementType, containsNull) => containsNull || canHoldNestedNull(elementType)
    case StructType(fields) => fields.exists(f => f.nullable || canHoldNestedNull(f.dataType))
    case _ => false
  }

  override def convert(
      expr: SortOrder,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val wrapped = if (needsRootNullWrapper(expr)) {
      CreateNamedStruct(
        Seq(Literal("root_null"), IsNull(expr.child), Literal("value"), expr.child))
    } else {
      expr.child
    }
    val childExpr = exprToProtoInternal(wrapped, inputs, binding)

    if (childExpr.isDefined) {
      val sortOrderBuilder = ExprOuterClass.SortOrder.newBuilder()

      sortOrderBuilder.setChild(childExpr.get)

      expr.direction match {
        case Ascending => sortOrderBuilder.setDirectionValue(0)
        case Descending => sortOrderBuilder.setDirectionValue(1)
      }

      val nullOrdering = if (needsRootNullWrapper(expr)) {
        expr.direction.defaultNullOrdering
      } else {
        expr.nullOrdering
      }
      nullOrdering match {
        case NullsFirst => sortOrderBuilder.setNullOrderingValue(0)
        case NullsLast => sortOrderBuilder.setNullOrderingValue(1)
      }

      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setSortOrder(sortOrderBuilder)
          .build())
    } else {
      None
    }
  }
}
