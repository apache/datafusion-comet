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

package org.apache.comet.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, NullIntolerant}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

case class StMakeEnvelope(
    xmin: Expression,
    ymin: Expression,
    xmax: Expression,
    ymax: Expression)
    extends Expression
    with NullIntolerant {
  override def dataType: DataType = StringType
  override def nullable: Boolean = true
  override def children: Seq[Expression] = Seq(xmin, ymin, xmax, ymax)
  override def eval(input: InternalRow): Any = {
    val xv = xmin.eval(input)
    val yv = ymin.eval(input)
    val xv2 = xmax.eval(input)
    val yv2 = ymax.eval(input)
    if (xv == null || yv == null || xv2 == null || yv2 == null) null
    else UTF8String.fromString(CometGeoFallback.makeEnvelope(
      xv.toString.toDouble, yv.toString.toDouble, xv2.toString.toDouble, yv2.toString.toDouble))
  }
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = ev
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression =
    copy(
      xmin = newChildren(0),
      ymin = newChildren(1),
      xmax = newChildren(2),
      ymax = newChildren(3))
}
