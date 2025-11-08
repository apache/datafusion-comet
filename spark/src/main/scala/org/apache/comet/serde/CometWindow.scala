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

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, SortOrder, WindowExpression}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.window.WindowExec

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.{exprToProto, windowExprToProto}

object CometWindow extends CometOperatorSerde[WindowExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] = Some(
    CometConf.COMET_EXEC_WINDOW_ENABLED)

  override def convert(
      op: WindowExec,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = {
    val output = op.child.output

    val winExprs: Array[WindowExpression] = op.windowExpression.flatMap { expr =>
      expr match {
        case alias: Alias =>
          alias.child match {
            case winExpr: WindowExpression =>
              Some(winExpr)
            case _ =>
              None
          }
        case _ =>
          None
      }
    }.toArray

    if (winExprs.length != op.windowExpression.length) {
      withInfo(op, "Unsupported window expression(s)")
      return None
    }

    if (op.partitionSpec.nonEmpty && op.orderSpec.nonEmpty &&
      !validatePartitionAndSortSpecsForWindowFunc(op.partitionSpec, op.orderSpec, op)) {
      return None
    }

    val windowExprProto = winExprs.map(windowExprToProto(_, output, op.conf))
    val partitionExprs = op.partitionSpec.map(exprToProto(_, op.child.output))

    val sortOrders = op.orderSpec.map(exprToProto(_, op.child.output))

    if (windowExprProto.forall(_.isDefined) && partitionExprs.forall(_.isDefined)
      && sortOrders.forall(_.isDefined)) {
      val windowBuilder = OperatorOuterClass.Window.newBuilder()
      windowBuilder.addAllWindowExpr(windowExprProto.map(_.get).toIterable.asJava)
      windowBuilder.addAllPartitionByList(partitionExprs.map(_.get).asJava)
      windowBuilder.addAllOrderByList(sortOrders.map(_.get).asJava)
      Some(builder.setWindow(windowBuilder).build())
    } else {
      None
    }

  }

  private def validatePartitionAndSortSpecsForWindowFunc(
      partitionSpec: Seq[Expression],
      orderSpec: Seq[SortOrder],
      op: SparkPlan): Boolean = {
    if (partitionSpec.length != orderSpec.length) {
      return false
    }

    val partitionColumnNames = partitionSpec.collect {
      case a: AttributeReference => a.name
      case other =>
        withInfo(op, s"Unsupported partition expression: ${other.getClass.getSimpleName}")
        return false
    }

    val orderColumnNames = orderSpec.collect { case s: SortOrder =>
      s.child match {
        case a: AttributeReference => a.name
        case other =>
          withInfo(op, s"Unsupported sort expression: ${other.getClass.getSimpleName}")
          return false
      }
    }

    if (partitionColumnNames.zip(orderColumnNames).exists { case (partCol, orderCol) =>
        partCol != orderCol
      }) {
      withInfo(op, "Partitioning and sorting specifications must be the same.")
      return false
    }

    true
  }

}
