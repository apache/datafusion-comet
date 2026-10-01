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

package org.apache.comet.local

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Descending, NamedExpression, NullsFirst, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.RangePartitioning
import org.apache.spark.sql.execution.{CollectLimitExec, SortExec, SparkPlan, TakeOrderedAndProjectExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.types.{DoubleType, FloatType}

import org.apache.comet.CometConf
import org.apache.comet.serde.LocalOuterClass.{LocalOutput, LocalSort}
import org.apache.comet.serde.QueryPlanSerde.exprToProto

/** Terminal global ordering/limit, applied once to the entire native query. */
private[local] object LocalOutputPlanner {
  import LocalParquetPlanner.{nativeOnly, supportedExpression}

  def plan(root: SparkPlan, session: SparkSession): Option[LocalQuerySpec] = {
    // Spark's physical limit includes its offset. Our wire fetch excludes it.
    val (body, limit, skip) = root match {
      case collect: CollectLimitExec
          if CometConf.COMET_EXEC_COLLECT_LIMIT_ENABLED.get(root.conf) =>
        (collect.child, collect.limit, collect.offset)
      case other => (other, -1, 0)
    }
    if (skip < 0 || limit < -1 || (limit >= 0 && skip > limit)) return None
    val terminal = LocalOutput.newBuilder().setSkip(skip.toLong)
    if (limit >= 0) terminal.setFetch((limit - skip).toLong)
    val extracted: Option[(SparkPlan, Seq[SortOrder], Seq[NamedExpression])] = body match {
      case top: TakeOrderedAndProjectExec
          if body == root &&
            CometConf.COMET_EXEC_TAKE_ORDERED_AND_PROJECT_ENABLED.get(root.conf) &&
            top.limit >= 0 && top.offset >= 0 && top.offset <= top.limit =>
        terminal.setSkip(top.offset.toLong).setFetch((top.limit - top.offset).toLong)
        Some((top.child, top.sortOrder, top.projectList))
      case sort: SortExec if sort.global =>
        sort.child match {
          case exchange: ShuffleExchangeExec =>
            exchange.outputPartitioning match {
              case range: RangePartitioning
                  if range.ordering.size == sort.sortOrder.size &&
                    range.ordering.zip(sort.sortOrder).forall { case (a, b) =>
                      a.semanticEquals(b)
                    } =>
                Some((exchange.child, sort.sortOrder, Nil))
              case _ => None
            }
          case child if child.outputPartitioning.numPartitions == 1 =>
            Some((child, sort.sortOrder, Nil))
          case _ => None
        }
      case other if body != root => Some((other, Nil, Nil))
      case _ => None
    }
    extracted.flatMap { case (input, orders, result) =>
      if ((orders.nonEmpty && !CometConf.COMET_EXEC_SORT_ENABLED.get(root.conf)) ||
        (result.nonEmpty && !CometConf.COMET_EXEC_PROJECT_ENABLED.get(root.conf)) ||
        !orders.forall(s =>
          supportedExpression(s.child) &&
            s.child.dataType != FloatType && s.child.dataType != DoubleType) ||
        !result.forall(supportedExpression) || root.output.isEmpty || root.output.size > 1024) {
        None
      } else {
        val keys = orders.map(s =>
          exprToProto(s.child, input.output).map { child =>
            LocalSort
              .newBuilder()
              .setChild(child)
              .setDescending(s.direction == Descending)
              .setNullsFirst(s.nullOrdering == NullsFirst)
              .build()
          })
        val projection = result.map(exprToProto(_, input.output))
        if (!keys.forall(_.isDefined) || !projection.forall(_.isDefined)) { None }
        else {
          val proto = terminal
            .addAllOrders(keys.flatten.asJava)
            .addAllResult(projection.flatten.asJava)
            .build()
          if (!nativeOnly(proto)) { None }
          else {
            LocalJoinPlanner
              .plan(input, session)
              .orElse(LocalAggregatePlanner.plan(input, session))
              .orElse(LocalParquetPlanner.plan(input, session))
              .flatMap {
                case scan: LocalParquetSpec =>
                  Some(scan.copy(columns = root.output.size, terminal = proto.toByteArray))
                case join: LocalJoinSpec =>
                  Some(join.copy(columns = root.output.size, terminal = proto.toByteArray))
                case _ => None
              }
          }
        }
      }
    }
  }
}
