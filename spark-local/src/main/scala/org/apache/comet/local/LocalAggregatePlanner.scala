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
import org.apache.spark.sql.catalyst.expressions.aggregate.{Count, Final, Max, Min, Partial}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, SinglePartition}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.types.{DoubleType, FloatType}

import org.apache.comet.CometConf
import org.apache.comet.serde.LocalOuterClass.LocalAggregate
import org.apache.comet.serde.QueryPlanSerde.{aggExprToProto, exprToProto}

/** Recognize an entire Spark aggregation, then discard its task/buffer/exchange boundaries. */
private[local] object LocalAggregatePlanner {
  import LocalParquetPlanner.{nativeOnly, supportedExpression}

  def plan(root: SparkPlan, session: SparkSession): Option[LocalParquetSpec] = root match {
    case last: HashAggregateExec
        if CometConf.COMET_EXEC_AGGREGATE_ENABLED.get(last.conf) &&
          last.aggregateExpressions.nonEmpty &&
          last.aggregateExpressions.forall(a => a.mode == Final && !a.isDistinct) =>
      val input = last.child match {
        case exchange: ShuffleExchangeExec =>
          exchange.child match {
            case first: HashAggregateExec =>
              exchange.outputPartitioning match {
                case hash: HashPartitioning
                    if first.groupingExpressions.nonEmpty &&
                      hash.expressions.size == first.groupingExpressions.size &&
                      hash.expressions.zip(first.groupingExpressions).forall {
                        case (key, group) =>
                          key.semanticEquals(group.toAttribute)
                      } =>
                  Some((first, hash.numPartitions))
                case SinglePartition if first.groupingExpressions.isEmpty => Some((first, 1))
                case _ => None
              }
            case _ => None
          }
        case first: HashAggregateExec if first.outputPartitioning.numPartitions == 1 =>
          Some((first, 1))
        case _ => None
      }
      input.flatMap { case (first, partitions) =>
        val paired = first.aggregateExpressions.size == last.aggregateExpressions.size &&
          first.aggregateExpressions.zip(last.aggregateExpressions).forall { case (a, b) =>
            a.mode == Partial && !a.isDistinct && a.resultId == b.resultId &&
            a.aggregateFunction.semanticEquals(b.aggregateFunction)
          }
        val keysMatch = first.groupingExpressions.size == last.groupingExpressions.size &&
          first.groupingExpressions.zip(last.groupingExpressions).forall { case (a, b) =>
            a.toAttribute.semanticEquals(b)
          }
        val supported = first.aggregateExpressions.forall { a =>
          val functionSupported = a.aggregateFunction match {
            case _: Count | _: Min | _: Max => true
            case _ => false
          }
          functionSupported && a.aggregateFunction.children.forall(supportedExpression) &&
          a.filter.forall(supportedExpression) &&
          a.dataType != FloatType && a.dataType != DoubleType
        }
        if (!paired || !keysMatch || !supported || partitions < 1 || partitions > 1024 ||
          !first.groupingExpressions.forall(e =>
            supportedExpression(e) &&
              e.dataType != FloatType && e.dataType != DoubleType) ||
          !last.resultExpressions.forall(supportedExpression)) { None }
        else {
          val grouping = first.groupingExpressions.map(exprToProto(_, first.child.output))
          val aggregates = first.aggregateExpressions.map(
            aggExprToProto(_, first.child.output, binding = true, conf = first.conf))
          val naturalOutput =
            last.groupingExpressions.map(_.toAttribute) ++ last.aggregateAttributes
          val result = last.resultExpressions.map(exprToProto(_, naturalOutput))
          if (!grouping.forall(_.isDefined) || !aggregates.forall(_.isDefined) ||
            !result.forall(_.isDefined)) { None }
          else {
            val aggregate = LocalAggregate
              .newBuilder()
              .addAllGrouping(grouping.flatten.asJava)
              .addAllAggregates(aggregates.flatten.asJava)
              .addAllResult(result.flatten.asJava)
              .setPartitions(partitions)
              .build()
            if (!nativeOnly(aggregate) || last.output.isEmpty || last.output.size > 1024) { None }
            else {
              LocalParquetPlanner.plan(first.child, session, allowEmptyOutput = true).map {
                input =>
                  input.copy(aggregate = aggregate.toByteArray, columns = last.output.size)
              }
            }
          }
        }
      }
    case _ => None
  }
}
