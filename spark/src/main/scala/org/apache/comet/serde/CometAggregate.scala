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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.{Final, Partial}
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec, ObjectHashAggregateExec}
import org.apache.spark.sql.types.MapType

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.OperatorOuterClass.{AggregateMode => CometAggregateMode, Operator}
import org.apache.comet.serde.QueryPlanSerde.{aggExprToProto, exprToProto}

trait CometBaseAggregate {

  def doConvert(
      aggregate: BaseAggregateExec,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = {
    val groupingExpressions = aggregate.groupingExpressions
    val aggregateExpressions = aggregate.aggregateExpressions
    val aggregateAttributes = aggregate.aggregateAttributes
    val resultExpressions = aggregate.resultExpressions
    val child = aggregate.child

    if (groupingExpressions.isEmpty && aggregateExpressions.isEmpty) {
      withInfo(aggregate, "No group by or aggregation")
      return None
    }

    // Aggregate expressions with filter are not supported yet.
    if (aggregateExpressions.exists(_.filter.isDefined)) {
      withInfo(aggregate, "Aggregate expression with filter is not supported")
      return None
    }

    if (groupingExpressions.exists(expr =>
        expr.dataType match {
          case _: MapType => true
          case _ => false
        })) {
      withInfo(aggregate, "Grouping on map types is not supported")
      return None
    }

    val groupingExprsWithInput =
      groupingExpressions.map(expr => expr.name -> exprToProto(expr, child.output))

    val emptyExprs = groupingExprsWithInput.collect {
      case (expr, proto) if proto.isEmpty => expr
    }

    if (emptyExprs.nonEmpty) {
      withInfo(aggregate, s"Unsupported group expressions: ${emptyExprs.mkString(", ")}")
      return None
    }

    val groupingExprs = groupingExprsWithInput.map(_._2)

    // In some of the cases, the aggregateExpressions could be empty.
    // For example, if the aggregate functions only have group by or if the aggregate
    // functions only have distinct aggregate functions:
    //
    // SELECT COUNT(distinct col2), col1 FROM test group by col1
    //  +- HashAggregate (keys =[col1# 6], functions =[count (distinct col2#7)] )
    //    +- Exchange hashpartitioning (col1#6, 10), ENSURE_REQUIREMENTS, [plan_id = 36]
    //      +- HashAggregate (keys =[col1#6], functions =[partial_count (distinct col2#7)] )
    //        +- HashAggregate (keys =[col1#6, col2#7], functions =[] )
    //          +- Exchange hashpartitioning (col1#6, col2#7, 10), ENSURE_REQUIREMENTS, ...
    //            +- HashAggregate (keys =[col1#6, col2#7], functions =[] )
    //              +- FileScan parquet spark_catalog.default.test[col1#6, col2#7] ......
    // If the aggregateExpressions is empty, we only want to build groupingExpressions,
    // and skip processing of aggregateExpressions.
    if (aggregateExpressions.isEmpty) {
      val hashAggBuilder = OperatorOuterClass.HashAggregate.newBuilder()
      hashAggBuilder.addAllGroupingExprs(groupingExprs.map(_.get).asJava)
      val attributes = groupingExpressions.map(_.toAttribute) ++ aggregateAttributes
      val resultExprs = resultExpressions.map(exprToProto(_, attributes))
      if (resultExprs.exists(_.isEmpty)) {
        withInfo(
          aggregate,
          s"Unsupported result expressions found in: $resultExpressions",
          resultExpressions: _*)
        return None
      }
      hashAggBuilder.addAllResultExprs(resultExprs.map(_.get).asJava)
      Some(builder.setHashAgg(hashAggBuilder).build())
    } else {
      val modes = aggregateExpressions.map(_.mode).distinct

      if (modes.size != 1) {
        // This shouldn't happen as all aggregation expressions should share the same mode.
        // Fallback to Spark nevertheless here.
        withInfo(aggregate, "All aggregate expressions do not have the same mode")
        return None
      }

      val mode = modes.head match {
        case Partial => CometAggregateMode.Partial
        case Final => CometAggregateMode.Final
        case _ =>
          withInfo(aggregate, s"Unsupported aggregation mode ${modes.head}")
          return None
      }

      // In final mode, the aggregate expressions are bound to the output of the
      // child and partial aggregate expressions buffer attributes produced by partial
      // aggregation. This is done in Spark `HashAggregateExec` internally. In Comet,
      // we don't have to do this because we don't use the merging expression.
      val binding = mode != CometAggregateMode.Final
      // `output` is only used when `binding` is true (i.e., non-Final)
      val output = child.output

      val aggExprs =
        aggregateExpressions.map(aggExprToProto(_, output, binding, aggregate.conf))
      if (childOp.nonEmpty && groupingExprs.forall(_.isDefined) &&
        aggExprs.forall(_.isDefined)) {
        val hashAggBuilder = OperatorOuterClass.HashAggregate.newBuilder()
        hashAggBuilder.addAllGroupingExprs(groupingExprs.map(_.get).asJava)
        hashAggBuilder.addAllAggExprs(aggExprs.map(_.get).asJava)
        if (mode == CometAggregateMode.Final) {
          val attributes = groupingExpressions.map(_.toAttribute) ++ aggregateAttributes
          val resultExprs = resultExpressions.map(exprToProto(_, attributes))
          if (resultExprs.exists(_.isEmpty)) {
            withInfo(
              aggregate,
              s"Unsupported result expressions found in: $resultExpressions",
              resultExpressions: _*)
            return None
          }
          hashAggBuilder.addAllResultExprs(resultExprs.map(_.get).asJava)
        }
        hashAggBuilder.setModeValue(mode.getNumber)
        Some(builder.setHashAgg(hashAggBuilder).build())
      } else {
        val allChildren: Seq[Expression] =
          groupingExpressions ++ aggregateExpressions ++ aggregateAttributes
        withInfo(aggregate, allChildren: _*)
        None
      }
    }

  }

}

object CometHashAggregate extends CometOperatorSerde[HashAggregateExec] with CometBaseAggregate {

  override def enabledConfig: Option[ConfigEntry[Boolean]] = Some(
    CometConf.COMET_EXEC_AGGREGATE_ENABLED)

  override def convert(
      aggregate: HashAggregateExec,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = {
    doConvert(aggregate, builder, childOp: _*)
  }
}

object CometObjectHashAggregate
    extends CometOperatorSerde[ObjectHashAggregateExec]
    with CometBaseAggregate {

  override def enabledConfig: Option[ConfigEntry[Boolean]] = Some(
    CometConf.COMET_EXEC_AGGREGATE_ENABLED)

  override def convert(
      aggregate: ObjectHashAggregateExec,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = {
    doConvert(aggregate, builder, childOp: _*)
  }
}
