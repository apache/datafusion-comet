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

import org.apache.spark.sql.catalyst.expressions.{Ascending, Expression, ExpressionSet, SortOrder}
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampNTZType}

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.OperatorOuterClass.{JoinType, Operator}
import org.apache.comet.serde.QueryPlanSerde.exprToProto

object CometSortMergeJoin extends CometOperatorSerde[SortMergeJoinExec] {
  override def enabledConfig: Option[ConfigEntry[Boolean]] = Some(
    CometConf.COMET_EXEC_SORT_MERGE_JOIN_ENABLED)

  override def convert(
      join: SortMergeJoinExec,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = {
    // `requiredOrders` and `getKeyOrdering` are copied from Spark's SortMergeJoinExec.
    def requiredOrders(keys: Seq[Expression]): Seq[SortOrder] = {
      keys.map(SortOrder(_, Ascending))
    }

    def getKeyOrdering(
        keys: Seq[Expression],
        childOutputOrdering: Seq[SortOrder]): Seq[SortOrder] = {
      val requiredOrdering = requiredOrders(keys)
      if (SortOrder.orderingSatisfies(childOutputOrdering, requiredOrdering)) {
        keys.zip(childOutputOrdering).map { case (key, childOrder) =>
          val sameOrderExpressionsSet = ExpressionSet(childOrder.children) - key
          SortOrder(key, Ascending, sameOrderExpressionsSet.toSeq)
        }
      } else {
        requiredOrdering
      }
    }

    if (join.condition.isDefined &&
      !CometConf.COMET_EXEC_SORT_MERGE_JOIN_WITH_JOIN_FILTER_ENABLED
        .get(join.conf)) {
      withInfo(
        join,
        s"${CometConf.COMET_EXEC_SORT_MERGE_JOIN_WITH_JOIN_FILTER_ENABLED.key} is not enabled",
        join.condition.get)
      return None
    }

    val condition = join.condition.map { cond =>
      val condProto = exprToProto(cond, join.left.output ++ join.right.output)
      if (condProto.isEmpty) {
        withInfo(join, cond)
        return None
      }
      condProto.get
    }

    val joinType = join.joinType match {
      case Inner => JoinType.Inner
      case LeftOuter => JoinType.LeftOuter
      case RightOuter => JoinType.RightOuter
      case FullOuter => JoinType.FullOuter
      case LeftSemi => JoinType.LeftSemi
      case LeftAnti => JoinType.LeftAnti
      case _ =>
        // Spark doesn't support other join types
        withInfo(join, s"Unsupported join type ${join.joinType}")
        return None
    }

    // Checks if the join keys are supported by DataFusion SortMergeJoin.
    val errorMsgs = join.leftKeys.flatMap { key =>
      if (!supportedSortMergeJoinEqualType(key.dataType)) {
        Some(s"Unsupported join key type ${key.dataType} on key: ${key.sql}")
      } else {
        None
      }
    }

    if (errorMsgs.nonEmpty) {
      withInfo(join, errorMsgs.flatten.mkString("\n"))
      return None
    }

    val leftKeys = join.leftKeys.map(exprToProto(_, join.left.output))
    val rightKeys = join.rightKeys.map(exprToProto(_, join.right.output))

    val sortOptions = getKeyOrdering(join.leftKeys, join.left.outputOrdering)
      .map(exprToProto(_, join.left.output))

    if (sortOptions.forall(_.isDefined) &&
      leftKeys.forall(_.isDefined) &&
      rightKeys.forall(_.isDefined) &&
      childOp.nonEmpty) {
      val joinBuilder = OperatorOuterClass.SortMergeJoin
        .newBuilder()
        .setJoinType(joinType)
        .addAllSortOptions(sortOptions.map(_.get).asJava)
        .addAllLeftJoinKeys(leftKeys.map(_.get).asJava)
        .addAllRightJoinKeys(rightKeys.map(_.get).asJava)
      condition.map(joinBuilder.setCondition)
      Some(builder.setSortMergeJoin(joinBuilder).build())
    } else {
      val allExprs: Seq[Expression] = join.leftKeys ++ join.rightKeys
      withInfo(join, allExprs: _*)
      None
    }

  }

  /**
   * Returns true if given datatype is supported as a key in DataFusion sort merge join.
   */
  private def supportedSortMergeJoinEqualType(dataType: DataType): Boolean = dataType match {
    case _: ByteType | _: ShortType | _: IntegerType | _: LongType | _: FloatType |
        _: DoubleType | _: StringType | _: DateType | _: DecimalType | _: BooleanType =>
      true
    case TimestampNTZType => true
    case _ => false
  }

}
