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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.optimizer.BuildRight
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins.ShuffledHashJoinExec
import org.apache.spark.sql.types.{DoubleType, FloatType}

import org.apache.comet.CometConf
import org.apache.comet.serde.LocalOuterClass.LocalJoin
import org.apache.comet.serde.OperatorOuterClass.{JoinType, Operator, SparkFilePartition}
import org.apache.comet.serde.QueryPlanSerde.exprToProto

/** Admit one complete shuffled equi-join; both exchanges become native channels. */
private[local] object LocalJoinPlanner {
  import LocalParquetPlanner.{nativeOnly, supportedExpression}

  def plan(root: SparkPlan, session: SparkSession): Option[LocalJoinSpec] = {
    val (body, output) = root match {
      case project: ProjectExec if CometConf.COMET_EXEC_PROJECT_ENABLED.get(project.conf) =>
        (project.child, project.projectList)
      case other => (other, other.output)
    }
    body match {
      case join: ShuffledHashJoinExec
          if CometConf.COMET_EXEC_HASH_JOIN_ENABLED.get(join.conf) &&
            join.condition.isEmpty && join.leftKeys.nonEmpty &&
            join.leftKeys.size == join.rightKeys.size &&
            output.nonEmpty && output.size <= 1024 && output.forall(supportedExpression) =>
        val kind = join.joinType match {
          case Inner => Some(JoinType.Inner)
          case LeftOuter => Some(JoinType.LeftOuter)
          case RightOuter => Some(JoinType.RightOuter)
          case FullOuter => Some(JoinType.FullOuter)
          case LeftSemi => Some(JoinType.LeftSemi)
          case LeftAnti => Some(JoinType.LeftAnti)
          case _ => None
        }
        def input(child: SparkPlan, keys: Seq[Expression]): Option[(LocalParquetSpec, Int)] =
          child match {
            case exchange: ShuffleExchangeExec =>
              exchange.outputPartitioning match {
                case hash: HashPartitioning
                    if hash.numPartitions >= 1 && hash.numPartitions <= 1024 &&
                      hash.expressions.size == keys.size &&
                      hash.expressions.zip(keys).forall { case (a, b) => a.semanticEquals(b) } =>
                  LocalParquetPlanner.plan(exchange.child, session).map((_, hash.numPartitions))
                case _ => None
              }
            case _ => None
          }
        val keysSupported = join.leftKeys.zip(join.rightKeys).forall { case (left, right) =>
          Seq(left, right).forall(e =>
            e.isInstanceOf[AttributeReference] &&
              supportedExpression(e) && e.dataType != FloatType && e.dataType != DoubleType) &&
          left.dataType == right.dataType
        }
        if (!keysSupported) { None }
        else {
          for {
            joinType <- kind
            left <- input(join.left, join.leftKeys)
            right <- input(join.right, join.rightKeys)
            if left._2 == right._2
            leftKeys = join.leftKeys.map(exprToProto(_, join.left.output))
            rightKeys = join.rightKeys.map(exprToProto(_, join.right.output))
            result = output.map(exprToProto(_, join.output))
            if (leftKeys ++ rightKeys ++ result).forall(_.isDefined)
            proto = LocalJoin
              .newBuilder()
              .setLeft(Operator.parseFrom(left._1.plan))
              .setRight(Operator.parseFrom(right._1.plan))
              .addAllLeftFiles(
                left._1.filePartitions.map(SparkFilePartition.parseFrom).toSeq.asJava)
              .addAllRightFiles(
                right._1.filePartitions.map(SparkFilePartition.parseFrom).toSeq.asJava)
              .addAllLeftKeys(leftKeys.flatten.asJava)
              .addAllRightKeys(rightKeys.flatten.asJava)
              .setJoinType(joinType)
              .setPartitions(left._2)
              .setBuildRight(join.buildSide == BuildRight)
              .addAllResult(result.flatten.asJava)
              .build()
            if nativeOnly(proto)
          } yield LocalJoinSpec(
            proto.toByteArray,
            left._1.batchSize,
            output.size,
            left._1.rowFilterPushdown,
            left._1.memoryLimit,
            left._1.spillEnabled)
        }
      case _ => None
    }
  }
}
