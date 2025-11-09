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
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashJoin, ShuffledHashJoinExec}

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.OperatorOuterClass.{BuildSide, JoinType, Operator}
import org.apache.comet.serde.QueryPlanSerde.exprToProto

trait CometHashJoin {

  def doConvert(
      join: HashJoin,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = {
    // `HashJoin` has only two implementations in Spark, but we check the type of the join to
    // make sure we are handling the correct join type.
    if (!(CometConf.COMET_EXEC_HASH_JOIN_ENABLED.get(join.conf) &&
        join.isInstanceOf[ShuffledHashJoinExec]) &&
      !(CometConf.COMET_EXEC_BROADCAST_HASH_JOIN_ENABLED.get(join.conf) &&
        join.isInstanceOf[BroadcastHashJoinExec])) {
      withInfo(join, s"Invalid hash join type ${join.nodeName}")
      return None
    }

    if (join.buildSide == BuildRight && join.joinType == LeftAnti) {
      // https://github.com/apache/datafusion-comet/issues/457
      withInfo(join, "BuildRight with LeftAnti is not supported")
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

    val leftKeys = join.leftKeys.map(exprToProto(_, join.left.output))
    val rightKeys = join.rightKeys.map(exprToProto(_, join.right.output))

    if (leftKeys.forall(_.isDefined) &&
      rightKeys.forall(_.isDefined) &&
      childOp.nonEmpty) {
      val joinBuilder = OperatorOuterClass.HashJoin
        .newBuilder()
        .setJoinType(joinType)
        .addAllLeftJoinKeys(leftKeys.map(_.get).asJava)
        .addAllRightJoinKeys(rightKeys.map(_.get).asJava)
        .setBuildSide(
          if (join.buildSide == BuildLeft) BuildSide.BuildLeft else BuildSide.BuildRight)
      condition.foreach(joinBuilder.setCondition)
      Some(builder.setHashJoin(joinBuilder).build())
    } else {
      val allExprs: Seq[Expression] = join.leftKeys ++ join.rightKeys
      withInfo(join, allExprs: _*)
      None
    }
  }
}

object CometBroadcastHashJoin extends CometOperatorSerde[HashJoin] with CometHashJoin {

  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_EXEC_HASH_JOIN_ENABLED)

  override def convert(
      join: HashJoin,
      builder: Operator.Builder,
      childOp: Operator*): Option[Operator] =
    doConvert(join, builder, childOp: _*)
}

object CometShuffleHashJoin extends CometOperatorSerde[HashJoin] with CometHashJoin {

  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_EXEC_HASH_JOIN_ENABLED)

  override def convert(
      join: HashJoin,
      builder: Operator.Builder,
      childOp: Operator*): Option[Operator] =
    doConvert(join, builder, childOp: _*)
}
