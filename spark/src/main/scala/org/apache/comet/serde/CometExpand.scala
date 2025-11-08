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
import org.apache.spark.sql.execution.ExpandExec

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.exprToProto

object CometExpand extends CometOperatorSerde[ExpandExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] = Some(
    CometConf.COMET_EXEC_EXPAND_ENABLED)

  override def convert(
      op: ExpandExec,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = {
    var allProjExprs: Seq[Expression] = Seq()
    val projExprs = op.projections.flatMap(_.map(e => {
      allProjExprs = allProjExprs :+ e
      exprToProto(e, op.child.output)
    }))

    if (projExprs.forall(_.isDefined) && childOp.nonEmpty) {
      val expandBuilder = OperatorOuterClass.Expand
        .newBuilder()
        .addAllProjectList(projExprs.map(_.get).asJava)
        .setNumExprPerProject(op.projections.head.size)
      Some(builder.setExpand(expandBuilder).build())
    } else {
      withInfo(op, allProjExprs: _*)
      None
    }

  }

}
