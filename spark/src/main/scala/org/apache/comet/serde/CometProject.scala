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

import scala.collection.JavaConverters._

import org.apache.spark.sql.execution.ProjectExec

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.exprToProto

object CometProject extends CometOperatorSerde[ProjectExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_EXEC_PROJECT_ENABLED)

  override def convert(
      op: ProjectExec,
      builder: Operator.Builder,
      childOp: Operator*): Option[OperatorOuterClass.Operator] = {
    val exprs = op.projectList.map(exprToProto(_, op.child.output))

    if (exprs.forall(_.isDefined) && childOp.nonEmpty) {
      val projectBuilder = OperatorOuterClass.Projection
        .newBuilder()
        .addAllProjectList(exprs.map(_.get).asJava)
      Some(builder.setProjection(projectBuilder).build())
    } else {
      withInfo(op, op.projectList: _*)
      None
    }
  }
}
