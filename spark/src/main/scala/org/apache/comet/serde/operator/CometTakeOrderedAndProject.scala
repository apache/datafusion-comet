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

package org.apache.comet.serde.operator

import org.apache.spark.sql.comet.{CometNativeExec, CometSinkPlaceHolder, CometTakeOrderedAndProjectExec}
import org.apache.spark.sql.execution.TakeOrderedAndProjectExec

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometSparkSessionExtensions.isCometShuffleEnabled
import org.apache.comet.serde.{Compatible, OperatorOuterClass, SupportLevel, Unsupported}
import org.apache.comet.serde.QueryPlanSerde.{exprToProto, supportedSortType}

object CometTakeOrderedAndProject extends CometSink[TakeOrderedAndProjectExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] = Some(
    CometConf.COMET_EXEC_TAKE_ORDERED_AND_PROJECT_ENABLED)

  override def getSupportLevel(op: TakeOrderedAndProjectExec): SupportLevel = {
    if (!isCometShuffleEnabled(op.conf)) {
      return Unsupported(Some("TakeOrderedAndProject requires shuffle to be enabled"))
    }
    op.projectList.foreach { p =>
      val o = exprToProto(p, op.child.output)
      if (o.isEmpty) {
        return Unsupported(Some(s"unsupported projection: $p"))
      }
      o
    }
    op.sortOrder.foreach { s =>
      val o = exprToProto(s, op.child.output)
      if (o.isEmpty) {
        return Unsupported(Some(s"unsupported sort order: $s"))
      }
      o
    }
    if (!supportedSortType(op, op.sortOrder)) {
      return Unsupported(Some("unsupported data type in sort order"))
    }
    Compatible()
  }

  override def createExec(
      nativeOp: OperatorOuterClass.Operator,
      op: TakeOrderedAndProjectExec): CometNativeExec = {
    CometSinkPlaceHolder(
      nativeOp,
      op,
      CometTakeOrderedAndProjectExec(
        op,
        op.output,
        op.limit,
        op.offset,
        op.sortOrder,
        op.projectList,
        op.child))
  }
}
