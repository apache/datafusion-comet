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

import org.apache.spark.sql.execution.SortExec

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.{exprToProto, supportedSortType}

object CometSort extends CometOperatorSerde[SortExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_EXEC_SORT_ENABLED)

  override def convert(
      op: SortExec,
      builder: Operator.Builder,
      childOp: Operator*): Option[OperatorOuterClass.Operator] = {
    if (!supportedSortType(op, op.sortOrder)) {
      withInfo(op, "Unsupported data type in sort expressions")
      return None
    }

    val sortOrders = op.sortOrder.map(exprToProto(_, op.child.output))

    if (sortOrders.forall(_.isDefined) && childOp.nonEmpty) {
      val sortBuilder = OperatorOuterClass.Sort
        .newBuilder()
        .addAllSortOrders(sortOrders.map(_.get).asJava)
      Some(builder.setSort(sortBuilder).build())
    } else {
      withInfo(op, "sort order not supported", op.sortOrder: _*)
      None
    }
  }

}
