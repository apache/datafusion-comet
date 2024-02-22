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

package org.apache.spark.sql.comet

import scala.collection.JavaConverters.asJavaIterableConverter

import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression, SortOrder}
import org.apache.spark.sql.execution.SparkPlan

import org.apache.comet.serde.OperatorOuterClass
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.{exprToProto, serializeDataType}

object CometExecUtils {

  /**
   * Prepare Projection + TopK native plan for CometTakeOrderedAndProjectExec.
   */
  def getProjectionNativePlan(
      projectList: Seq[NamedExpression],
      outputAttributes: Seq[Attribute],
      sortOrder: Seq[SortOrder],
      child: SparkPlan,
      limit: Int): Option[Operator] = {
    getTopKNativePlan(outputAttributes, sortOrder, child, limit).flatMap { topK =>
      val exprs = projectList.map(exprToProto(_, child.output))

      if (exprs.forall(_.isDefined)) {
        val projectBuilder = OperatorOuterClass.Projection.newBuilder()
        projectBuilder.addAllProjectList(exprs.map(_.get).asJava)
        val opBuilder = OperatorOuterClass.Operator
          .newBuilder()
          .addChildren(topK)
        Some(opBuilder.setProjection(projectBuilder).build())
      } else {
        None
      }
    }
  }

  /**
   * Prepare Limit native plan for Comet operators which take the first `limit` elements of each
   * child partition
   */
  def getLimitNativePlan(outputAttributes: Seq[Attribute], limit: Int): Option[Operator] = {
    val scanBuilder = OperatorOuterClass.Scan.newBuilder()
    val scanOpBuilder = OperatorOuterClass.Operator.newBuilder()

    val scanTypes = outputAttributes.flatten { attr =>
      serializeDataType(attr.dataType)
    }

    if (scanTypes.length == outputAttributes.length) {
      scanBuilder.addAllFields(scanTypes.asJava)

      val limitBuilder = OperatorOuterClass.Limit.newBuilder()
      limitBuilder.setLimit(limit)

      val limitOpBuilder = OperatorOuterClass.Operator
        .newBuilder()
        .addChildren(scanOpBuilder.setScan(scanBuilder))
      Some(limitOpBuilder.setLimit(limitBuilder).build())
    } else {
      None
    }
  }

  /**
   * Prepare TopK native plan for CometTakeOrderedAndProjectExec.
   */
  def getTopKNativePlan(
      outputAttributes: Seq[Attribute],
      sortOrder: Seq[SortOrder],
      child: SparkPlan,
      limit: Int): Option[Operator] = {
    val scanBuilder = OperatorOuterClass.Scan.newBuilder()
    val scanOpBuilder = OperatorOuterClass.Operator.newBuilder()

    val scanTypes = outputAttributes.flatten { attr =>
      serializeDataType(attr.dataType)
    }

    if (scanTypes.length == outputAttributes.length) {
      scanBuilder.addAllFields(scanTypes.asJava)

      val sortOrders = sortOrder.map(exprToProto(_, child.output))

      if (sortOrders.forall(_.isDefined)) {
        val sortBuilder = OperatorOuterClass.Sort.newBuilder()
        sortBuilder.addAllSortOrders(sortOrders.map(_.get).asJava)
        sortBuilder.setFetch(limit)

        val sortOpBuilder = OperatorOuterClass.Operator
          .newBuilder()
          .addChildren(scanOpBuilder.setScan(scanBuilder))
        Some(sortOpBuilder.setSort(sortBuilder).build())
      } else {
        None
      }
    } else {
      None
    }
  }
}
