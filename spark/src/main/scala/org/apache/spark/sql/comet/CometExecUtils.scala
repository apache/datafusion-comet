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
import scala.reflect.ClassTag

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression, SortOrder}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.serde.{OperatorOuterClass, QueryPlanSerde}
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.{exprToProto, serializeDataType}

object CometExecUtils {

  /**
   * Create an empty RDD with the given number of partitions.
   */
  def emptyRDDWithPartitions[T: ClassTag](
      sparkContext: SparkContext,
      numPartitions: Int): RDD[T] = {
    new EmptyRDDWithPartitions(sparkContext, numPartitions)
  }

  /**
   * Transform the given RDD into a new RDD that takes the first `limit` elements of each
   * partition. The limit operation is performed on the native side.
   */
  def getNativeLimitRDD(
      childPlan: SparkPlan,
      child: RDD[ColumnarBatch],
      outputAttribute: Seq[Attribute],
      limit: Int,
      offset: Int = 0): RDD[ColumnarBatch] = {
    val numParts = child.getNumPartitions
    child.mapPartitionsWithIndexInternal { case (idx, iter) =>
      val limitOp =
        CometExecUtils.getLimitNativePlan(childPlan, outputAttribute, limit, offset).get
      CometExec.getCometIterator(Seq(iter), outputAttribute.length, limitOp, numParts, idx)
    }
  }

  /**
   * Prepare Projection + TopK native plan for CometTakeOrderedAndProjectExec.
   */
  def getProjectionNativePlan(
      projectList: Seq[NamedExpression],
      outputAttributes: Seq[Attribute],
      sortOrder: Seq[SortOrder],
      child: SparkPlan,
      limit: Int,
      offset: Int = 0): Option[Operator] = {
    getTopKNativePlan(outputAttributes, sortOrder, child, limit, offset).flatMap { topK =>
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
  def getLimitNativePlan(
      child: SparkPlan,
      outputAttributes: Seq[Attribute],
      limit: Int,
      offset: Int = 0): Option[Operator] = {
    val scanBuilder = OperatorOuterClass.Scan
      .newBuilder()
      .setSource("LimitInput")
      .addAllInputOrdering(QueryPlanSerde.parsePlanSortOrderAsMuchAsCan(child).asJava)

    val scanOpBuilder = OperatorOuterClass.Operator.newBuilder()

    val scanTypes = outputAttributes.flatten { attr =>
      serializeDataType(attr.dataType)
    }

    if (scanTypes.length == outputAttributes.length) {
      scanBuilder.addAllFields(scanTypes.asJava)

      val limitBuilder = OperatorOuterClass.Limit.newBuilder()
      limitBuilder.setLimit(limit)
      limitBuilder.setOffset(offset)

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
      limit: Int,
      offset: Int = 0): Option[Operator] = {
    val scanBuilder = OperatorOuterClass.Scan
      .newBuilder()
      .setSource("TopKInput")
      .addAllInputOrdering(QueryPlanSerde.parsePlanSortOrderAsMuchAsCan(child).asJava)

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
        sortBuilder.setSkip(offset)

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

/** A simple RDD with no data, but with the given number of partitions. */
private class EmptyRDDWithPartitions[T: ClassTag](
    @transient private val sc: SparkContext,
    numPartitions: Int)
    extends RDD[T](sc, Nil) {

  override def getPartitions: Array[Partition] =
    Array.tabulate(numPartitions)(i => EmptyPartition(i))

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    Iterator.empty
  }
}

private case class EmptyPartition(index: Int) extends Partition
