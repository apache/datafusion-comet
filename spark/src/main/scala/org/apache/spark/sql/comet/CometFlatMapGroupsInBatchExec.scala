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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeSet, Expression, PythonUDF, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.comet.shims.ShimCometMapInBatch
import org.apache.spark.sql.execution.{ColumnarToRowExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.python.PythonSQLMetrics
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

import org.apache.comet.vector.CometStructVector

case class CometFlatMapGroupsInBatchExec(
    groupingAttributes: Seq[Attribute],
    func: Expression,
    output: Seq[Attribute],
    child: SparkPlan,
    pythonEvalType: Int,
    structInput: Boolean)
    extends UnaryExecNode
    with CometPlan
    with PythonSQLMetrics
    with ShimCometMapInBatch {

  override def supportsColumnar: Boolean = true

  override def producedAttributes: AttributeSet = AttributeSet(output)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] = {
    if (groupingAttributes.isEmpty) {
      AllTuples :: Nil
    } else {
      ClusteredDistribution(groupingAttributes) :: Nil
    }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(groupingAttributes.map(SortOrder(_, Ascending)))

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows")) ++
    pythonMetrics

  override def doExecute(): RDD[InternalRow] =
    ColumnarToRowExec(this).doExecute()

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val numInputRows = longMetric("numInputRows")

    val (dedupAttributes, argOffsets) =
      resolveArgOffsets(child.output, groupingAttributes)
    val inputSchema = DataTypeUtils.fromAttributes(dedupAttributes)
    val runnerInputConfig = groupedInputConfig(inputSchema, structInput)
    val resolvedRunnerInputs = runnerInputs(func.asInstanceOf[PythonUDF], conf)
    val childOutput = child.output
    val outputAttrs = output
    val evalType = pythonEvalType
    val metricsCopy = pythonMetrics
    val maxRowsPerBatch =
      if (conf.arrowMaxRecordsPerBatch > 0) conf.arrowMaxRecordsPerBatch else Int.MaxValue
    val maxBytesPerBatch =
      if (runnerInputConfig.framedGroups) groupedArrowMaxBytesPerBatch(conf)
      else Long.MaxValue

    child.executeColumnar().mapPartitionsInternal { batches =>
      val counting = batches.map { batch =>
        numInputRows += batch.numRows()
        batch
      }
      val groups =
        new CometBatchGroupedIterator(
          counting,
          groupingAttributes,
          childOutput,
          dedupAttributes,
          maxRowsPerBatch,
          maxBytesPerBatch,
          structInput)
      if (!groups.hasNext) {
        Iterator.empty
      } else {
        val context = TaskContext.get()
        val result = computeArrowPython(
          resolvedRunnerInputs,
          evalType,
          Array(argOffsets),
          runnerInputConfig,
          metricsCopy,
          groups,
          context.partitionId(),
          context)

        result.map { batch =>
          val structVector = batch.column(0).asInstanceOf[CometStructVector]
          val vectors: Array[ColumnVector] =
            outputAttrs.indices.map(i => structVector.getChild(i)).toArray
          val flattened = new ColumnarBatch(vectors, batch.numRows())
          numOutputRows += flattened.numRows()
          numOutputBatches += 1
          flattened
        }
      }
    }
  }

  override protected def withNewChildInternal(
      newChild: SparkPlan): CometFlatMapGroupsInBatchExec =
    copy(child = newChild)

  private def resolveArgOffsets(
      attributes: Seq[Attribute],
      grouping: Seq[Attribute]): (Seq[Attribute], Array[Int]) = {
    val dataAttributes = attributes.drop(grouping.length)
    val groupingIndicesInData = grouping.map { attribute =>
      dataAttributes.indexWhere(attribute.semanticEquals)
    }

    val groupingArgOffsets = ArrayBuffer.empty[Int]
    val nonDupGroupingAttributes = ArrayBuffer.empty[Attribute]
    val nonDupGroupingSize = groupingIndicesInData.count(_ == -1)

    grouping.zip(groupingIndicesInData).foreach {
      case (attribute, -1) =>
        groupingArgOffsets += nonDupGroupingAttributes.length
        nonDupGroupingAttributes += attribute
      case (_, index) =>
        groupingArgOffsets += index + nonDupGroupingSize
    }

    val dataArgOffsets = nonDupGroupingAttributes.length until
      (nonDupGroupingAttributes.length + dataAttributes.length)
    val argOffsetsLength = grouping.length + dataArgOffsets.length + 1
    val argOffsets =
      Array(argOffsetsLength, grouping.length) ++ groupingArgOffsets ++ dataArgOffsets

    (nonDupGroupingAttributes.toSeq ++ dataAttributes, argOffsets)
  }
}
