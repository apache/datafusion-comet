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

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.PythonUDF
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.comet.shims.ShimCometMapInBatch
import org.apache.spark.sql.execution.{ColumnarToRowExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.python.PythonSQLMetrics
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

import org.apache.comet.vector.CometStructVector

/**
 * Comet replacement for Spark's `MapInBatchExec` family (`PythonMapInArrowExec` /
 * `MapInArrowExec` in 4.1+ / `MapInPandasExec`). Feeds upstream Comet `ColumnarBatch` values
 * directly to a `CometArrowPythonRunner`, eliminating the per-row `InternalRow.getXXX` loop that
 * vanilla Spark's `ArrowPythonRunner` performs.
 *
 * Per-Spark-minor wiring lives in `ShimCometMapInBatch.computeArrowPython`.
 */
case class CometMapInBatchExec(
    func: Expression,
    output: Seq[Attribute],
    child: SparkPlan,
    isBarrier: Boolean,
    pythonEvalType: Int)
    extends UnaryExecNode
    with CometPlan
    with PythonSQLMetrics
    with ShimCometMapInBatch {

  override def supportsColumnar: Boolean = true

  override def producedAttributes: AttributeSet = AttributeSet(output)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows")) ++
    pythonMetrics

  // Fallback for row-consuming parents (e.g. a top-level `collect()` that produces rows).
  // Wraps this columnar exec in `ColumnarToRowExec`, reintroducing the row transition this
  // operator otherwise eliminates. Only fires when nothing downstream consumes columnar.
  override def doExecute(): RDD[InternalRow] = {
    ColumnarToRowExec(this).doExecute()
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val numInputRows = longMetric("numInputRows")

    val outputAttrs = output
    val childSchema = child.schema
    val evalType = pythonEvalType
    val metricsCopy = pythonMetrics

    // Resolve every `SQLConf`-derived input on the driver. `SQLConf.get` reads from a thread-local
    // `ConfigReader` that only exists on the driver, so dereferencing `conf` from inside the task
    // closure NPEs.
    val resolvedRunnerInputs = runnerInputs(func.asInstanceOf[PythonUDF], conf)

    val inputRDD = child.executeColumnar()

    def processPartition(batches: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
      val context = TaskContext.get()
      val counting = batches.map { b => numInputRows += b.numRows(); b }

      val columnarBatchIter = computeArrowPython(
        resolvedRunnerInputs,
        evalType,
        Array(Array(0)),
        StructType(Array(StructField("struct", childSchema))),
        metricsCopy,
        Iterator(counting),
        context.partitionId(),
        context)

      columnarBatchIter.map { batch =>
        // Python returns a single struct column; flatten to the user's output columns. The runner
        // produces Comet vectors, so the struct's children are already CometVectors that downstream
        // consumers (a stacked CometMapInBatchExec, or NativeUtil.exportBatch for a native Comet
        // operator) can use directly.
        val structVector = batch.column(0).asInstanceOf[CometStructVector]
        val outputVectors: Array[ColumnVector] =
          outputAttrs.indices.map(i => structVector.getChild(i)).toArray
        val flattenedBatch = new ColumnarBatch(outputVectors)
        flattenedBatch.setNumRows(batch.numRows())
        numOutputRows += flattenedBatch.numRows()
        numOutputBatches += 1
        flattenedBatch
      }
    }

    // Preserve isBarrier semantics: when set, run inside a barrier stage so all tasks
    // are gang-scheduled and BarrierTaskContext.barrier() works inside the UDF.
    if (isBarrier) {
      inputRDD.barrier().mapPartitions(processPartition)
    } else {
      inputRDD.mapPartitionsInternal(processPartition)
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): CometMapInBatchExec =
    copy(child = newChild)
}
