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

import java.util.concurrent.ConcurrentLinkedQueue

import scala.jdk.CollectionConverters._

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, PythonUDF}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.comet.execution.arrow.CometBatchDeepCopy
import org.apache.spark.sql.comet.shims.{ArrowEvalPythonArgs, ShimCometArrowEvalPython}
import org.apache.spark.sql.execution.{ColumnarToRowExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.python.PythonSQLMetrics
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

import org.apache.comet.CometArrowAllocator
import org.apache.comet.vector.CometVector

/**
 * Comet replacement for Spark's `ArrowEvalPythonExec`, which evaluates scalar Python UDFs: a
 * plain `udf()` when `spark.sql.execution.pythonUDF.arrow.enabled` is set, a `@pandas_udf`, or
 * Spark 4.1's `@arrow_udf`.
 *
 * Vanilla Spark evaluates these row by row even though both ends of the exchange are Arrow. For
 * every input row it copies the row into a `HybridRowQueue` (which spills to disk under memory
 * pressure), runs a `MutableProjection` to materialise the UDF arguments, and on the way back
 * joins the queued row with the worker's output through a `JoinedRow` and an `UnsafeProjection`
 * -- all beneath the `ColumnarToRow` transition a Comet child would otherwise force.
 *
 * This operator keeps the stage columnar instead. It sends the UDF argument columns straight from
 * the input batch to a `CometArrowEvalPythonRunner`, and appends the columns the worker returns
 * to the input batch's own columns to form the output batch, so no row is ever materialised.
 *
 * The input batch has to outlive the child iterator's advance, because the Python runner writes
 * from its own thread and reads results back asynchronously. Comet's native operators recycle the
 * buffers behind consecutive batches, so the batch is deep-copied on the way in (see
 * [[CometBatchDeepCopy]]). That copy replaces Spark's per-row `UnsafeRow` copy and its spill
 * path.
 *
 * Only eval types with a 1:1 input/output batch relationship reach this operator, and only when
 * every UDF argument is an attribute of the child; `ShimCometArrowEvalPython` enforces both.
 */
case class CometArrowEvalPythonExec(
    udfs: Seq[PythonUDF],
    resultAttrs: Seq[Attribute],
    override val child: SparkPlan,
    evalType: Int)
    extends UnaryExecNode
    with CometPlan
    with PythonSQLMetrics
    with ShimCometArrowEvalPython {

  override def output: Seq[Attribute] = child.output ++ resultAttrs

  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)

  override def supportsColumnar: Boolean = true

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows")) ++
    pythonMetrics

  @transient private lazy val resolvedArgs: ArrowEvalPythonArgs =
    resolveArrowEvalPythonArgs(udfs, child.output).getOrElse {
      throw new IllegalStateException(
        s"$nodeName requires every Python UDF argument to be an attribute of its child")
    }

  // Fallback for row-consuming parents (e.g. a top-level `collect()` that produces rows). Wraps
  // this columnar exec in `ColumnarToRowExec`, reintroducing the row transition this operator
  // otherwise eliminates. Only fires when nothing downstream consumes columnar.
  override def doExecute(): RDD[InternalRow] = {
    ColumnarToRowExec(this).doExecute()
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val numInputRows = longMetric("numInputRows")

    val args = resolvedArgs
    val childOutput = child.output
    // Spark's `EvalPythonEvaluatorFactory` names the exchanged columns positionally. Keep that
    // naming: for `SQL_ARROW_BATCHED_UDF` this schema is serialised to the worker, which builds
    // its per-argument converters from it.
    val inputSchema = StructType(args.inputColumnIndices.zipWithIndex.map { case (column, i) =>
      StructField(s"_$i", childOutput(column).dataType, childOutput(column).nullable)
    }.toArray)
    val inputColumnIndices = args.inputColumnIndices.toArray
    val argOffsets = args.argOffsets.map(_.toArray).toArray
    val evalTypeCopy = evalType
    val metricsCopy = pythonMetrics

    // Resolve every `SQLConf`-derived input on the driver. `SQLConf.get` reads from a thread-local
    // `ConfigReader` that only exists on the driver, so dereferencing `conf` from inside the task
    // closure NPEs.
    val resolvedRunnerInputs = runnerInputs(udfs, conf)

    child.executeColumnar().mapPartitionsInternal { batches =>
      val context = TaskContext.get()
      val allocator =
        CometArrowAllocator.newChildAllocator(s"$nodeName retained input", 0, Long.MaxValue)

      // Copies of the input batches, in the order the runner's writer thread consumed them. The
      // writer runs on its own thread, so ownership is handed to the reader across threads here.
      val retained = new ConcurrentLinkedQueue[VectorSchemaRoot]()
      // Backs the batch most recently handed downstream. Held until the following batch is
      // produced, mirroring how `CometExecIterator` keeps the previous batch alive.
      var current: VectorSchemaRoot = null

      context.addTaskCompletionListener[Unit] { _ =>
        if (current != null) {
          current.close()
          current = null
        }
        var pending = retained.poll()
        while (pending != null) {
          pending.close()
          pending = retained.poll()
        }
        allocator.close()
      }

      val pythonInput = batches.map { batch =>
        numInputRows += batch.numRows()
        val root = CometBatchDeepCopy.copy(batch, allocator)
        retained.add(root)
        // Only the argument columns are sent to the worker; the rest of the retained batch is
        // wrapped later, when the output batch is assembled.
        val arguments = new ColumnarBatch(inputColumnIndices.map(i => cometColumn(root, i)))
        arguments.setNumRows(batch.numRows())
        arguments
      }

      val outputIter = computeArrowEvalPython(
        resolvedRunnerInputs,
        evalTypeCopy,
        argOffsets,
        inputSchema,
        metricsCopy,
        Iterator(pythonInput),
        context.partitionId(),
        context)

      outputIter.map { pythonBatch =>
        if (current != null) {
          current.close()
          current = null
        }
        val root = retained.poll()
        if (root == null) {
          throw new IllegalStateException(
            s"$nodeName received more batches from the Python worker than it sent")
        }
        current = root
        if (root.getRowCount != pythonBatch.numRows()) {
          throw new IllegalStateException(
            s"$nodeName expected the Python worker to return one row per input row, but an " +
              s"input batch of ${root.getRowCount} rows produced ${pythonBatch.numRows()}")
        }

        // The worker returns one top-level column per UDF, in operator order, which is exactly
        // the order of `resultAttrs`.
        val pythonColumns = (0 until pythonBatch.numCols()).map(pythonBatch.column)
        val batch = new ColumnarBatch(cometColumns(root) ++ pythonColumns)
        batch.setNumRows(pythonBatch.numRows())
        numOutputRows += batch.numRows()
        numOutputBatches += 1
        batch
      }
    }
  }

  /**
   * Wraps every one of a retained root's Arrow vectors as a Comet column. Fresh wrappers each
   * call: they are thin views, and the root owns the buffers they read.
   */
  private def cometColumns(root: VectorSchemaRoot): Array[ColumnVector] =
    root.getFieldVectors.asScala.map { vector =>
      CometVector.getVector(vector, null).asInstanceOf[ColumnVector]
    }.toArray

  /** Wraps a single column of a retained root, for the subset sent to the Python worker. */
  private def cometColumn(root: VectorSchemaRoot, index: Int): ColumnVector =
    CometVector.getVector(root.getVector(index), null).asInstanceOf[ColumnVector]

  override protected def withNewChildInternal(newChild: SparkPlan): CometArrowEvalPythonExec =
    copy(child = newChild)
}
