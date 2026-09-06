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

package org.apache.spark.sql.comet.shims

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, PythonUDF}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.python.ArrowEvalPythonExec

/**
 * Shared 4.x bits for `ShimCometArrowEvalPython`. The matcher and argument resolution are
 * identical across 4.0/4.1/4.2; only the set of eval types and the runner's constructor differ
 * per minor, so each minor's `ShimCometArrowEvalPython` supplies `supportedEvalTypes` and
 * `computeArrowEvalPython`.
 */
trait Spark4xArrowEvalPythonSupport extends ShimPythonRunnerInputs {

  /**
   * Eval types this Spark minor can run natively. Excludes the iterator variants
   * (`SQL_SCALAR_PANDAS_ITER_UDF`, `SQL_SCALAR_ARROW_ITER_UDF`), which guarantee only that the
   * worker returns the same total number of rows, not the same batching. Comet pairs each output
   * batch with the input batch that produced it, so it needs the 1:1 relationship the
   * non-iterator types provide.
   */
  protected def supportedEvalTypes: Set[Int]

  protected def matchArrowEvalPython(plan: SparkPlan): Option[ArrowEvalPythonInfo] =
    plan match {
      case p: ArrowEvalPythonExec if supportedEvalTypes.contains(p.evalType) =>
        Some(ArrowEvalPythonInfo(p.udfs, p.resultAttrs, p.child, p.evalType))
      case _ => None
    }

  /**
   * Resolves every UDF argument to a column of `childOutput`, reproducing the deduplication
   * Spark's `EvalPythonEvaluatorFactory` performs so the worker sees the same argument layout.
   *
   * Returns `None` unless every argument is a plain attribute of the child. That restriction is
   * what lets Comet select argument columns straight out of the input batch instead of running a
   * projection per row, and it also excludes the two shapes this operator is not ready for:
   * chained UDFs (`f(g(x))`, whose argument is a `PythonUDF`) and keyword arguments (whose
   * argument is a `NamedArgumentExpression`), neither of which is an `Attribute`.
   */
  protected def resolveArrowEvalPythonArgs(
      udfs: Seq[PythonUDF],
      childOutput: Seq[Attribute]): Option[ArrowEvalPythonArgs] = {
    val inputColumnIndices = ArrayBuffer.empty[Int]
    val argOffsets = ArrayBuffer.empty[Seq[Int]]

    // The offset in the exchanged batch of `argument`, adding it as a new column when this is its
    // first use, or `None` when it is not a column of the child at all.
    def offsetOf(argument: Expression): Option[Int] = argument match {
      case attr: Attribute =>
        val childIndex = childOutput.indexWhere(_.exprId == attr.exprId)
        if (childIndex < 0) {
          None
        } else {
          val existing = inputColumnIndices.indexOf(childIndex)
          if (existing >= 0) {
            Some(existing)
          } else {
            inputColumnIndices += childIndex
            Some(inputColumnIndices.length - 1)
          }
        }
      case _ => None
    }

    val resolved = udfs.forall { udf =>
      val offsets = udf.children.map(offsetOf)
      argOffsets += offsets.flatten
      offsets.forall(_.isDefined)
    }

    if (resolved) Some(ArrowEvalPythonArgs(inputColumnIndices.toSeq, argOffsets.toSeq)) else None
  }
}
