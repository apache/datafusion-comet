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

import org.apache.spark.sql.catalyst.expressions.{Attribute, PythonUDF}
import org.apache.spark.sql.execution.SparkPlan

/**
 * Spark-version-agnostic projection of an `ArrowEvalPythonExec` that the Comet rewrite needs.
 * Lives outside the shims so the Comet planner can pattern-match on it without depending on the
 * Spark class it was matched from.
 */
case class ArrowEvalPythonInfo(
    udfs: Seq[PythonUDF],
    resultAttrs: Seq[Attribute],
    child: SparkPlan,
    evalType: Int)

/**
 * Where each scalar Python UDF's arguments sit in the batch exchanged with the Python worker.
 *
 * Spark's `EvalPythonEvaluatorFactory` flattens the arguments of every UDF in the operator into
 * one deduplicated list and sends that as a `_0`, `_1`, ... schema; Comet reproduces that layout
 * so the worker protocol is unchanged, but resolves it against the child's columns instead of
 * evaluating a projection per row.
 *
 * @param inputColumnIndices
 *   for each column of the batch sent to the worker, its index in the child's output
 * @param argOffsets
 *   for each UDF, the position in `inputColumnIndices` of each of its arguments
 */
case class ArrowEvalPythonArgs(inputColumnIndices: Seq[Int], argOffsets: Seq[Seq[Int]])
