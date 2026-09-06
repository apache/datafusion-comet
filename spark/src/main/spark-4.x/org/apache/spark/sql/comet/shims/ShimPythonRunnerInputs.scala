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

import org.apache.spark.JobArtifactSet
import org.apache.spark.api.python.ChainedPythonFunctions
import org.apache.spark.sql.catalyst.expressions.PythonUDF
import org.apache.spark.sql.execution.python.ArrowPythonRunner
import org.apache.spark.sql.internal.SQLConf

/**
 * The driver-resolved inputs every 4.x Comet Arrow Python runner needs, in the same shape across
 * 4.0 / 4.1 / 4.2. Shared by the `mapInArrow` / `mapInPandas` and scalar-UDF operators so a class
 * can mix in both shims.
 */
trait ShimPythonRunnerInputs {

  protected case class RunnerInputs(
      chainedFunc: Seq[(ChainedPythonFunctions, Long)],
      pythonRunnerConf: Map[String, String],
      jobArtifactUUID: Option[String])

  /**
   * Resolves the `SQLConf`-derived inputs the `ArrowPythonRunner` needs. Must be called on the
   * driver: `SQLConf.get` reads from a thread-local `ConfigReader` that only exists on the
   * driver, so dereferencing `conf` from a task closure NPEs.
   *
   * Each UDF contributes one chained function. Spark's `EvalPythonEvaluatorFactory` also folds a
   * `PythonUDF` applied to another `PythonUDF` into a single chain; that shape never reaches
   * here, because a nested UDF is not an `AttributeReference` and so fails the argument check
   * that admits an operator to the native path.
   */
  protected def runnerInputs(pythonUDFs: Seq[PythonUDF], conf: SQLConf): RunnerInputs =
    RunnerInputs(
      chainedFunc =
        pythonUDFs.map(udf => (ChainedPythonFunctions(Seq(udf.func)), udf.resultId.id)),
      pythonRunnerConf = ArrowPythonRunner.getPythonRunnerConfMap(conf),
      jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid))
}
