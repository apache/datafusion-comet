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

import org.apache.spark.sql.catalyst.expressions.PythonUDF
import org.apache.spark.sql.internal.SQLConf

/**
 * Spark 3.x stub for the driver-resolved inputs Comet's Arrow Python runners need.
 *
 * The columnar runners introduced in #4234 only target Spark 4.0+, so on 3.4 / 3.5 every operator
 * matcher returns `None` and this factory is never called.
 */
trait ShimPythonRunnerInputs {

  /** Stub; never constructed on Spark 3.x because the operator matchers always return `None`. */
  protected case class RunnerInputs()

  protected def runnerInputs(pythonUDFs: Seq[PythonUDF], conf: SQLConf): RunnerInputs =
    throw new UnsupportedOperationException(
      "Comet's Arrow Python runner is not supported on Spark 3.x")
}
