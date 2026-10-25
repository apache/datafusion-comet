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

package org.apache.spark.sql.execution.python

import org.apache.spark.api.python.PythonException

/**
 * Builds the `PythonException` for a Python UDF that Comet ran in its embedded interpreter, as
 * Spark's Python runners build it from a worker's error.
 *
 * With an error condition, the scheduler fails the job with this exception itself rather than
 * wrapping it, and PySpark's `convert_exception` raises it as `pyspark.errors.PythonException`.
 */
private[spark] object CometPythonException {

  /** `traceback` is the formatted Python traceback. */
  def apply(traceback: String): Throwable =
    new PythonException(
      errorClass = "PYTHON_EXCEPTION",
      messageParameters =
        Map("msg" -> "An exception was thrown from the Python worker", "traceback" -> traceback))
}
