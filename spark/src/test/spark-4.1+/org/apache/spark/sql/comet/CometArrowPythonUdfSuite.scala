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

import java.util.{Base64, Collections}

import scala.sys.process._

import org.apache.spark.api.python.{PythonEvalType, SimplePythonFunction}
import org.apache.spark.sql.{CometTestBase, Row}
import org.apache.spark.sql.execution.python.UserDefinedPythonFunction
import org.apache.spark.sql.functions.{array, map, struct}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, LongType, MapType, StructField, StructType, TimestampType}

import org.apache.comet.{CometConf, NativeBase}

class CometArrowPythonUdfSuite extends CometTestBase {

  test("scalar Arrow UDF falls back when the native feature is unavailable") {
    assume(!NativeBase.supportsPythonUdf())

    val function = SimplePythonFunction(
      Array.emptyByteArray,
      Collections.emptyMap[String, String](),
      Collections.emptyList[String](),
      "python3",
      "3.13",
      Collections.emptyList(),
      null)
    val udf = UserDefinedPythonFunction(
      "arrow_udf",
      function,
      LongType,
      PythonEvalType.SQL_SCALAR_ARROW_UDF,
      udfDeterministic = true)

    withSQLConf(CometConf.COMET_NATIVE_ARROW_PYTHON_UDF_ENABLED.key -> "true") {
      val source = spark.range(1, 2)
      val plan = source.select(udf(source.col("id"))).queryExecution.executedPlan
      assert(plan.collect { case _: CometArrowEvalPythonExec => true }.isEmpty)
    }
  }

  test("scalar Arrow UDF executes in the native pipeline") {
    assume(NativeBase.supportsPythonUdf(), "native library was built without python-udf")

    val python = sys.env.getOrElse("PYSPARK_PYTHON", "python3")
    val code =
      "import base64, pyspark.cloudpickle as cloudpickle, pyarrow.compute as pc; " +
        "from pyspark.sql.types import LongType; " +
        "print(base64.b64encode(cloudpickle.dumps((pc.negate, LongType()))).decode())"
    val command = Base64.getDecoder.decode(Seq(python, "-c", code).!!.trim)
    val pythonVersion =
      Seq(python, "-c", "import sys; print('%d.%d' % sys.version_info[:2])").!!.trim
    val function = SimplePythonFunction(
      command,
      Collections.emptyMap[String, String](),
      Collections.emptyList[String](),
      python,
      pythonVersion,
      Collections.emptyList(),
      null)
    val udf = UserDefinedPythonFunction(
      "negate_arrow",
      function,
      LongType,
      PythonEvalType.SQL_SCALAR_ARROW_UDF,
      udfDeterministic = true)

    withSQLConf(
      CometConf.COMET_NATIVE_ARROW_PYTHON_UDF_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val source = spark.range(1, 5)
      val df = source.select(udf(source.col("id")))
      assert(df.queryExecution.executedPlan.collect { case _: CometArrowEvalPythonExec =>
        true
      }.nonEmpty)
      checkAnswer(df, Seq(Row(-1L), Row(-2L), Row(-3L), Row(-4L)))

      val twoResults =
        source.select(udf(source.col("id")).as("first"), udf(source.col("id") + 1L).as("second"))
      val nativeUdfs = twoResults.queryExecution.executedPlan.collect {
        case op: CometArrowEvalPythonExec => op
      }
      assert(nativeUdfs.exists(_.nativeOp.getArrowPythonUdf.getFunctionsCount == 2))
      checkAnswer(twoResults, Seq(Row(-1L, -2L), Row(-2L, -3L), Row(-3L, -4L), Row(-4L, -5L)))
    }

    withSQLConf(CometConf.COMET_NATIVE_ARROW_PYTHON_UDF_ENABLED.key -> "false") {
      val source = spark.range(1, 2)
      val plan = source.select(udf(source.col("id"))).queryExecution.executedPlan
      assert(plan.collect { case _: CometArrowEvalPythonExec => true }.isEmpty)
    }

    withSQLConf(
      CometConf.COMET_NATIVE_ARROW_PYTHON_UDF_ENABLED.key -> "true",
      SQLConf.ARROW_EXECUTION_USE_LARGE_VAR_TYPES.key -> "true") {
      val source = spark.range(1, 2)
      val plan = source.select(udf(source.col("id"))).queryExecution.executedPlan
      assert(plan.collect { case _: CometArrowEvalPythonExec => true }.isEmpty)
    }
  }

  test("native Arrow UDF falls back for schemas and options with different Spark semantics") {
    assume(NativeBase.supportsPythonUdf(), "native library was built without python-udf")

    val function = SimplePythonFunction(
      Array.emptyByteArray,
      Collections.emptyMap[String, String](),
      Collections.emptyList[String](),
      "python3",
      "3.11",
      Collections.emptyList(),
      null)
    def arrowUdf(returnType: org.apache.spark.sql.types.DataType) = UserDefinedPythonFunction(
      "planning_only",
      function,
      returnType,
      PythonEvalType.SQL_SCALAR_ARROW_UDF,
      udfDeterministic = true)

    withSQLConf(
      CometConf.COMET_NATIVE_ARROW_PYTHON_UDF_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
      val source = spark.range(1)
      val timestamp = source.col("id").cast(TimestampType)
      val plans = Seq(
        source.select(arrowUdf(LongType)(timestamp)),
        source.select(arrowUdf(LongType)(array(source.col("id")))),
        source.select(arrowUdf(LongType)(map(source.col("id"), source.col("id")))),
        source.select(arrowUdf(LongType)(struct(source.col("id")))),
        source.select(arrowUdf(ArrayType(LongType))(source.col("id"))),
        source.select(arrowUdf(MapType(LongType, LongType))(source.col("id"))),
        source.select(
          arrowUdf(StructType(Seq(StructField("value", LongType))))(source.col("id"))),
        source.select(arrowUdf(TimestampType)(source.col("id"))))
      plans.foreach { df =>
        val plan = df.queryExecution.executedPlan
        assert(plan.collect { case _: CometArrowEvalPythonExec => true }.isEmpty)
      }
      withSQLConf("spark.sql.pyspark.udf.profiler" -> "perf") {
        val plan = source.select(arrowUdf(LongType)(source.col("id"))).queryExecution.executedPlan
        assert(plan.collect { case _: CometArrowEvalPythonExec => true }.isEmpty)
      }
    }
  }
}
