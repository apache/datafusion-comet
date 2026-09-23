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
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.python.UserDefinedPythonFunction
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.LongType

import org.apache.comet.{CometConf, NativeBase}

/**
 * Opt-in end-to-end benchmark of the Spark and native scalar Arrow UDF paths. Run with
 * COMET_ARROW_UDF_BENCHMARK=1, PYSPARK_PYTHON pointing to the Python used to build the native
 * library, and PYTHONPATH containing the matching Spark version's PySpark package. The benchmark
 * deliberately aggregates the UDF output so Spark cannot prune the UDF from the query.
 */
class CometArrowPythonUdfBenchmarkSuite extends CometTestBase {
  test("compare Spark and native scalar Arrow UDF execution") {
    assume(sys.env.get("COMET_ARROW_UDF_BENCHMARK").contains("1"))
    assume(NativeBase.supportsPythonUdf(), "native library was built without python-udf")

    val rows = sys.env.getOrElse("COMET_ARROW_UDF_BENCHMARK_ROWS", "1000000").toLong
    val warmups = sys.env.getOrElse("COMET_ARROW_UDF_BENCHMARK_WARMUPS", "2").toInt
    val iterations = sys.env.getOrElse("COMET_ARROW_UDF_BENCHMARK_ITERATIONS", "5").toInt
    val isolateUdf = sys.env.get("COMET_ARROW_UDF_BENCHMARK_ISOLATE_UDF").contains("1")
    val python = sys.env.getOrElse("PYSPARK_PYTHON", "python3")
    val code =
      "import base64, pickle, pyarrow.compute as pc; " +
        "from pyspark.sql.types import LongType; " +
        "print(base64.b64encode(pickle.dumps((pc.negate, LongType()))).decode())"
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

    val samples =
      scala.collection.mutable.Map(false -> Vector.empty[Double], true -> Vector.empty[Double])
    val expected = -rows * (rows - 1L) / 2L

    val configs = Seq(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "10000") ++
      (if (isolateUdf) {
         Seq(
           CometConf.COMET_EXEC_AGGREGATE_ENABLED.key -> "false",
           CometConf.COMET_SHUFFLE_ENABLED.key -> "false")
       } else {
         Seq.empty
       })
    withSQLConf(configs: _*) {
      println(
        s"ARROW_UDF_BENCHMARK spark=${spark.version} rows=$rows warmups=$warmups " +
          s"iterations=$iterations isolate_udf=$isolateUdf")
      for (iteration <- 0 until warmups + iterations; native <- Seq(false, true)) {
        withSQLConf(CometConf.COMET_NATIVE_ARROW_PYTHON_UDF_ENABLED.key -> native.toString) {
          val source = spark.range(0L, rows, 1L, 2)
          val df = source.select(udf(source.col("id")).as("value")).agg(sum("value"))
          val plan = df.queryExecution.executedPlan.toString()
          if (native) {
            assert(plan.contains("CometArrowEvalPython"), plan)
          } else {
            assert(plan.contains("ArrowEvalPython"), plan)
            assert(!plan.contains("CometArrowEvalPython"), plan)
          }
          if (iteration == 0) {
            println(s"ARROW_UDF_PLAN native=$native\n$plan")
          }
          val start = System.nanoTime()
          val actual = df.collect().head.getLong(0)
          val seconds = (System.nanoTime() - start).toDouble / 1e9
          assert(actual == expected, s"native=$native: $actual != $expected")
          if (iteration >= warmups) {
            samples(native) :+= seconds
            println(
              s"ARROW_UDF_SAMPLE native=$native iteration=${iteration - warmups} seconds=$seconds")
          }
        }
      }
    }

    def median(values: Seq[Double]): Double = {
      val sorted = values.sorted
      (sorted((sorted.size - 1) / 2) + sorted(sorted.size / 2)) / 2.0
    }
    if (iterations > 0) {
      val sparkMedian = median(samples(false))
      val nativeMedian = median(samples(true))
      println(
        s"ARROW_UDF_RESULT spark_median_seconds=$sparkMedian " +
          s"native_median_seconds=$nativeMedian speedup=${sparkMedian / nativeMedian}")
    }
  }
}
