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

package org.apache.spark.sql.benchmark

import java.util.{Base64, Collections}

import scala.sys.process._

import org.apache.spark.api.python.{PythonEvalType, SimplePythonFunction}
import org.apache.spark.sql.execution.python.UserDefinedPythonFunction
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.LongType

import org.apache.comet.{CometConf, NativeBase}

/**
 * End-to-end benchmark of Spark and native scalar Arrow UDF execution. Run with `make
 * benchmark-org.apache.spark.sql.benchmark.CometArrowPythonUdfBenchmark PROFILES=-Pspark-4.1
 * COMET_FEATURES=python-udf`, with PYO3_PYTHON set for the native build. Arguments are rows,
 * warmups, iterations, partitions, and mode (`arrow` or `python`). The Python mode performs a
 * per-element Python loop to expose GIL contention. Set `COMET_BENCHMARK_MASTER=local[16]` and
 * request at least 17 partitions for a concurrency comparison. Set PYSPARK_PYTHON and PYTHONPATH
 * for the embedded Python environment.
 */
object CometArrowPythonUdfBenchmark extends CometBenchmarkBase {
  override def runCometBenchmark(args: Array[String]): Unit = {
    require(NativeBase.supportsPythonUdf(), "native library was built without python-udf")

    val rows = args.headOption.map(_.toLong).getOrElse(1000000L)
    val warmups = args.lift(1).map(_.toInt).getOrElse(2)
    val iterations = args.lift(2).map(_.toInt).getOrElse(5)
    val partitions = args.lift(3).map(_.toInt).getOrElse(2)
    val mode = args.lift(4).getOrElse("arrow")
    require(Set("arrow", "python").contains(mode), s"Unknown benchmark mode: $mode")
    val python = sys.env.getOrElse("PYSPARK_PYTHON", "python3")
    val callable =
      if (mode == "arrow") "pc.negate" else "lambda a: pa.array([-x.as_py() for x in a])"
    val code =
      "import base64, pyspark.cloudpickle as cloudpickle, pyarrow as pa, " +
        "pyarrow.compute as pc; " +
        "from pyspark.sql.types import LongType; " +
        s"print(base64.b64encode(cloudpickle.dumps(($callable, LongType()))).decode())"
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
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_ONHEAP_ENABLED.key -> "true",
      CometConf.COMET_SPARK_TO_ARROW_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "10000")
    withSQLConf(configs: _*) {
      println(
        s"ARROW_UDF_BENCHMARK spark=${spark.version} rows=$rows warmups=$warmups " +
          s"iterations=$iterations partitions=$partitions mode=$mode")
      for (iteration <- 0 until warmups + iterations; native <- Seq(false, true)) {
        withSQLConf(CometConf.COMET_NATIVE_ARROW_PYTHON_UDF_ENABLED.key -> native.toString) {
          val source = spark.range(0L, rows, 1L, partitions)
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
