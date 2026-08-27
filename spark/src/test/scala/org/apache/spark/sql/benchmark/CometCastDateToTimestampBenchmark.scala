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

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.comet.CometProjectExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.udf.codegen.CometScalaUDFCodegen

/**
 * Compare native, dispatcher, and Spark row projection costs for column DATE to TIMESTAMP. Run
 * the same benchmark on the PR base and head with a release native library:
 * {{{
 *   make release
 *   make benchmark-org.apache.spark.sql.benchmark.CometCastDateToTimestampBenchmark
 * }}}
 * The optional first argument sets the row count (default 1,048,576).
 */
object CometCastDateToTimestampBenchmark extends CometBenchmarkBase {

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val rows = mainArgs.headOption.map(_.toInt).getOrElse(1024 * 1024)
    withTempPath { dir =>
      withTempTable("date_cast_input") {
        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          // Ordinary dates from 1960 through 2029, with 1% nulls. Materialize the DATE column
          // so neither engine can fold the cast or include date construction in the measurement.
          spark
            .range(rows)
            .selectExpr("CASE WHEN id % 100 = 0 THEN NULL " +
              "ELSE date_add(DATE '1960-01-01', CAST(id % 25567 AS INT)) END AS d")
            .coalesce(1)
            .write
            .parquet(dir.getCanonicalPath)
        }
        spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("date_cast_input")
        val query = "SELECT CAST(d AS TIMESTAMP) FROM date_cast_input"

        for (timezone <- Seq("UTC", "America/Los_Angeles")) {
          val benchmark = new Benchmark(
            s"DATE to TIMESTAMP in $timezone",
            rows,
            minNumIters = 10,
            output = output)
          for (dispatch <- Seq("true", "false")) {
            val configs = Seq(
              SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
              SQLConf.SESSION_LOCAL_TIMEZONE.key -> timezone,
              CometConf.COMET_ENABLED.key -> "true",
              CometConf.COMET_EXEC_ENABLED.key -> "true",
              CometConf.COMET_BATCH_SIZE.key -> "8192",
              CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> dispatch)
            val route = withSQLConf(configs: _*) {
              CometScalaUDFCodegen.resetStats()
              val df = spark.sql(query)
              df.noop()
              val plan = stripAQEPlan(df.queryExecution.executedPlan)
              val nativeProject = plan.exists(_.isInstanceOf[CometProjectExec])
              val stats = CometScalaUDFCodegen.stats()
              val dispatched = stats.compileCount + stats.cacheHitCount > 0
              val route = if (dispatched) {
                require(nativeProject, s"Dispatcher must remain in a Comet projection: $plan")
                "JVM dispatcher"
              } else if (nativeProject) {
                "native"
              } else {
                require(dispatch == "false", s"Unexpected row fallback: $plan")
                "Spark row projection"
              }
              if (timezone == "UTC") {
                require(route == "native", s"UTC control must stay native: $plan")
              }
              benchmark.out.println(s"timezone=$timezone dispatcher=$dispatch route=$route")
              benchmark.out.println(plan.treeString)
              route
            }
            benchmark.addCase(s"$route, dispatcher=$dispatch") { _ =>
              withSQLConf(configs: _*) {
                spark.sql(query).noop()
              }
            }
          }
          benchmark.run()
        }
      }
    }
  }
}
