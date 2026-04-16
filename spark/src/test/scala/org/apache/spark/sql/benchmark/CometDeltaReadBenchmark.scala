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
import org.apache.spark.sql.types._

import org.apache.comet.CometConf

/**
 * Benchmark to measure Comet Delta Lake read performance. To run this benchmark:
 * `SPARK_GENERATE_BENCHMARK_FILES=1 make
 * benchmark-org.apache.spark.sql.benchmark.CometDeltaReadBenchmark` Results will be written to
 * "spark/benchmarks/CometDeltaReadBenchmark-**results.txt".
 */
object CometDeltaReadBenchmark extends CometBenchmarkBase {

  def deltaScanBenchmark(values: Int, dataType: DataType): Unit = {
    val sqlBenchmark =
      new Benchmark(s"SQL Single ${dataType.sql} Delta Column Scan", values, output = output)

    withTempPath { dir =>
      withTempTable("deltaTable") {
        prepareDeltaTable(
          dir,
          spark.sql(s"SELECT CAST(value as ${dataType.sql}) id FROM $tbl"),
          "deltaTable")

        val query = dataType match {
          case BooleanType => "sum(cast(id as bigint))"
          case _ => "sum(id)"
        }

        sqlBenchmark.addCase("SQL Delta - Spark") { _ =>
          withSQLConf(
            "spark.memory.offHeap.enabled" -> "true",
            "spark.memory.offHeap.size" -> "10g") {
            spark.sql(s"select $query from deltaTable").noop()
          }
        }

        sqlBenchmark.addCase("SQL Delta - Comet Delta-Kernel") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            "spark.memory.offHeap.enabled" -> "true",
            "spark.memory.offHeap.size" -> "10g",
            CometConf.COMET_DELTA_NATIVE_ENABLED.key -> "true") {
            spark.sql(s"select $query from deltaTable").noop()
          }
        }

        sqlBenchmark.run()
      }
    }
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    runBenchmarkWithTable("SQL Single Numeric Delta Column Scan", 1024 * 1024 * 128) { v =>
      Seq(BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType)
        .foreach(deltaScanBenchmark(v, _))
    }
  }
}
