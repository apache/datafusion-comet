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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, LongType}

import org.apache.comet.expressions.{CometCast, CometEvalMode}
import org.apache.comet.serde.{Compatible, Incompatible, Unsupported}

/**
 * Benchmark to measure Comet execution performance. To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometCastBenchmark
 * }}}
 *
 * Results will be written to "spark/benchmarks/CometCastBenchmark-**results.txt".
 */

object CometCastBenchmark extends CometBenchmarkBase {

  override def getSparkSession: SparkSession = {
    val session = super.getSparkSession
    session.conf.set("parquet.enable.dictionary", "false")
    session.conf.set("spark.sql.shuffle.partitions", "2")
    session
  }

  def castExprSQL(toDataType: DataType, input: String): String = {
    s"CAST ($input AS ${toDataType.sql})"
  }

  override def runCometBenchmark(args: Array[String]): Unit = {

    //  TODO : Create all possible input datatypes. We only have Long inputs for now
    CometCast.supportedTypes.foreach { toDataType =>
      Seq(false, true).foreach { ansiMode =>
        CometCast.isSupported(
          LongType,
          toDataType,
          None,
          if (ansiMode) CometEvalMode.ANSI else CometEvalMode.LEGACY) match {
          case Compatible(notes) =>
            runBenchmarkWithTable(
              s"Running benchmark cast operation from : $LongType to : $toDataType",
              1024 * 1024 * 10) { v =>
              castBenchmark(v, LongType, toDataType, isAnsiMode = ansiMode)
            }
          case Incompatible(notes) => None
          case Unsupported(notes) => None
        }
      }
    }
  }

  def castBenchmark(
      values: Int,
      fromDataType: DataType,
      toDataType: DataType,
      isAnsiMode: Boolean): Unit = {

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        // Generate ANSI-safe data when in ANSI mode to avoid overflow exceptions
        // In legacy mode, use raw values to test overflow handling
        val dataExpr = if (isAnsiMode) {
          generateAnsiSafeData(toDataType)
        } else {
          "value"
        }

        prepareTable(dir, spark.sql(s"SELECT $dataExpr as value FROM $tbl"))

        val functionSQL = castExprSQL(toDataType, "value")
        val query = s"SELECT $functionSQL FROM parquetV1Table"
        val name =
          s"Cast function from : ${fromDataType} to : ${toDataType} , ansi mode enabled : ${isAnsiMode}"

        runExpressionBenchmark(name, values, query, isAnsiMode)
      }
    }
  }

}
