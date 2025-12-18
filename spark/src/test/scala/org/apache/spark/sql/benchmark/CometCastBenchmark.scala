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

import scala.util.Try

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, LongType}

import org.apache.comet.CometConf
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

  // Wrapper on SQL cast function
  case class BenchCastExpression(name: DataType) {
    override def toString: String = s"${name.sql}"
  }

  def castExprSQL(castExpr: BenchCastExpression, input: String): String = {
    s"CAST ($input AS $castExpr)"
  }

  private val castExpressionList = CometCast.supportedTypes.map(BenchCastExpression(_))

  override def runCometBenchmark(args: Array[String]): Unit = {

    //  TODO : Create all possible input datatypes. We only have Long inputs for now
    castExpressionList.foreach { castExpr =>
      Seq(true, false).foreach { k =>
        CometCast.isSupported(
          LongType,
          castExpr.name,
          None,
          if (k) CometEvalMode.ANSI else CometEvalMode.LEGACY) match {
          case Compatible(notes) =>
            runBenchmarkWithTable(s"Running benchmark $castExpr)", 1024 * 1024 * 10) { v =>
              castBenchmark(v, castExpr, isAnsiMode = k)
            }
          case Incompatible(notes) => None
          case Unsupported(notes) => None
        }
      }
    }
  }

  def castBenchmark(values: Int, castExpr: BenchCastExpression, isAnsiMode: Boolean): Unit = {

    val benchmark =
      new Benchmark(
        s"Cast function to : ${castExpr} , ansi mode enabled : ${isAnsiMode}",
        values,
        output = output)

    val functionSQL = castExprSQL(castExpr, "value")
    val query = s"SELECT $functionSQL FROM $tbl"

    benchmark.addCase(
      s"SQL Parquet - Spark Cast expr to : ${castExpr} , ansi mode enabled : ${isAnsiMode}") {
      _ =>
        withSQLConf(SQLConf.ANSI_ENABLED.key -> isAnsiMode.toString) {
          Try { spark.sql(query).noop() }
        }
    }

    benchmark.addCase(
      s"SQL Parquet - Comet Cast expr to : ${castExpr} , ansi mode enabled : ${isAnsiMode}") {
      _ =>
        withSQLConf(
          CometConf.COMET_ENABLED.key -> "true",
          CometConf.COMET_EXEC_ENABLED.key -> "true",
          SQLConf.ANSI_ENABLED.key -> isAnsiMode.toString) {
          Try { spark.sql(query).noop() }
        }
    }

    benchmark.run()
  }

}
