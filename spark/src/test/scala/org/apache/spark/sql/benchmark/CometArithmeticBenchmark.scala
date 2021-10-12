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
 * Benchmark to measure Comet expression evaluation performance. To run this benchmark:
 * `SPARK_GENERATE_BENCHMARK_FILES=1 make
 * benchmark-org.apache.spark.sql.benchmark.CometArithmeticBenchmark` Results will be written to
 * "spark/benchmarks/CometArithmeticBenchmark-**results.txt".
 */
object CometArithmeticBenchmark extends CometBenchmarkBase {
  private val table = "parquetV1Table"

  def integerArithmeticBenchmark(values: Int, op: BinaryOp, useDictionary: Boolean): Unit = {
    val dataType = IntegerType
    val benchmark = new Benchmark(
      s"Binary op ${dataType.sql}, dictionary = $useDictionary",
      values,
      output = output)

    withTempPath { dir =>
      withTempTable(table) {
        prepareTable(
          dir,
          spark.sql(
            s"SELECT CAST(value AS ${dataType.sql}) AS c1, " +
              s"CAST(value AS ${dataType.sql}) c2 FROM $tbl"))

        benchmark.addCase(s"$op ($dataType) - Spark") { _ =>
          spark.sql(s"SELECT c1 ${op.sig} c2 FROM $table").noop()
        }

        benchmark.addCase(s"$op ($dataType) - Comet (Scan)") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "true") {
            spark.sql(s"SELECT c1 ${op.sig} c2 FROM $table").noop()
          }
        }

        benchmark.addCase(s"$op ($dataType) - Comet (Scan, Exec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ALL_OPERATOR_ENABLED.key -> "true") {
            spark.sql(s"SELECT c1 ${op.sig} c2 FROM $table").noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def decimalArithmeticBenchmark(
      values: Int,
      dataType: DecimalType,
      op: BinaryOp,
      useDictionary: Boolean): Unit = {
    val benchmark = new Benchmark(
      s"Binary op ${dataType.sql}, dictionary = $useDictionary",
      values,
      output = output)
    val df = makeDecimalDataFrame(values, dataType, useDictionary)

    withTempPath { dir =>
      withTempTable(table) {
        df.createOrReplaceTempView(tbl)
        prepareTable(dir, spark.sql(s"SELECT dec AS c1, dec AS c2 FROM $tbl"))

        benchmark.addCase(s"$op ($dataType) - Spark") { _ =>
          spark.sql(s"SELECT c1 ${op.sig} c2 FROM $table").noop()
        }

        benchmark.addCase(s"$op ($dataType) - Comet (Scan)") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "true") {
            spark.sql(s"SELECT c1 ${op.sig} c2 FROM $table").noop()
          }
        }

        benchmark.addCase(s"$op ($dataType) - Comet (Scan, Exec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ALL_OPERATOR_ENABLED.key -> "true") {
            spark.sql(s"SELECT c1 ${op.sig} c2 FROM $table").noop()
          }
        }

        benchmark.run()
      }
    }
  }

  private val TOTAL: Int = 1024 * 1024 * 10

  override def runCometBenchmark(args: Array[String]): Unit = {
    Seq(true, false).foreach { useDictionary =>
      Seq(Plus, Mul, Div).foreach { op =>
        for ((precision, scale) <- Seq((5, 2), (18, 10), (38, 37))) {
          runBenchmark(op.name) {
            decimalArithmeticBenchmark(TOTAL, DecimalType(precision, scale), op, useDictionary)
          }
        }
      }
    }

    Seq(true, false).foreach { useDictionary =>
      Seq(Minus, Mul).foreach { op =>
        runBenchmarkWithTable(op.name, TOTAL, useDictionary) { v =>
          integerArithmeticBenchmark(v, op, useDictionary)
        }
      }
    }
  }

  private val Plus: BinaryOp = BinaryOp("plus", "+")
  private val Minus: BinaryOp = BinaryOp("minus", "-")
  private val Mul: BinaryOp = BinaryOp("mul", "*")
  private val Div: BinaryOp = BinaryOp("div", "/")

  case class BinaryOp(name: String, sig: String) {
    override def toString: String = name
  }
}
