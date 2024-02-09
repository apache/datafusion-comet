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

import org.apache.comet.CometConf

/**
 * Benchmark to measure Comet execution performance. To run this benchmark:
 * `SPARK_GENERATE_BENCHMARK_FILES=1 make
 * benchmark-org.apache.spark.sql.benchmark.CometStringExpressionBenchmark` Results will be
 * written to "spark/benchmarks/CometStringExpressionBenchmark-**results.txt".
 */
object CometStringExpressionBenchmark extends CometBenchmarkBase {

  def subStringExprBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Substring Expr", values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(dir, spark.sql(s"SELECT REPEAT(CAST(value AS STRING), 100) AS c1 FROM $tbl"))

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark.sql("select substring(c1, 1, 100) from parquetV1Table").noop()
        }

        benchmark.addCase("SQL Parquet - Comet (Scan)") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "true") {
            spark.sql("select substring(c1, 1, 100) from parquetV1Table").noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Scan, Exec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ALL_OPERATOR_ENABLED.key -> "true") {
            spark.sql("select substring(c1, 1, 100) from parquetV1Table").noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def stringSpaceExprBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("StringSpace Expr", values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(dir, spark.sql(s"SELECT CAST(RAND(1) * 100 AS INTEGER) AS c1 FROM $tbl"))

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark.sql("select space(c1) from parquetV1Table").noop()
        }

        benchmark.addCase("SQL Parquet - Comet (Scan)") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "true") {
            spark.sql("select space(c1) from parquetV1Table").noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Scan, Exec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ALL_OPERATOR_ENABLED.key -> "true") {
            spark.sql("select space(c1) from parquetV1Table").noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def asciiExprBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Expr ascii", values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(dir, spark.sql(s"SELECT REPEAT(CAST(value AS STRING), 100) AS c1 FROM $tbl"))

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark.sql("select ascii(c1) from parquetV1Table").noop()
        }

        benchmark.addCase("SQL Parquet - Comet (Scan)") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "true") {
            spark.sql("select ascii(c1) from parquetV1Table").noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Scan, Exec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ALL_OPERATOR_ENABLED.key -> "true") {
            spark.sql("select ascii(c1) from parquetV1Table").noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def bitLengthExprBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Expr bit_length", values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(dir, spark.sql(s"SELECT REPEAT(CAST(value AS STRING), 100) AS c1 FROM $tbl"))

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark.sql("select bit_length(c1) from parquetV1Table").noop()
        }

        benchmark.addCase("SQL Parquet - Comet (Scan)") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "true") {
            spark.sql("select bit_length(c1) from parquetV1Table").noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Scan, Exec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ALL_OPERATOR_ENABLED.key -> "true") {
            spark.sql("select bit_length(c1) from parquetV1Table").noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def octetLengthExprBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Expr octet_length", values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(dir, spark.sql(s"SELECT REPEAT(CAST(value AS STRING), 100) AS c1 FROM $tbl"))

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark.sql("select octet_length(c1) from parquetV1Table").noop()
        }

        benchmark.addCase("SQL Parquet - Comet (Scan)") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "true") {
            spark.sql("select octet_length(c1) from parquetV1Table").noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Scan, Exec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ALL_OPERATOR_ENABLED.key -> "true") {
            spark.sql("select octet_length(c1) from parquetV1Table").noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def upperExprBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Expr upper", values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(dir, spark.sql(s"SELECT REPEAT(CAST(value AS STRING), 100) AS c1 FROM $tbl"))

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark.sql("select upper(c1) from parquetV1Table").noop()
        }

        benchmark.addCase("SQL Parquet - Comet (Scan)") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "true") {
            spark.sql("select upper(c1) from parquetV1Table").noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Scan, Exec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ALL_OPERATOR_ENABLED.key -> "true") {
            spark.sql("select upper(c1) from parquetV1Table").noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def lowerExprBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Expr lower", values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(dir, spark.sql(s"SELECT REPEAT(CAST(value AS STRING), 100) AS c1 FROM $tbl"))

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark.sql("select lower(c1) from parquetV1Table").noop()
        }

        benchmark.addCase("SQL Parquet - Comet (Scan)") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "true") {
            spark.sql("select lower(c1) from parquetV1Table").noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Scan, Exec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ALL_OPERATOR_ENABLED.key -> "true") {
            spark.sql("select lower(c1) from parquetV1Table").noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def chrExprBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Expr chr", values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(dir, spark.sql(s"SELECT REPEAT(CAST(value AS STRING), 100) AS c1 FROM $tbl"))

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark.sql("select chr(c1) from parquetV1Table").noop()
        }

        benchmark.addCase("SQL Parquet - Comet (Scan)") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "true") {
            spark.sql("select chr(c1) from parquetV1Table").noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Scan, Exec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ALL_OPERATOR_ENABLED.key -> "true") {
            spark.sql("select chr(c1) from parquetV1Table").noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def initCapExprBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Expr initCap", values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(dir, spark.sql(s"SELECT REPEAT(CAST(value AS STRING), 100) AS c1 FROM $tbl"))

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark.sql("select initCap(c1) from parquetV1Table").noop()
        }

        benchmark.addCase("SQL Parquet - Comet (Scan)") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "true") {
            spark.sql("select initCap(c1) from parquetV1Table").noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Scan, Exec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ALL_OPERATOR_ENABLED.key -> "true") {
            spark.sql("select initCap(c1) from parquetV1Table").noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def trimExprBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Expr trim", values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(dir, spark.sql(s"SELECT REPEAT(CAST(value AS STRING), 100) AS c1 FROM $tbl"))

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark.sql("select trim(c1) from parquetV1Table").noop()
        }

        benchmark.addCase("SQL Parquet - Comet (Scan)") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "true") {
            spark.sql("select trim(c1) from parquetV1Table").noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Scan, Exec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ALL_OPERATOR_ENABLED.key -> "true") {
            spark.sql("select trim(c1) from parquetV1Table").noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def concatwsExprBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Expr concatws", values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(dir, spark.sql(s"SELECT REPEAT(CAST(value AS STRING), 100) AS c1 FROM $tbl"))

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark.sql("select concat_ws(' ', c1, c1) from parquetV1Table").noop()
        }

        benchmark.addCase("SQL Parquet - Comet (Scan)") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "true") {
            spark.sql("select concat_ws(' ', c1, c1) from parquetV1Table").noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Scan, Exec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ALL_OPERATOR_ENABLED.key -> "true") {
            spark.sql("select concat_ws(' ', c1, c1) from parquetV1Table").noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def lengthExprBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Expr length", values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(dir, spark.sql(s"SELECT REPEAT(CAST(value AS STRING), 100) AS c1 FROM $tbl"))

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark.sql("select length(c1) from parquetV1Table").noop()
        }

        benchmark.addCase("SQL Parquet - Comet (Scan)") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "true") {
            spark.sql("select length(c1) from parquetV1Table").noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Scan, Exec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ALL_OPERATOR_ENABLED.key -> "true") {
            spark.sql("select length(c1) from parquetV1Table").noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def repeatExprBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Expr repeat", values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(dir, spark.sql(s"SELECT REPEAT(CAST(value AS STRING), 100) AS c1 FROM $tbl"))

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark.sql("select repeat(c1, 3) from parquetV1Table").noop()
        }

        benchmark.addCase("SQL Parquet - Comet (Scan)") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "true") {
            spark.sql("select repeat(c1, 3) from parquetV1Table").noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Scan, Exec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ALL_OPERATOR_ENABLED.key -> "true") {
            spark.sql("select repeat(c1, 3) from parquetV1Table").noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def reverseExprBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Expr reverse", values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(dir, spark.sql(s"SELECT REPEAT(CAST(value AS STRING), 100) AS c1 FROM $tbl"))

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark.sql("select reverse(c1) from parquetV1Table").noop()
        }

        benchmark.addCase("SQL Parquet - Comet (Scan)") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "true") {
            spark.sql("select reverse(c1) from parquetV1Table").noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Scan, Exec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ALL_OPERATOR_ENABLED.key -> "true") {
            spark.sql("select reverse(c1) from parquetV1Table").noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def instrExprBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Expr instr", values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(dir, spark.sql(s"SELECT REPEAT(CAST(value AS STRING), 100) AS c1 FROM $tbl"))

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark.sql("select instr(c1, '123') from parquetV1Table").noop()
        }

        benchmark.addCase("SQL Parquet - Comet (Scan)") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "true") {
            spark.sql("select instr(c1, '123') from parquetV1Table").noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Scan, Exec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ALL_OPERATOR_ENABLED.key -> "true") {
            spark.sql("select instr(c1, '123') from parquetV1Table").noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def replaceExprBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Expr replace", values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(dir, spark.sql(s"SELECT REPEAT(CAST(value AS STRING), 100) AS c1 FROM $tbl"))

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark.sql("select replace(c1, '123', 'abc') from parquetV1Table").noop()
        }

        benchmark.addCase("SQL Parquet - Comet (Scan)") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "true") {
            spark.sql("select replace(c1, '123', 'abc') from parquetV1Table").noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Scan, Exec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ALL_OPERATOR_ENABLED.key -> "true") {
            spark.sql("select replace(c1, '123', 'abc') from parquetV1Table").noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def translateExprBenchmark(values: Int): Unit = {
    val benchmark = new Benchmark("Expr translate", values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(dir, spark.sql(s"SELECT REPEAT(CAST(value AS STRING), 100) AS c1 FROM $tbl"))

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark.sql("select translate(c1, '123456', 'aBcDeF') from parquetV1Table").noop()
        }

        benchmark.addCase("SQL Parquet - Comet (Scan)") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "true") {
            spark.sql("select translate(c1, '123456', 'aBcDeF') from parquetV1Table").noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Scan, Exec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ALL_OPERATOR_ENABLED.key -> "true") {
            spark.sql("select translate(c1, '123456', 'aBcDeF') from parquetV1Table").noop()
          }
        }

        benchmark.run()
      }
    }
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val values = 1024 * 1024;

    runBenchmarkWithTable("Substring", values) { v =>
      subStringExprBenchmark(v)
    }

    runBenchmarkWithTable("StringSpace", values) { v =>
      stringSpaceExprBenchmark(v)
    }

    runBenchmarkWithTable("ascii", values) { v =>
      asciiExprBenchmark(v)
    }

    runBenchmarkWithTable("bitLength", values) { v =>
      bitLengthExprBenchmark(v)
    }

    runBenchmarkWithTable("octet_length", values) { v =>
      octetLengthExprBenchmark(v)
    }

    runBenchmarkWithTable("upper", values) { v =>
      upperExprBenchmark(v)
    }

    runBenchmarkWithTable("lower", values) { v =>
      lowerExprBenchmark(v)
    }

    runBenchmarkWithTable("chr", values) { v =>
      chrExprBenchmark(v)
    }

    runBenchmarkWithTable("initCap", values) { v =>
      initCapExprBenchmark(v)
    }

    runBenchmarkWithTable("trim", values) { v =>
      trimExprBenchmark(v)
    }

    runBenchmarkWithTable("concatws", values) { v =>
      concatwsExprBenchmark(v)
    }

    runBenchmarkWithTable("repeat", values) { v =>
      repeatExprBenchmark(v)
    }

    runBenchmarkWithTable("length", values) { v =>
      lengthExprBenchmark(v)
    }

    runBenchmarkWithTable("reverse", values) { v =>
      reverseExprBenchmark(v)
    }

    runBenchmarkWithTable("instr", values) { v =>
      instrExprBenchmark(v)
    }

    runBenchmarkWithTable("replace", values) { v =>
      replaceExprBenchmark(v)
    }

    runBenchmarkWithTable("translate", values) { v =>
      translateExprBenchmark(v)
    }
  }
}
