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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.optimizer.ConstantFolding
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.iceberg.IcebergReflection

/**
 * Benchmark of Iceberg's system functions (`bucket`, `truncate`, `years`, `months`, `days`,
 * `hours`) with Comet on and off. The Spark case is Iceberg's own JVM implementation: Spark binds
 * each function as a `StaticInvoke` of the matching class under
 * `org.apache.iceberg.spark.functions` and whole-stage codegen calls it once per row.
 *
 * Every case is run over a no-null column and a column with one null in eight, and every case's
 * output is compared between the two engines over the same corpus that is then timed, so a timing
 * cannot come from an engine that computed something else.
 *
 * To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometIcebergSystemFunctionBenchmark
 * }}}
 * Results will be written to
 * "spark/benchmarks/CometIcebergSystemFunctionBenchmark-**results.txt".
 */
object CometIcebergSystemFunctionBenchmark extends CometBenchmarkBase {

  private val catalog = "benchmark_cat"

  /** One null in eight, matching the null rate of the correctness suite's corpus. */
  private val NullStride = 8

  /**
   * Column types each transform accepts. `str_dict` holds eight distinct values so Parquet
   * dictionary-encodes it, which is the shape a string partition column normally arrives in;
   * `str` is distinct per row. `truncate` on a decimal is absent because it falls back to Spark
   * (see the Iceberg user guide), so there is no native path to measure.
   */
  private val bucketTypes = Seq("int", "long", "dec", "str_dict", "str", "bin", "date", "ts")
  private val truncateTypes = Seq("int", "long", "str_dict", "str", "bin")

  /** (case name, query) for every transform, input type, and null variant. */
  private def cases: Seq[(String, String)] = {
    def variants(types: Seq[String])(select: String => String): Seq[(String, String)] =
      for {
        t <- types
        (suffix, tag) <- Seq("" -> "", "_n" -> ", nulls")
      } yield {
        val column = s"c_$t$suffix"
        s"$t$tag" -> s"select ${select(column)} from parquetV1Table"
      }

    val bucket = variants(bucketTypes)(c => s"$catalog.system.bucket(16, $c)")
      .map { case (name, query) => s"bucket($name)" -> query }
    val truncate = variants(truncateTypes)(c => s"$catalog.system.truncate(4, $c)")
      .map { case (name, query) => s"truncate($name)" -> query }
    val temporal = Seq("years", "months", "days").flatMap { fn =>
      variants(Seq("date", "ts"))(c => s"$catalog.system.$fn($c)").map { case (name, query) =>
        s"$fn($name)" -> query
      }
    }
    val hours = variants(Seq("ts"))(c => s"$catalog.system.hours($c)").map { case (name, query) =>
      s"hours($name)" -> query
    }
    bucket ++ truncate ++ temporal ++ hours
  }

  /**
   * Fails if the two engines disagree on `query`. Rows are compared positionally: both cases read
   * the same Parquet files with the same partitioning and neither plan shuffles, so the scan
   * order is the same. The confs match the ones the benchmark times.
   */
  private def verifyOutputsMatch(name: String, query: String): Unit = {
    def collect(cometEnabled: Boolean): Array[Row] =
      withSQLConf(
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> excludedRulesWith(ConstantFolding.ruleName),
        CometConf.COMET_ENABLED.key -> cometEnabled.toString,
        CometConf.COMET_EXEC_ENABLED.key -> cometEnabled.toString) {
        spark.sql(query).collect()
      }

    // `Row.equals` compares binary columns by reference, so normalize before comparing.
    def comparable(row: Row): String =
      row.toSeq
        .map {
          case bytes: Array[Byte] => bytes.mkString("[", ",", "]")
          case other => String.valueOf(other)
        }
        .mkString("|")

    val sparkRows = collect(false).map(comparable)
    val cometRows = collect(true).map(comparable)
    assert(
      sparkRows.length == cometRows.length,
      s"$name: Spark produced ${sparkRows.length} rows, Comet ${cometRows.length}")
    val mismatch = sparkRows.indices.find(i => sparkRows(i) != cometRows(i))
    mismatch.foreach { i =>
      throw new AssertionError(
        s"$name: row $i differs -- Spark ${sparkRows(i)}, Comet ${cometRows(i)}")
    }
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    if (!icebergOnClasspath) {
      // scalastyle:off println
      println("Iceberg is not on the classpath; skipping. Build with an Iceberg-enabled profile.")
      // scalastyle:on println
      return
    }
    // The Iceberg system functions are resolved through a v2 catalog, so one has to be
    // registered. No Iceberg table is read: the data stays in Parquet so both cases scan
    // identically and the only difference is who evaluates the transform.
    withTempPath { warehouse =>
      spark.conf.set(s"spark.sql.catalog.$catalog", "org.apache.iceberg.spark.SparkCatalog")
      spark.conf.set(s"spark.sql.catalog.$catalog.type", "hadoop")
      spark.conf.set(s"spark.sql.catalog.$catalog.warehouse", warehouse.getAbsolutePath)

      runBenchmarkWithTable("Iceberg system functions", 1024 * 1024) { v =>
        withTempPath { dir =>
          withTempTable("parquetV1Table") {
            prepareTable(dir, spark.sql(corpusQuery))

            cases.foreach { case (name, query) =>
              verifyOutputsMatch(name, query)
              runBenchmark(name) {
                runExpressionBenchmark(name, v, query)
              }
            }
          }
        }
      }
    }
  }

  /** Every column, followed by a `_n` twin carrying one null in [[NullStride]]. */
  private def corpusQuery: String = {
    val columns = Seq(
      "c_int" -> "CAST(value AS INT)",
      "c_long" -> "value",
      "c_dec" -> "CAST(value AS DECIMAL(38,10))",
      "c_str_dict" -> "CAST(PMOD(value, 8) AS STRING)",
      "c_str" -> "REPEAT(CAST(value AS STRING), 3)",
      "c_bin" -> "CAST(CAST(value AS STRING) AS BINARY)",
      "c_date" -> "DATE_ADD(DATE '1970-01-01', CAST(PMOD(value, 40000) AS INT))",
      "c_ts" -> "TIMESTAMP_SECONDS(PMOD(value, 4000000000))")
    val projections = columns.flatMap { case (name, expr) =>
      Seq(s"$expr AS $name", s"IF(PMOD(value, $NullStride) = 0, NULL, $expr) AS ${name}_n")
    }
    s"SELECT ${projections.mkString(", ")} FROM $tbl"
  }

  private def icebergOnClasspath: Boolean =
    try {
      IcebergReflection.loadClass("org.apache.iceberg.spark.functions.BucketFunction")
      true
    } catch {
      case _: ClassNotFoundException => false
    }
}
