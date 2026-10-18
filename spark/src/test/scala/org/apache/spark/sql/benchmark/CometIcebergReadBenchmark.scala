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

import java.io.File

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.comet.CometIcebergNativeScanExec
import org.apache.spark.sql.types._

import org.apache.comet.CometConf

/**
 * Benchmark to measure Comet Iceberg read performance. To run this benchmark:
 * `SPARK_GENERATE_BENCHMARK_FILES=1 make
 * benchmark-org.apache.spark.sql.benchmark.CometIcebergReadBenchmark` Results will be written to
 * "spark/benchmarks/CometIcebergReadBenchmark-**results.txt". Pass `--nested-only` to run only
 * the nested column cases.
 */
object CometIcebergReadBenchmark extends CometBenchmarkBase {

  def icebergScanBenchmark(values: Int, dataType: DataType): Unit = {
    val sqlBenchmark =
      new Benchmark(
        s"SQL Single ${dataType.sql} Iceberg Column Scan",
        values.toLong,
        output = output)

    withTempPath { dir =>
      withTempTable("icebergTable") {
        prepareIcebergTable(
          dir,
          spark.sql(s"SELECT CAST(value as ${dataType.sql}) id FROM $tbl"),
          "icebergTable")

        val query = dataType match {
          case BooleanType => "sum(cast(id as bigint))"
          case _ => "sum(id)"
        }

        sqlBenchmark.addCase("SQL Iceberg - Spark") { _ =>
          withSQLConf(
            "spark.memory.offHeap.enabled" -> "true",
            "spark.memory.offHeap.size" -> "10g") {
            spark.sql(s"select $query from icebergTable").noop()
          }
        }

        sqlBenchmark.addCase("SQL Iceberg - Comet Iceberg-Rust") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            "spark.memory.offHeap.enabled" -> "true",
            "spark.memory.offHeap.size" -> "10g",
            CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {
            spark.sql(s"select $query from icebergTable").noop()
          }
        }

        sqlBenchmark.run()
      }
    }
  }

  /**
   * Reads a TPC-H style nested `partsupp` table, whose `partsupp_data` column is an
   * `array<struct>` dominated by a text field, as in the nested TPC-H schema. Spark prunes the
   * struct to the fields a query uses. The pruned Comet case passes that pruned schema to
   * iceberg-rust, and the other reads every nested field and drops the unused ones after
   * decoding.
   */
  def icebergNestedProjectionBenchmark(values: Int): Unit = {
    withTempPath { dir =>
      configureIcebergHadoopCatalog(new File(dir, "iceberg-warehouse"))
      val table = s"$defaultIcebergCatalog.db.partsupp_nested"
      spark.sql(s"""
        CREATE TABLE $table (
          ps_partkey BIGINT,
          ps_suppkey BIGINT,
          partsupp_data ARRAY<STRUCT<
            ps_availqty: INT, ps_supplycost: DECIMAL(15, 2), ps_comment: STRING>>
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd')
      """)
      // TPC-H style comments: 16 words from a small vocabulary, about 128 characters each.
      val words = ("furiously carefully quickly slyly blithely fluffily ironic final regular " +
        "express special pending unusual bold even silent idle ruthless packages requests " +
        "accounts deposits foxes ideas theodolites pinto beans instructions dependencies " +
        "excuses platelets asymptotes courts dolphins sleep wake haggle nag use boost")
        .split(" ")
        .map(w => s"'$w'")
        .mkString(", ")
      spark.sql(s"""
        INSERT INTO $table
        SELECT id DIV 4, id % 4 + 1, array(named_struct(
          'ps_availqty', CAST(pmod(xxhash64(id, 1), 9999) + 1 AS INT),
          'ps_supplycost', CAST(pmod(xxhash64(id, 2), 99901) / 100 + 1 AS DECIMAL(15, 2)),
          'ps_comment', concat_ws(' ', transform(sequence(1, 16),
            i -> element_at(array($words), CAST(pmod(xxhash64(id, i), 40) + 1 AS INT))))))
        FROM range($values)
      """)

      val cometIceberg = Seq(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true")
      def pruneNestedFields(enabled: Boolean): (String, String) =
        CometConf.COMET_ICEBERG_NESTED_SCHEMA_PRUNING_ENABLED.key -> enabled.toString

      def explodeField(aggregate: String, field: String): String =
        s"SELECT $aggregate FROM $table LATERAL VIEW explode(partsupp_data.$field) e AS v"

      Seq(
        "one small field, as TPC-H q20 reads partsupp" -> explodeField("sum(v)", "ps_availqty"),
        "the text field, which pruning cannot skip" ->
          explodeField("sum(length(v))", "ps_comment"))
        .foreach { case (name, query) =>
          val benchmark =
            new Benchmark(s"Nested Iceberg column, $name", values.toLong, output = output)

          benchmark.addCase("SQL Iceberg - Spark") { _ =>
            spark.sql(query).noop()
          }
          benchmark.addCase("SQL Iceberg - Comet, every nested field") { _ =>
            withSQLConf(cometIceberg :+ pruneNestedFields(false): _*) {
              spark.sql(query).noop()
            }
          }
          benchmark.addCase("SQL Iceberg - Comet, pruned nested fields") { _ =>
            withSQLConf(cometIceberg :+ pruneNestedFields(true): _*) {
              spark.sql(query).noop()
            }
          }

          val bytesScanned = Seq(false, true).map { enabled =>
            var bytes = 0L
            withSQLConf(cometIceberg :+ pruneNestedFields(enabled): _*) {
              val df = spark.sql(query)
              df.collect()
              bytes = collect(df.queryExecution.executedPlan) {
                case scan: CometIcebergNativeScanExec => scan.metrics("bytes_scanned").value
              }.sum
            }
            bytes / 1024.0 / 1024.0
          }
          warn(
            benchmark,
            f"Comet bytes scanned: every nested field ${bytesScanned.head}%.1f MiB, " +
              f"pruned nested fields ${bytesScanned.last}%.1f MiB")
          benchmark.run()
        }

      spark.sql(s"DROP TABLE $table")
    }
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    if (!mainArgs.contains("--nested-only")) {
      runBenchmarkWithTable("SQL Single Numeric Iceberg Column Scan", 1024 * 1024 * 128) { v =>
        Seq(BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType)
          .foreach(icebergScanBenchmark(v, _))
      }
    }
    runBenchmark("SQL Nested Iceberg Column Scan") {
      icebergNestedProjectionBenchmark(1024 * 1024 * 4)
    }
  }
}
