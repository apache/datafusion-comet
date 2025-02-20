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
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}

import org.apache.comet.CometConf

object CometComplexTypeBenchmark extends CometBenchmarkBase {

  override def runCometBenchmark(args: Array[String]): Unit = {

    // create test data
    val schema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField(
          "metadata",
          StructType(
            Seq(
              StructField("name", StringType, nullable = true),
              StructField("tags", ArrayType(StringType), nullable = true))),
          nullable = true)))
    val data =
      Range(0, 1000000).map(i => Row(i, Row(s"name$i", Range(0, i % 10).map(j => s"tag$j"))))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    df.write.mode("overwrite").parquet("spark/target/complex_types.parquet")

    var benchmark = new Benchmark("bench1", 10, output = output)
    benchmark.addCase("read struct and array - Spark") { _ =>
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        spark.read.parquet("spark/target/complex_types.parquet").createOrReplaceTempView("t1")
        spark.sql("""SELECT SIZE(metadata.tags) AS tag_count, COUNT(*)
                    |FROM t1
                    |GROUP BY tag_count""".stripMargin)
      }
    }

    for (scan <- Seq("native_comet", "native_datafusion", "native_iceberg_compat")) {
      benchmark.addCase(s"read struct and array - Comet $scan") { _ =>
        withSQLConf(
          CometConf.COMET_ENABLED.key -> "true",
          CometConf.COMET_NATIVE_SCAN_IMPL.key -> scan,
          CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.key -> "true") {
          spark.read.parquet("spark/target/complex_types.parquet").createOrReplaceTempView("t1")
          spark.sql("""SELECT SIZE(metadata.tags) AS tag_count, COUNT(*)
                      |FROM t1
                      |GROUP BY tag_count""".stripMargin)
        }
      }
    }
    benchmark.run()

    benchmark = new Benchmark("bench2", 10, output = output)
    benchmark.addCase("to_json - Spark") { _ =>
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        spark.read.parquet("spark/target/complex_types.parquet").createOrReplaceTempView("t1")
        spark.sql("SELECT to_json(metadata) FROM t1")
      }
    }

    for (scan <- Seq("native_comet", "native_datafusion", "native_iceberg_compat")) {
      benchmark.addCase(s"to_json - Comet $scan") { _ =>
        withSQLConf(
          CometConf.COMET_ENABLED.key -> "true",
          CometConf.COMET_NATIVE_SCAN_IMPL.key -> scan,
          CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.key -> "true") {
          spark.read.parquet("spark/target/complex_types.parquet").createOrReplaceTempView("t1")
          spark.sql("SELECT to_json(metadata) FROM t1")
        }
      }
    }

    benchmark.run()
  }

}
