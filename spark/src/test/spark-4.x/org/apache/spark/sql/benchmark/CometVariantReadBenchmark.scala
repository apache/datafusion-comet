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
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.comet.CometNativeScanExec
import org.apache.spark.types.variant.VariantBuilder

import org.apache.comet.CometConf

/**
 * Matched warm local scans with repeated Parquet dictionaries. Both readers hash every returned
 * Variant's value and metadata bytes. Includes row conversion and consumption; excludes writes.
 * Run with -Pspark-4.0 or -Pspark-4.1 using benchmark-org.apache.spark.sql.benchmark.
 * CometVariantReadBenchmark [rows] [payloadBytes] [--reverse-cases].
 */
object CometVariantReadBenchmark extends CometBenchmarkBase {
  override def runCometBenchmark(args: Array[String]): Unit = {
    val sizes = args.filterNot(_ == "--reverse-cases")
    val rows = sizes.headOption.map(_.toInt).getOrElse(100000)
    val payloadBytes = sizes.lift(1).map(_.toInt).getOrElse(1024)
    val payload = "x" * payloadBytes
    val readers = if (args.contains("--reverse-cases")) Seq(true, false) else Seq(false, true)
    runBenchmark("Variant scans with repeated dictionaries") {
      withSQLConf(
        "spark.sql.sources.useV1SourceList" -> "parquet",
        "spark.sql.adaptive.enabled" -> "false",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "true",
        CometConf.COMET_ONHEAP_ENABLED.key -> "true",
        "spark.sql.variant.allowReadingShredded" -> "true",
        "spark.sql.variant.pushVariantIntoScan" -> "false") {
        for (shape <- Seq("canonical", "partially shredded", "fully shredded", "empty key")) {
          val key = if (shape == "empty key") "" else "payload"
          val json =
            if (shape == "canonical") s"""{"known":1,"$key":"$payload"}"""
            else s"""{"$key":"$payload"}"""
          // Parquet metadata includes shredded keys too; residual IDs use that dictionary.
          val builder = new VariantBuilder(false)
          Seq("known", key).foreach(builder.addKey)
          builder.appendVariant(VariantBuilder.parseJson(json, false))
          val value = builder.result()
          def binary(bytes: Array[Byte]): String =
            "X'" + bytes.map(b => f"${b & 0xff}%02X").mkString + "'"
          val metadata = binary(value.getMetadata)
          val residual = binary(value.getValue)
          val fields = shape match {
            case "canonical" => s"'metadata', $metadata, 'value', $residual"
            case "fully shredded" =>
              s"""'metadata', $metadata, 'typed_value', named_struct(
               |'known', named_struct('typed_value', 1),
               |'payload', named_struct('typed_value', '$payload'))""".stripMargin
            case _ =>
              s"""'metadata', $metadata, 'value', $residual, 'typed_value',
               |named_struct('known', named_struct('typed_value', 1))""".stripMargin
          }
          withTempPath { dir =>
            spark
              .sql(s"SELECT named_struct($fields) AS v FROM range($rows)")
              .coalesce(1)
              .write
              .option("parquet.enable.dictionary", "true")
              .parquet(dir.getCanonicalPath)
            def read() = spark.read.schema("v VARIANT").parquet(dir.getCanonicalPath)
            val expected = read().head()
            val benchmark = new Benchmark(
              s"Variant $shape: $payloadBytes payload bytes",
              rows,
              minNumIters = 5,
              output = output)
            for (enabled <- readers) {
              withSQLConf(CometConf.COMET_ENABLED.key -> enabled.toString) {
                val df = read()
                assert(df.head() == expected)
                assert(collect(df.queryExecution.executedPlan) { case scan: CometNativeScanExec =>
                  scan
                }.nonEmpty == enabled)
              }
              benchmark.addCase(if (enabled) "Comet" else "Spark") { _ =>
                withSQLConf(CometConf.COMET_ENABLED.key -> enabled.toString) {
                  read()
                    .mapPartitions { rows =>
                      Iterator.single(
                        rows.foldLeft(0L)((sum, row) => sum + row.get(0).hashCode()))
                    }(Encoders.scalaLong)
                    .collect()
                }
              }
            }
            benchmark.run()
          }
        }
      }
    }
  }
}
