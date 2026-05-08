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
 * Benchmark user-registered ScalaUDFs composed in trees, comparing the codegen dispatcher to the
 * "feature off" baseline (where a user UDF forces the containing operator to Spark) and to
 * Comet's native built-ins that are functionally equivalent.
 *
 * Four modes per composition:
 *
 *   - '''Spark''': all Comet disabled.
 *   - '''Comet (native built-ins)''': the composition rewritten using Comet-native Spark
 *     built-ins (`upper`, `lower`, `reverse`, `concat`, `length`). Ceiling for what pure native
 *     can do.
 *   - '''Comet (user UDFs, dispatcher disabled)''': user UDFs with
 *     `codegenDispatch.mode=disabled`. `CometScalaUDF.convert` returns `None`, the ScalaUDF's
 *     Project falls back to Spark. This is the state before the dispatcher landed: any user UDF
 *     loses Comet acceleration on the whole hosting operator.
 *   - '''Comet (user UDFs, codegen dispatch)''': user UDFs with the dispatcher forced on. One
 *     Janino-compiled kernel per (tree, input schema) handles the whole composition in one JNI
 *     hop.
 *
 * Story the numbers should tell: dispatcher (mode 4) tracks native (mode 2) and beats
 * dispatcher-disabled (mode 3) by the cost of the Spark fallback / ColumnarToRow hand-off.
 *
 * To run:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 \
 *     make benchmark-org.apache.spark.sql.benchmark.CometScalaUDFCompositionBenchmark
 * }}}
 */
object CometScalaUDFCompositionBenchmark extends CometBenchmarkBase {

  private def registerThreeLevelUdfs(): Unit = {
    spark.udf.register("lvl1_upper", (s: String) => if (s == null) null else s.toUpperCase)
    spark.udf.register("lvl2_reverse", (s: String) => if (s == null) null else s.reverse)
    spark.udf.register("lvl3_length", (s: String) => if (s == null) -1 else s.length)
  }

  private def registerMultiColUdfs(): Unit = {
    spark.udf.register("upperU", (s: String) => if (s == null) null else s.toUpperCase)
    spark.udf.register("lowerU", (s: String) => if (s == null) null else s.toLowerCase)
    spark.udf.register(
      "joinU",
      (a: String, b: String) => if (a == null || b == null) null else s"$a-$b")
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    runBenchmarkWithTable("scalaudf composition", 1024 * 1024) { v =>
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          prepareTable(
            dir,
            spark.sql(s"SELECT REPEAT(CAST(value AS STRING), 10) AS c1 FROM $tbl"))

          registerThreeLevelUdfs()
          runBenchmark("three-level composition: length(reverse(upper(c1)))") {
            runModes(
              name = "three-level",
              cardinality = v,
              nativeQuery = "SELECT length(reverse(upper(c1))) FROM parquetV1Table",
              udfQuery = "SELECT lvl3_length(lvl2_reverse(lvl1_upper(c1))) FROM parquetV1Table")
          }
        }
      }

      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          prepareTable(
            dir,
            spark.sql(
              s"SELECT REPEAT(CAST(value AS STRING), 10) AS c1, " +
                s"CAST(value AS STRING) AS c2 FROM $tbl"))

          registerMultiColUdfs()
          runBenchmark("multi-col composition: concat(upper(c1), '-', lower(c2))") {
            runModes(
              name = "multi-col",
              cardinality = v,
              nativeQuery = "SELECT concat(upper(c1), '-', lower(c2)) FROM parquetV1Table",
              udfQuery = "SELECT joinU(upperU(c1), lowerU(c2)) FROM parquetV1Table")
          }
        }
      }

      // Aggregate shape: SUM over the composition output. Picks up the cost of "dispatcher
      // disabled" breaking the columnar pipeline around an aggregate, not just the Project
      // itself. When the dispatcher is off, the Project falls back to Spark, which typically
      // drags the surrounding HashAggregate off Comet's columnar path too (ColumnarToRow hand-off
      // plus Spark's row-based aggregate). When the dispatcher is on, scan -> project -> agg
      // stays columnar end to end.
      withTempPath { dir =>
        withTempTable("parquetV1Table") {
          prepareTable(
            dir,
            spark.sql(s"SELECT REPEAT(CAST(value AS STRING), 10) AS c1 FROM $tbl"))

          registerThreeLevelUdfs()
          runBenchmark("agg over composition: SUM(length(reverse(upper(c1))))") {
            runModes(
              name = "agg-over-composition",
              cardinality = v,
              nativeQuery = "SELECT SUM(length(reverse(upper(c1)))) FROM parquetV1Table",
              udfQuery =
                "SELECT SUM(lvl3_length(lvl2_reverse(lvl1_upper(c1)))) FROM parquetV1Table")
          }
        }
      }
    }
  }

  private def runModes(
      name: String,
      cardinality: Long,
      nativeQuery: String,
      udfQuery: String): Unit = {
    val benchmark = new Benchmark(name, cardinality, output = output)

    benchmark.addCase("Spark") { _ =>
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        spark.sql(udfQuery).noop()
      }
    }

    // Pure Comet-native rewrite of the composition using built-ins. Ceiling for native perf.
    // Case conversion is enabled because upper/lower are in the tree.
    benchmark.addCase("Comet (native built-ins)") { _ =>
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_CASE_CONVERSION_ENABLED.key -> "true") {
        spark.sql(nativeQuery).noop()
      }
    }

    // User UDFs with dispatcher disabled. The ScalaUDF serde returns None, the hosting Project
    // falls back to Spark. State of the world before the dispatcher landed: any ScalaUDF in a
    // query sinks the containing operator.
    benchmark.addCase("Comet (user UDFs, dispatcher disabled)") { _ =>
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_CODEGEN_DISPATCH_MODE.key -> CometConf.CODEGEN_DISPATCH_DISABLED) {
        spark.sql(udfQuery).noop()
      }
    }

    // User UDFs through the codegen dispatcher. One Janino-compiled kernel for the whole tree,
    // one JNI hop per batch.
    benchmark.addCase("Comet (user UDFs, codegen dispatch)") { _ =>
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_CODEGEN_DISPATCH_MODE.key -> CometConf.CODEGEN_DISPATCH_FORCE) {
        spark.sql(udfQuery).noop()
      }
    }

    benchmark.run()
  }
}
