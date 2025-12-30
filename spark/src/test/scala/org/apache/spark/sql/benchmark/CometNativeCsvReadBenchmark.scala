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

import scala.util.Random

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.benchmark.CometExecBenchmark.withSQLConf
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataTypes, StructType}

import org.apache.comet.CometConf
import org.apache.comet.testing.{CsvGenerator, FuzzDataGenerator, SchemaGenOptions}

/**
 * Benchmark to measure Comet read performance. To run this benchmark:
 * `SPARK_GENERATE_BENCHMARK_FILES=1 make
 * benchmark-org.apache.spark.sql.benchmark.CometNativeCsvReadBenchmark` Results will be written
 * to "spark/benchmarks/CometNativeCsvReadBenchmark-**results.txt".
 */
object CometNativeCsvReadBenchmark extends CometBenchmarkBase {

  private def prepareCsvTable(dir: File, schema: StructType, numRows: Int): Unit = {
    val random = new Random(42)
    CsvGenerator.makeCsvFile(random, spark, schema, dir.getCanonicalPath, numRows)
  }

  override def runCometBenchmark(args: Array[String]): Unit = {
    val numRows = 2000000
    val benchmark = new Benchmark(s"Native csv read - $numRows rows", numRows, output = output)
    withTempPath { dir =>
      val schema = FuzzDataGenerator.generateSchema(
        SchemaGenOptions(primitiveTypes = Seq(
          DataTypes.BooleanType,
          DataTypes.ByteType,
          DataTypes.ShortType,
          DataTypes.IntegerType,
          DataTypes.LongType,
          DataTypes.FloatType,
          DataTypes.DoubleType,
          DataTypes.createDecimalType(10, 2),
          DataTypes.createDecimalType(36, 18),
          DataTypes.DateType,
          DataTypes.StringType)))
      prepareCsvTable(dir, schema, numRows)
      benchmark.addCase("Simple read") { _ =>
        withSQLConf(
          CometConf.COMET_ENABLED.key -> "true",
          CometConf.COMET_EXEC_ENABLED.key -> "true",
          CometConf.COMET_CSV_V2_NATIVE_ENABLED.key -> "true",
          CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.key -> "true",
          SQLConf.USE_V1_SOURCE_LIST.key -> "") {
          spark.read
            .schema(schema)
            .csv(dir.getCanonicalPath)
            .noop()
        }
      }
      benchmark.run()
    }
  }
}
