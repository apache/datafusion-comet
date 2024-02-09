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

import org.apache.spark.sql.{TPCDSQueries, TPCDSSchema}
import org.apache.spark.sql.execution.benchmark.TPCDSQueryBenchmark.tables
import org.apache.spark.sql.execution.benchmark.TPCDSQueryBenchmarkArguments
import org.apache.spark.sql.types.StructType

/**
 * Benchmark to measure Comet TPCDS query performance.
 *
 * To run this benchmark:
 * {{{
 * // Build [tpcds-kit](https://github.com/databricks/tpcds-kit)
 * cd /tmp && git clone https://github.com/databricks/tpcds-kit.git
 * cd tpcds-kit/tools && make OS=MACOS
 *
 * // GenTPCDSData
 * cd $COMET_HOME && mkdir /tmp/tpcds
 * make benchmark-org.apache.spark.sql.GenTPCDSData -- --dsdgenDir /tmp/tpcds-kit/tools --location /tmp/tpcds --scaleFactor 1
 *
 * // CometTPCDSQueryBenchmark
 * SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometTPCDSQueryBenchmark -- --data-location /tmp/tpcds
 * }}}
 *
 * Results will be written to "spark/benchmarks/CometTPCDSQueryBenchmark-**results.txt".
 */
object CometTPCDSQueryBenchmark extends CometTPCQueryBenchmarkBase with TPCDSQueries {
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val benchmarkArgs = new TPCDSQueryBenchmarkArguments(mainArgs)

    // If `--query-filter` defined, filters the queries that this option selects
    val queriesV1_4ToRun = filterQueries(tpcdsQueries, benchmarkArgs.queryFilter)
    val queriesV2_7ToRun = filterQueries(
      tpcdsQueriesV2_7,
      benchmarkArgs.queryFilter,
      nameSuffix = nameSuffixForQueriesV2_7)

    if ((queriesV1_4ToRun ++ queriesV2_7ToRun).isEmpty) {
      throw new RuntimeException(
        s"Empty queries to run. Bad query name filter: ${benchmarkArgs.queryFilter}")
    }

    val tableSizes = setupTables(
      benchmarkArgs.dataLocation,
      createTempView = false,
      tables,
      TPCDSSchemaHelper.getTableColumns)

    setupCBO(cometSpark, benchmarkArgs.cboEnabled, tables)

    runQueries("tpcds", queries = queriesV1_4ToRun, tableSizes, "TPCDS Snappy")
    runQueries(
      "tpcds-v2.7.0",
      queries = queriesV2_7ToRun,
      tableSizes,
      "TPCDS Snappy",
      nameSuffix = nameSuffixForQueriesV2_7)
  }
}

object TPCDSSchemaHelper extends TPCDSSchema {
  def getTableColumns: Map[String, StructType] =
    tableColumns.map(kv => kv._1 -> StructType.fromDDL(kv._2))
}
