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

import java.util.Locale

/**
 * Benchmark to measure Comet TPCH query performance.
 *
 * To run this benchmark:
 * {{{
 * // Set scale factor in GB
 * scale_factor=1
 *
 * // GenTPCHData to create the data set at /tmp/tpch/sf1_parquet
 * cd $COMET_HOME
 * make benchmark-org.apache.spark.sql.GenTPCHData -- --location /tmp --scaleFactor ${scale_factor}
 *
 * // CometTPCHQueryBenchmark
 * SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometTPCHQueryBenchmark -- --data-location /tmp/tpch/sf${scale_factor}_parquet
 * }}}
 *
 * Results will be written to "spark/benchmarks/CometTPCHQueryBenchmark-**results.txt".
 */
object CometTPCHQueryBenchmark extends CometTPCQueryBenchmarkBase {
  val tables: Seq[String] =
    Seq("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val benchmarkArgs = new TPCHQueryBenchmarkArguments(mainArgs)

    // List of all TPC-H queries
    val tpchQueries = (1 to 22).map(n => s"q$n")

    // If `--query-filter` defined, filters the queries that this option selects
    val queries = filterQueries(tpchQueries, benchmarkArgs.queryFilter)

    if (queries.isEmpty) {
      throw new RuntimeException(
        s"Empty queries to run. Bad query name filter: ${benchmarkArgs.queryFilter}")
    }

    val tableSizes =
      setupTables(benchmarkArgs.dataLocation, createTempView = !benchmarkArgs.cboEnabled, tables)

    setupCBO(cometSpark, benchmarkArgs.cboEnabled, tables)

    runQueries("tpch", queries, tableSizes, "TPCH Snappy")
    runQueries("tpch-extended", queries, tableSizes, " TPCH Extended Snappy")
  }
}

/**
 * Mostly copied from TPCDSQueryBenchmarkArguments. Only the help message is different TODO: make
 * TPCDSQueryBenchmarkArguments extensible to avoid copies
 */
class TPCHQueryBenchmarkArguments(val args: Array[String]) {
  var dataLocation: String = null
  var queryFilter: Set[String] = Set.empty
  var cboEnabled: Boolean = false

  parseArgs(args.toList)
  validateArguments()

  private def optionMatch(optionName: String, s: String): Boolean = {
    optionName == s.toLowerCase(Locale.ROOT)
  }

  private def parseArgs(inputArgs: List[String]): Unit = {
    var args = inputArgs

    while (args.nonEmpty) {
      args match {
        case optName :: value :: tail if optionMatch("--data-location", optName) =>
          dataLocation = value
          args = tail

        case optName :: value :: tail if optionMatch("--query-filter", optName) =>
          queryFilter = value.toLowerCase(Locale.ROOT).split(",").map(_.trim).toSet
          args = tail

        case optName :: tail if optionMatch("--cbo", optName) =>
          cboEnabled = true
          args = tail

        case _ =>
          // scalastyle:off println
          System.err.println("Unknown/unsupported param " + args)
          // scalastyle:on println
          printUsageAndExit(1)
      }
    }
  }

  private def printUsageAndExit(exitCode: Int): Unit = {
    // scalastyle:off
    System.err.println("""
      |Usage: spark-submit --class <this class> <spark sql test jar> [Options]
      |Options:
      |  --data-location      Path to TPCH data
      |  --query-filter       Queries to filter, e.g., q3,q5,q13
      |  --cbo                Whether to enable cost-based optimization
      |
      |------------------------------------------------------------------------------------------------------------------
      |In order to run this benchmark, please follow the instructions of
      |org.apache.spark.sql.GenTPCHData to generate the TPCH data.
      |Thereafter, the value of <TPCH data location> needs to be set to the location where the generated data is stored.
      """.stripMargin)
    // scalastyle:on
    System.exit(exitCode)
  }

  private def validateArguments(): Unit = {
    if (dataLocation == null) {
      // scalastyle:off println
      System.err.println("Must specify a data location")
      // scalastyle:on println
      printUsageAndExit(-1)
    }
  }
}
