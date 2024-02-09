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

package org.apache.spark.sql

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.benchmark.TPCDSQueryBenchmarkArguments

/**
 * Utility to list Comet execution enabling status for TPCH queries.
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
 * // CometTPCHQueriesList
 * make benchmark-org.apache.spark.sql.CometTPCHQueriesList -- --data-location /tmp/tpch/sf${scale_factor}_parquet
 * }}}
 *
 * Results will be written to "spark/inspections/CometTPCHQueriesList-results.txt".
 */
object CometTPCHQueriesList extends CometTPCQueryListBase with CometTPCQueryBase with Logging {
  val tables: Seq[String] =
    Seq("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")

  override def runSuite(mainArgs: Array[String]): Unit = {
    val benchmarkArgs = new TPCDSQueryBenchmarkArguments(mainArgs)

    // List of all TPC-H queries
    val tpchQueries = (1 to 22).map(n => s"q$n")

    // If `--query-filter` defined, filters the queries that this option selects
    val queries = filterQueries(tpchQueries, benchmarkArgs.queryFilter)

    if (queries.isEmpty) {
      throw new RuntimeException(
        s"Empty queries to run. Bad query name filter: ${benchmarkArgs.queryFilter}")
    }

    setupTables(benchmarkArgs.dataLocation, createTempView = !benchmarkArgs.cboEnabled, tables)

    setupCBO(cometSpark, benchmarkArgs.cboEnabled, tables)

    runQueries("tpch", queries, " TPCH Snappy")
    runQueries("tpch-extended", queries, " TPCH Extended Snappy")
  }
}
