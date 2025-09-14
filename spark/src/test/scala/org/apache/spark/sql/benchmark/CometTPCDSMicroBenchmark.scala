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

import scala.io.Source

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.execution.benchmark.TPCDSQueryBenchmark.tables
import org.apache.spark.sql.execution.benchmark.TPCDSQueryBenchmarkArguments
import org.apache.spark.sql.execution.datasources.LogicalRelation

import org.apache.comet.CometConf

/**
 * Benchmark to measure Comet query performance with micro benchmarks that use the TPCDS data.
 * These queries represent subsets of the full TPCDS queries.
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
 * // CometTPCDSMicroBenchmark
 * SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometTPCDSMicroBenchmark -- --data-location /tmp/tpcds
 * }}}
 *
 * Results will be written to "spark/benchmarks/CometTPCDSMicroBenchmark-**results.txt".
 */
object CometTPCDSMicroBenchmark extends CometTPCQueryBenchmarkBase {

  val queries: Seq[String] = Seq(
    "scan_decimal",
    "add_decimals",
    "add_many_decimals",
    "add_many_integers",
    "agg_high_cardinality",
    "agg_low_cardinality",
    "agg_sum_decimals_no_grouping",
    "agg_sum_integers_no_grouping",
    // "agg_stddev",
    "case_when_column_or_null",
    "case_when_scalar",
    "char_type",
    "filter_highly_selective",
    "filter_less_selective",
    "if_column_or_null",
    "join_anti",
    "join_condition",
    "join_exploding_output",
    "join_inner",
    // "join_left_outer",
    "join_semi",
    "rlike",
    "to_json")

  override def runQueries(
      queryLocation: String,
      queries: Seq[String],
      tableSizes: Map[String, Long],
      benchmarkName: String,
      nameSuffix: String = ""): Unit = {
    queries.foreach { name =>
      val source = Source.fromFile(s"src/test/resources/tpcds-micro-benchmarks/$name.sql")
      val queryString = source
        .getLines()
        .filterNot(_.startsWith("--"))
        .mkString("\n")
      source.close()

      // This is an indirect hack to estimate the size of each query's input by traversing the
      // logical plan and adding up the sizes of all tables that appear in the plan.
      val queryRelations = scala.collection.mutable.HashSet[String]()
      cometSpark.sql(queryString).queryExecution.analyzed.foreach {
        case SubqueryAlias(alias, _: LogicalRelation) =>
          queryRelations.add(alias.name)
        case rel: LogicalRelation if rel.catalogTable.isDefined =>
          queryRelations.add(rel.catalogTable.get.identifier.table)
        case HiveTableRelation(tableMeta, _, _, _, _) =>
          queryRelations.add(tableMeta.identifier.table)
        case _ =>
      }
      val numRows = queryRelations.map(tableSizes.getOrElse(_, 0L)).sum
      val benchmark = new Benchmark(benchmarkName, numRows, 2, output = output)
      benchmark.addCase(s"$name$nameSuffix") { _ =>
        cometSpark.sql(queryString).noop()
      }
      benchmark.addCase(s"$name$nameSuffix: Comet") { _ =>
        withSQLConf(
          CometConf.COMET_ENABLED.key -> "true",
          CometConf.COMET_EXEC_ENABLED.key -> "true",
          CometConf.COMET_REGEXP_ALLOW_INCOMPATIBLE.key -> "true",
          // enabling COMET_EXPLAIN_NATIVE_ENABLED may add overhead but is useful for debugging
          CometConf.COMET_EXPLAIN_NATIVE_ENABLED.key -> "false") {
          cometSpark.sql(queryString).noop()
        }
      }
      benchmark.run()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val benchmarkArgs = new TPCDSQueryBenchmarkArguments(mainArgs)

    // If `--query-filter` defined, filters the queries that this option selects
    val queriesToRun = filterQueries(queries, benchmarkArgs.queryFilter)

    val tableSizes = setupTables(
      benchmarkArgs.dataLocation,
      createTempView = false,
      tables,
      TPCDSSchemaHelper.getTableColumns)

    setupCBO(cometSpark, benchmarkArgs.cboEnabled, tables)

    runQueries("tpcdsmicro", queries = queriesToRun, tableSizes, "TPCDS Micro Benchmarks")
  }
}
