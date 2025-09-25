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
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{CometTPCQueryBase, SparkSession}
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.execution.datasources.LogicalRelation

import org.apache.comet.CometConf

/**
 * Base class for CometTPCDSQueryBenchmark and CometTPCHQueryBenchmark Mostly copied from
 * TPCDSQueryBenchmark. TODO: make TPCDSQueryBenchmark extensible to avoid copies
 */
trait CometTPCQueryBenchmarkBase extends SqlBasedBenchmark with CometTPCQueryBase with Logging {
  override def getSparkSession: SparkSession = cometSpark

  protected def runQueries(
      queryLocation: String,
      queries: Seq[String],
      tableSizes: Map[String, Long],
      benchmarkName: String,
      nameSuffix: String = ""): Unit = {
    queries.foreach { name =>
      val queryString = resourceToString(
        s"$queryLocation/$name.sql",
        classLoader = Thread.currentThread().getContextClassLoader)

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
          CometConf.COMET_EXEC_ENABLED.key -> "true") {
          cometSpark.sql(queryString).noop()
        }
      }
      benchmark.run()
    }
  }
}
